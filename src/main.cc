#include <algorithm>
#include <chrono>
#include <ctime>
#include <future>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include <drogon/HttpAppFramework.h>
#include <trantor/net/EventLoopThread.h>
#include <trantor/utils/Logger.h>

#include "alerts/AlertManager.h"
#include "alerts/CandleAlertMonitor.h"
#include "controllers/Routes.h"
#include "controllers/WsObserveController.h"
#include "core/AppContext.h"
#include "core/Config.h"
#include "ctrader/CTraderClient.h"
#include "ctrader/SymbolRegistry.h"
#include "market/MarketHub.h"
#include "services/Notifier.h"
#include "services/PostgresService.h"
#include "services/RedisService.h"
#include "util/ForexMarketHours.h"
#include "util/PairNormalizer.h"
#include "util/TimeUtil.h"

using namespace ctraderplus;

namespace {

void dispatchNotification(services::Notifier &notifier, services::RedisService *redis,
                          const std::string &dlqKey, const alerts::TriggeredAlert &t) {
    const alerts::Alert &a = t.alert;
    double target = a.alertType == "candle_close" ? a.threshold.value_or(0)
                                                  : a.targetPrice.value_or(0);
    std::string cond = a.alertType == "candle_close" ? a.direction.value_or("")
                                                     : a.condition.value_or("");

    auto onDone = [redis, dlqKey, a](bool ok) {
        if (!ok && redis && redis->connected()) {
            Json::Value j = a.toJson();
            Json::StreamWriterBuilder wb;
            wb["indentation"] = "";
            redis->pushJson(dlqKey, Json::writeString(wb, j));
        }
    };

    if (a.channel == "sms") {
        std::string text = !a.customMessage.empty()
                               ? a.customMessage
                               : services::Notifier::formatAlertMessage(
                                     a.pair, target, t.currentPrice, cond, "", a.alertType,
                                     t.timeframe);
        notifier.sendSms(a.phone, text, onDone);
    } else if (a.channel == "call") {
        std::string text = !a.customMessage.empty()
                               ? a.customMessage
                               : services::Notifier::formatAlertMessage(
                                     a.pair, target, t.currentPrice, cond, "", a.alertType,
                                     t.timeframe);
        notifier.sendCall(a.phone, text, onDone);
    } else {
        std::string msg = services::Notifier::formatAlertMessage(
            a.pair, target, t.currentPrice, cond, a.customMessage, a.alertType, t.timeframe);
        notifier.sendEmail(a.email, "Price Alert: " + a.pair, msg, onDone);
    }
}

}  // namespace

int main() {
    const auto &cfg = core::loadConfig();
    trantor::Logger::setLogLevel(trantor::Logger::kInfo);
    LOG_INFO << "Starting cTrader Plus C++ observer";

    // ---- Core singletons -------------------------------------------------
    static ctrader::SymbolRegistry registry;
    static ctrader::CTraderClient ctrader(cfg.ctrader);
    static market::MarketHub hub(cfg, registry);
    static alerts::AlertManager alertManager;
    static alerts::CandleAlertMonitor candleMonitor;
    static services::Notifier notifier(cfg);
    static services::PostgresService postgres(cfg);
    static services::RedisService redis(cfg);

    // ---- Worker loop for blocking DB + background tasks ------------------
    static trantor::EventLoopThread workerThread;
    workerThread.run();
    trantor::EventLoop *workerLoop = workerThread.getLoop();
    auto dbExec = [workerLoop](std::function<void()> task) {
        workerLoop->queueInLoop(std::move(task));
    };

    // ---- Connect Redis + Postgres (best effort) --------------------------
    bool redisOk = redis.connect();
    if (!redisOk) LOG_WARN << "Redis unavailable; cache and pub/sub disabled";
    bool pgOk = postgres.connect();
    if (!pgOk) LOG_WARN << "PostgreSQL unavailable; fix DATABASE_URL in .env";

    services::RedisService *redisPtr = redisOk ? &redis : nullptr;
    services::PostgresService *pgPtr = pgOk ? &postgres : nullptr;

    alertManager.configure(pgPtr, redisPtr, dbExec, cfg.redisAlertQueueKey);

    // ---- Wire MarketHub --------------------------------------------------
    hub.setRedis(redisPtr);
    hub.setPostgres(pgPtr);
    hub.setDbExecutor(dbExec);
    hub.setReadyFn([]() { return ctrader.isReady(); });
    hub.setBroadcastSink([](std::shared_ptr<Json::Value> grouped) {
        controllers::WsObserveController::broadcastToAll(std::move(grouped));
    });
    std::string dlqKey = cfg.notificationDlqKey;
    hub.setAlertSink([redisPtr, dlqKey](std::shared_ptr<std::vector<market::FlatPair>> pairs) {
        auto triggered = alertManager.checkPriceAlerts(*pairs);
        for (const auto &t : triggered)
            dispatchNotification(notifier, redisPtr, dlqKey, t);
    });

    // ---- Wire cTrader callbacks -----------------------------------------
    candleMonitor.configure(
        &cfg, &ctrader, &registry, &alertManager,
        [&](const alerts::TriggeredAlert &t) {
            dispatchNotification(notifier, redisPtr, dlqKey, t);
        });

    ctrader.setSymbolsCallback([](std::vector<ctrader::SymbolInfo> symbols) {
        registry.update(symbols);
    });
    ctrader.setStateCallback([&](bool ready) { candleMonitor.onConnectionReady(ready); });
    ctrader.setSpotCallback([&](const ctrader::SpotUpdate &u) {
        hub.onSpot(u);
        candleMonitor.onSpot(u);
    });

    // ---- Populate AppContext --------------------------------------------
    auto &app = core::AppContext::instance();
    app.config = &cfg;
    app.registry = &registry;
    app.ctrader = &ctrader;
    app.hub = &hub;
    app.alerts = &alertManager;
    app.postgres = pgPtr;
    app.redis = redisPtr;
    app.notifier = &notifier;
    app.dbExec = dbExec;
    app.startTime = std::chrono::steady_clock::now();

    // ---- Database migrations (blocking, before HTTP) ---------------------
    if (pgPtr) {
        LOG_INFO << "Running database migrations...";
        std::promise<int> migrationPromise;
        auto migrationFuture = migrationPromise.get_future();
        workerLoop->queueInLoop([&]() {
            try {
                int version = postgres.runMigrations();
                migrationPromise.set_value(version);
            } catch (...) {
                try {
                    migrationPromise.set_exception(std::current_exception());
                } catch (...) {
                }
            }
        });
        try {
            if (migrationFuture.wait_for(std::chrono::seconds(30)) !=
                std::future_status::ready) {
                LOG_ERROR << "Database migrations timed out after 30s";
                return 1;
            }
            int version = migrationFuture.get();
            app.dbMigrationsReady.store(true);
            LOG_INFO << "Database migrations complete (version=" << version << ")";
            workerLoop->queueInLoop([&]() { alertManager.loadAlerts(); });
        } catch (const std::exception &e) {
            LOG_ERROR << "Database migrations failed: " << e.what();
            return 1;
        }
    } else {
        LOG_WARN << "PostgreSQL unavailable; auth and user features disabled";
    }

    // ---- HTTP routes -----------------------------------------------------
    controllers::registerRoutes();

    // ---- Background tasks on the worker loop -----------------------------
    // Archive: drain Redis snapshot queue into PostgreSQL when market open.
    if (redisPtr && pgPtr) {
        workerLoop->runEvery(cfg.archiveIntervalSeconds, [&]() {
            if (!util::isForexMarketOpen()) return;
            redis.readQueue(cfg.archiveBatchSize, [&](std::vector<std::string> batch) {
                if (batch.empty()) return;
                workerLoop->queueInLoop([&, batch]() {
                    int n = postgres.insertSnapshots(batch);
                    if (n > 0) LOG_DEBUG << "Archived " << n << " price rows";
                });
            });
        });
    }

    // Retention: weekly cleanup at Sunday 22:00 UTC.
    if (pgPtr) {
        workerLoop->runEvery(60.0, [&]() {
            std::time_t now = std::time(nullptr);
            std::tm tmv{};
            gmtime_r(&now, &tmv);
            int weekdayMon0 = (tmv.tm_wday + 6) % 7;
            if (weekdayMon0 == 6 && tmv.tm_hour == 22 && tmv.tm_min < 5) {
                auto res = postgres.deleteOldData(cfg.retentionDays);
                LOG_INFO << "Retention cleanup: historical=" << res.first
                         << " metrics=" << res.second;
            }
        });
    }

    // Alert persistence flush: Redis event queue -> PostgreSQL.
    if (redisPtr && pgPtr) {
        constexpr int kAlertFlushBatchSize = 15;
        workerLoop->runEvery(0.25, [&]() {
            redis.readJsonQueue(cfg.redisAlertQueueKey, kAlertFlushBatchSize,
                                [&](std::vector<std::string> batch) {
                                    if (batch.empty()) return;
                                    workerLoop->queueInLoop([&, batch]() {
                                        for (const auto &js : batch) {
                                            Json::Value ev;
                                            Json::CharReaderBuilder b;
                                            std::unique_ptr<Json::CharReader> rd(
                                                b.newCharReader());
                                            std::string errs;
                                            rd->parse(js.c_str(), js.c_str() + js.size(), &ev,
                                                      &errs);
                                            std::string op = ev.get("op", "").asString();
                                            if (op == "upsert" && ev.isMember("alert"))
                                                postgres.upsertAlert(ev["alert"]);
                                            else if (op == "delete")
                                                postgres.deleteAlert(
                                                    ev.get("alert_id", "").asString());
                                        }
                                    });
                                });
        });
    }

    // Candle-close: live trendbar push (onSpot) + subscription sync + poll fallback.
    {
        double pollInterval =
            std::max(0.05, cfg.candleCheckIntervalSeconds);
        workerLoop->runEvery(pollInterval, [&]() { candleMonitor.pollFallback(); });
        workerLoop->runEvery(5.0, [&]() {
            if (ctrader.isReady() && util::isForexMarketOpen())
                candleMonitor.syncSubscriptions();
        });
    }

    // ---- Start cTrader + MarketHub on the HTTP loop ----------------------
    ctrader.start();
    drogon::app().getLoop()->queueInLoop([&]() { hub.start(drogon::app().getLoop()); });

    int threads = cfg.threadNum > 0 ? cfg.threadNum
                                    : std::max(1u, std::thread::hardware_concurrency());
    LOG_INFO << "HTTP listening on " << cfg.listenAddress << ":" << cfg.httpPort
             << " (threads=" << threads << ")";
    drogon::app()
        .addListener(cfg.listenAddress, cfg.httpPort)
        .setThreadNum(threads)
        .run();

    ctrader.stop();
    return 0;
}
