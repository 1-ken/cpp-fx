#include <algorithm>
#include <ctime>
#include <memory>
#include <set>
#include <thread>
#include <utility>
#include <vector>

#include <drogon/HttpAppFramework.h>
#include <trantor/net/EventLoopThread.h>
#include <trantor/utils/Logger.h>

#include "alerts/AlertManager.h"
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
    bool pgOk = postgres.connect();

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
    ctrader.setSymbolsCallback([](std::vector<ctrader::SymbolInfo> symbols) {
        registry.update(symbols);
    });
    ctrader.setSpotCallback([](const ctrader::SpotUpdate &u) { hub.onSpot(u); });

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
        workerLoop->runEvery(0.25, [&]() {
            redis.readJsonQueue(cfg.redisAlertQueueKey, 100,
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

    // Candle-close alert monitor: poll cTrader trendbars for active candle alerts.
    {
        double interval = std::max(2.0, cfg.candleCheckIntervalSeconds * 4.0);
        workerLoop->runEvery(interval, [&]() {
            if (!ctrader.isReady() || !util::isForexMarketOpen()) return;
            auto active = alertManager.getActiveAlerts();
            std::set<std::pair<int64_t, int>> seen;
            for (const auto &a : active) {
                if (a.alertType != "candle_close" || !a.interval) continue;
                auto symId = registry.idForCanonical(a.pair);
                if (!symId) continue;
                int period = util::intervalToTrendbarPeriod(*a.interval);
                int ivSec = util::intervalToSeconds(*a.interval);
                if (period == 0 || ivSec == 0) continue;
                auto key = std::make_pair(*symId, period);
                if (seen.count(key)) continue;
                seen.insert(key);
                std::string canon = a.pair;
                std::string ivStr = *a.interval;
                int64_t nowMs = static_cast<int64_t>(std::time(nullptr)) * 1000;
                ctrader.getTrendbars(
                    *symId, period, 0, nowMs, 3,
                    [&, canon, ivStr, ivSec](ctrader::TrendbarsResult res) {
                        if (!res.ok || res.bars.empty()) return;
                        std::time_t now = std::time(nullptr);
                        const ctrader::TrendbarData *closed = nullptr;
                        for (auto it = res.bars.rbegin(); it != res.bars.rend(); ++it) {
                            std::time_t end = it->utcTimestampMinutes * 60 + ivSec;
                            if (end <= now) {
                                closed = &(*it);
                                break;
                            }
                        }
                        if (!closed) return;
                        Json::Value candle;
                        candle["pair"] = canon;
                        candle["interval"] = ivStr;
                        candle["timestamp"] =
                            util::toIso8601(closed->utcTimestampMinutes * 60);
                        candle["close"] = closed->close;
                        std::vector<Json::Value> candles{candle};
                        auto triggered = alertManager.checkCandleAlerts(candles);
                        for (const auto &t : triggered)
                            dispatchNotification(notifier, redisPtr, cfg.notificationDlqKey,
                                                 t);
                    });
            }
        });
    }

    // Schema init runs on the worker loop (execSqlSync needs a running event
    // loop, but must not block Drogon's HTTP loop).
    if (pgPtr) {
        workerLoop->queueInLoop([&]() {
            try {
                postgres.initSchema();
                alertManager.loadAlerts();
            } catch (const std::exception &e) {
                LOG_WARN << "Postgres schema init failed: " << e.what();
            }
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
