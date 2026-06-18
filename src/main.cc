#include <algorithm>
#include <chrono>
#include <ctime>
#include <future>
#include <memory>
#include <set>
#include <string>
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
#include "ctrader/CTraderTokenStore.h"
#include "ctrader/SymbolRegistry.h"
#include "market/MarketHub.h"
#include "market/PrevDayLevelProvider.h"
#include "market/SymbolSubscriptionPlanner.h"
#include "services/NotificationQueue.h"
#include "services/Notifier.h"
#include "services/PostgresService.h"
#include "services/RedisService.h"
#include "util/ForexMarketHours.h"
#include "util/PairNormalizer.h"
#include "util/TimeUtil.h"

using namespace ctraderplus;

int main() {
    const auto &cfg = core::loadConfig();
    trantor::Logger::setLogLevel(trantor::Logger::kInfo);
    LOG_INFO << "Starting cTrader Plus C++ observer";

    static ctrader::SymbolRegistry registry;
    static market::MarketHub hub(cfg, registry);
    static alerts::AlertManager alertManager;
    static alerts::CandleAlertMonitor candleMonitor;
    static services::Notifier notifier(cfg);
    static services::PostgresService postgres(cfg);
    static services::RedisService redis(cfg);
    static market::SymbolSubscriptionPlanner subscriptionPlanner(cfg, registry, alertManager);
    static market::PrevDayLevelProvider prevDayLevels;
    static services::NotificationQueue notificationQueue;

    static trantor::EventLoopThread workerThread;
    workerThread.run();
    trantor::EventLoop *workerLoop = workerThread.getLoop();
    auto dbExec = [workerLoop](std::function<void()> task) {
        workerLoop->queueInLoop(std::move(task));
    };

    bool redisOk = redis.connect();
    if (!redisOk) LOG_WARN << "Redis unavailable; alert queue and OTP disabled";
    bool pgOk = postgres.connect();
    if (!pgOk) LOG_WARN << "PostgreSQL unavailable; fix DATABASE_URL in .env";

    services::RedisService *redisPtr = redisOk ? &redis : nullptr;
    services::PostgresService *pgPtr = pgOk ? &postgres : nullptr;

    core::CTraderConfig ctraderCfg = cfg.ctrader;
    if (redisOk) {
        ctrader::CTraderTokenStore::mergeFromRedis(redisPtr, cfg.redisCtraderTokenKey,
                                                   ctraderCfg);
    }
    static ctrader::CTraderClient ctrader(ctraderCfg);
    if (redisOk) {
        ctrader.setOnTokensRefreshed([&](const std::string &access,
                                         const std::string &refresh) {
            ctraderCfg.accessToken = access;
            ctraderCfg.refreshToken = refresh;
            ctrader::CTraderTokenStore::save(redisPtr, cfg.redisCtraderTokenKey,
                                             {access, refresh});
        });
    }

    subscriptionPlanner.setPostgres(pgPtr);
    prevDayLevels.configure(&ctrader, &registry);
    alertManager.configure(pgPtr, redisPtr, dbExec, cfg.redisAlertQueueKey);
    alertManager.setPrevDayLevelProvider(&prevDayLevels);

    notificationQueue.configure(cfg, &notifier, redisPtr, workerLoop);
    notificationQueue.startDlqRetryLoop();

    auto refreshSubscriptions = [&]() {
        if (cfg.ctrader.subscribeAllSymbols && cfg.subscribedPairs.empty()) return;
        if (!ctrader.isReady()) return;
        auto ids = subscriptionPlanner.computeSymbolIds();
        ctrader.refreshSpotSubscriptions(std::move(ids));
    };

    hub.setPostgres(pgPtr);
    hub.setDbExecutor(dbExec);
    hub.setReadyFn([]() { return ctrader.isReady(); });
    hub.setBroadcastSink([](std::shared_ptr<Json::Value> grouped) {
        controllers::WsObserveController::broadcastToAll(std::move(grouped));
    });
    hub.setAlertSink([&](std::shared_ptr<std::vector<market::FlatPair>> pairs) {
        alertManager.checkPriceAlerts(*pairs);
    });

    candleMonitor.configure(&cfg, &ctrader, &registry, &alertManager, {});

    alertManager.setSubscriptionChangeCallback(refreshSubscriptions);
    alertManager.setTriggerHandler([&](const alerts::TriggeredAlert &t) {
        notificationQueue.enqueue(t);
    });

    ctrader.setSymbolsCallback([&](std::vector<ctrader::SymbolInfo> symbols) {
        registry.update(symbols);
        if (!cfg.ctrader.subscribeAllSymbols || !cfg.subscribedPairs.empty())
            refreshSubscriptions();
    });
    ctrader.setStateCallback([&](bool ready) {
        candleMonitor.onConnectionReady(ready);
        if (ready &&
            (!cfg.ctrader.subscribeAllSymbols || !cfg.subscribedPairs.empty()))
            refreshSubscriptions();
    });
    ctrader.setSpotCallback([&](const ctrader::SpotUpdate &u) {
        hub.onSpot(u);
        candleMonitor.onSpot(u);
    });

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
    app.subscriptionPlanner = &subscriptionPlanner;
    app.refreshSubscriptions = refreshSubscriptions;
    app.startTime = std::chrono::steady_clock::now();

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
            workerLoop->queueInLoop([&]() {
                alertManager.loadAlerts();
                refreshSubscriptions();
            });
        } catch (const std::exception &e) {
            LOG_ERROR << "Database migrations failed: " << e.what();
            return 1;
        }
    } else {
        LOG_WARN << "PostgreSQL unavailable; auth and user features disabled";
    }

    controllers::registerRoutes();

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

    {
        double pollInterval = std::max(0.05, cfg.candleCheckIntervalSeconds);
        if (cfg.pollFallbackEnabled) {
            workerLoop->runEvery(pollInterval, [&]() { candleMonitor.pollFallback(); });
        }
        workerLoop->runEvery(5.0, [&]() {
            if (ctrader.isReady() && util::isForexMarketOpen())
                candleMonitor.syncSubscriptions();
        });
        workerLoop->runEvery(20.0, [&]() {
            if (!ctrader.isReady()) return;
            std::set<std::string> dolPairs;
            for (const auto &a : alertManager.getActiveAlerts()) {
                if (a.alertType == "prev_day_level") {
                    std::string canon = util::canonicalPair(a.pair);
                    if (!canon.empty()) dolPairs.insert(canon);
                }
            }
            prevDayLevels.setTrackedPairs(dolPairs);
            prevDayLevels.refreshDue();
        });
    }

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
