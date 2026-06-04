#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>

#include <json/json.h>

namespace ctraderplus::core {
struct Config;
}
namespace ctraderplus::ctrader {
class SymbolRegistry;
class CTraderClient;
}
namespace ctraderplus::market {
class MarketHub;
class SymbolSubscriptionPlanner;
}
namespace ctraderplus::alerts {
class AlertManager;
}
namespace ctraderplus::services {
class PostgresService;
class RedisService;
class Notifier;
}

namespace ctraderplus::core {

// Process-wide service registry. Populated in main() before the HTTP server
// starts; consumed by route handlers and background tasks.
struct AppContext {
    const Config *config = nullptr;
    ctrader::SymbolRegistry *registry = nullptr;
    ctrader::CTraderClient *ctrader = nullptr;
    market::MarketHub *hub = nullptr;
    alerts::AlertManager *alerts = nullptr;
    services::PostgresService *postgres = nullptr;  // may be null (degraded)
    services::RedisService *redis = nullptr;        // may be null (degraded)
    services::Notifier *notifier = nullptr;

    // Set true after versioned schema migrations succeed on startup.
    std::atomic<bool> dbMigrationsReady{false};

    // Runs a (possibly blocking) DB task off the HTTP event loops.
    std::function<void(std::function<void()>)> dbExec;

    // Fan a grouped snapshot out to connected WebSocket clients.
    std::function<void(std::shared_ptr<Json::Value>)> wsBroadcast;

    market::SymbolSubscriptionPlanner *subscriptionPlanner = nullptr;
    std::function<void()> refreshSubscriptions;

    std::chrono::steady_clock::time_point startTime = std::chrono::steady_clock::now();

    static AppContext &instance() {
        static AppContext ctx;
        return ctx;
    }
};

}  // namespace ctraderplus::core
