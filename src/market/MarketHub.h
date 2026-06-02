#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <json/json.h>
#include <trantor/net/EventLoop.h>

#include "ctrader/SymbolRegistry.h"
#include "ctrader/Types.h"

namespace ctraderplus::core {
struct Config;
}
namespace ctraderplus::services {
class RedisService;
class PostgresService;
}

namespace ctraderplus::market {

struct FlatPair {
    std::string pair;     // canonical
    std::string name;     // broker name
    std::string group;    // currencies/commodities
    double price = 0;
    bool hasPrice = false;
    double change = 0;
    bool hasChange = false;
    double bid = 0;
    double ask = 0;
};

// Central market state: ingests cTrader spot updates, periodically assembles a
// snapshot, fans it out to WebSocket subscribers, publishes to Redis, and
// drives alert evaluation. Replaces data_streaming_task in the Python app.
class MarketHub {
  public:
    MarketHub(const core::Config &cfg, ctrader::SymbolRegistry &registry);

    void setRedis(services::RedisService *redis) { redis_ = redis; }
    void setPostgres(services::PostgresService *pg) { postgres_ = pg; }
    void setReadyFn(std::function<bool()> fn) { readyFn_ = std::move(fn); }
    // Runs a (potentially blocking) DB task off the main event loop.
    void setDbExecutor(std::function<void(std::function<void()>)> exec) {
        dbExecutor_ = std::move(exec);
    }

    // Sink invoked every broadcast with the grouped snapshot (for WebSocket).
    void setBroadcastSink(std::function<void(std::shared_ptr<Json::Value>)> sink) {
        broadcastSink_ = std::move(sink);
    }
    // Invoked every broadcast with the flat pair list (for alert evaluation).
    void setAlertSink(std::function<void(std::shared_ptr<std::vector<FlatPair>>)> sink) {
        alertSink_ = std::move(sink);
    }

    // Called from the cTrader loop when a spot update arrives.
    void onSpot(const ctrader::SpotUpdate &update);

    // Start the periodic broadcast loop on the given event loop.
    void start(trantor::EventLoop *loop);

    // Build the grouped snapshot ({market_status, pairs:{currencies,commodities}, ts}).
    std::shared_ptr<Json::Value> buildGroupedSnapshot() const;

    // Latest live price (mid) for a canonical pair, if known.
    bool latestPrice(const std::string &canonicalPair, double &out) const;

    // ---- Stream health ----
    std::string lastSnapshotTs() const;
    double lastSnapshotAgeSeconds() const;
    int snapshotFailureCount() const { return snapshotFailureCount_.load(); }
    void incWs() { ++activeWs_; }
    void decWs() {
        int v = --activeWs_;
        if (v < 0) activeWs_.store(0);
    }
    int activeWsCount() const { return std::max(0, activeWs_.load()); }

  private:
    struct PairState {
        int64_t symbolId = 0;
        std::string canonical;
        std::string name;
        std::string group;
        double price = 0;
        bool hasPrice = false;
        double change = 0;
        bool hasChange = false;
        double bid = 0;
        bool hasBid = false;
        double ask = 0;
        bool hasAsk = false;
        std::string tsIso;
    };

    void tick();
    std::vector<FlatPair> snapshotFlat() const;
    void persistMetricIfDue(const std::string &status);

    const core::Config &cfg_;
    ctrader::SymbolRegistry &registry_;
    services::RedisService *redis_ = nullptr;
    services::PostgresService *postgres_ = nullptr;
    std::function<bool()> readyFn_;
    std::function<void(std::function<void()>)> dbExecutor_;
    std::function<void(std::shared_ptr<Json::Value>)> broadcastSink_;
    std::function<void(std::shared_ptr<std::vector<FlatPair>>)> alertSink_;

    mutable std::mutex mu_;
    std::map<int64_t, PairState> states_;
    std::string lastSnapshotTs_;
    std::atomic<int> snapshotFailureCount_{0};
    std::atomic<int> activeWs_{0};
    bool marketClosedLogged_ = false;
    double lastMetricsPersistMonotonic_ = 0;
};

}  // namespace ctraderplus::market
