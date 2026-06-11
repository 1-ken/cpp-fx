#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>
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

struct SnapshotBundle {
    std::shared_ptr<Json::Value> grouped;
    std::vector<FlatPair> flat;
};

// Central market state: ingests cTrader spot updates, periodically assembles a
// snapshot, fans it out to WebSocket subscribers, and drives alert evaluation.
class MarketHub {
  public:
    MarketHub(const core::Config &cfg, ctrader::SymbolRegistry &registry);

    void setPostgres(services::PostgresService *pg) { postgres_ = pg; }
    void setReadyFn(std::function<bool()> fn) { readyFn_ = std::move(fn); }
    void setDbExecutor(std::function<void(std::function<void()>)> exec) {
        dbExecutor_ = std::move(exec);
    }

    void setBroadcastSink(std::function<void(std::shared_ptr<Json::Value>)> sink) {
        broadcastSink_ = std::move(sink);
    }
    void setAlertSink(std::function<void(std::shared_ptr<std::vector<FlatPair>>)> sink) {
        alertSink_ = std::move(sink);
    }

    void onSpot(const ctrader::SpotUpdate &update);
    void start(trantor::EventLoop *loop);

    SnapshotBundle buildSnapshot() const;
    std::shared_ptr<Json::Value> buildGroupedSnapshot() const;

    bool latestPrice(const std::string &canonicalPair, double &out) const;

    /** Cache last in-bucket trend bar for WS forming-candle merge (key: pair:interval). */
    void cacheTrendbar(const std::string &canonicalPair,
                       const std::string &interval,
                       const ctrader::TrendbarData &bar);
    bool cachedTrendbar(const std::string &canonicalPair,
                        const std::string &interval,
                        ctrader::TrendbarData &out) const;

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
    void persistMetricIfDue(const std::string &status);

    const core::Config &cfg_;
    ctrader::SymbolRegistry &registry_;
    services::PostgresService *postgres_ = nullptr;
    std::function<bool()> readyFn_;
    std::function<void(std::function<void()>)> dbExecutor_;
    std::function<void(std::shared_ptr<Json::Value>)> broadcastSink_;
    std::function<void(std::shared_ptr<std::vector<FlatPair>>)> alertSink_;

    mutable std::mutex mu_;
    std::map<int64_t, PairState> states_;
    std::map<std::string, ctrader::TrendbarData> trendbarCache_;
    std::string lastSnapshotTs_;
    std::atomic<int> snapshotFailureCount_{0};
    std::atomic<int> activeWs_{0};
    bool marketClosedLogged_ = false;
    double lastMetricsPersistMonotonic_ = 0;
    bool filterPairs_ = false;
    std::unordered_set<std::string> allowedPairs_;
};

}  // namespace ctraderplus::market
