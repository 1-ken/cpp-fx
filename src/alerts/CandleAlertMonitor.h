#pragma once

#include <cstdint>
#include <ctime>
#include <deque>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <json/json.h>

namespace ctraderplus::alerts {
class AlertManager;
struct TriggeredAlert;
}  // namespace ctraderplus::alerts

namespace ctraderplus::core {
struct Config;
}
#include "ctrader/Types.h"

namespace ctraderplus::ctrader {
class CTraderClient;
class SymbolRegistry;
}  // namespace ctraderplus::ctrader

namespace ctraderplus::alerts {

// Event-driven candle-close monitoring: live trendbar push from cTrader spot
// events, with periodic subscription sync and historical poll fallback.
class CandleAlertMonitor {
  public:
    using DispatchFn = std::function<void(const TriggeredAlert &)>;

    void configure(const core::Config *cfg, ctrader::CTraderClient *ctrader,
                   ctrader::SymbolRegistry *registry, AlertManager *alerts,
                   DispatchFn dispatch);

    void onConnectionReady(bool ready);
    void onSpot(const ctrader::SpotUpdate &update);
    void syncSubscriptions();
    void pollFallback();

  private:
    struct SubKey {
        int64_t symbolId = 0;
        int period = 0;
        bool operator==(const SubKey &o) const {
            return symbolId == o.symbolId && period == o.period;
        }
    };
    struct SubKeyHash {
        std::size_t operator()(const SubKey &k) const {
            return std::hash<int64_t>{}((k.symbolId << 8) ^ static_cast<int64_t>(k.period));
        }
    };

    struct BarTrack {
        int64_t openMinutes = 0;
        ctrader::TrendbarData bar{};
    };

    struct WarmJob {
        SubKey key;
        std::string canon;
        std::string interval;
    };

    struct BarCacheEntry {
        std::time_t fetchedAt = 0;
        std::vector<ctrader::TrendbarData> bars;
    };

    std::unordered_set<SubKey, SubKeyHash> requiredSubscriptions() const;
    void enqueueWarm(WarmJob job);
    void pumpWarm();
    void evaluateCandles(const std::vector<Json::Value> &candles);
    void dispatchTriggered(const std::vector<TriggeredAlert> &triggered);
    void processLiveTrendbar(int64_t symbolId, const ctrader::TrendbarData &tb);
    std::optional<Json::Value> candleJsonFromBar(const std::string &canon,
                                                   const std::string &interval,
                                                   const ctrader::TrendbarData &bar) const;

    const core::Config *cfg_ = nullptr;
    ctrader::CTraderClient *ctrader_ = nullptr;
    ctrader::SymbolRegistry *registry_ = nullptr;
    AlertManager *alerts_ = nullptr;
    DispatchFn dispatch_;

    mutable std::mutex mu_;
    std::unordered_set<SubKey, SubKeyHash> subscribed_;
    std::unordered_map<SubKey, BarTrack, SubKeyHash> barState_;
    std::unordered_map<SubKey, std::pair<std::string, std::string>, SubKeyHash> meta_;
    std::deque<WarmJob> warmQueue_;
    int warmInFlight_ = 0;
    std::unordered_map<SubKey, BarCacheEntry, SubKeyHash> warmCache_;
};

}  // namespace ctraderplus::alerts
