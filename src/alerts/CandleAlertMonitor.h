#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <utility>

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
        bool operator<(const SubKey &o) const {
            return std::tie(symbolId, period) < std::tie(o.symbolId, o.period);
        }
    };

    struct BarTrack {
        int64_t openMinutes = 0;
        ctrader::TrendbarData bar{};
    };

    std::set<SubKey> requiredSubscriptions() const;
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
    std::set<SubKey> subscribed_;
    std::map<SubKey, BarTrack> barState_;
    std::map<SubKey, std::pair<std::string, std::string>> meta_;  // canon, interval
};

}  // namespace ctraderplus::alerts
