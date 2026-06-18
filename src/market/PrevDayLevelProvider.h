#pragma once

#include <ctime>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "ctrader/Types.h"

namespace ctraderplus::ctrader {
class CTraderClient;
class SymbolRegistry;
}  // namespace ctraderplus::ctrader

namespace ctraderplus::market {

// Shared draw-on-liquidity classification (mirrors the TypeScript chart engine
// and the Python backtest service).
std::string classifyDrawOutcome(double pdh, double pdl, double high, double low,
                                double close);
std::string drawForOutcome(const std::string &outcome);

// Current-day previous-day-high/low levels for a pair.
struct DayLevels {
    bool valid = false;
    double pdh = 0;       // last completed day's high
    double pdl = 0;       // last completed day's low
    std::string draw = "none";  // high | low | none (set by the last completed day)
};

// Classification of a just-closed daily candle versus the prior day.
struct CloseClassification {
    bool valid = false;
    double pdh = 0;
    double pdl = 0;
    std::string outcome;  // displaced_up | displaced_down | reversal_from_high | ...
};

// Caches recent D1 trend bars per pair and derives previous-day high/low levels
// + draw-on-liquidity classification. Lazily refreshed (UTC day rollover / TTL)
// off the cTrader event loop; reads are lock-guarded and non-blocking.
class PrevDayLevelProvider {
  public:
    void configure(ctrader::CTraderClient *ctrader, ctrader::SymbolRegistry *registry);

    // Register interest so the refresh loop keeps this pair's daily bars warm.
    void track(const std::string &canonicalPair);
    void setTrackedPairs(const std::set<std::string> &pairs);

    // Cached current levels for the forming day (invalid until first fetch).
    DayLevels currentLevels(const std::string &canonicalPair);

    // Classify a daily candle that closed at openTsSec against the prior day.
    CloseClassification classifyClose(const std::string &canonicalPair,
                                      std::time_t openTsSec, double high, double low,
                                      double close);

    // Fetch daily bars for tracked pairs that are stale or unfetched.
    void refreshDue();

  private:
    struct Entry {
        std::vector<ctrader::TrendbarData> dailyBars;  // ascending, fully closed
        std::time_t fetchedAt = 0;
        bool fetching = false;
    };

    void fetchPair(const std::string &canonicalPair);

    ctrader::CTraderClient *ctrader_ = nullptr;
    ctrader::SymbolRegistry *registry_ = nullptr;

    mutable std::mutex mu_;
    std::set<std::string> tracked_;
    std::map<std::string, Entry> cache_;
};

}  // namespace ctraderplus::market
