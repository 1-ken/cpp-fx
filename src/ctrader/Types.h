#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace ctraderplus::ctrader {

// cTrader transmits prices in 1/100000 of a price unit.
constexpr double kPriceScale = 100000.0;

struct SymbolInfo {
    int64_t id = 0;
    std::string name;       // e.g. "EUR/USD"
    bool enabled = true;
    int64_t baseAssetId = 0;
    int64_t quoteAssetId = 0;
    std::string description;
};

struct TrendbarData {
    int period = 0;                  // ProtoOATrendbarPeriod value
    int64_t utcTimestampMinutes = 0; // open tick time, in minutes since epoch
    double open = 0;
    double high = 0;
    double low = 0;
    double close = 0;
    int64_t volume = 0;
};

struct SpotUpdate {
    int64_t symbolId = 0;
    bool hasBid = false;
    double bid = 0;
    bool hasAsk = false;
    double ask = 0;
    bool hasSessionClose = false;
    double sessionClose = 0;
    int64_t timestampMs = 0;  // 0 if not provided
    std::vector<TrendbarData> liveTrendbars;
};

}  // namespace ctraderplus::ctrader
