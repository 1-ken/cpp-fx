#pragma once

#include <cstdint>
#include <string>

namespace ctraderplus::fvg {

enum class FvgDirection {
    Bullish,
    Bearish
};

enum class FvgStatus {
    Formed,
    Mitigated
};

inline const char *fvgDirectionToString(FvgDirection dir) {
    return dir == FvgDirection::Bullish ? "BULLISH" : "BEARISH";
}

inline const char *fvgStatusToString(FvgStatus status) {
    return status == FvgStatus::Formed ? "FORMED" : "MITIGATED";
}

struct ClosedBar {
    std::string symbol;
    std::string timeframe; // "M1", "M5", "M15", "H1", "H4", "D1"
    int64_t bucketStartSec = 0;
    double open = 0.0;
    double high = 0.0;
    double low = 0.0;
    double close = 0.0;
    int64_t volume = 0;
    bool isWarmup = false;
    // True when the aggregator only observed part of this bucket (process started mid-bucket
    // and the broker forming bar could not seed it). Its OHLC is incomplete, so consumers must
    // not run 3-bar detection on it; it only signals a break in the series.
    bool partial = false;
};

struct FVGZone {
    std::string id;               // ${symbol}_${timeframe}_${formed_timestamp}
    std::string symbol;
    std::string timeframe;
    double top = 0.0;
    double bottom = 0.0;
    bool is_bullish = true;
    int64_t formed_timestamp = 0; // unix seconds
    bool mitigated = false;
    int64_t mitigated_timestamp = 0;
};

}  // namespace ctraderplus::fvg
