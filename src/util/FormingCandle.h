#pragma once

#include <array>
#include <cstdint>
#include <ctime>
#include <json/json.h>
#include <optional>
#include <string>

#include "ctrader/Types.h"

namespace ctraderplus::util {

inline constexpr std::array<const char *, 7> kFormingIntervals = {
    "1m", "5m", "15m", "30m", "1h", "4h", "1d"};

// Running OHLC for the current bucket of one chart interval.
struct FormingBar {
    int64_t bucket = -1;
    double open = 0;
    double high = 0;
    double low = 0;
    double close = 0;
    bool valid = false;
};

inline void applyFormingTick(FormingBar &bar, int64_t bucket, double price) {
    if (!bar.valid || bar.bucket != bucket) {
        bar.bucket = bucket;
        bar.open = price;
        bar.high = price;
        bar.low = price;
        bar.close = price;
        bar.valid = true;
        return;
    }
    if (price > bar.high) bar.high = price;
    if (price < bar.low) bar.low = price;
    bar.close = price;
}

/** Build forming candle from spot price only (fallback when no trend bar cached). */
Json::Value buildFormingCandleFromSpot(double price, const std::string &interval);

/**
 * Forming candle JSON from the running bar, widened by an in-bucket trend bar.
 * A trend bar from an earlier bucket is ignored. Spot OHLC is the fallback when
 * `running` is missing or stale. `now == 0` uses the current time.
 */
Json::Value composeFormingCandle(const FormingBar *running,
                                  double livePrice,
                                  bool hasLivePrice,
                                  const std::string &interval,
                                  const ctrader::TrendbarData *cachedBar,
                                  std::time_t now = 0);

/**
 * Build forming candle merging optional last trend bar with live mid price.
 * Matches GET /historical/ohlc-with-forming logic.
 */
Json::Value buildFormingCandleMerged(double livePrice,
                                     const std::string &interval,
                                     const ctrader::TrendbarData *lastBar,
                                     const ctrader::TrendbarData *prevClosedBar);

}  // namespace ctraderplus::util
