#include "util/FormingCandle.h"

#include <algorithm>
#include <cmath>
#include <ctime>

#include "util/TimeUtil.h"

namespace ctraderplus::util {

namespace {

Json::Value makeFormingJson(double open,
                            double high,
                            double low,
                            double close,
                            int ivSec,
                            std::time_t bucket,
                            std::time_t now) {
    std::time_t bucketEnd = static_cast<std::time_t>(bucket + ivSec);
    double timeIn = static_cast<double>(now - bucket);
    Json::Value fc;
    fc["timestamp"] = toIso8601(bucket);
    fc["open"] = open;
    fc["high"] = high;
    fc["low"] = low;
    fc["close"] = close;
    fc["volume"] = 1;
    fc["is_forming"] = true;
    fc["expected_open"] = toIso8601(bucket);
    fc["expected_close"] = toIso8601(bucketEnd);
    fc["progress_percent"] = (timeIn / static_cast<double>(ivSec)) * 100.0;
    fc["time_remaining_seconds"] = static_cast<double>(ivSec) - timeIn;
    return fc;
}

}  // namespace

Json::Value buildFormingCandleFromSpot(double price, const std::string &interval) {
    int ivSec = intervalToSeconds(interval);
    if (ivSec == 0) ivSec = 60;
    std::time_t now = std::time(nullptr);
    long long bucket = (static_cast<long long>(now) / ivSec) * ivSec;
    Json::Value fc = makeFormingJson(price, price, price, price, ivSec,
                                     static_cast<std::time_t>(bucket), now);
    fc["interval"] = interval;
    return fc;
}

Json::Value buildFormingCandleMerged(double livePrice,
                                     const std::string &interval,
                                     const ctrader::TrendbarData *lastBar,
                                     const ctrader::TrendbarData *prevClosedBar) {
    int ivSec = intervalToSeconds(interval);
    if (ivSec == 0) ivSec = 60;
    std::time_t now = std::time(nullptr);
    long long bucket = (static_cast<long long>(now) / ivSec) * ivSec;
    const long long bucketMinute = bucket / 60;

    double open = livePrice;
    double high = livePrice;
    double low = livePrice;
    double close = livePrice;

    if (lastBar) {
        open = lastBar->open;
        high = std::max({lastBar->high, livePrice});
        low = std::min({lastBar->low, livePrice});
        close = livePrice;
    } else if (prevClosedBar) {
        open = prevClosedBar->close;
        high = std::max({prevClosedBar->high, livePrice});
        low = std::min({prevClosedBar->low, livePrice});
        close = livePrice;
    }

    std::time_t formingTs = static_cast<std::time_t>(bucketMinute * 60);
    if (formingTs < bucket) formingTs = static_cast<std::time_t>(bucket);

    Json::Value fc = makeFormingJson(open, high, low, close, ivSec, formingTs, now);
    fc["interval"] = interval;
    return fc;
}

}  // namespace ctraderplus::util
