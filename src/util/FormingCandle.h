#pragma once

#include <json/json.h>
#include <optional>
#include <string>

#include "ctrader/Types.h"

namespace ctraderplus::util {

/** Build forming candle from spot price only (fallback when no trend bar cached). */
Json::Value buildFormingCandleFromSpot(double price, const std::string &interval);

/**
 * Build forming candle merging optional last trend bar with live mid price.
 * Matches GET /historical/ohlc-with-forming logic.
 */
Json::Value buildFormingCandleMerged(double livePrice,
                                     const std::string &interval,
                                     const ctrader::TrendbarData *lastBar,
                                     const ctrader::TrendbarData *prevClosedBar);

}  // namespace ctraderplus::util
