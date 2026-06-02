#pragma once

#include <cstdint>
#include <ctime>
#include <string>

// Forex market hours utility. Ported from app/utils/forex_market_hours.py.
// Market is open 24/5: Sunday 22:00 UTC -> Friday 22:00 UTC.
namespace ctraderplus::util {

bool isForexMarketOpen(std::time_t nowUtc = 0);

// Seconds until the market opens (0 if already open).
long long secondsUntilMarketOpens(std::time_t nowUtc = 0);

}  // namespace ctraderplus::util
