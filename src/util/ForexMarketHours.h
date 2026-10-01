#pragma once

#include <cstdint>
#include <ctime>
#include <optional>
#include <string>

// Forex market hours utility. Ported from app/utils/forex_market_hours.py.
// Market is open 24/5: Sunday 22:00 UTC -> Friday 22:00 UTC.
namespace ctraderplus::util {

bool isForexMarketOpen(std::time_t nowUtc = 0);

// Seconds until the market opens (0 if already open).
long long secondsUntilMarketOpens(std::time_t nowUtc = 0);

// Start of the forex day that contains ts: 22:00 UTC rollover.
// Empty when the market is closed (Friday 22:00 UTC through Sunday 22:00 UTC).
// A candle belongs to the session that was already open at its open time.
std::optional<std::time_t> forexSessionStart(std::time_t ts);

}  // namespace ctraderplus::util
