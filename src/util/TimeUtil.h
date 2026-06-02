#pragma once

#include <chrono>
#include <cstdint>
#include <ctime>
#include <optional>
#include <string>

namespace ctraderplus::util {

// Current UTC time as ISO-8601 with microseconds + "+00:00" suffix.
std::string nowIso8601();

// Convert a UTC time_point (seconds + micros) to ISO-8601.
std::string toIso8601(std::time_t seconds, long micros = 0);

// Parse an ISO-8601 string (with or without tz / 'Z') to epoch seconds.
// Returns nullopt on failure. Fractional seconds are ignored.
std::optional<std::time_t> parseIso8601(const std::string &value);

// Interval ("1m","5m","15m","30m","1h","4h","1d") -> seconds. 0 if invalid.
int intervalToSeconds(const std::string &interval);

// Interval string -> cTrader ProtoOATrendbarPeriod enum value (0 if invalid).
int intervalToTrendbarPeriod(const std::string &interval);

}  // namespace ctraderplus::util
