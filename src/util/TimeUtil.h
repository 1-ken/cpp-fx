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

// Format a UTC ISO-8601 timestamp for display in Kenya (EAT, UTC+3, no DST).
// Returns empty string when input is missing or invalid.
std::string formatKenyaDateTime(const std::string &iso8601Utc);

// Interval ("1m","5m","15m","30m","1h","4h","1d") -> seconds. 0 if invalid.
int intervalToSeconds(const std::string &interval);

// Interval string -> cTrader ProtoOATrendbarPeriod enum value (0 if invalid).
int intervalToTrendbarPeriod(const std::string &interval);

// ProtoOATrendbarPeriod enum value -> interval string (empty if unknown).
std::string trendbarPeriodToInterval(int period);

}  // namespace ctraderplus::util
