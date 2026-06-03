#include "util/TimeUtil.h"

#include <cstdio>
#include <cstring>

namespace ctraderplus::util {

std::string toIso8601(std::time_t seconds, long micros) {
    std::tm tmv{};
#if defined(_WIN32)
    gmtime_s(&tmv, &seconds);
#else
    gmtime_r(&seconds, &tmv);
#endif
    char buf[64];
    std::snprintf(buf, sizeof(buf), "%04d-%02d-%02dT%02d:%02d:%02d.%06ld+00:00",
                  tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday, tmv.tm_hour,
                  tmv.tm_min, tmv.tm_sec, micros);
    return std::string(buf);
}

std::string nowIso8601() {
    auto now = std::chrono::system_clock::now();
    auto secs = std::chrono::system_clock::to_time_t(now);
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                  now.time_since_epoch()) % std::chrono::seconds(1);
    return toIso8601(secs, static_cast<long>(us.count()));
}

std::optional<std::time_t> parseIso8601(const std::string &value) {
    if (value.empty()) return std::nullopt;
    std::string v = value;
    // Normalize trailing Z -> drop (treat as UTC).
    if (!v.empty() && (v.back() == 'Z' || v.back() == 'z')) v.pop_back();

    int year = 0, mon = 0, day = 0, hour = 0, min = 0, sec = 0;
    // Accept "YYYY-MM-DD" optionally followed by "THH:MM:SS".
    int matched = std::sscanf(v.c_str(), "%d-%d-%dT%d:%d:%d", &year, &mon, &day,
                              &hour, &min, &sec);
    if (matched < 3) {
        matched = std::sscanf(v.c_str(), "%d-%d-%d %d:%d:%d", &year, &mon, &day,
                              &hour, &min, &sec);
    }
    if (matched < 3) return std::nullopt;

    std::tm tmv{};
    tmv.tm_year = year - 1900;
    tmv.tm_mon = mon - 1;
    tmv.tm_mday = day;
    tmv.tm_hour = hour;
    tmv.tm_min = min;
    tmv.tm_sec = sec;
#if defined(_WIN32)
    std::time_t t = _mkgmtime(&tmv);
#else
    std::time_t t = timegm(&tmv);
#endif
    if (t == static_cast<std::time_t>(-1)) return std::nullopt;
    return t;
}

int intervalToSeconds(const std::string &interval) {
    if (interval == "1m") return 60;
    if (interval == "5m") return 300;
    if (interval == "15m") return 900;
    if (interval == "30m") return 1800;
    if (interval == "1h") return 3600;
    if (interval == "4h") return 14400;
    if (interval == "1d") return 86400;
    return 0;
}

int intervalToTrendbarPeriod(const std::string &interval) {
    // Values from ProtoOATrendbarPeriod enum.
    if (interval == "1m") return 1;    // M1
    if (interval == "5m") return 5;    // M5
    if (interval == "15m") return 7;   // M15
    if (interval == "30m") return 8;   // M30
    if (interval == "1h") return 9;    // H1
    if (interval == "4h") return 10;   // H4
    if (interval == "1d") return 12;   // D1
    return 0;
}

std::string trendbarPeriodToInterval(int period) {
    switch (period) {
        case 1:
            return "1m";
        case 5:
            return "5m";
        case 7:
            return "15m";
        case 8:
            return "30m";
        case 9:
            return "1h";
        case 10:
            return "4h";
        case 12:
            return "1d";
        default:
            return "";
    }
}

}  // namespace ctraderplus::util
