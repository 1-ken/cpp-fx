#include "util/ForexMarketHours.h"

#include <atomic>

namespace ctraderplus::util {

namespace {
// Returns weekday with Monday=0 .. Sunday=6 (Python convention) and hour, in UTC.
void utcParts(std::time_t t, int &weekdayMon0, int &hour) {
    std::tm tmv{};
#if defined(_WIN32)
    gmtime_s(&tmv, &t);
#else
    gmtime_r(&t, &tmv);
#endif
    // tm_wday: Sunday=0 .. Saturday=6 -> convert to Monday=0 .. Sunday=6
    weekdayMon0 = (tmv.tm_wday + 6) % 7;
    hour = tmv.tm_hour;
}
}  // namespace

bool isForexMarketOpen(std::time_t nowUtc) {
    if (nowUtc == 0) nowUtc = std::time(nullptr);
    const std::time_t minute = nowUtc - (nowUtc % 60);
    static std::atomic<std::time_t> cachedMinute{-1};
    static std::atomic<int> cachedOpen{0};
    if (cachedMinute.load(std::memory_order_relaxed) == minute) {
        return cachedOpen.load(std::memory_order_relaxed) != 0;
    }
    int weekday, hour;
    utcParts(nowUtc, weekday, hour);

    bool open = true;
    if (weekday == 5) open = false;          // Saturday
    else if (weekday == 6) open = hour >= 22;  // Sunday: open from 22:00 UTC
    else if (weekday == 4) open = hour < 22;   // Friday: closes at 22:00 UTC
    cachedOpen.store(open ? 1 : 0, std::memory_order_relaxed);
    cachedMinute.store(minute, std::memory_order_relaxed);
    return open;
}

long long secondsUntilMarketOpens(std::time_t nowUtc) {
    if (nowUtc == 0) nowUtc = std::time(nullptr);
    if (isForexMarketOpen(nowUtc)) return 0;

    int weekday, hour;
    utcParts(nowUtc, weekday, hour);

    std::tm tmv{};
#if defined(_WIN32)
    gmtime_s(&tmv, &nowUtc);
#else
    gmtime_r(&nowUtc, &tmv);
#endif

    int daysAhead = 0;
    if (weekday == 5) {
        daysAhead = 1;  // Saturday -> Sunday 22:00
    } else if (weekday == 6 && hour < 22) {
        daysAhead = 0;  // Sunday before 22:00
    } else if (weekday == 4 && hour >= 22) {
        daysAhead = 2;  // Friday evening -> Sunday 22:00
    }

    std::tm target = tmv;
    target.tm_hour = 22;
    target.tm_min = 0;
    target.tm_sec = 0;
    target.tm_mday += daysAhead;
#if defined(_WIN32)
    std::time_t targetTime = _mkgmtime(&target);
#else
    std::time_t targetTime = timegm(&target);
#endif
    long long diff = static_cast<long long>(targetTime - nowUtc);
    return diff > 0 ? diff : 0;
}

std::optional<std::time_t> forexSessionStart(std::time_t ts) {
    if (ts <= 0 || !isForexMarketOpen(ts)) return std::nullopt;
    std::tm tmv{};
#if defined(_WIN32)
    gmtime_s(&tmv, &ts);
#else
    gmtime_r(&ts, &tmv);
#endif
    std::tm start = tmv;
    start.tm_hour = 22;
    start.tm_min = 0;
    start.tm_sec = 0;
    // Before today's rollover the session opened at yesterday's 22:00 UTC.
    if (tmv.tm_hour < 22) start.tm_mday -= 1;
#if defined(_WIN32)
    std::time_t startTime = _mkgmtime(&start);
#else
    std::time_t startTime = timegm(&start);
#endif
    if (startTime == static_cast<std::time_t>(-1)) return std::nullopt;
    return startTime;
}

}  // namespace ctraderplus::util
