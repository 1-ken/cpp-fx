#include "util/ForexMarketHours.h"

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
    int weekday, hour;
    utcParts(nowUtc, weekday, hour);

    if (weekday == 5) return false;          // Saturday
    if (weekday == 6) return hour >= 22;     // Sunday: open from 22:00 UTC
    if (weekday == 4) return hour < 22;      // Friday: closes at 22:00 UTC
    return true;                             // Mon-Thu
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

}  // namespace ctraderplus::util
