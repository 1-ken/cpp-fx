// Minimal assertion-based unit tests for the dependency-light utilities.
#include <cassert>
#include <iostream>
#include <string>

#include "util/ForexMarketHours.h"
#include "util/PairNormalizer.h"
#include "util/TimeUtil.h"

using namespace ctraderplus;

static int g_failures = 0;
#define CHECK(cond)                                                       \
    do {                                                                  \
        if (!(cond)) {                                                    \
            std::cerr << "FAIL: " << #cond << " @" << __LINE__ << "\n";   \
            ++g_failures;                                                 \
        }                                                                 \
    } while (0)

static void testPairNormalizer() {
    CHECK(util::canonicalPair("EUR/USD") == "EURUSD");
    CHECK(util::canonicalPair("eurusd") == "EURUSD");
    CHECK(util::canonicalPair("XAUUSD:CUR") == "XAUUSD");
    CHECK(util::canonicalPair("XAUUSDCUR") == "XAUUSD");
    CHECK(util::canonicalPair("CL1") == "CL1");
    CHECK(util::canonicalPair("HG1:COM") == "HG1");
    CHECK(util::canonicalPair("") == "");

    auto v = util::pairVariants("EUR/USD");
    bool hasCompact = false, hasSlash = false;
    for (auto &s : v) {
        if (s == "EURUSD") hasCompact = true;
        if (s == "EUR/USD") hasSlash = true;
    }
    CHECK(hasCompact && hasSlash);
}

static void testIntervals() {
    CHECK(util::intervalToSeconds("1m") == 60);
    CHECK(util::intervalToSeconds("4h") == 14400);
    CHECK(util::intervalToSeconds("1d") == 86400);
    CHECK(util::intervalToSeconds("bogus") == 0);
    CHECK(util::intervalToTrendbarPeriod("1m") == 1);
    CHECK(util::intervalToTrendbarPeriod("15m") == 7);
    CHECK(util::intervalToTrendbarPeriod("1h") == 9);
    CHECK(util::intervalToTrendbarPeriod("1d") == 12);
}

static void testMarketHours() {
    // 2026-06-06 is a Saturday -> closed.
    auto sat = util::parseIso8601("2026-06-06T12:00:00");
    CHECK(sat.has_value());
    CHECK(!util::isForexMarketOpen(*sat));
    // 2026-06-03 (Wednesday) 12:00 -> open.
    auto wed = util::parseIso8601("2026-06-03T12:00:00");
    CHECK(wed.has_value());
    CHECK(util::isForexMarketOpen(*wed));
    // Sunday 2026-06-07 21:00 -> closed; 23:00 -> open.
    auto sunEarly = util::parseIso8601("2026-06-07T21:00:00");
    auto sunLate = util::parseIso8601("2026-06-07T23:00:00");
    CHECK(sunEarly && !util::isForexMarketOpen(*sunEarly));
    CHECK(sunLate && util::isForexMarketOpen(*sunLate));
}

static void testTimeRoundTrip() {
    auto t = util::parseIso8601("2026-01-15T10:30:00");
    CHECK(t.has_value());
    std::string iso = util::toIso8601(*t);
    CHECK(iso.rfind("2026-01-15T10:30:00", 0) == 0);
}

int main() {
    testPairNormalizer();
    testIntervals();
    testMarketHours();
    testTimeRoundTrip();
    if (g_failures == 0) {
        std::cout << "All unit tests passed\n";
        return 0;
    }
    std::cerr << g_failures << " test(s) failed\n";
    return 1;
}
