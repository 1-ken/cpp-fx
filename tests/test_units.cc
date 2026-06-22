// Minimal assertion-based unit tests for the dependency-light utilities.
#include <cassert>
#include <iostream>
#include <string>

#include "core/Config.h"
#include "ctrader/SymbolRegistry.h"
#include "market/AllowedPairs.h"
#include "services/Notifier.h"
#include "util/ForexMarketHours.h"
#include "util/PairNormalizer.h"
#include "util/FormingCandle.h"
#include "util/TimeUtil.h"
#include "ctrader/Types.h"

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
    CHECK(util::trendbarPeriodToInterval(7) == "15m");
    CHECK(util::trendbarPeriodToInterval(99).empty());
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

static void testKenyaDateTime() {
    std::string kenya = util::formatKenyaDateTime("2025-06-03T11:30:05+00:00");
    CHECK(kenya.find("3 Jun 2025") != std::string::npos);
    CHECK(kenya.find("14:30:05 EAT") != std::string::npos);
    CHECK(util::formatKenyaDateTime("").empty());
}

static void testSymbolClassification() {
    CHECK(ctrader::SymbolRegistry::classifyGroup("EURUSD") == "currencies");
    CHECK(ctrader::SymbolRegistry::classifyGroup("US30") == "indices");
    CHECK(ctrader::SymbolRegistry::classifyGroup("SpotCrude") == "commodities");
    CHECK(ctrader::SymbolRegistry::classifyGroup("XAUUSD") == "currencies");
}

static void testAllowedPairs() {
    core::Config cfg;
    cfg.subscribedPairs = {"EURUSD", "GBPUSD", "US30", "SpotCrude", "XAUUSD"};
    CHECK(market::hasExplicitPairList(cfg));
    auto allowed = market::buildAllowedCanonicalSet(cfg);
    CHECK(allowed.size() == 5);
    CHECK(allowed.count("EURUSD") == 1);
    CHECK(allowed.count("US30") == 1);
    CHECK(market::isAllowedPair(cfg, "EUR/USD"));
    CHECK(!market::isAllowedPair(cfg, "HG1"));

    core::Config emptyCfg;
    CHECK(!market::hasExplicitPairList(emptyCfg));
    CHECK(market::isAllowedPair(emptyCfg, "HG1"));
}

static void testAlertNotificationFormat() {
    std::string sms = services::Notifier::formatAlertSms(
        "EURUSD", 1.0850, 1.0851, "above", "Entry zone reached", "price", "",
        "2025-06-03T11:30:05+00:00");
    CHECK(sms.find("PAIR: EURUSD") != std::string::npos);
    CHECK(sms.find("TYPE: price") != std::string::npos);
    CHECK(sms.find("MESSAGE: Entry zone reached") != std::string::npos);
    CHECK(sms.find("14:30:05 EAT") != std::string::npos);

    std::string subject = services::Notifier::formatAlertSubject("EURUSD", "price");
    CHECK(subject == "PRICE ALERT: EURUSD");
}

static void testFormingCandleMergedWithoutLastBar() {
    Json::Value fc = util::buildFormingCandleMerged(1.2345, "1d", nullptr, nullptr);
    CHECK(fc["open"].asDouble() == 1.2345);
    CHECK(fc["high"].asDouble() == 1.2345);
    CHECK(fc["low"].asDouble() == 1.2345);
    CHECK(fc["close"].asDouble() == 1.2345);
}

static void testFormingCandleMergedWithLastBar() {
    ctrader::TrendbarData bar{};
    bar.open = 1.10;
    bar.high = 1.15;
    bar.low = 1.08;
    bar.close = 1.12;
    Json::Value fc = util::buildFormingCandleMerged(1.13, "1d", &bar, nullptr);
    CHECK(fc["open"].asDouble() == 1.10);
    CHECK(fc["high"].asDouble() == 1.15);
    CHECK(fc["low"].asDouble() == 1.08);
    CHECK(fc["close"].asDouble() == 1.13);
}

static void testFormingCandleMergedDoesNotInheritPrevClosed() {
    ctrader::TrendbarData prev{};
    prev.open = 1.10;
    prev.high = 1.20;
    prev.low = 1.05;
    prev.close = 1.12;
    Json::Value fc = util::buildFormingCandleMerged(1.11, "1d", nullptr, &prev);
    CHECK(fc["high"].asDouble() == 1.11);
    CHECK(fc["low"].asDouble() == 1.11);
}

int main() {
    testPairNormalizer();
    testIntervals();
    testMarketHours();
    testTimeRoundTrip();
    testKenyaDateTime();
    testSymbolClassification();
    testAllowedPairs();
    testAlertNotificationFormat();
    testFormingCandleMergedWithoutLastBar();
    testFormingCandleMergedWithLastBar();
    testFormingCandleMergedDoesNotInheritPrevClosed();
    if (g_failures == 0) {
        std::cout << "All unit tests passed\n";
        return 0;
    }
    std::cerr << g_failures << " test(s) failed\n";
    return 1;
}
