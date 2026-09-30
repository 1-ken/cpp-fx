// Minimal assertion-based unit tests for the dependency-light utilities.
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <vector>

#include "alerts/AlertManager.h"
#include "core/Config.h"
#include "ctrader/SymbolRegistry.h"
#include "market/AllowedPairs.h"
#include "market/MarketHub.h"
#include "services/NotificationQueue.h"
#include "services/Notifier.h"
#include "util/ForexMarketHours.h"
#include "util/PairNormalizer.h"
#include "util/FormingCandle.h"
#include "market/StructureEngine.h"
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

static void testMarketStructureBosChoch() {
    using ctraderplus::market::StructureCandle;
    using ctraderplus::market::StructureOptions;
    using ctraderplus::market::computeMarketStructure;
    std::vector<StructureCandle> candles;
    auto add = [&](double o, double h, double l, double c, const char *ts) {
        StructureCandle b;
        b.open = o;
        b.high = h;
        b.low = l;
        b.close = c;
        b.timestamp = ts;
        candles.push_back(b);
    };
    add(100, 101, 99, 100, "t0");
    add(100, 105, 100, 104, "t1");
    add(104, 104.2, 102, 103, "t2");
    add(103, 103, 98, 99, "t3");
    add(99, 100, 95, 96, "t4");
    add(96, 98, 96, 97, "t5");
    add(97, 108, 97, 107, "t6");
    add(107, 107, 94, 94, "t7");
    StructureOptions opt;
    opt.breakK = 0;
    opt.minSwingAtr = 0;
    auto result = computeMarketStructure(candles, opt);
    bool bos = false, choch = false;
    for (const auto &ev : result.events) {
        if (ev.kind == "BOS") bos = true;
        if (ev.kind == "CHoCH") choch = true;
    }
    CHECK(bos);
    CHECK(choch);
}

static bool sameEvents(const std::vector<market::StructureEvent> &a,
                       const std::vector<market::StructureEvent> &b) {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); ++i) {
        if (a[i].kind != b[i].kind || a[i].dir != b[i].dir || a[i].timestamp != b[i].timestamp)
            return false;
        if (a[i].level != b[i].level) return false;
    }
    return true;
}

static void testIncrementalMatchesFull() {
    using market::IncrementalStructure;
    using market::StructureCandle;
    using market::StructureOptions;
    using market::computeMarketStructure;
    std::vector<StructureCandle> candles;
    auto add = [&](double o, double h, double l, double c, const char *ts) {
        StructureCandle b;
        b.open = o;
        b.high = h;
        b.low = l;
        b.close = c;
        b.timestamp = ts;
        candles.push_back(b);
    };
    add(100, 101, 99, 100, "t0");
    add(100, 105, 100, 104, "t1");
    add(104, 104.2, 102, 103, "t2");
    add(103, 103, 98, 99, "t3");
    add(99, 100, 95, 96, "t4");
    add(96, 98, 96, 97, "t5");
    add(97, 108, 97, 107, "t6");
    add(107, 107, 94, 94, "t7");
    StructureOptions opt;
    opt.breakK = 0;
    opt.minSwingAtr = 0;
    IncrementalStructure inc(opt);
    std::vector<market::StructureEvent> online;
    for (const auto &c : candles) {
        auto ev = inc.append(c);
        online.insert(online.end(), ev.begin(), ev.end());
    }
    auto full = computeMarketStructure(candles, opt);
    CHECK(sameEvents(online, full.events));
    CHECK(inc.trend() == full.trend);

    std::srand(42);
    for (int trial = 0; trial < 20; ++trial) {
        std::vector<StructureCandle> seq;
        double price = 100;
        for (int i = 0; i < 40; ++i) {
            double delta = (std::rand() % 200 - 100) / 50.0;
            StructureCandle b;
            b.open = price;
            b.close = price + delta;
            b.high = std::max(b.open, b.close) + (std::rand() % 50) / 100.0;
            b.low = std::min(b.open, b.close) - (std::rand() % 50) / 100.0;
            b.timestamp = "b" + std::to_string(trial) + "_" + std::to_string(i);
            price = b.close;
            seq.push_back(b);
        }
        StructureOptions ropt;
        ropt.breakK = 0.1;
        ropt.minSwingAtr = 0.2;
        IncrementalStructure rinc(ropt);
        std::vector<market::StructureEvent> rev;
        for (const auto &c : seq) {
            auto ev = rinc.append(c);
            rev.insert(rev.end(), ev.begin(), ev.end());
        }
        auto rfull = computeMarketStructure(seq, ropt);
        CHECK(sameEvents(rev, rfull.events));
    }
}

static void testAlertReplayFiresOnce() {
    alerts::AlertManager mgr;
    alerts::Alert a;
    a.id = "replay-1";
    a.userId = "user";
    a.pair = "EURUSD";
    a.alertType = "price";
    a.status = "active";
    a.condition = "above";
    a.targetPrice = 1.10;
    a.channels = {"sound"};
    a.normalizeChannels();
    mgr.cacheAlert(a);
    int fires = 0;
    std::string seenKey;
    mgr.setTriggerHandler([&](const alerts::TriggeredAlert &t) {
        ++fires;
        seenKey = services::NotificationQueue::idempotencyKey(t.alert);
    });
    market::FlatPair px;
    px.pair = "EURUSD";
    px.hasPrice = true;
    px.price = 1.25;
    mgr.checkPriceAlerts({px});
    mgr.checkPriceAlerts({px});
    CHECK(fires == 1);
    CHECK(seenKey.rfind("replay-1|", 0) == 0);
    CHECK(seenKey.size() >= std::string("|triggered").size());
    CHECK(seenKey.compare(seenKey.size() - std::string("|triggered").size(),
                          std::string("|triggered").size(), "|triggered") == 0);
    alerts::Alert again = a;
    again.status = "triggered";
    again.triggeredAt = "2026-01-01T00:00:00Z";
    const std::string key = services::NotificationQueue::idempotencyKey(again);
    CHECK(key == "replay-1|2026-01-01T00:00:00Z|triggered");
    CHECK(services::NotificationQueue::idempotencyKey(again) == key);
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
    testMarketStructureBosChoch();
    testIncrementalMatchesFull();
    testAlertReplayFiresOnce();
    if (g_failures == 0) {
        std::cout << "All unit tests passed\n";
        return 0;
    }
    std::cerr << g_failures << " test(s) failed\n";
    return 1;
}
