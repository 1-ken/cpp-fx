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

    auto unique = util::uniqueCanonicalPairs({"EUR/USD", "eurusd", "GBPUSD", "  ", "GBP/USD"});
    CHECK(unique.size() == 2);
    CHECK(unique[0] == "EURUSD");
    CHECK(unique[1] == "GBPUSD");
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

static void testFormingTickKeepsRangeAndResetsBucket() {
    util::FormingBar bar;
    util::applyFormingTick(bar, 1000, 1.10);
    util::applyFormingTick(bar, 1000, 1.20);
    util::applyFormingTick(bar, 1000, 1.05);
    CHECK(bar.open == 1.10);
    CHECK(bar.high == 1.20);
    CHECK(bar.low == 1.05);
    CHECK(bar.close == 1.05);
    util::applyFormingTick(bar, 2000, 1.30);
    CHECK(bar.bucket == 2000);
    CHECK(bar.open == 1.30);
    CHECK(bar.high == 1.30);
    CHECK(bar.low == 1.30);
    CHECK(bar.close == 1.30);
}

static void testComposeFormingIgnoresPreviousBucket() {
    const std::time_t now = 1700000040 + 10;
    util::FormingBar live;
    live.valid = true;
    live.bucket = 1700000040;
    live.open = 1.10;
    live.high = 1.12;
    live.low = 1.09;
    live.close = 1.11;

    ctrader::TrendbarData previous{};
    previous.utcTimestampMinutes = (1700000040 - 60) / 60;
    previous.open = 5;
    previous.high = 9;
    previous.low = 0.1;
    previous.close = 4;
    Json::Value kept = util::composeFormingCandle(&live, 1.11, true, "1m", &previous, now);
    CHECK(kept["open"].asDouble() == 1.10);
    CHECK(kept["high"].asDouble() == 1.12);
    CHECK(kept["low"].asDouble() == 1.09);
    CHECK(kept["close"].asDouble() == 1.11);

    ctrader::TrendbarData current{};
    current.utcTimestampMinutes = 1700000040 / 60;
    current.open = 1.101;
    current.high = 1.15;
    current.low = 1.07;
    current.close = 1.11;
    Json::Value widened = util::composeFormingCandle(&live, 1.11, true, "1m", &current, now);
    CHECK(widened["open"].asDouble() == 1.101);
    CHECK(widened["high"].asDouble() == 1.15);
    CHECK(widened["low"].asDouble() == 1.07);
    CHECK(widened["close"].asDouble() == 1.11);
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

static void testForexSessionStart() {
    auto at = [](const char *iso) {
        auto t = util::parseIso8601(iso);
        CHECK(t.has_value());
        return *t;
    };
    auto start = util::forexSessionStart(at("2026-06-03T12:00:00"));  // Wednesday
    CHECK(start && *start == at("2026-06-02T22:00:00"));
    auto rolled = util::forexSessionStart(at("2026-06-03T22:00:00"));
    CHECK(rolled && *rolled == at("2026-06-03T22:00:00"));
    auto before = util::forexSessionStart(at("2026-06-03T21:55:00"));
    CHECK(before && *before == at("2026-06-02T22:00:00"));
    CHECK(!util::forexSessionStart(at("2026-06-05T22:00:00")));  // Friday close
    CHECK(!util::forexSessionStart(at("2026-06-06T12:00:00")));  // Saturday
    CHECK(!util::forexSessionStart(at("2026-06-07T21:00:00")));  // Sunday before open
    auto sun = util::forexSessionStart(at("2026-06-07T22:00:00"));
    CHECK(sun && *sun == at("2026-06-07T22:00:00"));
    auto month = util::forexSessionStart(at("2026-06-01T10:00:00"));  // Monday
    CHECK(month && *month == at("2026-05-31T22:00:00"));
}

static Json::Value testCandle(const char *pair, const char *interval, const char *ts, double o,
                              double h, double l, double c) {
    Json::Value v;
    v["pair"] = pair;
    v["interval"] = interval;
    v["timestamp"] = ts;
    v["open"] = o;
    v["high"] = h;
    v["low"] = l;
    v["close"] = c;
    return v;
}

static std::vector<Json::Value> structureFixture(const char *interval, const char *breakTs) {
    const char *warm[] = {"2026-06-03T21:30:00", "2026-06-03T21:35:00", "2026-06-03T21:40:00",
                          "2026-06-03T21:45:00", "2026-06-03T21:50:00", "2026-06-03T21:55:00"};
    struct Bar {
        double o, h, l, c;
    };
    const Bar bars[] = {
        {100, 101, 99, 100}, {100, 105, 100, 104}, {104, 104.2, 102, 103},
        {103, 103, 98, 99},  {99, 100, 95, 96},    {96, 98, 96, 97},
        {97, 108, 97, 107},
    };
    std::vector<Json::Value> out;
    for (int i = 0; i < 6; ++i)
        out.push_back(testCandle("EURUSD", interval, warm[i], bars[i].o, bars[i].h, bars[i].l,
                                 bars[i].c));
    out.push_back(testCandle("EURUSD", interval, breakTs, 97, 108, 97, 107));
    return out;
}

static void feedCandles(alerts::AlertManager &mgr, const std::vector<Json::Value> &candles) {
    for (const auto &c : candles) mgr.checkCandleAlerts({c});
}

static alerts::Alert makeSessionAlert(const std::string &id,
                                      const std::vector<std::string> &intervals) {
    alerts::Alert a;
    a.id = id;
    a.userId = "user";
    a.pair = "EURUSD";
    a.alertType = "structure_session";
    a.status = "active";
    a.createdAt = "2026-06-03T20:00:00";
    a.intervals = intervals;
    a.interval = intervals.front();
    a.structureEvents = {"bos", "choch", "sweep"};
    a.structureDirection = "bull";
    a.minSwingAtr = 0;
    a.breakK = 0;
    a.sessionStepIndex = 0;
    auto sess = util::forexSessionStart(*util::parseIso8601("2026-06-03T22:00:00"));
    a.sessionStart = util::toIso8601(*sess);
    a.channels = {"sound"};
    a.normalizeChannels();
    return a;
}

static void testStructureSessionAlert() {
    alerts::AlertManager mgr;
    auto alert = makeSessionAlert("sess-1", {"5m"});
    mgr.cacheAlert(alert);
    int fires = 0;
    mgr.setTriggerHandler([&](const alerts::TriggeredAlert &) { ++fires; });

    auto bars = structureFixture("5m", "2026-06-03T22:00:00");
    feedCandles(mgr, std::vector<Json::Value>(bars.begin(), bars.end() - 1));
    CHECK(fires == 0);
    auto mid = mgr.getAlert("sess-1");
    CHECK(mid && mid->status == "active" && mid->sessionStepIndex == 0);
    CHECK(!mid->lastFiredSession);

    mgr.checkCandleAlerts({bars.back()});
    CHECK(fires == 1);
    auto fired = mgr.getAlert("sess-1");
    CHECK(fired && fired->status == "active");
    CHECK(fired->lastFiredSession.has_value());
    CHECK(fired->triggeredAt.has_value());

    mgr.checkCandleAlerts(
        {testCandle("EURUSD", "5m", "2026-06-03T22:05:00", 107, 107, 94, 94)});
    CHECK(fires == 1);

    mgr.checkCandleAlerts(
        {testCandle("EURUSD", "5m", "2026-06-04T22:05:00", 107, 107.2, 106.8, 107)});
    auto nextDay = mgr.getAlert("sess-1");
    CHECK(nextDay && nextDay->sessionStepIndex == 0);
    CHECK(fires == 1);
    auto nextStart = util::forexSessionStart(*util::parseIso8601("2026-06-04T22:05:00"));
    auto stored = util::parseIso8601(*nextDay->sessionStart);
    CHECK(nextStart && stored && *nextStart == *stored);

    alerts::AlertManager restarted;
    int fires2 = 0;
    restarted.setTriggerHandler([&](const alerts::TriggeredAlert &) { ++fires2; });
    restarted.cacheAlert(alerts::Alert::fromJson(fired->toJson()));
    feedCandles(restarted, bars);
    CHECK(fires2 == 0);
    auto kept = restarted.getAlert("sess-1");
    CHECK(kept && kept->status == "active" && kept->lastFiredSession == fired->lastFiredSession);
}

static void testSessionAlertsStayIndependentPerPair() {
    alerts::AlertManager mgr;
    auto eur = makeSessionAlert("sess-eur", {"5m"});
    auto gbp = makeSessionAlert("sess-gbp", {"5m"});
    gbp.pair = "GBPUSD";
    mgr.cacheAlert(eur);
    mgr.cacheAlert(gbp);
    std::vector<std::string> firedPairs;
    mgr.setTriggerHandler([&](const alerts::TriggeredAlert &t) {
        firedPairs.push_back(t.alert.pair);
    });
    feedCandles(mgr, structureFixture("5m", "2026-06-03T22:00:00"));
    CHECK(firedPairs.size() == 1);
    CHECK(firedPairs[0] == "EURUSD");
    auto gbpAfter = mgr.getAlert("sess-gbp");
    CHECK(gbpAfter && gbpAfter->status == "active");
    CHECK(gbpAfter->sessionStepIndex == 0);
    CHECK(!gbpAfter->lastFiredSession);
    auto eurAfter = mgr.getAlert("sess-eur");
    CHECK(eurAfter && eurAfter->lastFiredSession.has_value());
    CHECK(eurAfter->batchId == eur.batchId);
}

static void testStructureSessionSteps() {
    alerts::AlertManager mgr;
    mgr.cacheAlert(makeSessionAlert("sess-mtf", {"5m", "15m"}));
    int fires = 0;
    mgr.setTriggerHandler([&](const alerts::TriggeredAlert &) { ++fires; });

    auto higherFirst = structureFixture("15m", "2026-06-03T22:30:00");
    feedCandles(mgr, higherFirst);
    auto blocked = mgr.getAlert("sess-mtf");
    CHECK(blocked && blocked->sessionStepIndex == 0);
    CHECK(fires == 0);

    alerts::AlertManager ordered;
    ordered.cacheAlert(makeSessionAlert("sess-ord", {"5m", "15m"}));
    int orderedFires = 0;
    ordered.setTriggerHandler([&](const alerts::TriggeredAlert &) { ++orderedFires; });
    auto low = structureFixture("5m", "2026-06-03T22:00:00");
    feedCandles(ordered, std::vector<Json::Value>(low.begin(), low.end() - 1));
    const char *highWarm[] = {"2026-06-03T22:00:00", "2026-06-03T22:15:00", "2026-06-03T22:30:00",
                              "2026-06-03T22:45:00", "2026-06-03T23:00:00", "2026-06-03T23:15:00"};
    struct Bar {
        double o, h, l, c;
    };
    const Bar shape[] = {
        {100, 101, 99, 100}, {100, 105, 100, 104}, {104, 104.2, 102, 103},
        {103, 103, 98, 99},  {99, 100, 95, 96},    {96, 98, 96, 97},
    };
    for (int i = 0; i < 6; ++i) {
        ordered.checkCandleAlerts(
            {testCandle("EURUSD", "15m", highWarm[i], shape[i].o, shape[i].h, shape[i].l,
                        shape[i].c)});
    }
    CHECK(ordered.getAlert("sess-ord")->sessionStepIndex == 0);
    ordered.checkCandleAlerts({low.back()});
    auto stepped = ordered.getAlert("sess-ord");
    CHECK(stepped && stepped->sessionStepIndex == 1);
    CHECK(orderedFires == 0);
    CHECK(stepped->stepFiredAt && *stepped->stepFiredAt == "2026-06-03T22:00:00");

    ordered.checkCandleAlerts(
        {testCandle("EURUSD", "15m", "2026-06-03T23:30:00", 97, 108, 97, 107)});
    CHECK(orderedFires == 1);
    auto done = ordered.getAlert("sess-ord");
    CHECK(done && done->status == "active" && done->lastFiredSession.has_value());

    ordered.checkCandleAlerts(
        {testCandle("EURUSD", "5m", "2026-06-05T22:30:00", 100, 101, 99, 100)});
    auto weekend = ordered.getAlert("sess-ord");
    CHECK(weekend && weekend->sessionStepIndex == done->sessionStepIndex);
    CHECK(orderedFires == 1);

    alerts::AlertManager partial;
    partial.cacheAlert(makeSessionAlert("sess-partial", {"5m", "15m"}));
    feedCandles(partial, low);
    CHECK(partial.getAlert("sess-partial")->sessionStepIndex == 1);
    partial.checkCandleAlerts(
        {testCandle("EURUSD", "5m", "2026-06-04T22:05:00", 100, 101, 99, 100)});
    auto reset = partial.getAlert("sess-partial");
    CHECK(reset && reset->sessionStepIndex == 0);
    CHECK(!reset->stepFiredAt);
}

static void testMarketStructureUnchangedBySession() {
    alerts::AlertManager mgr;
    alerts::Alert a;
    a.id = "struct-old";
    a.userId = "user";
    a.pair = "EURUSD";
    a.alertType = "market_structure";
    a.status = "active";
    a.createdAt = "2026-06-03T20:00:00";
    a.interval = "5m";
    a.structureEvents = {"bos", "choch", "sweep"};
    a.structureDirection = "bull";
    a.minSwingAtr = 0;
    a.breakK = 0;
    a.channels = {"sound"};
    a.normalizeChannels();
    mgr.cacheAlert(a);
    int fires = 0;
    mgr.setTriggerHandler([&](const alerts::TriggeredAlert &t) {
        ++fires;
        CHECK(t.alertTypeLabel == "market_structure");
        CHECK(t.alert.status == "triggered");
    });
    feedCandles(mgr, structureFixture("5m", "2026-06-03T22:00:00"));
    CHECK(fires == 1);
    auto got = mgr.getAlert("struct-old");
    CHECK(got && got->status == "triggered");
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
    testForexSessionStart();
    testTimeRoundTrip();
    testKenyaDateTime();
    testSymbolClassification();
    testAllowedPairs();
    testAlertNotificationFormat();
    testFormingCandleMergedWithoutLastBar();
    testFormingCandleMergedWithLastBar();
    testFormingCandleMergedDoesNotInheritPrevClosed();
    testFormingTickKeepsRangeAndResetsBucket();
    testComposeFormingIgnoresPreviousBucket();
    testMarketStructureBosChoch();
    testIncrementalMatchesFull();
    testAlertReplayFiresOnce();
    testStructureSessionAlert();
    testSessionAlertsStayIndependentPerPair();
    testStructureSessionSteps();
    testMarketStructureUnchangedBySession();
    if (g_failures == 0) {
        std::cout << "All unit tests passed\n";
        return 0;
    }
    std::cerr << g_failures << " test(s) failed\n";
    return 1;
}
