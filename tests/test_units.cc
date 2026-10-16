// Minimal assertion-based unit tests for the dependency-light utilities.
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <vector>

#include "alerts/AlertEventLedger.h"
#include "alerts/ChannelDispatch.h"
#include "alerts/AlertManager.h"
#include "core/Config.h"
#include "ctrader/SymbolRegistry.h"
#include "market/AllowedPairs.h"
#include "market/MarketHub.h"
#include "services/NotificationQueue.h"
#include "services/Notifier.h"
#include "util/ForexMarketHours.h"
#include "util/PairNormalizer.h"
#include "util/Phone.h"
#include "util/FormingCandle.h"
#include "market/StructureEngine.h"
#include "util/TimeUtil.h"
#include "ctrader/Types.h"
#include "fvg/BarAggregator.h"
#include "fvg/FVGDetector.h"
#include "fvg/FVGRegistry.h"
#include "fvg/FVGService.h"
#include "fvg/FVGWarmupCoordinator.h"
#include "market/SymbolSubscriptionPlanner.h"

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

static alerts::Alert makeSweepAlert(const std::string &id, const std::vector<std::string> &kinds) {
    alerts::Alert a;
    a.id = id;
    a.userId = "user";
    a.pair = "EURUSD";
    a.alertType = "sweep_confirm";
    a.status = "active";
    a.createdAt = "2026-06-03T20:00:00";
    a.interval = "5m";
    a.structureEvents = kinds;
    a.structureDirection = "bear";
    a.minSwingAtr = 0;
    a.breakK = 0;
    a.channels = {"sound"};
    a.normalizeChannels();
    return a;
}

static std::string barTime(int index) {
    auto start = util::parseIso8601("2026-06-03T22:00:00");
    return util::toIso8601(*start + static_cast<std::time_t>(index) * 300);
}

static Json::Value sweepBar(int index, double o, double h, double l, double c) {
    const std::string ts = barTime(index);
    return testCandle("EURUSD", "5m", ts.c_str(), o, h, l, c);
}

static std::vector<Json::Value> hourSwingBars(const char *pair) {
    const double bars[][4] = {
        {100, 101, 90, 100}, {100, 130, 100, 120}, {120, 125, 80, 115}, {115, 118, 100, 110},
    };
    auto origin = util::parseIso8601("2026-06-03T18:00:00");
    std::vector<Json::Value> out;
    for (int i = 0; i < 4; ++i) {
        const std::string ts = util::toIso8601(*origin + static_cast<std::time_t>(i) * 3600);
        out.push_back(testCandle(pair, "1h", ts.c_str(), bars[i][0], bars[i][1], bars[i][2], bars[i][3]));
    }
    return out;
}

static void seedHourSwings(alerts::AlertManager &mgr, const char *pair = "EURUSD") {
    mgr.ingestStructureHistory(pair, "1h", hourSwingBars(pair));
}

static void testSweepConfirm() {
    const double setup[][4] = {
        {100, 101, 99, 100}, {100, 110, 100, 108}, {108, 109, 100, 102},
        {102, 108, 95, 97},  {97, 100, 96, 99},    {99, 135, 98, 105},
    };
    alerts::AlertManager mgr;
    mgr.cacheAlert(makeSweepAlert("sweep-1", {"bos", "choch", "cisd"}));
    seedHourSwings(mgr);
    int fires = 0;
    mgr.setTriggerHandler([&](const alerts::TriggeredAlert &t) {
        ++fires;
        CHECK(t.alert.status == "active");
        CHECK(t.alertTypeLabel == "sweep_confirm");
    });
    for (int i = 0; i < 6; ++i) mgr.checkCandleAlerts({sweepBar(i, setup[i][0], setup[i][1], setup[i][2], setup[i][3])});
    CHECK(fires == 0);
    auto armed = mgr.getAlert("sweep-1");
    CHECK(armed && armed->pendingDir && *armed->pendingDir == "bear");
    CHECK(armed->pendingBars == 1);
    CHECK(armed->pendingSweepLevel && *armed->pendingSweepLevel == 130);
    CHECK(armed->sweptHighAt.has_value());

    mgr.checkCandleAlerts({sweepBar(6, 105, 106, 90, 92)});
    CHECK(fires == 1);
    auto done = mgr.getAlert("sweep-1");
    CHECK(done && done->status == "active");
    CHECK(!done->pendingDir);
    CHECK(done->triggeredAt.has_value());
    mgr.checkCandleAlerts({sweepBar(6, 105, 106, 90, 92)});
    CHECK(fires == 1);

    alerts::AlertManager restarted;
    int firesRestart = 0;
    restarted.setTriggerHandler([&](const alerts::TriggeredAlert &) { ++firesRestart; });
    restarted.cacheAlert(alerts::Alert::fromJson(armed->toJson()));
    seedHourSwings(restarted);
    for (int i = 0; i < 6; ++i)
        restarted.checkCandleAlerts({sweepBar(i, setup[i][0], setup[i][1], setup[i][2], setup[i][3])});
    CHECK(firesRestart == 0);
    restarted.checkCandleAlerts({sweepBar(6, 105, 106, 90, 92)});
    CHECK(firesRestart == 1);

    alerts::AlertManager wrongWay;
    wrongWay.cacheAlert(makeSweepAlert("sweep-bull", {"bos", "choch"}));
    seedHourSwings(wrongWay);
    int wrongFires = 0;
    wrongWay.setTriggerHandler([&](const alerts::TriggeredAlert &) { ++wrongFires; });
    for (int i = 0; i < 6; ++i)
        wrongWay.checkCandleAlerts({sweepBar(i, setup[i][0], setup[i][1], setup[i][2], setup[i][3])});
    wrongWay.checkCandleAlerts({sweepBar(6, 105, 115, 104, 111)});
    CHECK(wrongFires == 0);
    CHECK(!wrongWay.getAlert("sweep-bull")->pendingDir);

    alerts::AlertManager cisd;
    cisd.cacheAlert(makeSweepAlert("sweep-cisd", {"cisd"}));
    seedHourSwings(cisd);
    alerts::Alert bosOnly;
    bosOnly.id = "bos-only";
    bosOnly.userId = "user";
    bosOnly.pair = "EURUSD";
    bosOnly.alertType = "market_structure";
    bosOnly.status = "active";
    bosOnly.createdAt = "2026-06-03T20:00:00";
    bosOnly.interval = "5m";
    bosOnly.structureEvents = {"bos"};
    bosOnly.structureDirection = "bear";
    bosOnly.minSwingAtr = 0;
    bosOnly.breakK = 0;
    bosOnly.channels = {"sound"};
    bosOnly.normalizeChannels();
    cisd.cacheAlert(bosOnly);
    int cisdFires = 0, bosFires = 0;
    cisd.setTriggerHandler([&](const alerts::TriggeredAlert &t) {
        if (t.alert.id == "sweep-cisd") ++cisdFires;
        if (t.alert.id == "bos-only") ++bosFires;
    });
    for (int i = 0; i < 6; ++i)
        cisd.checkCandleAlerts({sweepBar(i, setup[i][0], setup[i][1], setup[i][2], setup[i][3])});
    cisd.checkCandleAlerts({sweepBar(6, 100, 100.5, 96, 96)});
    CHECK(cisdFires == 1);
    CHECK(bosFires == 0);

    alerts::AlertManager sameBar;
    sameBar.cacheAlert(makeSweepAlert("sweep-same", {"bos"}));
    seedHourSwings(sameBar);
    int sameFires = 0;
    sameBar.setTriggerHandler([&](const alerts::TriggeredAlert &) { ++sameFires; });
    for (int i = 0; i < 5; ++i)
        sameBar.checkCandleAlerts({sweepBar(i, setup[i][0], setup[i][1], setup[i][2], setup[i][3])});
    sameBar.checkCandleAlerts({sweepBar(5, 99, 135, 90, 90)});
    sameBar.checkCandleAlerts({sweepBar(5, 99, 135, 90, 90)});
    CHECK(sameFires == 1);

    alerts::AlertManager late;
    late.cacheAlert(makeSweepAlert("sweep-late", {"bos"}));
    seedHourSwings(late);
    int lateFires = 0;
    late.setTriggerHandler([&](const alerts::TriggeredAlert &) { ++lateFires; });
    for (int i = 0; i < 6; ++i)
        late.checkCandleAlerts({sweepBar(i, setup[i][0], setup[i][1], setup[i][2], setup[i][3])});
    for (int i = 6; i < 18; ++i)
        late.checkCandleAlerts({sweepBar(i, 105, 106, 100, 104)});
    late.checkCandleAlerts({sweepBar(18, 104, 105, 90, 92)});
    CHECK(lateFires == 0);

    alerts::AlertManager inWindow;
    inWindow.cacheAlert(makeSweepAlert("sweep-window", {"bos"}));
    seedHourSwings(inWindow);
    int windowFires = 0;
    inWindow.setTriggerHandler([&](const alerts::TriggeredAlert &) { ++windowFires; });
    for (int i = 0; i < 6; ++i)
        inWindow.checkCandleAlerts(
            {sweepBar(i, setup[i][0], setup[i][1], setup[i][2], setup[i][3])});
    for (int i = 6; i < 16; ++i)
        inWindow.checkCandleAlerts({sweepBar(i, 105, 106, 100, 104)});
    inWindow.checkCandleAlerts({sweepBar(16, 104, 105, 90, 92)});
    CHECK(windowFires == 1);

    alerts::Alert eur = makeSweepAlert("sweep-eur", {"bos"});
    alerts::Alert gbp = makeSweepAlert("sweep-gbp", {"bos"});
    gbp.pair = "GBPUSD";
    alerts::AlertManager pairs;
    pairs.cacheAlert(eur);
    pairs.cacheAlert(gbp);
    seedHourSwings(pairs);
    int pairFires = 0;
    pairs.setTriggerHandler([&](const alerts::TriggeredAlert &t) {
        ++pairFires;
        CHECK(t.alert.pair == "EURUSD");
    });
    for (int i = 0; i < 5; ++i)
        pairs.checkCandleAlerts({sweepBar(i, setup[i][0], setup[i][1], setup[i][2], setup[i][3])});
    pairs.checkCandleAlerts({sweepBar(5, 99, 135, 90, 90)});
    CHECK(pairFires == 1);
    CHECK(!pairs.getAlert("sweep-gbp")->pendingDir);

    alerts::Alert gapAlert = makeSweepAlert("sweep-gap", {"bos"});
    gapAlert.triggeredAt = barTime(6);
    alerts::AlertManager gap;
    gap.cacheAlert(gapAlert);
    seedHourSwings(gap);
    int gapFires = 0;
    gap.setTriggerHandler([&](const alerts::TriggeredAlert &) { ++gapFires; });
    for (int i = 0; i < 5; ++i)
        gap.checkCandleAlerts({sweepBar(i, setup[i][0], setup[i][1], setup[i][2], setup[i][3])});
    gap.checkCandleAlerts({sweepBar(5, 99, 135, 90, 90)});
    CHECK(gapFires == 0);
    CHECK(!gap.getAlert("sweep-gap")->pendingDir);

    alerts::AlertManager again;
    again.cacheAlert(makeSweepAlert("sweep-again", {"bos"}));
    seedHourSwings(again);
    int againFires = 0;
    again.setTriggerHandler([&](const alerts::TriggeredAlert &) { ++againFires; });
    for (int i = 0; i < 6; ++i)
        again.checkCandleAlerts({sweepBar(i, setup[i][0], setup[i][1], setup[i][2], setup[i][3])});
    auto armedAgain = again.getAlert("sweep-again");
    auto firstSweepAt = armedAgain->pendingSweepAt;
    again.checkCandleAlerts({sweepBar(6, 105, 140, 100, 110)});
    CHECK(againFires == 0);
    auto still = again.getAlert("sweep-again");
    CHECK(still && still->pendingSweepAt == firstSweepAt);
    CHECK(still->pendingBars == 2);

    alerts::AlertManager restartedSweep;
    restartedSweep.cacheAlert(alerts::Alert::fromJson(armedAgain->toJson()));
    seedHourSwings(restartedSweep);
    std::vector<Json::Value> prior;
    for (int i = 0; i < 6; ++i)
        prior.push_back(sweepBar(i, setup[i][0], setup[i][1], setup[i][2], setup[i][3]));
    restartedSweep.ingestStructureHistory("EURUSD", "5m", prior);
    restartedSweep.checkCandleAlerts({sweepBar(6, 105, 140, 100, 110)});
    auto kept = restartedSweep.getAlert("sweep-again");
    CHECK(kept && kept->pendingSweepAt == firstSweepAt);
    CHECK(kept->pendingBars == 2);
    CHECK(kept->pendingDir && *kept->pendingDir == "bear");

    alerts::Alert anyDir = makeSweepAlert("sweep-replace", {"bos"});
    anyDir.structureDirection = "any";
    alerts::AlertManager replaced;
    replaced.cacheAlert(anyDir);
    seedHourSwings(replaced);
    for (int i = 0; i < 6; ++i)
        replaced.checkCandleAlerts({sweepBar(i, setup[i][0], setup[i][1], setup[i][2], setup[i][3])});
    replaced.checkCandleAlerts({sweepBar(6, 99, 100, 70, 98)});
    auto pending = replaced.getAlert("sweep-replace");
    CHECK(pending && pending->pendingDir && *pending->pendingDir == "bull");
    CHECK(pending->pendingSweepLevel && *pending->pendingSweepLevel == 80);

    alerts::AlertManager fiveOnly;
    fiveOnly.cacheAlert(makeSweepAlert("sweep-five", {"bos"}));
    seedHourSwings(fiveOnly);
    for (int i = 0; i < 5; ++i)
        fiveOnly.checkCandleAlerts({sweepBar(i, setup[i][0], setup[i][1], setup[i][2], setup[i][3])});
    fiveOnly.checkCandleAlerts({sweepBar(5, 99, 112, 98, 105)});
    CHECK(!fiveOnly.getAlert("sweep-five")->pendingDir);

    alerts::AlertManager unconfirmed;
    unconfirmed.cacheAlert(makeSweepAlert("sweep-open-hour", {"bos"}));
    auto partialHours = hourSwingBars("EURUSD");
    partialHours.resize(2);
    unconfirmed.ingestStructureHistory("EURUSD", "1h", partialHours);
    for (int i = 0; i < 5; ++i)
        unconfirmed.checkCandleAlerts(
            {sweepBar(i, setup[i][0], setup[i][1], setup[i][2], setup[i][3])});
    unconfirmed.checkCandleAlerts({sweepBar(5, 99, 135, 98, 105)});
    CHECK(!unconfirmed.getAlert("sweep-open-hour")->pendingDir);

    alerts::AlertManager broken;
    broken.cacheAlert(makeSweepAlert("sweep-broken", {"bos"}));
    seedHourSwings(broken);
    for (int i = 0; i < 4; ++i)
        broken.checkCandleAlerts({sweepBar(i, 100, 101, 99, 100)});
    broken.checkCandleAlerts({sweepBar(4, 100, 140, 99, 135)});
    broken.checkCandleAlerts({sweepBar(5, 135, 145, 120, 125)});
    CHECK(!broken.getAlert("sweep-broken")->pendingDir);

    alerts::Alert bull = makeSweepAlert("sweep-bullish", {"bos"});
    bull.structureDirection = "bull";
    alerts::AlertManager bullish;
    bullish.cacheAlert(bull);
    seedHourSwings(bullish);
    int bullFires = 0;
    bullish.setTriggerHandler([&](const alerts::TriggeredAlert &) { ++bullFires; });
    const double bullBars[][4] = {
        {100, 101, 99, 100}, {100, 110, 100, 108}, {108, 109, 100, 102},
        {102, 103, 99, 101}, {101, 102, 70, 100},
    };
    for (int i = 0; i < 5; ++i)
        bullish.checkCandleAlerts(
            {sweepBar(i, bullBars[i][0], bullBars[i][1], bullBars[i][2], bullBars[i][3])});
    CHECK(bullish.getAlert("sweep-bullish")->pendingDir &&
          *bullish.getAlert("sweep-bullish")->pendingDir == "bull");
    bullish.checkCandleAlerts({sweepBar(5, 100, 115, 99, 112)});
    CHECK(bullFires == 1);

    alerts::AlertManager liveHours;
    liveHours.cacheAlert(makeSweepAlert("sweep-live-hour", {"bos"}));
    for (const auto &bar : hourSwingBars("EURUSD")) liveHours.checkCandleAlerts({bar});
    int liveFires = 0;
    liveHours.setTriggerHandler([&](const alerts::TriggeredAlert &) { ++liveFires; });
    for (int i = 0; i < 6; ++i)
        liveHours.checkCandleAlerts({sweepBar(i, setup[i][0], setup[i][1], setup[i][2], setup[i][3])});
    liveHours.checkCandleAlerts({sweepBar(6, 105, 106, 90, 92)});
    CHECK(liveFires == 1);
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

static void testAlertEventLedger() {
    alerts::AlertEventLedger ledger;
    alerts::TriggeredAlert first;
    first.alert.id = "sweep-1";
    first.alert.userId = "user";
    first.alert.pair = "EURUSD";
    first.alert.alertType = "sweep_confirm";
    first.alert.status = "active";
    first.alert.triggeredAt = "2026-06-03T22:35:00+00:00";
    first.currentPrice = 1.1;
    first.timeframe = "5m";
    first.alertTypeLabel = "sweep_confirm";

    alerts::TriggeredAlert second = first;
    second.alert.triggeredAt = "2026-06-03T23:10:00+00:00";
    second.currentPrice = 1.08;

    CHECK(ledger.record(first));
    CHECK(ledger.record(second));
    CHECK(ledger.size() == 2);
    CHECK(ledger.events()[0].triggeredAt != ledger.events()[1].triggeredAt);

    CHECK(!ledger.record(first));
    CHECK(ledger.size() == 2);
    CHECK(ledger.events()[0].alertId == "sweep-1");
    CHECK(ledger.events()[1].alertId == "sweep-1");
    CHECK(alerts::AlertEventLedger::keyFor(first.alert.id, *first.alert.triggeredAt) !=
          alerts::AlertEventLedger::keyFor(second.alert.id, *second.alert.triggeredAt));

    CHECK(ledger.recordDelivery(first.alert.id, *first.alert.triggeredAt, "sound", "sent"));
    CHECK(ledger.recordDelivery(first.alert.id, *first.alert.triggeredAt, "call", "failed"));
    CHECK(ledger.events()[0].delivery.at("call") == "failed");

    std::vector<alerts::ChannelReport> reports = {
        {"sound", "sent", "", false},
        {"call", "failed", "provider rejected the call", true},
    };
    auto retry = alerts::channelsToRetry(reports);
    CHECK(retry.size() == 1);
    CHECK(retry[0] == "call");

    auto failed = alerts::AlertEventLedger::failedList("select failed");
    CHECK(!failed.ok);
    CHECK(failed.events.empty());
    CHECK(!alerts::AlertEventLedger::isEmptyInbox(failed));
    auto emptyOk = alerts::AlertEventLedger::ListResult{};
    emptyOk.ok = true;
    CHECK(alerts::AlertEventLedger::isEmptyInbox(emptyOk));
}

static void testCallWindow() {
    CHECK(alerts::kCallWindowSeconds == 60.0);
    CHECK(!alerts::callWindowBlocks(false, false, 0));
    CHECK(alerts::callWindowBlocks(true, false, 0));    // being placed
    CHECK(alerts::callWindowBlocks(false, true, 10));   // inside the minute
    CHECK(alerts::callWindowBlocks(false, true, 59.9));
    CHECK(!alerts::callWindowBlocks(false, true, 60));  // a new minute places a new call
    CHECK(!alerts::callWindowBlocks(false, true, 300));

    const auto s = alerts::callSnippet("GBPUSD", "sweep_confirm", 1.27345, "watch the open");
    CHECK(s == "GBPUSD sweep confirm at 1.27345: watch the open");
    CHECK(alerts::callSnippet("USDJPY", "price", 151.234, "") == "USDJPY at 151.23");
    CHECK(alerts::callSnippet("", "", 0, "") == "alert");

    // Held alerts are released when the minute ends, or at once after a failed call.
    CHECK(alerts::callReleaseDelaySeconds(false, 0) == 0.0);
    CHECK(alerts::callReleaseDelaySeconds(true, 0) == 60.0);
    CHECK(alerts::callReleaseDelaySeconds(true, 45) == 15.0);
    CHECK(alerts::callReleaseDelaySeconds(true, 60) == 0.0);
    CHECK(alerts::callReleaseDelaySeconds(true, 90) == 0.0);

    const std::vector<std::string> three = {"A one", "B two", "C three"};
    CHECK(alerts::callDigestMessage(three) == "A one. B two. C three");
    std::vector<std::string> eight;
    for (int i = 1; i <= 8; ++i) eight.push_back("P" + std::to_string(i));
    CHECK(alerts::callDigestMessage(eight) == "P1. P2. P3. P4. P5. and 3 more, check the app");
    CHECK(alerts::callDigestMessage({}) == "");

    std::string msg = "first";
    alerts::appendCallSnippet(msg, "GBPUSD sweep confirm");
    alerts::appendCallSnippet(msg, "USDJPY session");
    alerts::appendCallSnippet(msg, "GBPUSD sweep confirm");  // not repeated
    CHECK(msg == "first. GBPUSD sweep confirm. USDJPY session");
    std::string empty;
    alerts::appendCallSnippet(empty, "EURUSD");
    CHECK(empty == "EURUSD");
}

static void testPhoneE164() {
    CHECK(util::normalizePhone(" +254 712-345-678 ") == "+254712345678");
    CHECK(util::normalizePhone("00254712345678") == "+254712345678");
    CHECK(util::isE164(util::normalizePhone("+254712345678")));
    CHECK(!util::isE164("0712345678"));
    CHECK(!util::isE164(util::normalizePhone("0712345678")));
}

static alerts::Alert makeHourSweepAlert(const std::string &id, const std::string &dir,
                                        const char *pair = "EURUSD") {
    alerts::Alert a;
    a.id = id;
    a.userId = "user";
    a.pair = pair;
    a.alertType = "hour_sweep_cisd";
    a.status = "active";
    a.createdAt = "2026-06-03T20:00:00";
    a.interval = "5m";
    a.structureEvents = {"cisd"};
    a.structureDirection = dir;
    a.channels = {"sound"};
    a.normalizeChannels();
    return a;
}

// Previous 1h candle (21:00) spans 90..110. mirror flips prices around 100.
static void seedPrevHour(alerts::AlertManager &mgr, const char *pair = "EURUSD") {
    std::vector<Json::Value> hours = {
        testCandle(pair, "1h", "2026-06-03T19:00:00", 100, 101, 99, 100),
        testCandle(pair, "1h", "2026-06-03T20:00:00", 100, 101, 99, 100),
        testCandle(pair, "1h", "2026-06-03T21:00:00", 100, 110, 90, 105),
    };
    mgr.ingestStructureHistory(pair, "1h", hours);
}

struct HourBar {
    double o, h, l, c;
};

static Json::Value hourBar(int index, HourBar b, bool mirror, const char *pair = "EURUSD") {
    if (mirror) b = {200 - b.o, 200 - b.l, 200 - b.h, 200 - b.c};
    const std::string ts = barTime(index);
    return testCandle(pair, "5m", ts.c_str(), b.o, b.h, b.l, b.c);
}

static void testHourSweepCisd() {
    // Bars 0..5 build an up run from 102 into the 110 high. Bar 5 is the sweep.
    const HourBar setup[] = {
        {100, 102, 99, 101},  {101, 104, 100, 103}, {103, 104, 101, 102},
        {102, 106, 102, 105}, {105, 108, 104, 107}, {107, 111, 106, 110},
    };
    for (int mirror = 0; mirror < 2; ++mirror) {
        const bool m = mirror == 1;
        const std::string dir = m ? "bull" : "bear";
        alerts::AlertManager mgr;
        mgr.cacheAlert(makeHourSweepAlert("hs-1", dir));
        seedPrevHour(mgr);
        int fires = 0;
        mgr.setTriggerHandler([&](const alerts::TriggeredAlert &t) {
            ++fires;
            CHECK(t.alert.status == "active");
            CHECK(t.alertTypeLabel == "hour_sweep_cisd");
        });
        for (int i = 0; i < 6; ++i) mgr.checkCandleAlerts({hourBar(i, setup[i], m)});
        CHECK(fires == 0);
        auto armed = mgr.getAlert("hs-1");
        CHECK(armed && armed->pendingDir && *armed->pendingDir == (m ? "bull" : "bear"));
        CHECK(armed->pendingSweepLevel && *armed->pendingSweepLevel == (m ? 90 : 110));
        CHECK(armed->pendingRunOpen && *armed->pendingRunOpen == (m ? 98 : 102));
        CHECK(m ? armed->sweptLowAt.has_value() : armed->sweptHighAt.has_value());

        mgr.checkCandleAlerts({hourBar(6, {110, 110.5, 104, 105}, m)});  // no CISD yet
        CHECK(fires == 0);
        mgr.checkCandleAlerts({hourBar(7, {105, 106, 100, 101}, m)});  // closes through 102
        CHECK(fires == 1);
        auto done = mgr.getAlert("hs-1");
        CHECK(done && done->status == "active" && !done->pendingDir);
        CHECK(done->triggeredAt.has_value());

        mgr.checkCandleAlerts({hourBar(7, {105, 106, 100, 101}, m)});  // same candle again
        CHECK(fires == 1);
        mgr.checkCandleAlerts({hourBar(8, {101, 112, 100, 111}, m)});  // same side, same hour
        CHECK(!mgr.getAlert("hs-1")->pendingDir);
        mgr.checkCandleAlerts({hourBar(9, {111, 111, 90, 95}, m)});
        CHECK(fires == 1);
    }

    // A touch of the level arms. A close beyond it arms. Falling short does not.
    {
        alerts::AlertManager touch;
        touch.cacheAlert(makeHourSweepAlert("hs-touch", "any"));
        seedPrevHour(touch);
        for (int i = 0; i < 5; ++i) touch.checkCandleAlerts({hourBar(i, setup[i], false)});
        touch.checkCandleAlerts({hourBar(5, {107, 110, 106, 109}, false)});
        CHECK(touch.getAlert("hs-touch")->pendingDir.has_value());

        alerts::AlertManager closeBeyond;
        closeBeyond.cacheAlert(makeHourSweepAlert("hs-close", "any"));
        seedPrevHour(closeBeyond);
        for (int i = 0; i < 5; ++i) closeBeyond.checkCandleAlerts({hourBar(i, setup[i], false)});
        closeBeyond.checkCandleAlerts({hourBar(5, {107, 113, 106, 112}, false)});
        auto cb = closeBeyond.getAlert("hs-close");
        CHECK(cb->pendingDir && *cb->pendingDir == "bear");

        alerts::AlertManager short_;
        short_.cacheAlert(makeHourSweepAlert("hs-short", "any"));
        seedPrevHour(short_);
        for (int i = 0; i < 5; ++i) short_.checkCandleAlerts({hourBar(i, setup[i], false)});
        short_.checkCandleAlerts({hourBar(5, {107, 109.9, 106, 109}, false)});
        CHECK(!short_.getAlert("hs-short")->pendingDir);
    }

    // The sweep candle itself may close through the run open.
    {
        alerts::AlertManager same;
        same.cacheAlert(makeHourSweepAlert("hs-same", "bear"));
        seedPrevHour(same);
        int fires = 0;
        same.setTriggerHandler([&](const alerts::TriggeredAlert &) { ++fires; });
        for (int i = 0; i < 5; ++i) same.checkCandleAlerts({hourBar(i, setup[i], false)});
        same.checkCandleAlerts({hourBar(5, {107, 111, 100, 101}, false)});
        CHECK(fires == 1);
    }

    // Wrong direction never arms. A CISD after the hour ends never fires.
    {
        alerts::AlertManager wrong;
        wrong.cacheAlert(makeHourSweepAlert("hs-wrong", "bull"));
        seedPrevHour(wrong);
        for (int i = 0; i < 6; ++i) wrong.checkCandleAlerts({hourBar(i, setup[i], false)});
        CHECK(!wrong.getAlert("hs-wrong")->pendingDir);

        alerts::AlertManager late;
        late.cacheAlert(makeHourSweepAlert("hs-late", "bear"));
        seedPrevHour(late);
        int fires = 0;
        late.setTriggerHandler([&](const alerts::TriggeredAlert &) { ++fires; });
        for (int i = 0; i < 6; ++i) late.checkCandleAlerts({hourBar(i, setup[i], false)});
        CHECK(late.getAlert("hs-late")->pendingDir.has_value());
        late.checkCandleAlerts({hourBar(12, {105, 106, 100, 101}, false)});  // 23:00 candle
        CHECK(fires == 0);
        CHECK(!late.getAlert("hs-late")->pendingDir);
    }

    // No previous 1h candle: nothing arms.
    {
        alerts::AlertManager gap;
        gap.cacheAlert(makeHourSweepAlert("hs-gap", "any"));
        int fires = 0;
        gap.setTriggerHandler([&](const alerts::TriggeredAlert &) { ++fires; });
        for (int i = 0; i < 6; ++i) gap.checkCandleAlerts({hourBar(i, setup[i], false)});
        CHECK(!gap.getAlert("hs-gap")->pendingDir);
        CHECK(fires == 0);
    }

    // Restart: the armed state survives, replay does not fire, the CISD still does.
    {
        alerts::AlertManager first;
        first.cacheAlert(makeHourSweepAlert("hs-r", "bear"));
        seedPrevHour(first);
        for (int i = 0; i < 6; ++i) first.checkCandleAlerts({hourBar(i, setup[i], false)});
        auto armed = first.getAlert("hs-r");
        CHECK(armed && armed->pendingDir.has_value());

        alerts::AlertManager restarted;
        int fires = 0;
        restarted.setTriggerHandler([&](const alerts::TriggeredAlert &) { ++fires; });
        restarted.cacheAlert(alerts::Alert::fromJson(armed->toJson()));
        seedPrevHour(restarted);
        std::vector<Json::Value> prior;
        for (int i = 0; i < 6; ++i) prior.push_back(hourBar(i, setup[i], false));
        restarted.ingestStructureHistory("EURUSD", "5m", prior);
        CHECK(fires == 0);
        restarted.checkCandleAlerts({hourBar(6, {110, 110.5, 104, 105}, false)});
        restarted.checkCandleAlerts({hourBar(7, {105, 106, 100, 101}, false)});
        CHECK(fires == 1);
    }

    // Two pairs stay independent.
    {
        alerts::AlertManager two;
        two.cacheAlert(makeHourSweepAlert("hs-eur", "bear", "EURUSD"));
        two.cacheAlert(makeHourSweepAlert("hs-gbp", "bear", "GBPUSD"));
        seedPrevHour(two, "EURUSD");
        seedPrevHour(two, "GBPUSD");
        std::vector<std::string> firedPairs;
        two.setTriggerHandler([&](const alerts::TriggeredAlert &t) {
            firedPairs.push_back(t.alert.pair);
        });
        for (int i = 0; i < 6; ++i) two.checkCandleAlerts({hourBar(i, setup[i], false, "EURUSD")});
        two.checkCandleAlerts({hourBar(6, {110, 110.5, 104, 105}, false, "EURUSD")});
        two.checkCandleAlerts({hourBar(7, {105, 106, 100, 101}, false, "EURUSD")});
        CHECK(firedPairs.size() == 1 && firedPairs[0] == "EURUSD");
        CHECK(!two.getAlert("hs-gbp")->pendingDir);
    }
}

static void testFvgDetection() {
    using namespace ctraderplus::fvg;
    // 1. Bullish FVG test: b1.high < b3.low
    ClosedBar b1{"EURUSD", "M15", 1000, 1.0780, 1.0800, 1.0775, 1.0790, 100};
    ClosedBar b2{"EURUSD", "M15", 1900, 1.0790, 1.0850, 1.0785, 1.0845, 250};
    ClosedBar b3{"EURUSD", "M15", 2800, 1.0845, 1.0860, 1.0820, 1.0855, 120};

    auto bullish = FVGDetector::detectFVG(b1, b2, b3, 1.5);
    CHECK(bullish.has_value());
    CHECK(bullish->is_bullish == true);
    CHECK(bullish->top == 1.0820);
    CHECK(bullish->bottom == 1.0800);
    CHECK(bullish->id == "EURUSD_M15_2800");

    // 2. Bearish FVG test: b1.low > b3.high
    ClosedBar bb1{"EURUSD", "M15", 1000, 1.0920, 1.0930, 1.0900, 1.0910, 100};
    ClosedBar bb2{"EURUSD", "M15", 1900, 1.0910, 1.0915, 1.0830, 1.0835, 300};
    ClosedBar bb3{"EURUSD", "M15", 2800, 1.0835, 1.0870, 1.0820, 1.0840, 150};

    auto bearish = FVGDetector::detectFVG(bb1, bb2, bb3, 1.5);
    CHECK(bearish.has_value());
    CHECK(bearish->is_bullish == false);
    CHECK(bearish->top == 1.0900);
    CHECK(bearish->bottom == 1.0870);
    CHECK(bearish->id == "EURUSD_M15_2800");

    // 3. Noise filter test (gap too small)
    ClosedBar n1{"EURUSD", "M15", 1000, 1.0800, 1.08000, 1.0790, 1.0795, 100};
    ClosedBar n2{"EURUSD", "M15", 1900, 1.0795, 1.08100, 1.0795, 1.0805, 100};
    ClosedBar n3{"EURUSD", "M15", 2800, 1.0805, 1.08150, 1.08005, 1.0810, 100};
    auto noise = FVGDetector::detectFVG(n1, n2, n3, 1.5);
    CHECK(!noise.has_value());
}

static void testBarAggregatorMtf() {
    using namespace ctraderplus::fvg;
    std::vector<ClosedBar> completed;
    BarAggregator agg([&](const ClosedBar &b) {
        completed.push_back(b);
    });

    // Feed 5 1-minute bars covering bucket 0 to 300
    agg.onClosed1mBar("EURUSD", 0,   1.0800, 1.0810, 1.0790, 1.0805, 10);
    agg.onClosed1mBar("EURUSD", 60,  1.0805, 1.0820, 1.0800, 1.0815, 15);
    agg.onClosed1mBar("EURUSD", 120, 1.0815, 1.0825, 1.0810, 1.0820, 20);
    agg.onClosed1mBar("EURUSD", 180, 1.0820, 1.0830, 1.0815, 1.0825, 25);
    agg.onClosed1mBar("EURUSD", 240, 1.0825, 1.0835, 1.0820, 1.0830, 30);

    // 6th bar crosses into the next M5 bucket (timestamp 300)
    agg.onClosed1mBar("EURUSD", 300, 1.0830, 1.0840, 1.0825, 1.0835, 10);

    bool foundM5 = false;
    for (const auto &b : completed) {
        if (b.timeframe == "M5" && b.bucketStartSec == 0) {
            foundM5 = true;
            CHECK(b.open == 1.0800);
            CHECK(b.high == 1.0835);
            CHECK(b.low == 1.0790);
            CHECK(b.close == 1.0830);
            CHECK(b.volume == 100);
        }
    }
    CHECK(foundM5);
}

static void testFvgMitigationAndPruning() {
    using namespace ctraderplus::fvg;
    FVGRegistry registry(FVGRegistry::Options{2, 100});

    FVGZone z1;
    z1.id = "EURUSD_M15_1000";
    z1.symbol = "EURUSD";
    z1.timeframe = "M15";
    z1.top = 1.0850;
    z1.bottom = 1.0800;
    z1.is_bullish = true;
    z1.formed_timestamp = 1000;
    CHECK(registry.registerZone(z1));

    // Subsequent bar that does not fill the gap (low = 1.0820 > bottom 1.0800)
    ClosedBar barNoFill{"EURUSD", "M15", 2000, 1.0860, 1.0870, 1.0820, 1.0850, 100};
    auto mit1 = registry.checkMitigation(barNoFill);
    CHECK(mit1.empty());
    CHECK(registry.getActiveZones("EURUSD", "M15").size() == 1);

    // Subsequent bar that fills the gap (low = 1.0790 <= bottom 1.0800)
    ClosedBar barFill{"EURUSD", "M15", 3000, 1.0830, 1.0840, 1.0790, 1.0800, 150};
    auto mit2 = registry.checkMitigation(barFill);
    CHECK(mit2.size() == 1);
    CHECK(mit2[0].id == "EURUSD_M15_1000");
    CHECK(mit2[0].mitigated == true);
    CHECK(registry.getActiveZones("EURUSD", "M15").empty());

    // Retention limit pruning (max 2)
    FVGZone zA{"A", "EURUSD", "M15", 1.08, 1.07, true, 100, false, 0};
    FVGZone zB{"B", "EURUSD", "M15", 1.08, 1.07, true, 200, false, 0};
    FVGZone zC{"C", "EURUSD", "M15", 1.08, 1.07, true, 300, false, 0};
    registry.registerZone(zA);
    registry.registerZone(zB);
    registry.registerZone(zC);
    auto active = registry.getActiveZones("EURUSD", "M15");
    CHECK(active.size() == 2);
    CHECK(active[0].id == "B");
    CHECK(active[1].id == "C");
}

static void testFvgAlerts() {
    using namespace ctraderplus::fvg;
    alerts::AlertManager mgr;
    std::vector<alerts::TriggeredAlert> fired;
    mgr.setTriggerHandler([&fired](const alerts::TriggeredAlert &t) {
        fired.push_back(t);
    });

    alerts::Alert bullAlert;
    bullAlert.id = "fvg-bull";
    bullAlert.userId = "user-1";
    bullAlert.pair = "EURUSD";
    bullAlert.alertType = "fvg";
    bullAlert.interval = "15m";
    bullAlert.structureDirection = "bull";
    bullAlert.status = "active";
    bullAlert.createdAt = "2026-10-14T00:00:00Z";
    mgr.cacheAlert(bullAlert);

    alerts::Alert anyAlert;
    anyAlert.id = "fvg-any";
    anyAlert.userId = "user-2";
    anyAlert.pair = "EURUSD";
    anyAlert.alertType = "fvg";
    anyAlert.interval = "15m";
    anyAlert.structureDirection = "any";
    anyAlert.status = "active";
    anyAlert.createdAt = "2026-10-14T00:00:00Z";
    mgr.cacheAlert(anyAlert);

    alerts::Alert h1Alert;
    h1Alert.id = "fvg-h1";
    h1Alert.userId = "user-3";
    h1Alert.pair = "EURUSD";
    h1Alert.alertType = "fvg";
    h1Alert.interval = "1h";
    h1Alert.structureDirection = "any";
    h1Alert.status = "active";
    h1Alert.createdAt = "2026-10-14T00:00:00Z";
    mgr.cacheAlert(h1Alert);

    // 1. Bearish zone on M15: bullAlert should NOT fire; anyAlert SHOULD fire; h1Alert should NOT fire.
    FVGZone bearZone{"EURUSD_M15_100", "EURUSD", "M15", 1.0900, 1.0880, false, 100, false, 0};
    mgr.checkFvgAlerts(bearZone);

    CHECK(fired.size() == 1);
    CHECK(fired[0].alert.id == "fvg-any");
    CHECK(fired[0].alertTypeLabel == "fair_value_gap");
    CHECK(std::abs(fired[0].currentPrice - 1.0890) < 1e-5);
    CHECK(fired[0].timeframe == "15m");

    auto anyUpdated = mgr.getAlert("fvg-any");
    CHECK(anyUpdated.has_value());
    CHECK(anyUpdated->status == "triggered");

    auto bullCheck = mgr.getAlert("fvg-bull");
    CHECK(bullCheck.has_value());
    CHECK(bullCheck->status == "active");

    // 2. Bullish zone on M15: bullAlert SHOULD now fire!
    FVGZone bullZone{"EURUSD_M15_200", "EURUSD", "M15", 1.0850, 1.0830, true, 200, false, 0};
    mgr.checkFvgAlerts(bullZone);

    CHECK(fired.size() == 2);
    CHECK(fired[1].alert.id == "fvg-bull");
    CHECK(fired[1].alertTypeLabel == "fair_value_gap");
    CHECK(std::abs(fired[1].currentPrice - 1.0840) < 1e-5);

    auto bullUpdated = mgr.getAlert("fvg-bull");
    CHECK(bullUpdated.has_value());
    CHECK(bullUpdated->status == "triggered");

    // 3. Bullish zone on H1: h1Alert SHOULD fire!
    FVGZone h1Zone{"EURUSD_H1_300", "EURUSD", "H1", 1.0950, 1.0910, true, 300, false, 0};
    mgr.checkFvgAlerts(h1Zone);

    CHECK(fired.size() == 3);
    CHECK(fired[2].alert.id == "fvg-h1");
    CHECK(fired[2].timeframe == "1h");
    CHECK(std::abs(fired[2].currentPrice - 1.0930) < 1e-5);
}

static void testSymbolSubscriptionPlannerUniversalFvg() {
    core::Config cfg;
    cfg.majors = {"USD", "EUR", "JPY", "GBP"};
    cfg.ctrader.subscribeAllSymbols = false;
    cfg.fvgDetectorEnabled = true;
    cfg.fvgScanAllSymbols = true;

    ctrader::SymbolRegistry reg;
    std::vector<ctrader::SymbolInfo> syms = {
        {1, "EURUSD", true, 1, 2, "EUR/USD"},
        {2, "GBPJPY", true, 3, 4, "GBP/JPY"},
        {3, "XAUUSD", true, 5, 2, "Gold"},
        {4, "BTCUSD", true, 6, 2, "Bitcoin"},
        {5, "DISABLED", false, 7, 2, "Disabled Pair"}
    };
    reg.update(syms);

    alerts::AlertManager alerts;
    market::SymbolSubscriptionPlanner planner(cfg, reg, alerts);

    // 1. With fvgScanAllSymbols = true, all enabled broker pairs are subscribed even with 0 alerts!
    auto ids = planner.computeSymbolIds();
    CHECK(ids.size() == 4);
    CHECK(std::find(ids.begin(), ids.end(), 1) != ids.end());
    CHECK(std::find(ids.begin(), ids.end(), 2) != ids.end());
    CHECK(std::find(ids.begin(), ids.end(), 3) != ids.end());
    CHECK(std::find(ids.begin(), ids.end(), 4) != ids.end());
    CHECK(std::find(ids.begin(), ids.end(), 5) == ids.end()); // Disabled symbol excluded

    // 2. When fvgScanAllSymbols = false, only major forex pairs are subscribed when 0 alerts exist
    cfg.fvgScanAllSymbols = false;
    market::SymbolSubscriptionPlanner plannerScoped(cfg, reg, alerts);
    auto scopedIds = plannerScoped.computeSymbolIds();
    CHECK(scopedIds.size() == 2); // EURUSD and GBPJPY (majors)
    CHECK(std::find(scopedIds.begin(), scopedIds.end(), 1) != scopedIds.end());
    CHECK(std::find(scopedIds.begin(), scopedIds.end(), 2) != scopedIds.end());
    CHECK(std::find(scopedIds.begin(), scopedIds.end(), 3) == scopedIds.end());
    CHECK(std::find(scopedIds.begin(), scopedIds.end(), 4) == scopedIds.end());

    // 3. When an alert is created on a non-major pair (e.g. BTCUSD), it is subscribed
    alerts::Alert btcAlert;
    btcAlert.id = "alert-btc";
    btcAlert.pair = "BTCUSD";
    btcAlert.alertType = "fvg";
    btcAlert.status = "active";
    alerts.cacheAlert(btcAlert);

    auto withAlertIds = plannerScoped.computeSymbolIds();
    CHECK(withAlertIds.size() == 3);
    CHECK(std::find(withAlertIds.begin(), withAlertIds.end(), 1) != withAlertIds.end());
    CHECK(std::find(withAlertIds.begin(), withAlertIds.end(), 2) != withAlertIds.end());
    CHECK(std::find(withAlertIds.begin(), withAlertIds.end(), 4) != withAlertIds.end());
    CHECK(std::find(withAlertIds.begin(), withAlertIds.end(), 3) == withAlertIds.end()); // XAUUSD still not subscribed
}

static void testFvgHistoricalWarmup() {
    fvg::FVGService::Config cfg;
    cfg.enabled = true;
    cfg.minGapPips = 1.0;
    cfg.minAtrMultiplier = 0.0;
    cfg.maxUnmitigatedPerTimeframe = 20;
    cfg.maxAgeBars = 100;

    fvg::FVGService service(cfg);
    std::vector<Json::Value> broadcasts;
    service.setBroadcastSink([&](const Json::Value &msg) {
        broadcasts.push_back(msg);
    });
    service.start();

    // Prepare 4 historical bars forming a bullish FVG and 1 non-mitigating bar
    // Bar 1: [1.0800 - 1.0820]
    // Bar 2: [1.0820 - 1.0860] (displacement)
    // Bar 3: [1.0840 - 1.0850] (Candle 3 Low: 1.0840 > Candle 1 High: 1.0820 -> Gap [1.0820 - 1.0840])
    // Bar 4: [1.0845 - 1.0855] (Does not touch 1.0820 - remains active)
    fvg::ClosedBar b1{"EURUSD", "M15", 900, 1.0805, 1.0820, 1.0800, 1.0815, 100, true};
    fvg::ClosedBar b2{"EURUSD", "M15", 1800, 1.0820, 1.0860, 1.0818, 1.0855, 150, true};
    fvg::ClosedBar b3{"EURUSD", "M15", 2700, 1.0845, 1.0850, 1.0840, 1.0848, 120, true};
    fvg::ClosedBar b4{"EURUSD", "M15", 3600, 1.0848, 1.0855, 1.0845, 1.0850, 110, true};

    std::vector<fvg::ClosedBar> warmupBars = {b1, b2, b3, b4};
    service.feedHistoricalBars(warmupBars);

    // Give worker thread a moment to process the queue
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Warmup bars must NOT broadcast to WebSocket!
    CHECK(broadcasts.empty());

    // But the active registry MUST have the unmitigated zone!
    auto activeZones = service.registry().getActiveZones("EURUSD", "M15");
    CHECK(activeZones.size() == 1);
    CHECK(std::abs(activeZones[0].bottom - 1.0820) < 1e-5);
    CHECK(std::abs(activeZones[0].top - 1.0840) < 1e-5);
    CHECK(activeZones[0].is_bullish == true);

    // Now feed a real-time (non-warmup) bar that mitigates the gap (Low <= 1.0820)
    fvg::ClosedBar b5{"EURUSD", "M15", 4500, 1.0845, 1.0845, 1.0810, 1.0815, 200, false};
    service.feedClosedBar(b5);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Non-warmup bar SHOULD emit MITIGATED broadcast!
    CHECK(!broadcasts.empty());
    CHECK(broadcasts.back()["type"].asString() == "FVG_ALERT");
    CHECK(broadcasts.back()["data"]["status"].asString() == "MITIGATED");

    auto remaining = service.registry().getActiveZones("EURUSD", "M15");
    CHECK(remaining.empty());

    service.stop();
}

static void testUserHasActiveFvgAlertScoping() {
    alerts::AlertManager mgr;
    CHECK(!mgr.userHasActiveFvgAlert("user-1", "EURUSD", "M15"));  // no alerts -> nothing delivered

    alerts::Alert a;
    a.id = "own-fvg";
    a.userId = "user-1";
    a.pair = "EUR/USD";
    a.alertType = "fvg";
    a.interval = "15m";
    a.status = "active";
    a.createdAt = "2026-10-14T00:00:00Z";
    mgr.cacheAlert(a);

    CHECK(mgr.userHasActiveFvgAlert("user-1", "EURUSD", "M15"));   // owner, M15 == 15m
    CHECK(mgr.userHasActiveFvgAlert("user-1", "EURUSD", "15m"));
    CHECK(!mgr.userHasActiveFvgAlert("user-2", "EURUSD", "M15"));  // other user gets nothing
    CHECK(!mgr.userHasActiveFvgAlert("user-1", "GBPUSD", "M15"));  // other pair
    CHECK(!mgr.userHasActiveFvgAlert("user-1", "EURUSD", "H1"));   // other timeframe

    alerts::Alert price;
    price.id = "own-price";
    price.userId = "user-3";
    price.pair = "EURUSD";
    price.alertType = "price";
    price.status = "active";
    price.createdAt = "2026-10-14T00:00:00Z";
    mgr.cacheAlert(price);
    CHECK(!mgr.userHasActiveFvgAlert("user-3", "EURUSD", "M15"));  // non-FVG alert does not count
}

static void testFvgTimeframeMapping() {
    using namespace ctraderplus::fvg;
    // Warmup used to pass "M15" to a lookup that only knows "15m" and silently skipped it.
    CHECK(fvgTimeframeToInterval("M15") == "15m");
    CHECK(fvgTimeframeToInterval("15m") == "15m");
    CHECK(fvgTimeframeToInterval("M30") == "30m");
    CHECK(fvgTimeframeToInterval("H1") == "1h");
    CHECK(fvgTimeframeToInterval("H4") == "4h");
    CHECK(fvgTimeframeToInterval("D1") == "1d");
    CHECK(fvgTimeframeToInterval("junk").empty());
    for (const char *tf : {"M5", "M15", "M30", "H1", "H4", "D1"}) {
        CHECK(util::intervalToTrendbarPeriod(fvgTimeframeToInterval(tf)) != 0);
    }
    // Unconfigured coordinator must refuse politely, never crash.
    FVGWarmupCoordinator warm;
    CHECK(!warm.warmupSymbolTimeframe("EURUSD", "30m"));
}

static void testBarAggregatorM30PartialSeedAndOffset() {
    using namespace ctraderplus::fvg;
    auto feed = [](BarAggregator &agg, int64_t from, int64_t to, double base) {
        for (int64_t t = from; t <= to; t += 60) {
            agg.onClosed1mBar("EURUSD", t, base, base + 0.0010, base - 0.0010, base + 0.0005, 1);
        }
    };
    auto find = [](const std::vector<ClosedBar> &v, const char *tf, int64_t bucket) {
        for (const auto &b : v) {
            if (b.timeframe == tf && b.bucketStartSec == bucket) return &b;
        }
        return static_cast<const ClosedBar *>(nullptr);
    };

    {  // M30 is rolled up (it used to be missing, so 30m FVG alerts could never fire).
        std::vector<ClosedBar> out;
        BarAggregator agg([&](const ClosedBar &b) { out.push_back(b); });
        feed(agg, 0, 1800, 1.0800);  // the minute at 1800 closes the first M30 bucket
        auto *m30 = find(out, "M30", 0);
        CHECK(m30 != nullptr);
        if (m30) CHECK(!m30->partial);
    }
    {  // Starting mid-bucket without a seed: the first bucket is flagged partial.
        std::vector<ClosedBar> out;
        BarAggregator agg([&](const ClosedBar &b) { out.push_back(b); });
        feed(agg, 420, 900, 1.0800);  // first observed minute is 7 min into the M15 bucket
        auto *m15 = find(out, "M15", 0);
        CHECK(m15 != nullptr);
        if (m15) CHECK(m15->partial);
        // The following bucket is complete and must NOT be flagged.
        feed(agg, 960, 1800, 1.0800);
        auto *next = find(out, "M15", 900);
        CHECK(next != nullptr);
        if (next) CHECK(!next->partial);
    }
    {  // Seeded from the broker's forming bar: full OHLC, not partial.
        std::vector<ClosedBar> out;
        BarAggregator agg([&](const ClosedBar &b) { out.push_back(b); });
        agg.seedForming("EURUSD", "M15", 0, 1.0790, 1.0810, 1.0780, 1.0800);
        feed(agg, 420, 900, 1.0800);
        auto *m15 = find(out, "M15", 0);
        CHECK(m15 != nullptr);
        if (m15) {
            CHECK(!m15->partial);
            CHECK(std::abs(m15->open - 1.0790) < 1e-9);  // broker open, not the first live tick
            CHECK(std::abs(m15->low - 1.0780) < 1e-9);   // broker low survives
        }
    }
    {  // Broker D1 rolls at 22:00 UTC. Learned from the seed, live buckets follow it.
        std::vector<ClosedBar> out;
        BarAggregator agg([&](const ClosedBar &b) { out.push_back(b); });
        const int64_t dayStart = 79200;  // 22:00 UTC
        agg.seedForming("EURUSD", "D1", dayStart, 1.0800, 1.0900, 1.0700, 1.0850);
        agg.onClosed1mBar("EURUSD", dayStart, 1.08, 1.09, 1.07, 1.085, 1);
        // Crossing epoch midnight (86400) must not close the broker day.
        agg.onClosed1mBar("EURUSD", 86400 + 60, 1.085, 1.086, 1.084, 1.085, 1);
        CHECK(find(out, "D1", dayStart) == nullptr);
        // The next 22:00 UTC does.
        agg.onClosed1mBar("EURUSD", dayStart + 86400, 1.085, 1.086, 1.084, 1.085, 1);
        auto *d1 = find(out, "D1", dayStart);
        CHECK(d1 != nullptr);
        if (d1) CHECK(!d1->partial);
    }
}

static void testFvgDetectorIgnoresDuplicateAndStaleBars() {
    using namespace ctraderplus::fvg;
    FVGDetector det;
    ClosedBar b1{"EURUSD", "M15", 1000, 1.0780, 1.0800, 1.0775, 1.0790, 100};
    ClosedBar b2{"EURUSD", "M15", 1900, 1.0790, 1.0850, 1.0785, 1.0845, 250};
    ClosedBar b3{"EURUSD", "M15", 2800, 1.0845, 1.0860, 1.0820, 1.0855, 120};
    CHECK(!det.onClosedBar(b1).has_value());
    CHECK(!det.onClosedBar(b2).has_value());
    auto zone = det.onClosedBar(b3);
    CHECK(zone.has_value());
    // The same closing bar again (warmup overlapping live) must not form anything.
    CHECK(!det.onClosedBar(b3).has_value());
    // A stale, older bar must not slip into the window either.
    CHECK(!det.onClosedBar(b1).has_value());
    // A genuinely newer bar is still accepted.
    ClosedBar b4{"EURUSD", "M15", 3700, 1.0855, 1.0870, 1.0850, 1.0860, 100};
    CHECK(!det.onClosedBar(b4).has_value());  // no new gap, but it was processed without error
}

static void testFvgServicePartialBarBreaksSeries() {
    fvg::FVGService::Config cfg;
    cfg.enabled = true;
    cfg.minGapPips = 1.0;
    fvg::FVGService service(cfg);
    service.start();

    fvg::ClosedBar b1{"EURUSD", "M15", 900, 1.0780, 1.0800, 1.0775, 1.0790, 100};
    fvg::ClosedBar b2{"EURUSD", "M15", 1800, 1.0790, 1.0850, 1.0785, 1.0845, 250};
    // Incomplete bucket in the middle: must not take part in detection and must break the series.
    fvg::ClosedBar p{"EURUSD", "M15", 2700, 1.0845, 1.0850, 1.0840, 1.0848, 10};
    p.partial = true;
    // Without the break, (b1, b2, b3) would form a bullish gap [1.0800 - 1.0820].
    fvg::ClosedBar b3{"EURUSD", "M15", 3600, 1.0845, 1.0860, 1.0820, 1.0855, 120};
    service.feedClosedBar(b1);
    service.feedClosedBar(b2);
    service.feedClosedBar(p);
    service.feedClosedBar(b3);
    std::this_thread::sleep_for(std::chrono::milliseconds(80));
    CHECK(service.registry().getActiveZones("EURUSD", "M15").empty());
    service.stop();
}

static void testActiveFvgWatchList() {
    alerts::AlertManager mgr;
    CHECK(mgr.activeFvgWatchList().empty());
    auto make = [&](const char *id, const char *pair, const char *type, const char *iv,
                    const char *status) {
        alerts::Alert a;
        a.id = id;
        a.userId = "u";
        a.pair = pair;
        a.alertType = type;
        if (iv) a.interval = iv;
        a.status = status;
        a.createdAt = "2026-10-14T00:00:00Z";
        mgr.cacheAlert(a);
    };
    make("f1", "EUR/USD", "fvg", "30m", "active");
    make("f2", "EURUSD", "fvg", "M30", "active");   // same watch as f1 after normalization
    make("f3", "GBPUSD", "fvg", "15m", "active");
    make("f4", "USDJPY", "fvg", "1h", "triggered");  // not active
    make("p1", "AUDUSD", "price", nullptr, "active");  // not an FVG alert
    auto watch = mgr.activeFvgWatchList();
    CHECK(watch.size() == 2);
    if (watch.size() == 2) {
        CHECK(watch[0].first == "EURUSD" && watch[0].second == "30m");
        CHECK(watch[1].first == "GBPUSD" && watch[1].second == "15m");
    }
}

static void testFvgBatchAlerts() {
    alerts::AlertManager mgr;

    // 1. Validation tests on createFvgAlerts
    bool caughtInvalidIv = false;
    try {
        mgr.createFvgAlerts({"EURUSD"}, "invalid_iv", "any", "user-1", "", {"sound"}, "", "", "2026-10-20T00:00:00Z");
    } catch (const std::invalid_argument &) {
        caughtInvalidIv = true;
    }
    CHECK(caughtInvalidIv);

    bool caughtInvalidDir = false;
    try {
        mgr.createFvgAlerts({"EURUSD"}, "15m", "up", "user-1", "", {"sound"}, "", "", "2026-10-20T00:00:00Z");
    } catch (const std::invalid_argument &) {
        caughtInvalidDir = true;
    }
    CHECK(caughtInvalidDir);

    bool caughtEmptyPairs = false;
    try {
        mgr.createFvgAlerts({}, "15m", "bull", "user-1", "", {"sound"}, "", "", "2026-10-20T00:00:00Z");
    } catch (const std::invalid_argument &) {
        caughtEmptyPairs = true;
    }
    CHECK(caughtEmptyPairs);

    // 2. Multi-pair alerts batch triggering
    alerts::Alert a1;
    a1.id = "fvg-batch-1";
    a1.userId = "user-1";
    a1.pair = "EURUSD";
    a1.alertType = "fvg";
    a1.interval = "15m";
    a1.structureDirection = "bull";
    a1.batchId = "batch-xyz";
    a1.status = "active";
    a1.createdAt = "2026-10-14T00:00:00Z";
    mgr.cacheAlert(a1);

    alerts::Alert a2;
    a2.id = "fvg-batch-2";
    a2.userId = "user-1";
    a2.pair = "GBPUSD";
    a2.alertType = "fvg";
    a2.interval = "15m";
    a2.structureDirection = "bull";
    a2.batchId = "batch-xyz";
    a2.status = "active";
    a2.createdAt = "2026-10-14T00:00:00Z";
    mgr.cacheAlert(a2);

    fvg::FVGZone z1{"EURUSD_M15_200", "EURUSD", "M15", 1.0850, 1.0830, true, 200, false, 0};
    mgr.checkFvgAlerts(z1);

    auto updated1 = mgr.getAlert("fvg-batch-1");
    auto updated2 = mgr.getAlert("fvg-batch-2");
    CHECK(updated1.has_value() && updated1->status == "triggered");
    CHECK(updated2.has_value() && updated2->status == "active");  // GBPUSD still active!
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
    testSweepConfirm();
    testHourSweepCisd();
    testStructureSessionSteps();
    testMarketStructureUnchangedBySession();
    testAlertEventLedger();
    testPhoneE164();
    testCallWindow();
    testFvgDetection();
    testBarAggregatorMtf();
    testFvgMitigationAndPruning();
    testFvgAlerts();
    testSymbolSubscriptionPlannerUniversalFvg();
    testFvgHistoricalWarmup();
    testUserHasActiveFvgAlertScoping();
    testFvgTimeframeMapping();
    testBarAggregatorM30PartialSeedAndOffset();
    testFvgDetectorIgnoresDuplicateAndStaleBars();
    testFvgServicePartialBarBreaksSeries();
    testActiveFvgWatchList();
    testFvgBatchAlerts();
    if (g_failures == 0) {
        std::cout << "All unit tests passed\n";
        return 0;
    }
    std::cerr << g_failures << " test(s) failed\n";
    return 1;
}
