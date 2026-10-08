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
    if (g_failures == 0) {
        std::cout << "All unit tests passed\n";
        return 0;
    }
    std::cerr << g_failures << " test(s) failed\n";
    return 1;
}
