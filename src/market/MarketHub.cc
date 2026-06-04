#include "market/MarketHub.h"

#include <chrono>
#include <cmath>

#include <trantor/utils/Logger.h>

#include "core/Config.h"
#include "services/PostgresService.h"
#include "util/ForexMarketHours.h"
#include "util/PairNormalizer.h"
#include "util/TimeUtil.h"

namespace ctraderplus::market {

namespace {
constexpr double kMetricsPersistIntervalSeconds = 30.0;

double monotonicSeconds() {
    return std::chrono::duration<double>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}
}  // namespace

MarketHub::MarketHub(const core::Config &cfg, ctrader::SymbolRegistry &registry)
    : cfg_(cfg), registry_(registry) {}

void MarketHub::onSpot(const ctrader::SpotUpdate &update) {
    std::string canonical = registry_.canonicalForId(update.symbolId);
    if (canonical.empty()) return;

    std::lock_guard<std::mutex> lk(mu_);
    PairState &st = states_[update.symbolId];
    st.symbolId = update.symbolId;
    st.canonical = canonical;
    if (st.name.empty()) st.name = registry_.nameForId(update.symbolId);
    if (st.group.empty()) st.group = registry_.groupForId(update.symbolId);

    if (update.hasBid) {
        st.bid = update.bid;
        st.hasBid = true;
    }
    if (update.hasAsk) {
        st.ask = update.ask;
        st.hasAsk = true;
    }
    if (st.hasBid && st.hasAsk) {
        st.price = (st.bid + st.ask) / 2.0;
        st.hasPrice = true;
    } else if (st.hasBid) {
        st.price = st.bid;
        st.hasPrice = true;
    } else if (st.hasAsk) {
        st.price = st.ask;
        st.hasPrice = true;
    }
    if (update.hasSessionClose && update.sessionClose > 0 && st.hasPrice) {
        st.change = (st.price - update.sessionClose) / update.sessionClose * 100.0;
        st.hasChange = true;
    }
    st.tsIso = util::nowIso8601();
}

void MarketHub::start(trantor::EventLoop *loop) {
    loop->runEvery(cfg_.streamIntervalSeconds, [this]() { tick(); });
    LOG_INFO << "MarketHub broadcast loop started (interval="
             << cfg_.streamIntervalSeconds << "s)";
}

SnapshotBundle MarketHub::buildSnapshot() const {
    SnapshotBundle out;
    out.grouped = std::make_shared<Json::Value>();
    (*out.grouped)["market_status"] = util::isForexMarketOpen() ? "open" : "closed";

    Json::Value currencies(Json::arrayValue);
    Json::Value commodities(Json::arrayValue);
    std::string ts = util::nowIso8601();

    std::lock_guard<std::mutex> lk(mu_);
    if (!lastSnapshotTs_.empty()) ts = lastSnapshotTs_;
    out.flat.reserve(states_.size());

    for (const auto &kv : states_) {
        const PairState &s = kv.second;
        if (!s.hasPrice) continue;

        FlatPair fp;
        fp.pair = s.canonical;
        fp.name = s.name;
        fp.group = s.group;
        fp.price = s.price;
        fp.hasPrice = true;
        fp.change = s.change;
        fp.hasChange = s.hasChange;
        fp.bid = s.bid;
        fp.ask = s.ask;
        out.flat.push_back(std::move(fp));

        Json::Value item(Json::objectValue);
        item["pair"] = s.canonical;
        item["price"] = s.price;
        if (s.hasChange) item["change"] = s.change;
        else item["change"] = Json::Value::null;
        item["bid"] = s.hasBid ? Json::Value(s.bid) : Json::Value::null;
        item["ask"] = s.hasAsk ? Json::Value(s.ask) : Json::Value::null;
        item["common_name"] = s.name;
        item["source"] = s.group;
        if (s.group == "currencies")
            currencies.append(item);
        else
            commodities.append(item);
    }

    Json::Value pairs(Json::objectValue);
    pairs["currencies"] = currencies;
    pairs["commodities"] = commodities;
    (*out.grouped)["pairs"] = pairs;
    (*out.grouped)["ts"] = ts;
    return out;
}

std::shared_ptr<Json::Value> MarketHub::buildGroupedSnapshot() const {
    return buildSnapshot().grouped;
}

bool MarketHub::latestPrice(const std::string &canonicalPair, double &out) const {
    std::string canon = util::canonicalPair(canonicalPair);
    std::lock_guard<std::mutex> lk(mu_);
    for (const auto &kv : states_) {
        if (kv.second.canonical == canon && kv.second.hasPrice) {
            out = kv.second.price;
            return true;
        }
    }
    return false;
}

std::string MarketHub::lastSnapshotTs() const {
    std::lock_guard<std::mutex> lk(mu_);
    return lastSnapshotTs_;
}

double MarketHub::lastSnapshotAgeSeconds() const {
    std::string ts;
    {
        std::lock_guard<std::mutex> lk(mu_);
        ts = lastSnapshotTs_;
    }
    if (ts.empty()) return -1;
    auto parsed = util::parseIso8601(ts);
    if (!parsed) return -1;
    double age = static_cast<double>(std::time(nullptr) - *parsed);
    return age < 0 ? 0 : age;
}

void MarketHub::persistMetricIfDue(const std::string &status) {
    if (!postgres_ || !dbExecutor_) return;
    double now = monotonicSeconds();
    if (now - lastMetricsPersistMonotonic_ < kMetricsPersistIntervalSeconds) return;
    lastMetricsPersistMonotonic_ = now;
    int ws = activeWsCount();
    int failures = snapshotFailureCount_.load();
    std::time_t obs = std::time(nullptr);
    dbExecutor_([this, obs, ws, failures, status]() {
        postgres_->insertStreamMetric(obs, ws, 0, failures, status);
    });
}

void MarketHub::tick() {
    if (!util::isForexMarketOpen()) {
        if (!marketClosedLogged_) {
            LOG_INFO << "Forex market CLOSED. Data streaming paused.";
            marketClosedLogged_ = true;
        }
        persistMetricIfDue("market_closed");
        return;
    }
    if (marketClosedLogged_) {
        LOG_INFO << "Forex market OPEN. Resuming data streaming.";
        marketClosedLogged_ = false;
    }

    bool ready = readyFn_ ? readyFn_() : true;
    if (!ready) {
        int f = ++snapshotFailureCount_;
        if (f > cfg_.maxSnapshotFailures) snapshotFailureCount_.store(cfg_.maxSnapshotFailures);
        persistMetricIfDue("degraded");
        return;
    }

    auto bundle = buildSnapshot();
    std::string ts = util::nowIso8601();
    {
        std::lock_guard<std::mutex> lk(mu_);
        lastSnapshotTs_ = ts;
    }
    (*bundle.grouped)["ts"] = ts;
    snapshotFailureCount_.store(0);

    if (broadcastSink_) broadcastSink_(bundle.grouped);
    if (alertSink_) {
        alertSink_(std::make_shared<std::vector<FlatPair>>(std::move(bundle.flat)));
    }

    persistMetricIfDue("healthy");
}

}  // namespace ctraderplus::market
