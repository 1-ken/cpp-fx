#include "alerts/CandleAlertMonitor.h"

#include <algorithm>
#include <ctime>
#include <vector>

#include <trantor/utils/Logger.h>

#include "alerts/AlertManager.h"
#include "core/Config.h"
#include "ctrader/CTraderClient.h"
#include "market/AllowedPairs.h"
#include "ctrader/SymbolRegistry.h"
#include "ctrader/Types.h"
#include "util/ForexMarketHours.h"
#include "util/PairNormalizer.h"
#include "util/TimeUtil.h"

namespace ctraderplus::alerts {

namespace {

bool isBarFullyClosed(const ctrader::TrendbarData &bar, int ivSec) {
    if (ivSec <= 0) return false;
    std::time_t now = std::time(nullptr);
    std::time_t end = bar.utcTimestampMinutes * 60 + ivSec;
    return end <= now;
}

const ctrader::TrendbarData *pickLatestClosedBar(
    const std::vector<ctrader::TrendbarData> &bars, int ivSec) {
    if (ivSec <= 0 || bars.empty()) return nullptr;
    std::time_t now = std::time(nullptr);
    for (auto it = bars.rbegin(); it != bars.rend(); ++it) {
        std::time_t end = it->utcTimestampMinutes * 60 + ivSec;
        if (end <= now) return &(*it);
    }
    return nullptr;
}

}  // namespace

void CandleAlertMonitor::configure(const core::Config *cfg, ctrader::CTraderClient *ctrader,
                                   ctrader::SymbolRegistry *registry, AlertManager *alerts,
                                   DispatchFn dispatch) {
    cfg_ = cfg;
    ctrader_ = ctrader;
    registry_ = registry;
    alerts_ = alerts;
    dispatch_ = std::move(dispatch);
}

void CandleAlertMonitor::onConnectionReady(bool ready) {
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (!ready) {
            subscribed_.clear();
            barState_.clear();
            meta_.clear();
            return;
        }
    }
    syncSubscriptions();
}

std::set<CandleAlertMonitor::SubKey> CandleAlertMonitor::requiredSubscriptions() const {
    std::set<SubKey> needed;
    if (!alerts_) return needed;
    for (const auto &a : alerts_->getActiveAlerts()) {
        if (!registry_) continue;
        std::string interval;
        if (a.alertType == "candle_close" && a.interval) {
            interval = *a.interval;
        } else if (a.alertType == "prev_day_level") {
            const std::string trig = a.dolTrigger.value_or("sweep");
            if (trig != "displacement" && trig != "reversal") continue;
            interval = "1d";  // displacement/reversal confirm on the daily close
        } else {
            continue;
        }
        if (cfg_ && market::hasExplicitPairList(*cfg_) &&
            !market::isAllowedPair(*cfg_, a.pair))
            continue;
        auto symId = registry_->resolveId(a.pair);
        if (!symId) continue;
        int period = util::intervalToTrendbarPeriod(interval);
        if (period == 0) continue;
        needed.insert(SubKey{*symId, period});
    }
    return needed;
}

void CandleAlertMonitor::syncSubscriptions() {
    if (!ctrader_ || !ctrader_->isReady() || !util::isForexMarketOpen()) return;

    auto needed = requiredSubscriptions();
    std::set<SubKey> toAdd;
    std::set<SubKey> toRemove;
    {
        std::lock_guard<std::mutex> lk(mu_);
        for (const auto &k : needed) {
            if (!subscribed_.count(k)) toAdd.insert(k);
        }
        for (const auto &k : subscribed_) {
            if (!needed.count(k)) toRemove.insert(k);
        }
    }

    for (const auto &k : toRemove) {
        ctrader_->unsubscribeLiveTrendbar(k.symbolId, k.period);
        std::lock_guard<std::mutex> lk(mu_);
        subscribed_.erase(k);
        barState_.erase(k);
        meta_.erase(k);
    }
    for (const auto &k : toAdd) {
        std::string iv = util::trendbarPeriodToInterval(k.period);
        std::string canon = registry_ ? registry_->canonicalForId(k.symbolId) : "";
        if (iv.empty() || canon.empty()) continue;
        ctrader_->subscribeLiveTrendbar(k.symbolId, k.period);
        std::lock_guard<std::mutex> lk(mu_);
        subscribed_.insert(k);
        meta_[k] = {canon, iv};
        barState_.erase(k);
    }
}

std::optional<Json::Value> CandleAlertMonitor::candleJsonFromBar(
    const std::string &canon, const std::string &interval,
    const ctrader::TrendbarData &bar) const {
    int ivSec = util::intervalToSeconds(interval);
    if (!isBarFullyClosed(bar, ivSec)) return std::nullopt;
    Json::Value candle;
    candle["pair"] = canon;
    candle["interval"] = interval;
    candle["timestamp"] = util::toIso8601(bar.utcTimestampMinutes * 60);
    candle["open"] = bar.open;
    candle["high"] = bar.high;
    candle["low"] = bar.low;
    candle["close"] = bar.close;
    return candle;
}

void CandleAlertMonitor::evaluateCandles(const std::vector<Json::Value> &candles) {
    if (candles.empty() || !alerts_) return;
    auto triggered = alerts_->checkCandleAlerts(candles);
    dispatchTriggered(triggered);
}

void CandleAlertMonitor::dispatchTriggered(const std::vector<TriggeredAlert> &triggered) {
    if (!dispatch_) return;
    for (const auto &t : triggered) dispatch_(t);
}

void CandleAlertMonitor::processLiveTrendbar(int64_t symbolId,
                                             const ctrader::TrendbarData &tb) {
    SubKey key{symbolId, tb.period};
    std::string canon;
    std::string interval;
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (!subscribed_.count(key)) return;
        auto it = meta_.find(key);
        if (it != meta_.end()) {
            canon = it->second.first;
            interval = it->second.second;
        } else {
            canon = registry_ ? registry_->canonicalForId(symbolId) : "";
            interval = util::trendbarPeriodToInterval(tb.period);
            if (!canon.empty() && !interval.empty()) meta_[key] = {canon, interval};
        }
    }
    if (canon.empty() || interval.empty()) return;

    std::vector<Json::Value> toEval;
    {
        std::lock_guard<std::mutex> lk(mu_);
        BarTrack &track = barState_[key];
        if (track.openMinutes != 0 && tb.utcTimestampMinutes != track.openMinutes) {
            if (auto candle = candleJsonFromBar(canon, interval, track.bar))
                toEval.push_back(*candle);
            track.openMinutes = tb.utcTimestampMinutes;
            track.bar = tb;
        } else if (track.openMinutes == 0) {
            track.openMinutes = tb.utcTimestampMinutes;
            track.bar = tb;
        } else {
            track.bar = tb;
            if (isBarFullyClosed(track.bar, util::intervalToSeconds(interval))) {
                if (auto candle = candleJsonFromBar(canon, interval, track.bar))
                    toEval.push_back(*candle);
            }
        }
    }
    evaluateCandles(toEval);
}

void CandleAlertMonitor::onSpot(const ctrader::SpotUpdate &update) {
    if (!util::isForexMarketOpen() || update.liveTrendbars.empty()) return;
    for (const auto &tb : update.liveTrendbars) processLiveTrendbar(update.symbolId, tb);
}

void CandleAlertMonitor::pollFallback() {
    if (!cfg_ || !cfg_->pollFallbackEnabled) return;
    if (!ctrader_ || !ctrader_->isReady() || !util::isForexMarketOpen() || !alerts_) return;

    auto active = alerts_->getActiveAlerts();
    std::set<SubKey> seen;
    for (const auto &a : active) {
        std::string interval;
        if (a.alertType == "candle_close" && a.interval) {
            interval = *a.interval;
        } else if (a.alertType == "prev_day_level") {
            const std::string trig = a.dolTrigger.value_or("sweep");
            if (trig != "displacement" && trig != "reversal") continue;
            interval = "1d";
        } else {
            continue;
        }
        auto symId = registry_->idForCanonical(a.pair);
        if (!symId) continue;
        int period = util::intervalToTrendbarPeriod(interval);
        int ivSec = util::intervalToSeconds(interval);
        if (period == 0 || ivSec == 0) continue;
        SubKey key{*symId, period};
        if (seen.count(key)) continue;
        seen.insert(key);

        std::string canon = a.pair;
        std::string ivStr = interval;
        ctrader_->getTrendbars(
            *symId, period, 0, static_cast<int64_t>(std::time(nullptr)) * 1000, 3,
            [this, canon, ivStr, ivSec](ctrader::TrendbarsResult res) {
                if (!res.ok || res.bars.empty()) return;
                const ctrader::TrendbarData *closed =
                    pickLatestClosedBar(res.bars, ivSec);
                if (!closed) return;
                if (auto candle = candleJsonFromBar(canon, ivStr, *closed))
                    evaluateCandles({*candle});
            });
    }
}

}  // namespace ctraderplus::alerts
