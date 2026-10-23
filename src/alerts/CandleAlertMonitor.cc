#include "alerts/CandleAlertMonitor.h"

#include <algorithm>
#include <ctime>
#include <unordered_set>
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

bool isBarFullyClosed(const ctrader::TrendbarData &bar, int ivSec, std::time_t now = 0) {
    if (ivSec <= 0 || bar.utcTimestampMinutes <= 0) return false;
    if (now <= 0) now = std::time(nullptr);
    std::time_t end = bar.utcTimestampMinutes * 60 + ivSec;
    return end <= now;
}

const ctrader::TrendbarData *pickLatestClosedBar(
    const std::vector<ctrader::TrendbarData> &bars, int ivSec) {
    if (ivSec <= 0 || bars.empty()) return nullptr;
    std::time_t now = std::time(nullptr);
    const ctrader::TrendbarData *best = nullptr;
    for (const auto &b : bars) {
        std::time_t end = b.utcTimestampMinutes * 60 + ivSec;
        if (end <= now) {
            if (!best || b.utcTimestampMinutes > best->utcTimestampMinutes) best = &b;
        }
    }
    return best;
}

int64_t computeBucketOpenMinutes(int64_t tickSec, int ivSec, int64_t offsetSec) {
    if (ivSec <= 0 || tickSec <= 0) return 0;
    if (ivSec == 86400 && offsetSec == 0) {
        if (auto sess = util::forexSessionStart(static_cast<std::time_t>(tickSec))) {
            return static_cast<int64_t>(*sess / 60);
        }
    }
    int64_t shifted = tickSec - offsetSec;
    if (shifted < 0) return 0;
    int64_t bucketStartSec = (shifted / ivSec) * ivSec + offsetSec;
    return bucketStartSec / 60;
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
            warmQueue_.clear();
            warmInFlight_ = 0;
            return;
        }
    }
    syncSubscriptions();
}

std::unordered_set<CandleAlertMonitor::SubKey, CandleAlertMonitor::SubKeyHash>
CandleAlertMonitor::requiredSubscriptions() const {
    std::unordered_set<SubKey, SubKeyHash> needed;
    if (!alerts_) return needed;
    for (const auto &a : alerts_->getActiveAlerts()) {
        if (!registry_) continue;
        std::vector<std::string> intervals;
        if (a.alertType == "candle_close" && a.interval) {
            intervals.push_back(*a.interval);
        } else if (a.alertType == "market_structure" && a.interval) {
            intervals.push_back(*a.interval);
        } else if (a.alertType == "candle2_closure" && a.interval) {
            intervals.push_back(*a.interval);
        } else if (a.alertType == "sweep_confirm") {
            intervals.push_back("5m");
            intervals.push_back("1h");
        } else if (a.alertType == "hour_sweep_cisd") {
            intervals.push_back("1m");
            intervals.push_back("1h");
        } else if (a.alertType == "structure_session") {
            intervals = a.intervals;
        } else if (a.alertType == "prev_day_level") {
            if (!a.wantsDailyDolClose()) continue;
            intervals.push_back("1d");  // displacement/reversal confirm on the daily close
        } else {
            continue;
        }
        if (cfg_ && market::hasExplicitPairList(*cfg_) &&
            !market::isAllowedPair(*cfg_, a.pair))
            continue;
        auto symId = registry_->resolveId(a.pair);
        if (!symId) continue;
        for (const auto &interval : intervals) {
            int period = util::intervalToTrendbarPeriod(interval);
            if (period == 0) continue;
            needed.insert(SubKey{*symId, period});
        }
    }
    return needed;
}

void CandleAlertMonitor::syncSubscriptions() {
    if (ctrader_ && (!ctrader_->isReady() || !util::isForexMarketOpen())) return;

    auto needed = requiredSubscriptions();
    std::vector<SubKey> toAdd;
    std::vector<SubKey> toRemove;
    std::vector<SubKey> toCheckWarm;
    {
        std::lock_guard<std::mutex> lk(mu_);
        for (const auto &k : needed) {
            if (!subscribed_.count(k)) {
                toAdd.push_back(k);
            } else {
                toCheckWarm.push_back(k);
            }
        }
        for (const auto &k : subscribed_) {
            if (!needed.count(k)) toRemove.push_back(k);
        }
    }

    for (const auto &k : toRemove) {
        if (ctrader_) ctrader_->unsubscribeLiveTrendbar(k.symbolId, k.period);
        std::lock_guard<std::mutex> lk(mu_);
        subscribed_.erase(k);
        barState_.erase(k);
        meta_.erase(k);
    }
    for (const auto &k : toAdd) {
        std::string iv = util::trendbarPeriodToInterval(k.period);
        std::string canon = registry_ ? registry_->canonicalForId(k.symbolId) : "";
        if (iv.empty() || canon.empty()) continue;
        if (ctrader_) ctrader_->subscribeLiveTrendbar(k.symbolId, k.period);
        {
            std::lock_guard<std::mutex> lk(mu_);
            subscribed_.insert(k);
            meta_[k] = {canon, iv};
            barState_.erase(k);
        }
        if (ctrader_) enqueueWarm(WarmJob{k, canon, iv});
    }
    if (ctrader_ && alerts_) {
        for (const auto &k : toCheckWarm) {
            std::string iv = util::trendbarPeriodToInterval(k.period);
            std::string canon = registry_ ? registry_->canonicalForId(k.symbolId) : "";
            if (iv.empty() || canon.empty()) continue;
            if (alerts_->structureTrackSize(canon, iv) < 2) {
                enqueueWarm(WarmJob{k, canon, iv});
            }
        }
    }
}

void CandleAlertMonitor::applyWarmupBars(const SubKey &key, const std::string &canon,
                                         const std::string &iv,
                                         const std::vector<ctrader::TrendbarData> &bars) {
    if (bars.empty()) return;
    std::vector<ctrader::TrendbarData> sorted = bars;
    std::sort(sorted.begin(), sorted.end(),
              [](const ctrader::TrendbarData &a, const ctrader::TrendbarData &b) {
                  return a.utcTimestampMinutes < b.utcTimestampMinutes;
              });

    const int ivSec = util::intervalToSeconds(iv);
    const std::time_t now = std::time(nullptr);
    std::vector<Json::Value> hist;
    int64_t maxClosedOpenMinutes = 0;
    const ctrader::TrendbarData *formingBar = nullptr;

    for (const auto &bar : sorted) {
        if (bar.utcTimestampMinutes <= 0) continue;
        if (isBarFullyClosed(bar, ivSec, now)) {
            if (auto c = candleJsonFromBar(canon, iv, bar, false)) {
                hist.push_back(*c);
                if (bar.utcTimestampMinutes > maxClosedOpenMinutes) {
                    maxClosedOpenMinutes = bar.utcTimestampMinutes;
                }
            }
        } else {
            formingBar = &bar;
        }
    }

    if (!hist.empty() && alerts_) {
        alerts_->ingestStructureHistory(canon, iv, hist);
    }

    std::lock_guard<std::mutex> lk(mu_);
    if (!subscribed_.count(key)) return;
    BarTrack &track = barState_[key];
    if (ivSec > 0 && !sorted.empty()) {
        track.offsetSec = (sorted.back().utcTimestampMinutes * 60) % ivSec;
    }
    if (maxClosedOpenMinutes > track.lastEmittedOpenMinutes) {
        track.lastEmittedOpenMinutes = maxClosedOpenMinutes;
    }
    if (formingBar && formingBar->utcTimestampMinutes > track.lastEmittedOpenMinutes) {
        if (track.openMinutes == 0 || track.openMinutes < formingBar->utcTimestampMinutes) {
            track.openMinutes = formingBar->utcTimestampMinutes;
            track.bar = *formingBar;
            track.bar.period = key.period;
        } else if (track.openMinutes == formingBar->utcTimestampMinutes) {
            if (formingBar->open > 0) track.bar.open = formingBar->open;
            if (formingBar->high > track.bar.high || track.bar.high <= 0)
                track.bar.high = formingBar->high;
            if (formingBar->low < track.bar.low || track.bar.low <= 0)
                track.bar.low = formingBar->low;
            if (track.bar.close <= 0) track.bar.close = formingBar->close;
            track.bar.volume = std::max(track.bar.volume, formingBar->volume);
            track.bar.period = key.period;
        }
    }
}

void CandleAlertMonitor::seedFormingBar(int64_t symbolId, int period,
                                        const ctrader::TrendbarData &bar) {
    SubKey key{symbolId, period};
    const int ivSec = util::intervalToSeconds(util::trendbarPeriodToInterval(period));
    std::lock_guard<std::mutex> lk(mu_);
    BarTrack &track = barState_[key];
    if (ivSec > 0 && bar.utcTimestampMinutes > 0) {
        track.offsetSec = (bar.utcTimestampMinutes * 60) % ivSec;
    }
    track.openMinutes = bar.utcTimestampMinutes;
    track.bar = bar;
    track.bar.period = period;
}

void CandleAlertMonitor::enqueueWarm(WarmJob job) {
    constexpr int kWarmTtlSec = 60;
    const std::time_t now = std::time(nullptr);
    std::vector<ctrader::TrendbarData> cachedBars;
    bool cacheHit = false;
    const SubKey key = job.key;
    const std::string canon = job.canon;
    const std::string iv = job.interval;
    const int ivSec = util::intervalToSeconds(iv);
    {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = warmCache_.find(key);
        if (it != warmCache_.end() && now - it->second.fetchedAt < kWarmTtlSec &&
            (ivSec <= 0 || ((now / ivSec) == (it->second.fetchedAt / ivSec)))) {
            cachedBars = it->second.bars;
            cacheHit = true;
        } else {
            bool alreadyQueued = false;
            for (const auto &queued : warmQueue_) {
                if (queued.key == key) {
                    alreadyQueued = true;
                    break;
                }
            }
            if (!alreadyQueued) {
                warmQueue_.push_back(std::move(job));
            }
        }
    }
    if (cacheHit) {
        applyWarmupBars(key, canon, iv, cachedBars);
        return;
    }
    pumpWarm();
}

void CandleAlertMonitor::pumpWarm() {
    constexpr int kMaxInFlight = 4;
    WarmJob job;
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (warmInFlight_ >= kMaxInFlight || warmQueue_.empty() || !ctrader_) return;
        job = std::move(warmQueue_.front());
        warmQueue_.pop_front();
        ++warmInFlight_;
    }
    const int64_t toMs = static_cast<int64_t>(std::time(nullptr)) * 1000;
    const SubKey key = job.key;
    const std::string canon = job.canon;
    const std::string iv = job.interval;
    ctrader_->getTrendbars(
        key.symbolId, key.period, 0, toMs, 200,
        [this, key, canon, iv](ctrader::TrendbarsResult res) {
            if (res.ok) {
                {
                    std::lock_guard<std::mutex> lk(mu_);
                    warmCache_[key] = BarCacheEntry{std::time(nullptr), res.bars};
                }
                applyWarmupBars(key, canon, iv, res.bars);
            }
            {
                std::lock_guard<std::mutex> lk(mu_);
                if (warmInFlight_ > 0) --warmInFlight_;
            }
            pumpWarm();
        });
}

std::optional<Json::Value> CandleAlertMonitor::candleJsonFromBar(
    const std::string &canon, const std::string &interval,
    const ctrader::TrendbarData &bar, bool requireWallClockClosed) const {
    int ivSec = util::intervalToSeconds(interval);
    if (ivSec <= 0 || bar.utcTimestampMinutes <= 0) return std::nullopt;
    if (requireWallClockClosed && !isBarFullyClosed(bar, ivSec)) return std::nullopt;
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
    if (tb.utcTimestampMinutes <= 0) return;
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

    const int ivSec = util::intervalToSeconds(interval);
    std::vector<Json::Value> toEval;
    {
        std::lock_guard<std::mutex> lk(mu_);
        BarTrack &track = barState_[key];
        if (ivSec > 0) {
            track.offsetSec = (tb.utcTimestampMinutes * 60) % ivSec;
        }
        if (tb.utcTimestampMinutes <= track.lastEmittedOpenMinutes) {
            return;
        }
        if (track.openMinutes != 0 && tb.utcTimestampMinutes > track.openMinutes) {
            if (track.openMinutes > track.lastEmittedOpenMinutes) {
                if (auto candle = candleJsonFromBar(canon, interval, track.bar, false)) {
                    toEval.push_back(*candle);
                }
                track.lastEmittedOpenMinutes = track.openMinutes;
            }
            track.openMinutes = tb.utcTimestampMinutes;
            track.bar = tb;
        } else if (track.openMinutes == 0) {
            track.openMinutes = tb.utcTimestampMinutes;
            track.bar = tb;
        } else if (tb.utcTimestampMinutes == track.openMinutes) {
            if (tb.open > 0) track.bar.open = tb.open;
            if (tb.high > track.bar.high || track.bar.high <= 0) track.bar.high = tb.high;
            if (tb.low < track.bar.low || track.bar.low <= 0) track.bar.low = tb.low;
            if (tb.close > 0) track.bar.close = tb.close;
            track.bar.volume = std::max(track.bar.volume, tb.volume);
            track.bar.period = tb.period;
            track.bar.utcTimestampMinutes = tb.utcTimestampMinutes;
            if (isBarFullyClosed(track.bar, ivSec)) {
                if (auto candle = candleJsonFromBar(canon, interval, track.bar, false)) {
                    toEval.push_back(*candle);
                }
                track.lastEmittedOpenMinutes = track.openMinutes;
                track.openMinutes = 0;
            }
        }
    }
    evaluateCandles(toEval);
}

void CandleAlertMonitor::processSpotTick(int64_t symbolId, double price, int64_t tickSec) {
    if (price <= 0.0 || tickSec <= 0) return;
    std::vector<Json::Value> toEval;
    {
        std::lock_guard<std::mutex> lk(mu_);
        for (const auto &key : subscribed_) {
            if (key.symbolId != symbolId) continue;
            auto it = meta_.find(key);
            std::string canon;
            std::string interval;
            if (it != meta_.end()) {
                canon = it->second.first;
                interval = it->second.second;
            } else {
                canon = registry_ ? registry_->canonicalForId(symbolId) : "";
                interval = util::trendbarPeriodToInterval(key.period);
                if (!canon.empty() && !interval.empty()) meta_[key] = {canon, interval};
            }
            if (canon.empty() || interval.empty()) continue;

            const int ivSec = util::intervalToSeconds(interval);
            if (ivSec <= 0) continue;

            BarTrack &track = barState_[key];
            const int64_t bucketOpenMinutes =
                computeBucketOpenMinutes(tickSec, ivSec, track.offsetSec);
            if (bucketOpenMinutes <= 0 || bucketOpenMinutes <= track.lastEmittedOpenMinutes) {
                continue;
            }

            if (track.openMinutes > 0 && bucketOpenMinutes > track.openMinutes) {
                if (track.openMinutes > track.lastEmittedOpenMinutes) {
                    if (auto candle = candleJsonFromBar(canon, interval, track.bar, false)) {
                        toEval.push_back(*candle);
                    }
                    track.lastEmittedOpenMinutes = track.openMinutes;
                }
                track.openMinutes = bucketOpenMinutes;
                track.bar = ctrader::TrendbarData{};
                track.bar.period = key.period;
                track.bar.utcTimestampMinutes = bucketOpenMinutes;
                track.bar.open = price;
                track.bar.high = price;
                track.bar.low = price;
                track.bar.close = price;
                track.bar.volume = 1;
            } else if (track.openMinutes == 0) {
                track.openMinutes = bucketOpenMinutes;
                track.bar = ctrader::TrendbarData{};
                track.bar.period = key.period;
                track.bar.utcTimestampMinutes = bucketOpenMinutes;
                track.bar.open = price;
                track.bar.high = price;
                track.bar.low = price;
                track.bar.close = price;
                track.bar.volume = 1;
            } else if (bucketOpenMinutes == track.openMinutes) {
                if (track.bar.open <= 0.0) track.bar.open = price;
                if (track.bar.high <= 0.0 || price > track.bar.high) track.bar.high = price;
                if (track.bar.low <= 0.0 || price < track.bar.low) track.bar.low = price;
                track.bar.close = price;
                track.bar.volume += 1;
            }
        }
    }
    evaluateCandles(toEval);
}

void CandleAlertMonitor::flushClosedBars(std::time_t nowSec) {
    if (nowSec <= 0) nowSec = std::time(nullptr);
    if (!util::isForexMarketOpen(nowSec)) return;

    std::vector<Json::Value> toEval;
    {
        std::lock_guard<std::mutex> lk(mu_);
        for (auto &kv : barState_) {
            const SubKey &key = kv.first;
            BarTrack &track = kv.second;
            if (!subscribed_.count(key)) continue;
            if (track.openMinutes <= 0 || track.openMinutes <= track.lastEmittedOpenMinutes) {
                continue;
            }
            auto metaIt = meta_.find(key);
            if (metaIt == meta_.end()) continue;
            const std::string &canon = metaIt->second.first;
            const std::string &interval = metaIt->second.second;
            const int ivSec = util::intervalToSeconds(interval);
            if (ivSec <= 0) continue;

            const std::time_t closeSec = static_cast<std::time_t>(track.openMinutes * 60 + ivSec);
            if (closeSec <= nowSec) {
                if (auto candle = candleJsonFromBar(canon, interval, track.bar, false)) {
                    toEval.push_back(*candle);
                }
                track.lastEmittedOpenMinutes = track.openMinutes;
                track.openMinutes = 0;
            }
        }
    }
    evaluateCandles(toEval);
}

void CandleAlertMonitor::onSpot(const ctrader::SpotUpdate &update) {
    const int64_t tickSec = update.timestampMs > 0
                                ? (update.timestampMs / 1000)
                                : static_cast<int64_t>(std::time(nullptr));
    if (!util::isForexMarketOpen(static_cast<std::time_t>(tickSec))) return;

    for (const auto &tb : update.liveTrendbars) {
        processLiveTrendbar(update.symbolId, tb);
    }

    double price = 0.0;
    // Prefer bid to match cTrader's bid-based trendbar OHLC.
    if (update.hasBid && update.bid > 0.0) {
        price = update.bid;
    } else if (update.hasAsk && update.ask > 0.0) {
        price = update.ask;
    }
    if (price > 0.0) {
        processSpotTick(update.symbolId, price, tickSec);
    }
}

void CandleAlertMonitor::pollFallback() {
    if (!cfg_ || !cfg_->pollFallbackEnabled) return;
    if (!ctrader_ || !ctrader_->isReady() || !util::isForexMarketOpen() || !alerts_) return;

    auto active = alerts_->getActiveAlerts();
    std::unordered_set<SubKey, SubKeyHash> seen;
    for (const auto &a : active) {
        std::vector<std::string> intervals;
        if (a.alertType == "candle_close" && a.interval) {
            intervals.push_back(*a.interval);
        } else if (a.alertType == "market_structure" && a.interval) {
            intervals.push_back(*a.interval);
        } else if (a.alertType == "candle2_closure" && a.interval) {
            intervals.push_back(*a.interval);
        } else if (a.alertType == "sweep_confirm") {
            intervals.push_back("5m");
            intervals.push_back("1h");
        } else if (a.alertType == "hour_sweep_cisd") {
            intervals.push_back("1m");
            intervals.push_back("1h");
        } else if (a.alertType == "structure_session") {
            intervals = a.intervals;
        } else if (a.alertType == "prev_day_level") {
            if (!a.wantsDailyDolClose()) continue;
            intervals.push_back("1d");
        } else {
            continue;
        }
        auto symId = registry_->idForCanonical(a.pair);
        if (!symId) continue;
        for (const auto &interval : intervals) {
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
                [this, key, canon, ivStr, ivSec](ctrader::TrendbarsResult res) {
                    if (!res.ok || res.bars.empty()) return;
                    if (alerts_ && alerts_->structureTrackSize(canon, ivStr) < 2) {
                        applyWarmupBars(key, canon, ivStr, res.bars);
                    }
                    const ctrader::TrendbarData *closed =
                        pickLatestClosedBar(res.bars, ivSec);
                    if (!closed) return;
                    {
                        std::lock_guard<std::mutex> lk(mu_);
                        BarTrack &track = barState_[key];
                        if (closed->utcTimestampMinutes <= track.lastEmittedOpenMinutes) {
                            return;
                        }
                        track.lastEmittedOpenMinutes = closed->utcTimestampMinutes;
                    }
                    if (auto candle = candleJsonFromBar(canon, ivStr, *closed, false)) {
                        evaluateCandles({*candle});
                    }
                });
        }
    }
}

}  // namespace ctraderplus::alerts
