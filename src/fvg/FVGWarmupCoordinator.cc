#include "fvg/FVGWarmupCoordinator.h"

#include <algorithm>
#include <ctime>
#include <trantor/utils/Logger.h>

#include "core/Config.h"
#include "ctrader/CTraderClient.h"
#include "ctrader/SymbolRegistry.h"
#include "fvg/BarAggregator.h"
#include "fvg/FVGService.h"
#include "market/AllowedPairs.h"
#include "util/PairNormalizer.h"
#include "util/TimeUtil.h"

namespace ctraderplus::fvg {

namespace {
const std::vector<std::string> kWarmupTimeframes = {"M15", "H1", "H4"};
}

void FVGWarmupCoordinator::configure(const core::Config *cfg,
                                     ctrader::CTraderClient *ctrader,
                                     ctrader::SymbolRegistry *registry,
                                     FVGService *fvgService) {
    cfg_ = cfg;
    ctrader_ = ctrader;
    registry_ = registry;
    fvgService_ = fvgService;
}

bool FVGWarmupCoordinator::isSymbolWarmed(const std::string &symbol) const {
    std::string canon = util::canonicalPair(symbol);
    std::lock_guard<std::mutex> lk(mu_);
    return warmedSymbols_.count(canon) > 0;
}

void FVGWarmupCoordinator::startWarmup() {
    if (!cfg_ || !cfg_->fvgDetectorEnabled || !ctrader_ || !ctrader_->isReady() ||
        !registry_ || !fvgService_) {
        return;
    }

    std::vector<std::string> symbolsToWarm;
    if (cfg_->fvgScanAllSymbols) {
        for (const auto &s : registry_->all()) {
            if (!s.enabled) continue;
            std::string canon = util::canonicalPair(s.name);
            if (!canon.empty()) symbolsToWarm.push_back(canon);
        }
    }

    if (symbolsToWarm.empty() && market::hasExplicitPairList(*cfg_)) {
        for (const auto &p : cfg_->subscribedPairs) {
            std::string canon = util::canonicalPair(p);
            if (!canon.empty()) symbolsToWarm.push_back(canon);
        }
    }

    if (symbolsToWarm.empty()) {
        for (const auto &m : cfg_->majors) {
            for (const auto &q : cfg_->majors) {
                if (m != q) symbolsToWarm.push_back(m + q);
            }
        }
    }

    std::sort(symbolsToWarm.begin(), symbolsToWarm.end());
    symbolsToWarm.erase(std::unique(symbolsToWarm.begin(), symbolsToWarm.end()),
                        symbolsToWarm.end());

    if (cfg_->ctrader.maxSubscribedSymbols > 0 &&
        symbolsToWarm.size() > static_cast<size_t>(cfg_->ctrader.maxSubscribedSymbols)) {
        symbolsToWarm.resize(static_cast<size_t>(cfg_->ctrader.maxSubscribedSymbols));
    }

    LOG_INFO << "Starting FVG warmup for " << symbolsToWarm.size() << " symbols...";

    int barCount = cfg_->fvgWarmupBars > 0 ? cfg_->fvgWarmupBars : 50;
    for (const auto &sym : symbolsToWarm) {
        auto symId = registry_->resolveId(sym);
        if (!symId) continue;
        for (const auto &tf : kWarmupTimeframes) {
            int period = util::intervalToTrendbarPeriod(fvgTimeframeToInterval(tf));
            if (period == 0) continue;
            std::string key = sym + ":" + tf;
            std::lock_guard<std::mutex> lk(mu_);
            if (queuedKeys_.count(key) > 0) continue;
            queuedKeys_.insert(key);
            queue_.push_back(WarmJob{*symId, sym, tf, period, barCount, nullptr});
        }
    }

    pumpQueue();
}

void FVGWarmupCoordinator::warmupSymbol(const std::string &symbol,
                                       std::function<void()> onDone) {
    if (!cfg_ || !cfg_->fvgDetectorEnabled || !ctrader_ || !ctrader_->isReady() ||
        !registry_ || !fvgService_) {
        if (onDone) onDone();
        return;
    }

    std::string canon = util::canonicalPair(symbol);
    auto symId = registry_->resolveId(canon);
    if (!symId) {
        LOG_WARN << "Cannot warm up unknown symbol: " << symbol;
        if (onDone) onDone();
        return;
    }

    int barCount = cfg_->fvgWarmupBars > 0 ? cfg_->fvgWarmupBars : 50;
    {
        std::lock_guard<std::mutex> lk(mu_);
        for (size_t i = 0; i < kWarmupTimeframes.size(); ++i) {
            const auto &tf = kWarmupTimeframes[i];
            int period = util::intervalToTrendbarPeriod(fvgTimeframeToInterval(tf));
            if (period == 0) continue;
            std::string key = canon + ":" + tf;
            queuedKeys_.insert(key);
            auto cb = (i == kWarmupTimeframes.size() - 1) ? onDone : nullptr;
            queue_.push_back(WarmJob{*symId, canon, tf, period, barCount, cb});
        }
    }

    pumpQueue();
}

bool FVGWarmupCoordinator::warmupSymbolTimeframe(const std::string &symbol,
                                                 const std::string &timeframe) {
    if (!cfg_ || !cfg_->fvgDetectorEnabled || !ctrader_ || !ctrader_->isReady() || !registry_ ||
        !fvgService_) {
        return false;
    }
    const std::string tf = secondsToTimeframe(timeframeToSeconds(timeframe));
    if (tf.empty()) return false;
    const int period = util::intervalToTrendbarPeriod(fvgTimeframeToInterval(tf));
    if (period == 0) return false;

    const std::string canon = util::canonicalPair(symbol);
    if (canon.empty()) return false;
    auto symId = registry_->resolveId(canon);
    if (!symId) return false;

    const int barCount = cfg_->fvgWarmupBars > 0 ? cfg_->fvgWarmupBars : 50;
    {
        std::lock_guard<std::mutex> lk(mu_);
        const std::string key = canon + ":" + tf;
        if (queuedKeys_.count(key) > 0) return false;
        queuedKeys_.insert(key);
        // Front of the queue: alert-driven jobs must not wait behind a scan-all warmup.
        queue_.push_front(WarmJob{*symId, canon, tf, period, barCount, nullptr});
    }
    pumpQueue();
    return true;
}

void FVGWarmupCoordinator::pumpQueue() {
    std::vector<WarmJob> toLaunch;
    {
        std::lock_guard<std::mutex> lk(mu_);
        while (inFlight_ < kMaxInFlight && !queue_.empty()) {
            toLaunch.push_back(std::move(queue_.front()));
            queue_.pop_front();
            ++inFlight_;
        }
    }

    if (toLaunch.empty() || !ctrader_) return;

    for (const auto &job : toLaunch) {
        const int64_t toMs = static_cast<int64_t>(std::time(nullptr)) * 1000;
        ctrader_->getTrendbars(
            job.symbolId, job.period, 0, toMs, job.barCount,
            [this, job](ctrader::TrendbarsResult res) {
                if (res.ok && fvgService_) {
                    int tfSec = timeframeToSeconds(job.timeframe);
                    std::time_t now = std::time(nullptr);
                    // Oldest first: the detector requires ascending bucket times.
                    std::sort(res.bars.begin(), res.bars.end(),
                              [](const ctrader::TrendbarData &a, const ctrader::TrendbarData &b) {
                                  return a.utcTimestampMinutes < b.utcTimestampMinutes;
                              });
                    std::vector<ClosedBar> closedBars;
                    closedBars.reserve(res.bars.size());
                    const ctrader::TrendbarData *forming = nullptr;
                    for (const auto &tb : res.bars) {
                        std::time_t end = tb.utcTimestampMinutes * 60 + tfSec;
                        // Only closed bars go to the detector; the one still forming seeds the
                        // live aggregator so the current bucket is not built from a partial view.
                        if (end > now) {
                            forming = &tb;
                            continue;
                        }

                        ClosedBar cb;
                        cb.symbol = job.canon;
                        cb.timeframe = job.timeframe;
                        cb.bucketStartSec = tb.utcTimestampMinutes * 60;
                        cb.open = tb.open;
                        cb.high = tb.high;
                        cb.low = tb.low;
                        cb.close = tb.close;
                        cb.volume = tb.volume;
                        cb.isWarmup = true;
                        closedBars.push_back(cb);
                    }
                    if (!closedBars.empty()) {
                        fvgService_->feedHistoricalBars(closedBars);
                    }
                    if (forming) {
                        fvgService_->seedFormingBar(job.canon, job.timeframe,
                                                    forming->utcTimestampMinutes * 60,
                                                    forming->open, forming->high, forming->low,
                                                    forming->close);
                    }
                }

                if (job.onDone) {
                    try {
                        job.onDone();
                    } catch (...) {
                    }
                }

                {
                    std::lock_guard<std::mutex> lk(mu_);
                    if (inFlight_ > 0) --inFlight_;
                    warmedSymbols_.insert(job.canon);
                    // A failed fetch must not block future attempts for this pair.
                    if (!res.ok) queuedKeys_.erase(job.canon + ":" + job.timeframe);
                }

                pumpQueue();
            });
    }
}

}  // namespace ctraderplus::fvg
