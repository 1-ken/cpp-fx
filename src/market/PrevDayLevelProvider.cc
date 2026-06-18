#include "market/PrevDayLevelProvider.h"

#include <algorithm>

#include <trantor/utils/Logger.h>

#include "ctrader/CTraderClient.h"
#include "ctrader/SymbolRegistry.h"
#include "util/PairNormalizer.h"
#include "util/TimeUtil.h"

namespace ctraderplus::market {

namespace {
constexpr int kDailyPeriod = 12;       // ProtoOATrendbarPeriod D1
constexpr int kDaySeconds = 86400;
constexpr int kFetchTtlSeconds = 1800;  // refresh at most every 30 min
constexpr uint32_t kFetchCount = 6;

bool isDailyBarClosed(const ctrader::TrendbarData &bar, std::time_t now) {
    return bar.utcTimestampMinutes * 60 + kDaySeconds <= now;
}
}  // namespace

std::string classifyDrawOutcome(double pdh, double pdl, double high, double low,
                                double close) {
    const bool sweptHigh = high >= pdh;
    const bool sweptLow = low <= pdl;
    if (close > pdh) return "displaced_up";
    if (close < pdl) return "displaced_down";
    if (sweptHigh && sweptLow) return "swept_both";
    if (sweptHigh) return "reversal_from_high";
    if (sweptLow) return "reversal_from_low";
    return "inside";
}

std::string drawForOutcome(const std::string &outcome) {
    if (outcome == "displaced_up" || outcome == "reversal_from_low") return "high";
    if (outcome == "displaced_down" || outcome == "reversal_from_high") return "low";
    return "none";
}

void PrevDayLevelProvider::configure(ctrader::CTraderClient *ctrader,
                                     ctrader::SymbolRegistry *registry) {
    ctrader_ = ctrader;
    registry_ = registry;
}

void PrevDayLevelProvider::track(const std::string &canonicalPair) {
    std::string canon = util::canonicalPair(canonicalPair);
    if (canon.empty()) return;
    std::lock_guard<std::mutex> lk(mu_);
    tracked_.insert(canon);
}

void PrevDayLevelProvider::setTrackedPairs(const std::set<std::string> &pairs) {
    std::set<std::string> norm;
    for (const auto &p : pairs) {
        std::string canon = util::canonicalPair(p);
        if (!canon.empty()) norm.insert(canon);
    }
    std::lock_guard<std::mutex> lk(mu_);
    tracked_ = std::move(norm);
}

DayLevels PrevDayLevelProvider::currentLevels(const std::string &canonicalPair) {
    std::string canon = util::canonicalPair(canonicalPair);
    DayLevels out;
    std::lock_guard<std::mutex> lk(mu_);
    auto it = cache_.find(canon);
    if (it == cache_.end() || it->second.dailyBars.empty()) return out;
    const auto &bars = it->second.dailyBars;
    const auto &last = bars.back();
    out.valid = true;
    out.pdh = last.high;
    out.pdl = last.low;
    if (bars.size() >= 2) {
        const auto &prev = bars[bars.size() - 2];
        out.draw = drawForOutcome(
            classifyDrawOutcome(prev.high, prev.low, last.high, last.low, last.close));
    }
    return out;
}

CloseClassification PrevDayLevelProvider::classifyClose(const std::string &canonicalPair,
                                                        std::time_t openTsSec, double high,
                                                        double low, double close) {
    std::string canon = util::canonicalPair(canonicalPair);
    CloseClassification out;
    std::lock_guard<std::mutex> lk(mu_);
    auto it = cache_.find(canon);
    if (it == cache_.end()) return out;
    const auto &bars = it->second.dailyBars;
    // Prior day = the daily bar that opened exactly one day before this candle.
    const int64_t priorOpenMin = (openTsSec - kDaySeconds) / 60;
    const ctrader::TrendbarData *prior = nullptr;
    for (const auto &bar : bars) {
        if (bar.utcTimestampMinutes == priorOpenMin) {
            prior = &bar;
            break;
        }
    }
    if (!prior) return out;
    out.valid = true;
    out.pdh = prior->high;
    out.pdl = prior->low;
    out.outcome = classifyDrawOutcome(prior->high, prior->low, high, low, close);
    return out;
}

void PrevDayLevelProvider::refreshDue() {
    if (!ctrader_ || !ctrader_->isReady()) return;
    std::vector<std::string> due;
    std::time_t now = std::time(nullptr);
    {
        std::lock_guard<std::mutex> lk(mu_);
        for (const auto &canon : tracked_) {
            auto &entry = cache_[canon];
            if (entry.fetching) continue;
            if (entry.fetchedAt == 0 || now - entry.fetchedAt >= kFetchTtlSeconds) {
                entry.fetching = true;
                due.push_back(canon);
            }
        }
    }
    for (const auto &canon : due) fetchPair(canon);
}

void PrevDayLevelProvider::fetchPair(const std::string &canonicalPair) {
    if (!ctrader_ || !registry_) return;
    auto symId = registry_->idForCanonical(canonicalPair);
    if (!symId) {
        symId = registry_->resolveId(canonicalPair);
    }
    if (!symId) {
        std::lock_guard<std::mutex> lk(mu_);
        cache_[canonicalPair].fetching = false;
        return;
    }
    int64_t toMs = static_cast<int64_t>(std::time(nullptr)) * 1000;
    ctrader_->getTrendbars(
        *symId, kDailyPeriod, 0, toMs, kFetchCount,
        [this, canonicalPair](ctrader::TrendbarsResult res) {
            std::time_t now = std::time(nullptr);
            std::vector<ctrader::TrendbarData> closed;
            if (res.ok) {
                for (const auto &bar : res.bars) {
                    if (isDailyBarClosed(bar, now)) closed.push_back(bar);
                }
                std::sort(closed.begin(), closed.end(),
                          [](const ctrader::TrendbarData &a, const ctrader::TrendbarData &b) {
                              return a.utcTimestampMinutes < b.utcTimestampMinutes;
                          });
            }
            std::lock_guard<std::mutex> lk(mu_);
            auto &entry = cache_[canonicalPair];
            entry.fetching = false;
            if (!closed.empty()) {
                entry.dailyBars = std::move(closed);
                entry.fetchedAt = now;
            } else if (!res.ok) {
                LOG_DEBUG << "PrevDayLevelProvider fetch failed for " << canonicalPair
                          << ": " << res.error;
            }
        });
}

}  // namespace ctraderplus::market
