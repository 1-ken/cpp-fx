#include "fvg/FVGRegistry.h"
#include "fvg/BarAggregator.h"

#include <algorithm>

namespace ctraderplus::fvg {

FVGRegistry::FVGRegistry() = default;

FVGRegistry::FVGRegistry(const Options &opt) : opt_(opt) {}

bool FVGRegistry::registerZone(const FVGZone &zone) {
    std::lock_guard<std::mutex> lk(mu_);
    const std::string key = makeKey(zone.symbol, zone.timeframe);
    auto &vec = active_fvg_registry_[key];

    // Check duplicate
    for (const auto &existing : vec) {
        if (existing.id == zone.id) {
            return false;
        }
    }

    vec.push_back(zone);

    // Prune if exceeded max unmitigated
    if (opt_.maxUnmitigatedPerTimeframe > 0 &&
        static_cast<int>(vec.size()) > opt_.maxUnmitigatedPerTimeframe) {
        vec.erase(vec.begin(), vec.begin() + (vec.size() - opt_.maxUnmitigatedPerTimeframe));
    }

    return true;
}

std::vector<FVGZone> FVGRegistry::checkMitigation(const ClosedBar &bar) {
    std::vector<FVGZone> newlyMitigated;
    std::lock_guard<std::mutex> lk(mu_);
    const std::string key = makeKey(bar.symbol, bar.timeframe);
    auto it = active_fvg_registry_.find(key);
    if (it == active_fvg_registry_.end()) {
        return newlyMitigated;
    }

    auto &vec = it->second;
    auto writeIt = vec.begin();

    for (auto readIt = vec.begin(); readIt != vec.end(); ++readIt) {
        // Do not mitigate on the same bar that formed it
        if (readIt->formed_timestamp >= bar.bucketStartSec) {
            *writeIt++ = *readIt;
            continue;
        }

        bool mitigated = false;
        if (readIt->is_bullish) {
            // Price retraced downward into bullish gap: low touched or penetrated bottom
            if (bar.low <= readIt->bottom) {
                mitigated = true;
            }
        } else {
            // Price retraced upward into bearish gap: high touched or penetrated top
            if (bar.high >= readIt->top) {
                mitigated = true;
            }
        }

        if (mitigated) {
            FVGZone mz = *readIt;
            mz.mitigated = true;
            mz.mitigated_timestamp = bar.bucketStartSec;
            newlyMitigated.push_back(mz);
            // Dropped from active unmitigated list
        } else {
            *writeIt++ = *readIt;
        }
    }

    vec.erase(writeIt, vec.end());

    // Prune by age if maxAgeBars is set
    if (opt_.maxAgeBars > 0) {
        const int tfSec = timeframeToSeconds(bar.timeframe);
        if (tfSec > 0) {
            const int64_t maxAgeSec = static_cast<int64_t>(opt_.maxAgeBars) * tfSec;
            vec.erase(
                std::remove_if(vec.begin(), vec.end(),
                               [&](const FVGZone &z) {
                                   return (bar.bucketStartSec - z.formed_timestamp) > maxAgeSec;
                               }),
                vec.end());
        }
    }

    return newlyMitigated;
}

std::vector<FVGZone> FVGRegistry::getActiveZones(const std::string &symbol,
                                                const std::string &timeframe) const {
    std::vector<FVGZone> out;
    std::lock_guard<std::mutex> lk(mu_);

    if (!symbol.empty() && !timeframe.empty()) {
        const std::string key = makeKey(symbol, timeframe);
        auto it = active_fvg_registry_.find(key);
        if (it != active_fvg_registry_.end()) {
            return it->second;
        }
        return out;
    }

    for (const auto &kv : active_fvg_registry_) {
        for (const auto &zone : kv.second) {
            if (!symbol.empty() && zone.symbol != symbol) continue;
            if (!timeframe.empty() && zone.timeframe != timeframe) continue;
            out.push_back(zone);
        }
    }

    return out;
}

void FVGRegistry::prune(const std::string &symbol, const std::string &timeframe,
                        int64_t currentBarTimeSec) {
    std::lock_guard<std::mutex> lk(mu_);
    const std::string key = makeKey(symbol, timeframe);
    auto it = active_fvg_registry_.find(key);
    if (it == active_fvg_registry_.end()) return;

    auto &vec = it->second;
    const int tfSec = timeframeToSeconds(timeframe);
    if (opt_.maxAgeBars > 0 && tfSec > 0) {
        const int64_t maxAgeSec = static_cast<int64_t>(opt_.maxAgeBars) * tfSec;
        vec.erase(
            std::remove_if(vec.begin(), vec.end(),
                           [&](const FVGZone &z) {
                               return (currentBarTimeSec - z.formed_timestamp) > maxAgeSec;
                           }),
            vec.end());
    }

    if (opt_.maxUnmitigatedPerTimeframe > 0 &&
        static_cast<int>(vec.size()) > opt_.maxUnmitigatedPerTimeframe) {
        vec.erase(vec.begin(), vec.begin() + (vec.size() - opt_.maxUnmitigatedPerTimeframe));
    }
}

void FVGRegistry::clear() {
    std::lock_guard<std::mutex> lk(mu_);
    active_fvg_registry_.clear();
}

}  // namespace ctraderplus::fvg
