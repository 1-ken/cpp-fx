#include "market/AllowedPairs.h"

#include "core/Config.h"
#include "util/PairNormalizer.h"

namespace ctraderplus::market {

bool hasExplicitPairList(const core::Config &cfg) { return !cfg.subscribedPairs.empty(); }

std::unordered_set<std::string> buildAllowedCanonicalSet(const core::Config &cfg) {
    std::unordered_set<std::string> out;
    out.reserve(cfg.subscribedPairs.size());
    for (const auto &pair : cfg.subscribedPairs) {
        std::string canon = util::canonicalPair(pair);
        if (!canon.empty()) out.insert(std::move(canon));
    }
    return out;
}

bool isAllowedPair(const core::Config &cfg, const std::string &canonical) {
    if (!hasExplicitPairList(cfg)) return true;
    std::string canon = util::canonicalPair(canonical);
    if (canon.empty()) return false;
    const auto allowed = buildAllowedCanonicalSet(cfg);
    return allowed.count(canon) > 0;
}

}  // namespace ctraderplus::market
