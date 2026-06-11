#include "market/SymbolSubscriptionPlanner.h"

#include <algorithm>
#include <set>
#include <unordered_set>

#include <trantor/utils/Logger.h>

#include "alerts/AlertManager.h"
#include "core/Config.h"
#include "ctrader/SymbolRegistry.h"
#include "market/AllowedPairs.h"
#include "services/PostgresService.h"
#include "util/PairNormalizer.h"

namespace ctraderplus::market {

SymbolSubscriptionPlanner::SymbolSubscriptionPlanner(const core::Config &cfg,
                                                     ctrader::SymbolRegistry &registry,
                                                     alerts::AlertManager &alerts)
    : cfg_(cfg), registry_(registry), alerts_(alerts) {}

bool SymbolSubscriptionPlanner::isMajorForexPair(
    const std::string &canonical, const std::vector<std::string> &majors) {
    if (canonical.size() != 6) return false;
    std::string base = canonical.substr(0, 3);
    std::string quote = canonical.substr(3, 3);
    auto has = [&](const std::string &ccy) {
        return std::find(majors.begin(), majors.end(), ccy) != majors.end();
    };
    return has(base) && has(quote);
}

std::vector<int64_t> SymbolSubscriptionPlanner::computeSymbolIds() const {
    if (hasExplicitPairList(cfg_)) {
        const auto allowed = buildAllowedCanonicalSet(cfg_);
        std::vector<int64_t> ids;
        ids.reserve(allowed.size());
        size_t resolved = 0;
        for (const auto &pair : allowed) {
            auto id = registry_.resolveId(pair);
            if (id) {
                ids.push_back(*id);
                ++resolved;
            } else {
                LOG_WARN << "Unresolved subscribed pair: " << pair;
            }
        }
        std::sort(ids.begin(), ids.end());
        ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
        LOG_INFO << "Scoped spot subscriptions: " << ids.size() << " symbols ("
                 << cfg_.subscribedPairs.size() << " pairs requested, " << resolved
                 << " resolved)";
        return ids;
    }

    if (cfg_.ctrader.subscribeAllSymbols) {
        auto ids = registry_.enabledIds();
        if (cfg_.ctrader.maxSubscribedSymbols > 0 &&
            ids.size() > static_cast<size_t>(cfg_.ctrader.maxSubscribedSymbols)) {
            ids.resize(static_cast<size_t>(cfg_.ctrader.maxSubscribedSymbols));
        }
        return ids;
    }

    std::unordered_set<std::string> pairs;
    for (const auto &sym : registry_.all()) {
        if (!sym.enabled) continue;
        std::string canon = util::canonicalPair(sym.name);
        if (canon.empty()) continue;
        if (isMajorForexPair(canon, cfg_.majors)) pairs.insert(canon);
    }

    for (const auto &a : alerts_.getActiveAlerts()) {
        std::string canon = util::canonicalPair(a.pair);
        if (!canon.empty()) pairs.insert(canon);
    }

    if (postgres_) {
        for (const auto &p : postgres_->listAllFavoritePairs()) {
            std::string canon = util::canonicalPair(p);
            if (!canon.empty()) pairs.insert(canon);
        }
    }

    std::vector<int64_t> ids;
    ids.reserve(pairs.size());
    for (const auto &p : pairs) {
        auto id = registry_.resolveId(p);
        if (id) ids.push_back(*id);
    }
    std::sort(ids.begin(), ids.end());
    ids.erase(std::unique(ids.begin(), ids.end()), ids.end());

    if (cfg_.ctrader.maxSubscribedSymbols > 0 &&
        ids.size() > static_cast<size_t>(cfg_.ctrader.maxSubscribedSymbols)) {
        ids.resize(static_cast<size_t>(cfg_.ctrader.maxSubscribedSymbols));
    }

    LOG_INFO << "Scoped spot subscriptions: " << ids.size() << " symbols ("
             << pairs.size() << " pairs requested)";
    return ids;
}

}  // namespace ctraderplus::market
