#include "ctrader/SymbolRegistry.h"

#include <algorithm>

#include "util/PairNormalizer.h"

namespace ctraderplus::ctrader {

std::string SymbolRegistry::classifyGroup(const std::string &canonical) {
    // 6-letter alpha forex pairs (EURUSD, XAUUSD, ...) -> "currencies".
    // Everything else (CL1, HG1, US10Y, indices, ...) -> "commodities".
    if (canonical.size() == 6 &&
        std::all_of(canonical.begin(), canonical.end(),
                    [](unsigned char c) { return std::isalpha(c) != 0; })) {
        return "currencies";
    }
    return "commodities";
}

void SymbolRegistry::update(const std::vector<SymbolInfo> &symbols) {
    std::lock_guard<std::mutex> lk(mu_);
    byId_.clear();
    canonicalById_.clear();
    idByCanonical_.clear();
    for (const auto &s : symbols) {
        byId_[s.id] = s;
        std::string canon = util::canonicalPair(s.name);
        if (canon.empty()) canon = s.name;
        canonicalById_[s.id] = canon;
        idByCanonical_[canon] = s.id;
    }
}

bool SymbolRegistry::has(int64_t id) const {
    std::lock_guard<std::mutex> lk(mu_);
    return byId_.count(id) > 0;
}

std::string SymbolRegistry::nameForId(int64_t id) const {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = byId_.find(id);
    return it == byId_.end() ? "" : it->second.name;
}

std::string SymbolRegistry::canonicalForId(int64_t id) const {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = canonicalById_.find(id);
    return it == canonicalById_.end() ? "" : it->second;
}

std::string SymbolRegistry::groupForId(int64_t id) const {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = canonicalById_.find(id);
    if (it == canonicalById_.end()) return "commodities";
    return classifyGroup(it->second);
}

std::optional<int64_t> SymbolRegistry::idForCanonical(const std::string &canonical) const {
    std::string canon = util::canonicalPair(canonical);
    std::lock_guard<std::mutex> lk(mu_);
    auto it = idByCanonical_.find(canon);
    if (it == idByCanonical_.end()) return std::nullopt;
    return it->second;
}

std::vector<SymbolInfo> SymbolRegistry::all() const {
    std::lock_guard<std::mutex> lk(mu_);
    std::vector<SymbolInfo> out;
    out.reserve(byId_.size());
    for (const auto &kv : byId_) out.push_back(kv.second);
    return out;
}

std::vector<int64_t> SymbolRegistry::enabledIds() const {
    std::lock_guard<std::mutex> lk(mu_);
    std::vector<int64_t> out;
    for (const auto &kv : byId_) {
        if (kv.second.enabled) out.push_back(kv.first);
    }
    return out;
}

size_t SymbolRegistry::size() const {
    std::lock_guard<std::mutex> lk(mu_);
    return byId_.size();
}

}  // namespace ctraderplus::ctrader
