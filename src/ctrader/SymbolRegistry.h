#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "ctrader/Types.h"

namespace ctraderplus::ctrader {

// Thread-safe registry of broker symbols. Maps symbolId <-> name, exposes the
// canonical pair spelling, and a heuristic group ("currencies" for 6-letter
// alpha forex pairs, otherwise "commodities").
class SymbolRegistry {
  public:
    void update(const std::vector<SymbolInfo> &symbols);

    bool has(int64_t id) const;
    std::string nameForId(int64_t id) const;          // raw broker name (EUR/USD)
    std::string canonicalForId(int64_t id) const;      // EURUSD
    std::string groupForId(int64_t id) const;          // "currencies"/"commodities"/"indices"
    std::optional<int64_t> idForCanonical(const std::string &canonical) const;
    std::optional<int64_t> resolveId(const std::string &pair) const;

    std::vector<SymbolInfo> all() const;
    std::vector<int64_t> enabledIds() const;
    size_t size() const;

    // Group classification for a canonical pair name.
    static std::string classifyGroup(const std::string &canonical);

  private:
    mutable std::mutex mu_;
    std::unordered_map<int64_t, SymbolInfo> byId_;
    std::unordered_map<int64_t, std::string> canonicalById_;
    std::unordered_map<std::string, int64_t> idByCanonical_;
};

}  // namespace ctraderplus::ctrader
