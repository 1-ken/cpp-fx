#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "fvg/Types.h"

namespace ctraderplus::fvg {

class FVGRegistry {
  public:
    struct Options {
        int maxUnmitigatedPerTimeframe = 20;
        int maxAgeBars = 100;
    };

    FVGRegistry();
    explicit FVGRegistry(const Options &opt);

    // Register a newly formed FVG zone. Returns true if added, false if duplicate.
    bool registerZone(const FVGZone &zone);

    // Check mitigation of active zones against a newly closed bar.
    // Returns a list of zones that became mitigated by this bar.
    std::vector<FVGZone> checkMitigation(const ClosedBar &bar);

    // Get active (unmitigated) zones for a given symbol and optional timeframe
    std::vector<FVGZone> getActiveZones(const std::string &symbol = "",
                                        const std::string &timeframe = "") const;

    // Prune stale or excess zones
    void prune(const std::string &symbol, const std::string &timeframe, int64_t currentBarTimeSec);

    void clear();

  private:
    static std::string makeKey(const std::string &symbol, const std::string &timeframe) {
        return symbol + "_" + timeframe;
    }

    Options opt_;
    mutable std::mutex mu_;
    // Key: Symbol + "_" + Timeframe
    std::unordered_map<std::string, std::vector<FVGZone>> active_fvg_registry_;
};

}  // namespace ctraderplus::fvg
