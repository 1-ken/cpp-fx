#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace ctraderplus::core {
struct Config;
}
namespace ctraderplus::alerts {
class AlertManager;
}
namespace ctraderplus::ctrader {
class SymbolRegistry;
}
namespace ctraderplus::services {
class PostgresService;
}

namespace ctraderplus::market {

// Computes which broker symbol IDs to subscribe for spot events: major FX
// crosses, active alert pairs, and user favorites (when not subscribing all).
class SymbolSubscriptionPlanner {
  public:
    SymbolSubscriptionPlanner(const core::Config &cfg, ctrader::SymbolRegistry &registry,
                              alerts::AlertManager &alerts);

    void setPostgres(services::PostgresService *pg) { postgres_ = pg; }

    std::vector<int64_t> computeSymbolIds() const;

  private:
    static bool isMajorForexPair(const std::string &canonical,
                                 const std::vector<std::string> &majors);

    const core::Config &cfg_;
    ctrader::SymbolRegistry &registry_;
    alerts::AlertManager &alerts_;
    services::PostgresService *postgres_ = nullptr;
};

}  // namespace ctraderplus::market
