#pragma once

#include <atomic>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>

namespace ctraderplus::core {
struct Config;
}
namespace ctraderplus::ctrader {
class CTraderClient;
class SymbolRegistry;
}
namespace ctraderplus::fvg {
class FVGService;

class FVGWarmupCoordinator {
  public:
    FVGWarmupCoordinator() = default;

    void configure(const core::Config *cfg,
                   ctrader::CTraderClient *ctrader,
                   ctrader::SymbolRegistry *registry,
                   FVGService *fvgService);

    // Warm up the configured set of symbols or all enabled symbols
    void startWarmup();

    // Warm up a specific symbol on-demand (e.g. when viewed in UI or REST query)
    void warmupSymbol(const std::string &symbol, std::function<void()> onDone = nullptr);

    bool isSymbolWarmed(const std::string &symbol) const;

  private:
    struct WarmJob {
        int64_t symbolId = 0;
        std::string canon;
        std::string timeframe;
        int period = 0;
        int barCount = 50;
        std::function<void()> onDone;
    };

    void pumpQueue();

    const core::Config *cfg_ = nullptr;
    ctrader::CTraderClient *ctrader_ = nullptr;
    ctrader::SymbolRegistry *registry_ = nullptr;
    FVGService *fvgService_ = nullptr;

    mutable std::mutex mu_;
    std::deque<WarmJob> queue_;
    std::unordered_set<std::string> warmedSymbols_;
    std::unordered_set<std::string> queuedKeys_;
    size_t inFlight_ = 0;
    static constexpr size_t kMaxInFlight = 4;
};

}  // namespace ctraderplus::fvg
