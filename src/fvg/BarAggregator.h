#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "fvg/Types.h"

namespace ctraderplus::fvg {

class BarAggregator {
  public:
    using BarCallback = std::function<void(const ClosedBar &)>;

    explicit BarAggregator(BarCallback onBarCompleted = nullptr);

    void setBarCallback(BarCallback cb) {
        std::lock_guard<std::mutex> lk(mu_);
        onBarCompleted_ = std::move(cb);
    }

    // Ingest a completed 1-minute bar and roll up into higher timeframes (M5, M15, H1, H4, D1).
    void onClosed1mBar(const std::string &symbol, int64_t bucketStartSec,
                       double open, double high, double low, double close,
                       int64_t volume = 0);

    // Direct ingestion of an already completed bar for any timeframe
    void onDirectClosedBar(const ClosedBar &bar);

    // Feed a tick (updates running 1-minute bar; rolls over when minute advances)
    void onTick(const std::string &symbol, int64_t timestampSec, double price);

    // Flush any pending incomplete bars (e.g., during shutdown or testing)
    std::vector<ClosedBar> flushIncompleteBars();

  private:
    struct RunningBar {
        int64_t bucketStartSec = -1;
        double open = 0.0;
        double high = 0.0;
        double low = 0.0;
        double close = 0.0;
        int64_t volume = 0;
        bool valid = false;
    };

    void process1mClosedBarLocked(const ClosedBar &m1Bar, std::vector<ClosedBar> &emitted);

    mutable std::mutex mu_;
    BarCallback onBarCompleted_;

    // Key: symbol -> running 1m tick aggregation bar
    std::map<std::string, RunningBar> running1mTicks_;

    // Key: symbol + ":" + timeframe -> running higher-TF aggregated bar
    std::map<std::string, RunningBar> runningHtfBars_;
};

int timeframeToSeconds(const std::string &tf);
std::string secondsToTimeframe(int sec);

}  // namespace ctraderplus::fvg
