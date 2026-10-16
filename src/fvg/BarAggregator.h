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

    // Seed the currently forming higher-timeframe bucket with the broker's own forming bar
    // (open/high/low/close so far). This gives continuity after a restart mid-bucket and also
    // teaches the aggregator the broker's bucket alignment for this symbol/timeframe (for
    // example the D1 rollover at 22:00 UTC) so live buckets line up with broker candles.
    // `timeframe` is "M5", "M15", "M30", "H1", "H4" or "D1". Unsupported values are ignored.
    void seedForming(const std::string &symbol, const std::string &timeframe,
                     int64_t brokerBucketStartSec, double open, double high, double low,
                     double close);

  private:
    struct RunningBar {
        int64_t bucketStartSec = -1;
        double open = 0.0;
        double high = 0.0;
        double low = 0.0;
        double close = 0.0;
        int64_t volume = 0;
        bool valid = false;
        // First observed minute was not the first minute of the bucket and no broker seed
        // arrived: OHLC is incomplete, so the bar is reported as `partial` when it closes.
        bool partial = false;
    };

    void process1mClosedBarLocked(const ClosedBar &m1Bar, std::vector<ClosedBar> &emitted);

    mutable std::mutex mu_;
    BarCallback onBarCompleted_;

    // Key: symbol -> running 1m tick aggregation bar
    std::map<std::string, RunningBar> running1mTicks_;

    // Key: symbol + ":" + timeframe -> running higher-TF aggregated bar
    std::map<std::string, RunningBar> runningHtfBars_;

    // Key: symbol + ":" + timeframe -> seconds the broker's buckets are shifted from epoch
    // alignment. Absent means 0 (pure epoch alignment, the original behaviour).
    std::map<std::string, int64_t> bucketOffsets_;
};

int timeframeToSeconds(const std::string &tf);
std::string secondsToTimeframe(int sec);
// "M15"/"15m" -> "15m", "H4"/"4h" -> "4h", ... Empty string when unsupported. Used to look up
// the cTrader trendbar period, which only understands the lowercase interval form.
std::string fvgTimeframeToInterval(const std::string &tf);

}  // namespace ctraderplus::fvg
