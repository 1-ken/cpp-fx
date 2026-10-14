#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include <json/json.h>

#include "fvg/BarAggregator.h"
#include "fvg/FVGDetector.h"
#include "fvg/FVGRegistry.h"
#include "fvg/Types.h"

namespace ctraderplus::fvg {

class FVGService {
  public:
    using BroadcastSink = std::function<void(const Json::Value &)>;

    struct Config {
        bool enabled = true;
        double minGapPips = 1.5;
        double minAtrMultiplier = 0.0;
        int maxUnmitigatedPerTimeframe = 20;
        int maxAgeBars = 100;
    };

    FVGService();
    explicit FVGService(const Config &cfg);
    ~FVGService();

    void setBroadcastSink(BroadcastSink sink) {
        broadcastSink_ = std::move(sink);
    }

    void start();
    void stop();
    bool isRunning() const { return running_.load(); }

    // Thread-safe inputs called from market/hub or candle monitor without blocking
    void feedTick(const std::string &symbol, int64_t timestampSec, double price);
    void feed1mClosedBar(const std::string &symbol, int64_t bucketStartSec,
                         double open, double high, double low, double close,
                         int64_t volume = 0);
    void feedClosedBar(const ClosedBar &bar);

    // Direct access to registry for REST query
    const FVGRegistry &registry() const { return registry_; }

    static Json::Value serializeZone(const FVGZone &zone, const char *status);

  private:
    void workerLoop();
    void processCompletedBar(const ClosedBar &bar);
    void enqueueBar(const ClosedBar &bar);

    Config cfg_;
    std::atomic<bool> running_{false};

    BarAggregator aggregator_;
    FVGDetector detector_;
    FVGRegistry registry_;

    std::mutex queueMu_;
    std::condition_variable queueCv_;
    std::queue<ClosedBar> queue_;

    std::thread workerThread_;
    BroadcastSink broadcastSink_;
};

}  // namespace ctraderplus::fvg
