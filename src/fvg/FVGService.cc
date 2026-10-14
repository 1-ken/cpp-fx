#include "fvg/FVGService.h"

#include <trantor/utils/Logger.h>

namespace ctraderplus::fvg {

FVGService::FVGService() : FVGService(Config{}) {}

FVGService::FVGService(const Config &cfg)
    : cfg_(cfg),
      aggregator_([this](const ClosedBar &b) { enqueueBar(b); }),
      detector_(FVGDetector::Options{cfg.minGapPips, cfg.minAtrMultiplier, 14}),
      registry_(FVGRegistry::Options{cfg.maxUnmitigatedPerTimeframe, cfg.maxAgeBars}) {}

FVGService::~FVGService() {
    stop();
}

void FVGService::start() {
    if (!cfg_.enabled) {
        LOG_INFO << "FVG detector is disabled by feature flag";
        return;
    }
    if (running_.exchange(true)) {
        return;
    }
    workerThread_ = std::thread(&FVGService::workerLoop, this);
    LOG_INFO << "FVGService worker thread started";
}

void FVGService::stop() {
    if (!running_.exchange(false)) {
        return;
    }
    queueCv_.notify_all();
    if (workerThread_.joinable()) {
        workerThread_.join();
    }
    LOG_INFO << "FVGService worker thread stopped";
}

void FVGService::feedTick(const std::string &symbol, int64_t timestampSec, double price) {
    if (!cfg_.enabled) return;
    aggregator_.onTick(symbol, timestampSec, price);
}

void FVGService::feed1mClosedBar(const std::string &symbol, int64_t bucketStartSec,
                                 double open, double high, double low, double close,
                                 int64_t volume) {
    if (!cfg_.enabled) return;
    aggregator_.onClosed1mBar(symbol, bucketStartSec, open, high, low, close, volume);
}

void FVGService::feedClosedBar(const ClosedBar &bar) {
    if (!cfg_.enabled) return;
    enqueueBar(bar);
}

void FVGService::enqueueBar(const ClosedBar &bar) {
    {
        std::lock_guard<std::mutex> lk(queueMu_);
        // Limit queue size to avoid unbounded memory if worker falls behind
        if (queue_.size() < 10000) {
            queue_.push(bar);
        } else {
            LOG_WARN << "FVGService queue full; dropping completed bar for " << bar.symbol;
        }
    }
    queueCv_.notify_one();
}

void FVGService::workerLoop() {
    while (running_) {
        ClosedBar bar;
        {
            std::unique_lock<std::mutex> lk(queueMu_);
            queueCv_.wait(lk, [this] { return !running_ || !queue_.empty(); });
            if (!running_ && queue_.empty()) {
                break;
            }
            if (queue_.empty()) {
                continue;
            }
            bar = std::move(queue_.front());
            queue_.pop();
        }

        processCompletedBar(bar);
    }
}

void FVGService::processCompletedBar(const ClosedBar &bar) {
    // 1. Detection of new FVG on closed bar
    auto formedZone = detector_.onClosedBar(bar);
    if (formedZone) {
        if (registry_.registerZone(*formedZone)) {
            LOG_INFO << "FVG Formed: " << formedZone->id << " [" << formedZone->bottom
                     << " - " << formedZone->top << "] dir="
                     << (formedZone->is_bullish ? "BULLISH" : "BEARISH");

            if (broadcastSink_) {
                Json::Value payload(Json::objectValue);
                payload["type"] = "FVG_ALERT";
                payload["data"] = serializeZone(*formedZone, "FORMED");
                broadcastSink_(payload);
            }
        }
    }

    // 2. Mitigation check against existing active zones
    auto newlyMitigated = registry_.checkMitigation(bar);
    for (const auto &mz : newlyMitigated) {
        LOG_INFO << "FVG Mitigated: " << mz.id;
        if (broadcastSink_) {
            Json::Value payload(Json::objectValue);
            payload["type"] = "FVG_ALERT";
            payload["data"] = serializeZone(mz, "MITIGATED");
            broadcastSink_(payload);
        }
    }
}

Json::Value FVGService::serializeZone(const FVGZone &zone, const char *status) {
    Json::Value data(Json::objectValue);
    data["id"] = zone.id;
    data["symbol"] = zone.symbol;
    data["timeframe"] = zone.timeframe;
    data["direction"] = zone.is_bullish ? "BULLISH" : "BEARISH";
    data["top"] = zone.top;
    data["bottom"] = zone.bottom;
    data["bar_time"] = static_cast<Json::Int64>(zone.formed_timestamp);
    data["status"] = status;
    return data;
}

}  // namespace ctraderplus::fvg
