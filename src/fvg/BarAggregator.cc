#include "fvg/BarAggregator.h"

#include <algorithm>

namespace ctraderplus::fvg {

namespace {
const std::vector<std::string> kHigherTimeframes = {"M5", "M15", "M30", "H1", "H4", "D1"};

// Floor `t` to the start of its bucket for buckets of `tfSec` seconds shifted by `offset`.
// Uses floor division so values below the offset still land in the correct (earlier) bucket.
int64_t bucketStartFor(int64_t t, int tfSec, int64_t offset) {
    int64_t shifted = t - offset;
    int64_t q = shifted / tfSec;
    if (shifted % tfSec < 0) --q;
    return q * tfSec + offset;
}
}  // namespace

int timeframeToSeconds(const std::string &tf) {
    if (tf == "M1" || tf == "1m") return 60;
    if (tf == "M5" || tf == "5m") return 300;
    if (tf == "M15" || tf == "15m") return 900;
    if (tf == "M30" || tf == "30m") return 1800;
    if (tf == "H1" || tf == "1h") return 3600;
    if (tf == "H4" || tf == "4h") return 14400;
    if (tf == "D1" || tf == "1d") return 86400;
    return 0;
}

std::string secondsToTimeframe(int sec) {
    switch (sec) {
        case 60: return "M1";
        case 300: return "M5";
        case 900: return "M15";
        case 1800: return "M30";
        case 3600: return "H1";
        case 14400: return "H4";
        case 86400: return "D1";
        default: return "";
    }
}

std::string fvgTimeframeToInterval(const std::string &tf) {
    switch (timeframeToSeconds(tf)) {
        case 60: return "1m";
        case 300: return "5m";
        case 900: return "15m";
        case 1800: return "30m";
        case 3600: return "1h";
        case 14400: return "4h";
        case 86400: return "1d";
        default: return "";
    }
}

BarAggregator::BarAggregator(BarCallback onBarCompleted)
    : onBarCompleted_(std::move(onBarCompleted)) {}

void BarAggregator::onDirectClosedBar(const ClosedBar &bar) {
    BarCallback cb;
    {
        std::lock_guard<std::mutex> lk(mu_);
        cb = onBarCompleted_;
    }
    if (cb) {
        cb(bar);
    }
}

void BarAggregator::onClosed1mBar(const std::string &symbol, int64_t bucketStartSec,
                                  double open, double high, double low, double close,
                                  int64_t volume) {
    ClosedBar m1Bar;
    m1Bar.symbol = symbol;
    m1Bar.timeframe = "M1";
    m1Bar.bucketStartSec = bucketStartSec;
    m1Bar.open = open;
    m1Bar.high = high;
    m1Bar.low = low;
    m1Bar.close = close;
    m1Bar.volume = volume;

    std::vector<ClosedBar> completed;
    BarCallback cb;
    {
        std::lock_guard<std::mutex> lk(mu_);
        cb = onBarCompleted_;
        completed.push_back(m1Bar);
        process1mClosedBarLocked(m1Bar, completed);
    }

    if (cb) {
        for (const auto &bar : completed) {
            cb(bar);
        }
    }
}

void BarAggregator::onTick(const std::string &symbol, int64_t timestampSec, double price) {
    const int64_t m1Bucket = (timestampSec / 60) * 60;
    ClosedBar closed1m;
    bool hasClosed1m = false;

    std::vector<ClosedBar> completed;
    BarCallback cb;
    {
        std::lock_guard<std::mutex> lk(mu_);
        cb = onBarCompleted_;
        RunningBar &r = running1mTicks_[symbol];
        if (r.valid && r.bucketStartSec != m1Bucket) {
            // Minute boundary crossed: finalize previous 1m bar
            closed1m.symbol = symbol;
            closed1m.timeframe = "M1";
            closed1m.bucketStartSec = r.bucketStartSec;
            closed1m.open = r.open;
            closed1m.high = r.high;
            closed1m.low = r.low;
            closed1m.close = r.close;
            closed1m.volume = r.volume;
            hasClosed1m = true;

            // Start new bucket
            r.bucketStartSec = m1Bucket;
            r.open = price;
            r.high = price;
            r.low = price;
            r.close = price;
            r.volume = 1;
        } else if (!r.valid) {
            r.bucketStartSec = m1Bucket;
            r.open = price;
            r.high = price;
            r.low = price;
            r.close = price;
            r.volume = 1;
            r.valid = true;
        } else {
            // In-bucket tick
            if (price > r.high) r.high = price;
            if (price < r.low) r.low = price;
            r.close = price;
            r.volume += 1;
        }

        if (hasClosed1m) {
            completed.push_back(closed1m);
            process1mClosedBarLocked(closed1m, completed);
        }
    }

    if (cb && !completed.empty()) {
        for (const auto &bar : completed) {
            cb(bar);
        }
    }
}

void BarAggregator::process1mClosedBarLocked(const ClosedBar &m1Bar,
                                            std::vector<ClosedBar> &emitted) {
    for (const auto &htf : kHigherTimeframes) {
        const int tfSec = timeframeToSeconds(htf);
        if (tfSec <= 0) continue;

        const std::string key = m1Bar.symbol + ":" + htf;
        int64_t offset = 0;
        if (auto offIt = bucketOffsets_.find(key); offIt != bucketOffsets_.end()) {
            offset = offIt->second;
        }
        const int64_t htfBucket = bucketStartFor(m1Bar.bucketStartSec, tfSec, offset);
        RunningBar &htfRunning = runningHtfBars_[key];

        if (htfRunning.valid && htfRunning.bucketStartSec != htfBucket) {
            // Previous HTF bucket is complete. If we only saw part of it, say so: the OHLC is
            // incomplete and must not feed 3-bar detection.
            ClosedBar htfClosed;
            htfClosed.symbol = m1Bar.symbol;
            htfClosed.timeframe = htf;
            htfClosed.bucketStartSec = htfRunning.bucketStartSec;
            htfClosed.open = htfRunning.open;
            htfClosed.high = htfRunning.high;
            htfClosed.low = htfRunning.low;
            htfClosed.close = htfRunning.close;
            htfClosed.volume = htfRunning.volume;
            htfClosed.partial = htfRunning.partial;
            emitted.push_back(htfClosed);

            // Start new HTF bucket. Subsequent buckets are always taken as observed: a missing
            // first minute (no ticks) or a market open mid-bucket is the broker's reality too.
            htfRunning.bucketStartSec = htfBucket;
            htfRunning.open = m1Bar.open;
            htfRunning.high = m1Bar.high;
            htfRunning.low = m1Bar.low;
            htfRunning.close = m1Bar.close;
            htfRunning.volume = m1Bar.volume;
            htfRunning.partial = false;
        } else if (!htfRunning.valid) {
            htfRunning.bucketStartSec = htfBucket;
            htfRunning.open = m1Bar.open;
            htfRunning.high = m1Bar.high;
            htfRunning.low = m1Bar.low;
            htfRunning.close = m1Bar.close;
            htfRunning.volume = m1Bar.volume;
            htfRunning.valid = true;
            // First bucket ever observed for this key: complete only if we saw its first minute.
            htfRunning.partial = (m1Bar.bucketStartSec != htfBucket);
        } else {
            // Merge 1m into running HTF bucket
            htfRunning.high = std::max(htfRunning.high, m1Bar.high);
            htfRunning.low = std::min(htfRunning.low, m1Bar.low);
            htfRunning.close = m1Bar.close;
            htfRunning.volume += m1Bar.volume;
        }
    }
}

void BarAggregator::seedForming(const std::string &symbol, const std::string &timeframe,
                                int64_t brokerBucketStartSec, double open, double high,
                                double low, double close) {
    const int tfSec = timeframeToSeconds(timeframe);
    // M1 is built directly from ticks and has no higher-timeframe running bucket.
    if (tfSec <= 60) return;
    const std::string tf = secondsToTimeframe(tfSec);
    const std::string key = symbol + ":" + tf;
    const int64_t newOffset = ((brokerBucketStartSec % tfSec) + tfSec) % tfSec;

    std::lock_guard<std::mutex> lk(mu_);
    int64_t oldOffset = 0;
    if (auto it = bucketOffsets_.find(key); it != bucketOffsets_.end()) oldOffset = it->second;
    RunningBar &r = runningHtfBars_[key];

    if (newOffset != oldOffset) {
        // Bucket alignment changed: whatever was accumulated used the wrong boundaries.
        bucketOffsets_[key] = newOffset;
        r = RunningBar{};
    }

    auto applySeed = [&]() {
        r.bucketStartSec = brokerBucketStartSec;
        r.open = open;
        r.high = high;
        r.low = low;
        r.close = close;
        r.volume = 0;
        r.valid = true;
        r.partial = false;
    };

    if (!r.valid) {
        applySeed();
    } else if (r.bucketStartSec == brokerBucketStartSec) {
        // Live ticks already merged into this bucket: the broker knows the true open and the
        // full high/low range so far; keep the (newer) live close.
        r.open = open;
        r.high = std::max(r.high, high);
        r.low = std::min(r.low, low);
        r.partial = false;
    } else if (r.bucketStartSec < brokerBucketStartSec) {
        // Running bucket is older than the broker's forming bucket (stale): replace it.
        applySeed();
    }
    // else: running bucket is already newer than the seed; ignore the seed.
}

std::vector<ClosedBar> BarAggregator::flushIncompleteBars() {
    std::vector<ClosedBar> out;
    std::lock_guard<std::mutex> lk(mu_);
    for (auto &kv : runningHtfBars_) {
        if (kv.second.valid) {
            auto pos = kv.first.find(':');
            std::string sym = (pos != std::string::npos) ? kv.first.substr(0, pos) : kv.first;
            std::string tf = (pos != std::string::npos) ? kv.first.substr(pos + 1) : "HTF";

            ClosedBar b;
            b.symbol = sym;
            b.timeframe = tf;
            b.bucketStartSec = kv.second.bucketStartSec;
            b.open = kv.second.open;
            b.high = kv.second.high;
            b.low = kv.second.low;
            b.close = kv.second.close;
            b.volume = kv.second.volume;
            b.partial = kv.second.partial;
            out.push_back(b);
            kv.second.valid = false;
        }
    }
    return out;
}

}  // namespace ctraderplus::fvg
