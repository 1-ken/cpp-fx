#include "fvg/BarAggregator.h"

#include <algorithm>

namespace ctraderplus::fvg {

namespace {
const std::vector<std::string> kHigherTimeframes = {"M5", "M15", "H1", "H4", "D1"};
}

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

        const int64_t htfBucket = (m1Bar.bucketStartSec / tfSec) * tfSec;
        const std::string key = m1Bar.symbol + ":" + htf;
        RunningBar &htfRunning = runningHtfBars_[key];

        if (htfRunning.valid && htfRunning.bucketStartSec != htfBucket) {
            // Previous HTF bucket is complete!
            ClosedBar htfClosed;
            htfClosed.symbol = m1Bar.symbol;
            htfClosed.timeframe = htf;
            htfClosed.bucketStartSec = htfRunning.bucketStartSec;
            htfClosed.open = htfRunning.open;
            htfClosed.high = htfRunning.high;
            htfClosed.low = htfRunning.low;
            htfClosed.close = htfRunning.close;
            htfClosed.volume = htfRunning.volume;
            emitted.push_back(htfClosed);

            // Start new HTF bucket
            htfRunning.bucketStartSec = htfBucket;
            htfRunning.open = m1Bar.open;
            htfRunning.high = m1Bar.high;
            htfRunning.low = m1Bar.low;
            htfRunning.close = m1Bar.close;
            htfRunning.volume = m1Bar.volume;
        } else if (!htfRunning.valid) {
            htfRunning.bucketStartSec = htfBucket;
            htfRunning.open = m1Bar.open;
            htfRunning.high = m1Bar.high;
            htfRunning.low = m1Bar.low;
            htfRunning.close = m1Bar.close;
            htfRunning.volume = m1Bar.volume;
            htfRunning.valid = true;
        } else {
            // Merge 1m into running HTF bucket
            htfRunning.high = std::max(htfRunning.high, m1Bar.high);
            htfRunning.low = std::min(htfRunning.low, m1Bar.low);
            htfRunning.close = m1Bar.close;
            htfRunning.volume += m1Bar.volume;
        }
    }
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
            out.push_back(b);
            kv.second.valid = false;
        }
    }
    return out;
}

}  // namespace ctraderplus::fvg
