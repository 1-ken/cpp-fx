#include "market/StructureEngine.h"

#include <algorithm>
#include <cmath>

namespace ctraderplus::market {

namespace {

double clamp01(double v) {
    return std::max(0.0, std::min(1.0, v));
}

class FractalPivotDetector {
  public:
    explicit FractalPivotDetector(double minSwingAtr) : minSwingAtr_(minSwingAtr) {}

    std::optional<StructurePivot> update(const StructureCandle &c, int index, double atr) {
        std::optional<StructurePivot> pivot;
        if (prevPrev_ && prev_) {
            const bool isSwingHigh = prev_->high > prevPrev_->high && prev_->high > c.high;
            const bool isSwingLow = prev_->low < prevPrev_->low && prev_->low < c.low;
            if (isSwingHigh) {
                const double leg =
                    lastLow_ ? std::fabs(prev_->high - lastLow_->price) : 1e300;
                const bool ok = minSwingAtr_ <= 0 || !lastLow_ || leg >= atr * minSwingAtr_;
                if (ok) {
                    StructurePivot p;
                    p.type = "high";
                    p.price = prev_->high;
                    p.index = index - 1;
                    p.confirmedAt = index;
                    p.timestamp = prev_->timestamp;
                    lastHigh_ = p;
                    pivot = p;
                }
            }
            if (isSwingLow && !pivot) {
                const double leg =
                    lastHigh_ ? std::fabs(prev_->low - lastHigh_->price) : 1e300;
                const bool ok = minSwingAtr_ <= 0 || !lastHigh_ || leg >= atr * minSwingAtr_;
                if (ok) {
                    StructurePivot p;
                    p.type = "low";
                    p.price = prev_->low;
                    p.index = index - 1;
                    p.confirmedAt = index;
                    p.timestamp = prev_->timestamp;
                    lastLow_ = p;
                    pivot = p;
                }
            }
        }
        prevPrev_ = prev_;
        prev_ = c;
        return pivot;
    }

  private:
    double minSwingAtr_ = 0;
    std::optional<StructureCandle> prev_;
    std::optional<StructureCandle> prevPrev_;
    std::optional<StructurePivot> lastHigh_;
    std::optional<StructurePivot> lastLow_;
};

class Engine {
  public:
    explicit Engine(double breakK) : breakK_(breakK) {}

    void onPivot(StructurePivot &pivot, double atr) {
        if (pivot.type == "high") {
            if (lastHigh_) pivot.label = pivot.price > lastHigh_->price ? "HH" : "LH";
            lastHigh_ = pivot;
        } else {
            if (lastLow_) pivot.label = pivot.price > lastLow_->price ? "HL" : "LL";
            lastLow_ = pivot;
        }
        (void)atr;
        (void)clamp01;
    }

    std::vector<StructureEvent> onBar(const StructureCandle &candle, int index, double atr) {
        std::vector<StructureEvent> evs;
        const double buffer = atr * breakK_;
        if (lastHigh_ && !lastHigh_->broken) {
            if (candle.close > lastHigh_->price + buffer) {
                StructureEvent ev;
                ev.kind = trend_ == "down" ? "CHoCH" : "BOS";
                ev.dir = "bull";
                ev.level = lastHigh_->price;
                ev.fromIndex = lastHigh_->index;
                ev.index = index;
                ev.timestamp = candle.timestamp;
                ev.fromTimestamp = lastHigh_->timestamp;
                evs.push_back(ev);
                trend_ = "up";
                lastHigh_->broken = true;
                lastHigh_->brokenAt = index;
            } else if (candle.high > lastHigh_->price && candle.close < lastHigh_->price &&
                       !lastHigh_->swept) {
                StructureEvent ev;
                ev.kind = "SWEEP";
                ev.dir = "bear";
                ev.level = lastHigh_->price;
                ev.fromIndex = lastHigh_->index;
                ev.index = index;
                ev.timestamp = candle.timestamp;
                ev.fromTimestamp = lastHigh_->timestamp;
                evs.push_back(ev);
                lastHigh_->swept = true;
            }
        }
        if (lastLow_ && !lastLow_->broken) {
            if (candle.close < lastLow_->price - buffer) {
                StructureEvent ev;
                ev.kind = trend_ == "up" ? "CHoCH" : "BOS";
                ev.dir = "bear";
                ev.level = lastLow_->price;
                ev.fromIndex = lastLow_->index;
                ev.index = index;
                ev.timestamp = candle.timestamp;
                ev.fromTimestamp = lastLow_->timestamp;
                evs.push_back(ev);
                trend_ = "down";
                lastLow_->broken = true;
                lastLow_->brokenAt = index;
            } else if (candle.low < lastLow_->price && candle.close > lastLow_->price &&
                       !lastLow_->swept) {
                StructureEvent ev;
                ev.kind = "SWEEP";
                ev.dir = "bull";
                ev.level = lastLow_->price;
                ev.fromIndex = lastLow_->index;
                ev.index = index;
                ev.timestamp = candle.timestamp;
                ev.fromTimestamp = lastLow_->timestamp;
                evs.push_back(ev);
                lastLow_->swept = true;
            }
        }
        events_.insert(events_.end(), evs.begin(), evs.end());
        return evs;
    }

    std::vector<StructureEvent> events_;
    std::string trend_;

  private:
    double breakK_ = 0.25;
    std::optional<StructurePivot> lastHigh_;
    std::optional<StructurePivot> lastLow_;
};

}  // namespace

std::vector<double> computeAtr(const std::vector<StructureCandle> &candles, int period) {
    std::vector<double> atr(candles.size(), 0);
    if (candles.empty()) return atr;
    double sum = 0;
    for (size_t i = 0; i < candles.size(); ++i) {
        const auto &c = candles[i];
        const double prevClose = i == 0 ? c.close : candles[i - 1].close;
        const double tr = i == 0
                              ? c.high - c.low
                              : std::max({c.high - c.low, std::fabs(c.high - prevClose),
                                          std::fabs(c.low - prevClose)});
        if (static_cast<int>(i) < period) {
            sum += tr;
            atr[i] = sum / static_cast<double>(i + 1);
        } else {
            atr[i] = (atr[i - 1] * (period - 1) + tr) / period;
        }
    }
    return atr;
}

StructureResult computeMarketStructure(const std::vector<StructureCandle> &candles,
                                       const StructureOptions &opt) {
    StructureResult out;
    auto atr = computeAtr(candles, opt.atrPeriod);
    FractalPivotDetector detector(opt.minSwingAtr);
    Engine engine(opt.breakK);
    for (int i = 0; i < static_cast<int>(candles.size()); ++i) {
        const double a = atr[static_cast<size_t>(i)];
        engine.onBar(candles[static_cast<size_t>(i)], i, a);
        auto pivot = detector.update(candles[static_cast<size_t>(i)], i, a);
        if (pivot) engine.onPivot(*pivot, a);
    }
    out.events = engine.events_;
    out.trend = engine.trend_;
    return out;
}

std::string structureKindKey(const std::string &kind) {
    if (kind == "CHoCH") return "choch";
    if (kind == "SWEEP") return "sweep";
    return "bos";
}

}  // namespace ctraderplus::market
