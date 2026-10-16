#include "fvg/FVGDetector.h"

#include <algorithm>
#include <cmath>

namespace ctraderplus::fvg {

double pipSizeForSymbol(const std::string &symbol) {
    if (symbol.find("JPY") != std::string::npos) {
        return 0.01;
    }
    if (symbol.find("XAU") != std::string::npos || symbol.find("GOLD") != std::string::npos) {
        return 0.1;
    }
    if (symbol.find("BTC") != std::string::npos || symbol.find("USDT") != std::string::npos) {
        return 1.0;
    }
    return 0.0001;
}

FVGDetector::FVGDetector() = default;

FVGDetector::FVGDetector(const Options &opt) : opt_(opt) {}

std::optional<FVGZone> FVGDetector::detectFVG(const ClosedBar &b1,
                                            const ClosedBar &b2,
                                            const ClosedBar &b3,
                                            double minGapPips,
                                            double currentAtr,
                                            double minAtrMultiplier) {
    const double pipSize = pipSizeForSymbol(b3.symbol);
    const double minGap = minGapPips * pipSize;
    const double atrThreshold = (minAtrMultiplier > 0.0 && currentAtr > 0.0)
                                    ? currentAtr * minAtrMultiplier
                                    : 0.0;
    const double effectiveThreshold = std::max(minGap, atrThreshold);

    // Bullish FVG: b1.High < b3.Low
    if (b1.high < b3.low) {
        const double gap = b3.low - b1.high;
        if (gap >= effectiveThreshold) {
            FVGZone zone;
            zone.id = b3.symbol + "_" + b3.timeframe + "_" + std::to_string(b3.bucketStartSec);
            zone.symbol = b3.symbol;
            zone.timeframe = b3.timeframe;
            zone.top = b3.low;
            zone.bottom = b1.high;
            zone.is_bullish = true;
            zone.formed_timestamp = b3.bucketStartSec;
            zone.mitigated = false;
            return zone;
        }
    }

    // Bearish FVG: b1.Low > b3.High
    if (b1.low > b3.high) {
        const double gap = b1.low - b3.high;
        if (gap >= effectiveThreshold) {
            FVGZone zone;
            zone.id = b3.symbol + "_" + b3.timeframe + "_" + std::to_string(b3.bucketStartSec);
            zone.symbol = b3.symbol;
            zone.timeframe = b3.timeframe;
            zone.top = b1.low;
            zone.bottom = b3.high;
            zone.is_bullish = false;
            zone.formed_timestamp = b3.bucketStartSec;
            zone.mitigated = false;
            return zone;
        }
    }

    return std::nullopt;
}

std::optional<FVGZone> FVGDetector::onClosedBar(const ClosedBar &bar) {
    const std::string key = bar.symbol + ":" + bar.timeframe;
    SeriesState &st = series_[key];

    // Ignore duplicate or stale bars (for example warmup history overlapping live bars).
    // Letting them in would corrupt the 3-bar window and could fabricate or hide a gap.
    if (!st.bars.empty() && bar.bucketStartSec <= st.bars.back().bucketStartSec) {
        return std::nullopt;
    }

    // Calculate True Range for ATR
    if (!st.bars.empty()) {
        const auto &prev = st.bars.back();
        const double tr = std::max({bar.high - bar.low,
                                    std::fabs(bar.high - prev.close),
                                    std::fabs(bar.low - prev.close)});
        st.trueRanges.push_back(tr);
        st.atrSum += tr;
        if (static_cast<int>(st.trueRanges.size()) > opt_.atrPeriod) {
            st.atrSum -= st.trueRanges.front();
            st.trueRanges.pop_front();
        }
    }

    st.bars.push_back(bar);
    if (st.bars.size() > 50) {
        st.bars.pop_front();
    }

    if (st.bars.size() < 3) {
        return std::nullopt;
    }

    const size_t n = st.bars.size();
    const ClosedBar &b1 = st.bars[n - 3];
    const ClosedBar &b2 = st.bars[n - 2];
    const ClosedBar &b3 = st.bars[n - 1];

    const double currentAtr = (!st.trueRanges.empty())
                                  ? (st.atrSum / st.trueRanges.size())
                                  : 0.0;

    return detectFVG(b1, b2, b3, opt_.minGapPips, currentAtr, opt_.minAtrMultiplier);
}

void FVGDetector::reset() {
    series_.clear();
}

void FVGDetector::resetSeries(const std::string &symbol, const std::string &timeframe) {
    series_.erase(symbol + ":" + timeframe);
}

}  // namespace ctraderplus::fvg
