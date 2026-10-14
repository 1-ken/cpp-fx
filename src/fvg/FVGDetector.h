#pragma once

#include <deque>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "fvg/Types.h"

namespace ctraderplus::fvg {

// Returns pip size for a given currency pair or instrument.
// JPY pairs: 0.01; Gold/XAU: 0.1; Others: 0.0001
double pipSizeForSymbol(const std::string &symbol);

class FVGDetector {
  public:
    struct Options {
        double minGapPips = 1.5;
        double minAtrMultiplier = 0.0;
        int atrPeriod = 14;
    };

    FVGDetector();
    explicit FVGDetector(const Options &opt);

    // Pure mathematical function to check if 3 consecutive closed bars form an FVG.
    // b1: Candle t-2, b2: Candle t-1 (displacement), b3: Candle t (just closed).
    static std::optional<FVGZone> detectFVG(const ClosedBar &b1,
                                            const ClosedBar &b2,
                                            const ClosedBar &b3,
                                            double minGapPips = 1.5,
                                            double currentAtr = 0.0,
                                            double minAtrMultiplier = 0.0);

    // Feed a completed closed bar for a symbol and timeframe.
    // Returns a formed FVGZone if one was confirmed by this closed bar.
    std::optional<FVGZone> onClosedBar(const ClosedBar &bar);

    void setOptions(const Options &opt) { opt_ = opt; }
    const Options &options() const { return opt_; }

    void reset();

  private:
    struct SeriesState {
        std::deque<ClosedBar> bars;
        std::deque<double> trueRanges;
        double atrSum = 0.0;
    };

    Options opt_;
    // Key: symbol + ":" + timeframe
    std::map<std::string, SeriesState> series_;
};

}  // namespace ctraderplus::fvg
