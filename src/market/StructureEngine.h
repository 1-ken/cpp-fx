#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace ctraderplus::market {

struct StructureCandle {
    std::string timestamp;
    double open = 0;
    double high = 0;
    double low = 0;
    double close = 0;
};

struct StructurePivot {
    std::string type;  // high | low
    double price = 0;
    int index = 0;
    int confirmedAt = 0;
    std::string timestamp;
    std::string label;
    bool broken = false;
    int brokenAt = -1;
    bool swept = false;
};

struct StructureEvent {
    std::string kind;  // BOS | CHoCH | SWEEP
    std::string dir;   // bull | bear
    double level = 0;
    int fromIndex = 0;
    int index = 0;
    std::string timestamp;
    std::string fromTimestamp;
};

struct StructureOptions {
    double minSwingAtr = 0;
    double breakK = 0.25;
    int atrPeriod = 14;
};

struct StructureResult {
    std::vector<StructureEvent> events;
    std::string trend;  // up | down | empty
};

std::vector<double> computeAtr(const std::vector<StructureCandle> &candles, int period = 14);

StructureResult computeMarketStructure(const std::vector<StructureCandle> &candles,
                                       const StructureOptions &opt = {});

// Online equivalent of computeMarketStructure: appending every bar yields the
// same events as a full scan of that series.
class IncrementalStructure {
  public:
    explicit IncrementalStructure(StructureOptions opt = {});
    IncrementalStructure(const IncrementalStructure &) = delete;
    IncrementalStructure &operator=(const IncrementalStructure &) = delete;
    IncrementalStructure(IncrementalStructure &&) noexcept;
    IncrementalStructure &operator=(IncrementalStructure &&) noexcept;
    ~IncrementalStructure();

    std::vector<StructureEvent> append(const StructureCandle &candle);
    void reset();
    const std::vector<StructureEvent> &events() const;
    std::string trend() const;
    int barsApplied() const;

  private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

std::string structureKindKey(const std::string &kind);

}  // namespace ctraderplus::market
