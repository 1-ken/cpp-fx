#pragma once

#include <atomic>
#include <cstdint>
#include <string>

namespace ctraderplus::core {

// Process-wide counters. Logged periodically as one structured line.
struct Metrics {
    std::atomic<uint64_t> wsBroadcasts{0};
    std::atomic<uint64_t> wsBroadcastDurationUs{0};
    std::atomic<int> wsClients{0};
    std::atomic<uint64_t> wsDroppedSlow{0};

    std::atomic<uint64_t> alertEvalCount{0};
    std::atomic<uint64_t> alertEvalDurationUs{0};
    std::atomic<uint64_t> alertTriggerTotal{0};

    std::atomic<int> ctraderInflight{0};

    std::atomic<uint64_t> pgQueryCount{0};
    std::atomic<uint64_t> pgQueryDurationUs{0};

    std::atomic<int> notificationQueueDepth{0};
    std::atomic<uint64_t> notificationDlqDepth{0};
    std::atomic<uint64_t> notificationRetryTotal{0};

    std::atomic<uint64_t> snapshotSeq{0};

    static Metrics &instance();
    std::string jsonLine() const;
};

}  // namespace ctraderplus::core
