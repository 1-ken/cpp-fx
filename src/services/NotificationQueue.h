#pragma once

#include <chrono>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>

#include <trantor/net/EventLoop.h>

#include "alerts/AlertManager.h"
#include "core/Config.h"

namespace ctraderplus::services {
class Notifier;
class RedisService;
}

namespace ctraderplus::services {

// Async notification dispatch on the worker event loop with retries and DLQ.
class NotificationQueue {
  public:
    void configure(const core::Config &cfg, Notifier *notifier, RedisService *redis,
                   trantor::EventLoop *workerLoop);

    void enqueue(alerts::TriggeredAlert triggered);

    void startDlqRetryLoop();

  private:
    struct Job {
        alerts::TriggeredAlert triggered;
        int attempts = 0;
    };

    struct CallGate {
        bool inFlight = false;
        std::chrono::steady_clock::time_point lastPlaced{};
    };

    void pump();
    void processJob(Job job);
    void pushDlq(const alerts::Alert &a);

    static std::string callCoalesceKey(const alerts::Alert &a);
    static bool alertHasCallChannel(const alerts::Alert &a);
    static void stripCallChannel(alerts::Alert &a);
    static void appendCallMessage(alerts::Alert &dst, const alerts::Alert &src);
    bool shouldSkipCallLocked(const std::string &key) const;
    bool tryMergeCallIntoPendingLocked(alerts::TriggeredAlert &incoming);
    void dispatchOneChannel(const alerts::TriggeredAlert &t, const std::string &channel,
                            std::function<void(bool)> onDone);
    void dispatchAllChannels(const alerts::TriggeredAlert &t, std::function<void(bool)> onDone);

    const core::Config *cfg_ = nullptr;
    Notifier *notifier_ = nullptr;
    RedisService *redis_ = nullptr;
    trantor::EventLoop *loop_ = nullptr;

    std::mutex mu_;
    std::deque<Job> pending_;
    std::unordered_map<std::string, CallGate> callGates_;
    bool pumping_ = false;

    static constexpr double kCallQuietWindowSeconds = 15.0;
};

}  // namespace ctraderplus::services
