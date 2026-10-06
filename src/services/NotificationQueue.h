#pragma once

#include <chrono>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include <trantor/net/EventLoop.h>

#include "alerts/AlertManager.h"
#include "alerts/ChannelDispatch.h"
#include "core/Config.h"
#include "services/RedisService.h"

namespace ctraderplus::services {
class Notifier;
}

namespace ctraderplus::services {

// Async notification dispatch on the worker event loop with retries and DLQ.
class NotificationQueue {
  public:
    void configure(const core::Config &cfg, Notifier *notifier, RedisService *redis,
                   trantor::EventLoop *workerLoop);

    void enqueue(alerts::TriggeredAlert triggered);

    void startDlqRetryLoop();

    static std::string idempotencyKey(const alerts::Alert &alert);

  private:
    struct Job {
        alerts::TriggeredAlert triggered;
        int attempts = 0;
        std::string idem;
        bool mirrored = false;
        std::vector<std::string> onlyChannels;
    };

    struct CallGate {
        bool inFlight = false;
        std::chrono::steady_clock::time_point lastPlaced{};
    };

    void pump();
    void processJob(Job job);
    void pushDlq(const alerts::Alert &a);
    void startStreamLoop();
    void handleStreamMessages(std::vector<services::RedisService::StreamMessage> msgs);
    void noteStreamId(const std::string &idem, const std::string &streamId);
    void ackIdem(const std::string &idem);
    std::string jobPayload(const Job &job) const;

    static std::string callCoalesceKey(const alerts::Alert &a);
    static bool alertHasCallChannel(const alerts::Alert &a);
    static void stripCallChannel(alerts::Alert &a);
    static void appendCallMessage(alerts::Alert &dst, const alerts::Alert &src);
    bool shouldSkipCallLocked(const std::string &key) const;
    bool tryMergeCallIntoPendingLocked(alerts::TriggeredAlert &incoming);
    void dispatchOneChannel(const alerts::TriggeredAlert &t, const std::string &channel,
                            std::function<void(alerts::ChannelReport)> onDone);
    void dispatchAllChannels(const alerts::TriggeredAlert &t,
                            const std::vector<std::string> &channels,
                            std::function<void(std::vector<alerts::ChannelReport>)> onDone);

    const core::Config *cfg_ = nullptr;
    Notifier *notifier_ = nullptr;
    RedisService *redis_ = nullptr;
    trantor::EventLoop *loop_ = nullptr;

    std::mutex mu_;
    std::deque<Job> pending_;
    std::unordered_map<std::string, CallGate> callGates_;
    std::unordered_map<std::string, std::string> streamIds_;
    std::unordered_set<std::string> queuedIdems_;
    std::unordered_set<std::string> deliveredIdems_;
    std::string runId_;
    std::string streamKey_ = "fx:alerts:notifications";
    std::string streamGroup_ = "notif";
    bool streamStarted_ = false;
    bool pumping_ = false;

    static constexpr double kCallQuietWindowSeconds = 15.0;
};

}  // namespace ctraderplus::services
