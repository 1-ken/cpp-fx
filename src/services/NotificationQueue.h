#pragma once

#include <deque>
#include <functional>
#include <mutex>

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

    void pump();
    void processJob(Job job);
    void pushDlq(const alerts::Alert &a);

    const core::Config *cfg_ = nullptr;
    Notifier *notifier_ = nullptr;
    RedisService *redis_ = nullptr;
    trantor::EventLoop *loop_ = nullptr;

    std::mutex mu_;
    std::deque<Job> pending_;
    bool pumping_ = false;
};

}  // namespace ctraderplus::services
