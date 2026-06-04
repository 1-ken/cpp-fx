#include "services/NotificationQueue.h"

#include <json/json.h>

#include <trantor/utils/Logger.h>

#include "services/Notifier.h"
#include "services/RedisService.h"

namespace ctraderplus::services {

namespace {

void dispatchOne(Notifier &notifier, RedisService *redis, const std::string &dlqKey,
                 const alerts::TriggeredAlert &t, std::function<void(bool)> onDone) {
    const alerts::Alert &a = t.alert;
    double target = a.alertType == "candle_close" ? a.threshold.value_or(0)
                                                  : a.targetPrice.value_or(0);
    std::string cond = a.alertType == "candle_close" ? a.direction.value_or("")
                                                     : a.condition.value_or("");

    if (a.channel == "sms") {
        std::string text = !a.customMessage.empty()
                               ? a.customMessage
                               : Notifier::formatAlertMessage(
                                     a.pair, target, t.currentPrice, cond, "", a.alertType,
                                     t.timeframe);
        notifier.sendSms(a.phone, text, onDone);
    } else if (a.channel == "call") {
        std::string text = !a.customMessage.empty()
                               ? a.customMessage
                               : Notifier::formatAlertMessage(
                                     a.pair, target, t.currentPrice, cond, "", a.alertType,
                                     t.timeframe);
        notifier.sendCall(a.phone, text, onDone);
    } else {
        std::string msg = Notifier::formatAlertMessage(
            a.pair, target, t.currentPrice, cond, a.customMessage, a.alertType, t.timeframe);
        notifier.sendEmail(a.email, "Price Alert: " + a.pair, msg, onDone);
    }
}

}  // namespace

void NotificationQueue::configure(const core::Config &cfg, Notifier *notifier,
                                  RedisService *redis, trantor::EventLoop *workerLoop) {
    cfg_ = &cfg;
    notifier_ = notifier;
    redis_ = redis;
    loop_ = workerLoop;
}

void NotificationQueue::enqueue(alerts::TriggeredAlert triggered) {
    if (!loop_ || !notifier_) return;
    {
        std::lock_guard<std::mutex> lk(mu_);
        pending_.push_back(Job{std::move(triggered), 0});
    }
    loop_->queueInLoop([this]() { pump(); });
}

void NotificationQueue::pump() {
    if (!loop_ || !notifier_) return;
    int workers = std::max(1, cfg_->notificationWorkerCount);
    for (int i = 0; i < workers; ++i) {
        Job job;
        {
            std::lock_guard<std::mutex> lk(mu_);
            if (pending_.empty()) return;
            job = std::move(pending_.front());
            pending_.pop_front();
        }
        processJob(job);
    }
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (!pending_.empty()) loop_->queueInLoop([this]() { pump(); });
    }
}

void NotificationQueue::processJob(Job job) {
    auto onDone = [this, job = std::move(job)](bool ok) mutable {
        if (ok) return;
        ++job.attempts;
        if (job.attempts < cfg_->notificationMaxRetries) {
            double delay = cfg_->notificationRetryDelaySeconds;
            loop_->runAfter(delay, [this, j = std::move(job)]() mutable {
                {
                    std::lock_guard<std::mutex> lk(mu_);
                    pending_.push_front(std::move(j));
                }
                pump();
            });
            return;
        }
        pushDlq(job.triggered.alert);
    };
    dispatchOne(*notifier_, redis_, cfg_->notificationDlqKey, job.triggered, onDone);
}

void NotificationQueue::pushDlq(const alerts::Alert &a) {
    if (!redis_ || !redis_->connected()) return;
    Json::Value j = a.toJson();
    Json::StreamWriterBuilder wb;
    wb["indentation"] = "";
    redis_->pushJson(cfg_->notificationDlqKey, Json::writeString(wb, j));
}

void NotificationQueue::startDlqRetryLoop() {
    if (!loop_ || !redis_ || !notifier_) return;
    const double interval = std::max(1.0, cfg_->notificationRetryDelaySeconds);
    const std::string dlqKey = cfg_->notificationDlqKey;
    loop_->runEvery(interval, [this, dlqKey]() {
        if (!redis_ || !redis_->connected()) return;
        constexpr int kBatch = 10;
        redis_->readJsonQueue(dlqKey, kBatch, [this, dlqKey](std::vector<std::string> batch) {
            if (batch.empty()) return;
            std::vector<std::string> failed;
            for (const auto &js : batch) {
                Json::Value alertJson;
                Json::CharReaderBuilder b;
                std::unique_ptr<Json::CharReader> rd(b.newCharReader());
                std::string errs;
                if (!rd->parse(js.c_str(), js.c_str() + js.size(), &alertJson, &errs)) {
                    failed.push_back(js);
                    continue;
                }
                alerts::TriggeredAlert t;
                t.alert = alerts::Alert::fromJson(alertJson);
                t.currentPrice = t.alert.lastCheckedPrice.value_or(0);
                if (t.alert.alertType == "candle_close")
                    t.timeframe = t.alert.interval.value_or("");
                enqueue(std::move(t));
            }
            if (!failed.empty() && redis_) redis_->requeueJsonBatch(dlqKey, failed);
        });
    });
}

}  // namespace ctraderplus::services
