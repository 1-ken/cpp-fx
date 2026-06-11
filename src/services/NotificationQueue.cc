#include "services/NotificationQueue.h"

#include <atomic>
#include <json/json.h>
#include <memory>

#include <trantor/utils/Logger.h>

#include "services/Notifier.h"
#include "services/RedisService.h"
#include "util/TimeUtil.h"

namespace ctraderplus::services {

namespace {

void dispatchOneChannel(Notifier &notifier, const alerts::TriggeredAlert &t,
                        const std::string &channel,
                        std::function<void(bool)> onDone) {
    alerts::Alert a = t.alert;
    a.channel = channel;
    alerts::TriggeredAlert copy = t;
    copy.alert = a;

    double target = a.alertType == "candle_close" ? a.threshold.value_or(0)
                                                  : a.targetPrice.value_or(0);
    std::string cond = a.alertType == "candle_close" ? a.direction.value_or("")
                                                     : a.condition.value_or("");
    std::string triggeredAt = a.triggeredAt.value_or(util::nowIso8601());

    if (channel == "sound") {
        LOG_INFO << "[alerts] in-app sound alert triggered pair=" << a.pair << " id=" << a.id;
        onDone(true);
        return;
    }

    LOG_INFO << "[alerts] dispatch notification channel=" << channel << " pair=" << a.pair
             << " id=" << a.id;

    std::string smsBody = Notifier::formatAlertSms(
        a.pair, target, copy.currentPrice, cond, a.customMessage, a.alertType, copy.timeframe,
        triggeredAt);
    std::string emailBody = Notifier::formatAlertEmailBody(
        a.pair, target, copy.currentPrice, cond, a.customMessage, a.alertType, copy.timeframe,
        triggeredAt);
    std::string subject = Notifier::formatAlertSubject(a.pair, a.alertType);

    if (channel == "sms") {
        notifier.sendSms(a.phone, smsBody, [a, onDone](bool ok) {
            if (ok) {
                LOG_INFO << "[alerts] SMS sent pair=" << a.pair << " phone=" << a.phone;
            } else {
                LOG_WARN << "[alerts] SMS failed pair=" << a.pair << " phone=" << a.phone;
            }
            onDone(ok);
        });
    } else if (channel == "call") {
        notifier.sendCall(a.phone, a.customMessage, [a, onDone](bool ok) {
            if (ok) {
                LOG_INFO << "[alerts] call placed pair=" << a.pair << " phone=" << a.phone;
            } else {
                LOG_WARN << "[alerts] call failed pair=" << a.pair << " phone=" << a.phone;
            }
            onDone(ok);
        });
    } else {
        notifier.sendEmail(a.email, subject, emailBody, [a, onDone](bool ok) {
            if (ok) {
                LOG_INFO << "[alerts] email sent pair=" << a.pair << " email=" << a.email;
            } else {
                LOG_WARN << "[alerts] email failed pair=" << a.pair << " email=" << a.email;
            }
            onDone(ok);
        });
    }
}

void dispatchAllChannels(Notifier &notifier, const alerts::TriggeredAlert &t,
                         std::function<void(bool)> onDone) {
    auto channels = t.alert.effectiveChannels();
    if (channels.empty()) {
        LOG_ERROR << "[alerts] notification skipped: no channels id=" << t.alert.id;
        onDone(false);
        return;
    }
    if (channels.size() == 1) {
        dispatchOneChannel(notifier, t, channels.front(), std::move(onDone));
        return;
    }

    auto remaining = std::make_shared<std::atomic<size_t>>(channels.size());
    auto anyOk = std::make_shared<std::atomic<bool>>(false);
    for (const auto &channel : channels) {
        dispatchOneChannel(notifier, t, channel, [remaining, anyOk, onDone](bool ok) {
            if (ok) anyOk->store(true);
            if (remaining->fetch_sub(1) == 1) onDone(anyOk->load());
        });
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
    alerts::TriggeredAlert triggered = std::move(job.triggered);
    auto onDone = [this, job = std::move(job), triggered](bool ok) mutable {
        if (ok) return;
        ++job.attempts;
        if (job.attempts < cfg_->notificationMaxRetries) {
            double delay = cfg_->notificationRetryDelaySeconds;
            job.triggered = triggered;
            loop_->runAfter(delay, [this, j = std::move(job)]() mutable {
                {
                    std::lock_guard<std::mutex> lk(mu_);
                    pending_.push_front(std::move(j));
                }
                pump();
            });
            return;
        }
        pushDlq(triggered.alert);
    };
    dispatchAllChannels(*notifier_, triggered, onDone);
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
