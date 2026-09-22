#include "services/NotificationQueue.h"

#include <algorithm>
#include <atomic>
#include <json/json.h>
#include <memory>

#include <trantor/utils/Logger.h>

#include "services/Notifier.h"
#include "services/RedisService.h"
#include "services/PostgresService.h"
#include "services/SubscriptionService.h"
#include "core/AppContext.h"
#include "util/TimeUtil.h"

namespace ctraderplus::services {

namespace {

std::string joinDolTriggers(const alerts::Alert &a) {
    if (a.dolTriggers.empty()) return "sweep";
    std::string out;
    for (size_t i = 0; i < a.dolTriggers.size(); ++i) {
        if (i) out += "/";
        out += a.dolTriggers[i];
    }
    return out;
}

}  // namespace

std::string NotificationQueue::callCoalesceKey(const alerts::Alert &a) {
    return a.userId + "|" + a.phone;
}

bool NotificationQueue::alertHasCallChannel(const alerts::Alert &a) {
    const auto channels = a.effectiveChannels();
    return std::find(channels.begin(), channels.end(), "call") != channels.end();
}

void NotificationQueue::stripCallChannel(alerts::Alert &a) {
    a.channels.erase(std::remove(a.channels.begin(), a.channels.end(), "call"), a.channels.end());
    if (a.channel == "call") {
        a.channel = a.channels.empty() ? "sound" : a.channels.front();
    }
}

void NotificationQueue::appendCallMessage(alerts::Alert &dst, const alerts::Alert &src) {
    std::string snippet = src.customMessage;
    if (snippet.empty()) {
        snippet = src.pair.empty() ? "alert triggered" : (src.pair + " alert");
    }
    if (dst.customMessage.empty()) {
        dst.customMessage = std::move(snippet);
        return;
    }
    if (dst.customMessage.find(snippet) == std::string::npos) {
        dst.customMessage += ". ";
        dst.customMessage += snippet;
    }
}

bool NotificationQueue::shouldSkipCallLocked(const std::string &key) const {
    auto it = callGates_.find(key);
    if (it == callGates_.end()) return false;
    if (it->second.inFlight) return true;
    if (it->second.lastPlaced.time_since_epoch().count() == 0) return false;
    const auto elapsed = std::chrono::steady_clock::now() - it->second.lastPlaced;
    return elapsed < std::chrono::duration<double>(kCallQuietWindowSeconds);
}

bool NotificationQueue::tryMergeCallIntoPendingLocked(alerts::TriggeredAlert &incoming) {
    if (!alertHasCallChannel(incoming.alert) || incoming.alert.phone.empty()) return false;

    const std::string key = callCoalesceKey(incoming.alert);
    for (auto &job : pending_) {
        if (!alertHasCallChannel(job.triggered.alert)) continue;
        if (callCoalesceKey(job.triggered.alert) != key) continue;
        appendCallMessage(job.triggered.alert, incoming.alert);
        stripCallChannel(incoming.alert);
        LOG_INFO << "[alerts] coalesced call into pending job phone=" << incoming.alert.phone
                 << " pair=" << incoming.alert.pair;
        return true;
    }

    if (shouldSkipCallLocked(key)) {
        stripCallChannel(incoming.alert);
        LOG_INFO << "[alerts] skipped call (in-flight or quiet window) phone=" << incoming.alert.phone
                 << " pair=" << incoming.alert.pair;
        return true;
    }
    return false;
}

void NotificationQueue::dispatchOneChannel(const alerts::TriggeredAlert &t,
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
    if (a.alertType == "prev_day_level") {
        target = copy.currentPrice;
        const std::string ref = a.levelRef.value_or("both");
        const std::string trig = joinDolTriggers(a);
        const std::string refTxt = ref == "high" ? "PDH" : ref == "low" ? "PDL" : "PDH/PDL";
        const std::string trigTxt = trig == "sweep"          ? "swept"
                                    : trig == "displacement" ? "displaced beyond"
                                    : trig == "reversal"     ? "reversal at"
                                    : trig == "draw_met"     ? "draw reached"
                                                             : trig;
        cond = trigTxt + " " + refTxt + " @";
    }
    std::string triggeredAt = a.triggeredAt.value_or(util::nowIso8601());

    if (channel == "sound") {
        LOG_INFO << "[alerts] in-app sound alert triggered pair=" << a.pair << " id=" << a.id;
        onDone(true);
        return;
    }

    auto &app = core::AppContext::instance();
    if ((channel == "sms" || channel == "call") && app.postgres && app.postgres->available()) {
        services::SubscriptionService sub(*app.postgres);
        auto check = sub.canSendNotification(a.userId, channel);
        if (!check.allowed) {
            LOG_WARN << "[alerts] notification skipped (" << check.code << "): " << check.message
                     << " pair=" << a.pair << " id=" << a.id << " channel=" << channel;
            onDone(true);
            return;
        }
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
        notifier_->sendSms(a.phone, smsBody, [a, onDone, postgres = app.postgres](bool ok) {
            if (ok) {
                LOG_INFO << "[alerts] SMS sent pair=" << a.pair << " phone=" << a.phone;
                if (postgres && postgres->available()) postgres->incrementDailySms(a.userId);
            } else {
                LOG_WARN << "[alerts] SMS failed pair=" << a.pair << " phone=" << a.phone;
            }
            onDone(ok);
        });
    } else if (channel == "call") {
        const std::string key = callCoalesceKey(a);
        {
            std::lock_guard<std::mutex> lk(mu_);
            if (shouldSkipCallLocked(key)) {
                LOG_INFO << "[alerts] skipped duplicate call at dispatch phone=" << a.phone
                         << " pair=" << a.pair;
                onDone(true);
                return;
            }
            callGates_[key].inFlight = true;
        }

        const std::string callMessage = a.customMessage;
        notifier_->sendCall(
            a.phone, callMessage,
            [this, a, key, onDone, postgres = app.postgres](bool ok) {
                {
                    std::lock_guard<std::mutex> lk(mu_);
                    auto &gate = callGates_[key];
                    gate.inFlight = false;
                    gate.lastPlaced = std::chrono::steady_clock::now();
                }
                if (ok) {
                    LOG_INFO << "[alerts] call placed pair=" << a.pair << " phone=" << a.phone;
                    if (postgres && postgres->available()) postgres->incrementDailyCall(a.userId);
                } else {
                    LOG_WARN << "[alerts] call failed pair=" << a.pair << " phone=" << a.phone;
                }
                onDone(ok);
            });
    } else {
        notifier_->sendEmail(a.email, subject, emailBody, [a, onDone](bool ok) {
            if (ok) {
                LOG_INFO << "[alerts] email sent pair=" << a.pair << " email=" << a.email;
            } else {
                LOG_WARN << "[alerts] email failed pair=" << a.pair << " email=" << a.email;
            }
            onDone(ok);
        });
    }
}

void NotificationQueue::dispatchAllChannels(const alerts::TriggeredAlert &t,
                                            std::function<void(bool)> onDone) {
    auto channels = t.alert.effectiveChannels();
    if (channels.empty()) {
        LOG_ERROR << "[alerts] notification skipped: no channels id=" << t.alert.id;
        onDone(false);
        return;
    }
    if (channels.size() == 1) {
        dispatchOneChannel(t, channels.front(), std::move(onDone));
        return;
    }

    auto remaining = std::make_shared<std::atomic<size_t>>(channels.size());
    auto anyOk = std::make_shared<std::atomic<bool>>(false);
    for (const auto &channel : channels) {
        dispatchOneChannel(t, channel, [remaining, anyOk, onDone](bool ok) {
            if (ok) anyOk->store(true);
            if (remaining->fetch_sub(1) == 1) onDone(anyOk->load());
        });
    }
}

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
        tryMergeCallIntoPendingLocked(triggered);
        if (triggered.alert.effectiveChannels().empty()) {
            return;
        }
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
    dispatchAllChannels(triggered, onDone);
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
                else if (t.alert.alertType == "prev_day_level")
                    t.timeframe = "1d";
                enqueue(std::move(t));
            }
            if (!failed.empty() && redis_) redis_->requeueJsonBatch(dlqKey, failed);
        });
    });
}

}  // namespace ctraderplus::services
