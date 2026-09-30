#include "services/NotificationQueue.h"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdlib>
#include <json/json.h>
#include <memory>

#include <trantor/utils/Logger.h>

#include "services/Notifier.h"
#include "services/RedisService.h"
#include "services/PostgresService.h"
#include "services/SubscriptionService.h"
#include "core/AppContext.h"
#include "core/Metrics.h"
#include "util/TimeUtil.h"
#include "util/Uuid.h"

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

std::string NotificationQueue::idempotencyKey(const alerts::Alert &alert) {
    return alert.id + "|" + alert.triggeredAt.value_or("") + "|" + alert.status;
}

void NotificationQueue::configure(const core::Config &cfg, Notifier *notifier,
                                  RedisService *redis, trantor::EventLoop *workerLoop) {
    cfg_ = &cfg;
    notifier_ = notifier;
    redis_ = redis;
    loop_ = workerLoop;
    if (runId_.empty()) {
        runId_ = util::generateUuid();
        if (runId_.empty()) runId_ = "local";
    }
}

std::string NotificationQueue::jobPayload(const Job &job) const {
    Json::Value root = job.triggered.alert.toJson();
    root["current_price"] = job.triggered.currentPrice;
    root["timeframe"] = job.triggered.timeframe;
    root["alert_type_label"] = job.triggered.alertTypeLabel;
    root["idem"] = job.idem;
    Json::StreamWriterBuilder wb;
    wb["indentation"] = "";
    return Json::writeString(wb, root);
}

void NotificationQueue::noteStreamId(const std::string &idem, const std::string &streamId) {
    if (idem.empty() || streamId.empty()) return;
    bool alreadyDone = false;
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (deliveredIdems_.count(idem)) alreadyDone = true;
        else streamIds_[idem] = streamId;
    }
    if (alreadyDone && redis_) redis_->ackStream(streamKey_, streamGroup_, streamId);
}

void NotificationQueue::ackIdem(const std::string &idem) {
    std::string id;
    {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = streamIds_.find(idem);
        if (it == streamIds_.end()) return;
        id = it->second;
        streamIds_.erase(it);
    }
    if (redis_) redis_->ackStream(streamKey_, streamGroup_, id);
}

void NotificationQueue::enqueue(alerts::TriggeredAlert triggered) {
    if (!loop_ || !notifier_) return;
    const std::string idem = idempotencyKey(triggered.alert);
    Job job;
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (deliveredIdems_.count(idem) || queuedIdems_.count(idem)) return;
        tryMergeCallIntoPendingLocked(triggered);
        if (triggered.alert.effectiveChannels().empty()) return;
        queuedIdems_.insert(idem);
        job.triggered = std::move(triggered);
        job.idem = idem;
        pending_.push_back(job);
        core::Metrics::instance().notificationQueueDepth.store(static_cast<int>(pending_.size()),
                                                               std::memory_order_relaxed);
    }
    bool mirrorExternal = false;
    for (const auto &channel : job.triggered.alert.effectiveChannels()) {
        if (channel != "sound") {
            mirrorExternal = true;
            break;
        }
    }
    if (mirrorExternal && redis_ && redis_->connected()) {
        redis_->streamAdd(streamKey_, jobPayload(job), idem, runId_,
                          [this, idem](std::optional<std::string> id) {
                              if (id) noteStreamId(idem, *id);
                          });
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
            if (pending_.empty()) {
                core::Metrics::instance().notificationQueueDepth.store(0, std::memory_order_relaxed);
                return;
            }
            job = std::move(pending_.front());
            pending_.pop_front();
            core::Metrics::instance().notificationQueueDepth.store(
                static_cast<int>(pending_.size()), std::memory_order_relaxed);
        }
        processJob(job);
    }
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (!pending_.empty()) loop_->queueInLoop([this]() { pump(); });
    }
}

void NotificationQueue::processJob(Job job) {
    alerts::TriggeredAlert triggered = job.triggered;
    const std::string idem = job.idem.empty() ? idempotencyKey(triggered.alert) : job.idem;
    job.idem = idem;
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (deliveredIdems_.count(idem)) {
            // Already handed to a provider for this alert revision.
        }
    }
    if (redis_ && redis_->connected() && job.attempts == 0) {
        redis_->setStringEx("fx:alerts:idem:" + idem, "done", 7 * 24 * 3600, [](bool) {});
    }
    auto onDone = [this, job = std::move(job), triggered, idem](bool ok) mutable {
        if (ok) {
            {
                std::lock_guard<std::mutex> lk(mu_);
                deliveredIdems_.insert(idem);
                queuedIdems_.erase(idem);
            }
            ackIdem(idem);
            return;
        }
        ++job.attempts;
        core::Metrics::instance().notificationRetryTotal.fetch_add(1, std::memory_order_relaxed);
        if (job.attempts < cfg_->notificationMaxRetries) {
            const double base = std::max(0.1, cfg_->notificationRetryDelaySeconds);
            double delay = base * std::pow(2.0, static_cast<double>(job.attempts - 1));
            const double jitter = 0.5 + (static_cast<double>(std::rand() % 1000) / 1000.0);
            delay = std::min(60.0, delay * jitter);
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
        {
            std::lock_guard<std::mutex> lk(mu_);
            queuedIdems_.erase(idem);
        }
        ackIdem(idem);
        pushDlq(triggered.alert);
    };
    dispatchAllChannels(triggered, onDone);
}

void NotificationQueue::pushDlq(const alerts::Alert &a) {
    if (!redis_ || !redis_->connected()) return;
    Json::Value j = a.toJson();
    Json::StreamWriterBuilder wb;
    wb["indentation"] = "";
    const std::string payload = Json::writeString(wb, j);
    redis_->pushJson(cfg_->notificationDlqKey, payload);
    redis_->streamAdd(cfg_->notificationDlqKey + ":stream", payload, idempotencyKey(a), runId_,
                      {});
    core::Metrics::instance().notificationDlqDepth.fetch_add(1, std::memory_order_relaxed);
}

void NotificationQueue::handleStreamMessages(std::vector<RedisService::StreamMessage> msgs) {
    for (auto &msg : msgs) {
        if (msg.runId == runId_) {
            noteStreamId(msg.idem, msg.id);
            continue;
        }
        if (!redis_) continue;
        redis_->getString("fx:alerts:idem:" + msg.idem,
                          [this, msg](std::optional<std::string> stored) {
                              if (stored && *stored == "done") {
                                  redis_->ackStream(streamKey_, streamGroup_, msg.id);
                                  return;
                              }
                              Json::Value alertJson;
                              Json::CharReaderBuilder b;
                              std::unique_ptr<Json::CharReader> rd(b.newCharReader());
                              std::string errs;
                              if (!rd->parse(msg.payload.c_str(), msg.payload.c_str() + msg.payload.size(),
                                             &alertJson, &errs)) {
                                  redis_->ackStream(streamKey_, streamGroup_, msg.id);
                                  return;
                              }
                              alerts::TriggeredAlert t;
                              t.alert = alerts::Alert::fromJson(alertJson);
                              t.currentPrice = alertJson.get("current_price", 0).asDouble();
                              t.timeframe = alertJson.get("timeframe", "").asString();
                              t.alertTypeLabel = alertJson.get("alert_type_label", "price").asString();
                              noteStreamId(msg.idem.empty() ? idempotencyKey(t.alert) : msg.idem, msg.id);
                              enqueue(std::move(t));
                          });
    }
}

void NotificationQueue::startStreamLoop() {
    if (!loop_ || !redis_ || streamStarted_) return;
    streamStarted_ = true;
    redis_->ensureStreamGroup(streamKey_, streamGroup_);
    redis_->ensureStreamGroup(cfg_->notificationDlqKey + ":stream", streamGroup_);
    auto again = std::make_shared<std::function<void()>>();
    *again = [this, again]() {
        if (!loop_) return;
        if (!redis_ || !redis_->connected()) {
            loop_->runAfter(1.0, [again]() { (*again)(); });
            return;
        }
        redis_->readStreamGroup(streamKey_, streamGroup_, runId_, 16, 1000,
                                [this, again](std::vector<RedisService::StreamMessage> msgs) {
                                    if (!msgs.empty()) handleStreamMessages(std::move(msgs));
                                    if (loop_) loop_->runAfter(0.05, [again]() { (*again)(); });
                                });
    };
    loop_->queueInLoop([again]() { (*again)(); });
}

void NotificationQueue::startDlqRetryLoop() {
    startStreamLoop();
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
