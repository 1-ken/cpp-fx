#include "alerts/AlertManager.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <future>
#include <memory>
#include <random>

#include <trantor/utils/Logger.h>

#include "market/MarketHub.h"
#include "services/PostgresService.h"
#include "services/RedisService.h"
#include "util/PairNormalizer.h"
#include "util/TimeUtil.h"

namespace ctraderplus::alerts {

namespace {
std::string newUuid() {
    // Lightweight UUID v4-ish generator (sufficient for alert ids).
    static thread_local std::mt19937_64 rng(
        std::chrono::steady_clock::now().time_since_epoch().count());
    static const char *hex = "0123456789abcdef";
    auto r = [&]() { return rng(); };
    char buf[37];
    uint64_t a = r(), b = r();
    int idx = 0;
    auto put = [&](uint64_t v, int n) {
        for (int i = 0; i < n; ++i) buf[idx++] = hex[(v >> (4 * i)) & 0xF];
    };
    put(a, 8);
    buf[idx++] = '-';
    put(a >> 32, 4);
    buf[idx++] = '-';
    put((a >> 48) | 0x4000, 4);  // version 4
    buf[idx++] = '-';
    put((b & 0x3FFF) | 0x8000, 4);  // variant
    buf[idx++] = '-';
    put(b >> 16, 12);
    buf[idx] = '\0';
    return std::string(buf, 36);
}

std::optional<std::time_t> parseCandleTs(const Json::Value &ts) {
    if (ts.isNumeric()) return static_cast<std::time_t>(ts.asInt64());
    if (ts.isString()) return util::parseIso8601(ts.asString());
    return std::nullopt;
}
}  // namespace

void AlertManager::configure(services::PostgresService *pg, services::RedisService *redis,
                             std::function<void(std::function<void()>)> dbExecutor,
                             std::string redisAlertQueueKey) {
    postgres_ = pg;
    redis_ = redis;
    dbExecutor_ = std::move(dbExecutor);
    redisAlertQueueKey_ = std::move(redisAlertQueueKey);
}

void AlertManager::loadAlerts() {
    if (!postgres_) {
        std::lock_guard<std::mutex> lk(mu_);
        alerts_.clear();
        rebuildIndexes();
        LOG_WARN << "AlertManager started without PostgreSQL; alert cache cleared";
        return;
    }
    auto rows = postgres_->listAlerts();
    std::lock_guard<std::mutex> lk(mu_);
    alerts_.clear();
    for (auto &row : rows) {
        Alert a = Alert::fromJson(row);
        std::string canon = util::canonicalPair(a.pair);
        if (!canon.empty()) a.pair = canon;
        if (a.alertType == "candle_close" && a.interval) {
            std::string iv = *a.interval;
            std::transform(iv.begin(), iv.end(), iv.begin(), ::tolower);
            a.interval = iv;
        }
        alerts_[a.id] = a;
    }
    rebuildIndexes();
    LOG_INFO << "Loaded " << alerts_.size() << " alerts";
    int dbCount = postgres_->countAlerts();
    if (dbCount != static_cast<int>(alerts_.size())) {
        LOG_WARN << "Alert count mismatch: PostgreSQL has " << dbCount
                 << " rows, loaded " << alerts_.size()
                 << " into memory; check alerts.data JSONB";
    }
}

std::string AlertManager::candleIndexKey(const std::string &pair,
                                         const std::string &interval) {
    return pair + "|" + interval;
}

void AlertManager::bumpUserRevision(const std::string &userId) {
    std::lock_guard<std::mutex> lk(revMu_);
    ++userAlertsRevision_[userId];
}

void AlertManager::notifySubscriptionChange() {
    if (onSubscriptionChange_) onSubscriptionChange_();
}

uint64_t AlertManager::userAlertsRevision(const std::string &userId) const {
    std::lock_guard<std::mutex> lk(revMu_);
    auto it = userAlertsRevision_.find(userId);
    return it == userAlertsRevision_.end() ? 0 : it->second;
}

void AlertManager::rebuildIndexes() {
    activePriceIndex_.clear();
    activeCandleIndex_.clear();
    for (const auto &kv : alerts_) {
        const Alert &a = kv.second;
        if (a.status != "active") continue;
        std::string key = util::canonicalPair(a.pair);
        if (key.empty()) continue;
        if (a.alertType == "price") {
            activePriceIndex_[key].push_back(a.id);
        } else if (a.alertType == "candle_close" && a.interval) {
            std::string iv = *a.interval;
            std::transform(iv.begin(), iv.end(), iv.begin(), ::tolower);
            activeCandleIndex_[candleIndexKey(key, iv)].push_back(a.id);
        }
    }
}

void AlertManager::persistAlert(const Alert &a) {
    if (!postgres_) return;
    Json::Value alertJson = a.toJson();
    const std::string alertId = a.id;
    auto write = [this, alertJson, alertId]() {
        if (!postgres_->upsertAlert(alertJson)) {
            LOG_ERROR << "upsertAlert failed (async) alert_id=" << alertId;
        }
    };
    if (dbExecutor_) {
        dbExecutor_(std::move(write));
    } else {
        write();
    }
}

bool AlertManager::persistAlertSync(const Alert &a) {
    if (!postgres_) return false;
    Json::Value alertJson = a.toJson();
    if (!dbExecutor_) return postgres_->upsertAlert(alertJson);
    auto prom = std::make_shared<std::promise<bool>>();
    auto fut = prom->get_future();
    dbExecutor_([this, alertJson, prom]() {
        try {
            prom->set_value(postgres_->upsertAlert(alertJson));
        } catch (...) {
            try {
                prom->set_exception(std::current_exception());
            } catch (...) {
            }
        }
    });
    return fut.get();
}

bool AlertManager::persistDeleteSync(const std::string &id) {
    if (!postgres_) return false;
    if (!dbExecutor_) return postgres_->deleteAlert(id);
    auto prom = std::make_shared<std::promise<bool>>();
    auto fut = prom->get_future();
    dbExecutor_([this, id, prom]() {
        try {
            prom->set_value(postgres_->deleteAlert(id));
        } catch (...) {
            try {
                prom->set_exception(std::current_exception());
            } catch (...) {
            }
        }
    });
    return fut.get();
}

void AlertManager::persistDelete(const std::string &id) {
    if (!postgres_) return;
    if (dbExecutor_) {
        dbExecutor_([this, id]() {
            if (!postgres_->deleteAlert(id)) {
                LOG_ERROR << "deleteAlert failed (async) alert_id=" << id;
            }
        });
    } else if (!postgres_->deleteAlert(id)) {
        LOG_ERROR << "deleteAlert failed (async) alert_id=" << id;
    }
}

int AlertManager::intervalSeconds(const std::string &interval) {
    return util::intervalToSeconds(interval);
}

Alert AlertManager::createPriceAlert(const std::string &pair, double targetPrice,
                                     const std::string &condition,
                                     const std::string &userId, const std::string &email,
                                     const std::vector<std::string> &channels,
                                     const std::string &phone,
                                     const std::string &customMessage) {
    if (!postgres_) throw std::runtime_error("Database unavailable");
    Alert a;
    a.id = newUuid();
    a.userId = userId;
    std::string canon = util::canonicalPair(pair);
    a.pair = canon.empty() ? pair : canon;
    a.alertType = "price";
    a.targetPrice = targetPrice;
    a.condition = condition;
    a.email = email;
    a.channels = channels;
    a.normalizeChannels();
    a.phone = phone;
    a.customMessage = customMessage;
    a.status = "active";
    a.createdAt = util::nowIso8601();
    {
        std::lock_guard<std::mutex> lk(mu_);
        alerts_[a.id] = a;
        rebuildIndexes();
    }
    if (!persistAlertSync(a)) {
        std::lock_guard<std::mutex> lk(mu_);
        alerts_.erase(a.id);
        rebuildIndexes();
        throw std::runtime_error("Alert not persisted");
    }
    bumpUserRevision(a.userId);
    notifySubscriptionChange();
    LOG_INFO << "Created price alert " << a.id << " " << a.pair << " @ " << targetPrice;
    return a;
}

Alert AlertManager::createCandleAlert(const std::string &pair, const std::string &interval,
                                      const std::string &direction, double threshold,
                                      const std::string &userId, const std::string &email,
                                      const std::vector<std::string> &channels,
                                      const std::string &phone,
                                      const std::string &customMessage) {
    if (!postgres_) throw std::runtime_error("Database unavailable");
    std::string iv = interval;
    std::transform(iv.begin(), iv.end(), iv.begin(), ::tolower);
    if (intervalSeconds(iv) == 0)
        throw std::invalid_argument("Invalid interval. Must be one of: 1m, 5m, 15m, 30m, 1h, 4h, 1d");

    Alert a;
    a.id = newUuid();
    a.userId = userId;
    std::string canon = util::canonicalPair(pair);
    a.pair = canon.empty() ? pair : canon;
    a.alertType = "candle_close";
    a.interval = iv;
    a.direction = direction;
    a.threshold = threshold;
    a.email = email;
    a.channels = channels;
    a.normalizeChannels();
    a.phone = phone;
    a.customMessage = customMessage;
    a.status = "active";
    a.createdAt = util::nowIso8601();
    {
        std::lock_guard<std::mutex> lk(mu_);
        alerts_[a.id] = a;
        rebuildIndexes();
    }
    if (!persistAlertSync(a)) {
        std::lock_guard<std::mutex> lk(mu_);
        alerts_.erase(a.id);
        rebuildIndexes();
        throw std::runtime_error("Alert not persisted");
    }
    bumpUserRevision(a.userId);
    notifySubscriptionChange();
    LOG_INFO << "Created candle alert " << a.id << " " << a.pair << " " << iv << " "
             << direction << " " << threshold;
    return a;
}

std::optional<Alert> AlertManager::getAlert(const std::string &id) const {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = alerts_.find(id);
    if (it == alerts_.end()) return std::nullopt;
    return it->second;
}

std::vector<Alert> AlertManager::getAllAlerts() const {
    std::lock_guard<std::mutex> lk(mu_);
    std::vector<Alert> out;
    for (const auto &kv : alerts_) out.push_back(kv.second);
    return out;
}

std::vector<Alert> AlertManager::getActiveAlerts() const {
    std::lock_guard<std::mutex> lk(mu_);
    std::vector<Alert> out;
    for (const auto &kv : alerts_)
        if (kv.second.status == "active") out.push_back(kv.second);
    return out;
}

std::vector<Alert> AlertManager::getAllAlertsForUser(const std::string &userId) const {
    std::lock_guard<std::mutex> lk(mu_);
    std::vector<Alert> out;
    for (const auto &kv : alerts_)
        if (kv.second.userId == userId) out.push_back(kv.second);
    return out;
}

std::vector<Alert> AlertManager::getActiveAlertsForUser(const std::string &userId) const {
    std::lock_guard<std::mutex> lk(mu_);
    std::vector<Alert> out;
    for (const auto &kv : alerts_)
        if (kv.second.status == "active" && kv.second.userId == userId)
            out.push_back(kv.second);
    return out;
}

std::vector<Alert> AlertManager::getActiveAlertsSortedForUser(const std::string &userId) const {
    auto out = getActiveAlertsForUser(userId);
    std::sort(out.begin(), out.end(), [](const Alert &a, const Alert &b) {
        if (a.createdAt != b.createdAt) return a.createdAt > b.createdAt;
        return a.id > b.id;
    });
    return out;
}

bool AlertManager::isAlertOwnedBy(const std::string &id, const std::string &userId) const {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = alerts_.find(id);
    return it != alerts_.end() && it->second.userId == userId;
}

bool AlertManager::deleteAlert(const std::string &id,
                               const std::optional<std::string> &userId) {
    if (!postgres_) return false;
    std::string affectedUser;
    Alert previous;
    {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = alerts_.find(id);
        if (it == alerts_.end()) return false;
        if (userId && it->second.userId != *userId) return false;
        affectedUser = it->second.userId;
        previous = it->second;
        alerts_.erase(it);
        rebuildIndexes();
    }
    if (!persistDeleteSync(id)) {
        std::lock_guard<std::mutex> lk(mu_);
        alerts_[id] = previous;
        rebuildIndexes();
        LOG_ERROR << "Failed to persist delete for alert " << id;
        return false;
    }
    bumpUserRevision(affectedUser);
    notifySubscriptionChange();
    LOG_INFO << "Deleted alert " << id;
    return true;
}

std::optional<Alert> AlertManager::updateAlert(const std::string &id,
                                               const Json::Value &updates,
                                               const std::optional<std::string> &userId) {
    if (!postgres_) throw std::runtime_error("Database unavailable");
    Alert updated;
    Alert previous;
    {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = alerts_.find(id);
        if (it == alerts_.end()) return std::nullopt;
        if (userId && it->second.userId != *userId) return std::nullopt;
        previous = it->second;
        Alert &a = it->second;

        auto setStr = [&](const char *k, std::string &dst) {
            if (updates.isMember(k) && updates[k].isString()) dst = updates[k].asString();
        };
        if (a.alertType == "price") {
            if (updates.isMember("target_price") && updates["target_price"].isNumeric())
                a.targetPrice = updates["target_price"].asDouble();
            if (updates.isMember("condition") && updates["condition"].isString())
                a.condition = updates["condition"].asString();
            if (updates.isMember("channels") && updates["channels"].isArray()) {
                a.channels.clear();
                for (const auto &c : updates["channels"]) {
                    if (c.isString() && !c.asString().empty()) a.channels.push_back(c.asString());
                }
                a.normalizeChannels();
            } else {
                setStr("channel", a.channel);
                a.normalizeChannels();
            }
            setStr("email", a.email);
            setStr("phone", a.phone);
            setStr("custom_message", a.customMessage);
            setStr("status", a.status);
        } else if (a.alertType == "candle_close") {
            if (updates.isMember("interval") && updates["interval"].isString()) {
                std::string iv = updates["interval"].asString();
                std::transform(iv.begin(), iv.end(), iv.begin(), ::tolower);
                if (intervalSeconds(iv) == 0)
                    throw std::invalid_argument(
                        "Invalid interval. Must be one of: 1m, 5m, 15m, 30m, 1h, 4h, 1d");
                a.interval = iv;
            }
            if (updates.isMember("direction") && updates["direction"].isString())
                a.direction = updates["direction"].asString();
            if (updates.isMember("threshold") && updates["threshold"].isNumeric())
                a.threshold = updates["threshold"].asDouble();
            if (updates.isMember("channels") && updates["channels"].isArray()) {
                a.channels.clear();
                for (const auto &c : updates["channels"]) {
                    if (c.isString() && !c.asString().empty()) a.channels.push_back(c.asString());
                }
                a.normalizeChannels();
            } else {
                setStr("channel", a.channel);
                a.normalizeChannels();
            }
            setStr("email", a.email);
            setStr("phone", a.phone);
            setStr("custom_message", a.customMessage);
            setStr("status", a.status);
        }
        updated = a;
        rebuildIndexes();
    }
    if (!persistAlertSync(updated)) {
        std::lock_guard<std::mutex> lk(mu_);
        alerts_[id] = previous;
        rebuildIndexes();
        throw std::runtime_error("Alert not persisted");
    }
    bumpUserRevision(updated.userId);
    notifySubscriptionChange();
    LOG_INFO << "Updated alert " << id;
    return updated;
}

void AlertManager::triggerAlert(Alert &a, double price) {
    a.status = "triggered";
    a.triggeredAt = util::nowIso8601();
    a.lastCheckedPrice = price;
    a.closePrice = price;
}

bool AlertManager::priceConditionMet(const Alert &a, double current) {
    if (a.alertType != "price") return false;
    std::string cond = a.condition.value_or("");
    double target = a.targetPrice.value_or(0);
    if (cond == "above") return current >= target;
    if (cond == "below") return current <= target;
    if (cond == "equal") return std::fabs(current - target) <= 0.0001;
    return false;
}

std::optional<TriggeredAlert> AlertManager::tryTriggerPriceAlert(const std::string &alertId,
                                                                  double currentPrice) {
    if (!postgres_) return std::nullopt;
    std::optional<TriggeredAlert> result;
    Alert persisted;
    Alert previous;
    {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = alerts_.find(alertId);
        if (it == alerts_.end()) return std::nullopt;
        Alert &a = it->second;
        if (a.status != "active" || a.alertType != "price") return std::nullopt;
        a.lastCheckedPrice = currentPrice;
        if (!priceConditionMet(a, currentPrice)) return std::nullopt;
        previous = a;
        triggerAlert(a, currentPrice);
        TriggeredAlert t;
        t.alert = a;
        t.currentPrice = currentPrice;
        t.alertTypeLabel = "price";
        result = t;
        persisted = a;
        rebuildIndexes();
    }
    if (!persistAlertSync(persisted)) {
        std::lock_guard<std::mutex> lk(mu_);
        alerts_[alertId] = previous;
        rebuildIndexes();
        LOG_ERROR << "Failed to persist triggered price alert " << alertId;
        return std::nullopt;
    }
    bumpUserRevision(persisted.userId);
    LOG_INFO << "Triggered price alert " << persisted.id << " " << persisted.pair
             << " channel=" << persisted.channel << " price=" << currentPrice
             << " target=" << persisted.targetPrice.value_or(0);
    if (onTriggered_) onTriggered_(*result);
    return result;
}

std::vector<TriggeredAlert> AlertManager::checkPriceAlerts(
    const std::vector<market::FlatPair> &pairs) {
    std::vector<TriggeredAlert> triggered;
    std::unordered_map<std::string, double> prices;
    for (const auto &p : pairs) {
        if (!p.hasPrice) continue;
        prices[util::canonicalPair(p.pair)] = p.price;
    }

    struct PersistBatch {
        Alert before;
        Alert after;
    };
    std::vector<PersistBatch> toPersist;
    {
        std::lock_guard<std::mutex> lk(mu_);
        for (const auto &pr : prices) {
            auto idxIt = activePriceIndex_.find(pr.first);
            if (idxIt == activePriceIndex_.end()) continue;
            double current = pr.second;
            for (const auto &alertId : idxIt->second) {
                auto it = alerts_.find(alertId);
                if (it == alerts_.end()) continue;
                Alert &a = it->second;
                if (a.status != "active" || a.alertType != "price") continue;
                a.lastCheckedPrice = current;
                if (!priceConditionMet(a, current)) continue;
                Alert before = a;
                triggerAlert(a, current);
                TriggeredAlert t;
                t.alert = a;
                t.currentPrice = current;
                t.alertTypeLabel = "price";
                triggered.push_back(t);
                toPersist.push_back({before, a});
                LOG_INFO << "Triggered price alert " << a.id << " " << a.pair
                         << " channel=" << a.channel << " price=" << current
                         << " target=" << a.targetPrice.value_or(0);
            }
        }
        if (!triggered.empty()) rebuildIndexes();
    }
    std::vector<TriggeredAlert> notified;
    for (const auto &batch : toPersist) {
        if (postgres_ && !persistAlertSync(batch.after)) {
            std::lock_guard<std::mutex> lk(mu_);
            alerts_[batch.after.id] = batch.before;
            rebuildIndexes();
            LOG_ERROR << "Failed to persist triggered price alert " << batch.after.id;
            continue;
        }
        bumpUserRevision(batch.after.userId);
        for (const auto &t : triggered) {
            if (t.alert.id == batch.after.id) notified.push_back(t);
        }
    }
    for (const auto &t : notified) {
        if (onTriggered_) onTriggered_(t);
    }
    return notified;
}

std::vector<TriggeredAlert> AlertManager::checkCandleAlerts(
    const std::vector<Json::Value> &candles) {
    std::vector<TriggeredAlert> triggered;

    // Build lookup (pair, interval) -> candle.
    struct Key {
        std::string pair, interval;
        bool operator==(const Key &o) const { return pair == o.pair && interval == o.interval; }
    };
    std::vector<std::pair<Key, Json::Value>> lookup;
    auto find = [&](const Key &k) -> const Json::Value * {
        for (auto &e : lookup)
            if (e.first == k) return &e.second;
        return nullptr;
    };
    for (const auto &c : candles) {
        std::string iv = c.get("interval", "").asString();
        std::transform(iv.begin(), iv.end(), iv.begin(), ::tolower);
        Key k{util::canonicalPair(c.get("pair", "").asString()), iv};
        if (!find(k)) lookup.emplace_back(k, c);
    }

    struct PersistBatch {
        Alert before;
        Alert after;
        bool isTrigger = false;
    };
    std::vector<PersistBatch> toPersist;
    {
        std::lock_guard<std::mutex> lk(mu_);
        for (const auto &entry : lookup) {
            const Key &k = entry.first;
            const Json::Value &candle = entry.second;
            auto idxIt = activeCandleIndex_.find(candleIndexKey(k.pair, k.interval));
            if (idxIt == activeCandleIndex_.end()) continue;
            for (const auto &alertId : idxIt->second) {
            auto it = alerts_.find(alertId);
            if (it == alerts_.end()) continue;
            Alert &a = it->second;
            if (a.status != "active" || a.alertType != "candle_close") continue;
            std::string iv = a.interval.value_or("");
            std::transform(iv.begin(), iv.end(), iv.begin(), ::tolower);
            if (util::canonicalPair(a.pair) != k.pair || iv != k.interval) continue;

            double close = candle.get("close", 0.0).asDouble();
            Json::Value tsVal = candle["timestamp"];
            std::string candleTsStr =
                tsVal.isString() ? tsVal.asString() : std::to_string(tsVal.asInt64());

            auto candleStart = parseCandleTs(tsVal);
            int ivSec = intervalSeconds(iv);
            auto createdAt = util::parseIso8601(a.createdAt);
            if (candleStart && ivSec && createdAt) {
                std::time_t closeTime = *candleStart + ivSec;
                if (closeTime <= *createdAt) {
                    Alert before = a;
                    a.lastEvaluatedCandleTime = candleTsStr;
                    toPersist.push_back({before, a, false});
                    continue;
                }
            }
            if (a.lastEvaluatedCandleTime && *a.lastEvaluatedCandleTime == candleTsStr)
                continue;

            bool should = false;
            std::string dir = a.direction.value_or("");
            double thr = a.threshold.value_or(0);
            if (dir == "above" && close >= thr)
                should = true;
            else if (dir == "below" && close <= thr)
                should = true;
            if (should) {
                Alert before = a;
                a.status = "triggered";
                a.triggeredAt = util::nowIso8601();
                a.lastCheckedPrice = close;
                a.closePrice = close;
                a.lastEvaluatedCandleTime = candleTsStr;
                TriggeredAlert t;
                t.alert = a;
                t.currentPrice = close;
                t.alertTypeLabel = "candle_close";
                t.timeframe = iv;
                triggered.push_back(t);
                toPersist.push_back({before, a, true});
                LOG_INFO << "Triggered candle alert " << a.id << " " << a.pair
                         << " channel=" << a.channel << " close=" << close;
            }
            }
        }
        if (!triggered.empty()) rebuildIndexes();
    }
    std::vector<TriggeredAlert> notified;
    for (const auto &batch : toPersist) {
        if (!postgres_) continue;
        if (batch.isTrigger) {
            if (!persistAlertSync(batch.after)) {
                std::lock_guard<std::mutex> lk(mu_);
                alerts_[batch.after.id] = batch.before;
                rebuildIndexes();
                LOG_ERROR << "Failed to persist triggered candle alert " << batch.after.id;
                continue;
            }
            bumpUserRevision(batch.after.userId);
            for (const auto &t : triggered) {
                if (t.alert.id == batch.after.id) notified.push_back(t);
            }
        } else {
            persistAlert(batch.after);
        }
    }
    for (const auto &t : notified) {
        if (onTriggered_) onTriggered_(t);
    }
    return notified;
}

int AlertManager::flushPersistenceEvents(int batchSize) {
    if (!redis_ || !redis_->connected() || !postgres_) return 0;
    int applied = 0;
    redis_->readJsonQueue(
        redisAlertQueueKey_, std::max(1, batchSize),
        [this, &applied](std::vector<std::string> batch) {
            for (const auto &js : batch) {
                Json::Value ev;
                Json::CharReaderBuilder b;
                std::unique_ptr<Json::CharReader> reader(b.newCharReader());
                std::string errs;
                reader->parse(js.c_str(), js.c_str() + js.size(), &ev, &errs);
                std::string op = ev.get("op", "").asString();
                if (op == "upsert" && ev.isMember("alert"))
                    postgres_->upsertAlert(ev["alert"]);
                else if (op == "delete")
                    postgres_->deleteAlert(ev.get("alert_id", "").asString());
                ++applied;
            }
        });
    return applied;
}

}  // namespace ctraderplus::alerts
