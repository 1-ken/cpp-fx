#include "alerts/AlertManager.h"

#include <algorithm>
#include <chrono>
#include <cmath>
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
        LOG_WARN << "AlertManager started without PostgreSQL; alerts stay in-memory only";
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
}

void AlertManager::rebuildIndexes() {
    activePriceIndex_.clear();
    for (const auto &kv : alerts_) {
        const Alert &a = kv.second;
        if (a.status != "active" || a.alertType != "price") continue;
        std::string key = util::canonicalPair(a.pair);
        if (key.empty()) continue;
        activePriceIndex_[key].push_back(a.id);
    }
}

void AlertManager::persistAlert(const Alert &a) {
    Json::Value payload(Json::objectValue);
    payload["event_id"] = newUuid();
    payload["event_ts"] = util::nowIso8601();
    payload["op"] = "upsert";
    payload["alert_id"] = a.id;
    payload["alert"] = a.toJson();
    if (redis_ && redis_->connected()) {
        Json::StreamWriterBuilder wb;
        wb["indentation"] = "";
        redis_->pushJson(redisAlertQueueKey_, Json::writeString(wb, payload));
        return;
    }
    if (postgres_ && dbExecutor_) {
        Json::Value alertJson = a.toJson();
        dbExecutor_([this, alertJson]() { postgres_->upsertAlert(alertJson); });
    }
}

void AlertManager::persistDelete(const std::string &id) {
    if (redis_ && redis_->connected()) {
        Json::Value payload(Json::objectValue);
        payload["event_id"] = newUuid();
        payload["event_ts"] = util::nowIso8601();
        payload["op"] = "delete";
        payload["alert_id"] = id;
        Json::StreamWriterBuilder wb;
        wb["indentation"] = "";
        redis_->pushJson(redisAlertQueueKey_, Json::writeString(wb, payload));
        return;
    }
    if (postgres_ && dbExecutor_) {
        dbExecutor_([this, id]() { postgres_->deleteAlert(id); });
    }
}

int AlertManager::intervalSeconds(const std::string &interval) {
    return util::intervalToSeconds(interval);
}

Alert AlertManager::createPriceAlert(const std::string &pair, double targetPrice,
                                     const std::string &condition,
                                     const std::string &userId, const std::string &email,
                                     const std::string &channel, const std::string &phone,
                                     const std::string &customMessage) {
    Alert a;
    a.id = newUuid();
    a.userId = userId;
    std::string canon = util::canonicalPair(pair);
    a.pair = canon.empty() ? pair : canon;
    a.alertType = "price";
    a.targetPrice = targetPrice;
    a.condition = condition;
    a.email = email;
    a.channel = channel;
    a.phone = phone;
    a.customMessage = customMessage;
    a.status = "active";
    a.createdAt = util::nowIso8601();
    {
        std::lock_guard<std::mutex> lk(mu_);
        alerts_[a.id] = a;
        rebuildIndexes();
    }
    persistAlert(a);
    LOG_INFO << "Created price alert " << a.id << " " << a.pair << " @ " << targetPrice;
    return a;
}

Alert AlertManager::createCandleAlert(const std::string &pair, const std::string &interval,
                                      const std::string &direction, double threshold,
                                      const std::string &userId, const std::string &email,
                                      const std::string &channel, const std::string &phone,
                                      const std::string &customMessage) {
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
    a.channel = channel;
    a.phone = phone;
    a.customMessage = customMessage;
    a.status = "active";
    a.createdAt = util::nowIso8601();
    {
        std::lock_guard<std::mutex> lk(mu_);
        alerts_[a.id] = a;
        rebuildIndexes();
    }
    persistAlert(a);
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
    {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = alerts_.find(id);
        if (it == alerts_.end()) return false;
        if (userId && it->second.userId != *userId) return false;
        alerts_.erase(it);
        rebuildIndexes();
    }
    persistDelete(id);
    LOG_INFO << "Deleted alert " << id;
    return true;
}

std::optional<Alert> AlertManager::updateAlert(const std::string &id,
                                               const Json::Value &updates,
                                               const std::optional<std::string> &userId) {
    Alert updated;
    {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = alerts_.find(id);
        if (it == alerts_.end()) return std::nullopt;
        if (userId && it->second.userId != *userId) return std::nullopt;
        Alert &a = it->second;

        auto setStr = [&](const char *k, std::string &dst) {
            if (updates.isMember(k) && updates[k].isString()) dst = updates[k].asString();
        };
        if (a.alertType == "price") {
            if (updates.isMember("target_price") && updates["target_price"].isNumeric())
                a.targetPrice = updates["target_price"].asDouble();
            if (updates.isMember("condition") && updates["condition"].isString())
                a.condition = updates["condition"].asString();
            setStr("channel", a.channel);
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
            setStr("channel", a.channel);
            setStr("email", a.email);
            setStr("phone", a.phone);
            setStr("custom_message", a.customMessage);
            setStr("status", a.status);
        }
        updated = a;
        rebuildIndexes();
    }
    persistAlert(updated);
    LOG_INFO << "Updated alert " << id;
    return updated;
}

void AlertManager::triggerAlert(Alert &a, double price) {
    a.status = "triggered";
    a.triggeredAt = util::nowIso8601();
    a.lastCheckedPrice = price;
    a.closePrice = price;
}

std::vector<TriggeredAlert> AlertManager::checkPriceAlerts(
    const std::vector<market::FlatPair> &pairs) {
    std::vector<TriggeredAlert> triggered;
    std::unordered_map<std::string, double> prices;
    for (const auto &p : pairs) {
        if (!p.hasPrice) continue;
        prices[util::canonicalPair(p.pair)] = p.price;
    }

    std::vector<Alert> toPersist;
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
                bool should = false;
                std::string cond = a.condition.value_or("");
                double target = a.targetPrice.value_or(0);
                if (cond == "above" && current >= target)
                    should = true;
                else if (cond == "below" && current <= target)
                    should = true;
                else if (cond == "equal" && std::fabs(current - target) <= 0.0001)
                    should = true;
                if (should) {
                    triggerAlert(a, current);
                    TriggeredAlert t;
                    t.alert = a;
                    t.currentPrice = current;
                    t.alertTypeLabel = "price";
                    triggered.push_back(t);
                    toPersist.push_back(a);
                }
            }
        }
        if (!triggered.empty()) rebuildIndexes();
    }
    for (auto &a : toPersist) persistAlert(a);
    return triggered;
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

    std::vector<Alert> toPersist;
    {
        std::lock_guard<std::mutex> lk(mu_);
        for (auto &kv : alerts_) {
            Alert &a = kv.second;
            if (a.status != "active" || a.alertType != "candle_close") continue;
            std::string iv = a.interval.value_or("");
            std::transform(iv.begin(), iv.end(), iv.begin(), ::tolower);
            Key k{util::canonicalPair(a.pair), iv};
            const Json::Value *candle = find(k);
            if (!candle) continue;

            double close = (*candle).get("close", 0.0).asDouble();
            Json::Value tsVal = (*candle)["timestamp"];
            std::string candleTsStr =
                tsVal.isString() ? tsVal.asString() : std::to_string(tsVal.asInt64());

            auto candleStart = parseCandleTs(tsVal);
            int ivSec = intervalSeconds(iv);
            auto createdAt = util::parseIso8601(a.createdAt);
            if (candleStart && ivSec && createdAt) {
                std::time_t closeTime = *candleStart + ivSec;
                if (closeTime <= *createdAt) {
                    a.lastEvaluatedCandleTime = candleTsStr;
                    toPersist.push_back(a);
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
                toPersist.push_back(a);
            }
        }
        if (!triggered.empty()) rebuildIndexes();
    }
    for (auto &a : toPersist) persistAlert(a);
    return triggered;
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
