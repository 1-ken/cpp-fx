#include "alerts/AlertManager.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <ctime>
#include <future>
#include <memory>
#include <random>
#include <thread>

#include <trantor/utils/Logger.h>

#include "ctrader/CTraderClient.h"
#include "ctrader/SymbolRegistry.h"
#include "market/MarketHub.h"
#include "market/PrevDayLevelProvider.h"
#include "market/StructureEngine.h"
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

bool isPastExpiry(const Alert &a, std::time_t now = std::time(nullptr)) {
    if (!a.expiresAt || a.expiresAt->empty()) return false;
    auto t = util::parseIso8601(*a.expiresAt);
    return t.has_value() && *t <= now;
}

bool dolPriceTriggered(const Alert &a, const market::DayLevels &lv, double price) {
    const std::string ref = a.levelRef.value_or("both");
    if (a.hasDolTrigger("draw_met")) {
        if (lv.draw == "high" && price >= lv.pdh) return true;
        if (lv.draw == "low" && price <= lv.pdl) return true;
    }
    if (a.hasDolTrigger("sweep")) {
        const bool hitHigh = price >= lv.pdh;
        const bool hitLow = price <= lv.pdl;
        if (ref == "high") return hitHigh;
        if (ref == "low") return hitLow;
        return hitHigh || hitLow;
    }
    return false;
}

bool dolCloseTriggered(const Alert &a, const std::string &outcome) {
    const std::string ref = a.levelRef.value_or("both");
    if (a.hasDolTrigger("displacement")) {
        if (outcome == "displaced_up" && (ref == "high" || ref == "both")) return true;
        if (outcome == "displaced_down" && (ref == "low" || ref == "both")) return true;
    }
    if (a.hasDolTrigger("reversal")) {
        if (outcome == "reversal_from_high" && (ref == "high" || ref == "both")) return true;
        if (outcome == "reversal_from_low" && (ref == "low" || ref == "both")) return true;
    }
    return false;
}

// First same-UTC-day 1m bar that traded through PDH and/or PDL per level_ref.
std::optional<std::pair<double, std::time_t>> firstSweepTouchToday(
    const Alert &a, const market::DayLevels &lv,
    const std::vector<ctrader::TrendbarData> &bars, int64_t dayStartMinutes) {
    const std::string ref = a.levelRef.value_or("both");
    std::vector<ctrader::TrendbarData> ordered = bars;
    std::sort(ordered.begin(), ordered.end(),
              [](const ctrader::TrendbarData &x, const ctrader::TrendbarData &y) {
                  return x.utcTimestampMinutes < y.utcTimestampMinutes;
              });
    for (const auto &b : ordered) {
        if (b.utcTimestampMinutes < dayStartMinutes) continue;
        const bool hitHigh = b.high >= lv.pdh;
        const bool hitLow = b.low <= lv.pdl;
        if (ref == "high") {
            if (!hitHigh) continue;
            return std::make_pair(lv.pdh, static_cast<std::time_t>(b.utcTimestampMinutes * 60));
        }
        if (ref == "low") {
            if (!hitLow) continue;
            return std::make_pair(lv.pdl, static_cast<std::time_t>(b.utcTimestampMinutes * 60));
        }
        // both: first bar that hit either; prefer PDH if same bar hits both
        if (hitHigh)
            return std::make_pair(lv.pdh, static_cast<std::time_t>(b.utcTimestampMinutes * 60));
        if (hitLow)
            return std::make_pair(lv.pdl, static_cast<std::time_t>(b.utcTimestampMinutes * 60));
    }
    return std::nullopt;
}

void invokeComplete(const std::function<void()> &onComplete) {
    if (onComplete) onComplete();
}

struct UtcYmd {
    int year = 0;
    int month = 0;
    int day = 0;
    bool operator<(const UtcYmd &o) const {
        if (year != o.year) return year < o.year;
        if (month != o.month) return month < o.month;
        return day < o.day;
    }
};

UtcYmd utcYmdFromEpoch(std::time_t t) {
    std::tm tm{};
    gmtime_r(&t, &tm);
    return UtcYmd{tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday};
}

std::optional<UtcYmd> utcYmdFromIso(const std::string &iso) {
    auto ts = util::parseIso8601(iso);
    if (!ts) return std::nullopt;
    return utcYmdFromEpoch(*ts);
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
    activeDolIndex_.clear();
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
        } else if (a.alertType == "market_structure" && a.interval) {
            std::string iv = *a.interval;
            std::transform(iv.begin(), iv.end(), iv.begin(), ::tolower);
            activeCandleIndex_[candleIndexKey(key, iv)].push_back(a.id);
        } else if (a.alertType == "prev_day_level") {
            // An alert can select both live and daily-close triggers.
            if (a.wantsDailyDolClose()) {
                activeCandleIndex_[candleIndexKey(key, "1d")].push_back(a.id);
            }
            if (a.wantsLiveDolPrice()) {
                activeDolIndex_[key].push_back(a.id);
            }
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
                                     const std::string &customMessage,
                                     const std::string &expiresAt,
                                     std::optional<std::string> dependsOnAlertId) {
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
    a.expiresAt = expiresAt;
    a.createdAt = util::nowIso8601();

    std::optional<Alert> parentPatch;
    {
        std::lock_guard<std::mutex> lk(mu_);
        parentPatch = applyDependsOnLocked(a, userId, dependsOnAlertId);
        if (parentPatch) alerts_[parentPatch->id] = *parentPatch;
        alerts_[a.id] = a;
        rebuildIndexes();
    }
    if (parentPatch && !persistAlertSync(*parentPatch)) {
        std::lock_guard<std::mutex> lk(mu_);
        alerts_.erase(a.id);
        rebuildIndexes();
        throw std::runtime_error("Alert not persisted");
    }
    if (!persistAlertSync(a)) {
        std::lock_guard<std::mutex> lk(mu_);
        alerts_.erase(a.id);
        rebuildIndexes();
        throw std::runtime_error("Alert not persisted");
    }
    bumpUserRevision(a.userId);
    notifySubscriptionChange();
    LOG_INFO << "Created price alert " << a.id << " " << a.pair << " @ " << targetPrice
             << " status=" << a.status;
    return a;
}

Alert AlertManager::createCandleAlert(const std::string &pair, const std::string &interval,
                                      const std::string &direction, double threshold,
                                      const std::string &userId, const std::string &email,
                                      const std::vector<std::string> &channels,
                                      const std::string &phone,
                                      const std::string &customMessage,
                                      const std::string &expiresAt,
                                      std::optional<std::string> dependsOnAlertId) {
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
    a.expiresAt = expiresAt;
    a.createdAt = util::nowIso8601();

    std::optional<Alert> parentPatch;
    {
        std::lock_guard<std::mutex> lk(mu_);
        parentPatch = applyDependsOnLocked(a, userId, dependsOnAlertId);
        if (parentPatch) alerts_[parentPatch->id] = *parentPatch;
        alerts_[a.id] = a;
        rebuildIndexes();
    }
    if (parentPatch && !persistAlertSync(*parentPatch)) {
        std::lock_guard<std::mutex> lk(mu_);
        alerts_.erase(a.id);
        rebuildIndexes();
        throw std::runtime_error("Alert not persisted");
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
             << direction << " " << threshold << " status=" << a.status;
    return a;
}

Alert AlertManager::createDrawAlert(const std::string &pair, const std::string &levelRef,
                                    const std::vector<std::string> &dolTriggers,
                                    const std::string &userId, const std::string &email,
                                    const std::vector<std::string> &channels,
                                    const std::string &phone,
                                    const std::string &customMessage,
                                    const std::optional<std::string> &batchId,
                                    const std::string &expiresAt,
                                    std::optional<std::string> dependsOnAlertId) {
    if (!postgres_) throw std::runtime_error("Database unavailable");
    if (levelRef != "high" && levelRef != "low" && levelRef != "both")
        throw std::invalid_argument("level_ref must be one of: high, low, both");
    if (dolTriggers.empty())
        throw std::invalid_argument("dol_trigger must include at least one trigger");
    std::vector<std::string> normalizedTriggers;
    for (const auto &raw : dolTriggers) {
        std::string t = raw;
        std::transform(t.begin(), t.end(), t.begin(), ::tolower);
        if (t != "sweep" && t != "displacement" && t != "reversal" && t != "draw_met")
            throw std::invalid_argument(
                "dol_trigger must be one of: sweep, displacement, reversal, draw_met");
        if (std::find(normalizedTriggers.begin(), normalizedTriggers.end(), t) ==
            normalizedTriggers.end())
            normalizedTriggers.push_back(t);
    }

    Alert a;
    a.id = newUuid();
    a.userId = userId;
    std::string canon = util::canonicalPair(pair);
    a.pair = canon.empty() ? pair : canon;
    a.alertType = "prev_day_level";
    a.levelRef = levelRef;
    a.dolTriggers = normalizedTriggers;
    a.batchId = batchId;
    a.email = email;
    a.channels = channels;
    a.normalizeChannels();
    a.phone = phone;
    a.customMessage = customMessage;
    a.expiresAt = expiresAt;
    a.createdAt = util::nowIso8601();

    std::optional<Alert> parentPatch;
    {
        std::lock_guard<std::mutex> lk(mu_);
        parentPatch = applyDependsOnLocked(a, userId, dependsOnAlertId);
        if (parentPatch) alerts_[parentPatch->id] = *parentPatch;
        alerts_[a.id] = a;
        rebuildIndexes();
    }
    if (parentPatch && !persistAlertSync(*parentPatch)) {
        std::lock_guard<std::mutex> lk(mu_);
        alerts_.erase(a.id);
        rebuildIndexes();
        throw std::runtime_error("Alert not persisted");
    }
    if (!persistAlertSync(a)) {
        std::lock_guard<std::mutex> lk(mu_);
        alerts_.erase(a.id);
        rebuildIndexes();
        throw std::runtime_error("Alert not persisted");
    }
    if (dolProvider_) dolProvider_->track(a.pair);
    bumpUserRevision(a.userId);
    notifySubscriptionChange();
    std::string trigLog;
    for (size_t i = 0; i < normalizedTriggers.size(); ++i) {
        if (i) trigLog += ",";
        trigLog += normalizedTriggers[i];
    }
    LOG_INFO << "Created draw-on-liquidity alert " << a.id << " " << a.pair << " "
             << levelRef << " " << trigLog << " status=" << a.status;

    // Sweep lookback only when the alert is immediately active and includes sweep.
    if (a.status == "active" && a.hasDolTrigger("sweep")) {
        {
            std::lock_guard<std::mutex> lk(mu_);
            sweepLookbackPending_[a.id] = true;
        }
        auto fence = std::make_shared<std::promise<void>>();
        auto fut = fence->get_future();
        scheduleSweepLookback(a.id, 0, [fence]() {
            try {
                fence->set_value();
            } catch (...) {
            }
        });
        fut.wait_for(std::chrono::milliseconds(2500));
        if (auto latest = getAlert(a.id)) return *latest;
    }
    return a;
}

Alert AlertManager::createStructureAlert(const std::string &pair, const std::string &interval,
                                         const std::vector<std::string> &structureEvents,
                                         const std::string &structureDirection,
                                         const std::string &userId, const std::string &email,
                                         const std::vector<std::string> &channels,
                                         const std::string &phone,
                                         const std::string &customMessage,
                                         const std::string &expiresAt,
                                         std::optional<double> minSwingAtr,
                                         std::optional<double> breakK,
                                         std::optional<std::string> dependsOnAlertId) {
    if (!postgres_) throw std::runtime_error("Database unavailable");
    std::string iv = interval;
    std::transform(iv.begin(), iv.end(), iv.begin(), ::tolower);
    if (intervalSeconds(iv) == 0)
        throw std::invalid_argument("Invalid interval. Must be one of: 1m, 5m, 15m, 30m, 1h, 4h, 1d");
    if (structureEvents.empty())
        throw std::invalid_argument("structure_event must include at least one event");
    std::vector<std::string> normalizedEvents;
    for (const auto &raw : structureEvents) {
        std::string ev = raw;
        std::transform(ev.begin(), ev.end(), ev.begin(), ::tolower);
        if (ev == "any") {
            normalizedEvents = {"bos", "choch", "sweep"};
            break;
        }
        if (ev != "bos" && ev != "choch" && ev != "sweep")
            throw std::invalid_argument("structure_event must be bos, choch, or sweep");
        if (std::find(normalizedEvents.begin(), normalizedEvents.end(), ev) ==
            normalizedEvents.end())
            normalizedEvents.push_back(ev);
    }
    std::string dir = structureDirection;
    std::transform(dir.begin(), dir.end(), dir.begin(), ::tolower);
    if (dir != "bull" && dir != "bear" && dir != "any")
        throw std::invalid_argument("structure_direction must be bull, bear, or any");

    Alert a;
    a.id = newUuid();
    a.userId = userId;
    std::string canon = util::canonicalPair(pair);
    a.pair = canon.empty() ? pair : canon;
    a.alertType = "market_structure";
    a.interval = iv;
    a.structureEvents = normalizedEvents;
    a.structureDirection = dir;
    a.minSwingAtr = minSwingAtr.value_or(0);
    a.breakK = breakK.value_or(0.25);
    a.email = email;
    a.channels = channels;
    a.normalizeChannels();
    a.phone = phone;
    a.customMessage = customMessage;
    a.expiresAt = expiresAt;
    a.createdAt = util::nowIso8601();

    std::optional<Alert> parentPatch;
    {
        std::lock_guard<std::mutex> lk(mu_);
        parentPatch = applyDependsOnLocked(a, userId, dependsOnAlertId);
        if (parentPatch) alerts_[parentPatch->id] = *parentPatch;
        alerts_[a.id] = a;
        rebuildIndexes();
    }
    if (parentPatch && !persistAlertSync(*parentPatch)) {
        std::lock_guard<std::mutex> lk(mu_);
        alerts_.erase(a.id);
        rebuildIndexes();
        throw std::runtime_error("Alert not persisted");
    }
    if (!persistAlertSync(a)) {
        std::lock_guard<std::mutex> lk(mu_);
        alerts_.erase(a.id);
        rebuildIndexes();
        throw std::runtime_error("Alert not persisted");
    }
    bumpUserRevision(a.userId);
    notifySubscriptionChange();
    std::string evLog;
    for (size_t i = 0; i < normalizedEvents.size(); ++i) {
        if (i) evLog += ",";
        evLog += normalizedEvents[i];
    }
    LOG_INFO << "Created structure alert " << a.id << " " << a.pair << " " << iv << " " << evLog
             << " " << dir << " status=" << a.status
             << (a.dependsOnAlertId ? (" depends_on=" + *a.dependsOnAlertId) : "");
    return a;
}

void AlertManager::ingestStructureHistory(const std::string &pair, const std::string &interval,
                                          const std::vector<Json::Value> &candles) {
    std::string iv = interval;
    std::transform(iv.begin(), iv.end(), iv.begin(), ::tolower);
    std::string key = candleIndexKey(util::canonicalPair(pair), iv);
    std::lock_guard<std::mutex> lk(mu_);
    auto &track = structureTracks_[key];
    track.candles.clear();
    for (const auto &c : candles) {
        market::StructureCandle bar;
        bar.timestamp = c.get("timestamp", "").asString();
        bar.open = c.get("open", 0.0).asDouble();
        bar.high = c.get("high", 0.0).asDouble();
        bar.low = c.get("low", 0.0).asDouble();
        bar.close = c.get("close", 0.0).asDouble();
        if (!bar.timestamp.empty()) track.candles.push_back(std::move(bar));
    }
    track.warmed = !track.candles.empty();
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

void AlertManager::triggerAlert(Alert &a, double price,
                                const std::optional<std::string> &triggeredAtIso) {
    a.status = "triggered";
    a.triggeredAt = triggeredAtIso.value_or(util::nowIso8601());
    a.lastCheckedPrice = price;
    a.closePrice = price;
}

std::optional<Alert> AlertManager::applyDependsOnLocked(
    Alert &a, const std::string &userId,
    const std::optional<std::string> &dependsOnAlertId) {
    if (!dependsOnAlertId || dependsOnAlertId->empty()) {
        a.chainId = newUuid();
        a.sequenceIndex = 0;
        a.status = "active";
        return std::nullopt;
    }
    auto pit = alerts_.find(*dependsOnAlertId);
    if (pit == alerts_.end())
        throw std::invalid_argument("depends_on_alert_id not found");
    Alert &parent = pit->second;
    if (parent.userId != userId)
        throw std::invalid_argument("depends_on_alert_id not found");
    if (util::canonicalPair(parent.pair) != util::canonicalPair(a.pair))
        throw std::invalid_argument("depends_on_alert_id must be the same pair");
    if (parent.status != "active" && parent.status != "waiting")
        throw std::invalid_argument(
            "depends_on_alert_id must be an active or waiting alert");

    std::optional<Alert> parentPatch;
    if (!parent.chainId || parent.chainId->empty()) {
        parent.chainId = newUuid();
        parent.sequenceIndex = 0;
        parentPatch = parent;
    }
    a.dependsOnAlertId = parent.id;
    a.chainId = parent.chainId;
    a.sequenceIndex = parent.sequenceIndex.value_or(0) + 1;
    a.status = "waiting";
    if (a.status != "waiting" || !a.dependsOnAlertId || *a.dependsOnAlertId != parent.id)
        throw std::runtime_error("depends_on_alert_id did not queue alert as waiting");
    return parentPatch;
}

std::vector<std::pair<Alert, Alert>> AlertManager::armDependentsLocked(
    const std::string &parentId, const std::optional<std::string> &skipCandleTs) {
    std::vector<std::pair<Alert, Alert>> out;
    for (auto &kv : alerts_) {
        Alert &next = kv.second;
        if (next.status != "waiting") continue;
        if (!next.dependsOnAlertId || *next.dependsOnAlertId != parentId) continue;
        Alert before = next;
        next.status = "active";
        next.requireUnmetSinceArm = true;
        if (skipCandleTs && !skipCandleTs->empty())
            next.lastEvaluatedCandleTime = *skipCandleTs;
        out.emplace_back(before, next);
        LOG_INFO << "Armed queue step " << next.id << " after " << parentId
                 << " (require unmet before trigger)";
    }
    return out;
}

void AlertManager::armDependentAlerts(const std::string &parentId,
                                      const std::optional<std::string> &skipCandleTs) {
    if (!postgres_) return;
    struct PersistBatch {
        Alert before;
        Alert after;
    };
    std::vector<PersistBatch> toPersist;
    {
        std::lock_guard<std::mutex> lk(mu_);
        for (auto &pair : armDependentsLocked(parentId, skipCandleTs)) {
            toPersist.push_back({pair.first, pair.second});
        }
        if (!toPersist.empty()) rebuildIndexes();
    }
    bool any = false;
    for (const auto &batch : toPersist) {
        if (!persistAlertSync(batch.after)) {
            std::lock_guard<std::mutex> lk(mu_);
            alerts_[batch.after.id] = batch.before;
            rebuildIndexes();
            LOG_ERROR << "Failed to persist armed queue alert " << batch.after.id;
            continue;
        }
        bumpUserRevision(batch.after.userId);
        any = true;
    }
    if (any) notifySubscriptionChange();
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
        if (isPastExpiry(a)) {
            previous = a;
            a.status = "expired";
            persisted = a;
            rebuildIndexes();
            result = std::nullopt;
        } else {
            a.lastCheckedPrice = currentPrice;
            const bool met = priceConditionMet(a, currentPrice);
            if (a.requireUnmetSinceArm) {
                if (met) return std::nullopt;
                previous = a;
                a.requireUnmetSinceArm = false;
                persisted = a;
                rebuildIndexes();
                result = std::nullopt;
                // Fall through to persist the cleared gate without triggering.
            } else if (!met) {
                return std::nullopt;
            } else {
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
        }
    }
    if (!persistAlertSync(persisted)) {
        std::lock_guard<std::mutex> lk(mu_);
        alerts_[alertId] = previous;
        rebuildIndexes();
        LOG_ERROR << "Failed to persist alert " << alertId;
        return std::nullopt;
    }
    bumpUserRevision(persisted.userId);
    if (!result) {
        if (persisted.status == "expired") {
            LOG_INFO << "Expired price alert " << persisted.id << " " << persisted.pair
                     << " (past expires_at)";
        }
        return std::nullopt;
    }
    LOG_INFO << "Triggered price alert " << persisted.id << " " << persisted.pair
             << " channel=" << persisted.channel << " price=" << currentPrice
             << " target=" << persisted.targetPrice.value_or(0);
    if (onTriggered_) onTriggered_(*result);
    armDependentAlerts(persisted.id);
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
                if (isPastExpiry(a)) {
                    Alert before = a;
                    a.status = "expired";
                    toPersist.push_back({before, a});
                    LOG_INFO << "Expired price alert " << a.id << " " << a.pair
                             << " (past expires_at)";
                    continue;
                }
                a.lastCheckedPrice = current;
                const bool met = priceConditionMet(a, current);
                if (a.requireUnmetSinceArm) {
                    if (met) continue;
                    Alert before = a;
                    a.requireUnmetSinceArm = false;
                    toPersist.push_back({before, a});
                    continue;
                }
                if (!met) continue;
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
                for (auto &armed : armDependentsLocked(a.id, std::nullopt)) {
                    toPersist.push_back({armed.first, armed.second});
                }
            }
        }
        if (dolProvider_) {
            for (const auto &pr : prices) {
                auto dIt = activeDolIndex_.find(pr.first);
                if (dIt == activeDolIndex_.end()) continue;
                auto levels = dolProvider_->currentLevels(pr.first);
                if (!levels.valid) continue;
                double current = pr.second;
                for (const auto &alertId : dIt->second) {
                    auto it = alerts_.find(alertId);
                    if (it == alerts_.end()) continue;
                    Alert &a = it->second;
                    if (a.status != "active" || a.alertType != "prev_day_level") continue;
                    if (isPastExpiry(a)) {
                        Alert before = a;
                        a.status = "expired";
                        toPersist.push_back({before, a});
                        LOG_INFO << "Expired draw alert " << a.id << " " << a.pair
                                 << " (past expires_at)";
                        continue;
                    }
                    if (sweepLookbackPending_.count(a.id)) continue;
                    a.lastCheckedPrice = current;
                    const bool met = dolPriceTriggered(a, levels, current);
                    if (a.requireUnmetSinceArm) {
                        if (met) continue;
                        Alert before = a;
                        a.requireUnmetSinceArm = false;
                        toPersist.push_back({before, a});
                        continue;
                    }
                    if (!met) continue;
                    Alert before = a;
                    triggerAlert(a, current);
                    TriggeredAlert t;
                    t.alert = a;
                    t.currentPrice = current;
                    t.alertTypeLabel = "prev_day_level";
                    t.timeframe = "1d";
                    triggered.push_back(t);
                    toPersist.push_back({before, a});
                    LOG_INFO << "Triggered draw alert " << a.id << " " << a.pair
                             << " trigger=" << (a.dolTriggers.empty() ? "sweep" : a.dolTriggers.front())
                             << " price=" << current;
                    for (auto &armed : armDependentsLocked(a.id, std::nullopt)) {
                        toPersist.push_back({armed.first, armed.second});
                    }
                }
            }
        }
        if (!triggered.empty() || !toPersist.empty()) rebuildIndexes();
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
            if (a.status != "active") continue;
            if (isPastExpiry(a)) {
                Alert before = a;
                a.status = "expired";
                toPersist.push_back({before, a, true});
                LOG_INFO << "Expired alert " << a.id << " " << a.pair << " (past expires_at)";
                continue;
            }
            const bool isCandleClose = a.alertType == "candle_close";
            const bool isDol = a.alertType == "prev_day_level";
            if (!isCandleClose && !isDol) continue;
            std::string iv = isCandleClose ? a.interval.value_or("") : std::string("1d");
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
            const char *typeLabel = "candle_close";
            if (isCandleClose) {
                std::string dir = a.direction.value_or("");
                double thr = a.threshold.value_or(0);
                if (dir == "above" && close >= thr)
                    should = true;
                else if (dir == "below" && close <= thr)
                    should = true;
            } else {
                typeLabel = "prev_day_level";
                if (!dolProvider_ || !candleStart) continue;
                double high = candle.get("high", close).asDouble();
                double low = candle.get("low", close).asDouble();
                auto cls = dolProvider_->classifyClose(k.pair, *candleStart, high, low, close);
                if (!cls.valid) continue;  // levels not warm yet; retry on next emit
                should = dolCloseTriggered(a, cls.outcome);
                if (!should) {
                    Alert before = a;
                    a.lastEvaluatedCandleTime = candleTsStr;
                    toPersist.push_back({before, a, false});
                    continue;
                }
            }
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
                t.alertTypeLabel = typeLabel;
                t.timeframe = iv;
                triggered.push_back(t);
                toPersist.push_back({before, a, true});
                LOG_INFO << "Triggered " << typeLabel << " alert " << a.id << " " << a.pair
                         << " channel=" << a.channel << " close=" << close;
                for (auto &armed : armDependentsLocked(a.id, candleTsStr)) {
                    toPersist.push_back({armed.first, armed.second, true});
                }
            }
            }

            Json::Value tsVal = candle["timestamp"];
            std::string candleTsStr =
                tsVal.isString() ? tsVal.asString() : std::to_string(tsVal.asInt64());
            std::string trackKey = candleIndexKey(k.pair, k.interval);
            auto &track = structureTracks_[trackKey];
            if (track.candles.empty() || track.candles.back().timestamp != candleTsStr) {
                market::StructureCandle bar;
                bar.timestamp = candleTsStr;
                bar.open = candle.get("open", 0.0).asDouble();
                bar.high = candle.get("high", 0.0).asDouble();
                bar.low = candle.get("low", 0.0).asDouble();
                bar.close = candle.get("close", 0.0).asDouble();
                track.candles.push_back(std::move(bar));
            }
            auto idxItStruct = activeCandleIndex_.find(trackKey);
            if (idxItStruct != activeCandleIndex_.end() && track.candles.size() >= 5) {
                auto candleStart = parseCandleTs(tsVal);
                for (const auto &alertId : idxItStruct->second) {
                    auto it = alerts_.find(alertId);
                    if (it == alerts_.end()) continue;
                    Alert &a = it->second;
                    if (a.status != "active" || a.alertType != "market_structure") continue;
                    if (isPastExpiry(a)) {
                        Alert before = a;
                        a.status = "expired";
                        toPersist.push_back({before, a, true});
                        LOG_INFO << "Expired structure alert " << a.id << " " << a.pair
                                 << " (past expires_at)";
                        continue;
                    }
                    std::string iv = a.interval.value_or("");
                    std::transform(iv.begin(), iv.end(), iv.begin(), ::tolower);
                    if (iv != k.interval) continue;
                    if (a.lastEvaluatedCandleTime && *a.lastEvaluatedCandleTime == candleTsStr)
                        continue;
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
                    market::StructureOptions opt;
                    opt.minSwingAtr = a.minSwingAtr.value_or(0);
                    opt.breakK = a.breakK.value_or(0.25);
                    auto result = market::computeMarketStructure(track.candles, opt);
                    bool should = false;
                    for (const auto &ev : result.events) {
                        if (ev.timestamp != candleTsStr) continue;
                        const std::string keyKind = market::structureKindKey(ev.kind);
                        if (!a.matchesStructureEvent(keyKind)) continue;
                        const std::string dir = a.structureDirection.value_or("any");
                        if (dir != "any" && dir != ev.dir) continue;
                        should = true;
                        break;
                    }
                    if (!should) {
                        Alert before = a;
                        a.lastEvaluatedCandleTime = candleTsStr;
                        toPersist.push_back({before, a, false});
                        continue;
                    }
                    double close = candle.get("close", 0.0).asDouble();
                    Alert before = a;
                    a.status = "triggered";
                    a.triggeredAt = util::nowIso8601();
                    a.lastCheckedPrice = close;
                    a.closePrice = close;
                    a.lastEvaluatedCandleTime = candleTsStr;
                    TriggeredAlert t;
                    t.alert = a;
                    t.currentPrice = close;
                    t.alertTypeLabel = "market_structure";
                    t.timeframe = iv;
                    triggered.push_back(t);
                    toPersist.push_back({before, a, true});
                    LOG_INFO << "Triggered market_structure alert " << a.id << " " << a.pair
                             << " close=" << close;

                    for (auto &armed : armDependentsLocked(a.id, candleTsStr)) {
                        toPersist.push_back({armed.first, armed.second, true});
                    }
                }
            }
        }
        if (!triggered.empty() || !toPersist.empty()) rebuildIndexes();
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

int AlertManager::expireStalePrevDayAlerts() {
    if (!postgres_) return 0;
    const UtcYmd today = utcYmdFromEpoch(std::time(nullptr));

    struct PersistBatch {
        Alert before;
        Alert after;
    };
    std::vector<PersistBatch> toPersist;
    {
        std::lock_guard<std::mutex> lk(mu_);
        for (auto &kv : alerts_) {
            Alert &a = kv.second;
            if (a.status != "active" || a.alertType != "prev_day_level") continue;
            auto createdDay = utcYmdFromIso(a.createdAt);
            if (!createdDay || !(*createdDay < today)) continue;
            Alert before = a;
            a.status = "expired";
            sweepLookbackPending_.erase(a.id);
            toPersist.push_back({before, a});
            LOG_INFO << "Expired prev-day alert " << a.id << " " << a.pair
                     << " (created " << a.createdAt << ")";
        }
        if (!toPersist.empty()) rebuildIndexes();
    }

    int expired = 0;
    for (const auto &batch : toPersist) {
        if (!persistAlertSync(batch.after)) {
            std::lock_guard<std::mutex> lk(mu_);
            alerts_[batch.after.id] = batch.before;
            rebuildIndexes();
            LOG_ERROR << "Failed to persist expired prev-day alert " << batch.after.id;
            continue;
        }
        bumpUserRevision(batch.after.userId);
        ++expired;
    }
    if (expired > 0) notifySubscriptionChange();
    return expired;
}

int AlertManager::expireTimedOutAlerts() {
    if (!postgres_) return 0;
    const std::time_t now = std::time(nullptr);

    struct PersistBatch {
        Alert before;
        Alert after;
    };
    std::vector<PersistBatch> toPersist;
    {
        std::lock_guard<std::mutex> lk(mu_);
        for (auto &kv : alerts_) {
            Alert &a = kv.second;
            if (a.status != "active" && a.status != "waiting") continue;
            if (!isPastExpiry(a, now)) continue;
            Alert before = a;
            a.status = "expired";
            sweepLookbackPending_.erase(a.id);
            toPersist.push_back({before, a});
            LOG_INFO << "Expired alert " << a.id << " " << a.pair << " (expires_at "
                     << a.expiresAt.value_or("") << ")";
        }
        if (!toPersist.empty()) rebuildIndexes();
    }

    int expired = 0;
    for (const auto &batch : toPersist) {
        if (!persistAlertSync(batch.after)) {
            std::lock_guard<std::mutex> lk(mu_);
            alerts_[batch.after.id] = batch.before;
            rebuildIndexes();
            LOG_ERROR << "Failed to persist timed-out alert " << batch.after.id;
            continue;
        }
        bumpUserRevision(batch.after.userId);
        ++expired;
    }
    if (expired > 0) notifySubscriptionChange();
    return expired;
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

void AlertManager::clearSweepLookbackPending(const std::string &alertId) {
    std::lock_guard<std::mutex> lk(mu_);
    sweepLookbackPending_.erase(alertId);
}

void AlertManager::scheduleSweepLookback(const std::string &alertId, int attempt,
                                         std::function<void()> onComplete) {
    static std::atomic<int> stagger{0};
    int delayMs = 0;
    if (attempt == 0) {
        delayMs = (stagger.fetch_add(1) % 30) * 40;  // 0–1160ms stagger for batch creates
    } else {
        delayMs = 2000;
    }
    std::thread([this, alertId, attempt, onComplete = std::move(onComplete), delayMs]() mutable {
        if (delayMs > 0) std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
        auto run = [this, alertId, attempt, onComplete = std::move(onComplete)]() mutable {
            runSweepLookback(alertId, attempt, std::move(onComplete));
        };
        if (dbExecutor_)
            dbExecutor_(std::move(run));
        else
            run();
    }).detach();
}

void AlertManager::runSweepLookback(const std::string &alertId, int attempt,
                                    std::function<void()> onComplete) {
    constexpr int kMaxAttempts = 15;

    auto finish = [this, alertId, onComplete]() {
        clearSweepLookbackPending(alertId);
        invokeComplete(onComplete);
    };

    Alert snapshot;
    {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = alerts_.find(alertId);
        if (it == alerts_.end() || it->second.status != "active" ||
            it->second.alertType != "prev_day_level") {
            sweepLookbackPending_.erase(alertId);
            invokeComplete(onComplete);
            return;
        }
        if (!it->second.hasDolTrigger("sweep")) {
            sweepLookbackPending_.erase(alertId);
            invokeComplete(onComplete);
            return;
        }
        snapshot = it->second;
    }

    if (!ctrader_ || !ctrader_->isReady() || !registry_ || !dolProvider_) {
        if (dolProvider_) dolProvider_->refreshDue();
        if (attempt + 1 >= kMaxAttempts) {
            finish();
            return;
        }
        scheduleSweepLookback(alertId, attempt + 1, std::move(onComplete));
        return;
    }

    auto levels = dolProvider_->currentLevels(snapshot.pair);
    if (!levels.valid) {
        dolProvider_->track(snapshot.pair);
        dolProvider_->refreshDue();
        if (attempt + 1 >= kMaxAttempts) {
            finish();
            return;
        }
        scheduleSweepLookback(alertId, attempt + 1, std::move(onComplete));
        return;
    }

    auto symId = registry_->idForCanonical(snapshot.pair);
    if (!symId) symId = registry_->resolveId(snapshot.pair);
    if (!symId) {
        LOG_WARN << "Sweep lookback: no symbol id for " << snapshot.pair;
        finish();
        return;
    }

    std::time_t now = std::time(nullptr);
    std::tm tm{};
    gmtime_r(&now, &tm);
    tm.tm_hour = 0;
    tm.tm_min = 0;
    tm.tm_sec = 0;
    std::time_t dayStart = timegm(&tm);
    const int64_t dayStartMinutes = static_cast<int64_t>(dayStart) / 60;
    const int64_t fromMs = static_cast<int64_t>(dayStart) * 1000;
    const int64_t toMs = static_cast<int64_t>(now) * 1000;
    const int period = util::intervalToTrendbarPeriod("1m");

    ctrader_->getTrendbars(
        *symId, period, fromMs, toMs, 1500,
        [this, alertId, attempt, onComplete = std::move(onComplete), levels, snapshot,
         dayStartMinutes](ctrader::TrendbarsResult res) mutable {
            if (!res.ok) {
                LOG_DEBUG << "Sweep lookback fetch failed for " << snapshot.pair << ": "
                          << res.error;
                if (attempt + 1 >= kMaxAttempts) {
                    clearSweepLookbackPending(alertId);
                    invokeComplete(onComplete);
                    return;
                }
                scheduleSweepLookback(alertId, attempt + 1, std::move(onComplete));
                return;
            }

            auto touch = firstSweepTouchToday(snapshot, levels, res.bars, dayStartMinutes);
            if (touch) {
                const std::string touchedAt = util::toIso8601(touch->second);
                if (finalizeSweepLookbackTrigger(alertId, touch->first, touchedAt)) {
                    LOG_INFO << "Sweep lookback triggered " << alertId << " " << snapshot.pair
                             << " at " << touchedAt << " price=" << touch->first;
                }
            } else {
                LOG_DEBUG << "Sweep lookback: no same-day touch yet for " << alertId << " "
                          << snapshot.pair;
            }
            clearSweepLookbackPending(alertId);
            invokeComplete(onComplete);
        });
}

bool AlertManager::finalizeSweepLookbackTrigger(const std::string &alertId, double touchPrice,
                                                const std::string &touchedAtIso) {
    if (!postgres_) return false;
    std::optional<TriggeredAlert> result;
    Alert persisted;
    Alert previous;
    {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = alerts_.find(alertId);
        if (it == alerts_.end()) return false;
        Alert &a = it->second;
        if (a.status != "active" || a.alertType != "prev_day_level") return false;
        if (isPastExpiry(a)) {
            previous = a;
            a.status = "expired";
            persisted = a;
            rebuildIndexes();
            result = std::nullopt;
        } else {
            previous = a;
            triggerAlert(a, touchPrice, touchedAtIso);
            TriggeredAlert t;
            t.alert = a;
            t.currentPrice = touchPrice;
            t.alertTypeLabel = "prev_day_level";
            t.timeframe = "1m";
            result = t;
            persisted = a;
            rebuildIndexes();
        }
    }
    if (!persistAlertSync(persisted)) {
        std::lock_guard<std::mutex> lk(mu_);
        alerts_[alertId] = previous;
        rebuildIndexes();
        LOG_ERROR << "Failed to persist lookback-triggered draw alert " << alertId;
        return false;
    }
    bumpUserRevision(persisted.userId);
    if (!result) return false;
    if (onTriggered_) onTriggered_(*result);
    armDependentAlerts(persisted.id);
    return true;
}

}  // namespace ctraderplus::alerts
