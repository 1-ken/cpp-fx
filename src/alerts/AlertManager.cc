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

#include "core/Metrics.h"
#include "ctrader/CTraderClient.h"
#include "ctrader/SymbolRegistry.h"
#include "market/MarketHub.h"
#include "market/PrevDayLevelProvider.h"
#include "market/StructureEngine.h"
#include "services/PostgresService.h"
#include "services/RedisService.h"
#include "util/ForexMarketHours.h"
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
    std::lock_guard<std::mutex> lk(mu_);
    ++userAlertsRevision_[userId];
}

void AlertManager::notifySubscriptionChange() {
    if (onSubscriptionChange_) onSubscriptionChange_();
}

uint64_t AlertManager::userAlertsRevision(const std::string &userId) const {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = userAlertsRevision_.find(userId);
    return it == userAlertsRevision_.end() ? 0 : it->second;
}

void AlertManager::cacheAlert(Alert alert) {
    alert.normalizeChannels();
    std::lock_guard<std::mutex> lk(mu_);
    alerts_[alert.id] = std::move(alert);
    rebuildIndexes();
}

namespace {
constexpr std::size_t kStructureWindow = 512;

std::string structureOptKey(const market::StructureOptions &opt) {
    return std::to_string(opt.minSwingAtr) + "|" + std::to_string(opt.breakK) + "|" +
           std::to_string(opt.atrPeriod);
}
}  // namespace

void AlertManager::appendStructureCandle(StructureTrack &track, market::StructureCandle bar) {
    if (!track.candles.empty() && track.candles.back().timestamp == bar.timestamp) return;
    track.candles.push_back(std::move(bar));
    if (track.candles.size() > kStructureWindow) {
        track.candles.erase(track.candles.begin(),
                            track.candles.begin() +
                                static_cast<std::ptrdiff_t>(track.candles.size() - kStructureWindow));
        for (auto &kv : track.engines) {
            kv.second.reset();
            for (const auto &c : track.candles) kv.second.append(c);
        }
        return;
    }
    for (auto &kv : track.engines) kv.second.append(track.candles.back());
}

market::IncrementalStructure &AlertManager::structureEngine(StructureTrack &track,
                                                            const market::StructureOptions &opt) {
    auto [it, inserted] = track.engines.try_emplace(structureOptKey(opt), opt);
    (void)inserted;
    auto &eng = it->second;
    if (eng.barsApplied() != static_cast<int>(track.candles.size())) {
        eng.reset();
        for (const auto &c : track.candles) eng.append(c);
    }
    return eng;
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
        } else if (a.alertType == "sweep_confirm") {
            activeCandleIndex_[candleIndexKey(key, "5m")].push_back(a.id);
            activeCandleIndex_[candleIndexKey(key, "1h")].push_back(a.id);
        } else if (a.alertType == "structure_session") {
            for (const auto &raw : a.intervals) {
                std::string iv = raw;
                std::transform(iv.begin(), iv.end(), iv.begin(), ::tolower);
                if (iv.empty()) continue;
                activeCandleIndex_[candleIndexKey(key, iv)].push_back(a.id);
            }
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
    expiryByMinute_.clear();
    activePrevDayIds_.clear();
    for (const auto &kv : alerts_) {
        const Alert &a = kv.second;
        if (a.status == "active" && a.alertType == "prev_day_level")
            activePrevDayIds_.push_back(a.id);
        if (a.status != "active" && a.status != "waiting") continue;
        if (!a.expiresAt) continue;
        auto exp = util::parseIso8601(*a.expiresAt);
        if (!exp) continue;
        const std::time_t minute = *exp - (*exp % 60);
        expiryByMinute_[minute].push_back(a.id);
    }
}

void AlertManager::persistAlertThen(const Alert &a, std::function<void(bool)> done) {
    if (!postgres_) {
        if (done) done(true);
        return;
    }
    Json::Value alertJson = a.toJson();
    const std::string alertId = a.id;
    auto write = [this, alertJson, alertId, done = std::move(done)]() {
        const bool ok = postgres_->upsertAlert(alertJson);
        if (!ok) LOG_ERROR << "upsertAlert failed (async) alert_id=" << alertId;
        if (done) done(ok);
    };
    if (dbExecutor_)
        dbExecutor_(std::move(write));
    else
        write();
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
    auto write = [this, alertJson]() {
        const auto t0 = std::chrono::steady_clock::now();
        const bool ok = postgres_->upsertAlert(alertJson);
        const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        core::Metrics::instance().pgQueryCount.fetch_add(1, std::memory_order_relaxed);
        core::Metrics::instance().pgQueryDurationUs.fetch_add(static_cast<uint64_t>(us > 0 ? us : 0),
                                                             std::memory_order_relaxed);
        return ok;
    };
    if (!dbExecutor_ || (dbLoop_ && dbLoop_->isInLoopThread())) return write();
    auto prom = std::make_shared<std::promise<bool>>();
    auto fut = prom->get_future();
    dbExecutor_([write, prom]() {
        try {
            prom->set_value(write());
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
    if (!dbExecutor_ || (dbLoop_ && dbLoop_->isInLoopThread())) return postgres_->deleteAlert(id);
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

namespace {
struct SessionSpec {
    std::vector<std::string> intervals;
    std::vector<std::string> events;
    std::string direction;
};

SessionSpec normalizeSessionSpec(const std::vector<std::string> &intervals,
                                 const std::vector<std::string> &structureEvents,
                                 const std::string &structureDirection) {
    if (intervals.empty())
        throw std::invalid_argument("intervals must include at least one timeframe");
    SessionSpec spec;
    for (const auto &raw : intervals) {
        std::string iv = raw;
        std::transform(iv.begin(), iv.end(), iv.begin(), ::tolower);
        if (iv == "1d" || util::intervalToSeconds(iv) == 0 ||
            util::intervalToSeconds(iv) > util::intervalToSeconds("4h"))
            throw std::invalid_argument(
                "Invalid interval. Session alerts support 1m, 5m, 15m, 30m, 1h, 4h");
        if (std::find(spec.intervals.begin(), spec.intervals.end(), iv) == spec.intervals.end())
            spec.intervals.push_back(iv);
    }
    std::sort(spec.intervals.begin(), spec.intervals.end(),
              [](const std::string &lhs, const std::string &rhs) {
                  return util::intervalToSeconds(lhs) < util::intervalToSeconds(rhs);
              });
    if (structureEvents.empty())
        throw std::invalid_argument("structure_event must include at least one event");
    for (const auto &raw : structureEvents) {
        std::string ev = raw;
        std::transform(ev.begin(), ev.end(), ev.begin(), ::tolower);
        if (ev == "any") {
            spec.events = {"bos", "choch", "sweep"};
            break;
        }
        if (ev != "bos" && ev != "choch" && ev != "sweep")
            throw std::invalid_argument("structure_event must be bos, choch, or sweep");
        if (std::find(spec.events.begin(), spec.events.end(), ev) == spec.events.end())
            spec.events.push_back(ev);
    }
    spec.direction = structureDirection;
    std::transform(spec.direction.begin(), spec.direction.end(), spec.direction.begin(),
                   ::tolower);
    if (spec.direction != "bull" && spec.direction != "bear" && spec.direction != "any")
        throw std::invalid_argument("structure_direction must be bull, bear, or any");
    if (spec.intervals.size() > 1 && spec.direction == "any")
        throw std::invalid_argument(
            "structure_direction must be bull or bear when more than one timeframe is selected");
    return spec;
}
}  // namespace

std::vector<Alert> AlertManager::createStructureSessionAlerts(
    const std::vector<std::string> &pairs, const std::vector<std::string> &intervals,
    const std::vector<std::string> &structureEvents, const std::string &structureDirection,
    const std::string &userId, const std::string &email, const std::vector<std::string> &channels,
    const std::string &phone, const std::string &customMessage, const std::string &expiresAt,
    std::optional<double> minSwingAtr, std::optional<double> breakK) {
    if (!postgres_) throw std::runtime_error("Database unavailable");
    const SessionSpec spec = normalizeSessionSpec(intervals, structureEvents, structureDirection);
    const std::vector<std::string> unique = util::uniqueCanonicalPairs(pairs);
    if (unique.empty()) throw std::invalid_argument("At least one pair is required");
    if (static_cast<int>(unique.size()) > kMaxBatchPairs)
        throw std::invalid_argument("Select at most " + std::to_string(kMaxBatchPairs) + " pairs");

    const std::string createdAt = util::nowIso8601();
    const std::time_t now = std::time(nullptr);
    std::string sessionStart;
    if (auto sess = util::forexSessionStart(now)) {
        sessionStart = util::toIso8601(*sess);
    } else {
        const long long wait = util::secondsUntilMarketOpens(now);
        sessionStart = util::toIso8601(now + static_cast<std::time_t>(wait));
    }
    std::optional<std::string> batchId;
    if (unique.size() > 1) batchId = newUuid();

    std::vector<Alert> built;
    built.reserve(unique.size());
    for (const auto &pair : unique) {
        Alert a;
        a.id = newUuid();
        a.userId = userId;
        a.pair = pair;
        a.alertType = "structure_session";
        a.intervals = spec.intervals;
        a.interval = spec.intervals.front();
        a.structureEvents = spec.events;
        a.structureDirection = spec.direction;
        a.minSwingAtr = minSwingAtr.value_or(0);
        a.breakK = breakK.value_or(0.25);
        a.sessionStepIndex = 0;
        a.sessionStart = sessionStart;
        a.batchId = batchId;
        a.email = email;
        a.channels = channels;
        a.normalizeChannels();
        a.phone = phone;
        a.customMessage = customMessage;
        a.expiresAt = expiresAt;
        a.createdAt = createdAt;
        a.status = "active";
        built.push_back(std::move(a));
    }

    commitCreatedAlerts(built);
    std::string ivLog;
    for (size_t i = 0; i < spec.intervals.size(); ++i) {
        if (i) ivLog += ">";
        ivLog += spec.intervals[i];
    }
    LOG_INFO << "Created " << built.size() << " structure_session alert(s) " << ivLog << " "
             << spec.direction << " session=" << sessionStart
             << (batchId ? (" batch=" + *batchId) : "");
    return built;
}

Alert AlertManager::createStructureSessionAlert(
    const std::string &pair, const std::vector<std::string> &intervals,
    const std::vector<std::string> &structureEvents, const std::string &structureDirection,
    const std::string &userId, const std::string &email, const std::vector<std::string> &channels,
    const std::string &phone, const std::string &customMessage, const std::string &expiresAt,
    std::optional<double> minSwingAtr, std::optional<double> breakK) {
    auto made = createStructureSessionAlerts({pair}, intervals, structureEvents, structureDirection,
                                             userId, email, channels, phone, customMessage,
                                             expiresAt, minSwingAtr, breakK);
    return made.front();
}

void AlertManager::commitCreatedAlerts(const std::vector<Alert> &built) {
    if (built.empty()) return;
    {
        std::lock_guard<std::mutex> lk(mu_);
        for (const auto &a : built) alerts_[a.id] = a;
        rebuildIndexes();
    }
    std::vector<std::string> persisted;
    for (const auto &a : built) {
        if (!persistAlertSync(a)) {
            {
                std::lock_guard<std::mutex> lk(mu_);
                for (const auto &made : built) alerts_.erase(made.id);
                rebuildIndexes();
            }
            for (const auto &id : persisted) persistDeleteSync(id);
            throw std::runtime_error("Alert not persisted");
        }
        persisted.push_back(a.id);
    }
    bumpUserRevision(built.front().userId);
    notifySubscriptionChange();
}

std::vector<Alert> AlertManager::createSweepConfirmAlerts(
    const std::vector<std::string> &pairs, const std::vector<std::string> &confirmations,
    const std::string &direction, const std::string &userId, const std::string &email,
    const std::vector<std::string> &channels, const std::string &phone,
    const std::string &customMessage, const std::string &expiresAt,
    std::optional<double> minSwingAtr, std::optional<double> breakK) {
    if (!postgres_) throw std::runtime_error("Database unavailable");
    if (confirmations.empty())
        throw std::invalid_argument("Select at least one confirmation");
    std::vector<std::string> kinds;
    for (const auto &raw : confirmations) {
        std::string ev = raw;
        std::transform(ev.begin(), ev.end(), ev.begin(), ::tolower);
        if (ev != "bos" && ev != "choch" && ev != "cisd")
            throw std::invalid_argument("confirmation must be bos, choch, or cisd");
        if (std::find(kinds.begin(), kinds.end(), ev) == kinds.end()) kinds.push_back(ev);
    }
    std::string dir = direction;
    std::transform(dir.begin(), dir.end(), dir.begin(), ::tolower);
    if (dir != "bull" && dir != "bear" && dir != "any")
        throw std::invalid_argument("structure_direction must be bull, bear, or any");
    const std::vector<std::string> unique = util::uniqueCanonicalPairs(pairs);
    if (unique.empty()) throw std::invalid_argument("At least one pair is required");
    if (static_cast<int>(unique.size()) > kMaxBatchPairs)
        throw std::invalid_argument("Select at most " + std::to_string(kMaxBatchPairs) + " pairs");

    const std::string createdAt = util::nowIso8601();
    std::optional<std::string> batchId;
    if (unique.size() > 1) batchId = newUuid();
    std::vector<Alert> built;
    built.reserve(unique.size());
    for (const auto &pair : unique) {
        Alert a;
        a.id = newUuid();
        a.userId = userId;
        a.pair = pair;
        a.alertType = "sweep_confirm";
        a.interval = "5m";
        a.structureEvents = kinds;
        a.structureDirection = dir;
        a.minSwingAtr = minSwingAtr.value_or(0);
        a.breakK = breakK.value_or(0.25);
        a.batchId = batchId;
        a.email = email;
        a.channels = channels;
        a.normalizeChannels();
        a.phone = phone;
        a.customMessage = customMessage;
        a.expiresAt = expiresAt;
        a.createdAt = createdAt;
        a.status = "active";
        built.push_back(std::move(a));
    }
    commitCreatedAlerts(built);
    LOG_INFO << "Created " << built.size() << " sweep_confirm alert(s) dir=" << dir
             << (batchId ? (" batch=" + *batchId) : "");
    return built;
}

void AlertManager::ingestStructureHistory(const std::string &pair, const std::string &interval,
                                          const std::vector<Json::Value> &candles) {
    std::string iv = interval;
    std::transform(iv.begin(), iv.end(), iv.begin(), ::tolower);
    std::string key = candleIndexKey(util::canonicalPair(pair), iv);
    std::lock_guard<std::mutex> lk(mu_);
    auto &track = structureTracks_[key];
    track.candles.clear();
    track.engines.clear();
    for (const auto &c : candles) {
        market::StructureCandle bar;
        bar.timestamp = c.get("timestamp", "").asString();
        bar.open = c.get("open", 0.0).asDouble();
        bar.high = c.get("high", 0.0).asDouble();
        bar.low = c.get("low", 0.0).asDouble();
        bar.close = c.get("close", 0.0).asDouble();
        if (!bar.timestamp.empty()) track.candles.push_back(std::move(bar));
    }
    if (track.candles.size() > 512) {
        track.candles.erase(track.candles.begin(),
                            track.candles.begin() +
                                static_cast<std::ptrdiff_t>(track.candles.size() - 512));
    }
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
        } else {
            if (updates.isMember("channels") && updates["channels"].isArray()) {
                a.channels.clear();
                for (const auto &c : updates["channels"]) {
                    if (c.isString() && !c.asString().empty()) a.channels.push_back(c.asString());
                }
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
    const auto evalStarted = std::chrono::steady_clock::now();
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
        std::vector<TriggeredAlert> mine;
        for (const auto &t : triggered) {
            if (t.alert.id == batch.after.id) mine.push_back(t);
        }
        if (!postgres_) {
            bumpUserRevision(batch.after.userId);
            notified.insert(notified.end(), mine.begin(), mine.end());
            continue;
        }
        Alert before = batch.before;
        Alert after = batch.after;
        persistAlertThen(after, [this, before, after, mine](bool ok) {
            if (!ok) {
                std::lock_guard<std::mutex> lk(mu_);
                alerts_[after.id] = before;
                rebuildIndexes();
                LOG_ERROR << "Failed to persist triggered price alert " << after.id;
                return;
            }
            bumpUserRevision(after.userId);
            core::Metrics::instance().alertTriggerTotal.fetch_add(mine.size(),
                                                                  std::memory_order_relaxed);
            if (!onTriggered_) return;
            for (const auto &t : mine) onTriggered_(t);
        });
        notified.insert(notified.end(), mine.begin(), mine.end());
    }
    if (!postgres_) {
        for (const auto &t : notified) {
            if (onTriggered_) onTriggered_(t);
        }
    }
    const auto evalUs = std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now() - evalStarted)
                            .count();
    core::Metrics::instance().alertEvalCount.fetch_add(1, std::memory_order_relaxed);
    core::Metrics::instance().alertEvalDurationUs.fetch_add(
        static_cast<uint64_t>(evalUs > 0 ? evalUs : 0), std::memory_order_relaxed);
    if (!postgres_) {
        core::Metrics::instance().alertTriggerTotal.fetch_add(notified.size(),
                                                              std::memory_order_relaxed);
    }
    return notified;
}

namespace {
constexpr int kSweepConfirmWindow = 12;
constexpr int kSweepConfirmGapSec = 15 * 60;

double deliveryRunOpen(const std::vector<market::StructureCandle> &candles,
                       const std::string &sweepTs, bool upRun) {
    int idx = -1;
    for (int i = static_cast<int>(candles.size()) - 1; i >= 0; --i) {
        if (candles[static_cast<size_t>(i)].timestamp == sweepTs) {
            idx = i;
            break;
        }
    }
    if (idx < 0) return 0;
    int run = idx;
    for (int i = idx - 1; i >= 0; --i) {
        const auto &bar = candles[static_cast<size_t>(i)];
        const bool up = bar.close > bar.open;
        const bool down = bar.close < bar.open;
        if (upRun ? up : down)
            run = i;
        else
            break;
    }
    return candles[static_cast<size_t>(run)].open;
}

struct HourSwing {
    bool ok = false;
    double price = 0;
    std::string at;
};

struct HourSwings {
    HourSwing high;
    HourSwing low;
};

// Latest 1h fractal swing that no later close has broken. Hours that are still
// open at fiveOpen are ignored, so the swing exists only after its confirming
// hour has closed.
HourSwings latestUnbrokenHourSwings(const std::vector<market::StructureCandle> &hours,
                                    const std::vector<market::StructureCandle> &fives,
                                    std::time_t fiveOpen) {
    struct ClosedHour {
        market::StructureCandle bar;
        std::time_t start = 0;
    };
    std::vector<ClosedHour> closed;
    closed.reserve(hours.size());
    for (const auto &bar : hours) {
        auto start = util::parseIso8601(bar.timestamp);
        if (!start || *start + 3600 > fiveOpen) continue;
        closed.push_back({bar, *start});
    }
    auto brokenAfter = [&](std::size_t pivot, double price, bool highLevel, std::time_t confirmedClose) {
        for (std::size_t j = pivot + 1; j < closed.size(); ++j) {
            const double c = closed[j].bar.close;
            if (highLevel ? c > price : c < price) return true;
        }
        for (const auto &bar : fives) {
            auto start = util::parseIso8601(bar.timestamp);
            if (!start || *start + 300 <= confirmedClose) continue;
            if (highLevel ? bar.close > price : bar.close < price) return true;
        }
        return false;
    };
    HourSwings out;
    for (std::size_t i = 1; i + 1 < closed.size(); ++i) {
        const auto &prev = closed[i - 1].bar;
        const auto &mid = closed[i].bar;
        const auto &next = closed[i + 1].bar;
        const std::time_t confirmedClose = closed[i + 1].start + 3600;
        if (mid.high > prev.high && mid.high > next.high &&
            !brokenAfter(i, mid.high, true, confirmedClose)) {
            out.high = {true, mid.high, mid.timestamp};
        }
        if (mid.low < prev.low && mid.low < next.low &&
            !brokenAfter(i, mid.low, false, confirmedClose)) {
            out.low = {true, mid.low, mid.timestamp};
        }
    }
    return out;
}

bool wantsKind(const Alert &a, const std::string &kind) {
    return std::find(a.structureEvents.begin(), a.structureEvents.end(), kind) !=
           a.structureEvents.end();
}
}  // namespace

AlertManager::SessionStepResult AlertManager::evalSweepConfirmLocked(
    Alert &a, const Json::Value &candle, const std::string &candleTsStr, StructureTrack &track) {
    if (a.lastEvaluatedCandleTime && *a.lastEvaluatedCandleTime == candleTsStr)
        return SessionStepResult::Unchanged;
    auto candleStart = parseCandleTs(candle["timestamp"]);
    const int ivSec = intervalSeconds("5m");
    auto createdAt = util::parseIso8601(a.createdAt);
    if (candleStart && createdAt && *candleStart + ivSec <= *createdAt) {
        a.lastEvaluatedCandleTime = candleTsStr;
        return SessionStepResult::Unchanged;
    }
    a.lastEvaluatedCandleTime = candleTsStr;
    if (track.candles.size() < 5) return SessionStepResult::Unchanged;

    market::StructureOptions opt;
    opt.minSwingAtr = a.minSwingAtr.value_or(0);
    opt.breakK = a.breakK.value_or(0.25);
    auto &engine = structureEngine(track, opt);
    bool sweepBear = false, sweepBull = false;
    bool bosBear = false, bosBull = false, chochBear = false, chochBull = false;
    double sweepLevelBear = 0, sweepLevelBull = 0;
    std::string sweepHighAt, sweepLowAt;
    for (const auto &ev : engine.events()) {
        if (ev.timestamp != candleTsStr) continue;
        const std::string kind = market::structureKindKey(ev.kind);
        if (kind == "bos" && ev.dir == "bear") {
            bosBear = true;
        } else if (kind == "bos" && ev.dir == "bull") {
            bosBull = true;
        } else if (kind == "choch" && ev.dir == "bear") {
            chochBear = true;
        } else if (kind == "choch" && ev.dir == "bull") {
            chochBull = true;
        }
    }
    const double high = candle.get("high", 0.0).asDouble();
    const double low = candle.get("low", 0.0).asDouble();
    const double close = candle.get("close", 0.0).asDouble();
    if (candleStart) {
        const std::string hourKey = candleIndexKey(util::canonicalPair(a.pair), "1h");
        auto hourIt = structureTracks_.find(hourKey);
        if (hourIt != structureTracks_.end()) {
            const auto levels =
                latestUnbrokenHourSwings(hourIt->second.candles, track.candles, *candleStart);
            if (levels.high.ok && high > levels.high.price && close < levels.high.price &&
                (!a.sweptHighAt || *a.sweptHighAt != levels.high.at)) {
                sweepBear = true;
                sweepLevelBear = levels.high.price;
                sweepHighAt = levels.high.at;
            }
            if (levels.low.ok && low < levels.low.price && close > levels.low.price &&
                (!a.sweptLowAt || *a.sweptLowAt != levels.low.at)) {
                sweepBull = true;
                sweepLevelBull = levels.low.price;
                sweepLowAt = levels.low.at;
            }
        }
    }
    const std::string want = a.structureDirection.value_or("any");
    bool changed = false;
    bool fired = false;

    auto clearPending = [&]() {
        if (!a.pendingDir && !a.pendingSweepAt && a.pendingBars == 0) return;
        a.pendingDir.reset();
        a.pendingSweepAt.reset();
        a.pendingSweepLevel.reset();
        a.pendingRunOpen.reset();
        a.pendingBars = 0;
        changed = true;
    };
    auto confirmed = [&](const std::string &dir) {
        if (dir == "bear") {
            if (wantsKind(a, "bos") && bosBear) return true;
            if (wantsKind(a, "choch") && chochBear) return true;
            if (wantsKind(a, "cisd") && a.pendingRunOpen && close < *a.pendingRunOpen) return true;
        } else if (dir == "bull") {
            if (wantsKind(a, "bos") && bosBull) return true;
            if (wantsKind(a, "choch") && chochBull) return true;
            if (wantsKind(a, "cisd") && a.pendingRunOpen && close > *a.pendingRunOpen) return true;
        }
        return false;
    };
    auto oppositeBreak = [&](const std::string &dir) {
        if (dir == "bear") return bosBull || chochBull;
        if (dir == "bull") return bosBear || chochBear;
        return false;
    };
    auto tryFire = [&]() {
        if (fired || !a.pendingDir) return;
        if (a.triggeredAt && candleStart) {
            auto last = util::parseIso8601(*a.triggeredAt);
            if (last && *candleStart + ivSec < *last + kSweepConfirmGapSec) {
                clearPending();
                return;
            }
        }
        if (candleStart)
            a.triggeredAt = util::toIso8601(*candleStart + ivSec);
        else
            a.triggeredAt = util::nowIso8601();
        a.lastCheckedPrice = close;
        a.closePrice = close;
        clearPending();
        fired = true;
    };

    if (a.pendingDir && a.pendingSweepAt && *a.pendingSweepAt != candleTsStr) {
        a.pendingBars += 1;
        changed = true;
        if (a.pendingBars > kSweepConfirmWindow) clearPending();
    }
    if (a.pendingDir && a.pendingBars <= kSweepConfirmWindow) {
        if (oppositeBreak(*a.pendingDir))
            clearPending();
        else if (confirmed(*a.pendingDir))
            tryFire();
    }
    const bool allowBear = want == "any" || want == "bear";
    const bool allowBull = want == "any" || want == "bull";
    if (!fired && ((sweepBear && allowBear) || (sweepBull && allowBull))) {
        const bool bear = sweepBear && allowBear;
        a.pendingDir = bear ? "bear" : "bull";
        a.pendingSweepAt = candleTsStr;
        a.pendingSweepLevel = bear ? sweepLevelBear : sweepLevelBull;
        a.pendingRunOpen = deliveryRunOpen(track.candles, candleTsStr, bear);
        a.pendingBars = 1;
        if (bear)
            a.sweptHighAt = sweepHighAt;
        else
            a.sweptLowAt = sweepLowAt;
        changed = true;
        if (confirmed(*a.pendingDir)) tryFire();
    }
    if (fired) return SessionStepResult::Triggered;
    if (changed) return SessionStepResult::Updated;
    return SessionStepResult::Unchanged;
}

AlertManager::SessionStepResult AlertManager::evalStructureSessionLocked(
    Alert &a, const std::string &interval, const Json::Value &candle,
    const std::string &candleTsStr, StructureTrack &track) {
    if (a.intervals.empty()) return SessionStepResult::Unchanged;
    auto candleStart = parseCandleTs(candle["timestamp"]);
    if (!candleStart) return SessionStepResult::Unchanged;
    const int ivSec = intervalSeconds(interval);
    auto createdAt = util::parseIso8601(a.createdAt);
    if (ivSec > 0 && createdAt && *candleStart + ivSec <= *createdAt)
        return SessionStepResult::Unchanged;

    auto candleSession = util::forexSessionStart(*candleStart);
    if (!candleSession) return SessionStepResult::Unchanged;
    auto stored = a.sessionStart ? util::parseIso8601(*a.sessionStart) : std::nullopt;
    bool rolled = false;
    if (!stored || *candleSession > *stored) {
        a.sessionStart = util::toIso8601(*candleSession);
        a.sessionStepIndex = 0;
        a.stepFiredAt.reset();
        stored = candleSession;
        rolled = true;
    } else if (*candleSession < *stored) {
        return SessionStepResult::Unchanged;
    }
    if (a.lastFiredSession) {
        auto fired = util::parseIso8601(*a.lastFiredSession);
        if (fired && stored && *fired == *stored)
            return rolled ? SessionStepResult::Updated : SessionStepResult::Unchanged;
    }
    if (a.sessionStepIndex < 0 ||
        a.sessionStepIndex >= static_cast<int>(a.intervals.size())) {
        return rolled ? SessionStepResult::Updated : SessionStepResult::Unchanged;
    }
    std::string want = a.intervals[static_cast<size_t>(a.sessionStepIndex)];
    std::transform(want.begin(), want.end(), want.begin(), ::tolower);
    if (want != interval) return rolled ? SessionStepResult::Updated : SessionStepResult::Unchanged;
    if (a.stepFiredAt) {
        auto prev = util::parseIso8601(*a.stepFiredAt);
        if (prev && *candleStart <= *prev)
            return rolled ? SessionStepResult::Updated : SessionStepResult::Unchanged;
    }
    if (track.candles.size() < 5) return rolled ? SessionStepResult::Updated : SessionStepResult::Unchanged;

    market::StructureOptions opt;
    opt.minSwingAtr = a.minSwingAtr.value_or(0);
    opt.breakK = a.breakK.value_or(0.25);
    auto &engine = structureEngine(track, opt);
    bool matched = false;
    for (const auto &ev : engine.events()) {
        if (ev.timestamp != candleTsStr) continue;
        const std::string keyKind = market::structureKindKey(ev.kind);
        if (!a.matchesStructureEvent(keyKind)) continue;
        const std::string dir = a.structureDirection.value_or("any");
        if (dir != "any" && dir != ev.dir) continue;
        matched = true;
        break;
    }
    if (!matched) return rolled ? SessionStepResult::Updated : SessionStepResult::Unchanged;

    a.stepFiredAt = candleTsStr;
    const bool lastStep = a.sessionStepIndex + 1 >= static_cast<int>(a.intervals.size());
    if (!lastStep) {
        a.sessionStepIndex += 1;
        if (!a.intervals.empty())
            a.interval = a.intervals[static_cast<size_t>(a.sessionStepIndex)];
        return SessionStepResult::Updated;
    }
    const double close = candle.get("close", 0.0).asDouble();
    a.lastFiredSession = a.sessionStart;
    a.triggeredAt = util::nowIso8601();
    a.lastCheckedPrice = close;
    a.closePrice = close;
    a.sessionStepIndex = static_cast<int>(a.intervals.size());
    return SessionStepResult::Triggered;
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
        bool durable = false;
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
            market::StructureCandle bar;
            bar.timestamp = candleTsStr;
            bar.open = candle.get("open", 0.0).asDouble();
            bar.high = candle.get("high", 0.0).asDouble();
            bar.low = candle.get("low", 0.0).asDouble();
            bar.close = candle.get("close", 0.0).asDouble();
            appendStructureCandle(track, std::move(bar));
            auto idxItStruct = activeCandleIndex_.find(trackKey);
            if (idxItStruct != activeCandleIndex_.end() && track.candles.size() >= 5) {
                auto candleStart = parseCandleTs(tsVal);
                for (const auto &alertId : idxItStruct->second) {
                    auto it = alerts_.find(alertId);
                    if (it == alerts_.end()) continue;
                    Alert &a = it->second;
                    if (a.status == "active" && a.alertType == "sweep_confirm") {
                        if (k.interval != "5m") continue;
                        if (isPastExpiry(a)) {
                            Alert before = a;
                            a.status = "expired";
                            toPersist.push_back({before, a, true, true});
                            LOG_INFO << "Expired sweep_confirm alert " << a.id << " " << a.pair
                                     << " (past expires_at)";
                            continue;
                        }
                        Alert before = a;
                        const auto step =
                            evalSweepConfirmLocked(a, candle, candleTsStr, track);
                        if (step == SessionStepResult::Unchanged) continue;
                        if (step == SessionStepResult::Triggered) {
                            TriggeredAlert t;
                            t.alert = a;
                            t.currentPrice = candle.get("close", 0.0).asDouble();
                            t.alertTypeLabel = "sweep_confirm";
                            t.timeframe = "5m";
                            triggered.push_back(t);
                            toPersist.push_back({before, a, true, true});
                            LOG_INFO << "Triggered sweep_confirm alert " << a.id << " " << a.pair
                                     << " close=" << t.currentPrice;
                        } else {
                            toPersist.push_back({before, a, false, true});
                        }
                        continue;
                    }
                    if (a.status == "active" && a.alertType == "structure_session") {
                        if (isPastExpiry(a)) {
                            Alert before = a;
                            a.status = "expired";
                            toPersist.push_back({before, a, true, true});
                            LOG_INFO << "Expired structure_session alert " << a.id << " "
                                     << a.pair << " (past expires_at)";
                            continue;
                        }
                        Alert before = a;
                        const auto step = evalStructureSessionLocked(a, k.interval, candle,
                                                                     candleTsStr, track);
                        if (step == SessionStepResult::Unchanged) continue;
                        const double close = candle.get("close", 0.0).asDouble();
                        if (step == SessionStepResult::Triggered) {
                            TriggeredAlert t;
                            t.alert = a;
                            t.currentPrice = close;
                            t.alertTypeLabel = "structure_session";
                            std::string tf;
                            for (size_t i = 0; i < a.intervals.size(); ++i) {
                                if (i) tf += ">";
                                tf += a.intervals[i];
                            }
                            t.timeframe = tf;
                            triggered.push_back(t);
                            toPersist.push_back({before, a, true, true});
                            LOG_INFO << "Triggered structure_session alert " << a.id << " "
                                     << a.pair << " close=" << close;
                        } else {
                            toPersist.push_back({before, a, false, true});
                        }
                        continue;
                    }
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
                    auto &engine = structureEngine(track, opt);
                    bool should = false;
                    for (const auto &ev : engine.events()) {
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
        if (batch.isTrigger) {
            std::vector<TriggeredAlert> mine;
            for (const auto &t : triggered) {
                if (t.alert.id == batch.after.id) mine.push_back(t);
            }
            if (!postgres_) {
                bumpUserRevision(batch.after.userId);
                notified.insert(notified.end(), mine.begin(), mine.end());
                continue;
            }
            Alert before = batch.before;
            Alert after = batch.after;
            persistAlertThen(after, [this, before, after, mine](bool ok) {
                if (!ok) {
                    std::lock_guard<std::mutex> lk(mu_);
                    alerts_[after.id] = before;
                    rebuildIndexes();
                    LOG_ERROR << "Failed to persist triggered candle alert " << after.id;
                    return;
                }
                bumpUserRevision(after.userId);
                if (!onTriggered_) return;
                for (const auto &t : mine) onTriggered_(t);
            });
        } else if (batch.durable) {
            Alert before = batch.before;
            Alert after = batch.after;
            persistAlertThen(after, [this, before, after](bool ok) {
                if (!ok) {
                    std::lock_guard<std::mutex> lk(mu_);
                    alerts_[after.id] = before;
                    rebuildIndexes();
                    LOG_ERROR << "Failed to persist structure session step " << after.id;
                    return;
                }
                bumpUserRevision(after.userId);
            });
        } else {
            persistAlert(batch.after);
        }
    }
    if (!postgres_) {
        for (const auto &t : notified) {
            if (onTriggered_) onTriggered_(t);
        }
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
        for (const auto &id : activePrevDayIds_) {
            auto it = alerts_.find(id);
            if (it == alerts_.end()) continue;
            Alert &a = it->second;
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
        std::vector<std::string> due;
        for (auto it = expiryByMinute_.begin(); it != expiryByMinute_.end();) {
            if (it->first > now) break;
            due.insert(due.end(), it->second.begin(), it->second.end());
            if (it->first + 60 <= now)
                it = expiryByMinute_.erase(it);
            else
                ++it;
        }
        for (const auto &id : due) {
            auto it = alerts_.find(id);
            if (it == alerts_.end()) continue;
            Alert &a = it->second;
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
