#pragma once

#include <ctime>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <trantor/net/EventLoop.h>

#include <json/json.h>

#include "alerts/Alert.h"
#include "market/StructureEngine.h"

namespace ctraderplus::services {
class PostgresService;
class RedisService;
}
namespace ctraderplus::market {
struct FlatPair;
class PrevDayLevelProvider;
}
namespace ctraderplus::ctrader {
class CTraderClient;
class SymbolRegistry;
}

namespace ctraderplus::alerts {

// Triggered alert payload passed to notification dispatch.
struct TriggeredAlert {
    Alert alert;
    double currentPrice = 0;
    std::string timeframe;
    std::string alertTypeLabel = "price";
    std::string eventId;
};

// Upper bound for multi-pair alert creates (prev_day_level, structure_session, sweep_confirm).
constexpr int kMaxBatchPairs = 100;

// Port of app/services/alert_service.py AlertManager.
class AlertManager {
  public:
    void configure(services::PostgresService *pg, services::RedisService *redis,
                   std::function<void(std::function<void()>)> dbExecutor,
                   std::string redisAlertQueueKey);
    void setSubscriptionChangeCallback(std::function<void()> cb) {
        onSubscriptionChange_ = std::move(cb);
    }
    void setTriggerHandler(std::function<void(const TriggeredAlert &)> cb) {
        onTriggered_ = std::move(cb);
    }
    void setPrevDayLevelProvider(market::PrevDayLevelProvider *provider) {
        dolProvider_ = provider;
    }
    void setCTraderClient(ctrader::CTraderClient *client) { ctrader_ = client; }
    void setSymbolRegistry(ctrader::SymbolRegistry *registry) { registry_ = registry; }
    void setDbLoop(trantor::EventLoop *loop) { dbLoop_ = loop; }
    // Install an alert in memory without writing Postgres. Used by replay tests.
    void cacheAlert(Alert alert);
    void loadAlerts();
    bool dbPersistenceEnabled() const { return postgres_ != nullptr; }

    uint64_t userAlertsRevision(const std::string &userId) const;

    Alert createPriceAlert(const std::string &pair, double targetPrice,
                           const std::string &condition, const std::string &userId,
                           const std::string &email,
                           const std::vector<std::string> &channels,
                           const std::string &phone, const std::string &customMessage,
                           const std::string &expiresAt,
                           std::optional<std::string> dependsOnAlertId = std::nullopt);
    Alert createCandleAlert(const std::string &pair, const std::string &interval,
                            const std::string &direction, double threshold,
                            const std::string &userId, const std::string &email,
                            const std::vector<std::string> &channels,
                            const std::string &phone, const std::string &customMessage,
                            const std::string &expiresAt,
                            std::optional<std::string> dependsOnAlertId = std::nullopt);
    Alert createDrawAlert(const std::string &pair, const std::string &levelRef,
                          const std::vector<std::string> &dolTriggers, const std::string &userId,
                          const std::string &email,
                          const std::vector<std::string> &channels,
                          const std::string &phone, const std::string &customMessage,
                          const std::optional<std::string> &batchId,
                          const std::string &expiresAt,
                          std::optional<std::string> dependsOnAlertId = std::nullopt);
    Alert createStructureSessionAlert(const std::string &pair,
                                      const std::vector<std::string> &intervals,
                                      const std::vector<std::string> &structureEvents,
                                      const std::string &structureDirection,
                                      const std::string &userId, const std::string &email,
                                      const std::vector<std::string> &channels,
                                      const std::string &phone, const std::string &customMessage,
                                      const std::string &expiresAt,
                                      std::optional<double> minSwingAtr = std::nullopt,
                                      std::optional<double> breakK = std::nullopt);
    // One independent session alert per pair. Same settings, shared batch id when
    // there is more than one pair. Saves all of them or none.
    std::vector<Alert> createSweepConfirmAlerts(
        const std::vector<std::string> &pairs, const std::vector<std::string> &confirmations,
        const std::string &direction, const std::string &userId, const std::string &email,
        const std::vector<std::string> &channels, const std::string &phone,
        const std::string &customMessage, const std::string &expiresAt,
        std::optional<double> minSwingAtr = std::nullopt,
        std::optional<double> breakK = std::nullopt);
    // hour_sweep_cisd: one independent alert per pair. direction is bull, bear, or any.
    std::vector<Alert> createHourSweepCisdAlerts(
        const std::vector<std::string> &pairs, const std::string &direction,
        const std::string &userId, const std::string &email,
        const std::vector<std::string> &channels, const std::string &phone,
        const std::string &customMessage, const std::string &expiresAt);
    std::vector<Alert> createStructureSessionAlerts(
        const std::vector<std::string> &pairs, const std::vector<std::string> &intervals,
        const std::vector<std::string> &structureEvents, const std::string &structureDirection,
        const std::string &userId, const std::string &email,
        const std::vector<std::string> &channels, const std::string &phone,
        const std::string &customMessage, const std::string &expiresAt,
        std::optional<double> minSwingAtr = std::nullopt,
        std::optional<double> breakK = std::nullopt);
    Alert createStructureAlert(const std::string &pair, const std::string &interval,
                               const std::vector<std::string> &structureEvents,
                               const std::string &structureDirection,
                               const std::string &userId, const std::string &email,
                               const std::vector<std::string> &channels,
                               const std::string &phone, const std::string &customMessage,
                               const std::string &expiresAt,
                               std::optional<double> minSwingAtr = std::nullopt,
                               std::optional<double> breakK = std::nullopt,
                               std::optional<std::string> dependsOnAlertId = std::nullopt);

    void ingestStructureHistory(const std::string &pair, const std::string &interval,
                                const std::vector<Json::Value> &candles);

    std::optional<Alert> getAlert(const std::string &id) const;
    std::vector<Alert> getAllAlerts() const;
    std::vector<Alert> getActiveAlerts() const;
    std::vector<Alert> getAllAlertsForUser(const std::string &userId) const;
    std::vector<Alert> getActiveAlertsForUser(const std::string &userId) const;
    std::vector<Alert> getActiveAlertsSortedForUser(const std::string &userId) const;
    bool isAlertOwnedBy(const std::string &id, const std::string &userId) const;

    bool deleteAlert(const std::string &id, const std::optional<std::string> &userId);
    std::optional<Alert> updateAlert(const std::string &id, const Json::Value &updates,
                                     const std::optional<std::string> &userId);

    std::vector<TriggeredAlert> checkPriceAlerts(const std::vector<market::FlatPair> &pairs);
    // Evaluate one active price alert against a live quote (e.g. right after create).
    std::optional<TriggeredAlert> tryTriggerPriceAlert(const std::string &alertId,
                                                       double currentPrice);
    // candles: list of {pair, interval, timestamp(iso or epoch sec), close}
    std::vector<TriggeredAlert> checkCandleAlerts(const std::vector<Json::Value> &candles);

    // Expire active prev_day_level alerts whose create UTC day is before today.
    // Silent (no notification). Returns how many were expired.
    int expireStalePrevDayAlerts();
    // Expire any active alert whose expires_at is in the past. Silent.
    int expireTimedOutAlerts();

    int flushPersistenceEvents(int batchSize);

  private:
    struct StructureTrack {
        std::vector<market::StructureCandle> candles;
        std::unordered_map<std::string, market::IncrementalStructure> engines;
    };

    void rebuildIndexes();
    void persistAlert(const Alert &a);
    void persistAlertThen(const Alert &a, std::function<void(bool)> done);
    bool persistAlertSync(const Alert &a);
    bool persistDeleteSync(const std::string &id);
    void persistDelete(const std::string &id);
    void triggerAlert(Alert &a, double price,
                      const std::optional<std::string> &triggeredAtIso = std::nullopt);
    // Link a new alert into a same-pair queue. Caller must hold mu_.
    // Returns parent snapshot to persist when chain_id was backfilled.
    std::optional<Alert> applyDependsOnLocked(Alert &a, const std::string &userId,
                                              const std::optional<std::string> &dependsOnAlertId);
    // Arm waiting children of parentId. Caller must hold mu_. Returns before/after pairs.
    std::vector<std::pair<Alert, Alert>> armDependentsLocked(
        const std::string &parentId, const std::optional<std::string> &skipCandleTs);
    // Persist armed dependents (acquires lock).
    void armDependentAlerts(const std::string &parentId,
                            const std::optional<std::string> &skipCandleTs = std::nullopt);
    static bool priceConditionMet(const Alert &a, double current);
    void bumpUserRevision(const std::string &userId);
    void notifySubscriptionChange();
    static std::string candleIndexKey(const std::string &pair, const std::string &interval);
    void appendStructureCandle(StructureTrack &track, market::StructureCandle bar);
    market::IncrementalStructure &structureEngine(StructureTrack &track,
                                                  const market::StructureOptions &opt);

    static int intervalSeconds(const std::string &interval);

    enum class SessionStepResult { Unchanged, Updated, Triggered };
    // Insert every alert, or delete the ones already saved and remove them all.
    void commitCreatedAlerts(const std::vector<Alert> &built);
    SessionStepResult evalSweepConfirmLocked(Alert &a, const Json::Value &candle,
                                             const std::string &candleTsStr,
                                             StructureTrack &track);
    SessionStepResult evalHourSweepCisdLocked(Alert &a, const Json::Value &candle,
                                              const std::string &candleTsStr,
                                              StructureTrack &track);
    // Caller holds mu_. Mutates a when the session day rolls or a step matches.
    SessionStepResult evalStructureSessionLocked(Alert &a, const std::string &interval,
                                                 const Json::Value &candle,
                                                 const std::string &candleTsStr,
                                                 StructureTrack &track);

    // Same-day 1m lookback: if PDH/PDL already touched today, fire with that time.
    void scheduleSweepLookback(const std::string &alertId, int attempt,
                               std::function<void()> onComplete = {});
    void runSweepLookback(const std::string &alertId, int attempt,
                          std::function<void()> onComplete);
    bool finalizeSweepLookbackTrigger(const std::string &alertId, double touchPrice,
                                      const std::string &touchedAtIso);
    void clearSweepLookbackPending(const std::string &alertId);

    mutable std::mutex mu_;
    std::unordered_map<std::string, Alert> alerts_;
    std::unordered_map<std::string, std::vector<std::string>> activePriceIndex_;
    std::unordered_map<std::string, std::vector<std::string>> activeCandleIndex_;
    std::unordered_map<std::string, std::vector<std::string>> activeDolIndex_;
    std::unordered_map<std::string, uint64_t> userAlertsRevision_;
    std::unordered_map<std::string, bool> sweepLookbackPending_;
    std::unordered_map<std::string, StructureTrack> structureTracks_;
    std::map<std::time_t, std::vector<std::string>> expiryByMinute_;
    std::vector<std::string> activePrevDayIds_;

    std::function<void()> onSubscriptionChange_;
    std::function<void(const TriggeredAlert &)> onTriggered_;

    services::PostgresService *postgres_ = nullptr;
    services::RedisService *redis_ = nullptr;
    market::PrevDayLevelProvider *dolProvider_ = nullptr;
    ctrader::CTraderClient *ctrader_ = nullptr;
    ctrader::SymbolRegistry *registry_ = nullptr;
    std::function<void(std::function<void()>)> dbExecutor_;
    trantor::EventLoop *dbLoop_ = nullptr;
    std::string redisAlertQueueKey_ = "fx:alerts:events";
};

}  // namespace ctraderplus::alerts
