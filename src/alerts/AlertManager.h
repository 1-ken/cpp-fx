#pragma once

#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <json/json.h>

#include "alerts/Alert.h"

namespace ctraderplus::services {
class PostgresService;
class RedisService;
}
namespace ctraderplus::market {
struct FlatPair;
}

namespace ctraderplus::alerts {

// Triggered alert payload passed to notification dispatch.
struct TriggeredAlert {
    Alert alert;
    double currentPrice = 0;
    std::string timeframe;
    std::string alertTypeLabel = "price";
};

// Port of app/services/alert_service.py AlertManager.
class AlertManager {
  public:
    void configure(services::PostgresService *pg, services::RedisService *redis,
                   std::function<void(std::function<void()>)> dbExecutor,
                   std::string redisAlertQueueKey);
    void loadAlerts();

    Alert createPriceAlert(const std::string &pair, double targetPrice,
                           const std::string &condition, const std::string &userId,
                           const std::string &email, const std::string &channel,
                           const std::string &phone, const std::string &customMessage);
    Alert createCandleAlert(const std::string &pair, const std::string &interval,
                            const std::string &direction, double threshold,
                            const std::string &userId, const std::string &email,
                            const std::string &channel, const std::string &phone,
                            const std::string &customMessage);

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
    // candles: list of {pair, interval, timestamp(iso or epoch sec), close}
    std::vector<TriggeredAlert> checkCandleAlerts(const std::vector<Json::Value> &candles);

    int flushPersistenceEvents(int batchSize);

  private:
    void rebuildIndexes();
    void persistAlert(const Alert &a);
    void persistDelete(const std::string &id);
    void triggerAlert(Alert &a, double price);

    static int intervalSeconds(const std::string &interval);

    mutable std::mutex mu_;
    std::unordered_map<std::string, Alert> alerts_;
    std::unordered_map<std::string, std::vector<std::string>> activePriceIndex_;

    services::PostgresService *postgres_ = nullptr;
    services::RedisService *redis_ = nullptr;
    std::function<void(std::function<void()>)> dbExecutor_;
    std::string redisAlertQueueKey_ = "fx:alerts:events";
};

}  // namespace ctraderplus::alerts
