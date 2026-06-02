#pragma once

#include <cstdint>
#include <ctime>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <json/json.h>
#include <drogon/orm/DbClient.h>

#include "core/Config.h"

namespace ctraderplus::services {

struct HistoricalRow {
    std::string pair;
    double price = 0;
    std::time_t observedAt = 0;
};

struct StreamMetricRow {
    std::time_t observedAt = 0;
    int wsSubscriberCount = 0;
    int queueSubscriberCount = 0;
    int snapshotFailureCount = 0;
    std::string streamStatus;
};

struct UserStateRow {
    std::string userId;
    std::time_t firstSeenAt = 0;
    std::optional<std::time_t> onboardingCompletedAt;
};

// Synchronous PostgreSQL access (run on a dedicated worker loop). Ported from
// app/services/postgres_service.py. Methods throw drogon::orm::DrogonDbException
// on failure; callers should catch.
class PostgresService {
  public:
    explicit PostgresService(const core::Config &cfg);

    bool connect();
    bool available() const { return client_ != nullptr; }
    void initSchema();

    // Archive: insert flat pairs from snapshot JSON strings. Returns row count.
    int insertSnapshots(const std::vector<std::string> &snapshotJsons);

    std::vector<HistoricalRow> queryHistory(const std::string &pair,
                                            std::optional<std::time_t> start,
                                            std::optional<std::time_t> end, int limit,
                                            bool descending);

    void insertStreamMetric(std::time_t observedAt, int ws, int queue, int failures,
                            const std::string &status);
    std::vector<StreamMetricRow> queryStreamMetrics(std::optional<std::time_t> start,
                                                    std::optional<std::time_t> end,
                                                    int limit, bool descending);

    // Returns {historical_deleted, metrics_deleted}.
    std::pair<int, int> deleteOldData(int daysToKeep);

    // Alert persistence (payload = alert.to_dict()).
    void upsertAlert(const Json::Value &alert);
    bool deleteAlert(const std::string &alertId);
    std::vector<Json::Value> listAlerts();

    UserStateRow getOrCreateUserState(const std::string &userId);
    UserStateRow completeUserOnboarding(const std::string &userId);

  private:
    const core::Config &cfg_;
    drogon::orm::DbClientPtr client_;
};

}  // namespace ctraderplus::services
