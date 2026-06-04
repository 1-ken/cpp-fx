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

struct UserRow {
    std::string userId;
    std::string username;
    std::string passwordHash;
    std::string email;
    std::string displayName;
    std::string avatarUrl;
    std::string authProvider;
    std::time_t createdAt = 0;
    bool disabled = false;
};

// Synchronous PostgreSQL access (run on a dedicated worker loop). Ported from
// app/services/postgres_service.py. Methods throw drogon::orm::DrogonDbException
// on failure; callers should catch.
class PostgresService {
  public:
    explicit PostgresService(const core::Config &cfg);

    bool connect();
    bool available() const { return client_ != nullptr; }
    // Applies versioned migrations; returns highest applied version.
    int runMigrations();
    void initSchema();  // deprecated alias for runMigrations()

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

    std::pair<int, int> deleteOldData(int daysToKeep);

    void upsertAlert(const Json::Value &alert);
    bool deleteAlert(const std::string &alertId);
    std::vector<Json::Value> listAlerts();

    UserStateRow getOrCreateUserState(const std::string &userId);
    UserStateRow completeUserOnboarding(const std::string &userId);

    // Users / auth
    UserRow createUser(const std::string &userId, const std::string &username,
                       const std::string &passwordHash);
    std::optional<UserRow> findUserByUsername(const std::string &username);
    void upsertGoogleUser(const std::string &userId, const std::string &googleSub,
                          const std::string &email, const std::string &displayName,
                          const std::string &avatarUrl);

    // Favorites
    std::vector<std::string> listFavorites(const std::string &userId);
    std::vector<std::string> listAllFavoritePairs() const;
    void addFavorite(const std::string &userId, const std::string &pair);
    bool removeFavorite(const std::string &userId, const std::string &pair);

    // Activity log
    void logActivity(const std::string &userId, const std::string &eventType,
                     const std::string &ipAddress, const std::string &userAgent,
                     const Json::Value &metadata);

    // Admin aggregations
    Json::Value adminOverview();
    Json::Value adminListUsers();
    Json::Value adminListAlerts(const std::string &statusFilter, int limit);
    Json::Value adminListActivity(const std::string &eventTypeFilter, int limit);

  private:
    const core::Config &cfg_;
    drogon::orm::DbClientPtr client_;
};

}  // namespace ctraderplus::services
