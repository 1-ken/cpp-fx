#include "services/Migrations.h"

#include <algorithm>
#include <functional>
#include <stdexcept>
#include <vector>

#include <trantor/utils/Logger.h>

using namespace drogon::orm;

namespace ctraderplus::services {

namespace {

bool columnExists(const DbClientPtr &client, const std::string &table,
                  const std::string &column) {
    auto r = client->execSqlSync(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2 LIMIT 1",
        table, column);
    return r.size() > 0;
}

void migrateAlertsJsonb(const DbClientPtr &client) {
    client->execSqlSync("ALTER TABLE alerts ADD COLUMN IF NOT EXISTS data JSONB");

    if (!columnExists(client, "alerts", "channel")) return;

    client->execSqlSync(
        "UPDATE alerts SET data = jsonb_build_object("
        "'id', id,"
        "'user_id', COALESCE(user_id, 'legacy-unassigned'),"
        "'pair', pair,"
        "'status', status,"
        "'alert_type', COALESCE(alert_type, 'price'),"
        "'created_at', to_char(created_at AT TIME ZONE 'UTC', "
        "'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),"
        "'channel', COALESCE(channel, 'email'),"
        "'email', COALESCE(email, ''),"
        "'phone', COALESCE(phone, ''),"
        "'custom_message', COALESCE(custom_message, ''),"
        "'triggered_at', CASE WHEN triggered_at IS NULL THEN NULL ELSE to_char("
        "triggered_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') END,"
        "'last_checked_price', last_checked_price,"
        "'close_price', close_price,"
        "'target_price', target_price,"
        "'condition', condition,"
        "'interval', \"interval\","
        "'direction', direction,"
        "'threshold', threshold,"
        "'last_evaluated_candle_time', CASE WHEN last_evaluated_candle_time IS NULL "
        "THEN NULL ELSE to_char(last_evaluated_candle_time AT TIME ZONE 'UTC', "
        "'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') END"
        ") WHERE data IS NULL");

    auto r = client->execSqlSync(
        "SELECT COUNT(*)::bigint AS n FROM alerts WHERE data IS NOT NULL");
    if (r.size() > 0) {
        LOG_INFO << "Migrated legacy alerts rows to JSONB data column (total with data: "
                 << r[0]["n"].as<long long>() << ")";
    }
}

void migrationBaseline(const DbClientPtr &client) {
    client->execSqlSync(
        "CREATE TABLE IF NOT EXISTS historical_prices ("
        "id BIGSERIAL PRIMARY KEY,"
        "pair VARCHAR(64) NOT NULL,"
        "price DOUBLE PRECISION NOT NULL,"
        "observed_at TIMESTAMPTZ NOT NULL)");
    client->execSqlSync(
        "CREATE INDEX IF NOT EXISTS ix_hist_pair_time ON historical_prices(pair, observed_at)");
    client->execSqlSync(
        "CREATE TABLE IF NOT EXISTS stream_metrics ("
        "id BIGSERIAL PRIMARY KEY,"
        "observed_at TIMESTAMPTZ NOT NULL,"
        "ws_subscriber_count INT NOT NULL DEFAULT 0,"
        "queue_subscriber_count INT NOT NULL DEFAULT 0,"
        "snapshot_failure_count INT NOT NULL DEFAULT 0,"
        "stream_status VARCHAR(32) NOT NULL DEFAULT 'healthy')");
    client->execSqlSync(
        "CREATE INDEX IF NOT EXISTS ix_metrics_time ON stream_metrics(observed_at)");
    client->execSqlSync(
        "CREATE TABLE IF NOT EXISTS alerts ("
        "id VARCHAR(64) PRIMARY KEY,"
        "user_id VARCHAR(128) NOT NULL DEFAULT 'legacy-unassigned',"
        "pair VARCHAR(64) NOT NULL,"
        "status VARCHAR(32) NOT NULL DEFAULT 'active',"
        "alert_type VARCHAR(32) NOT NULL DEFAULT 'price',"
        "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
        "data JSONB NOT NULL DEFAULT '{}'::jsonb)");
    client->execSqlSync("CREATE INDEX IF NOT EXISTS ix_alerts_user_id ON alerts(user_id)");
    client->execSqlSync(
        "CREATE TABLE IF NOT EXISTS user_states ("
        "user_id VARCHAR(128) PRIMARY KEY,"
        "first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
        "onboarding_completed_at TIMESTAMPTZ)");
    // Column/table names mirror the legacy Python/Alembic schema so a fresh C++
    // install and an existing shared database stay compatible.
    client->execSqlSync(
        "CREATE TABLE IF NOT EXISTS users ("
        "user_id VARCHAR(128) PRIMARY KEY,"
        "username VARCHAR(128) UNIQUE,"
        "password_hash TEXT,"
        "email VARCHAR(256),"
        "display_name VARCHAR(256),"
        "avatar_url TEXT,"
        "auth_provider VARCHAR(32) NOT NULL DEFAULT 'credentials',"
        "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())");
    client->execSqlSync(
        "CREATE INDEX IF NOT EXISTS ix_users_username ON users(username)");
    client->execSqlSync(
        "CREATE TABLE IF NOT EXISTS user_favorites ("
        "id BIGSERIAL PRIMARY KEY,"
        "user_id VARCHAR(128) NOT NULL,"
        "pair VARCHAR(64) NOT NULL,"
        "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
        "CONSTRAINT uq_user_favorites_user_pair UNIQUE (user_id, pair))");
    client->execSqlSync(
        "CREATE INDEX IF NOT EXISTS ix_user_favorites_user_id ON user_favorites(user_id)");
    client->execSqlSync(
        "CREATE TABLE IF NOT EXISTS user_activity_log ("
        "id UUID PRIMARY KEY,"
        "user_id VARCHAR(128),"
        "event_type VARCHAR(64) NOT NULL,"
        "ip_address VARCHAR(64),"
        "user_agent VARCHAR(512),"
        "metadata JSONB NOT NULL DEFAULT '{}',"
        "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())");
    client->execSqlSync(
        "CREATE INDEX IF NOT EXISTS ix_user_activity_log_created_at "
        "ON user_activity_log(created_at DESC)");
    client->execSqlSync(
        "CREATE INDEX IF NOT EXISTS ix_user_activity_log_event_type "
        "ON user_activity_log(event_type)");
}

void migrationUsersGoogleSub(const DbClientPtr &client) {
    client->execSqlSync(
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS google_sub VARCHAR(128)");
    client->execSqlSync(
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_users_google_sub ON users(google_sub) "
        "WHERE google_sub IS NOT NULL");
}

struct MigrationStep {
    int version;
    const char *name;
    std::function<void(const DbClientPtr &)> apply;
};

void recordMigration(const DbClientPtr &client, int version, const char *name);

void ensureCppMigrationsTable(const DbClientPtr &client) {
    client->execSqlSync(
        "CREATE TABLE IF NOT EXISTS cpp_schema_migrations ("
        "version INT PRIMARY KEY,"
        "name VARCHAR(128) NOT NULL,"
        "applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())");
}

bool tableExists(const DbClientPtr &client, const std::string &table) {
    auto r = client->execSqlSync(
        "SELECT to_regclass('public.' || $1) IS NOT NULL AS ok", table);
    return r.size() > 0 && r[0]["ok"].as<bool>();
}

// Infer how far a legacy Python/Alembic database already is (without touching
// schema_migrations, which uses varchar version labels in that stack).
int inferLegacySchemaVersion(const DbClientPtr &client) {
    int version = 0;
    if (tableExists(client, "historical_prices") || tableExists(client, "users"))
        version = 1;
    if (!columnExists(client, "alerts", "data")) return version;
    version = 2;
    if (columnExists(client, "users", "google_sub")) version = 3;
    return version;
}

void reconcileTrackedVersion(const DbClientPtr &client, int tracked, int actual) {
    if (tracked <= actual) return;
    LOG_WARN << "cpp_schema_migrations had v" << tracked << " but schema is v" << actual
             << "; re-applying pending migrations";
    client->execSqlSync("DELETE FROM cpp_schema_migrations WHERE version > $1", actual);
}

void seedCppMigrations(const DbClientPtr &client, int upToVersion,
                       const std::vector<MigrationStep> &steps) {
    for (const auto &step : steps) {
        if (step.version > upToVersion) break;
        recordMigration(client, step.version, step.name);
    }
}

int currentMigrationVersion(const DbClientPtr &client) {
    ensureCppMigrationsTable(client);
    auto r = client->execSqlSync(
        "SELECT COALESCE(MAX(version), 0)::int AS v FROM cpp_schema_migrations");
    if (r.size() == 0) return 0;
    int tracked = r[0]["v"].as<int>();
    int actual = inferLegacySchemaVersion(client);
    if (tracked > 0) {
        reconcileTrackedVersion(client, tracked, actual);
        return std::min(tracked, actual);
    }

    int legacy = actual;
    if (legacy > 0) {
        LOG_INFO << "Detected legacy PostgreSQL schema at version " << legacy
                 << "; recording in cpp_schema_migrations";
    }
    return legacy;
}

void recordMigration(const DbClientPtr &client, int version, const char *name) {
    client->execSqlSync(
        "INSERT INTO cpp_schema_migrations(version, name) VALUES($1, $2) "
        "ON CONFLICT (version) DO NOTHING",
        version, name);
}

}  // namespace

int runMigrations(const DbClientPtr &client) {
    if (!client) throw std::runtime_error("PostgreSQL client is null");

    const std::vector<MigrationStep> steps = {
        {1, "baseline", migrationBaseline},
        {2, "alerts_jsonb", migrateAlertsJsonb},
        {3, "users_google_sub", migrationUsersGoogleSub},
    };

    int current = currentMigrationVersion(client);
    int appliedMax = current;

    if (current > 0) {
        auto tracked = client->execSqlSync(
            "SELECT COUNT(*)::bigint AS n FROM cpp_schema_migrations");
        if (tracked.size() > 0 && tracked[0]["n"].as<long long>() == 0) {
            seedCppMigrations(client, current, steps);
        }
    }

    for (const auto &step : steps) {
        if (step.version <= current) continue;
        LOG_INFO << "Applying migration v" << step.version << " " << step.name;
        step.apply(client);
        recordMigration(client, step.version, step.name);
        appliedMax = step.version;
        LOG_INFO << "Migration v" << step.version << " " << step.name << " applied";
    }

    return appliedMax;
}

}  // namespace ctraderplus::services
