#include "services/Migrations.h"

#include <algorithm>
#include <cctype>
#include <stdexcept>
#include <vector>

#include <trantor/utils/Logger.h>

using namespace drogon::orm;

namespace ctraderplus::services {

namespace {

constexpr int kSchemaReconcileVersion = 7;

bool columnExists(const DbClientPtr &client, const std::string &table,
                  const std::string &column) {
    auto r = client->execSqlSync(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2 LIMIT 1",
        table, column);
    return r.size() > 0;
}

bool tableExists(const DbClientPtr &client, const std::string &table) {
    auto r = client->execSqlSync(
        "SELECT to_regclass('public.' || $1) IS NOT NULL AS ok", table);
    return r.size() > 0 && r[0]["ok"].as<bool>();
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
}

void reconcileAlertsLegacyNullable(const DbClientPtr &client) {
    static const std::vector<std::pair<std::string, bool>> legacyCols = {
        {"channel", false},       {"email", false},         {"phone", false},
        {"custom_message", false}, {"triggered_at", false}, {"last_checked_price", false},
        {"close_price", false},   {"target_price", false},  {"condition", false},
        {"interval", true},       {"direction", false},    {"threshold", false},
        {"last_evaluated_candle_time", false},
    };
    for (const auto &[col, quoted] : legacyCols) {
        if (!columnExists(client, "alerts", col)) continue;
        const std::string sqlCol = quoted ? "\"" + col + "\"" : col;
        client->execSqlSync("ALTER TABLE alerts ALTER COLUMN " + sqlCol + " DROP NOT NULL");
    }
}

void reconcileLegacyPricingUsers(const DbClientPtr &client) {
    // Pre-pricing users: never logged in since last_login column existed (NULL),
    // already onboarded, but subscription state was backfilled incorrectly.
    auto r = client->execSqlSync(
        "UPDATE user_states us SET "
        "pricing_intro_required = TRUE, "
        "subscription_tier = 'none', "
        "trial_started_at = NULL, "
        "tour_completed_at = NULL, "
        "paywall_dismissed_at = NULL "
        "FROM users u "
        "WHERE us.user_id = u.user_id "
        "AND us.pricing_intro_required = FALSE "
        "AND us.onboarding_completed_at IS NOT NULL "
        "AND u.last_login IS NULL "
        "AND (us.trial_started_at IS NOT NULL "
        "OR us.subscription_tier IN ('trial', 'free') "
        "OR us.tour_completed_at IS NOT NULL)");
    if (r.affectedRows() > 0) {
        LOG_INFO << "Reconciled legacy pre-pricing users for tour-first trial (rows="
                 << r.affectedRows() << ")";
    }
}

std::string trimMigrationStr(const std::string &s) {
    size_t a = s.find_first_not_of(" \t\r\n");
    if (a == std::string::npos) return "";
    size_t b = s.find_last_not_of(" \t\r\n");
    return s.substr(a, b - a + 1);
}

bool migrationUsernameTaken(const DbClientPtr &client, const std::string &username,
                            const std::string &excludeUserId) {
    auto r = client->execSqlSync(
        "SELECT 1 FROM users WHERE username = $1 AND user_id <> $2 LIMIT 1", username,
        excludeUserId);
    return r.size() > 0;
}

std::string migrationResolveUniqueUsername(const DbClientPtr &client,
                                           const std::string &candidate,
                                           const std::string &excludeUserId) {
    std::string base = candidate;
    if (base.size() > 128) base = base.substr(0, 128);
    if (!migrationUsernameTaken(client, base, excludeUserId)) return base;
    for (int suffix = 2; suffix < 1000; ++suffix) {
        std::string attempt = base;
        std::string tag = "-" + std::to_string(suffix);
        if (attempt.size() + tag.size() > 128) {
            attempt = attempt.substr(0, 128 - tag.size());
        }
        attempt += tag;
        if (!migrationUsernameTaken(client, attempt, excludeUserId)) return attempt;
    }
    return base.substr(0, 120) + "-x";
}

void reconcileGoogleUsernames(const DbClientPtr &client) {
    auto rows = client->execSqlSync(
        "SELECT user_id, COALESCE(display_name, '') AS display_name "
        "FROM users "
        "WHERE auth_provider = 'google' "
        "AND username LIKE 'google:%' "
        "AND display_name IS NOT NULL "
        "AND TRIM(display_name) <> '' "
        "ORDER BY created_at ASC");
    int updated = 0;
    for (const auto &row : rows) {
        std::string userId = row["user_id"].as<std::string>();
        std::string displayName = trimMigrationStr(row["display_name"].as<std::string>());
        if (displayName.empty()) continue;
        std::string username =
            migrationResolveUniqueUsername(client, displayName, userId);
        client->execSqlSync(
            "UPDATE users SET username = $2, display_name = $2 WHERE user_id = $1", userId,
            username);
        ++updated;
    }
    if (updated > 0) {
        LOG_INFO << "Reconciled Google usernames from display_name (rows=" << updated << ")";
    }
}

void reconcileApplicationSchema(const DbClientPtr &client) {
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

    client->execSqlSync(
        "CREATE TABLE IF NOT EXISTS cpp_schema_migrations ("
        "version INT PRIMARY KEY,"
        "name VARCHAR(128) NOT NULL,"
        "applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())");

    client->execSqlSync(
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS google_sub VARCHAR(128)");
    client->execSqlSync(
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_users_google_sub ON users(google_sub) "
        "WHERE google_sub IS NOT NULL");
    client->execSqlSync(
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_login TIMESTAMPTZ");

    client->execSqlSync("ALTER TABLE alerts ADD COLUMN IF NOT EXISTS data JSONB");
    client->execSqlSync(
        "ALTER TABLE alerts ALTER COLUMN data SET DEFAULT '{}'::jsonb");
    if (columnExists(client, "alerts", "channel")) {
        migrateAlertsJsonb(client);
    } else {
        client->execSqlSync(
            "UPDATE alerts SET data = '{}'::jsonb WHERE data IS NULL");
    }
    reconcileAlertsLegacyNullable(client);

    client->execSqlSync(
        "ALTER TABLE user_states ADD COLUMN IF NOT EXISTS tour_completed_at TIMESTAMPTZ");
    client->execSqlSync(
        "ALTER TABLE user_states ADD COLUMN IF NOT EXISTS trial_started_at TIMESTAMPTZ");
    client->execSqlSync(
        "ALTER TABLE user_states ADD COLUMN IF NOT EXISTS subscription_tier "
        "VARCHAR(32) NOT NULL DEFAULT 'none'");
    client->execSqlSync(
        "ALTER TABLE user_states ADD COLUMN IF NOT EXISTS paywall_dismissed_at TIMESTAMPTZ");
    client->execSqlSync(
        "ALTER TABLE user_states ADD COLUMN IF NOT EXISTS pricing_intro_required "
        "BOOLEAN NOT NULL DEFAULT FALSE");

    client->execSqlSync(
        "CREATE TABLE IF NOT EXISTS user_daily_usage ("
        "user_id VARCHAR(128) NOT NULL,"
        "usage_date DATE NOT NULL,"
        "sms_sent INT NOT NULL DEFAULT 0,"
        "calls_made INT NOT NULL DEFAULT 0,"
        "PRIMARY KEY (user_id, usage_date))");
    client->execSqlSync(
        "CREATE INDEX IF NOT EXISTS ix_user_daily_usage_user_date "
        "ON user_daily_usage(user_id, usage_date)");

    client->execSqlSync(
        "CREATE TABLE IF NOT EXISTS marketers ("
        "code VARCHAR(64) PRIMARY KEY,"
        "name VARCHAR(256) NOT NULL,"
        "active BOOLEAN NOT NULL DEFAULT true,"
        "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())");

    client->execSqlSync(
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS referred_by_marketer_code VARCHAR(64)");
    client->execSqlSync(
        "CREATE INDEX IF NOT EXISTS ix_users_referred_by_marketer "
        "ON users(referred_by_marketer_code) "
        "WHERE referred_by_marketer_code IS NOT NULL");

    client->execSqlSync(
        "CREATE TABLE IF NOT EXISTS user_feedback ("
        "id UUID PRIMARY KEY,"
        "user_id VARCHAR(128) NOT NULL,"
        "enjoying BOOLEAN NOT NULL,"
        "improvements TEXT,"
        "source VARCHAR(64) NOT NULL DEFAULT 'alert_create',"
        "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())");
    client->execSqlSync(
        "CREATE INDEX IF NOT EXISTS ix_user_feedback_created_at "
        "ON user_feedback(created_at DESC)");

    client->execSqlSync(
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS phone VARCHAR(32)");

    reconcileLegacyPricingUsers(client);
    reconcileGoogleUsernames(client);
}

void validateApplicationSchema(const DbClientPtr &client) {
    if (!tableExists(client, "alerts")) {
        throw std::runtime_error("Schema validation failed: alerts table missing");
    }
    if (!columnExists(client, "alerts", "data")) {
        throw std::runtime_error("Schema validation failed: alerts.data column missing");
    }
    const std::vector<std::string> required = {"id", "user_id", "pair", "status",
                                               "alert_type", "created_at", "data"};
    for (const auto &col : required) {
        if (!columnExists(client, "alerts", col)) {
            throw std::runtime_error("Schema validation failed: alerts." + col +
                                     " column missing");
        }
    }
    if (columnExists(client, "alerts", "channel")) {
        auto r = client->execSqlSync(
            "SELECT is_nullable FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'alerts' "
            "AND column_name = 'channel' LIMIT 1");
        if (r.size() > 0 && r[0]["is_nullable"].as<std::string>() == "NO") {
            throw std::runtime_error(
                "Schema validation failed: alerts.channel is NOT NULL");
        }
    }
    if (!columnExists(client, "users", "last_login")) {
        throw std::runtime_error("Schema validation failed: users.last_login missing");
    }
    if (!columnExists(client, "user_states", "pricing_intro_required")) {
        throw std::runtime_error(
            "Schema validation failed: user_states.pricing_intro_required missing");
    }
    if (!tableExists(client, "user_feedback")) {
        throw std::runtime_error("Schema validation failed: user_feedback table missing");
    }
    LOG_INFO << "PostgreSQL schema validated (reconcile compatible)";
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

    LOG_INFO << "Reconciling PostgreSQL application schema...";
    reconcileApplicationSchema(client);
    validateApplicationSchema(client);
    recordMigration(client, kSchemaReconcileVersion, "schema_reconcile");
    LOG_INFO << "Schema reconcile complete (version=" << kSchemaReconcileVersion << ")";
    return kSchemaReconcileVersion;
}

}  // namespace ctraderplus::services
