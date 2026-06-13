#include "services/PostgresService.h"

#include <memory>
#include <optional>
#include <sstream>

#include <trantor/utils/Logger.h>

#include "services/Migrations.h"
#include "util/PairNormalizer.h"
#include "util/TimeUtil.h"
#include "util/Uuid.h"

using namespace drogon::orm;

namespace ctraderplus::services {

namespace {
double parsePrice(const Json::Value &v) {
    if (v.isNumeric()) return v.asDouble();
    if (v.isString()) {
        std::string s = v.asString();
        std::string cleaned;
        for (char c : s)
            if (c != ',') cleaned.push_back(c);
        try {
            return std::stod(cleaned);
        } catch (...) {
            return 0;
        }
    }
    return 0;
}

Json::Value parseJson(const std::string &s) {
    Json::Value root;
    Json::CharReaderBuilder b;
    std::unique_ptr<Json::CharReader> reader(b.newCharReader());
    std::string errs;
    reader->parse(s.c_str(), s.c_str() + s.size(), &root, &errs);
    return root;
}

std::string redactPostgresDsn(const std::string &dsn) {
    const auto scheme = dsn.find("://");
    const auto at = dsn.find('@');
    if (scheme == std::string::npos || at == std::string::npos || at <= scheme + 3)
        return dsn;
    const auto colon = dsn.find(':', scheme + 3);
    if (colon == std::string::npos || colon >= at) return dsn;
    return dsn.substr(0, colon + 1) + "***" + dsn.substr(at);
}

std::string dsnWithConnectTimeout(const std::string &dsn, int seconds) {
    if (dsn.find("connect_timeout=") != std::string::npos) return dsn;
    const char sep = (dsn.find('?') != std::string::npos) ? '&' : '?';
    return dsn + sep + "connect_timeout=" + std::to_string(seconds);
}

}  // namespace

PostgresService::PostgresService(const core::Config &cfg) : cfg_(cfg) {}

bool PostgresService::connect() {
    if (cfg_.postgresDsn.empty()) {
        LOG_WARN << "DATABASE_URL not set; PostgreSQL disabled";
        return false;
    }
    constexpr double kProbeTimeoutSec = 10.0;
    constexpr int kConnectTimeoutSec = 5;
    const std::string dsn = dsnWithConnectTimeout(cfg_.postgresDsn, kConnectTimeoutSec);
    LOG_INFO << "Connecting to PostgreSQL (" << redactPostgresDsn(dsn) << ")...";
    try {
        client_ = DbClient::newPgClient(dsn, static_cast<size_t>(std::max(1, cfg_.postgresConnNum)));
        if (!client_) {
            LOG_WARN << "PostgreSQL client creation returned null";
            return false;
        }
        // Apply a query timeout only for the startup probe so a bad DSN/down DB
        // fails fast instead of blocking forever on a buffered query. The timeout
        // path leaves a connection busy if it fires, which would slowly exhaust
        // the pool, so it is disabled again for steady-state queries below.
        client_->setTimeout(kProbeTimeoutSec);
        client_->execSqlSync("SELECT 1");
        client_->setTimeout(0.0);
        LOG_INFO << "PostgreSQL connected";
        return true;
    } catch (const std::exception &e) {
        LOG_WARN << "PostgreSQL connection test failed: " << e.what();
        client_ = nullptr;
        return false;
    }
}

int PostgresService::runMigrations() {
    if (!client_) throw std::runtime_error("PostgreSQL client is not available");
    return services::runMigrations(client_);
}

void PostgresService::initSchema() {
    (void)runMigrations();
}

int PostgresService::insertSnapshots(const std::vector<std::string> &snapshotJsons) {
    if (!client_) return 0;
    int count = 0;
    for (const auto &js : snapshotJsons) {
        Json::Value snap = parseJson(js);
        std::string ts = snap.get("ts", "").asString();
        auto epoch = util::parseIso8601(ts);
        double epochSec = epoch ? static_cast<double>(*epoch)
                                : static_cast<double>(std::time(nullptr));
        const Json::Value &pairs = snap["pairs"];
        if (!pairs.isArray()) continue;
        for (const auto &p : pairs) {
            std::string pair = util::canonicalPair(p.get("pair", "").asString());
            if (pair.empty()) continue;
            double price = parsePrice(p["price"]);
            try {
                client_->execSqlSync(
                    "INSERT INTO historical_prices(pair, price, observed_at) "
                    "VALUES($1, $2, to_timestamp($3))",
                    pair, price, epochSec);
                ++count;
            } catch (const std::exception &e) {
                LOG_DEBUG << "insertSnapshots row failed: " << e.what();
            }
        }
    }
    return count;
}

std::vector<HistoricalRow> PostgresService::queryHistory(const std::string &pair,
                                                         std::optional<std::time_t> start,
                                                         std::optional<std::time_t> end,
                                                         int limit, bool descending) {
    std::vector<HistoricalRow> out;
    if (!client_) return out;

    std::ostringstream sql;
    sql << "SELECT pair, price, EXTRACT(EPOCH FROM observed_at)::bigint AS epoch "
           "FROM historical_prices WHERE 1=1";
    std::vector<std::string> variants;
    if (!pair.empty()) {
        variants = util::pairVariants(pair);
        sql << " AND pair = ANY($1::text[])";
    }
    // Build ANY array literal for variants.
    std::string arrayLiteral = "{";
    for (size_t i = 0; i < variants.size(); ++i) {
        if (i) arrayLiteral += ",";
        arrayLiteral += "\"" + variants[i] + "\"";
    }
    arrayLiteral += "}";

    int nextParam = pair.empty() ? 1 : 2;
    int startParam = 0, endParam = 0;
    if (start) {
        startParam = nextParam++;
        sql << " AND observed_at >= to_timestamp($" << startParam << ")";
    }
    if (end) {
        endParam = nextParam++;
        sql << " AND observed_at <= to_timestamp($" << endParam << ")";
    }
    sql << (descending ? " ORDER BY observed_at DESC" : " ORDER BY observed_at ASC");
    sql << " LIMIT " << limit;

    try {
        Result r = [&]() {
            if (!pair.empty() && start && end)
                return client_->execSqlSync(sql.str(), arrayLiteral,
                                            static_cast<double>(*start),
                                            static_cast<double>(*end));
            if (!pair.empty() && start)
                return client_->execSqlSync(sql.str(), arrayLiteral,
                                            static_cast<double>(*start));
            if (!pair.empty() && end)
                return client_->execSqlSync(sql.str(), arrayLiteral,
                                            static_cast<double>(*end));
            if (!pair.empty())
                return client_->execSqlSync(sql.str(), arrayLiteral);
            if (start && end)
                return client_->execSqlSync(sql.str(), static_cast<double>(*start),
                                            static_cast<double>(*end));
            if (start)
                return client_->execSqlSync(sql.str(), static_cast<double>(*start));
            if (end)
                return client_->execSqlSync(sql.str(), static_cast<double>(*end));
            return client_->execSqlSync(sql.str());
        }();
        for (const auto &row : r) {
            HistoricalRow h;
            h.pair = row["pair"].as<std::string>();
            h.price = row["price"].as<double>();
            h.observedAt = static_cast<std::time_t>(row["epoch"].as<long long>());
            out.push_back(std::move(h));
        }
    } catch (const std::exception &e) {
        LOG_ERROR << "queryHistory failed: " << e.what();
    }
    return out;
}

void PostgresService::insertStreamMetric(std::time_t observedAt, int ws, int queue,
                                         int failures, const std::string &status) {
    if (!client_) return;
    try {
        client_->execSqlSync(
            "INSERT INTO stream_metrics(observed_at, ws_subscriber_count, "
            "queue_subscriber_count, snapshot_failure_count, stream_status) "
            "VALUES(to_timestamp($1), $2, $3, $4, $5)",
            static_cast<double>(observedAt), ws, queue, failures, status.substr(0, 32));
    } catch (const std::exception &e) {
        LOG_ERROR << "insertStreamMetric failed: " << e.what();
    }
}

std::vector<StreamMetricRow> PostgresService::queryStreamMetrics(
    std::optional<std::time_t> start, std::optional<std::time_t> end, int limit,
    bool descending) {
    std::vector<StreamMetricRow> out;
    if (!client_) return out;
    std::ostringstream sql;
    sql << "SELECT EXTRACT(EPOCH FROM observed_at)::bigint AS epoch, ws_subscriber_count, "
           "queue_subscriber_count, snapshot_failure_count, stream_status "
           "FROM stream_metrics WHERE 1=1";
    int p = 1;
    int startP = 0, endP = 0;
    if (start) {
        startP = p++;
        sql << " AND observed_at >= to_timestamp($" << startP << ")";
    }
    if (end) {
        endP = p++;
        sql << " AND observed_at <= to_timestamp($" << endP << ")";
    }
    sql << (descending ? " ORDER BY observed_at DESC" : " ORDER BY observed_at ASC");
    sql << " LIMIT " << limit;
    try {
        Result r = [&]() {
            if (start && end)
                return client_->execSqlSync(sql.str(), static_cast<double>(*start),
                                            static_cast<double>(*end));
            if (start)
                return client_->execSqlSync(sql.str(), static_cast<double>(*start));
            if (end) return client_->execSqlSync(sql.str(), static_cast<double>(*end));
            return client_->execSqlSync(sql.str());
        }();
        for (const auto &row : r) {
            StreamMetricRow m;
            m.observedAt = static_cast<std::time_t>(row["epoch"].as<long long>());
            m.wsSubscriberCount = row["ws_subscriber_count"].as<int>();
            m.queueSubscriberCount = row["queue_subscriber_count"].as<int>();
            m.snapshotFailureCount = row["snapshot_failure_count"].as<int>();
            m.streamStatus = row["stream_status"].as<std::string>();
            out.push_back(std::move(m));
        }
    } catch (const std::exception &e) {
        LOG_ERROR << "queryStreamMetrics failed: " << e.what();
    }
    return out;
}

std::pair<int, int> PostgresService::deleteOldData(int daysToKeep) {
    if (!client_) return {0, 0};
    int days = std::max(1, daysToKeep);
    int hist = 0, met = 0;
    try {
        auto r1 = client_->execSqlSync(
            "DELETE FROM historical_prices WHERE observed_at < NOW() - ($1 || ' days')::interval",
            std::to_string(days));
        hist = static_cast<int>(r1.affectedRows());
        auto r2 = client_->execSqlSync(
            "DELETE FROM stream_metrics WHERE observed_at < NOW() - ($1 || ' days')::interval",
            std::to_string(days));
        met = static_cast<int>(r2.affectedRows());
    } catch (const std::exception &e) {
        LOG_ERROR << "deleteOldData failed: " << e.what();
    }
    return {hist, met};
}

bool PostgresService::upsertAlert(const Json::Value &alert) {
    if (!client_) return false;
    std::string id = alert.get("id", "").asString();
    if (id.empty()) return false;
    std::string userId = alert.get("user_id", "legacy-unassigned").asString();
    std::string pair = alert.get("pair", "").asString();
    std::string status = alert.get("status", "active").asString();
    std::string alertType = alert.get("alert_type", "price").asString();
    std::string createdAt = alert.get("created_at", "").asString();
    auto epoch = util::parseIso8601(createdAt);
    double epochSec = epoch ? static_cast<double>(*epoch)
                            : static_cast<double>(std::time(nullptr));
    Json::StreamWriterBuilder wb;
    wb["indentation"] = "";
    std::string data = Json::writeString(wb, alert);

    auto runUpsert = [&]() {
        client_->execSqlSync(
            "INSERT INTO alerts(id, user_id, pair, status, alert_type, created_at, data) "
            "VALUES($1,$2,$3,$4,$5,to_timestamp($6),$7::jsonb) "
            "ON CONFLICT (id) DO UPDATE SET user_id=EXCLUDED.user_id, pair=EXCLUDED.pair, "
            "status=EXCLUDED.status, alert_type=EXCLUDED.alert_type, "
            "created_at=EXCLUDED.created_at, data=EXCLUDED.data",
            id, userId, pair, status, alertType, epochSec, data);
    };

    try {
        runUpsert();
        return true;
    } catch (const std::exception &e) {
        std::string msg = e.what();
        if (msg.find("current transaction is aborted") != std::string::npos ||
            msg.find("pipeline") != std::string::npos) {
            try {
                runUpsert();
                return true;
            } catch (const std::exception &e2) {
                ++alertUpsertFailures_;
                LOG_ERROR << "upsertAlert failed after retry: " << e2.what()
                          << " alert_id=" << id;
                return false;
            }
        }
        ++alertUpsertFailures_;
        LOG_ERROR << "upsertAlert failed: " << msg << " alert_id=" << id;
        return false;
    }
}

int PostgresService::countAlerts() const {
    if (!client_) return 0;
    try {
        auto r = client_->execSqlSync("SELECT COUNT(*)::int AS n FROM alerts");
        return r.empty() ? 0 : r[0]["n"].as<int>();
    } catch (...) {
        return 0;
    }
}

bool PostgresService::deleteAlert(const std::string &alertId) {
    if (!client_) return false;
    try {
        auto r = client_->execSqlSync("DELETE FROM alerts WHERE id=$1", alertId);
        return r.affectedRows() > 0;
    } catch (const std::exception &e) {
        LOG_ERROR << "deleteAlert failed: " << e.what();
        return false;
    }
}

std::vector<Json::Value> PostgresService::listAlerts() {
    std::vector<Json::Value> out;
    if (!client_) return out;
    try {
        auto r = client_->execSqlSync(
            "SELECT data::text AS data FROM alerts ORDER BY created_at DESC, id DESC");
        for (const auto &row : r) {
            Json::Value v = parseJson(row["data"].as<std::string>());
            if (!v.isMember("user_id") || v["user_id"].asString().empty())
                v["user_id"] = "legacy-unassigned";
            out.push_back(std::move(v));
        }
    } catch (const std::exception &e) {
        LOG_ERROR << "listAlerts failed: " << e.what();
    }
    return out;
}

UserStateRow PostgresService::getOrCreateUserState(const std::string &userId) {
    return getUserStateFull(userId);
}

UserStateRow PostgresService::getUserStateFull(const std::string &userId) {
    UserStateRow row;
    row.userId = userId;
    if (!client_) return row;
    try {
        client_->execSqlSync(
            "INSERT INTO user_states(user_id, first_seen_at) VALUES($1, NOW()) "
            "ON CONFLICT (user_id) DO NOTHING",
            userId);
        auto r = client_->execSqlSync(
            "SELECT EXTRACT(EPOCH FROM first_seen_at)::bigint AS fs, "
            "EXTRACT(EPOCH FROM onboarding_completed_at)::bigint AS oc, "
            "onboarding_completed_at IS NULL AS oc_null, "
            "EXTRACT(EPOCH FROM tour_completed_at)::bigint AS tc, "
            "tour_completed_at IS NULL AS tc_null, "
            "EXTRACT(EPOCH FROM trial_started_at)::bigint AS ts, "
            "trial_started_at IS NULL AS ts_null, "
            "COALESCE(subscription_tier, 'none') AS subscription_tier, "
            "COALESCE(pricing_intro_required, FALSE) AS pricing_intro_required, "
            "EXTRACT(EPOCH FROM paywall_dismissed_at)::bigint AS pd, "
            "paywall_dismissed_at IS NULL AS pd_null "
            "FROM user_states WHERE user_id=$1",
            userId);
        if (r.size() > 0) {
            const auto &rr = r[0];
            row.firstSeenAt = static_cast<std::time_t>(rr["fs"].as<long long>());
            if (!rr["oc_null"].as<bool>())
                row.onboardingCompletedAt =
                    static_cast<std::time_t>(rr["oc"].as<long long>());
            if (!rr["tc_null"].as<bool>())
                row.tourCompletedAt = static_cast<std::time_t>(rr["tc"].as<long long>());
            if (!rr["ts_null"].as<bool>())
                row.trialStartedAt = static_cast<std::time_t>(rr["ts"].as<long long>());
            row.subscriptionTier = rr["subscription_tier"].as<std::string>();
            row.pricingIntroRequired = rr["pricing_intro_required"].as<bool>();
            if (!rr["pd_null"].as<bool>())
                row.paywallDismissedAt = static_cast<std::time_t>(rr["pd"].as<long long>());
        }
    } catch (const std::exception &e) {
        LOG_ERROR << "getUserStateFull failed: " << e.what();
        throw;
    }
    return row;
}

UserStateRow PostgresService::completeUserOnboarding(const std::string &userId) {
    if (!client_) throw std::runtime_error("Database unavailable");
    try {
        client_->execSqlSync(
            "INSERT INTO user_states(user_id, first_seen_at, onboarding_completed_at) "
            "VALUES($1, NOW(), NOW()) "
            "ON CONFLICT (user_id) DO UPDATE SET "
            "onboarding_completed_at = COALESCE(user_states.onboarding_completed_at, NOW())",
            userId);
        client_->execSqlSync(
            "UPDATE user_states SET "
            "trial_started_at = COALESCE(trial_started_at, NOW()), "
            "subscription_tier = CASE "
            "WHEN subscription_tier IN ('none', '') THEN 'trial' "
            "ELSE subscription_tier END "
            "WHERE user_id = $1 AND pricing_intro_required = FALSE",
            userId);
        return getUserStateFull(userId);
    } catch (const std::exception &e) {
        LOG_ERROR << "completeUserOnboarding failed: " << e.what();
        throw;
    }
}

UserStateRow PostgresService::completeTour(const std::string &userId) {
    if (!client_) throw std::runtime_error("Database unavailable");
    try {
        client_->execSqlSync(
            "INSERT INTO user_states(user_id, first_seen_at, tour_completed_at) "
            "VALUES($1, NOW(), NOW()) "
            "ON CONFLICT (user_id) DO UPDATE SET "
            "tour_completed_at = COALESCE(user_states.tour_completed_at, NOW())",
            userId);
        client_->execSqlSync(
            "UPDATE user_states SET "
            "trial_started_at = COALESCE(trial_started_at, NOW()), "
            "subscription_tier = 'trial', "
            "pricing_intro_required = FALSE "
            "WHERE user_id = $1 AND pricing_intro_required = TRUE",
            userId);
        return getUserStateFull(userId);
    } catch (const std::exception &e) {
        LOG_ERROR << "completeTour failed: " << e.what();
        throw;
    }
}

void PostgresService::autoDowngradeIfTrialExpired(const std::string &userId) {
    if (!client_) return;
    try {
        client_->execSqlSync(
            "UPDATE user_states SET subscription_tier = 'free' "
            "WHERE user_id = $1 AND subscription_tier = 'trial' "
            "AND trial_started_at IS NOT NULL "
            "AND trial_started_at <= NOW() - INTERVAL '14 days'",
            userId);
    } catch (const std::exception &e) {
        LOG_ERROR << "autoDowngradeIfTrialExpired failed: " << e.what();
    }
}

void PostgresService::dismissPaywall(const std::string &userId) {
    if (!client_) throw std::runtime_error("Database unavailable");
    client_->execSqlSync(
        "UPDATE user_states SET paywall_dismissed_at = COALESCE(paywall_dismissed_at, NOW()) "
        "WHERE user_id = $1",
        userId);
}

void PostgresService::setSubscriptionTier(const std::string &userId, const std::string &tier) {
    if (!client_) throw std::runtime_error("Database unavailable");
    client_->execSqlSync("UPDATE user_states SET subscription_tier = $2 WHERE user_id = $1",
                         userId, tier);
}

DailyUsageRow PostgresService::getDailyUsage(const std::string &userId) {
    DailyUsageRow usage;
    if (!client_) return usage;
    try {
        auto r = client_->execSqlSync(
            "SELECT COALESCE(sms_sent, 0)::int AS sms, COALESCE(calls_made, 0)::int AS calls "
            "FROM user_daily_usage WHERE user_id = $1 AND usage_date = CURRENT_DATE",
            userId);
        if (r.size() > 0) {
            usage.smsSent = r[0]["sms"].as<int>();
            usage.callsMade = r[0]["calls"].as<int>();
        }
    } catch (const std::exception &e) {
        LOG_ERROR << "getDailyUsage failed: " << e.what();
    }
    return usage;
}

bool PostgresService::incrementDailySms(const std::string &userId) {
    if (!client_) return false;
    try {
        auto r = client_->execSqlSync(
            "INSERT INTO user_daily_usage(user_id, usage_date, sms_sent, calls_made) "
            "VALUES($1, CURRENT_DATE, 1, 0) "
            "ON CONFLICT (user_id, usage_date) DO UPDATE SET "
            "sms_sent = user_daily_usage.sms_sent + 1 "
            "WHERE user_daily_usage.sms_sent < 10 "
            "RETURNING sms_sent",
            userId);
        return r.size() > 0;
    } catch (const std::exception &e) {
        LOG_ERROR << "incrementDailySms failed: " << e.what();
        return false;
    }
}

bool PostgresService::incrementDailyCall(const std::string &userId) {
    if (!client_) return false;
    try {
        auto r = client_->execSqlSync(
            "INSERT INTO user_daily_usage(user_id, usage_date, sms_sent, calls_made) "
            "VALUES($1, CURRENT_DATE, 0, 1) "
            "ON CONFLICT (user_id, usage_date) DO UPDATE SET "
            "calls_made = user_daily_usage.calls_made + 1 "
            "WHERE user_daily_usage.calls_made < 5 "
            "RETURNING calls_made",
            userId);
        return r.size() > 0;
    } catch (const std::exception &e) {
        LOG_ERROR << "incrementDailyCall failed: " << e.what();
        return false;
    }
}

UserRow PostgresService::createUser(const std::string &userId, const std::string &username,
                                    const std::string &passwordHash) {
    if (!client_) throw std::runtime_error("Database unavailable");
    client_->execSqlSync(
        "INSERT INTO users(user_id, username, password_hash, auth_provider, created_at) "
        "VALUES($1, $2, $3, 'credentials', NOW())",
        userId, username, passwordHash);
    auto opt = findUserByUsername(username);
    if (!opt) throw std::runtime_error("Failed to create user");
    return *opt;
}

std::optional<UserRow> PostgresService::findUserByUsername(const std::string &username) {
    if (!client_) return std::nullopt;
    try {
        auto r = client_->execSqlSync(
            "SELECT user_id, username, COALESCE(password_hash,'') AS password_hash, "
            "COALESCE(email,'') AS email, COALESCE(display_name,'') AS display_name, "
            "COALESCE(avatar_url,'') AS avatar_url, auth_provider, "
            "EXTRACT(EPOCH FROM created_at)::bigint AS created_at "
            "FROM users WHERE username=$1",
            username);
        if (r.size() == 0) return std::nullopt;
        const auto &row = r[0];
        UserRow u;
        u.userId = row["user_id"].as<std::string>();
        u.username = row["username"].as<std::string>();
        u.passwordHash = row["password_hash"].as<std::string>();
        u.email = row["email"].as<std::string>();
        u.displayName = row["display_name"].as<std::string>();
        u.avatarUrl = row["avatar_url"].as<std::string>();
        u.authProvider = row["auth_provider"].as<std::string>();
        u.createdAt = static_cast<std::time_t>(row["created_at"].as<long long>());
        u.disabled = false;
        return u;
    } catch (const std::exception &e) {
        LOG_ERROR << "findUserByUsername failed: " << e.what();
        return std::nullopt;
    }
}

void PostgresService::upsertGoogleUser(const std::string &userId,
                                       const std::string &googleSub,
                                       const std::string &email,
                                       const std::string &displayName,
                                       const std::string &avatarUrl) {
    if (!client_) return;
    std::string uname = "google:" + googleSub;
    if (uname.size() > 128) uname = uname.substr(0, 128);
    client_->execSqlSync(
        "INSERT INTO users(user_id, username, email, display_name, avatar_url, google_sub, "
        "auth_provider, created_at) "
        "VALUES($1, $2, $3, $4, $5, $6, 'google', NOW()) "
        "ON CONFLICT (user_id) DO UPDATE SET "
        "email=EXCLUDED.email, display_name=EXCLUDED.display_name, "
        "avatar_url=EXCLUDED.avatar_url, google_sub=EXCLUDED.google_sub, auth_provider='google'",
        userId, uname, email, displayName, avatarUrl, googleSub);
}

void PostgresService::updateLastLogin(const std::string &userId) {
    if (!client_) return;
    try {
        client_->execSqlSync("UPDATE users SET last_login = NOW() WHERE user_id = $1", userId);
    } catch (const std::exception &e) {
        LOG_ERROR << "updateLastLogin failed: " << e.what();
    }
}

std::vector<std::string> PostgresService::listFavorites(const std::string &userId) {
    std::vector<std::string> out;
    if (!client_) return out;
    try {
        auto r = client_->execSqlSync(
            "SELECT pair FROM user_favorites WHERE user_id=$1 ORDER BY created_at ASC", userId);
        for (const auto &row : r) out.push_back(row["pair"].as<std::string>());
    } catch (const std::exception &e) {
        LOG_ERROR << "listFavorites failed: " << e.what();
    }
    return out;
}

std::vector<std::string> PostgresService::listAllFavoritePairs() const {
    std::vector<std::string> out;
    if (!client_) return out;
    try {
        auto r = client_->execSqlSync(
            "SELECT DISTINCT pair FROM user_favorites ORDER BY pair ASC");
        for (const auto &row : r) out.push_back(row["pair"].as<std::string>());
    } catch (const std::exception &e) {
        LOG_ERROR << "listAllFavoritePairs failed: " << e.what();
    }
    return out;
}

void PostgresService::addFavorite(const std::string &userId, const std::string &pair) {
    if (!client_) throw std::runtime_error("Database unavailable");
    client_->execSqlSync(
        "INSERT INTO user_favorites(user_id, pair, created_at) VALUES($1, $2, NOW()) "
        "ON CONFLICT (user_id, pair) DO NOTHING",
        userId, pair);
}

bool PostgresService::removeFavorite(const std::string &userId, const std::string &pair) {
    if (!client_) return false;
    try {
        auto r = client_->execSqlSync("DELETE FROM user_favorites WHERE user_id=$1 AND pair=$2",
                                      userId, pair);
        return r.affectedRows() > 0;
    } catch (const std::exception &e) {
        LOG_ERROR << "removeFavorite failed: " << e.what();
        return false;
    }
}

void PostgresService::logActivity(const std::string &userId, const std::string &eventType,
                                  const std::string &ipAddress,
                                  const std::string &userAgent,
                                  const Json::Value &metadata) {
    if (!client_) return;
    Json::StreamWriterBuilder wb;
    wb["indentation"] = "";
    std::string meta = Json::writeString(wb, metadata.isObject() ? metadata : Json::Value());
    try {
        std::string id = util::generateUuid();
        if (id.empty()) id = std::to_string(std::time(nullptr));
        client_->execSqlSync(
            "INSERT INTO user_activity_log(id, user_id, event_type, ip_address, user_agent, "
            "metadata, created_at) "
            "VALUES($1, $2, $3, $4, $5, $6::jsonb, NOW())",
            id, userId, eventType, ipAddress, userAgent, meta);
    } catch (const std::exception &e) {
        LOG_DEBUG << "logActivity failed: " << e.what();
    }
}

Json::Value PostgresService::adminOverview() {
    Json::Value v(Json::objectValue);
    if (!client_) return v;

    auto countQuery = [&](const std::string &sql) -> int64_t {
        try {
            auto r = client_->execSqlSync(sql);
            if (r.size() > 0) return r[0]["n"].as<long long>();
        } catch (...) {
        }
        return 0;
    };

    v["users_count"] = static_cast<Json::Int64>(countQuery("SELECT COUNT(*)::bigint AS n FROM users"));
    v["active_alerts"] = static_cast<Json::Int64>(
        countQuery("SELECT COUNT(*)::bigint AS n FROM alerts WHERE status='active'"));
    v["triggered_alerts"] = static_cast<Json::Int64>(
        countQuery("SELECT COUNT(*)::bigint AS n FROM alerts WHERE status='triggered'"));
    v["favorites_count"] =
        static_cast<Json::Int64>(countQuery("SELECT COUNT(*)::bigint AS n FROM user_favorites"));
    v["new_users_7d"] = static_cast<Json::Int64>(countQuery(
        "SELECT COUNT(*)::bigint AS n FROM users WHERE created_at >= NOW() - INTERVAL '7 days'"));
    v["recent_activity_7d"] = static_cast<Json::Int64>(countQuery(
        "SELECT COUNT(*)::bigint AS n FROM user_activity_log WHERE created_at >= NOW() - INTERVAL '7 days'"));

    Json::Value byChannel(Json::objectValue);
    Json::Value byStatus(Json::objectValue);
    try {
        auto r1 = client_->execSqlSync(
            "SELECT COALESCE(data->>'channel','email') AS ch, COUNT(*)::bigint AS n "
            "FROM alerts GROUP BY ch");
        for (const auto &row : r1)
            byChannel[row["ch"].as<std::string>()] =
                static_cast<Json::Int64>(row["n"].as<long long>());
        auto r2 = client_->execSqlSync(
            "SELECT status AS st, COUNT(*)::bigint AS n FROM alerts GROUP BY status");
        for (const auto &row : r2)
            byStatus[row["st"].as<std::string>()] =
                static_cast<Json::Int64>(row["n"].as<long long>());
    } catch (...) {
    }
    v["alerts_by_channel"] = byChannel;
    v["alerts_by_status"] = byStatus;
    return v;
}

Json::Value PostgresService::adminListUsers() {
    Json::Value items(Json::arrayValue);
    if (!client_) return items;
    try {
        auto r = client_->execSqlSync(
            "SELECT u.user_id, COALESCE(u.username,'') AS username, u.email, u.auth_provider, "
            "to_char(u.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at, "
            "(SELECT COUNT(*)::int FROM alerts a WHERE a.user_id=u.user_id) AS alert_count, "
            "(SELECT COUNT(*)::int FROM alerts a WHERE a.user_id=u.user_id AND a.status='active') "
            "AS active_alerts, "
            "(SELECT COUNT(*)::int FROM alerts a WHERE a.user_id=u.user_id AND a.status='triggered') "
            "AS triggered_alerts, "
            "(SELECT COUNT(*)::int FROM user_favorites f WHERE f.user_id=u.user_id) AS favorites_count, "
            "(SELECT COUNT(*)::int FROM user_activity_log al WHERE al.user_id=u.user_id) AS activity_count, "
            "to_char(u.last_login AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS last_login_at "
            "FROM users u ORDER BY u.created_at DESC LIMIT 500");
        for (const auto &row : r) {
            Json::Value u;
            u["user_id"] = row["user_id"].as<std::string>();
            u["username"] = row["username"].as<std::string>();
            if (row["email"].isNull())
                u["email"] = Json::Value::null;
            else
                u["email"] = row["email"].as<std::string>();
            u["auth_provider"] = row["auth_provider"].as<std::string>();
            u["created_at"] = row["created_at"].as<std::string>();
            u["alert_count"] = row["alert_count"].as<int>();
            u["active_alerts"] = row["active_alerts"].as<int>();
            u["triggered_alerts"] = row["triggered_alerts"].as<int>();
            u["favorites_count"] = row["favorites_count"].as<int>();
            u["activity_count"] = row["activity_count"].as<int>();
            if (row["last_login_at"].isNull())
                u["last_login_at"] = Json::Value::null;
            else
                u["last_login_at"] = row["last_login_at"].as<std::string>();
            items.append(u);
        }
    } catch (const std::exception &e) {
        LOG_ERROR << "adminListUsers failed: " << e.what();
    }
    return items;
}

Json::Value PostgresService::adminListAlerts(const std::string &statusFilter, int limit) {
    Json::Value items(Json::arrayValue);
    if (!client_) return items;
    limit = std::max(1, std::min(limit, 500));
    try {
        std::string sql =
            "SELECT a.id, a.user_id, a.pair, a.status, a.alert_type, "
            "to_char(a.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at, "
            "a.data::text AS data, COALESCE(u.username,'') AS username, u.email "
            "FROM alerts a LEFT JOIN users u ON u.user_id=a.user_id ";
        if (!statusFilter.empty() && statusFilter != "all") {
            sql += "WHERE a.status=$1 ";
            sql += "ORDER BY a.created_at DESC LIMIT " + std::to_string(limit);
            auto r = client_->execSqlSync(sql, statusFilter);
            for (const auto &row : r) {
                Json::Value alert = parseJson(row["data"].as<std::string>());
                Json::Value item;
                item["id"] = alert.get("id", row["id"].as<std::string>()).asString();
                item["user_id"] = row["user_id"].as<std::string>();
                item["username"] = row["username"].as<std::string>();
                if (row["email"].isNull())
                    item["email"] = Json::Value::null;
                else
                    item["email"] = row["email"].as<std::string>();
                item["created_by"] =
                    row["username"].as<std::string>().empty()
                        ? row["user_id"].as<std::string>()
                        : row["username"].as<std::string>();
                item["pair"] = alert.get("pair", row["pair"].as<std::string>()).asString();
                item["channel"] = alert.get("channel", "email").asString();
                item["status"] = alert.get("status", row["status"].as<std::string>()).asString();
                item["alert_type"] = alert.get("alert_type", "price").asString();
                item["target_price"] =
                    alert.isMember("target_price") && alert["target_price"].isNumeric()
                        ? alert["target_price"]
                        : Json::Value::null;
                item["threshold"] =
                    alert.isMember("threshold") && alert["threshold"].isNumeric()
                        ? alert["threshold"]
                        : Json::Value::null;
                item["condition"] =
                    alert.isMember("condition") && alert["condition"].isString()
                        ? alert["condition"]
                        : Json::Value::null;
                item["created_at"] = row["created_at"].as<std::string>();
                if (alert.isMember("triggered_at") && !alert["triggered_at"].isNull())
                    item["triggered_at"] = alert["triggered_at"];
                else
                    item["triggered_at"] = Json::Value::null;
                items.append(item);
            }
        } else {
            sql += "ORDER BY a.created_at DESC LIMIT " + std::to_string(limit);
            auto r = client_->execSqlSync(sql);
            for (const auto &row : r) {
                Json::Value alert = parseJson(row["data"].as<std::string>());
                Json::Value item;
                item["id"] = alert.get("id", row["id"].as<std::string>()).asString();
                item["user_id"] = row["user_id"].as<std::string>();
                item["username"] = row["username"].as<std::string>();
                if (row["email"].isNull())
                    item["email"] = Json::Value::null;
                else
                    item["email"] = row["email"].as<std::string>();
                item["created_by"] =
                    row["username"].as<std::string>().empty()
                        ? row["user_id"].as<std::string>()
                        : row["username"].as<std::string>();
                item["pair"] = alert.get("pair", row["pair"].as<std::string>()).asString();
                item["channel"] = alert.get("channel", "email").asString();
                item["status"] = alert.get("status", row["status"].as<std::string>()).asString();
                item["alert_type"] = alert.get("alert_type", "price").asString();
                item["target_price"] =
                    alert.isMember("target_price") && alert["target_price"].isNumeric()
                        ? alert["target_price"]
                        : Json::Value::null;
                item["threshold"] =
                    alert.isMember("threshold") && alert["threshold"].isNumeric()
                        ? alert["threshold"]
                        : Json::Value::null;
                item["condition"] =
                    alert.isMember("condition") && alert["condition"].isString()
                        ? alert["condition"]
                        : Json::Value::null;
                item["created_at"] = row["created_at"].as<std::string>();
                if (alert.isMember("triggered_at") && !alert["triggered_at"].isNull())
                    item["triggered_at"] = alert["triggered_at"];
                else
                    item["triggered_at"] = Json::Value::null;
                items.append(item);
            }
        }
    } catch (const std::exception &e) {
        LOG_ERROR << "adminListAlerts failed: " << e.what();
    }
    return items;
}

Json::Value PostgresService::adminListActivity(const std::string &eventTypeFilter, int limit) {
    Json::Value items(Json::arrayValue);
    if (!client_) return items;
    limit = std::max(1, std::min(limit, 500));
    try {
        if (!eventTypeFilter.empty()) {
            auto r = client_->execSqlSync(
                "SELECT al.id::text, al.user_id, al.event_type, al.ip_address, al.user_agent, "
                "al.metadata::text AS metadata, "
                "to_char(al.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at, "
                "COALESCE(u.username,'') AS username, u.email "
                "FROM user_activity_log al LEFT JOIN users u ON u.user_id=al.user_id "
                "WHERE al.event_type=$1 ORDER BY al.created_at DESC LIMIT " +
                    std::to_string(limit),
                eventTypeFilter);
            for (const auto &row : r) {
                Json::Value item;
                item["id"] = row["id"].as<std::string>();
                if (row["user_id"].isNull())
                    item["user_id"] = Json::Value::null;
                else
                    item["user_id"] = row["user_id"].as<std::string>();
                item["username"] = row["username"].as<std::string>();
                if (row["email"].isNull())
                    item["email"] = Json::Value::null;
                else
                    item["email"] = row["email"].as<std::string>();
                item["created_by"] =
                    row["username"].as<std::string>().empty()
                        ? (row["user_id"].isNull() ? "" : row["user_id"].as<std::string>())
                        : row["username"].as<std::string>();
                item["event_type"] = row["event_type"].as<std::string>();
                if (row["ip_address"].isNull())
                    item["ip_address"] = Json::Value::null;
                else
                    item["ip_address"] = row["ip_address"].as<std::string>();
                if (row["user_agent"].isNull())
                    item["user_agent"] = Json::Value::null;
                else
                    item["user_agent"] = row["user_agent"].as<std::string>();
                item["metadata"] = parseJson(row["metadata"].as<std::string>());
                item["created_at"] = row["created_at"].as<std::string>();
                items.append(item);
            }
        } else {
            auto r = client_->execSqlSync(
                "SELECT al.id::text, al.user_id, al.event_type, al.ip_address, al.user_agent, "
                "al.metadata::text AS metadata, "
                "to_char(al.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at, "
                "COALESCE(u.username,'') AS username, u.email "
                "FROM user_activity_log al LEFT JOIN users u ON u.user_id=al.user_id "
                "ORDER BY al.created_at DESC LIMIT " +
                std::to_string(limit));
            for (const auto &row : r) {
                Json::Value item;
                item["id"] = row["id"].as<std::string>();
                if (row["user_id"].isNull())
                    item["user_id"] = Json::Value::null;
                else
                    item["user_id"] = row["user_id"].as<std::string>();
                item["username"] = row["username"].as<std::string>();
                if (row["email"].isNull())
                    item["email"] = Json::Value::null;
                else
                    item["email"] = row["email"].as<std::string>();
                item["created_by"] =
                    row["username"].as<std::string>().empty()
                        ? (row["user_id"].isNull() ? "" : row["user_id"].as<std::string>())
                        : row["username"].as<std::string>();
                item["event_type"] = row["event_type"].as<std::string>();
                if (row["ip_address"].isNull())
                    item["ip_address"] = Json::Value::null;
                else
                    item["ip_address"] = row["ip_address"].as<std::string>();
                if (row["user_agent"].isNull())
                    item["user_agent"] = Json::Value::null;
                else
                    item["user_agent"] = row["user_agent"].as<std::string>();
                item["metadata"] = parseJson(row["metadata"].as<std::string>());
                item["created_at"] = row["created_at"].as<std::string>();
                items.append(item);
            }
        }
    } catch (const std::exception &e) {
        LOG_ERROR << "adminListActivity failed: " << e.what();
    }
    return items;
}

}  // namespace ctraderplus::services
