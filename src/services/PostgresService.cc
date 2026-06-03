#include "services/PostgresService.h"

#include <memory>
#include <sstream>

#include <trantor/utils/Logger.h>

#include "util/PairNormalizer.h"
#include "util/TimeUtil.h"

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

bool columnExists(const drogon::orm::DbClientPtr &client, const std::string &table,
                  const std::string &column) {
    auto r = client->execSqlSync(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2 LIMIT 1",
        table, column);
    return r.size() > 0;
}

void migrateAlertsSchema(const drogon::orm::DbClientPtr &client) {
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
}  // namespace

PostgresService::PostgresService(const core::Config &cfg) : cfg_(cfg) {}

bool PostgresService::connect() {
    try {
        client_ = DbClient::newPgClient(cfg_.postgresDsn, /*connNum=*/2);
        LOG_INFO << "PostgreSQL client created";
        return client_ != nullptr;
    } catch (const std::exception &e) {
        LOG_WARN << "PostgreSQL unavailable: " << e.what();
        client_ = nullptr;
        return false;
    }
}

void PostgresService::initSchema() {
    if (!client_) return;
    client_->execSqlSync(
        "CREATE TABLE IF NOT EXISTS historical_prices ("
        "id BIGSERIAL PRIMARY KEY,"
        "pair VARCHAR(64) NOT NULL,"
        "price DOUBLE PRECISION NOT NULL,"
        "observed_at TIMESTAMPTZ NOT NULL)");
    client_->execSqlSync(
        "CREATE INDEX IF NOT EXISTS ix_hist_pair_time ON historical_prices(pair, observed_at)");
    client_->execSqlSync(
        "CREATE TABLE IF NOT EXISTS stream_metrics ("
        "id BIGSERIAL PRIMARY KEY,"
        "observed_at TIMESTAMPTZ NOT NULL,"
        "ws_subscriber_count INT NOT NULL DEFAULT 0,"
        "queue_subscriber_count INT NOT NULL DEFAULT 0,"
        "snapshot_failure_count INT NOT NULL DEFAULT 0,"
        "stream_status VARCHAR(32) NOT NULL DEFAULT 'healthy')");
    client_->execSqlSync(
        "CREATE INDEX IF NOT EXISTS ix_metrics_time ON stream_metrics(observed_at)");
    client_->execSqlSync(
        "CREATE TABLE IF NOT EXISTS alerts ("
        "id VARCHAR(64) PRIMARY KEY,"
        "user_id VARCHAR(128) NOT NULL DEFAULT 'legacy-unassigned',"
        "pair VARCHAR(64) NOT NULL,"
        "status VARCHAR(32) NOT NULL DEFAULT 'active',"
        "alert_type VARCHAR(32) NOT NULL DEFAULT 'price',"
        "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
        "data JSONB NOT NULL)");
    client_->execSqlSync("CREATE INDEX IF NOT EXISTS ix_alerts_user_id ON alerts(user_id)");
    client_->execSqlSync(
        "CREATE TABLE IF NOT EXISTS user_states ("
        "user_id VARCHAR(128) PRIMARY KEY,"
        "first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
        "onboarding_completed_at TIMESTAMPTZ)");
    migrateAlertsSchema(client_);
    LOG_INFO << "PostgreSQL schema ensured";
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

void PostgresService::upsertAlert(const Json::Value &alert) {
    if (!client_) return;
    std::string id = alert.get("id", "").asString();
    if (id.empty()) return;
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
    try {
        client_->execSqlSync(
            "INSERT INTO alerts(id, user_id, pair, status, alert_type, created_at, data) "
            "VALUES($1,$2,$3,$4,$5,to_timestamp($6),$7::jsonb) "
            "ON CONFLICT (id) DO UPDATE SET user_id=EXCLUDED.user_id, pair=EXCLUDED.pair, "
            "status=EXCLUDED.status, alert_type=EXCLUDED.alert_type, data=EXCLUDED.data",
            id, userId, pair, status, alertType, epochSec, data);
    } catch (const std::exception &e) {
        LOG_ERROR << "upsertAlert failed: " << e.what();
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
            "onboarding_completed_at IS NULL AS is_null "
            "FROM user_states WHERE user_id=$1",
            userId);
        if (r.size() > 0) {
            const auto &rr = r[0];
            row.firstSeenAt = static_cast<std::time_t>(rr["fs"].as<long long>());
            if (!rr["is_null"].as<bool>())
                row.onboardingCompletedAt =
                    static_cast<std::time_t>(rr["oc"].as<long long>());
        }
    } catch (const std::exception &e) {
        LOG_ERROR << "getOrCreateUserState failed: " << e.what();
        throw;
    }
    return row;
}

UserStateRow PostgresService::completeUserOnboarding(const std::string &userId) {
    UserStateRow row;
    row.userId = userId;
    if (!client_) throw std::runtime_error("Database unavailable");
    try {
        client_->execSqlSync(
            "INSERT INTO user_states(user_id, first_seen_at, onboarding_completed_at) "
            "VALUES($1, NOW(), NOW()) "
            "ON CONFLICT (user_id) DO UPDATE SET onboarding_completed_at = "
            "COALESCE(user_states.onboarding_completed_at, NOW())",
            userId);
        auto r = client_->execSqlSync(
            "SELECT EXTRACT(EPOCH FROM first_seen_at)::bigint AS fs, "
            "EXTRACT(EPOCH FROM onboarding_completed_at)::bigint AS oc "
            "FROM user_states WHERE user_id=$1",
            userId);
        if (r.size() > 0) {
            const auto &rr = r[0];
            row.firstSeenAt = static_cast<std::time_t>(rr["fs"].as<long long>());
            row.onboardingCompletedAt =
                static_cast<std::time_t>(rr["oc"].as<long long>());
        }
    } catch (const std::exception &e) {
        LOG_ERROR << "completeUserOnboarding failed: " << e.what();
        throw;
    }
    return row;
}

}  // namespace ctraderplus::services
