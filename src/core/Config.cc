#include "core/Config.h"

#include <cstdlib>
#include <fstream>
#include <memory>
#include <mutex>
#include <sstream>

#include <json/json.h>

namespace ctraderplus::core {

namespace {
std::unique_ptr<Config> g_config;
std::once_flag g_once;

std::string trim(const std::string &s) {
    size_t a = s.find_first_not_of(" \t\r\n");
    if (a == std::string::npos) return "";
    size_t b = s.find_last_not_of(" \t\r\n");
    return s.substr(a, b - a + 1);
}

void loadDotEnv(const std::string &path) {
    std::ifstream f(path);
    if (!f.is_open()) return;
    std::string line;
    while (std::getline(f, line)) {
        std::string t = trim(line);
        if (t.empty() || t[0] == '#') continue;
        auto eq = t.find('=');
        if (eq == std::string::npos) continue;
        std::string key = trim(t.substr(0, eq));
        std::string val = trim(t.substr(eq + 1));
        if (val.size() >= 2 && ((val.front() == '"' && val.back() == '"') ||
                                (val.front() == '\'' && val.back() == '\''))) {
            val = val.substr(1, val.size() - 2);
        }
        if (key.empty()) continue;
        // Do not override variables already present in the environment.
        if (std::getenv(key.c_str()) == nullptr) {
            ::setenv(key.c_str(), val.c_str(), 0);
        }
    }
}

std::string envStr(const char *name, const std::string &def = "") {
    const char *v = std::getenv(name);
    return (v && *v) ? std::string(v) : def;
}

bool envBool(const char *name, bool def) {
    const char *v = std::getenv(name);
    if (!v || !*v) return def;
    std::string s(v);
    for (auto &c : s) c = static_cast<char>(std::tolower(c));
    return s == "1" || s == "true" || s == "yes" || s == "on";
}

Json::Value loadJsonFile(const std::string &path) {
    Json::Value root;
    std::ifstream f(path);
    if (!f.is_open()) return root;
    std::stringstream ss;
    ss << f.rdbuf();
    std::string content = ss.str();
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    std::string errs;
    reader->parse(content.c_str(), content.c_str() + content.size(), &root, &errs);
    return root;
}

double jnum(const Json::Value &v, const char *key, double def) {
    return v.isMember(key) && v[key].isNumeric() ? v[key].asDouble() : def;
}
int jint(const Json::Value &v, const char *key, int def) {
    return v.isMember(key) && v[key].isNumeric() ? v[key].asInt() : def;
}
bool jbool(const Json::Value &v, const char *key, bool def) {
    return v.isMember(key) && v[key].isBool() ? v[key].asBool() : def;
}
std::string jstr(const Json::Value &v, const char *key, const std::string &def) {
    return v.isMember(key) && v[key].isString() ? v[key].asString() : def;
}

void build(Config &c, const std::string &configPath, const std::string &envPath) {
    loadDotEnv(envPath);
    Json::Value root = loadJsonFile(configPath);

    // ---- cTrader (json then env override) ----
    Json::Value ct = root["ctrader"];
    c.ctrader.liveHost = jstr(ct, "liveHost", c.ctrader.liveHost);
    c.ctrader.demoHost = jstr(ct, "demoHost", c.ctrader.demoHost);
    c.ctrader.host = jstr(ct, "host", c.ctrader.host);
    c.ctrader.port = jint(ct, "port", c.ctrader.port);
    c.ctrader.heartbeatIntervalSeconds =
        jint(ct, "heartbeatIntervalSeconds", c.ctrader.heartbeatIntervalSeconds);
    c.ctrader.reconnectBaseDelaySeconds =
        jnum(ct, "reconnectBaseDelaySeconds", c.ctrader.reconnectBaseDelaySeconds);
    c.ctrader.reconnectMaxDelaySeconds =
        jnum(ct, "reconnectMaxDelaySeconds", c.ctrader.reconnectMaxDelaySeconds);
    c.ctrader.requestTimeoutSeconds =
        jnum(ct, "requestTimeoutSeconds", c.ctrader.requestTimeoutSeconds);
    c.ctrader.includeArchivedSymbols =
        jbool(ct, "includeArchivedSymbols", c.ctrader.includeArchivedSymbols);
    c.ctrader.subscribeAllSymbols =
        jbool(ct, "subscribeAllSymbols", c.ctrader.subscribeAllSymbols);
    c.ctrader.maxSubscribedSymbols =
        jint(ct, "maxSubscribedSymbols", c.ctrader.maxSubscribedSymbols);
    c.ctrader.reconnectCircuitBreakerThreshold =
        jint(ct, "reconnectCircuitBreakerThreshold",
             c.ctrader.reconnectCircuitBreakerThreshold);
    c.ctrader.reconnectCircuitBreakerWindowSeconds =
        jnum(ct, "reconnectCircuitBreakerWindowSeconds",
             c.ctrader.reconnectCircuitBreakerWindowSeconds);
    c.ctrader.reconnectCircuitBreakerCooldownSeconds =
        jnum(ct, "reconnectCircuitBreakerCooldownSeconds",
             c.ctrader.reconnectCircuitBreakerCooldownSeconds);

    c.ctrader.clientId = envStr("CTRADER_CLIENT_ID");
    c.ctrader.clientSecret = envStr("CTRADER_CLIENT_SECRET");
    c.ctrader.accessToken = envStr("CTRADER_ACCESS_TOKEN");
    c.ctrader.refreshToken = envStr("CTRADER_REFRESH_TOKEN");
    {
        std::string acc = envStr("CTRADER_ACCOUNT_ID");
        if (!acc.empty()) {
            try {
                c.ctrader.accountId = std::stoll(acc);
            } catch (...) {
            }
        }
    }
    c.ctrader.host = envStr("CTRADER_HOST", c.ctrader.host);

    // ---- HTTP server ----
    Json::Value srv = root["server"];
    c.listenAddress = jstr(srv, "listenAddress", c.listenAddress);
    c.httpPort = jint(srv, "port", c.httpPort);
    c.threadNum = jint(srv, "threadNum", c.threadNum);
    {
        std::string p = envStr("PORT");
        if (!p.empty()) {
            try {
                c.httpPort = std::stoi(p);
            } catch (...) {
            }
        }
    }
    c.wsUrl = envStr("WS_URL");
    c.apiBaseUrl = envStr("API_BASE_URL");

    // ---- Tuning ----
    c.streamIntervalSeconds = jnum(root, "streamIntervalSeconds", c.streamIntervalSeconds);
    c.snapshotTimeoutSeconds = jnum(root, "snapshotTimeoutSeconds", c.snapshotTimeoutSeconds);
    c.wsSendTimeoutSeconds = jnum(root, "wsSendTimeoutSeconds", c.wsSendTimeoutSeconds);
    c.wsMaxConnectionsPerIp = jint(root, "wsMaxConnectionsPerIp", c.wsMaxConnectionsPerIp);
    c.alertActionTimeoutSeconds =
        jnum(root, "alertActionTimeoutSeconds", c.alertActionTimeoutSeconds);
    c.maxSnapshotFailures = jint(root, "maxSnapshotFailures", c.maxSnapshotFailures);
    c.staleSnapshotSeconds = jnum(root, "staleSnapshotSeconds", c.staleSnapshotSeconds);

    // ---- Redis ----
    c.redisUrl = envStr("REDIS_URL", jstr(root, "redisUrl", c.redisUrl));
    c.redisChannel = jstr(root, "redisChannel", c.redisChannel);
    c.redisLatestKey = jstr(root, "redisLatestKey", c.redisLatestKey);
    c.redisQueueKey = jstr(root, "redisQueueKey", c.redisQueueKey);
    c.redisRecentKey = jstr(root, "redisRecentKey", c.redisRecentKey);
    c.redisRecentMaxlen = jint(root, "redisRecentMaxlen", c.redisRecentMaxlen);
    c.redisAlertQueueKey = jstr(root, "redisAlertQueueKey", c.redisAlertQueueKey);
    c.redisCtraderTokenKey = jstr(root, "redisCtraderTokenKey", c.redisCtraderTokenKey);
    c.redisPubsubEnabled =
        envBool("REDIS_PUBSUB_ENABLED", jbool(root, "redisPubsubEnabled", c.redisPubsubEnabled));
    c.notificationDlqKey = jstr(root, "notificationDlqKey", c.notificationDlqKey);

    // ---- Postgres ----
    c.postgresDsn = envStr("DATABASE_URL", jstr(root, "postgresDsn", c.postgresDsn));
    c.postgresConnNum = jint(root, "postgresConnNum", c.postgresConnNum);
    {
        const std::string prefix = "postgresql+asyncpg://";
        if (c.postgresDsn.rfind(prefix, 0) == 0) {
            c.postgresDsn = "postgresql://" + c.postgresDsn.substr(prefix.size());
        }
    }

    // ---- Archive / retention / candle polling ----
    c.tickArchiveEnabled = jbool(root, "tickArchiveEnabled", c.tickArchiveEnabled);
    c.archiveIntervalSeconds = jnum(root, "archiveIntervalSeconds", c.archiveIntervalSeconds);
    c.archiveBatchSize = jint(root, "archiveBatchSize", c.archiveBatchSize);
    c.candleCheckIntervalSeconds =
        jnum(root, "candleCheckIntervalSeconds", c.candleCheckIntervalSeconds);
    c.pollFallbackEnabled = jbool(root, "pollFallbackEnabled", c.pollFallbackEnabled);
    c.notificationWorkerCount =
        jint(root, "notificationWorkerCount", c.notificationWorkerCount);
    c.notificationMaxRetries = jint(root, "notificationMaxRetries", c.notificationMaxRetries);
    c.notificationRetryDelaySeconds =
        jnum(root, "notificationRetryDelaySeconds", c.notificationRetryDelaySeconds);
    c.notificationTimezone =
        jstr(root, "notificationTimezone", c.notificationTimezone);
    c.retentionDays = jint(root, "retentionDays", c.retentionDays);

    if (root.isMember("majors") && root["majors"].isArray()) {
        c.majors.clear();
        for (const auto &m : root["majors"]) {
            if (m.isString()) c.majors.push_back(m.asString());
        }
    }

    if (root.isMember("subscribedPairs") && root["subscribedPairs"].isArray()) {
        c.subscribedPairs.clear();
        for (const auto &p : root["subscribedPairs"]) {
            if (p.isString()) c.subscribedPairs.push_back(p.asString());
        }
    }
    c.ctrader.enforcePairAllowlist = !c.subscribedPairs.empty();

    // ---- Auth ----
    c.nextAuthSecret = envStr("NEXTAUTH_SECRET");
    c.authDisabled = envBool("AUTH_DISABLED", false);

    // ---- Admin / CORS ----
    c.adminPhone = envStr("ADMIN_PHONE");
    {
        std::string ttl = envStr("OTP_TTL_SECONDS");
        if (!ttl.empty()) {
            try {
                c.otpTtlSeconds = std::stoi(ttl);
            } catch (...) {
            }
        }
    }
    c.corsAllowOrigin = envStr("CORS_ALLOW_ORIGIN", c.corsAllowOrigin);

    // ---- Notifications ----
    c.sendgridApiKey = envStr("SENDGRID_API_KEY");
    c.sendgridFromEmail = envStr("SENDGRID_FROM_EMAIL", c.sendgridFromEmail);
    c.smsGateUsername = envStr("SMS_GATE_USERNAME");
    c.smsGatePassword = envStr("SMS_GATE_PASSWORD");
    c.twilioAccountSid = envStr("TWILIO_ACCOUNT_SID");
    c.twilioAuthToken = envStr("TWILIO_AUTH_TOKEN");
    c.twilioFromNumber = envStr("TWILIO_FROM_NUMBER");
}
}  // namespace

const Config &loadConfig(const std::string &configPath, const std::string &envPath) {
    std::call_once(g_once, [&]() {
        g_config = std::make_unique<Config>();
        build(*g_config, configPath, envPath);
    });
    return *g_config;
}

const Config &getConfig() {
    if (!g_config) return loadConfig();
    return *g_config;
}

}  // namespace ctraderplus::core
