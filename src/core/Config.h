#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace ctraderplus::core {

struct CTraderConfig {
    std::string clientId;
    std::string clientSecret;
    std::string accessToken;
    std::string refreshToken;
    int64_t accountId = 0;  // ctidTraderAccountId
    std::string host = "live";  // "live" or "demo"
    std::string liveHost = "live.ctraderapi.com";
    std::string demoHost = "demo.ctraderapi.com";
    int port = 5035;
    int heartbeatIntervalSeconds = 10;
    double reconnectBaseDelaySeconds = 1.0;
    double reconnectMaxDelaySeconds = 30.0;
    double requestTimeoutSeconds = 15.0;
    bool includeArchivedSymbols = false;
    bool subscribeAllSymbols = true;
    int maxSubscribedSymbols = 0;  // 0 = unlimited

    std::string resolvedHost() const {
        return host == "demo" ? demoHost : liveHost;
    }
};

struct Config {
    // cTrader
    CTraderConfig ctrader;

    // HTTP server
    std::string listenAddress = "0.0.0.0";
    int httpPort = 8000;
    int threadNum = 0;  // 0 -> hardware concurrency
    std::string wsUrl;
    std::string apiBaseUrl;

    // Streaming / resiliency tuning
    double streamIntervalSeconds = 0.5;
    double snapshotTimeoutSeconds = 30.0;
    double wsSendTimeoutSeconds = 3.0;
    double alertActionTimeoutSeconds = 8.0;
    int maxSnapshotFailures = 4;
    double staleSnapshotSeconds = 5.0;

    // Redis
    std::string redisUrl = "redis://localhost:6379/0";
    std::string redisChannel = "fx:observer:snapshot";
    std::string redisLatestKey = "fx:observer:latest";
    std::string redisQueueKey = "fx:observer:queue";
    std::string redisRecentKey = "fx:observer:recent";
    int redisRecentMaxlen = 200;
    std::string redisAlertQueueKey = "fx:alerts:events";
    bool redisPubsubEnabled = true;
    std::string notificationDlqKey = "fx:alerts:notifications:dlq";

    // Postgres
    std::string postgresDsn = "postgresql://user:password@localhost:5432/observer";

    // Archive / retention
    double archiveIntervalSeconds = 30.0;
    int archiveBatchSize = 200;
    double candleCheckIntervalSeconds = 0.25;
    int notificationWorkerCount = 4;
    int notificationMaxRetries = 3;
    double notificationRetryDelaySeconds = 1.0;
    int retentionDays = 14;

    std::vector<std::string> majors = {"USD", "EUR", "JPY", "GBP",
                                       "AUD", "CAD", "CHF", "NZD"};

    // Auth
    std::string nextAuthSecret;
    bool authDisabled = false;

    // Notification providers
    std::string sendgridApiKey;
    std::string sendgridFromEmail = "alerts@example.com";
    std::string africasTalkingUsername;
    std::string africasTalkingApiKey;
    std::string africasTalkingSenderId;
    std::string twilioAccountSid;
    std::string twilioAuthToken;
    std::string twilioFromNumber;
};

// Load .env (if present) into the process environment, then build Config from
// config.json (path) overlaid with environment variables. Loaded once.
const Config &loadConfig(const std::string &configPath = "config.json",
                         const std::string &envPath = ".env");
const Config &getConfig();

}  // namespace ctraderplus::core
