#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <drogon/nosql/RedisClient.h>

#include "core/Config.h"

namespace ctraderplus::services {

// Thin wrapper over Drogon's async Redis client: alert event queue, OTP, DLQ.
// All methods are best-effort; failures are logged and swallowed so the app
// keeps running in a degraded mode (matching the Python implementation).
class RedisService {
  public:
    explicit RedisService(const core::Config &cfg);

    bool connect();
    bool connected() const { return client_ != nullptr; }

    // Generic JSON queue helpers (used for alert events + DLQ).
    void pushJson(const std::string &key, const std::string &json);
    void readJsonQueue(const std::string &key, int batch,
                       std::function<void(std::vector<std::string>)> cb);
    void requeueJsonBatch(const std::string &key, const std::vector<std::string> &items);

    // OTP / short-lived string keys
    void setStringEx(const std::string &key, const std::string &value, int ttlSeconds,
                     std::function<void(bool)> cb);
    void getString(const std::string &key,
                   std::function<void(std::optional<std::string>)> cb);
    void deleteKey(const std::string &key, std::function<void()> cb);

    // Blocking helpers for startup paths (cTrader token load).
    std::optional<std::string> getStringSync(const std::string &key);
    bool setStringSync(const std::string &key, const std::string &value);

  private:
    const core::Config &cfg_;
    std::string host_ = "127.0.0.1";
    int port_ = 6379;
    int db_ = 0;
    std::string username_;
    std::string password_;
    bool urlParsedOk_ = false;
    drogon::nosql::RedisClientPtr client_;
};

}  // namespace ctraderplus::services
