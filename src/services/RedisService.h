#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <drogon/nosql/RedisClient.h>

#include "core/Config.h"

namespace ctraderplus::services {

// Thin wrapper over Drogon's async Redis client providing the operations the
// observer needs: snapshot cache/queue, alert event queue, and DLQ.
// All methods are best-effort; failures are logged and swallowed so the app
// keeps running in a degraded mode (matching the Python implementation).
class RedisService {
  public:
    explicit RedisService(const core::Config &cfg);

    bool connect();
    bool connected() const { return client_ != nullptr; }

    // Cache latest + push to archive queue + recent list + publish channel.
    void publishSnapshot(const std::string &json);

    // Pop up to `batch` items from the archive queue.
    void readQueue(int batch, std::function<void(std::vector<std::string>)> cb);

    // Generic JSON queue helpers (used for alert events + DLQ).
    void pushJson(const std::string &key, const std::string &json);
    void readJsonQueue(const std::string &key, int batch,
                       std::function<void(std::vector<std::string>)> cb);
    void requeueJsonBatch(const std::string &key, const std::vector<std::string> &items);

  private:
    const core::Config &cfg_;
    std::string host_ = "127.0.0.1";
    int port_ = 6379;
    int db_ = 0;
    std::string password_;
    bool urlParsedOk_ = false;
    drogon::nosql::RedisClientPtr client_;
};

}  // namespace ctraderplus::services
