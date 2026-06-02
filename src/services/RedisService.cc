#include "services/RedisService.h"

#include <regex>

#include <trantor/utils/Logger.h>

namespace ctraderplus::services {

namespace {
// Parse redis://[:password@]host:port/db
void parseUrl(const std::string &url, std::string &host, int &port, int &db,
              std::string &password) {
    std::smatch m;
    static const std::regex re(
        R"(redis://(?:([^@:]*):?([^@]*)@)?([^:/]+)(?::(\d+))?(?:/(\d+))?)");
    if (std::regex_match(url, m, re)) {
        if (m[2].matched && m[2].length() > 0)
            password = m[2].str();
        else if (m[1].matched && m[1].length() > 0)
            password = m[1].str();
        if (m[3].matched) host = m[3].str();
        if (m[4].matched && m[4].length() > 0) port = std::stoi(m[4].str());
        if (m[5].matched && m[5].length() > 0) db = std::stoi(m[5].str());
    }
}
}  // namespace

RedisService::RedisService(const core::Config &cfg) : cfg_(cfg) {
    parseUrl(cfg.redisUrl, host_, port_, db_, password_);
}

bool RedisService::connect() {
    try {
        client_ = drogon::nosql::RedisClient::newRedisClient(
            trantor::InetAddress(host_, static_cast<uint16_t>(port_)),
            /*connections=*/2, password_, static_cast<unsigned int>(db_));
        LOG_INFO << "Redis client created for " << host_ << ":" << port_ << "/" << db_;
        return client_ != nullptr;
    } catch (const std::exception &e) {
        LOG_WARN << "Redis unavailable: " << e.what();
        client_ = nullptr;
        return false;
    }
}

void RedisService::publishSnapshot(const std::string &json) {
    if (!client_) return;
    auto onErr = [](const std::exception &e) {
        LOG_DEBUG << "Redis publishSnapshot error: " << e.what();
    };
    auto noop = [](const drogon::nosql::RedisResult &) {};
    client_->execCommandAsync(noop, onErr, "SET %s %s", cfg_.redisLatestKey.c_str(),
                              json.c_str());
    client_->execCommandAsync(noop, onErr, "RPUSH %s %s", cfg_.redisQueueKey.c_str(),
                              json.c_str());
    client_->execCommandAsync(noop, onErr, "LPUSH %s %s", cfg_.redisRecentKey.c_str(),
                              json.c_str());
    client_->execCommandAsync(noop, onErr, "LTRIM %s 0 %d", cfg_.redisRecentKey.c_str(),
                              std::max(cfg_.redisRecentMaxlen - 1, 0));
    if (cfg_.redisPubsubEnabled) {
        client_->execCommandAsync(noop, onErr, "PUBLISH %s %s",
                                  cfg_.redisChannel.c_str(), json.c_str());
    }
}

void RedisService::readQueue(int batch, std::function<void(std::vector<std::string>)> cb) {
    if (!client_) {
        cb({});
        return;
    }
    auto onErr = [cb](const std::exception &e) {
        LOG_DEBUG << "Redis readQueue error: " << e.what();
        cb({});
    };
    client_->execCommandAsync(
        [cb](const drogon::nosql::RedisResult &r) {
            std::vector<std::string> out;
            if (r.type() == drogon::nosql::RedisResultType::kArray) {
                for (const auto &item : r.asArray()) out.push_back(item.asString());
            } else if (r.type() == drogon::nosql::RedisResultType::kString) {
                out.push_back(r.asString());
            }
            cb(std::move(out));
        },
        onErr, "LPOP %s %d", cfg_.redisQueueKey.c_str(), batch);
}

void RedisService::pushJson(const std::string &key, const std::string &json) {
    if (!client_) return;
    client_->execCommandAsync(
        [](const drogon::nosql::RedisResult &) {},
        [](const std::exception &e) { LOG_DEBUG << "Redis pushJson error: " << e.what(); },
        "RPUSH %s %s", key.c_str(), json.c_str());
}

void RedisService::readJsonQueue(const std::string &key, int batch,
                                 std::function<void(std::vector<std::string>)> cb) {
    if (!client_) {
        cb({});
        return;
    }
    auto onErr = [cb](const std::exception &e) {
        LOG_DEBUG << "Redis readJsonQueue error: " << e.what();
        cb({});
    };
    client_->execCommandAsync(
        [cb](const drogon::nosql::RedisResult &r) {
            std::vector<std::string> out;
            if (r.type() == drogon::nosql::RedisResultType::kArray) {
                for (const auto &item : r.asArray()) out.push_back(item.asString());
            } else if (r.type() == drogon::nosql::RedisResultType::kString) {
                out.push_back(r.asString());
            }
            cb(std::move(out));
        },
        onErr, "LPOP %s %d", key.c_str(), batch);
}

void RedisService::requeueJsonBatch(const std::string &key,
                                    const std::vector<std::string> &items) {
    if (!client_ || items.empty()) return;
    // LPUSH in reverse to preserve original order.
    for (auto it = items.rbegin(); it != items.rend(); ++it) {
        client_->execCommandAsync(
            [](const drogon::nosql::RedisResult &) {},
            [](const std::exception &e) {
                LOG_DEBUG << "Redis requeue error: " << e.what();
            },
            "LPUSH %s %s", key.c_str(), it->c_str());
    }
}

}  // namespace ctraderplus::services
