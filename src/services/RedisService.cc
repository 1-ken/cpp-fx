#include "services/RedisService.h"

#include <regex>

#include <trantor/utils/Logger.h>

#include "util/HostResolve.h"

namespace ctraderplus::services {

namespace {

std::string trim(const std::string &s) {
    size_t a = s.find_first_not_of(" \t\r\n");
    if (a == std::string::npos) return "";
    size_t b = s.find_last_not_of(" \t\r\n");
    return s.substr(a, b - a + 1);
}

std::string redactRedisUrl(const std::string &url) {
    static const std::regex authRe(R"(redis://([^:@/]+):([^@]+)@)");
    return std::regex_replace(url, authRe, "redis://$1:***@");
}

// Parse redis://[user:password@]host[:port][/db]
bool parseUrl(const std::string &url, std::string &username, std::string &host, int &port,
              int &db, std::string &password) {
    const std::string t = trim(url);
    if (t.empty()) return false;

    const std::string scheme = "redis://";
    if (t.rfind(scheme, 0) != 0) return false;

    std::string rest = t.substr(scheme.size());
    auto at = rest.find('@');
    if (at != std::string::npos) {
        std::string auth = rest.substr(0, at);
        rest = rest.substr(at + 1);
        auto colon = auth.find(':');
        if (colon != std::string::npos) {
            username = auth.substr(0, colon);
            password = auth.substr(colon + 1);
        } else {
            password = auth;
        }
    }

    auto slash = rest.find('/');
    std::string hostPort = slash == std::string::npos ? rest : rest.substr(0, slash);
    if (slash != std::string::npos && slash + 1 < rest.size()) {
        try {
            db = std::stoi(rest.substr(slash + 1));
        } catch (...) {
            return false;
        }
    }

    auto colon = hostPort.find(':');
    if (colon != std::string::npos) {
        host = hostPort.substr(0, colon);
        try {
            port = std::stoi(hostPort.substr(colon + 1));
        } catch (...) {
            return false;
        }
    } else {
        host = hostPort;
    }

    if (host.empty() || host == "0.0.0.0") return false;
    if (port <= 0 || port > 65535) return false;
    return true;
}

}  // namespace

RedisService::RedisService(const core::Config &cfg) : cfg_(cfg) {
    urlParsedOk_ = parseUrl(cfg.redisUrl, username_, host_, port_, db_, password_);
    if (!urlParsedOk_) {
        LOG_ERROR << "Invalid or missing REDIS_URL (expected redis://[user:password@]host[:port][/db]): "
                  << (cfg.redisUrl.empty() ? "(empty)" : redactRedisUrl(cfg.redisUrl));
    }
}

bool RedisService::connect() {
    if (!urlParsedOk_) {
        LOG_WARN << "Redis connect skipped: REDIS_URL was not parsed";
        return false;
    }

    std::string connectIp =
        util::isIpv4Literal(host_) ? host_ : util::resolveHostToIpv4(host_);
    if (connectIp.empty()) {
        LOG_ERROR << "Failed to resolve Redis host: " << host_;
        return false;
    }

    try {
        client_ = drogon::nosql::RedisClient::newRedisClient(
            trantor::InetAddress(connectIp, static_cast<uint16_t>(port_)),
            /*connections=*/2, password_, static_cast<unsigned int>(db_), username_);
        std::string log = "Redis client created for " + host_ + " (" + connectIp + ":" +
                          std::to_string(port_) + "/" + std::to_string(db_) + ")";
        if (!username_.empty()) log += " acl_user=" + username_;
        LOG_INFO << log;
        return client_ != nullptr;
    } catch (const std::exception &e) {
        LOG_WARN << "Redis unavailable (" << host_ << " -> " << connectIp << ":" << port_
                 << "): " << e.what();
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

void RedisService::setStringEx(const std::string &key, const std::string &value,
                               int ttlSeconds, std::function<void(bool)> cb) {
    if (!client_) {
        cb(false);
        return;
    }
    client_->execCommandAsync(
        [cb](const drogon::nosql::RedisResult &) { cb(true); },
        [cb](const std::exception &e) {
            LOG_DEBUG << "Redis setStringEx error: " << e.what();
            cb(false);
        },
        "SET %s %s EX %d", key.c_str(), value.c_str(), ttlSeconds);
}

void RedisService::getString(const std::string &key,
                             std::function<void(std::optional<std::string>)> cb) {
    if (!client_) {
        cb(std::nullopt);
        return;
    }
    client_->execCommandAsync(
        [cb](const drogon::nosql::RedisResult &r) {
            if (r.type() == drogon::nosql::RedisResultType::kString) {
                cb(r.asString());
            } else {
                cb(std::nullopt);
            }
        },
        [cb](const std::exception &e) {
            LOG_DEBUG << "Redis getString error: " << e.what();
            cb(std::nullopt);
        },
        "GET %s", key.c_str());
}

void RedisService::deleteKey(const std::string &key, std::function<void()> cb) {
    if (!client_) {
        cb();
        return;
    }
    client_->execCommandAsync(
        [cb](const drogon::nosql::RedisResult &) { cb(); },
        [cb](const std::exception &) { cb(); }, "DEL %s", key.c_str());
}

void RedisService::requeueJsonBatch(const std::string &key,
                                    const std::vector<std::string> &items) {
    if (!client_ || items.empty()) return;
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
