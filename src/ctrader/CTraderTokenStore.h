#pragma once

#include <optional>
#include <string>

#include "core/Config.h"
#include "ctrader/CTraderAuth.h"

namespace ctraderplus::services {
class RedisService;
}

namespace ctraderplus::ctrader {

// Persists cTrader OAuth tokens in Redis so container restarts reuse refreshed tokens.
class CTraderTokenStore {
  public:
    static std::optional<CTraderTokenPair> load(services::RedisService *redis,
                                                const std::string &key);
    static bool save(services::RedisService *redis, const std::string &key,
                     const CTraderTokenPair &tokens);

    // If Redis has tokens, override cfg access/refresh (Redis wins over env).
    static bool mergeFromRedis(services::RedisService *redis, const std::string &key,
                               core::CTraderConfig &cfg);
};

}  // namespace ctraderplus::ctrader
