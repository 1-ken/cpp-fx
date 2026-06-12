#include "ctrader/CTraderTokenStore.h"

#include <json/json.h>
#include <trantor/utils/Logger.h>
#include "services/RedisService.h"
#include "util/TimeUtil.h"

namespace ctraderplus::ctrader {

namespace {

CTraderTokenPair pairFromJson(const Json::Value &v) {
    CTraderTokenPair t;
    t.accessToken = v.get("access_token", "").asString();
    t.refreshToken = v.get("refresh_token", "").asString();
    return t;
}

}  // namespace

std::optional<CTraderTokenPair> CTraderTokenStore::load(services::RedisService *redis,
                                                      const std::string &key) {
    if (!redis || !redis->connected()) return std::nullopt;
    auto raw = redis->getStringSync(key);
    if (!raw || raw->empty()) return std::nullopt;

    Json::Value v;
    Json::CharReaderBuilder b;
    std::unique_ptr<Json::CharReader> reader(b.newCharReader());
    std::string errs;
    if (!reader->parse(raw->c_str(), raw->c_str() + raw->size(), &v, &errs)) {
        LOG_WARN << "cTrader token store: invalid JSON in Redis key " << key;
        return std::nullopt;
    }
    auto tokens = pairFromJson(v);
    if (tokens.accessToken.empty()) return std::nullopt;
    return tokens;
}

bool CTraderTokenStore::save(services::RedisService *redis, const std::string &key,
                           const CTraderTokenPair &tokens) {
    if (!redis || !redis->connected() || tokens.accessToken.empty()) return false;
    Json::Value v;
    v["access_token"] = tokens.accessToken;
    v["refresh_token"] = tokens.refreshToken;
    v["updated_at"] = util::nowIso8601();
    Json::StreamWriterBuilder wb;
    wb["indentation"] = "";
    return redis->setStringSync(key, Json::writeString(wb, v));
}

bool CTraderTokenStore::mergeFromRedis(services::RedisService *redis, const std::string &key,
                                     core::CTraderConfig &cfg) {
    auto stored = load(redis, key);
    if (!stored) {
        if (cfg.refreshToken.empty()) {
            LOG_ERROR << "cTrader tokens: CTRADER_REFRESH_TOKEN missing (env only)";
        } else {
            LOG_INFO << "cTrader tokens: using env (no Redis override at " << key << ")";
        }
        return false;
    }
    cfg.accessToken = stored->accessToken;
    if (!stored->refreshToken.empty()) cfg.refreshToken = stored->refreshToken;
    LOG_INFO << "cTrader tokens: loaded from Redis (" << key << ")";
    return true;
}

}  // namespace ctraderplus::ctrader
