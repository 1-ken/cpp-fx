#include "ctrader/CTraderAuth.h"

#include <sstream>

#include <drogon/HttpClient.h>
#include <drogon/HttpRequest.h>
#include <drogon/utils/Utilities.h>
#include <json/json.h>
#include <trantor/utils/Logger.h>

using namespace drogon;

namespace ctraderplus::ctrader {

namespace {

std::string buildRefreshPath(const core::CTraderConfig &cfg) {
    std::ostringstream qs;
    qs << "/apps/token?grant_type=refresh_token"
       << "&refresh_token=" << utils::urlEncode(cfg.refreshToken)
       << "&client_id=" << utils::urlEncode(cfg.clientId)
       << "&client_secret=" << utils::urlEncode(cfg.clientSecret);
    return qs.str();
}

std::optional<CTraderTokenPair> parseTokenResponse(const std::string &body) {
    Json::Value root;
    Json::CharReaderBuilder b;
    std::unique_ptr<Json::CharReader> reader(b.newCharReader());
    std::string errs;
    if (!reader->parse(body.c_str(), body.c_str() + body.size(), &root, &errs)) {
        LOG_WARN << "cTrader HTTP refresh: invalid JSON response";
        return std::nullopt;
    }
    if (root.isMember("errorCode") && !root["errorCode"].isNull()) {
        LOG_WARN << "cTrader HTTP refresh error: " << root.get("errorCode", "").asString()
                 << " " << root.get("description", "").asString();
        return std::nullopt;
    }
    std::string access = root.get("accessToken", "").asString();
    if (access.empty()) access = root.get("access_token", "").asString();
    if (access.empty()) {
        LOG_WARN << "cTrader HTTP refresh: missing accessToken in response";
        return std::nullopt;
    }
    CTraderTokenPair pair;
    pair.accessToken = access;
    pair.refreshToken = root.get("refreshToken", "").asString();
    if (pair.refreshToken.empty()) pair.refreshToken = root.get("refresh_token", "").asString();
    return pair;
}

}  // namespace

void refreshAccessTokenHttp(const core::CTraderConfig &cfg, trantor::EventLoop *eventLoop,
                            std::function<void(std::optional<CTraderTokenPair>)> done) {
    if (!eventLoop || cfg.refreshToken.empty() || cfg.clientId.empty() ||
        cfg.clientSecret.empty()) {
        if (cfg.refreshToken.empty()) {
            LOG_ERROR << "cTrader HTTP refresh skipped: CTRADER_REFRESH_TOKEN missing";
        }
        done(std::nullopt);
        return;
    }

    eventLoop->queueInLoop([cfg, done = std::move(done)]() mutable {
        auto client = HttpClient::newHttpClient("https://openapi.ctrader.com");
        auto req = HttpRequest::newHttpRequest();
        req->setMethod(Get);
        req->setPath(buildRefreshPath(cfg));
        req->addHeader("Accept", "application/json");

        client->sendRequest(
            req,
            [done = std::move(done)](ReqResult result, const HttpResponsePtr &resp) mutable {
                if (result != ReqResult::Ok || !resp) {
                    LOG_WARN << "cTrader HTTP refresh: request failed";
                    done(std::nullopt);
                    return;
                }
                const int code = static_cast<int>(resp->getStatusCode());
                if (code < 200 || code >= 300) {
                    LOG_WARN << "cTrader HTTP refresh: HTTP " << code << " body="
                             << resp->getBody();
                    done(std::nullopt);
                    return;
                }
                done(parseTokenResponse(std::string(resp->getBody())));
            });
    });
}

}  // namespace ctraderplus::ctrader
