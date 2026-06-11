#include "controllers/WsObserveController.h"

#include <algorithm>
#include <ctime>
#include <unordered_map>

#include <trantor/utils/Logger.h>

#include "alerts/AlertManager.h"
#include "core/ApiLog.h"
#include "core/AppContext.h"
#include "core/Auth.h"
#include "core/Config.h"
#include "ctrader/CTraderClient.h"
#include "ctrader/SymbolRegistry.h"
#include "market/MarketHub.h"
#include "util/FormingCandle.h"
#include "util/PairNormalizer.h"
#include "util/TimeUtil.h"

using namespace drogon;

namespace ctraderplus::controllers {

std::mutex WsObserveController::connsMu_;
std::set<WebSocketConnectionPtr> WsObserveController::conns_;

namespace {

std::mutex wsConnStatsMu_;
std::unordered_map<std::string, int> wsConnsPerUser_;
std::unordered_map<std::string, int> wsConnsPerIp_;

int countUserWsConnections(const std::string &userId) {
    std::lock_guard<std::mutex> lk(wsConnStatsMu_);
    auto it = wsConnsPerUser_.find(userId);
    return it == wsConnsPerUser_.end() ? 0 : it->second;
}

int countIpWsConnections(const std::string &ip) {
    std::lock_guard<std::mutex> lk(wsConnStatsMu_);
    auto it = wsConnsPerIp_.find(ip);
    return it == wsConnsPerIp_.end() ? 0 : it->second;
}

void trackWsOpen(const std::string &userId, const std::string &ip) {
    std::lock_guard<std::mutex> lk(wsConnStatsMu_);
    ++wsConnsPerUser_[userId];
    if (!ip.empty()) ++wsConnsPerIp_[ip];
}

void trackWsClose(const std::string &userId, const std::string &ip) {
    std::lock_guard<std::mutex> lk(wsConnStatsMu_);
    if (auto it = wsConnsPerUser_.find(userId); it != wsConnsPerUser_.end()) {
        if (--it->second <= 0) wsConnsPerUser_.erase(it);
    }
    if (!ip.empty()) {
        if (auto it = wsConnsPerIp_.find(ip); it != wsConnsPerIp_.end()) {
            if (--it->second <= 0) wsConnsPerIp_.erase(it);
        }
    }
}

const std::set<std::string> kValidIntervals = {"1m", "5m", "15m", "30m", "1h", "4h", "1d"};

std::string toJsonString(const Json::Value &v) {
    Json::StreamWriterBuilder wb;
    wb["indentation"] = "";
    return Json::writeString(wb, v);
}

Json::Value buildFormingForPair(const std::string &canon,
                                const std::string &interval,
                                market::MarketHub *hub) {
    if (!hub) return Json::Value::null;
    double price = 0;
    if (!hub->latestPrice(canon, price)) return Json::Value::null;

    ctrader::TrendbarData cached{};
    const ctrader::TrendbarData *lastBar = nullptr;
    if (hub->cachedTrendbar(canon, interval, cached)) {
        lastBar = &cached;
    }

    if (lastBar) {
        return util::buildFormingCandleMerged(price, interval, lastBar, nullptr);
    }
    return util::buildFormingCandleFromSpot(price, interval);
}

Json::Value enrich(const Json::Value &grouped, WsConnContext &ctx) {
    auto &app = core::AppContext::instance();
    Json::Value payload = grouped;  // deep copy

    Json::Value topForming(Json::nullValue);
    double chartLivePrice = 0;
    bool hasChartLivePrice = false;

    // Optional pair filter + forming candle enrichment.
    if (ctx.pairCanon) {
        const std::string canon = *ctx.pairCanon;
        Json::Value forming = buildFormingForPair(canon, ctx.interval, app.hub);
        if (!forming.isNull()) {
            topForming = forming;
            chartLivePrice = forming.get("close", 0.0).asDouble();
            hasChartLivePrice = true;
        } else if (app.hub && app.hub->latestPrice(canon, chartLivePrice)) {
            hasChartLivePrice = true;
        }

        Json::Value filtered(Json::objectValue);
        for (const std::string &group : {std::string("currencies"), std::string("commodities")}) {
            Json::Value matched(Json::arrayValue);
            if (payload["pairs"].isMember(group)) {
                for (const auto &item : payload["pairs"][group]) {
                    if (util::canonicalPair(item.get("pair", "").asString()) != canon) continue;
                    Json::Value enriched = item;
                    if (!forming.isNull()) {
                        for (const auto &k : forming.getMemberNames()) enriched[k] = forming[k];
                    }
                    matched.append(enriched);
                }
            }
            filtered[group] = matched;
        }
        payload["pairs"] = filtered;

        if (ctx.hasStreamParams) {
            payload["forming_candle"] = topForming;
            payload["has_forming_candle"] = !topForming.isNull();
            if (hasChartLivePrice) {
                payload["chart_live_price"] = chartLivePrice;
            }
        }
    }

    if (!ctx.hasStreamParams && app.alerts) {
        uint64_t rev = app.alerts->userAlertsRevision(ctx.userId);
        if (rev != ctx.lastAlertsRevision || !ctx.hasCachedAlerts) {
            Json::Value alerts(Json::objectValue);
            Json::Value active(Json::arrayValue);
            for (const auto &a : app.alerts->getActiveAlertsForUser(ctx.userId))
                active.append(a.toJson());
            Json::Value triggered(Json::arrayValue);
            for (const auto &a : app.alerts->getAllAlertsForUser(ctx.userId))
                if (a.status == "triggered") triggered.append(a.toJson());
            alerts["active"] = active;
            alerts["triggered"] = triggered;
            ctx.lastAlertsRevision = rev;
            ctx.cachedAlerts = alerts;
            ctx.hasCachedAlerts = true;
        }
        payload["alerts"] = ctx.cachedAlerts;
    }

    Json::Value stream(Json::objectValue);
    stream["interval"] = ctx.interval;
    stream["pair"] = ctx.pairCanon ? Json::Value(*ctx.pairCanon) : Json::Value::null;
    stream["stream_key"] = (ctx.pairCanon ? *ctx.pairCanon : "all") + ":" + ctx.interval;
    payload["stream"] = stream;
    return payload;
}
}  // namespace

void WsObserveController::handleNewConnection(const HttpRequestPtr &req,
                                              const WebSocketConnectionPtr &conn) {
    auto &app = core::AppContext::instance();

    std::string token = req->getParameter("access_token");
    auto auth = core::verifyWsAccessToken(token.empty() ? std::nullopt
                                                        : std::optional<std::string>(token));
    if (!auth.ok) {
        conn->shutdown(CloseCode::kViolation, "unauthorized");
        return;
    }

    const std::string peerIp = req->getPeerAddr().toIp();
    const int ipLimit = app.config->wsMaxConnectionsPerIp;
    if (ipLimit > 0 && countIpWsConnections(peerIp) >= ipLimit) {
        conn->shutdown(CloseCode::kViolation, "too many connections from this IP");
        return;
    }

    auto ctx = std::make_shared<WsConnContext>();
    ctx->userId = auth.userId;
    ctx->peerIp = peerIp;

    std::string intervalParam = req->getParameter("interval");
    std::string pairParam = req->getParameter("pair");
    ctx->hasStreamParams = req->getParameters().count("interval") > 0 ||
                           req->getParameters().count("pair") > 0;

    std::string interval = intervalParam.empty() ? "1m" : intervalParam;
    std::transform(interval.begin(), interval.end(), interval.begin(), ::tolower);
    if (kValidIntervals.find(interval) == kValidIntervals.end()) interval = "1m";
    ctx->interval = interval;

    if (!pairParam.empty()) {
        std::string first = pairParam.substr(0, pairParam.find(','));
        std::string canon = util::canonicalPair(first);
        if (!canon.empty()) ctx->pairCanon = canon;
    }

    bool ready = app.ctrader && app.ctrader->isReady();
    if (!ready) {
        Json::Value err;
        err["error"] = "Observer not ready";
        conn->send(toJsonString(err));
        conn->shutdown(CloseCode::kNormalClosure, "not ready");
        return;
    }

    if (app.hub) {
        app.hub->incWs();
        ctx->counted = true;
    }
    conn->setContext(ctx);
    {
        std::lock_guard<std::mutex> lk(connsMu_);
        conns_.insert(conn);
    }
    trackWsOpen(ctx->userId, peerIp);
    const int userConns = countUserWsConnections(ctx->userId);
    LOG_INFO << "WebSocket connected user=" << core::hashUserIdForLog(ctx->userId)
             << " user_conns=" << userConns << " interval=" << ctx->interval
             << " pair=" << (ctx->pairCanon ? *ctx->pairCanon : "all") << ")";
}

void WsObserveController::handleNewMessage(const WebSocketConnectionPtr &,
                                           std::string &&, const WebSocketMessageType &) {
    // No inbound protocol; ignore client messages.
}

void WsObserveController::handleConnectionClosed(const WebSocketConnectionPtr &conn) {
    auto &app = core::AppContext::instance();
    if (conn->hasContext()) {
        auto ctx = conn->getContext<WsConnContext>();
        if (ctx) {
            trackWsClose(ctx->userId, ctx->peerIp);
            if (ctx->counted && app.hub) app.hub->decWs();
        }
    }
    std::lock_guard<std::mutex> lk(connsMu_);
    conns_.erase(conn);
}

void WsObserveController::broadcastToAll(std::shared_ptr<Json::Value> grouped) {
    if (!grouped) return;
    std::vector<WebSocketConnectionPtr> snapshot;
    {
        std::lock_guard<std::mutex> lk(connsMu_);
        snapshot.assign(conns_.begin(), conns_.end());
    }
    for (auto &conn : snapshot) {
        if (!conn->connected() || !conn->hasContext()) continue;
        auto ctx = conn->getContext<WsConnContext>();
        if (!ctx) continue;
        Json::Value payload = enrich(*grouped, *ctx);
        conn->send(toJsonString(payload));
    }
}

}  // namespace ctraderplus::controllers
