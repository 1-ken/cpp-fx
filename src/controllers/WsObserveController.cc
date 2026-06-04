#include "controllers/WsObserveController.h"

#include <algorithm>
#include <ctime>

#include <trantor/utils/Logger.h>

#include "alerts/AlertManager.h"
#include "core/AppContext.h"
#include "core/Auth.h"
#include "core/Config.h"
#include "ctrader/CTraderClient.h"
#include "ctrader/SymbolRegistry.h"
#include "market/MarketHub.h"
#include "util/PairNormalizer.h"
#include "util/TimeUtil.h"

using namespace drogon;

namespace ctraderplus::controllers {

std::mutex WsObserveController::connsMu_;
std::set<WebSocketConnectionPtr> WsObserveController::conns_;

namespace {
const std::set<std::string> kValidIntervals = {"1m", "5m", "15m", "30m", "1h", "4h", "1d"};

std::string toJsonString(const Json::Value &v) {
    Json::StreamWriterBuilder wb;
    wb["indentation"] = "";
    return Json::writeString(wb, v);
}

Json::Value buildFormingCandle(double price, const std::string &interval) {
    int ivSec = util::intervalToSeconds(interval);
    if (ivSec == 0) ivSec = 60;
    std::time_t now = std::time(nullptr);
    long long bucket = (static_cast<long long>(now) / ivSec) * ivSec;
    std::time_t bucketEnd = static_cast<std::time_t>(bucket + ivSec);
    double timeIn = static_cast<double>(now - bucket);
    Json::Value c(Json::objectValue);
    c["timestamp"] = util::toIso8601(static_cast<std::time_t>(bucket));
    c["open"] = price;
    c["high"] = price;
    c["low"] = price;
    c["close"] = price;
    c["volume"] = 1;
    c["is_forming"] = true;
    c["interval"] = interval;
    c["expected_open"] = util::toIso8601(static_cast<std::time_t>(bucket));
    c["expected_close"] = util::toIso8601(bucketEnd);
    c["progress_percent"] = (timeIn / ivSec) * 100.0;
    c["time_remaining_seconds"] = static_cast<double>(ivSec) - timeIn;
    return c;
}

Json::Value enrich(const Json::Value &grouped, WsConnContext &ctx) {
    auto &app = core::AppContext::instance();
    Json::Value payload = grouped;  // deep copy

    // Optional pair filter + forming candle enrichment.
    if (ctx.pairCanon) {
        const std::string canon = *ctx.pairCanon;
        Json::Value filtered(Json::objectValue);
        for (const std::string &group : {std::string("currencies"), std::string("commodities")}) {
            Json::Value matched(Json::arrayValue);
            if (payload["pairs"].isMember(group)) {
                for (const auto &item : payload["pairs"][group]) {
                    if (util::canonicalPair(item.get("pair", "").asString()) != canon) continue;
                    Json::Value enriched = item;
                    double price = 0;
                    if (app.hub && app.hub->latestPrice(canon, price)) {
                        Json::Value fc = buildFormingCandle(price, ctx.interval);
                        for (const auto &k : fc.getMemberNames()) enriched[k] = fc[k];
                    }
                    matched.append(enriched);
                }
            }
            filtered[group] = matched;
        }
        payload["pairs"] = filtered;
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

    auto ctx = std::make_shared<WsConnContext>();
    ctx->userId = auth.userId;

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
    LOG_INFO << "WebSocket connected (interval=" << ctx->interval
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
        if (ctx && ctx->counted && app.hub) app.hub->decWs();
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
