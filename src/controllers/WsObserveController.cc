#include "controllers/WsObserveController.h"

#include <algorithm>
#include <chrono>
#include <ctime>
#include <map>
#include <set>
#include <tuple>
#include <unordered_map>

#include <trantor/utils/Logger.h>

#include "alerts/AlertManager.h"
#include "core/ApiLog.h"
#include "core/AppContext.h"
#include "core/Auth.h"
#include "core/Config.h"
#include "core/Metrics.h"
#include "ctrader/CTraderClient.h"
#include "ctrader/SymbolRegistry.h"
#include "market/MarketHub.h"
#include "util/FormingCandle.h"
#include "util/PairNormalizer.h"
#include "util/TimeUtil.h"

using namespace drogon;

namespace ctraderplus::controllers {

std::mutex WsObserveController::connsMu_;
std::vector<WebSocketConnectionPtr> WsObserveController::conns_;

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
    const bool hasPrice = hub->latestPrice(canon, price);
    const auto running = hub->formingBar(canon, interval);

    ctrader::TrendbarData cached{};
    const ctrader::TrendbarData *lastBar = nullptr;
    if (hub->cachedTrendbar(canon, interval, cached)) {
        lastBar = &cached;
    }

    return util::composeFormingCandle(running ? &*running : nullptr, price, hasPrice, interval,
                                      lastBar);
}

Json::Value enrich(const Json::Value &grouped, WsConnContext &ctx,
                   const Json::Value *precomputedForming = nullptr) {
    auto &app = core::AppContext::instance();
    Json::Value payload = grouped;  // deep copy

    Json::Value topForming(Json::nullValue);
    double chartLivePrice = 0;
    bool hasChartLivePrice = false;

    // Optional pair filter + forming candle enrichment.
    if (ctx.pairCanon) {
        const std::string canon = *ctx.pairCanon;
        Json::Value forming = precomputedForming ? *precomputedForming
                                                 : buildFormingForPair(canon, ctx.interval, app.hub);
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
            Json::Value waiting(Json::arrayValue);
            Json::Value triggered(Json::arrayValue);
            Json::Value expired(Json::arrayValue);
            Json::Value allArr(Json::arrayValue);
            for (const auto &a : app.alerts->getActiveAlertsForUser(ctx.userId))
                active.append(a.toJson());
            for (const auto &a : app.alerts->getAllAlertsForUser(ctx.userId)) {
                allArr.append(a.toJson());
                if (a.status == "waiting") waiting.append(a.toJson());
                else if (a.status == "triggered") triggered.append(a.toJson());
                else if (a.status == "expired") expired.append(a.toJson());
            }
            alerts["active"] = active;
            alerts["waiting"] = waiting;
            alerts["triggered"] = triggered;
            alerts["expired"] = expired;
            alerts["all"] = allArr;
            alerts["total"] = static_cast<int>(allArr.size());
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
    ctx->lastPong = std::chrono::steady_clock::now();
    conn->setPingMessage("", std::chrono::seconds(10));
    {
        std::lock_guard<std::mutex> lk(connsMu_);
        conns_.push_back(conn);
    }
    trackWsOpen(ctx->userId, peerIp);
    const int userConns = countUserWsConnections(ctx->userId);
    LOG_INFO << "WebSocket connected user=" << core::hashUserIdForLog(ctx->userId)
             << " user_conns=" << userConns << " interval=" << ctx->interval
             << " pair=" << (ctx->pairCanon ? *ctx->pairCanon : "all") << ")";
}

void WsObserveController::handleNewMessage(const WebSocketConnectionPtr &conn,
                                           std::string &&, const WebSocketMessageType &type) {
    if (type != WebSocketMessageType::Pong && type != WebSocketMessageType::Ping) return;
    if (!conn || !conn->hasContext()) return;
    auto ctx = conn->getContext<WsConnContext>();
    if (ctx) ctx->lastPong = std::chrono::steady_clock::now();
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
    conns_.erase(std::remove(conns_.begin(), conns_.end(), conn), conns_.end());
}

namespace {

struct FanoutKey {
    std::string interval;
    std::string pair;
    bool hasStreamParams = false;
    std::string userId;
    bool operator<(const FanoutKey &o) const {
        return std::tie(interval, pair, hasStreamParams, userId) <
               std::tie(o.interval, o.pair, o.hasStreamParams, o.userId);
    }
};

bool sendWithBackpressure(const WebSocketConnectionPtr &conn, WsConnContext &ctx,
                          std::string msg) {
    auto &app = core::AppContext::instance();
    const double timeout = app.config ? app.config->wsSendTimeoutSeconds : 3.0;
    const auto now = std::chrono::steady_clock::now();
    if (ctx.inflightBytes > 0 && timeout > 0) {
        const double age =
            std::chrono::duration<double>(now - ctx.inflightSince).count();
        if (age > timeout) {
            core::Metrics::instance().wsDroppedSlow.fetch_add(1, std::memory_order_relaxed);
            LOG_WARN << "ws_dropped_slow user=" << core::hashUserIdForLog(ctx.userId)
                     << " inflight_bytes=" << ctx.inflightBytes;
            conn->forceClose();
            return false;
        }
    }
    if (ctx.inflightBytes == 0) ctx.inflightSince = now;
    ctx.inflightBytes += msg.size();
    const uint64_t ticket = ++ctx.sendTicket;
    const std::size_t n = msg.size();
    conn->send(msg);
    auto loop = drogon::app().getLoop();
    if (loop) {
        loop->runAfter(0, [conn, ticket, n]() {
            if (!conn || !conn->connected() || !conn->hasContext()) return;
            auto held = conn->getContext<WsConnContext>();
            if (!held || held->sendTicket != ticket) return;
            if (held->inflightBytes >= n) held->inflightBytes -= n;
            else held->inflightBytes = 0;
            if (held->inflightBytes == 0) held->inflightSince = {};
        });
    }
    return true;
}

}  // namespace

void WsObserveController::broadcastToAll(std::shared_ptr<Json::Value> grouped) {
    if (!grouped) return;
    const auto started = std::chrono::steady_clock::now();
    struct Target {
        WebSocketConnectionPtr conn;
        std::shared_ptr<WsConnContext> ctx;
    };
    std::vector<Target> snapshot;
    std::vector<WebSocketConnectionPtr> silent;
    {
        std::lock_guard<std::mutex> lk(connsMu_);
        const auto now = std::chrono::steady_clock::now();
        const double timeout = core::AppContext::instance().config
                                   ? core::AppContext::instance().config->wsSendTimeoutSeconds
                                   : 3.0;
        const double staleAfter = std::max(20.0, timeout + 10.0);
        snapshot.reserve(conns_.size());
        for (auto it = conns_.begin(); it != conns_.end();) {
            auto &conn = *it;
            if (!conn || !conn->connected() || !conn->hasContext()) {
                it = conns_.erase(it);
                continue;
            }
            auto ctx = conn->getContext<WsConnContext>();
            if (!ctx) {
                it = conns_.erase(it);
                continue;
            }
            const double quiet =
                std::chrono::duration<double>(now - ctx->lastPong).count();
            if (ctx->lastPong.time_since_epoch().count() != 0 && quiet > staleAfter) {
                core::Metrics::instance().wsDroppedSlow.fetch_add(1, std::memory_order_relaxed);
                LOG_WARN << "ws_dropped_silent user=" << core::hashUserIdForLog(ctx->userId);
                silent.push_back(conn);
                it = conns_.erase(it);
                continue;
            }
            snapshot.push_back({conn, ctx});
            ++it;
        }
    }
    for (auto &conn : silent) {
        if (conn) conn->forceClose();
    }
    core::Metrics::instance().wsClients.store(static_cast<int>(snapshot.size()),
                                              std::memory_order_relaxed);

    std::map<std::string, Json::Value> formingByKey;
    auto formingFor = [&](const std::string &pair, const std::string &interval) -> Json::Value {
        const std::string key = pair + "|" + interval;
        auto it = formingByKey.find(key);
        if (it == formingByKey.end()) {
            it = formingByKey.emplace(key, buildFormingForPair(pair, interval,
                                                               core::AppContext::instance().hub))
                     .first;
        }
        return it->second;
    };

    std::map<FanoutKey, std::vector<Target>> groups;
    for (auto &target : snapshot) {
        FanoutKey key;
        key.interval = target.ctx->interval;
        key.hasStreamParams = target.ctx->hasStreamParams;
        if (target.ctx->pairCanon) key.pair = *target.ctx->pairCanon;
        if (!target.ctx->hasStreamParams) key.userId = target.ctx->userId;
        groups[key].push_back(std::move(target));
    }

    for (auto &group : groups) {
        if (group.second.empty() || !group.second.front().ctx) continue;
        Json::Value forming(Json::nullValue);
        const Json::Value *formingPtr = nullptr;
        if (!group.first.pair.empty()) {
            forming = formingFor(group.first.pair, group.first.interval);
            formingPtr = &forming;
        }
        Json::Value payload = enrich(*grouped, *group.second.front().ctx, formingPtr);
        for (std::size_t i = 1; i < group.second.size(); ++i) {
            auto &ctx = *group.second[i].ctx;
            ctx.lastAlertsRevision = group.second.front().ctx->lastAlertsRevision;
            ctx.cachedAlerts = group.second.front().ctx->cachedAlerts;
            ctx.hasCachedAlerts = group.second.front().ctx->hasCachedAlerts;
        }
        std::string wire;
        try {
            wire = toJsonString(payload);
        } catch (const std::exception &e) {
            LOG_WARN << "WebSocket broadcast failed: " << e.what();
            continue;
        }
        for (auto &target : group.second) {
            if (!target.conn || !target.conn->connected() || !target.ctx) continue;
            try {
                sendWithBackpressure(target.conn, *target.ctx, wire);
            } catch (const std::exception &e) {
                LOG_WARN << "WebSocket broadcast failed: " << e.what();
            }
        }
    }
    const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - started)
                        .count();
    core::Metrics::instance().wsBroadcasts.fetch_add(1, std::memory_order_relaxed);
    core::Metrics::instance().wsBroadcastDurationUs.fetch_add(
        static_cast<uint64_t>(us > 0 ? us : 0), std::memory_order_relaxed);
}

void WsObserveController::pushTriggered(const std::string &userId, const Json::Value &frame) {
    if (userId.empty()) return;
    std::string wire;
    try {
        wire = toJsonString(frame);
    } catch (const std::exception &e) {
        LOG_WARN << "alert frame encode failed: " << e.what();
        return;
    }
    std::vector<WebSocketConnectionPtr> targets;
    {
        std::lock_guard<std::mutex> lk(connsMu_);
        for (const auto &conn : conns_) {
            if (!conn || !conn->connected() || !conn->hasContext()) continue;
            auto ctx = conn->getContext<WsConnContext>();
            if (ctx && ctx->userId == userId) targets.push_back(conn);
        }
    }
    for (auto &conn : targets) {
        try {
            conn->send(wire);
        } catch (const std::exception &e) {
            LOG_WARN << "alert frame send failed: " << e.what();
        }
    }
}

}  // namespace ctraderplus::controllers
