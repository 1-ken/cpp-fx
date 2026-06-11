#include "ctrader/CTraderClient.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <random>

#include <netinet/tcp.h>
#include <sys/socket.h>

#include <trantor/utils/Logger.h>

#include "ctrader/ProtoUtil.h"
#include "util/HostResolve.h"

#include "OpenApiCommonMessages.pb.h"
#include "OpenApiMessages.pb.h"
#include "OpenApiModelMessages.pb.h"

namespace ctraderplus::ctrader {

namespace {
constexpr size_t kSpotSubscribeBatch = 100;

double scalePrice(uint64_t raw) { return static_cast<double>(raw) / kPriceScale; }

TrendbarData convertTrendbar(const ProtoOATrendbar &tb) {
    TrendbarData d;
    d.period = static_cast<int>(tb.period());
    d.utcTimestampMinutes = tb.utctimestampinminutes();
    double low = static_cast<double>(tb.low()) / kPriceScale;
    d.low = low;
    d.open = (static_cast<double>(tb.low()) + static_cast<double>(tb.deltaopen())) / kPriceScale;
    d.high = (static_cast<double>(tb.low()) + static_cast<double>(tb.deltahigh())) / kPriceScale;
    d.close = (static_cast<double>(tb.low()) + static_cast<double>(tb.deltaclose())) / kPriceScale;
    d.volume = tb.volume();
    return d;
}

std::vector<int64_t> sortedUniqueIds(std::vector<int64_t> ids) {
    std::sort(ids.begin(), ids.end());
    ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
    return ids;
}

std::vector<int64_t> idsNotYetSubscribed(const std::vector<int64_t> &sortedTarget,
                                         const std::vector<int64_t> &sortedSubscribed) {
    std::vector<int64_t> delta;
    delta.reserve(sortedTarget.size());
    for (int64_t id : sortedTarget) {
        if (!std::binary_search(sortedSubscribed.begin(), sortedSubscribed.end(), id)) {
            delta.push_back(id);
        }
    }
    return delta;
}

std::vector<int64_t> idsToUnsubscribe(const std::vector<int64_t> &sortedTarget,
                                      const std::vector<int64_t> &sortedSubscribed) {
    std::vector<int64_t> remove;
    for (int64_t id : sortedSubscribed) {
        if (!std::binary_search(sortedTarget.begin(), sortedTarget.end(), id)) {
            remove.push_back(id);
        }
    }
    return remove;
}

bool shouldSubscribeAllSymbols(const core::CTraderConfig &cfg) {
    return cfg.subscribeAllSymbols && !cfg.enforcePairAllowlist;
}

double monotonicSeconds() {
    return std::chrono::duration<double>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

double jitteredDelay(double base, double maxDelay) {
    static thread_local std::mt19937 rng(
        static_cast<uint32_t>(monotonicSeconds() * 1000.0));
    std::uniform_real_distribution<double> dist(0.0, base * 0.2);
    return std::min(base + dist(rng), maxDelay);
}

}  // namespace

CTraderClient::CTraderClient(const core::CTraderConfig &cfg) : cfg_(cfg) {
    reconnectDelay_ = cfg_.reconnectBaseDelaySeconds;
}

CTraderClient::~CTraderClient() { stop(); }

void CTraderClient::start() {
    loopThread_.run();
    loop_ = loopThread_.getLoop();
    loop_->queueInLoop([this]() {
        // Periodic heartbeat.
        loop_->runEvery(static_cast<double>(cfg_.heartbeatIntervalSeconds),
                        [this]() { sendHeartbeat(); });
        connect();
    });
}

void CTraderClient::stop() {
    stopping_.store(true);
    if (loop_) {
        loop_->runInLoop([this]() {
            if (conn_) conn_->shutdown();
            if (client_) client_->disconnect();
        });
    }
}

void CTraderClient::connect() {
    if (stopping_.load()) return;
    state_ = State::Connecting;
    ready_.store(false);

    std::string ip = util::resolveHostToIpv4(cfg_.resolvedHost());
    if (ip.empty()) {
        LOG_ERROR << "cTrader: failed to resolve host " << cfg_.resolvedHost();
        scheduleReconnect();
        return;
    }

    trantor::InetAddress addr(ip, static_cast<uint16_t>(cfg_.port));
    client_ = std::make_shared<trantor::TcpClient>(loop_, addr, "ctrader");
    client_->enableSSL(trantor::TLSPolicy::defaultClientPolicy(cfg_.resolvedHost()));
    client_->setConnectionCallback(
        [this](const trantor::TcpConnectionPtr &c) { onConnection(c); });
    client_->setMessageCallback(
        [this](const trantor::TcpConnectionPtr &c, trantor::MsgBuffer *b) {
            onMessage(c, b);
        });
    client_->setConnectionErrorCallback([this]() {
        LOG_WARN << "cTrader: connection error";
        scheduleReconnect();
    });
    client_->setSockOptCallback([](int fd) {
        int on = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &on, sizeof(on));
#ifdef TCP_KEEPIDLE
        int idle = 60;
        int interval = 10;
        int count = 3;
        ::setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof(idle));
        ::setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &interval, sizeof(interval));
        ::setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &count, sizeof(count));
#endif
    });
    LOG_INFO << "cTrader: connecting to " << cfg_.resolvedHost() << ":" << cfg_.port
             << " (" << ip << ")";
    client_->connect();
}

void CTraderClient::scheduleReconnect(double minDelaySeconds) {
    if (stopping_.load()) return;
    if (reconnectScheduled_.exchange(true)) return;

    double now = monotonicSeconds();
    if (circuitBreakerUntil_ > now) {
        double delay = circuitBreakerUntil_ - now;
        LOG_ERROR << "cTrader: circuit breaker open; reconnect paused for " << delay << "s";
        loop_->runAfter(delay, [this]() {
            reconnectScheduled_.store(false);
            connect();
        });
        return;
    }

    noteReconnectAttempt();
    double delay = jitteredDelay(reconnectDelay_, cfg_.reconnectMaxDelaySeconds);
    delay = std::max(delay, minDelaySeconds);
    reconnectDelay_ = std::min(reconnectDelay_ * 2.0, cfg_.reconnectMaxDelaySeconds);
    LOG_INFO << "cTrader: reconnecting in " << delay << "s";
    loop_->runAfter(delay, [this]() {
        reconnectScheduled_.store(false);
        connect();
    });
}

void CTraderClient::onConnection(const trantor::TcpConnectionPtr &conn) {
    if (conn->connected()) {
        conn_ = conn;
        state_ = State::AppAuth;
        LOG_INFO << "cTrader: TCP/TLS connected; authenticating application";
        sendApplicationAuth();
    } else {
        LOG_WARN << "cTrader: disconnected";
        conn_.reset();
        ready_.store(false);
        state_ = State::Disconnected;
        subscribedSpotIds_.clear();
        pendingTrendbars_.clear();
        if (stateCb_) stateCb_(false);
        scheduleReconnect();
    }
}

void CTraderClient::onMessage(const trantor::TcpConnectionPtr &, trantor::MsgBuffer *buf) {
    while (buf->readableBytes() >= 4) {
        int32_t len = buf->peekInt32();  // big-endian -> host order
        if (len < 0) {
            LOG_ERROR << "cTrader: negative frame length; closing";
            if (conn_) conn_->shutdown();
            return;
        }
        if (buf->readableBytes() < static_cast<size_t>(4 + len)) break;
        buf->retrieve(4);
        std::string payload(buf->peek(), static_cast<size_t>(len));
        buf->retrieve(static_cast<size_t>(len));
        handleFrame(payload);
    }
}

void CTraderClient::handleFrame(const std::string &payload) {
    ProtoMessage envelope;
    if (!envelope.ParseFromString(payload)) {
        LOG_ERROR << "cTrader: failed to parse ProtoMessage envelope";
        return;
    }
    const uint32_t type = envelope.payloadtype();
    const std::string &inner = envelope.payload();

    if (type == PROTO_OA_APPLICATION_AUTH_RES) {
        LOG_INFO << "cTrader: application authenticated";
        if (!cfg_.refreshToken.empty()) {
            state_ = State::RefreshingToken;
            sendRefreshToken();
        } else {
            state_ = State::AccountAuth;
            sendAccountAuth();
        }
        return;
    }
    if (type == PROTO_OA_REFRESH_TOKEN_RES) {
        ProtoOARefreshTokenRes res;
        res.ParseFromString(inner);
        cfg_.accessToken = res.accesstoken();
        if (res.has_refreshtoken() && !res.refreshtoken().empty()) {
            cfg_.refreshToken = res.refreshtoken();
        }
        LOG_INFO << "cTrader: access token refreshed";
        state_ = State::AccountAuth;
        sendAccountAuth();
        return;
    }
    if (type == PROTO_OA_ACCOUNT_AUTH_RES) {
        LOG_INFO << "cTrader: account authenticated";
        state_ = State::LoadingSymbols;
        sendSymbolsListReq();
        return;
    }
    if (type == PROTO_OA_SYMBOLS_LIST_RES) {
        ProtoOASymbolsListRes res;
        res.ParseFromString(inner);
        std::vector<SymbolInfo> symbols;
        symbols.reserve(res.symbol_size());
        std::vector<int64_t> ids;
        for (int i = 0; i < res.symbol_size(); ++i) {
            const auto &s = res.symbol(i);
            SymbolInfo info;
            info.id = s.symbolid();
            info.name = s.symbolname();
            info.enabled = s.has_enabled() ? s.enabled() : true;
            info.baseAssetId = s.baseassetid();
            info.quoteAssetId = s.quoteassetid();
            info.description = s.description();
            symbols.push_back(info);
            if (info.enabled) ids.push_back(info.id);
        }
        LOG_INFO << "cTrader: received " << symbols.size() << " symbols";
        if (symbolsCb_) symbolsCb_(symbols);

        if (shouldSubscribeAllSymbols(cfg_)) {
            if (cfg_.maxSubscribedSymbols > 0 &&
                ids.size() > static_cast<size_t>(cfg_.maxSubscribedSymbols)) {
                ids.resize(static_cast<size_t>(cfg_.maxSubscribedSymbols));
            }
            ids = sortedUniqueIds(std::move(ids));
            subscribeSpotsBatched(ids);
            subscribedSpotIds_ = std::move(ids);
        } else if (!pendingSpotIds_.empty()) {
            pendingSpotIds_ = sortedUniqueIds(std::move(pendingSpotIds_));
            subscribeSpotsBatched(pendingSpotIds_);
            subscribedSpotIds_ = pendingSpotIds_;
            pendingSpotIds_.clear();
        }
        state_ = State::Ready;
        ready_.store(true);
        resetBackoffOnReady();
        if (stateCb_) stateCb_(true);
        return;
    }
    if (type == PROTO_OA_SPOT_EVENT) {
        ProtoOASpotEvent ev;
        ev.ParseFromString(inner);
        SpotUpdate up;
        up.symbolId = ev.symbolid();
        up.hasBid = ev.has_bid();
        if (up.hasBid) up.bid = scalePrice(ev.bid());
        up.hasAsk = ev.has_ask();
        if (up.hasAsk) up.ask = scalePrice(ev.ask());
        up.hasSessionClose = ev.has_sessionclose();
        if (up.hasSessionClose) up.sessionClose = scalePrice(ev.sessionclose());
        up.timestampMs = ev.has_timestamp() ? ev.timestamp() : 0;
        for (int i = 0; i < ev.trendbar_size(); ++i) {
            up.liveTrendbars.push_back(convertTrendbar(ev.trendbar(i)));
        }
        if (spotCb_) spotCb_(up);
        return;
    }
    if (type == PROTO_OA_GET_TRENDBARS_RES) {
        ProtoOAGetTrendbarsRes res;
        res.ParseFromString(inner);
        TrendbarsResult result;
        result.ok = true;
        result.symbolId = res.has_symbolid() ? res.symbolid() : 0;
        result.period = static_cast<int>(res.period());
        result.hasMore = res.has_hasmore() ? res.hasmore() : false;
        for (int i = 0; i < res.trendbar_size(); ++i) {
            result.bars.push_back(convertTrendbar(res.trendbar(i)));
        }
        const std::string id = envelope.clientmsgid();
        auto it = pendingTrendbars_.find(id);
        if (it != pendingTrendbars_.end()) {
            if (it->second.timeoutTimer != trantor::InvalidTimerId)
                loop_->invalidateTimer(it->second.timeoutTimer);
            auto cb = std::move(it->second.cb);
            pendingTrendbars_.erase(it);
            if (cb) cb(std::move(result));
        }
        return;
    }
    if (type == PROTO_OA_ERROR_RES) {
        ProtoOAErrorRes err;
        err.ParseFromString(inner);
        const std::string code = err.errorcode();
        uint64_t retryAfterMs = err.has_retryafter() ? err.retryafter() : 0;
        if (code == "ALREADY_SUBSCRIBED") {
            LOG_DEBUG << "cTrader: already subscribed (ignored)";
        } else {
            handleOaError(code, err.description(), retryAfterMs);
        }
        const std::string id = envelope.clientmsgid();
        auto it = pendingTrendbars_.find(id);
        if (it != pendingTrendbars_.end()) {
            if (it->second.timeoutTimer != trantor::InvalidTimerId)
                loop_->invalidateTimer(it->second.timeoutTimer);
            auto cb = std::move(it->second.cb);
            pendingTrendbars_.erase(it);
            TrendbarsResult result;
            result.ok = false;
            result.error = code + ": " + err.description();
            if (cb) cb(std::move(result));
        }
        return;
    }
    if (type == HEARTBEAT_EVENT) {
        return;  // server heartbeat; nothing to do
    }
    if (type == PROTO_OA_ACCOUNTS_TOKEN_INVALIDATED_EVENT ||
        type == PROTO_OA_CLIENT_DISCONNECT_EVENT) {
        LOG_ERROR << "cTrader: session invalidated by server; forcing reconnect";
        ready_.store(false);
        if (stateCb_) stateCb_(false);
        forceDisconnectAndReconnect(5.0);
        return;
    }
    // Other payload types are ignored for this read-only market data client.
}

void CTraderClient::sendApplicationAuth() {
    ProtoOAApplicationAuthReq req;
    req.set_clientid(cfg_.clientId);
    req.set_clientsecret(cfg_.clientSecret);
    sendFramed(frame(req));
}

void CTraderClient::sendRefreshToken() {
    ProtoOARefreshTokenReq req;
    req.set_refreshtoken(cfg_.refreshToken);
    sendFramed(frame(req));
}

void CTraderClient::sendAccountAuth() {
    if (outboundPaused()) return;
    ProtoOAAccountAuthReq req;
    req.set_ctidtraderaccountid(cfg_.accountId);
    req.set_accesstoken(cfg_.accessToken);
    sendFramed(frame(req));
}

void CTraderClient::sendSymbolsListReq() {
    ProtoOASymbolsListReq req;
    req.set_ctidtraderaccountid(cfg_.accountId);
    req.set_includearchivedsymbols(cfg_.includeArchivedSymbols);
    sendFramed(frame(req));
}

void CTraderClient::refreshSpotSubscriptions(std::vector<int64_t> symbolIds) {
    if (!loop_) return;
    loop_->queueInLoop([this, ids = std::move(symbolIds)]() mutable {
        ids = sortedUniqueIds(std::move(ids));
        if (!ready_.load() || state_ != State::Ready) {
            pendingSpotIds_ = std::move(ids);
            return;
        }
        if (shouldSubscribeAllSymbols(cfg_)) return;
        if (ids == subscribedSpotIds_) return;

        std::vector<int64_t> toRemove =
            idsToUnsubscribe(ids, subscribedSpotIds_);
        std::vector<int64_t> toAdd = idsNotYetSubscribed(ids, subscribedSpotIds_);
        if (!toRemove.empty()) unsubscribeSpotsBatched(toRemove);
        if (!toAdd.empty()) subscribeSpotsBatched(toAdd);
        subscribedSpotIds_ = std::move(ids);
    });
}

void CTraderClient::subscribeSpotsBatched(const std::vector<int64_t> &ids) {
    if (ids.empty() || outboundPaused()) return;
    const bool stagger =
        lastRateLimitAt_ > 0 && (monotonicSeconds() - lastRateLimitAt_) < 120.0;
    const size_t batchCount =
        (ids.size() + kSpotSubscribeBatch - 1) / kSpotSubscribeBatch;

    auto sendOneBatch = [this](const std::vector<int64_t> &all, size_t off) {
        if (outboundPaused()) return;
        ProtoOASubscribeSpotsReq req;
        req.set_ctidtraderaccountid(cfg_.accountId);
        size_t end = std::min(off + kSpotSubscribeBatch, all.size());
        for (size_t i = off; i < end; ++i) req.add_symbolid(all[i]);
        sendFramed(frame(req));
    };

    if (!stagger || batchCount <= 1) {
        for (size_t off = 0; off < ids.size(); off += kSpotSubscribeBatch) {
            sendOneBatch(ids, off);
        }
    } else {
        auto idsCopy = std::make_shared<std::vector<int64_t>>(ids);
        for (size_t batch = 0; batch < batchCount; ++batch) {
            size_t off = batch * kSpotSubscribeBatch;
            double delay = static_cast<double>(batch) * 0.2;
            loop_->runAfter(delay, [this, idsCopy, off, sendOneBatch]() {
                sendOneBatch(*idsCopy, off);
            });
        }
        LOG_INFO << "cTrader: staggering spot subscribe across " << batchCount
                 << " batches (recent rate limit)";
    }
    LOG_INFO << "cTrader: subscribed to spots for " << ids.size() << " symbols";
}

void CTraderClient::unsubscribeSpotsBatched(const std::vector<int64_t> &ids) {
    if (ids.empty()) return;
    for (size_t off = 0; off < ids.size(); off += kSpotSubscribeBatch) {
        ProtoOAUnsubscribeSpotsReq req;
        req.set_ctidtraderaccountid(cfg_.accountId);
        size_t end = std::min(off + kSpotSubscribeBatch, ids.size());
        for (size_t i = off; i < end; ++i) req.add_symbolid(ids[i]);
        sendFramed(frame(req));
    }
    LOG_INFO << "cTrader: unsubscribed from spots for " << ids.size() << " symbols";
}

void CTraderClient::subscribeLiveTrendbar(int64_t symbolId, int period) {
    if (!loop_) return;
    loop_->queueInLoop([this, symbolId, period]() {
        if (!ready_.load()) return;
        ProtoOASubscribeLiveTrendbarReq req;
        req.set_ctidtraderaccountid(cfg_.accountId);
        req.set_period(static_cast<ProtoOATrendbarPeriod>(period));
        req.set_symbolid(symbolId);
        sendFramed(frame(req));
    });
}

void CTraderClient::unsubscribeLiveTrendbar(int64_t symbolId, int period) {
    if (!loop_) return;
    loop_->queueInLoop([this, symbolId, period]() {
        if (!ready_.load()) return;
        ProtoOAUnsubscribeLiveTrendbarReq req;
        req.set_ctidtraderaccountid(cfg_.accountId);
        req.set_period(static_cast<ProtoOATrendbarPeriod>(period));
        req.set_symbolid(symbolId);
        sendFramed(frame(req));
    });
}

void CTraderClient::sendHeartbeat() {
    if (!conn_ || !conn_->connected()) return;
    ProtoHeartbeatEvent hb;
    sendFramed(frame(hb));
}

void CTraderClient::sendFramed(const std::string &framed) {
    if (conn_ && conn_->connected()) {
        conn_->send(framed);
    }
}

std::string CTraderClient::nextClientMsgId() {
    return "req-" + std::to_string(++msgIdCounter_);
}

std::string CTraderClient::lastErrorCode() const {
    std::lock_guard<std::mutex> lk(statsMu_);
    return lastErrorCode_;
}

bool CTraderClient::circuitBreakerOpen() const {
    return circuitBreakerUntil_ > monotonicSeconds();
}

bool CTraderClient::outboundPaused() const {
    return outboundPausedUntil_ > monotonicSeconds();
}

void CTraderClient::resetBackoffOnReady() {
    reconnectDelay_ = cfg_.reconnectBaseDelaySeconds;
    reconnectAttemptsInWindow_ = 0;
    reconnectWindowStart_ = std::chrono::steady_clock::now();
    circuitBreakerUntil_ = 0;
}

void CTraderClient::noteReconnectAttempt() {
    ++reconnectCount_;
    auto now = std::chrono::steady_clock::now();
    double windowSec = cfg_.reconnectCircuitBreakerWindowSeconds;
    if (std::chrono::duration<double>(now - reconnectWindowStart_).count() > windowSec) {
        reconnectWindowStart_ = now;
        reconnectAttemptsInWindow_ = 0;
    }
    if (++reconnectAttemptsInWindow_ >= cfg_.reconnectCircuitBreakerThreshold) {
        circuitBreakerUntil_ =
            monotonicSeconds() + cfg_.reconnectCircuitBreakerCooldownSeconds;
        LOG_ERROR << "cTrader: circuit breaker opened after "
                  << reconnectAttemptsInWindow_ << " reconnect attempts in "
                  << windowSec << "s";
        reconnectAttemptsInWindow_ = 0;
        reconnectWindowStart_ = now;
    }
}

void CTraderClient::handleOaError(const std::string &code, const std::string &description,
                                  uint64_t retryAfterMs) {
    {
        std::lock_guard<std::mutex> lk(statsMu_);
        lastErrorCode_ = code;
    }
    LOG_WARN << "cTrader: error code=" << code << " description=" << description
             << " retry_after_ms=" << retryAfterMs
             << " account_id=" << cfg_.accountId;

    if (code == "BLOCKED_PAYLOAD_TYPE") {
        ++rateLimitCount_;
        lastRateLimitAt_ = monotonicSeconds();
        double pauseSec = std::max(60.0, static_cast<double>(retryAfterMs) / 1000.0);
        outboundPausedUntil_ = monotonicSeconds() + pauseSec;
        LOG_WARN << "cTrader: rate limited; pausing outbound requests for " << pauseSec << "s";
        return;
    }
    if (code == "CANT_ROUTE_REQUEST") {
        forceDisconnectAndReconnect(5.0);
        return;
    }
    if (code == "CH_ACCESS_TOKEN_INVALID" || code == "OA_AUTH_TOKEN_EXPIRED") {
        forceDisconnectAndReconnect(2.0);
    }
}

void CTraderClient::forceDisconnectAndReconnect(double minDelaySeconds) {
    subscribedSpotIds_.clear();
    pendingTrendbars_.clear();
    ready_.store(false);
    state_ = State::Disconnected;
    if (conn_) conn_->shutdown();
    conn_.reset();
    if (stateCb_) stateCb_(false);
    scheduleReconnect(minDelaySeconds);
}

void CTraderClient::getTrendbars(int64_t symbolId, int period, int64_t fromMs,
                                 int64_t toMs, uint32_t count, TrendbarsCallback cb) {
    if (!loop_) {
        TrendbarsResult r;
        r.ok = false;
        r.error = "client not started";
        cb(std::move(r));
        return;
    }
    loop_->queueInLoop([this, symbolId, period, fromMs, toMs, count,
                        cb = std::move(cb)]() mutable {
        if (!ready_.load()) {
            TrendbarsResult r;
            r.ok = false;
            r.error = "cTrader not ready";
            cb(std::move(r));
            return;
        }
        std::string id = nextClientMsgId();
        ProtoOAGetTrendbarsReq req;
        req.set_ctidtraderaccountid(cfg_.accountId);
        if (fromMs > 0) req.set_fromtimestamp(fromMs);
        if (toMs > 0) req.set_totimestamp(toMs);
        req.set_period(static_cast<ProtoOATrendbarPeriod>(period));
        req.set_symbolid(symbolId);
        if (count > 0) req.set_count(count);

        Pending p;
        p.cb = std::move(cb);
        p.timeoutTimer = loop_->runAfter(cfg_.requestTimeoutSeconds, [this, id]() {
            auto it = pendingTrendbars_.find(id);
            if (it != pendingTrendbars_.end()) {
                auto cb2 = std::move(it->second.cb);
                pendingTrendbars_.erase(it);
                TrendbarsResult r;
                r.ok = false;
                r.error = "request timed out";
                if (cb2) cb2(std::move(r));
            }
        });
        pendingTrendbars_[id] = std::move(p);
        sendFramed(frame(req, id));
    });
}

}  // namespace ctraderplus::ctrader
