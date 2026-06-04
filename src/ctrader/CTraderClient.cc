#include "ctrader/CTraderClient.h"

#include <algorithm>
#include <cstring>

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
    LOG_INFO << "cTrader: connecting to " << cfg_.resolvedHost() << ":" << cfg_.port
             << " (" << ip << ")";
    client_->connect();
}

void CTraderClient::scheduleReconnect() {
    if (stopping_.load()) return;
    double delay = reconnectDelay_;
    reconnectDelay_ = std::min(reconnectDelay_ * 2.0, cfg_.reconnectMaxDelaySeconds);
    LOG_INFO << "cTrader: reconnecting in " << delay << "s";
    loop_->runAfter(delay, [this]() { connect(); });
}

void CTraderClient::onConnection(const trantor::TcpConnectionPtr &conn) {
    if (conn->connected()) {
        conn_ = conn;
        reconnectDelay_ = cfg_.reconnectBaseDelaySeconds;
        state_ = State::AppAuth;
        LOG_INFO << "cTrader: TCP/TLS connected; authenticating application";
        sendApplicationAuth();
    } else {
        LOG_WARN << "cTrader: disconnected";
        conn_.reset();
        ready_.store(false);
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

        if (cfg_.subscribeAllSymbols) {
            if (cfg_.maxSubscribedSymbols > 0 &&
                ids.size() > static_cast<size_t>(cfg_.maxSubscribedSymbols)) {
                ids.resize(static_cast<size_t>(cfg_.maxSubscribedSymbols));
            }
            subscribeSpotsBatched(ids);
            subscribedSpotIds_ = ids;
        } else if (!pendingSpotIds_.empty()) {
            subscribeSpotsBatched(pendingSpotIds_);
            subscribedSpotIds_ = pendingSpotIds_;
            pendingSpotIds_.clear();
        }
        state_ = State::Ready;
        ready_.store(true);
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
        LOG_WARN << "cTrader: error " << err.errorcode() << " - " << err.description();
        // Fail any pending request that carried this clientMsgId.
        const std::string id = envelope.clientmsgid();
        auto it = pendingTrendbars_.find(id);
        if (it != pendingTrendbars_.end()) {
            if (it->second.timeoutTimer != trantor::InvalidTimerId)
                loop_->invalidateTimer(it->second.timeoutTimer);
            auto cb = std::move(it->second.cb);
            pendingTrendbars_.erase(it);
            TrendbarsResult result;
            result.ok = false;
            result.error = err.errorcode() + ": " + err.description();
            if (cb) cb(std::move(result));
        }
        return;
    }
    if (type == HEARTBEAT_EVENT) {
        return;  // server heartbeat; nothing to do
    }
    if (type == PROTO_OA_ACCOUNTS_TOKEN_INVALIDATED_EVENT ||
        type == PROTO_OA_CLIENT_DISCONNECT_EVENT) {
        LOG_ERROR << "cTrader: session invalidated by server; will reconnect";
        ready_.store(false);
        if (stateCb_) stateCb_(false);
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

void CTraderClient::sendAccountAuth() {
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
        if (!ready_.load() || state_ != State::Ready) {
            pendingSpotIds_ = std::move(ids);
            return;
        }
        if (ids == subscribedSpotIds_) return;
        subscribeSpotsBatched(ids);
        subscribedSpotIds_ = std::move(ids);
    });
}

void CTraderClient::subscribeSpotsBatched(const std::vector<int64_t> &ids) {
    for (size_t off = 0; off < ids.size(); off += kSpotSubscribeBatch) {
        ProtoOASubscribeSpotsReq req;
        req.set_ctidtraderaccountid(cfg_.accountId);
        size_t end = std::min(off + kSpotSubscribeBatch, ids.size());
        for (size_t i = off; i < end; ++i) req.add_symbolid(ids[i]);
        sendFramed(frame(req));
    }
    LOG_INFO << "cTrader: subscribed to spots for " << ids.size() << " symbols";
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
