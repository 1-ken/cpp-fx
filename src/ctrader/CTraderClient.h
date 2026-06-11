#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <trantor/net/EventLoopThread.h>
#include <trantor/net/TcpClient.h>

#include "core/Config.h"
#include "ctrader/Types.h"

namespace ctraderplus::ctrader {

struct TrendbarsResult {
    bool ok = false;
    std::string error;
    int64_t symbolId = 0;
    int period = 0;
    std::vector<TrendbarData> bars;
    bool hasMore = false;
};

using SymbolsCallback = std::function<void(std::vector<SymbolInfo>)>;
using SpotCallback = std::function<void(const SpotUpdate &)>;
using StateCallback = std::function<void(bool ready)>;
using TrendbarsCallback = std::function<void(TrendbarsResult)>;

// Manages the TLS Protobuf connection to the cTrader Open API: application +
// account auth, heartbeats, symbol discovery, spot subscriptions, live and
// historical trend bars. All socket I/O runs on a dedicated event loop.
class CTraderClient {
  public:
    explicit CTraderClient(const core::CTraderConfig &cfg);
    ~CTraderClient();

    void setSymbolsCallback(SymbolsCallback cb) { symbolsCb_ = std::move(cb); }
    void setSpotCallback(SpotCallback cb) { spotCb_ = std::move(cb); }
    void setStateCallback(StateCallback cb) { stateCb_ = std::move(cb); }

    void start();
    void stop();

    bool isReady() const { return ready_.load(); }

    // Request historical trend bars. The callback is invoked on the client's
    // event loop (success or failure / timeout).
    void getTrendbars(int64_t symbolId, int period, int64_t fromMs, int64_t toMs,
                      uint32_t count, TrendbarsCallback cb);

    // Subscribe / unsubscribe to a live trend bar series for a symbol.
    void subscribeLiveTrendbar(int64_t symbolId, int period);
    void unsubscribeLiveTrendbar(int64_t symbolId, int period);

    // Replace spot subscriptions (scoped mode). No-op until symbols are loaded.
    void refreshSpotSubscriptions(std::vector<int64_t> symbolIds);

  private:
    enum class State { Disconnected, Connecting, AppAuth, AccountAuth, LoadingSymbols, Ready };

    void connect();
    void scheduleReconnect();
    void onConnection(const trantor::TcpConnectionPtr &conn);
    void onMessage(const trantor::TcpConnectionPtr &conn, trantor::MsgBuffer *buf);
    void handleFrame(const std::string &payload);

    void sendApplicationAuth();
    void sendAccountAuth();
    void sendSymbolsListReq();
    void subscribeSpotsBatched(const std::vector<int64_t> &ids);
    void unsubscribeSpotsBatched(const std::vector<int64_t> &ids);
    void sendHeartbeat();
    void sendFramed(const std::string &framed);

    std::string nextClientMsgId();

    core::CTraderConfig cfg_;
    trantor::EventLoopThread loopThread_;
    trantor::EventLoop *loop_ = nullptr;
    std::shared_ptr<trantor::TcpClient> client_;
    trantor::TcpConnectionPtr conn_;

    std::atomic<bool> ready_{false};
    std::atomic<bool> stopping_{false};
    State state_ = State::Disconnected;
    double reconnectDelay_ = 1.0;
    trantor::TimerId heartbeatTimer_ = trantor::InvalidTimerId;

    uint64_t msgIdCounter_ = 0;

    struct Pending {
        TrendbarsCallback cb;
        trantor::TimerId timeoutTimer = trantor::InvalidTimerId;
    };
    std::unordered_map<std::string, Pending> pendingTrendbars_;

    SymbolsCallback symbolsCb_;
    SpotCallback spotCb_;
    StateCallback stateCb_;

    std::vector<int64_t> pendingSpotIds_;
    std::vector<int64_t> subscribedSpotIds_;
};

}  // namespace ctraderplus::ctrader
