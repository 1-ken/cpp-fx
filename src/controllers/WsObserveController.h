#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include <drogon/WebSocketController.h>
#include <json/json.h>

namespace ctraderplus::controllers {

struct WsConnContext {
    std::string userId;
    std::string peerIp;
    std::string interval = "1m";
    std::optional<std::string> pairCanon;
    bool hasStreamParams = false;
    bool counted = false;
    uint64_t lastAlertsRevision = 0;
    bool hasCachedAlerts = false;
    Json::Value cachedAlerts;
    std::size_t inflightBytes = 0;
    std::chrono::steady_clock::time_point inflightSince{};
    uint64_t sendTicket = 0;
    std::chrono::steady_clock::time_point lastPong{};
};

// WebSocket endpoint /ws/observe. Mirrors the Python ws_observe handler:
// validates the access_token, supports interval/pair query params, attaches
// per-user alerts and a forming candle, and streams the market snapshot.
class WsObserveController : public drogon::WebSocketController<WsObserveController> {
  public:
    void handleNewConnection(const drogon::HttpRequestPtr &req,
                             const drogon::WebSocketConnectionPtr &conn) override;
    void handleNewMessage(const drogon::WebSocketConnectionPtr &conn, std::string &&msg,
                          const drogon::WebSocketMessageType &type) override;
    void handleConnectionClosed(const drogon::WebSocketConnectionPtr &conn) override;

    WS_PATH_LIST_BEGIN
    WS_PATH_ADD("/ws/observe");
    WS_PATH_LIST_END

    // Fan a grouped snapshot out to all connected clients (with per-conn
    // enrichment). Invoked by MarketHub's broadcast sink.
    static void broadcastToAll(std::shared_ptr<Json::Value> grouped);
    static void pushTriggered(const std::string &userId, const Json::Value &frame);

  private:
    static std::mutex connsMu_;
    static std::vector<drogon::WebSocketConnectionPtr> conns_;
};

}  // namespace ctraderplus::controllers
