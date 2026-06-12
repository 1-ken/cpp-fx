#pragma once

#include <functional>
#include <optional>
#include <string>

#include <trantor/net/EventLoop.h>

#include "core/Config.h"

namespace ctraderplus::ctrader {

struct CTraderTokenPair {
    std::string accessToken;
    std::string refreshToken;
};

// Refresh OAuth tokens via openapi.ctrader.com (must run HTTP on eventLoop).
void refreshAccessTokenHttp(const core::CTraderConfig &cfg, trantor::EventLoop *eventLoop,
                            std::function<void(std::optional<CTraderTokenPair>)> done);

}  // namespace ctraderplus::ctrader
