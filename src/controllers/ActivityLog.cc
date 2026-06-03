#include "controllers/ActivityLog.h"

#include <drogon/HttpRequest.h>

#include "core/AppContext.h"
#include "services/PostgresService.h"

namespace ctraderplus::controllers {

std::string clientIp(const drogon::HttpRequestPtr &req) {
    return req->getHeader("x-forwarded-for").empty() ? req->getPeerAddr().toIp()
                                                     : req->getHeader("x-forwarded-for");
}

std::string clientUserAgent(const drogon::HttpRequestPtr &req) {
    return req->getHeader("user-agent");
}

void logActivityAsync(const std::string &userId, const std::string &eventType,
                      const std::string &ipAddress, const std::string &userAgent,
                      const Json::Value &metadata) {
    auto &app = core::AppContext::instance();
    if (!app.postgres || !app.dbExec) return;
    app.dbExec([pg = app.postgres, userId, eventType, ipAddress, userAgent, metadata]() {
        pg->logActivity(userId, eventType, ipAddress, userAgent, metadata);
    });
}

}  // namespace ctraderplus::controllers
