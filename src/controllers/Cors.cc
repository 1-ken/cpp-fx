#include "controllers/Cors.h"

#include <drogon/HttpAppFramework.h>
#include <drogon/HttpResponse.h>

#include "core/Config.h"

namespace ctraderplus::controllers {

namespace {

void applyCorsHeaders(const drogon::HttpResponsePtr &resp) {
    const auto &cfg = core::getConfig();
    if (cfg.corsAllowOrigin.empty()) return;
    resp->addHeader("Access-Control-Allow-Origin", cfg.corsAllowOrigin);
    resp->addHeader("Access-Control-Allow-Headers", "Authorization, Content-Type");
    resp->addHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
    resp->addHeader("Access-Control-Max-Age", "86400");
}

void handleOptions(const drogon::HttpRequestPtr &,
                   std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
    auto resp = drogon::HttpResponse::newHttpResponse();
    resp->setStatusCode(drogon::k204NoContent);
    applyCorsHeaders(resp);
    cb(resp);
}

}  // namespace

void registerCors() {
    auto &fw = drogon::app();

    fw.registerPostHandlingAdvice([](const drogon::HttpRequestPtr &,
                                     const drogon::HttpResponsePtr &resp) {
        applyCorsHeaders(resp);
    });

    fw.registerPreRoutingAdvice(
        [](const drogon::HttpRequestPtr &req,
           std::function<void(const drogon::HttpResponsePtr &)> &&callback,
           std::function<void()> &&pass) {
            if (req->method() != drogon::Options) {
                // Not a CORS preflight: continue normal routing. Failing to
                // invoke this chain callback hangs every non-OPTIONS request.
                pass();
                return;
            }
            handleOptions(req, std::move(callback));
        });
}

}  // namespace ctraderplus::controllers
