#include "controllers/FavoritesRoutes.h"

#include <drogon/HttpAppFramework.h>
#include <drogon/HttpResponse.h>
#include <json/json.h>

#include "controllers/ActivityLog.h"
#include "controllers/HttpUtil.h"
#include "core/ApiLog.h"
#include "core/AppContext.h"
#include "core/Auth.h"
#include "core/DbReady.h"
#include "services/PostgresService.h"
#include "util/PairNormalizer.h"

using namespace drogon;
using ctraderplus::core::AppContext;

namespace ctraderplus::controllers {

namespace {

using ::ctraderplus::controllers::errResp;
using ::ctraderplus::controllers::jsonResp;

bool authOrReject(const HttpRequestPtr &req,
                  std::function<void(const HttpResponsePtr &)> &cb,
                  std::string &userIdOut) {
    auto auth = core::getCurrentUserId(req->getHeader("authorization"));
    if (!auth.ok) {
        cb(errResp(auth.detail, auth.statusCode));
        return false;
    }
    userIdOut = auth.userId;
    return true;
}

bool dbReadyOrReject(std::function<void(const HttpResponsePtr &)> &cb) {
    if (core::isDbReadyForAuth()) return true;
    core::logApiOutcome("favorites", "db_ready", false, 503, "database_not_ready");
    cb(errResp("Database not ready", 503));
    return false;
}

void listFavorites(const HttpRequestPtr &req,
                   std::function<void(const HttpResponsePtr &)> &&cb) {
    std::string uid;
    if (!authOrReject(req, cb, uid)) return;
    if (!dbReadyOrReject(cb)) return;

    auto cbPtr = std::make_shared<std::function<void(const HttpResponsePtr &)>>(std::move(cb));
    core::runOnDbWorker([cbPtr, uid]() {
        if (!core::withPostgres([&](services::PostgresService &pg) {
                try {
                    auto pairs = pg.listFavorites(uid);
                    Json::Value arr(Json::arrayValue);
                    for (const auto &p : pairs) arr.append(p);
                    Json::Value v;
                    v["pairs"] = arr;
                    core::logApiOutcome("favorites", "list", true, 200,
                                        "count=" + std::to_string(pairs.size()), uid);
                    (*cbPtr)(jsonResp(v));
                } catch (const std::exception &e) {
                    core::logApiOutcome("favorites", "list", false, 500, e.what(), uid);
                    (*cbPtr)(errResp("Failed to list favorites", 500));
                }
            })) {
            (*cbPtr)(errResp("Database not ready", 503));
        }
    });
}

void addFavorite(const HttpRequestPtr &req,
                 std::function<void(const HttpResponsePtr &)> &&cb) {
    std::string uid;
    if (!authOrReject(req, cb, uid)) return;
    if (!dbReadyOrReject(cb)) return;

    auto body = req->getJsonObject();
    if (!body) {
        core::logApiOutcome("favorites", "add", false, 400, "invalid_body", uid);
        cb(errResp("Invalid JSON body", 400));
        return;
    }
    std::string pair = util::canonicalPair(body->get("pair", "").asString());
    if (pair.empty()) {
        core::logApiOutcome("favorites", "add", false, 400, "pair_required", uid);
        cb(errResp("pair is required", 400));
        return;
    }

    const std::string ip = clientIp(req);
    const std::string ua = clientUserAgent(req);
    auto cbPtr = std::make_shared<std::function<void(const HttpResponsePtr &)>>(std::move(cb));
    core::runOnDbWorker([cbPtr, uid, pair, ip, ua]() {
        if (!core::withPostgres([&](services::PostgresService &pg) {
                try {
                    pg.addFavorite(uid, pair);
                    Json::Value meta;
                    meta["pair"] = pair;
                    logActivityAsync(uid, "favorite_add", ip, ua, meta);
                    core::logApiOutcome("favorites", "add", true, 200, "pair=" + pair, uid);
                    Json::Value v;
                    v["pairs"] = Json::Value(Json::arrayValue);
                    for (const auto &p : pg.listFavorites(uid)) v["pairs"].append(p);
                    if (AppContext::instance().refreshSubscriptions)
                        AppContext::instance().refreshSubscriptions();
                    (*cbPtr)(jsonResp(v));
                } catch (const std::exception &e) {
                    core::logApiOutcome("favorites", "add", false, 500, e.what(), uid);
                    (*cbPtr)(errResp(e.what(), 500));
                }
            })) {
            (*cbPtr)(errResp("Database not ready", 503));
        }
    });
}

void removeFavorite(const HttpRequestPtr &req,
                    std::function<void(const HttpResponsePtr &)> &&cb,
                    std::string pairParam) {
    std::string uid;
    if (!authOrReject(req, cb, uid)) return;
    if (!dbReadyOrReject(cb)) return;

    std::string pair = util::canonicalPair(pairParam);
    if (pair.empty()) {
        core::logApiOutcome("favorites", "remove", false, 400, "pair_required", uid);
        cb(errResp("pair is required", 400));
        return;
    }

    const std::string ip = clientIp(req);
    const std::string ua = clientUserAgent(req);
    auto cbPtr = std::make_shared<std::function<void(const HttpResponsePtr &)>>(std::move(cb));
    core::runOnDbWorker([cbPtr, uid, pair, ip, ua]() {
        if (!core::withPostgres([&](services::PostgresService &pg) {
                try {
                    pg.removeFavorite(uid, pair);
                    Json::Value meta;
                    meta["pair"] = pair;
                    logActivityAsync(uid, "favorite_remove", ip, ua, meta);
                    core::logApiOutcome("favorites", "remove", true, 200, "pair=" + pair, uid);
                    Json::Value v;
                    v["pairs"] = Json::Value(Json::arrayValue);
                    for (const auto &p : pg.listFavorites(uid)) v["pairs"].append(p);
                    if (AppContext::instance().refreshSubscriptions)
                        AppContext::instance().refreshSubscriptions();
                    (*cbPtr)(jsonResp(v));
                } catch (const std::exception &e) {
                    core::logApiOutcome("favorites", "remove", false, 500, e.what(), uid);
                    (*cbPtr)(errResp(e.what(), 500));
                }
            })) {
            (*cbPtr)(errResp("Database not ready", 503));
        }
    });
}

}  // namespace

void registerFavoritesRoutes() {
    auto &fw = drogon::app();
    fw.registerHandler("/api/v1/me/favorites", &listFavorites, {Get});
    fw.registerHandler("/api/v1/me/favorites", &addFavorite, {Post});
    fw.registerHandler("/api/v1/me/favorites/{1}", &removeFavorite, {Delete});
}

}  // namespace ctraderplus::controllers
