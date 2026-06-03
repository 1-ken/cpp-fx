#include "controllers/FavoritesRoutes.h"

#include <drogon/HttpAppFramework.h>
#include <drogon/HttpResponse.h>
#include <json/json.h>

#include "controllers/ActivityLog.h"
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

HttpResponsePtr jsonResp(const Json::Value &v, int code = 200) {
    auto resp = HttpResponse::newHttpJsonResponse(v);
    resp->setStatusCode(static_cast<HttpStatusCode>(code));
    return resp;
}

HttpResponsePtr errResp(const std::string &msg, int code) {
    Json::Value v;
    v["detail"] = msg;
    return jsonResp(v, code);
}

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

    if (!core::withPostgres([&](services::PostgresService &pg) {
            try {
                auto pairs = pg.listFavorites(uid);
                Json::Value arr(Json::arrayValue);
                for (const auto &p : pairs) arr.append(p);
                Json::Value v;
                v["pairs"] = arr;
                core::logApiOutcome("favorites", "list", true, 200,
                                    "count=" + std::to_string(pairs.size()), uid);
                cb(jsonResp(v));
            } catch (const std::exception &e) {
                core::logApiOutcome("favorites", "list", false, 500, e.what(), uid);
                cb(errResp("Failed to list favorites", 500));
            }
        })) {
        cb(errResp("Database not ready", 503));
    }
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

    if (!core::withPostgres([&](services::PostgresService &pg) {
            try {
                pg.addFavorite(uid, pair);
                Json::Value meta;
                meta["pair"] = pair;
                logActivityAsync(uid, "favorite_add", clientIp(req), clientUserAgent(req), meta);
                core::logApiOutcome("favorites", "add", true, 200, "pair=" + pair, uid);
                Json::Value v;
                v["pairs"] = Json::Value(Json::arrayValue);
                for (const auto &p : pg.listFavorites(uid)) v["pairs"].append(p);
                cb(jsonResp(v));
            } catch (const std::exception &e) {
                core::logApiOutcome("favorites", "add", false, 500, e.what(), uid);
                cb(errResp(e.what(), 500));
            }
        })) {
        cb(errResp("Database not ready", 503));
    }
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

    if (!core::withPostgres([&](services::PostgresService &pg) {
            try {
                pg.removeFavorite(uid, pair);
                Json::Value meta;
                meta["pair"] = pair;
                logActivityAsync(uid, "favorite_remove", clientIp(req), clientUserAgent(req),
                                 meta);
                core::logApiOutcome("favorites", "remove", true, 200, "pair=" + pair, uid);
                Json::Value v;
                v["pairs"] = Json::Value(Json::arrayValue);
                for (const auto &p : pg.listFavorites(uid)) v["pairs"].append(p);
                cb(jsonResp(v));
            } catch (const std::exception &e) {
                core::logApiOutcome("favorites", "remove", false, 500, e.what(), uid);
                cb(errResp(e.what(), 500));
            }
        })) {
        cb(errResp("Database not ready", 503));
    }
}

}  // namespace

void registerFavoritesRoutes() {
    auto &fw = drogon::app();
    fw.registerHandler("/api/v1/me/favorites", &listFavorites, {Get});
    fw.registerHandler("/api/v1/me/favorites", &addFavorite, {Post});
    fw.registerHandler("/api/v1/me/favorites/{1}", &removeFavorite, {Delete});
}

}  // namespace ctraderplus::controllers
