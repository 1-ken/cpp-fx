#include "controllers/AuthRoutes.h"

#include <algorithm>
#include <cctype>

#include <drogon/HttpAppFramework.h>
#include <drogon/HttpResponse.h>
#include <json/json.h>
#include <trantor/utils/Logger.h>

#include "controllers/ActivityLog.h"
#include "core/ApiLog.h"
#include "core/AppContext.h"
#include "core/Auth.h"
#include "core/DbReady.h"
#include "core/Password.h"
#include "services/PostgresService.h"
#include "util/Uuid.h"

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

std::string trim(const std::string &s) {
    size_t a = s.find_first_not_of(" \t\r\n");
    if (a == std::string::npos) return "";
    size_t b = s.find_last_not_of(" \t\r\n");
    return s.substr(a, b - a + 1);
}

bool authOrReject(const HttpRequestPtr &req,
                  std::function<void(const HttpResponsePtr &)> &cb,
                  std::string &userIdOut) {
    auto auth = core::getCurrentUserId(req->getHeader("authorization"));
    if (!auth.ok) {
        core::logApiOutcome("auth", "bearer", false, auth.statusCode, auth.detail);
        cb(errResp(auth.detail, auth.statusCode));
        return false;
    }
    userIdOut = auth.userId;
    return true;
}

bool dbReadyOrReject(std::function<void(const HttpResponsePtr &)> &cb) {
    if (core::isDbReadyForAuth()) return true;
    core::logApiOutcome("auth", "db_ready", false, 503, "database_not_ready");
    cb(errResp("Database not ready", 503));
    return false;
}

void authRegister(const HttpRequestPtr &req,
                  std::function<void(const HttpResponsePtr &)> &&cb) {
    if (!dbReadyOrReject(cb)) return;

    auto body = req->getJsonObject();
    if (!body) {
        core::logApiOutcome("auth", "register", false, 400, "invalid_body");
        cb(errResp("Invalid request body", 400));
        return;
    }
    std::string username =
        core::normalizeUsername(trim(body->get("username", "").asString()));
    std::string password = body->get("password", "").asString();
    if (username.empty() || password.empty()) {
        core::logApiOutcome("auth", "register", false, 400, "missing_fields");
        cb(errResp("Username and password are required", 400));
        return;
    }
    if (username.size() < 3) {
        core::logApiOutcome("auth", "register", false, 400, "username_too_short", username);
        cb(errResp("Username must be at least 3 characters", 400));
        return;
    }
    if (password.size() < 6) {
        core::logApiOutcome("auth", "register", false, 400, "password_too_short", username);
        cb(errResp("Password must be at least 6 characters", 400));
        return;
    }

    if (!core::withPostgres([&](services::PostgresService &pg) {
            try {
                if (pg.findUserByUsername(username)) {
                    core::logApiOutcome("auth", "register", false, 409, "username_taken",
                                        username);
                    cb(errResp("Username already taken", 409));
                    return;
                }
                std::string userId = util::generateUuid();
                if (userId.empty()) userId = username;
                std::string hash = core::hashPassword(password);
                pg.createUser(userId, username, hash);
                logActivityAsync(userId, "register", clientIp(req), clientUserAgent(req));
                core::logApiOutcome("auth", "register", true, 200, "username=" + username,
                                    userId);
                Json::Value v;
                v["user_id"] = userId;
                v["username"] = username;
                cb(jsonResp(v));
            } catch (const std::exception &e) {
                LOG_ERROR << "[auth] register ERROR: " << e.what();
                core::logApiOutcome("auth", "register", false, 500, e.what(), username);
                cb(errResp("Registration failed", 500));
            }
        })) {
        cb(errResp("Database not ready", 503));
    }
}

void authLogin(const HttpRequestPtr &req,
               std::function<void(const HttpResponsePtr &)> &&cb) {
    if (!dbReadyOrReject(cb)) return;

    auto body = req->getJsonObject();
    if (!body) {
        core::logApiOutcome("auth", "login", false, 400, "invalid_body");
        cb(errResp("Invalid request body", 400));
        return;
    }
    std::string username =
        core::normalizeUsername(trim(body->get("username", "").asString()));
    std::string password = body->get("password", "").asString();
    if (username.empty() || password.empty()) {
        core::logApiOutcome("auth", "login", false, 400, "missing_fields");
        cb(errResp("Username and password are required", 400));
        return;
    }

    if (!core::withPostgres([&](services::PostgresService &pg) {
            try {
                auto user = pg.findUserByUsername(username);
                if (!user || !core::verifyPassword(password, user->passwordHash)) {
                    Json::Value failMeta;
                    failMeta["username"] = username;
                    logActivityAsync("", "login_failed", clientIp(req), clientUserAgent(req),
                                     failMeta);
                    core::logApiOutcome("auth", "login", false, 401, "invalid_credentials",
                                        username);
                    cb(errResp("Invalid username or password", 401));
                    return;
                }
                logActivityAsync(user->userId, "login_success", clientIp(req),
                                 clientUserAgent(req));
                core::logApiOutcome("auth", "login", true, 200, "username=" + username,
                                    user->userId);
                Json::Value v;
                v["user_id"] = user->userId;
                v["username"] = user->username;
                cb(jsonResp(v));
            } catch (const std::exception &e) {
                LOG_ERROR << "[auth] login ERROR: " << e.what();
                core::logApiOutcome("auth", "login", false, 500, e.what(), username);
                cb(errResp("Login failed", 500));
            }
        })) {
        cb(errResp("Database not ready", 503));
    }
}

void authGoogleSync(const HttpRequestPtr &req,
                    std::function<void(const HttpResponsePtr &)> &&cb) {
    std::string uid;
    if (!authOrReject(req, cb, uid)) return;
    if (!dbReadyOrReject(cb)) return;

    auto body = req->getJsonObject();
    if (!body) {
        core::logApiOutcome("auth", "google_sync", false, 400, "invalid_body", uid);
        cb(errResp("Invalid JSON body", 400));
        return;
    }
    std::string googleSub = body->get("google_sub", "").asString();
    std::string email = body->get("email", "").asString();
    std::string displayName = body->get("display_name", "").asString();
    std::string avatarUrl = body->get("avatar_url", "").asString();

    if (!core::withPostgres([&](services::PostgresService &pg) {
            try {
                pg.upsertGoogleUser(uid, googleSub, email, displayName, avatarUrl);
                logActivityAsync(uid, "google_oauth", clientIp(req), clientUserAgent(req));
                core::logApiOutcome("auth", "google_sync", true, 200,
                                    "google_sub=" + googleSub, uid);
                Json::Value v;
                v["success"] = true;
                cb(jsonResp(v));
            } catch (const std::exception &e) {
                LOG_ERROR << "[auth] google_sync ERROR: " << e.what();
                core::logApiOutcome("auth", "google_sync", false, 500, e.what(), uid);
                cb(errResp("Google sync failed", 500));
            }
        })) {
        cb(errResp("Database not ready", 503));
    }
}

}  // namespace

void registerAuthRoutes() {
    auto &fw = drogon::app();
    fw.registerHandler("/api/v1/auth/register", &authRegister, {Post});
    fw.registerHandler("/api/v1/auth/login", &authLogin, {Post});
    fw.registerHandler("/api/v1/auth/oauth/google-sync", &authGoogleSync, {Post});
}

}  // namespace ctraderplus::controllers
