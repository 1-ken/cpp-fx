#include "controllers/AuthRoutes.h"

#include <algorithm>
#include <cctype>
#include <optional>

#include <drogon/HttpAppFramework.h>
#include <drogon/HttpResponse.h>
#include <json/json.h>
#include <trantor/utils/Logger.h>

#include "controllers/ActivityLog.h"
#include "controllers/HttpUtil.h"
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

using ::ctraderplus::controllers::errResp;
using ::ctraderplus::controllers::isValidMarketerCodeFormat;
using ::ctraderplus::controllers::jsonResp;
using ::ctraderplus::controllers::normalizeMarketerCode;
using ::ctraderplus::controllers::trim;

std::optional<std::string> resolveMarketerCode(const std::string &raw,
                                               services::PostgresService &pg) {
    std::string code = normalizeMarketerCode(trim(raw));
    if (code.empty()) return std::nullopt;
    if (!isValidMarketerCodeFormat(code)) return std::nullopt;
    if (!pg.isActiveMarketer(code)) return std::nullopt;
    return code;
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

    std::string marketerRaw;
    if (body->isMember("marketer_code"))
        marketerRaw = body->get("marketer_code", "").asString();
    const std::string ip = clientIp(req);
    const std::string ua = clientUserAgent(req);
    auto cbPtr = std::make_shared<std::function<void(const HttpResponsePtr &)>>(std::move(cb));
    core::runOnDbWorker([cbPtr, username, password, marketerRaw, ip, ua]() {
        if (!core::withPostgres([&](services::PostgresService &pg) {
                try {
                    if (pg.findUserByUsername(username)) {
                        core::logApiOutcome("auth", "register", false, 409, "username_taken",
                                            username);
                        (*cbPtr)(errResp("Username already taken", 409));
                        return;
                    }
                    std::string userId = util::generateUuid();
                    if (userId.empty()) userId = username;
                    std::string hash = core::hashPassword(password);
                    std::string marketerCode;
                    auto resolved = resolveMarketerCode(marketerRaw, pg);
                    if (resolved) marketerCode = *resolved;
                    pg.createUser(userId, username, hash, marketerCode);
                    logActivityAsync(userId, "register", ip, ua);
                    core::logApiOutcome("auth", "register", true, 200, "username=" + username,
                                        userId);
                    Json::Value v;
                    v["user_id"] = userId;
                    v["username"] = username;
                    (*cbPtr)(jsonResp(v));
                } catch (const std::exception &e) {
                    LOG_ERROR << "[auth] register ERROR: " << e.what();
                    core::logApiOutcome("auth", "register", false, 500, e.what(), username);
                    (*cbPtr)(errResp("Registration failed", 500));
                }
            })) {
            (*cbPtr)(errResp("Database not ready", 503));
        }
    });
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

    const std::string ip = clientIp(req);
    const std::string ua = clientUserAgent(req);
    auto cbPtr = std::make_shared<std::function<void(const HttpResponsePtr &)>>(std::move(cb));
    core::runOnDbWorker([cbPtr, username, password, ip, ua]() {
        if (!core::withPostgres([&](services::PostgresService &pg) {
                try {
                    auto user = pg.findUserByUsername(username);
                    if (!user || !core::verifyPassword(password, user->passwordHash)) {
                        Json::Value failMeta;
                        failMeta["username"] = username;
                        logActivityAsync("", "login_failed", ip, ua, failMeta);
                        core::logApiOutcome("auth", "login", false, 401, "invalid_credentials",
                                            username);
                        (*cbPtr)(errResp("Invalid username or password", 401));
                        return;
                    }
                    logActivityAsync(user->userId, "login_success", ip, ua);
                    pg.updateLastLogin(user->userId);
                    core::logApiOutcome("auth", "login", true, 200, "username=" + username,
                                        user->userId);
                    Json::Value v;
                    v["user_id"] = user->userId;
                    v["username"] = user->username;
                    (*cbPtr)(jsonResp(v));
                } catch (const std::exception &e) {
                    LOG_ERROR << "[auth] login ERROR: " << e.what();
                    core::logApiOutcome("auth", "login", false, 500, e.what(), username);
                    (*cbPtr)(errResp("Login failed", 500));
                }
            })) {
            (*cbPtr)(errResp("Database not ready", 503));
        }
    });
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

    const std::string ip = clientIp(req);
    const std::string ua = clientUserAgent(req);
    auto cbPtr = std::make_shared<std::function<void(const HttpResponsePtr &)>>(std::move(cb));
    core::runOnDbWorker([cbPtr, uid, googleSub, email, displayName, avatarUrl, ip, ua]() {
        if (!core::withPostgres([&](services::PostgresService &pg) {
                try {
                    pg.upsertGoogleUser(uid, googleSub, email, displayName, avatarUrl);
                    pg.updateLastLogin(uid);
                    logActivityAsync(uid, "google_oauth", ip, ua);
                    core::logApiOutcome("auth", "google_sync", true, 200,
                                        "google_sub=" + googleSub, uid);
                    Json::Value v;
                    v["success"] = true;
                    (*cbPtr)(jsonResp(v));
                } catch (const std::exception &e) {
                    LOG_ERROR << "[auth] google_sync ERROR: " << e.what();
                    core::logApiOutcome("auth", "google_sync", false, 500, e.what(), uid);
                    (*cbPtr)(errResp("Google sync failed", 500));
                }
            })) {
            (*cbPtr)(errResp("Database not ready", 503));
        }
    });
}

void authClaimReferral(const HttpRequestPtr &req,
                     std::function<void(const HttpResponsePtr &)> &&cb) {
    std::string uid;
    if (!authOrReject(req, cb, uid)) return;
    if (!dbReadyOrReject(cb)) return;

    auto body = req->getJsonObject();
    if (!body) {
        core::logApiOutcome("auth", "claim_referral", false, 400, "invalid_body", uid);
        cb(errResp("Invalid request body", 400));
        return;
    }

    const std::string marketerRaw = body->get("marketer_code", "").asString();
    auto cbPtr = std::make_shared<std::function<void(const HttpResponsePtr &)>>(std::move(cb));
    core::runOnDbWorker([cbPtr, uid, marketerRaw]() {
        if (!core::withPostgres([&](services::PostgresService &pg) {
                try {
                    auto resolved = resolveMarketerCode(marketerRaw, pg);
                    Json::Value v;
                    v["success"] = true;
                    v["attributed"] = false;
                    if (resolved) {
                        v["attributed"] = pg.claimReferral(uid, *resolved);
                    }
                    core::logApiOutcome("auth", "claim_referral", true, 200,
                                        v["attributed"].asBool() ? "attributed" : "skipped", uid);
                    (*cbPtr)(jsonResp(v));
                } catch (const std::exception &e) {
                    LOG_ERROR << "[auth] claim_referral ERROR: " << e.what();
                    core::logApiOutcome("auth", "claim_referral", false, 500, e.what(), uid);
                    (*cbPtr)(errResp("Claim referral failed", 500));
                }
            })) {
            (*cbPtr)(errResp("Database not ready", 503));
        }
    });
}

}  // namespace

void registerAuthRoutes() {
    auto &fw = drogon::app();
    fw.registerHandler("/api/v1/auth/register", &authRegister, {Post});
    fw.registerHandler("/api/v1/auth/login", &authLogin, {Post});
    fw.registerHandler("/api/v1/auth/oauth/google-sync", &authGoogleSync, {Post});
    fw.registerHandler("/api/v1/auth/claim-referral", &authClaimReferral, {Post});
}

}  // namespace ctraderplus::controllers
