#include "controllers/AdminRoutes.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cctype>
#include <map>
#include <mutex>
#include <optional>
#include <random>

#include <drogon/HttpAppFramework.h>
#include <drogon/HttpResponse.h>
#include <json/json.h>
#include <trantor/utils/Logger.h>

#include "core/ApiLog.h"
#include "core/AppContext.h"
#include "core/Auth.h"
#include "core/Config.h"
#include "core/DbReady.h"
#include "ctrader/CTraderClient.h"
#include "market/MarketHub.h"
#include "services/Notifier.h"
#include "services/PostgresService.h"
#include "services/RedisService.h"
#include "util/TimeUtil.h"

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

std::string normalizePhone(const std::string &phone) {
    std::string out;
    for (char c : phone) {
        if (std::isdigit(static_cast<unsigned char>(c))) out.push_back(c);
        else if (c == '+') out.push_back(c);
    }
    return out;
}

std::mutex g_otpMu;
std::map<std::string, std::pair<std::string, std::time_t>> g_otpMemory;

void storeOtp(const std::string &phone, const std::string &code, int ttlSec) {
    std::lock_guard<std::mutex> lock(g_otpMu);
    g_otpMemory[phone] = {code, std::time(nullptr) + ttlSec};
}

std::optional<std::string> consumeOtp(const std::string &phone, const std::string &code) {
    std::lock_guard<std::mutex> lock(g_otpMu);
    auto it = g_otpMemory.find(phone);
    if (it == g_otpMemory.end()) return std::nullopt;
    if (std::time(nullptr) > it->second.second) {
        g_otpMemory.erase(it);
        return std::nullopt;
    }
    if (it->second.first != code) return std::nullopt;
    g_otpMemory.erase(it);
    return code;
}

std::string generateOtpCode() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(100000, 999999);
    return std::to_string(dist(gen));
}

bool adminOrReject(const HttpRequestPtr &req,
                   std::function<void(const HttpResponsePtr &)> &cb) {
    auto auth = core::requireAdmin(req->getHeader("authorization"));
    if (!auth.ok) {
        core::logApiOutcome("admin", "bearer", false, auth.statusCode, auth.detail);
        cb(errResp(auth.detail, auth.statusCode));
        return false;
    }
    return true;
}

bool adminDbReadyOrReject(std::function<void(const HttpResponsePtr &)> &cb) {
    if (core::isDbReadyForAuth()) return true;
    core::logApiOutcome("admin", "db_ready", false, 503, "database_not_ready");
    cb(errResp("Database not ready", 503));
    return false;
}

Json::Value buildHealthPayload() {
    auto &app = AppContext::instance();
    Json::Value checks(Json::objectValue);
    std::string overall = "ok";

    double uptime = std::chrono::duration<double>(
                        std::chrono::steady_clock::now() - app.startTime)
                        .count();
    checks["uptime_seconds"] = std::round(uptime * 10) / 10.0;

    bool obsReady = app.ctrader && app.ctrader->isReady();
    checks["observer"] = obsReady ? "up" : "down";
    if (!obsReady) overall = "down";

    checks["stream_task"] = "up";
    checks["alert_task"] = "up";

    bool redisUp = app.redis && app.redis->connected();
    checks["redis"] = redisUp ? "up" : "unavailable";
    if (!redisUp && overall == "ok") overall = "degraded";

    bool pgUp = app.postgres && app.postgres->available();
    checks["postgres"] = pgUp ? "up" : "unavailable";
    if (!pgUp && overall == "ok") overall = "degraded";

    int failures = app.hub ? app.hub->snapshotFailureCount() : 0;
    checks["stream_failures"] = failures;
    checks["last_snapshot_ts"] = app.hub ? app.hub->lastSnapshotTs() : "";
    if (app.config && failures >= app.config->maxSnapshotFailures) overall = "down";

    Json::Value out;
    out["status"] = overall;
    out["timestamp"] = util::nowIso8601();
    out["checks"] = checks;
    return out;
}

void adminOtpRequest(const HttpRequestPtr &req,
                     std::function<void(const HttpResponsePtr &)> &&cb) {
    const auto &cfg = core::getConfig();
    auto body = req->getJsonObject();
    if (!body) {
        cb(errResp("Invalid request body", 400));
        return;
    }
    std::string phone = normalizePhone(trim(body->get("phone", "").asString()));
    std::string adminPhone = normalizePhone(cfg.adminPhone);
    if (adminPhone.empty()) {
        core::logApiOutcome("admin", "otp_request", false, 503, "admin_phone_not_configured");
        cb(errResp("Admin phone is not configured", 503));
        return;
    }
    if (phone != adminPhone) {
        core::logApiOutcome("admin", "otp_request", false, 403, "unauthorized_phone");
        cb(errResp("Unauthorized phone number", 403));
        return;
    }

    std::string code = generateOtpCode();
    std::string redisKey = "admin:otp:" + phone;
    int ttl = std::max(60, cfg.otpTtlSeconds);

    auto &app = AppContext::instance();
    auto finish = [cb](bool smsOk) {
        if (!smsOk) {
            core::logApiOutcome("admin", "otp_request", false, 500, "sms_failed");
            cb(errResp("Could not send OTP", 500));
            return;
        }
        core::logApiOutcome("admin", "otp_request", true, 200, "otp_sent");
        Json::Value resp;
        resp["success"] = true;
        cb(jsonResp(resp));
    };

    auto sendSms = [&]() {
        if (!app.notifier) {
            LOG_INFO << "Admin OTP (no notifier): " << code;
            finish(true);
            return;
        }
        std::string msg = "FX Alert admin code: " + code;
        app.notifier->sendSms(cfg.adminPhone, msg, [finish](bool smsOk) {
            if (!smsOk) LOG_WARN << "Admin OTP SMS failed; code logged server-side";
            finish(true);
        });
    };

    storeOtp(phone, code, ttl);
    if (app.redis && app.redis->connected()) {
        app.redis->setStringEx(redisKey, code, ttl, [sendSms](bool) { sendSms(); });
    } else {
        LOG_INFO << "Admin OTP (redis unavailable): " << code;
        sendSms();
    }
}

void adminOtpVerify(const HttpRequestPtr &req,
                    std::function<void(const HttpResponsePtr &)> &&cb) {
    const auto &cfg = core::getConfig();
    auto body = req->getJsonObject();
    if (!body) {
        cb(errResp("Invalid request body", 400));
        return;
    }
    std::string phone = normalizePhone(trim(body->get("phone", "").asString()));
    std::string code = trim(body->get("code", "").asString());
    std::string adminPhone = normalizePhone(cfg.adminPhone);
    if (adminPhone.empty() || phone != adminPhone) {
        core::logApiOutcome("admin", "otp_verify", false, 403, "unauthorized_phone");
        cb(errResp("Unauthorized", 403));
        return;
    }
    if (code.size() != 6) {
        core::logApiOutcome("admin", "otp_verify", false, 400, "invalid_code_format");
        cb(errResp("Invalid OTP", 400));
        return;
    }

    auto &app = AppContext::instance();
    std::string redisKey = "admin:otp:" + phone;

    auto issueToken = [cb]() {
        try {
            std::map<std::string, std::string> claims{{"role", "admin"}};
            std::string token = core::signToken("admin", 8 * 3600, claims);
            core::logApiOutcome("admin", "otp_verify", true, 200, "token_issued");
            Json::Value v;
            v["access_token"] = token;
            cb(jsonResp(v));
        } catch (const std::exception &e) {
            core::logApiOutcome("admin", "otp_verify", false, 500, e.what());
            cb(errResp(e.what(), 500));
        }
    };

    if (consumeOtp(phone, code)) {
        issueToken();
        return;
    }

    if (!app.redis || !app.redis->connected()) {
        core::logApiOutcome("admin", "otp_verify", false, 401, "invalid_otp");
        cb(errResp("Invalid OTP", 401));
        return;
    }

    app.redis->getString(redisKey, [phone, code, issueToken, cb](std::optional<std::string> stored) {
        if (!stored || *stored != code) {
            core::logApiOutcome("admin", "otp_verify", false, 401, "invalid_otp");
            cb(errResp("Invalid OTP", 401));
            return;
        }
        consumeOtp(phone, code);
        issueToken();
    });
}

void adminMetricsExtended(const HttpRequestPtr &req,
                          std::function<void(const HttpResponsePtr &)> &&cb) {
    if (!adminOrReject(req, cb)) return;
    if (!adminDbReadyOrReject(cb)) return;
    auto &app = AppContext::instance();
    auto callback = std::make_shared<std::function<void(const HttpResponsePtr &)>>(std::move(cb));
    app.dbExec([&app, callback]() {
        try {
            (*callback)(jsonResp(app.postgres->adminOverview()));
            core::logApiOutcome("admin", "metrics_extended", true, 200, "ok");
        } catch (const std::exception &e) {
            core::logApiOutcome("admin", "metrics_extended", false, 500, e.what());
            (*callback)(errResp("Failed to load metrics", 500));
        }
    });
}

void adminMetricsUsers(const HttpRequestPtr &req,
                       std::function<void(const HttpResponsePtr &)> &&cb) {
    if (!adminOrReject(req, cb)) return;
    if (!adminDbReadyOrReject(cb)) return;
    auto &app = AppContext::instance();
    auto callback = std::make_shared<std::function<void(const HttpResponsePtr &)>>(std::move(cb));
    app.dbExec([&app, callback]() {
        try {
            Json::Value v;
            v["items"] = app.postgres->adminListUsers();
            core::logApiOutcome("admin", "metrics_users", true, 200,
                                "count=" + std::to_string(v["items"].size()));
            (*callback)(jsonResp(v));
        } catch (const std::exception &e) {
            core::logApiOutcome("admin", "metrics_users", false, 500, e.what());
            (*callback)(errResp("Failed to load users", 500));
        }
    });
}

void adminAlerts(const HttpRequestPtr &req,
                 std::function<void(const HttpResponsePtr &)> &&cb) {
    if (!adminOrReject(req, cb)) return;
    if (!adminDbReadyOrReject(cb)) return;
    std::string status = req->getParameter("status");
    int limit = 100;
    if (!req->getParameter("limit").empty()) {
        try {
            limit = std::stoi(req->getParameter("limit"));
        } catch (...) {
        }
    }
    auto &app = AppContext::instance();
    auto callback = std::make_shared<std::function<void(const HttpResponsePtr &)>>(std::move(cb));
    app.dbExec([&app, status, limit, callback]() {
        try {
            Json::Value v;
            v["items"] = app.postgres->adminListAlerts(status, limit);
            core::logApiOutcome("admin", "alerts", true, 200,
                                "count=" + std::to_string(v["items"].size()));
            (*callback)(jsonResp(v));
        } catch (const std::exception &e) {
            core::logApiOutcome("admin", "alerts", false, 500, e.what());
            (*callback)(errResp("Failed to load alerts", 500));
        }
    });
}

void adminActivity(const HttpRequestPtr &req,
                   std::function<void(const HttpResponsePtr &)> &&cb) {
    if (!adminOrReject(req, cb)) return;
    if (!adminDbReadyOrReject(cb)) return;
    std::string eventType = req->getParameter("event_type");
    int limit = 100;
    if (!req->getParameter("limit").empty()) {
        try {
            limit = std::stoi(req->getParameter("limit"));
        } catch (...) {
        }
    }
    auto &app = AppContext::instance();
    auto callback = std::make_shared<std::function<void(const HttpResponsePtr &)>>(std::move(cb));
    app.dbExec([&app, eventType, limit, callback]() {
        try {
            Json::Value v;
            v["items"] = app.postgres->adminListActivity(eventType, limit);
            core::logApiOutcome("admin", "activity", true, 200,
                                "count=" + std::to_string(v["items"].size()));
            (*callback)(jsonResp(v));
        } catch (const std::exception &e) {
            core::logApiOutcome("admin", "activity", false, 500, e.what());
            (*callback)(errResp("Failed to load activity", 500));
        }
    });
}

void adminSystemHealth(const HttpRequestPtr &req,
                       std::function<void(const HttpResponsePtr &)> &&cb) {
    if (!adminOrReject(req, cb)) return;
    Json::Value out = buildHealthPayload();
    std::string status = out.get("status", "ok").asString();
    int code = status == "ok" ? 200 : 503;
    core::logApiOutcome("admin", "system_health", status == "ok", code, status);
    cb(jsonResp(out, code));
}

}  // namespace

void registerAdminRoutes() {
    auto &fw = drogon::app();
    fw.registerHandler("/api/v1/admin/otp/request", &adminOtpRequest, {Post});
    fw.registerHandler("/api/v1/admin/otp/verify", &adminOtpVerify, {Post});
    fw.registerHandler("/api/v1/admin/metrics/extended", &adminMetricsExtended, {Get});
    fw.registerHandler("/api/v1/admin/metrics/users", &adminMetricsUsers, {Get});
    fw.registerHandler("/api/v1/admin/alerts", &adminAlerts, {Get});
    fw.registerHandler("/api/v1/admin/activity", &adminActivity, {Get});
    fw.registerHandler("/api/v1/admin/system/health", &adminSystemHealth, {Get});
}

}  // namespace ctraderplus::controllers
