#include "controllers/Routes.h"

#include "controllers/ActivityLog.h"
#include "controllers/AdminRoutes.h"
#include "controllers/AuthRoutes.h"
#include "controllers/Cors.h"
#include "controllers/FavoritesRoutes.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <ctime>
#include <optional>
#include <set>
#include <sstream>
#include <string>

#include <drogon/HttpAppFramework.h>
#include <drogon/HttpResponse.h>
#include <json/json.h>

#include "alerts/AlertManager.h"
#include "controllers/Dashboard.h"
#include "controllers/OpenApiDoc.h"
#include "core/ApiLog.h"
#include "core/AppContext.h"
#include "core/Auth.h"
#include "core/Config.h"
#include "core/DbReady.h"
#include "ctrader/CTraderClient.h"
#include "ctrader/SymbolRegistry.h"
#include "market/MarketHub.h"
#include "market/SymbolSubscriptionPlanner.h"
#include "services/PostgresService.h"
#include "services/RedisService.h"
#include "util/ForexMarketHours.h"
#include "util/FormingCandle.h"
#include "util/PairNormalizer.h"
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

HttpResponsePtr errResp(const std::string &detailKey, const std::string &msg, int code) {
    Json::Value v;
    v[detailKey] = msg;
    return jsonResp(v, code);
}

// Resolve the authenticated user; on failure invokes cb with an error response.
bool authOrReject(const HttpRequestPtr &req,
                  std::function<void(const HttpResponsePtr &)> &cb,
                  std::string &userIdOut) {
    auto auth = core::getCurrentUserId(req->getHeader("authorization"));
    if (!auth.ok) {
        cb(errResp("detail", auth.detail, auth.statusCode));
        return false;
    }
    userIdOut = auth.userId;
    return true;
}

int clampInt(int v, int lo, int hi) { return v < lo ? lo : (v > hi ? hi : v); }

std::string trimStr(const std::string &s) {
    size_t a = s.find_first_not_of(" \t\r\n");
    if (a == std::string::npos) return "";
    size_t b = s.find_last_not_of(" \t\r\n");
    return s.substr(a, b - a + 1);
}

bool requiresCustomMessage(const std::string &channel, const std::string &customMessage) {
    return (channel == "sms" || channel == "call") && trimStr(customMessage).empty();
}

bool channelValid(const std::string &c) {
    return c == "email" || c == "sms" || c == "call" || c == "sound";
}

bool channelRequiresEmail(const std::string &channel) { return channel == "email"; }

bool channelRequiresPhone(const std::string &channel) {
    return channel == "sms" || channel == "call";
}

bool channelsRequireEmail(const std::vector<std::string> &channels) {
    return std::any_of(channels.begin(), channels.end(),
                       [](const std::string &c) { return channelRequiresEmail(c); });
}

bool channelsRequirePhone(const std::vector<std::string> &channels) {
    return std::any_of(channels.begin(), channels.end(),
                       [](const std::string &c) { return channelRequiresPhone(c); });
}

bool channelsRequireCustomMessage(const std::vector<std::string> &channels,
                                  const std::string &customMessage) {
    return std::any_of(channels.begin(), channels.end(), [&](const std::string &c) {
        return requiresCustomMessage(c, customMessage);
    });
}

std::vector<std::string> parseChannels(const Json::Value &body, std::string &err) {
    std::vector<std::string> channels;
    if (body.isMember("channels") && body["channels"].isArray()) {
        for (const auto &c : body["channels"]) {
            if (!c.isString()) {
                err = "channels must contain strings";
                return {};
            }
            std::string ch = c.asString();
            if (!channelValid(ch)) {
                err = "Channel must be 'email', 'sms', 'call', or 'sound'";
                return {};
            }
            if (std::find(channels.begin(), channels.end(), ch) == channels.end())
                channels.push_back(ch);
        }
    } else if (body.isMember("channel") && body["channel"].isString()) {
        std::string ch = body["channel"].asString();
        if (!channelValid(ch)) {
            err = "Channel must be 'email', 'sms', 'call', or 'sound'";
            return {};
        }
        channels.push_back(ch);
    } else {
        channels.push_back("email");
    }
    if (channels.empty()) {
        err = "At least one notification channel is required";
        return {};
    }
    return channels;
}

std::optional<std::time_t> parseQueryTime(const std::string &s) {
    if (s.empty()) return std::nullopt;
    return util::parseIso8601(s);
}

const std::set<std::string> kIntervals = {"1m", "5m", "15m", "30m", "1h", "4h", "1d"};

// ---- Handlers -------------------------------------------------------------

void health(const HttpRequestPtr &, std::function<void(const HttpResponsePtr &)> &&cb) {
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
    cb(jsonResp(out, overall == "ok" ? 200 : 503));
}

void ping(const HttpRequestPtr &, std::function<void(const HttpResponsePtr &)> &&cb) {
    Json::Value v;
    v["pong"] = true;
    cb(jsonResp(v));
}

void dashboard(const HttpRequestPtr &, std::function<void(const HttpResponsePtr &)> &&cb) {
    auto resp = HttpResponse::newHttpResponse();
    resp->setStatusCode(k200OK);
    resp->setContentTypeCode(CT_TEXT_HTML);
    resp->setBody(kDashboardHtml);
    cb(resp);
}

void openApiSpec(const HttpRequestPtr &, std::function<void(const HttpResponsePtr &)> &&cb) {
    auto resp = HttpResponse::newHttpResponse();
    resp->setStatusCode(k200OK);
    resp->setContentTypeString("application/yaml");
    resp->setBody(openApiYaml());
    cb(resp);
}

void apiDocs(const HttpRequestPtr &, std::function<void(const HttpResponsePtr &)> &&cb) {
    auto resp = HttpResponse::newHttpResponse();
    resp->setStatusCode(k200OK);
    resp->setContentTypeCode(CT_TEXT_HTML);
    resp->setBody(kSwaggerUiHtml);
    cb(resp);
}

void clientConfig(const HttpRequestPtr &req,
                  std::function<void(const HttpResponsePtr &)> &&cb) {
    auto &app = AppContext::instance();
    std::string uid;
    if (!authOrReject(req, cb, uid)) return;
    Json::Value v;
    v["wsUrl"] = app.config ? app.config->wsUrl : "";
    cb(jsonResp(v));
}

void snapshot(const HttpRequestPtr &req, std::function<void(const HttpResponsePtr &)> &&cb) {
    auto &app = AppContext::instance();
    std::string uid;
    if (!authOrReject(req, cb, uid)) return;
    if (!app.ctrader || !app.ctrader->isReady()) {
        cb(errResp("error", "Observer not ready", 503));
        return;
    }
    auto grouped = app.hub->buildGroupedSnapshot();
    cb(jsonResp(*grouped));
}

void streamHealth(const HttpRequestPtr &req,
                  std::function<void(const HttpResponsePtr &)> &&cb) {
    auto &app = AppContext::instance();
    std::string uid;
    if (!authOrReject(req, cb, uid)) return;
    const auto &cfg = *app.config;
    int failures = app.hub->snapshotFailureCount();
    double age = app.hub->lastSnapshotAgeSeconds();
    std::string status = "healthy";
    if (failures > 0) status = "degraded";
    if (failures >= cfg.maxSnapshotFailures) status = "stale";
    else if (age >= 0 && age > std::max(5.0, cfg.streamIntervalSeconds * 6)) status = "stale";

    Json::Value v;
    v["status"] = status;
    v["stream_interval_seconds"] = cfg.streamIntervalSeconds;
    v["snapshot_timeout_seconds"] = cfg.snapshotTimeoutSeconds;
    v["max_snapshot_failures"] = cfg.maxSnapshotFailures;
    v["consecutive_snapshot_failures"] = failures;
    v["last_snapshot_ts"] = app.hub->lastSnapshotTs();
    v["last_snapshot_age_seconds"] = age >= 0 ? Json::Value(age) : Json::Value::null;
    v["subscriber_count"] = app.hub->activeWsCount();
    v["ws_subscriber_count"] = app.hub->activeWsCount();
    v["queue_subscriber_count"] = 0;
    v["retention_days"] = cfg.retentionDays;
    v["ctrader_connected"] = app.ctrader && app.ctrader->isReady();
    if (app.ctrader) {
        v["ctrader_reconnect_count"] = app.ctrader->reconnectCount();
        v["ctrader_rate_limit_count"] = app.ctrader->rateLimitCount();
        v["ctrader_last_error"] = app.ctrader->lastErrorCode();
        v["ctrader_circuit_breaker_open"] = app.ctrader->circuitBreakerOpen();
        v["ctrader_token_degraded"] = app.ctrader->isTokenDegraded();
    }
    if (app.postgres) {
        v["db_alert_upsert_failures_total"] =
            static_cast<Json::Int64>(app.postgres->alertUpsertFailures());
    }
    if (app.subscriptionPlanner && app.registry) {
        auto ids = app.subscriptionPlanner->computeSymbolIds();
        v["subscribed_symbol_count"] = static_cast<int>(ids.size());
    }
    cb(jsonResp(v));
}

void me(const HttpRequestPtr &req, std::function<void(const HttpResponsePtr &)> &&cb) {
    auto &app = AppContext::instance();
    std::string uid;
    if (!authOrReject(req, cb, uid)) return;
    if (!core::isDbReadyForAuth()) {
        core::logApiOutcome("user", "me", false, 503, "database_not_ready", uid);
        cb(errResp("detail", "Database not ready", 503));
        return;
    }

    auto buildBootstrap = [&app, uid](bool firstTime,
                                      std::optional<std::time_t> completedAt) {
        Json::Value v;
        v["userId"] = uid;
        v["isFirstTimeUser"] = firstTime;
        v["onboardingCompletedAt"] =
            completedAt ? Json::Value(util::toIso8601(*completedAt)) : Json::Value::null;
        v["authRequired"] = !app.config->authDisabled;
        v["wsUrl"] = app.config->wsUrl;
        v["apiBaseUrl"] =
            app.config->apiBaseUrl.empty() ? Json::Value::null : Json::Value(app.config->apiBaseUrl);
        return v;
    };

    if (!app.postgres || !app.postgres->available()) {
        cb(jsonResp(buildBootstrap(true, std::nullopt)));
        return;
    }
    if (!core::withPostgres([&](services::PostgresService &pg) {
            try {
                auto st = pg.getOrCreateUserState(uid);
                bool firstTime = !st.onboardingCompletedAt.has_value();
                core::logApiOutcome("user", "me", true, 200,
                                    firstTime ? "first_time" : "returning", uid);
                cb(jsonResp(buildBootstrap(firstTime, st.onboardingCompletedAt)));
            } catch (const std::exception &e) {
                core::logApiOutcome("user", "me", false, 500, e.what(), uid);
                cb(jsonResp(buildBootstrap(true, std::nullopt)));
            }
        })) {
        cb(errResp("detail", "Database not ready", 503));
    }
}

void onboardingComplete(const HttpRequestPtr &req,
                        std::function<void(const HttpResponsePtr &)> &&cb) {
    auto &app = AppContext::instance();
    std::string uid;
    if (!authOrReject(req, cb, uid)) return;
    if (!core::isDbReadyForAuth()) {
        core::logApiOutcome("user", "onboarding_complete", false, 503, "database_not_ready",
                            uid);
        cb(errResp("detail", "Database not ready", 503));
        return;
    }
    if (!core::withPostgres([&](services::PostgresService &pg) {
            try {
                auto st = pg.completeUserOnboarding(uid);
                if (!st.onboardingCompletedAt) {
                    cb(errResp("detail", "Database unavailable", 503));
                    return;
                }
                Json::Value v;
                v["success"] = true;
                v["userId"] = st.userId;
                v["onboardingCompletedAt"] = util::toIso8601(*st.onboardingCompletedAt);
                v["isFirstTimeUser"] = false;
                core::logApiOutcome("user", "onboarding_complete", true, 200, "ok", uid);
                cb(jsonResp(v));
            } catch (const std::exception &e) {
                core::logApiOutcome("user", "onboarding_complete", false, 503, e.what(), uid);
                cb(errResp("detail", "Database unavailable", 503));
            }
        })) {
        cb(errResp("detail", "Database not ready", 503));
    }
}

void historical(const HttpRequestPtr &req, std::function<void(const HttpResponsePtr &)> &&cb) {
    std::string uid;
    if (!authOrReject(req, cb, uid)) return;
    (void)uid;
    Json::Value v;
    v["error"] = "Tick history retired; use GET /historical/ohlc or /historical/ohlc-with-forming";
    v["successor"] = "/historical/ohlc";
    cb(jsonResp(v, 410));
}

void streamMetrics(const HttpRequestPtr &req,
                   std::function<void(const HttpResponsePtr &)> &&cb) {
    auto &app = AppContext::instance();
    std::string uid;
    if (!authOrReject(req, cb, uid)) return;
    if (!app.postgres || !app.postgres->available()) {
        cb(errResp("error", "Historical storage not available", 503));
        return;
    }
    int limit = clampInt(req->getParameter("limit").empty()
                             ? 1000 : std::atoi(req->getParameter("limit").c_str()), 1, 5000);
    bool descending = req->getParameter("order") != "asc";
    std::time_t retentionFloor =
        std::time(nullptr) - static_cast<std::time_t>(app.config->retentionDays) * 86400;
    auto startOpt = parseQueryTime(req->getParameter("start"));
    auto endOpt = parseQueryTime(req->getParameter("end"));
    std::time_t start = (!startOpt || *startOpt < retentionFloor) ? retentionFloor : *startOpt;

    auto callback = std::make_shared<std::function<void(const HttpResponsePtr &)>>(std::move(cb));
    app.dbExec([&app, start, endOpt, limit, descending, callback]() {
        auto rows = app.postgres->queryStreamMetrics(start, endOpt, limit, descending);
        Json::Value items(Json::arrayValue);
        for (const auto &m : rows) {
            Json::Value it;
            it["observed_at"] = util::toIso8601(m.observedAt);
            it["ws_subscriber_count"] = m.wsSubscriberCount;
            it["queue_subscriber_count"] = m.queueSubscriberCount;
            it["snapshot_failure_count"] = m.snapshotFailureCount;
            it["stream_status"] = m.streamStatus;
            items.append(it);
        }
        Json::Value v;
        v["count"] = (int)items.size();
        v["items"] = items;
        (*callback)(jsonResp(v));
    });
}

Json::Value closedCandleJson(const ctrader::TrendbarData &b, int ivSec, bool isForming) {
    std::time_t ts = b.utcTimestampMinutes * 60;
    Json::Value c;
    c["timestamp"] = util::toIso8601(ts);
    c["open"] = b.open;
    c["high"] = b.high;
    c["low"] = b.low;
    c["close"] = b.close;
    c["volume"] = static_cast<Json::Int64>(b.volume);
    c["is_forming"] = isForming;
    c["expected_open"] = util::toIso8601(ts);
    c["expected_close"] = util::toIso8601(ts + ivSec);
    return c;
}

void historicalOhlc(const HttpRequestPtr &req,
                    std::function<void(const HttpResponsePtr &)> &&cb, bool withForming) {
    auto &app = AppContext::instance();
    std::string uid;
    if (!authOrReject(req, cb, uid)) return;

    std::string pair = req->getParameter("pair");
    std::string interval = req->getParameter("interval");
    if (interval.empty()) interval = "5m";
    std::transform(interval.begin(), interval.end(), interval.begin(), ::tolower);
    if (kIntervals.find(interval) == kIntervals.end()) {
        cb(errResp("error", "Invalid interval. Must be one of: 1m, 5m, 15m, 30m, 1h, 4h, 1d", 400));
        return;
    }
    if (!app.ctrader || !app.ctrader->isReady()) {
        cb(errResp("error", "Market data source not ready", 503));
        return;
    }
    std::string canon = util::canonicalPair(pair);
    auto symId = app.registry->idForCanonical(canon);
    if (!symId) {
        cb(errResp("error", "Unknown symbol: " + pair, 400));
        return;
    }

    int ivSec = util::intervalToSeconds(interval);
    int period = util::intervalToTrendbarPeriod(interval);
    int limit = clampInt(req->getParameter("limit").empty()
                             ? 1000 : std::atoi(req->getParameter("limit").c_str()), 1, 5000);
    auto startOpt = parseQueryTime(req->getParameter("start"));
    auto endOpt = parseQueryTime(req->getParameter("end"));
    int64_t toMs = endOpt ? static_cast<int64_t>(*endOpt) * 1000
                          : static_cast<int64_t>(std::time(nullptr)) * 1000;
    int64_t fromMs = 0;
    if (startOpt) {
        fromMs = static_cast<int64_t>(*startOpt) * 1000;
    } else {
        int64_t toSec = toMs / 1000;
        int64_t lookbackSec = static_cast<int64_t>(limit) * static_cast<int64_t>(ivSec);
        int64_t fromSec = toSec - lookbackSec;
        if (fromSec < 0) {
            fromSec = 0;
        }
        fromMs = fromSec * 1000;
    }

    auto callback = std::make_shared<std::function<void(const HttpResponsePtr &)>>(std::move(cb));
    std::string pairOut = pair;
    app.ctrader->getTrendbars(
        *symId, period, fromMs, toMs, static_cast<uint32_t>(limit),
        [callback, ivSec, interval, pairOut, canon, withForming, startOpt, endOpt,
         &app](ctrader::TrendbarsResult res) {
            if (!res.ok) {
                (*callback)(errResp("error", "Failed to query OHLC: " + res.error, 502));
                return;
            }
            std::time_t nowForFilter = std::time(nullptr);
            long long bucketForFilter =
                (static_cast<long long>(nowForFilter) / ivSec) * ivSec;
            const long long bucketMinuteForFilter = bucketForFilter / 60;

            Json::Value candles(Json::arrayValue);
            const ctrader::TrendbarData *inBucketBar = nullptr;
            for (const auto &b : res.bars) {
                if (!withForming && b.utcTimestampMinutes >= bucketMinuteForFilter) {
                    inBucketBar = &b;
                    continue;
                }
                candles.append(closedCandleJson(b, ivSec, false));
            }

            Json::Value out;
            out["pair"] = pairOut;
            out["interval"] = interval;
            out["start"] = startOpt ? Json::Value(util::toIso8601(*startOpt)) : Json::Value::null;
            out["end"] = endOpt ? Json::Value(util::toIso8601(*endOpt)) : Json::Value::null;

            if (!withForming) {
                if (app.hub && inBucketBar) {
                    app.hub->cacheTrendbar(canon, interval, *inBucketBar);
                } else if (app.hub && !res.bars.empty()) {
                    for (auto it = res.bars.rbegin(); it != res.bars.rend(); ++it) {
                        if (it->utcTimestampMinutes >= bucketMinuteForFilter) {
                            app.hub->cacheTrendbar(canon, interval, *it);
                            break;
                        }
                    }
                }
                out["count"] = (int)candles.size();
                out["candles"] = candles;
                (*callback)(jsonResp(out));
                return;
            }

            std::time_t now = std::time(nullptr);
            long long bucket = (static_cast<long long>(now) / ivSec) * ivSec;

            Json::Value closedOnly(Json::arrayValue);
            const ctrader::TrendbarData *lastBar = nullptr;
            const long long bucketMinute = bucket / 60;
            for (const auto &b : res.bars) {
                if (b.utcTimestampMinutes >= bucketMinute) {
                    lastBar = &b;
                    continue;
                }
                closedOnly.append(closedCandleJson(b, ivSec, false));
            }
            // Safeguard: if bucket filter removed every bar, keep history as closed
            // and use only the final trendbar to seed the forming candle.
            if (closedOnly.empty() && !res.bars.empty()) {
                for (size_t i = 0; i + 1 < res.bars.size(); ++i) {
                    closedOnly.append(closedCandleJson(res.bars[i], ivSec, false));
                }
                lastBar = &res.bars.back();
            }
            int closedCount = static_cast<int>(closedOnly.size());

            bool hasForming = false;
            Json::Value formingCandle(Json::nullValue);
            double livePrice = 0;
            if (app.hub && app.hub->latestPrice(canon, livePrice)) {
                const ctrader::TrendbarData *prevClosed = nullptr;
                if (!lastBar && !res.bars.empty()) {
                    prevClosed = &res.bars.back();
                }
                formingCandle = util::buildFormingCandleMerged(
                    livePrice, interval, lastBar, prevClosed);
                if (lastBar && app.hub) {
                    app.hub->cacheTrendbar(canon, interval, *lastBar);
                }
                hasForming = true;
            }
            out["closed_candles_count"] = closedCount;
            out["count"] = closedCount;
            out["has_forming_candle"] = hasForming;
            out["forming_candle"] = formingCandle;
            out["last_update"] = app.hub ? app.hub->lastSnapshotTs() : util::nowIso8601();
            out["candles"] = closedOnly;
            (*callback)(jsonResp(out));
        });
}

// ---- Alerts ----

bool rejectAlertsWithoutDb(std::function<void(const HttpResponsePtr &)> &cb) {
    if (!core::isDbReadyForAuth()) {
        cb(errResp("detail", "Database not ready", 503));
        return true;
    }
    auto &app = AppContext::instance();
    if (!app.alerts || !app.alerts->dbPersistenceEnabled()) {
        cb(errResp("detail", "Alert persistence unavailable", 503));
        return true;
    }
    return false;
}

void createAlert(const HttpRequestPtr &req,
                 std::function<void(const HttpResponsePtr &)> &&cb) {
    auto &app = AppContext::instance();
    std::string uid;
    if (!authOrReject(req, cb, uid)) return;
    if (rejectAlertsWithoutDb(cb)) return;
    auto body = req->getJsonObject();
    if (!body) {
        cb(errResp("detail", "Invalid JSON body", 400));
        return;
    }
    const Json::Value &b = *body;
    std::string pair = b.get("pair", "").asString();
    // trim
    auto trim = [](std::string s) {
        size_t a = s.find_first_not_of(" \t");
        size_t z = s.find_last_not_of(" \t");
        return a == std::string::npos ? std::string() : s.substr(a, z - a + 1);
    };
    pair = trim(pair);
    if (pair.empty()) {
        cb(errResp("detail", "Pair name cannot be empty", 400));
        return;
    }
    if (pair.find(':') != std::string::npos) {
        auto pos = pair.find(':');
        if (pos == 0 || pos == pair.size() - 1) {
            cb(errResp("detail",
                       "Commodity pair must be in format 'SYMBOL:TYPE' (e.g., 'XAUUSD:CUR')", 400));
            return;
        }
    }

    std::string parseErr;
    auto channels = parseChannels(b, parseErr);
    if (channels.empty()) {
        cb(errResp("detail", parseErr.empty() ? "Invalid channels" : parseErr, 400));
        return;
    }
    std::string email = b.get("email", "").asString();
    std::string phone = b.get("phone", "").asString();
    std::string customMessage = trimStr(b.get("custom_message", "").asString());
    bool isCandle = b.isMember("interval") && b["interval"].isString() &&
                    !b["interval"].asString().empty();

    if (channelsRequireEmail(channels) && email.empty()) {
        cb(errResp("detail", "Email is required for email alerts", 400));
        return;
    }
    if (channelsRequirePhone(channels) && phone.empty()) {
        cb(errResp("detail", "Phone is required for SMS/call alerts", 400));
        return;
    }
    if (channelsRequireCustomMessage(channels, customMessage)) {
        cb(errResp("detail", "custom_message is required for SMS and call alerts", 400));
        return;
    }

    if (isCandle) {
        std::string interval = b["interval"].asString();
        std::string direction = b.get("direction", "").asString();
        double threshold = b.get("threshold", 0.0).asDouble();
        if (direction != "above" && direction != "below") {
            cb(errResp("detail", "Direction must be 'above' or 'below'", 400));
            return;
        }
        try {
            auto a = app.alerts->createCandleAlert(pair, interval, direction, threshold, uid,
                                                   email, channels, phone, customMessage);
            std::ostringstream detail;
            detail << "pair=" << pair << " channels=" << channels.size()
                   << " interval=" << interval;
            core::logApiOutcome("alerts", "create", true, 200, detail.str(), uid);
            Json::Value meta;
            meta["pair"] = pair;
            meta["alert_id"] = a.id;
            meta["type"] = "candle_close";
            logActivityAsync(uid, "alert_create", clientIp(req), clientUserAgent(req), meta);
            Json::Value v;
            v["success"] = true;
            v["alert"] = a.toJson();
            cb(jsonResp(v));
        } catch (const std::runtime_error &e) {
            const std::string msg = e.what();
            if (msg == "Alert not persisted" || msg == "Database unavailable") {
                cb(errResp("detail", msg, 503));
            } else {
                cb(errResp("detail", msg, 400));
            }
        } catch (const std::exception &e) {
            cb(errResp("detail", e.what(), 400));
        }
        return;
    }

    std::string condition = b.get("condition", "").asString();
    double target = b.get("target_price", 0.0).asDouble();
    if (condition != "above" && condition != "below" && condition != "equal") {
        cb(errResp("detail", "Condition must be 'above', 'below', or 'equal'", 400));
        return;
    }
    try {
        auto a = app.alerts->createPriceAlert(pair, target, condition, uid, email, channels,
                                              phone, customMessage);
        double livePrice = 0;
        if (app.hub && app.hub->latestPrice(a.pair, livePrice)) {
            app.alerts->tryTriggerPriceAlert(a.id, livePrice);
            if (auto updated = app.alerts->getAlert(a.id)) a = *updated;
        }
        std::ostringstream detail;
        detail << "pair=" << pair << " channels=" << channels.size() << " target=" << target
               << " condition=" << condition;
        if (a.status == "triggered") detail << " triggered=1";
        core::logApiOutcome("alerts", "create", true, 200, detail.str(), uid);
        Json::Value meta;
        meta["pair"] = pair;
        meta["alert_id"] = a.id;
        meta["type"] = "price";
        logActivityAsync(uid, "alert_create", clientIp(req), clientUserAgent(req), meta);
        Json::Value v;
        v["success"] = true;
        v["alert"] = a.toJson();
        cb(jsonResp(v));
    } catch (const std::runtime_error &e) {
        const std::string msg = e.what();
        if (msg == "Alert not persisted" || msg == "Database unavailable") {
            cb(errResp("detail", msg, 503));
        } else {
            cb(errResp("detail", msg, 400));
        }
    }
}

void listAlerts(const HttpRequestPtr &req,
                std::function<void(const HttpResponsePtr &)> &&cb) {
    auto &app = AppContext::instance();
    std::string uid;
    if (!authOrReject(req, cb, uid)) return;
    if (rejectAlertsWithoutDb(cb)) return;
    auto all = app.alerts->getAllAlertsForUser(uid);
    Json::Value active(Json::arrayValue), triggered(Json::arrayValue), allArr(Json::arrayValue);
    for (const auto &a : app.alerts->getActiveAlertsSortedForUser(uid)) active.append(a.toJson());
    for (const auto &a : all) {
        allArr.append(a.toJson());
        if (a.status == "triggered") triggered.append(a.toJson());
    }
    core::logApiOutcome("alerts", "list", true, 200,
                        "active=" + std::to_string(active.size()) + " triggered=" +
                            std::to_string(triggered.size()),
                        uid);
    Json::Value v;
    v["total"] = (int)all.size();
    v["active"] = active;
    v["triggered"] = triggered;
    v["all"] = allArr;
    cb(jsonResp(v));
}

void getAlert(const HttpRequestPtr &req, std::function<void(const HttpResponsePtr &)> &&cb,
              std::string alertId) {
    auto &app = AppContext::instance();
    std::string uid;
    if (!authOrReject(req, cb, uid)) return;
    if (rejectAlertsWithoutDb(cb)) return;
    if (!app.alerts->isAlertOwnedBy(alertId, uid)) {
        cb(errResp("detail", "Alert not found", 404));
        return;
    }
    auto a = app.alerts->getAlert(alertId);
    cb(jsonResp(a->toJson()));
}

void deleteAlert(const HttpRequestPtr &req, std::function<void(const HttpResponsePtr &)> &&cb,
                 std::string alertId) {
    auto &app = AppContext::instance();
    std::string uid;
    if (!authOrReject(req, cb, uid)) return;
    if (rejectAlertsWithoutDb(cb)) return;
    if (!app.alerts->isAlertOwnedBy(alertId, uid)) {
        cb(errResp("detail", "Alert not found", 404));
        return;
    }
    if (!app.alerts->deleteAlert(alertId, uid)) {
        cb(errResp("detail", "Alert not persisted", 503));
        return;
    }
    Json::Value meta;
    meta["alert_id"] = alertId;
    logActivityAsync(uid, "alert_delete", clientIp(req), clientUserAgent(req), meta);
    Json::Value v;
    v["success"] = true;
    v["message"] = "Alert deleted";
    cb(jsonResp(v));
}

void updateAlert(const HttpRequestPtr &req, std::function<void(const HttpResponsePtr &)> &&cb,
                 std::string alertId) {
    auto &app = AppContext::instance();
    std::string uid;
    if (!authOrReject(req, cb, uid)) return;
    if (rejectAlertsWithoutDb(cb)) return;
    auto existing = app.alerts->getAlert(alertId);
    if (!existing || existing->userId != uid) {
        cb(errResp("detail", "Alert not found", 404));
        return;
    }
    auto body = req->getJsonObject();
    Json::Value updates = body ? *body : Json::Value(Json::objectValue);
    std::vector<std::string> channels = existing->effectiveChannels();
    if (updates.isMember("channels") || updates.isMember("channel")) {
        std::string parseErr;
        channels = parseChannels(updates, parseErr);
        if (channels.empty()) {
            cb(errResp("detail", parseErr.empty() ? "Invalid channels" : parseErr, 400));
            return;
        }
    }
    std::string email = existing->email;
    if (updates.isMember("email") && updates["email"].isString())
        email = updates["email"].asString();
    std::string phone = existing->phone;
    if (updates.isMember("phone") && updates["phone"].isString())
        phone = updates["phone"].asString();
    std::string customMessage = existing->customMessage;
    if (updates.isMember("custom_message") && updates["custom_message"].isString())
        customMessage = trimStr(updates["custom_message"].asString());
    if (channelsRequireEmail(channels) && email.empty()) {
        cb(errResp("detail", "Email is required for email alerts", 400));
        return;
    }
    if (channelsRequirePhone(channels) && phone.empty()) {
        cb(errResp("detail", "Phone is required for SMS/call alerts", 400));
        return;
    }
    if (channelsRequireCustomMessage(channels, customMessage)) {
        cb(errResp("detail", "custom_message is required for SMS and call alerts", 400));
        return;
    }
    try {
        auto updated = app.alerts->updateAlert(alertId, updates, uid);
        if (!updated) {
            cb(errResp("detail", "Alert not found", 404));
            return;
        }
        Json::Value meta;
        meta["alert_id"] = alertId;
        meta["pair"] = updated->pair;
        logActivityAsync(uid, "alert_update", clientIp(req), clientUserAgent(req), meta);
        Json::Value v;
        v["success"] = true;
        v["alert"] = updated->toJson();
        cb(jsonResp(v));
    } catch (const std::runtime_error &e) {
        const std::string msg = e.what();
        if (msg == "Alert not persisted" || msg == "Database unavailable") {
            cb(errResp("detail", msg, 503));
        } else {
            cb(errResp("detail", msg, 400));
        }
    } catch (const std::exception &e) {
        cb(errResp("detail", e.what(), 400));
    }
}

}  // namespace

void registerRoutes() {
    auto &fw = drogon::app();

    fw.registerHandler("/", [](const HttpRequestPtr &,
                               std::function<void(const HttpResponsePtr &)> &&cb) {
        auto resp = HttpResponse::newRedirectionResponse("/dashboard");
        cb(resp);
    }, {Get});

    fw.registerHandler("/health", &health, {Get});
    fw.registerHandler("/ping", &ping, {Get});
    fw.registerHandler("/dashboard", &dashboard, {Get});
    fw.registerHandler("/openapi.yaml", &openApiSpec, {Get});
    fw.registerHandler("/docs", &apiDocs, {Get});
    fw.registerHandler("/snapshot", &snapshot, {Get});
    fw.registerHandler("/client-config", &clientConfig, {Get});
    fw.registerHandler("/stream-health", &streamHealth, {Get});
    fw.registerHandler("/me", &me, {Get});
    fw.registerHandler("/onboarding/complete", &onboardingComplete, {Post});

    fw.registerHandler("/historical", &historical, {Get});
    fw.registerHandler("/historical/stream-metrics", &streamMetrics, {Get});
    fw.registerHandler("/historical/ohlc",
                       [](const HttpRequestPtr &req,
                          std::function<void(const HttpResponsePtr &)> &&cb) {
                           historicalOhlc(req, std::move(cb), false);
                       },
                       {Get});
    fw.registerHandler("/historical/ohlc-with-forming",
                       [](const HttpRequestPtr &req,
                          std::function<void(const HttpResponsePtr &)> &&cb) {
                           historicalOhlc(req, std::move(cb), true);
                       },
                       {Get});

    fw.registerHandler("/api/v1/alerts", &createAlert, {Post});
    fw.registerHandler("/api/v1/alerts", &listAlerts, {Get});
    fw.registerHandler("/api/v1/alerts/{1}", &getAlert, {Get});
    fw.registerHandler("/api/v1/alerts/{1}", &deleteAlert, {Delete});
    fw.registerHandler("/api/v1/alerts/{1}", &updateAlert, {Put});

    registerCors();
    registerAuthRoutes();
    registerFavoritesRoutes();
    registerAdminRoutes();
}

}  // namespace ctraderplus::controllers
