#pragma once

#include <optional>
#include <string>

#include <json/json.h>

namespace ctraderplus::alerts {

// Mirrors the Python Alert dataclass (app/services/alert_service.py). Field
// names in toJson() match the JSON contract consumed by the frontend.
struct Alert {
    std::string id;
    std::string userId = "legacy-unassigned";
    std::string pair;
    std::string status = "active";  // active, triggered, disabled
    std::string createdAt;
    std::string alertType = "price";  // price | candle_close
    std::string channel = "email";    // email | sms | call
    std::string email;
    std::string phone;
    std::string customMessage;
    std::optional<std::string> triggeredAt;
    std::optional<double> lastCheckedPrice;
    std::optional<double> closePrice;

    // price alert
    std::optional<double> targetPrice;
    std::optional<std::string> condition;  // above | below | equal

    // candle_close alert
    std::optional<std::string> interval;
    std::optional<std::string> direction;  // above | below
    std::optional<double> threshold;
    std::optional<std::string> lastEvaluatedCandleTime;

    Json::Value toJson() const {
        Json::Value v(Json::objectValue);
        v["id"] = id;
        v["user_id"] = userId;
        v["pair"] = pair;
        v["status"] = status;
        v["created_at"] = createdAt;
        v["alert_type"] = alertType;
        v["channel"] = channel;
        v["email"] = email;
        v["phone"] = phone;
        v["custom_message"] = customMessage;
        v["triggered_at"] = triggeredAt ? Json::Value(*triggeredAt) : Json::Value::null;
        v["last_checked_price"] =
            lastCheckedPrice ? Json::Value(*lastCheckedPrice) : Json::Value::null;
        v["close_price"] = closePrice ? Json::Value(*closePrice) : Json::Value::null;
        v["target_price"] = targetPrice ? Json::Value(*targetPrice) : Json::Value::null;
        v["condition"] = condition ? Json::Value(*condition) : Json::Value::null;
        v["interval"] = interval ? Json::Value(*interval) : Json::Value::null;
        v["direction"] = direction ? Json::Value(*direction) : Json::Value::null;
        v["threshold"] = threshold ? Json::Value(*threshold) : Json::Value::null;
        v["last_evaluated_candle_time"] =
            lastEvaluatedCandleTime ? Json::Value(*lastEvaluatedCandleTime)
                                    : Json::Value::null;
        return v;
    }

    static Alert fromJson(const Json::Value &v) {
        Alert a;
        a.id = v.get("id", "").asString();
        a.userId = v.get("user_id", "legacy-unassigned").asString();
        if (a.userId.empty()) a.userId = "legacy-unassigned";
        a.pair = v.get("pair", "").asString();
        a.status = v.get("status", "active").asString();
        a.createdAt = v.get("created_at", "").asString();
        a.alertType = v.get("alert_type", "price").asString();
        a.channel = v.get("channel", "email").asString();
        a.email = v.get("email", "").asString();
        a.phone = v.get("phone", "").asString();
        a.customMessage = v.get("custom_message", "").asString();
        auto optStr = [&](const char *k) -> std::optional<std::string> {
            if (v.isMember(k) && v[k].isString() && !v[k].asString().empty())
                return v[k].asString();
            return std::nullopt;
        };
        auto optNum = [&](const char *k) -> std::optional<double> {
            if (v.isMember(k) && v[k].isNumeric()) return v[k].asDouble();
            return std::nullopt;
        };
        a.triggeredAt = optStr("triggered_at");
        a.lastCheckedPrice = optNum("last_checked_price");
        a.closePrice = optNum("close_price");
        a.targetPrice = optNum("target_price");
        a.condition = optStr("condition");
        a.interval = optStr("interval");
        a.direction = optStr("direction");
        a.threshold = optNum("threshold");
        a.lastEvaluatedCandleTime = optStr("last_evaluated_candle_time");
        return a;
    }
};

}  // namespace ctraderplus::alerts
