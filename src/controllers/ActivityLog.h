#pragma once

#include <drogon/HttpRequest.h>
#include <json/json.h>
#include <string>

namespace ctraderplus::controllers {

void logActivityAsync(const std::string &userId, const std::string &eventType,
                      const std::string &ipAddress, const std::string &userAgent,
                      const Json::Value &metadata = Json::Value(Json::objectValue));

std::string clientIp(const drogon::HttpRequestPtr &req);
std::string clientUserAgent(const drogon::HttpRequestPtr &req);

}  // namespace ctraderplus::controllers
