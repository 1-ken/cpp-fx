#pragma once

#include <cctype>
#include <string>

#include <drogon/HttpResponse.h>
#include <json/json.h>

namespace ctraderplus::controllers {

inline drogon::HttpResponsePtr jsonResp(const Json::Value &v, int code = 200) {
    auto resp = drogon::HttpResponse::newHttpJsonResponse(v);
    resp->setStatusCode(static_cast<drogon::HttpStatusCode>(code));
    return resp;
}

inline drogon::HttpResponsePtr errResp(const std::string &msg, int code) {
    Json::Value v;
    v["detail"] = msg;
    return jsonResp(v, code);
}

inline drogon::HttpResponsePtr errResp(const std::string &detailKey, const std::string &msg, int code) {
    Json::Value v;
    v[detailKey] = msg;
    return jsonResp(v, code);
}

inline std::string trim(const std::string &s) {
    size_t a = s.find_first_not_of(" \t\r\n");
    if (a == std::string::npos) return "";
    size_t b = s.find_last_not_of(" \t\r\n");
    return s.substr(a, b - a + 1);
}

inline std::string trimStr(const std::string &s) { return trim(s); }

inline std::string normalizeMarketerCode(const std::string &code) {
    std::string out;
    out.reserve(code.size());
    for (unsigned char c : code) {
        if (c >= 'A' && c <= 'Z')
            out.push_back(static_cast<char>(c + 32));
        else if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_')
            out.push_back(static_cast<char>(c));
    }
    return out;
}

inline bool isValidMarketerCodeFormat(const std::string &code) {
    if (code.size() < 3 || code.size() > 32) return false;
    for (unsigned char c : code) {
        if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_')) return false;
    }
    return true;
}

}  // namespace ctraderplus::controllers
