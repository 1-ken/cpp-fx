#include "core/Auth.h"

#include "core/Config.h"

#include <jwt-cpp/jwt.h>
#include <trantor/utils/Logger.h>

#include <chrono>
#include <map>
#include <stdexcept>

namespace ctraderplus::core {

AuthResult decodeAccessToken(const std::string &token) {
    const auto &cfg = getConfig();
    AuthResult res;

    if (cfg.nextAuthSecret.empty()) {
        res.ok = false;
        res.statusCode = 500;
        res.detail = "NEXTAUTH_SECRET is not configured on the observer API";
        return res;
    }

    try {
        auto decoded = jwt::decode(token);
        auto verifier =
            jwt::verify().allow_algorithm(jwt::algorithm::hs256{cfg.nextAuthSecret});
        verifier.verify(decoded);

        if (!decoded.has_payload_claim("sub") || !decoded.has_expires_at()) {
            res.ok = false;
            res.statusCode = 401;
            res.detail = "Invalid token payload";
            return res;
        }
        std::string sub = decoded.get_payload_claim("sub").as_string();
        if (sub.empty()) {
            res.ok = false;
            res.statusCode = 401;
            res.detail = "Invalid token payload";
            return res;
        }
        res.ok = true;
        res.userId = sub;
        return res;
    } catch (const std::exception &e) {
        LOG_DEBUG << "[auth] JWT decode failed: " << e.what();
        res.ok = false;
        res.statusCode = 401;
        res.detail = "Invalid or expired token";
        return res;
    }
}

AuthResult getCurrentUserId(const std::string &authorizationHeader) {
    const auto &cfg = getConfig();
    if (cfg.authDisabled) {
        return AuthResult{true, "dev-user", 200, ""};
    }

    const std::string prefix = "Bearer ";
    std::string token;
    if (authorizationHeader.size() > prefix.size() &&
        authorizationHeader.compare(0, prefix.size(), prefix) == 0) {
        token = authorizationHeader.substr(prefix.size());
    } else {
        token = authorizationHeader;
    }

    if (token.empty()) {
        return AuthResult{false, "", 401, "Missing authorization token"};
    }
    return decodeAccessToken(token);
}

AuthResult verifyWsAccessToken(const std::optional<std::string> &token) {
    const auto &cfg = getConfig();
    if (cfg.authDisabled) {
        return AuthResult{true, "dev-user", 200, ""};
    }
    if (!token || token->empty()) {
        return AuthResult{false, "", 401, "Missing access_token"};
    }
    return decodeAccessToken(*token);
}

std::string signToken(const std::string &sub, int ttlSeconds,
                      const std::map<std::string, std::string> &extraClaims) {
    const auto &cfg = getConfig();
    if (cfg.nextAuthSecret.empty()) {
        throw std::runtime_error("NEXTAUTH_SECRET is not configured");
    }
    auto now = std::chrono::system_clock::now();
    auto exp = now + std::chrono::seconds(ttlSeconds);

    auto builder = jwt::create()
                       .set_type("JWT")
                       .set_issued_at(now)
                       .set_expires_at(exp)
                       .set_payload_claim("sub", jwt::claim(sub));

    for (const auto &[k, v] : extraClaims) {
        builder.set_payload_claim(k, jwt::claim(v));
    }

    return builder.sign(jwt::algorithm::hs256{cfg.nextAuthSecret});
}

AuthResult requireAdmin(const std::string &authorizationHeader) {
    const auto &cfg = getConfig();
    if (cfg.nextAuthSecret.empty()) {
        return AuthResult{false, "", 500,
                          "NEXTAUTH_SECRET is not configured on the observer API"};
    }

    const std::string prefix = "Bearer ";
    std::string token;
    if (authorizationHeader.size() > prefix.size() &&
        authorizationHeader.compare(0, prefix.size(), prefix) == 0) {
        token = authorizationHeader.substr(prefix.size());
    } else {
        token = authorizationHeader;
    }
    if (token.empty()) {
        return AuthResult{false, "", 401, "Missing authorization token"};
    }

    try {
        auto decoded = jwt::decode(token);
        auto verifier =
            jwt::verify().allow_algorithm(jwt::algorithm::hs256{cfg.nextAuthSecret});
        verifier.verify(decoded);

        if (!decoded.has_payload_claim("role") ||
            decoded.get_payload_claim("role").as_string() != "admin") {
            return AuthResult{false, "", 403, "Admin access required"};
        }
        if (!decoded.has_payload_claim("sub")) {
            return AuthResult{false, "", 401, "Invalid admin token"};
        }
        return AuthResult{true, decoded.get_payload_claim("sub").as_string(), 200, ""};
    } catch (const std::exception &) {
        return AuthResult{false, "", 401, "Invalid or expired token"};
    }
}

}  // namespace ctraderplus::core
