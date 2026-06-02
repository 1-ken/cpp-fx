#include "core/Auth.h"

#include "core/Config.h"

#include <jwt-cpp/jwt.h>

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
        auto verifier = jwt::verify()
                            .allow_algorithm(jwt::algorithm::hs256{cfg.nextAuthSecret})
                            .with_claim("sub", jwt::claim())  // require presence below
                            ;
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
    } catch (const std::exception &) {
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

    // Expect "Bearer <token>".
    const std::string prefix = "Bearer ";
    std::string token;
    if (authorizationHeader.size() > prefix.size() &&
        authorizationHeader.compare(0, prefix.size(), prefix) == 0) {
        token = authorizationHeader.substr(prefix.size());
    } else {
        token = authorizationHeader;  // tolerate raw token
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

}  // namespace ctraderplus::core
