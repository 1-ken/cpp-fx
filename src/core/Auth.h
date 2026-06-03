#pragma once

#include <map>
#include <optional>
#include <string>

namespace ctraderplus::core {

struct AuthResult {
    bool ok = false;
    std::string userId;
    int statusCode = 401;       // used when !ok
    std::string detail;         // error message when !ok
};

// Validate an HS256 NextAuth token, returning the `sub` claim as the user id.
AuthResult decodeAccessToken(const std::string &token);

// Resolve the current user from an HTTP Authorization header value
// ("Bearer <token>"). Honors AUTH_DISABLED (returns "dev-user").
AuthResult getCurrentUserId(const std::string &authorizationHeader);

// Validate a WebSocket `access_token` query parameter. Honors AUTH_DISABLED.
AuthResult verifyWsAccessToken(const std::optional<std::string> &token);

// Sign an HS256 JWT (NextAuth-compatible). extraClaims merged into payload.
std::string signToken(const std::string &sub, int ttlSeconds,
                      const std::map<std::string, std::string> &extraClaims = {});

// Admin bearer token: requires role=admin claim.
AuthResult requireAdmin(const std::string &authorizationHeader);

}  // namespace ctraderplus::core
