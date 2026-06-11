#pragma once

#include <string>

namespace ctraderplus::core {

void logApiOutcome(const char *area, const char *action, bool ok, int httpStatus,
                   const std::string &detail, const std::string &userId = "");

std::string normalizeUsername(const std::string &username);

// Short stable hash for logs (never log raw user_id / PII).
std::string hashUserIdForLog(const std::string &userId);

}  // namespace ctraderplus::core
