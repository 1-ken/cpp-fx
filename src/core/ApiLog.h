#pragma once

#include <string>

namespace ctraderplus::core {

void logApiOutcome(const char *area, const char *action, bool ok, int httpStatus,
                   const std::string &detail, const std::string &userId = "");

std::string normalizeUsername(const std::string &username);

}  // namespace ctraderplus::core
