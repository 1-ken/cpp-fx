#include "core/ApiLog.h"

#include <cctype>

#include <trantor/utils/Logger.h>

namespace ctraderplus::core {

std::string normalizeUsername(const std::string &username) {
    std::string out;
    out.reserve(username.size());
    for (unsigned char c : username) {
        if (c < 128)
            out.push_back(static_cast<char>(std::tolower(c)));
        else
            out.push_back(static_cast<char>(c));
    }
    return out;
}

void logApiOutcome(const char *area, const char *action, bool ok, int httpStatus,
                   const std::string &detail, const std::string &userId) {
    std::string msg = std::string("[") + area + "] " + action + (ok ? " OK" : " FAIL")
                      + " status=" + std::to_string(httpStatus);
    if (!detail.empty()) msg += " detail=" + detail;
    if (!userId.empty()) msg += " user_id=" + userId;
    if (ok)
        LOG_INFO << msg;
    else
        LOG_WARN << msg;
}

}  // namespace ctraderplus::core
