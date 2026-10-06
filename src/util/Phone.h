#pragma once

#include <string>

namespace ctraderplus::util {

// Strip formatting. A leading 00 becomes +. A digit-only number gains a leading +.
std::string normalizePhone(const std::string &phone);

// True for + followed by 8 to 15 digits, not starting with 0.
bool isE164(const std::string &phone);

}  // namespace ctraderplus::util
