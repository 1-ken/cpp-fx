#pragma once

#include <string>

namespace ctraderplus::util {

// True if host is a dotted IPv4 literal (inet_pton succeeds).
bool isIpv4Literal(const std::string &host);

// Resolve hostname to IPv4 string via getaddrinfo. Returns "" on failure.
std::string resolveHostToIpv4(const std::string &host);

}  // namespace ctraderplus::util
