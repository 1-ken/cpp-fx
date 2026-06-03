#include "util/HostResolve.h"

#include <arpa/inet.h>
#include <netdb.h>

namespace ctraderplus::util {

bool isIpv4Literal(const std::string &host) {
    if (host.empty()) return false;
    struct in_addr addr {};
    return ::inet_pton(AF_INET, host.c_str(), &addr) == 1;
}

std::string resolveHostToIpv4(const std::string &host) {
    if (host.empty()) return "";
    if (isIpv4Literal(host)) return host;

    struct addrinfo hints {};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo *res = nullptr;
    if (::getaddrinfo(host.c_str(), nullptr, &hints, &res) != 0 || !res) {
        return "";
    }
    char ip[INET_ADDRSTRLEN] = {0};
    auto *addr = reinterpret_cast<struct sockaddr_in *>(res->ai_addr);
    ::inet_ntop(AF_INET, &addr->sin_addr, ip, sizeof(ip));
    ::freeaddrinfo(res);
    return std::string(ip);
}

}  // namespace ctraderplus::util
