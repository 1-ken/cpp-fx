#pragma once

#include <openssl/rand.h>

#include <sstream>
#include <string>

namespace ctraderplus::util {

inline std::string generateUuid() {
    unsigned char b[16];
    if (RAND_bytes(b, 16) != 1) return "";
    b[6] = static_cast<unsigned char>((b[6] & 0x0f) | 0x40);
    b[8] = static_cast<unsigned char>((b[8] & 0x3f) | 0x80);
    static const char hex[] = "0123456789abcdef";
    std::ostringstream oss;
    for (int i = 0; i < 16; ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10) oss << '-';
        oss << hex[b[i] >> 4] << hex[b[i] & 0x0f];
    }
    return oss.str();
}

}  // namespace ctraderplus::util
