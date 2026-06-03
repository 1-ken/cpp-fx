#include "core/Password.h"

#include <openssl/evp.h>
#include <openssl/rand.h>

#include <cctype>
#include <sstream>
#include <stdexcept>
#include <vector>

namespace ctraderplus::core {

namespace {

constexpr int kIterations = 120000;
constexpr int kSaltLen = 16;
constexpr int kHashLen = 32;

std::string base64Encode(const unsigned char *data, size_t len) {
    static const char *tbl =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((len + 2) / 3) * 4);
    for (size_t i = 0; i < len; i += 3) {
        unsigned int n = static_cast<unsigned int>(data[i]) << 16;
        if (i + 1 < len) n |= static_cast<unsigned int>(data[i + 1]) << 8;
        if (i + 2 < len) n |= static_cast<unsigned int>(data[i + 2]);
        out.push_back(tbl[(n >> 18) & 63]);
        out.push_back(tbl[(n >> 12) & 63]);
        out.push_back(i + 1 < len ? tbl[(n >> 6) & 63] : '=');
        out.push_back(i + 2 < len ? tbl[n & 63] : '=');
    }
    return out;
}

bool base64Decode(const std::string &in, std::vector<unsigned char> &out) {
    auto val = [](char c) -> int {
        if (c >= 'A' && c <= 'Z') return c - 'A';
        if (c >= 'a' && c <= 'z') return c - 'a' + 26;
        if (c >= '0' && c <= '9') return c - '0' + 52;
        if (c == '+') return 62;
        if (c == '/') return 63;
        return -1;
    };
    out.clear();
    int buf = 0, bits = 0;
    for (char c : in) {
        if (c == '=') break;
        if (std::isspace(static_cast<unsigned char>(c))) continue;
        int v = val(c);
        if (v < 0) return false;
        buf = (buf << 6) | v;
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            out.push_back(static_cast<unsigned char>((buf >> bits) & 0xFF));
        }
    }
    return !out.empty();
}

std::vector<std::string> split(const std::string &s, char delim) {
    std::vector<std::string> parts;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) parts.push_back(item);
    return parts;
}

}  // namespace

std::string hashPassword(const std::string &password) {
    if (password.empty()) throw std::invalid_argument("Password cannot be empty");

    unsigned char salt[kSaltLen];
    if (RAND_bytes(salt, kSaltLen) != 1) throw std::runtime_error("Failed to generate salt");

    unsigned char hash[kHashLen];
    if (PKCS5_PBKDF2_HMAC(password.c_str(), static_cast<int>(password.size()), salt,
                          kSaltLen, kIterations, EVP_sha256(), kHashLen, hash) != 1) {
        throw std::runtime_error("PBKDF2 failed");
    }

    std::ostringstream oss;
    oss << "pbkdf2$" << kIterations << "$" << base64Encode(salt, kSaltLen) << "$"
        << base64Encode(hash, kHashLen);
    return oss.str();
}

bool verifyPassword(const std::string &password, const std::string &encodedHash) {
    if (password.empty() || encodedHash.empty()) return false;
    auto parts = split(encodedHash, '$');
    if (parts.size() != 4 || parts[0] != "pbkdf2") return false;

    int iters = 0;
    try {
        iters = std::stoi(parts[1]);
    } catch (...) {
        return false;
    }
    if (iters < 10000) return false;

    std::vector<unsigned char> salt, expected;
    if (!base64Decode(parts[2], salt) || !base64Decode(parts[3], expected)) return false;

    unsigned char hash[64];
    int hashLen = static_cast<int>(expected.size());
    if (hashLen > static_cast<int>(sizeof(hash))) return false;

    if (PKCS5_PBKDF2_HMAC(password.c_str(), static_cast<int>(password.size()),
                          salt.data(), static_cast<int>(salt.size()), iters,
                          EVP_sha256(), hashLen, hash) != 1) {
        return false;
    }
    return std::equal(expected.begin(), expected.end(), hash, hash + hashLen);
}

}  // namespace ctraderplus::core
