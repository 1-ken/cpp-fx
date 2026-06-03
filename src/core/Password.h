#pragma once

#include <string>

namespace ctraderplus::core {

// Returns encoded hash: pbkdf2$<iters>$<salt_b64>$<hash_b64>
std::string hashPassword(const std::string &password);

bool verifyPassword(const std::string &password, const std::string &encodedHash);

}  // namespace ctraderplus::core
