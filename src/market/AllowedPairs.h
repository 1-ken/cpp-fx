#pragma once

#include <string>
#include <unordered_set>

namespace ctraderplus::core {
struct Config;
}

namespace ctraderplus::market {

bool hasExplicitPairList(const core::Config &cfg);
std::unordered_set<std::string> buildAllowedCanonicalSet(const core::Config &cfg);
bool isAllowedPair(const core::Config &cfg, const std::string &canonical);

}  // namespace ctraderplus::market
