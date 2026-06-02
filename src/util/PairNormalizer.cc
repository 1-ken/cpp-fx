#include "util/PairNormalizer.h"

#include <algorithm>
#include <array>
#include <cctype>

namespace ctraderplus::util {

namespace {
constexpr std::array<const char *, 3> kProviderSuffixes = {"CUR", "COM", "IND"};

std::string upperTrim(const std::string &value) {
    size_t start = 0;
    size_t end = value.size();
    while (start < end && std::isspace(static_cast<unsigned char>(value[start]))) ++start;
    while (end > start && std::isspace(static_cast<unsigned char>(value[end - 1]))) --end;
    std::string out = value.substr(start, end - start);
    std::transform(out.begin(), out.end(), out.begin(),
                   [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    return out;
}

bool isAlpha(const std::string &s) {
    return !s.empty() && std::all_of(s.begin(), s.end(), [](unsigned char c) {
               return std::isalpha(c) != 0;
           });
}

bool endsWith(const std::string &s, const std::string &suffix) {
    return s.size() >= suffix.size() &&
           s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
}
}  // namespace

std::string canonicalPair(const std::string &value) {
    if (value.empty()) return "";
    std::string normalized = upperTrim(value);
    if (normalized.empty()) return "";

    auto colon = normalized.find(':');
    if (colon != std::string::npos) {
        normalized = normalized.substr(0, colon);
    } else {
        for (const char *suffix : kProviderSuffixes) {
            std::string suf(suffix);
            if (normalized.size() > suf.size() && endsWith(normalized, suf)) {
                std::string base = normalized.substr(0, normalized.size() - suf.size());
                if (base.size() == 6 && isAlpha(base)) {
                    normalized = base;
                    break;
                }
            }
        }
    }

    std::string compact;
    compact.reserve(normalized.size());
    for (char c : normalized) {
        if (c != '/') compact.push_back(c);
    }
    if (compact.size() == 6 && isAlpha(compact)) {
        return compact;
    }
    return normalized;
}

std::vector<std::string> pairVariants(const std::string &value) {
    std::vector<std::string> variants;
    std::string canonical = canonicalPair(value);
    if (canonical.empty()) return variants;

    variants.push_back(canonical);
    std::string compact = canonical;
    compact.erase(std::remove(compact.begin(), compact.end(), '/'), compact.end());

    auto contains = [&](const std::string &v) {
        return std::find(variants.begin(), variants.end(), v) != variants.end();
    };

    if (compact.size() == 6 && isAlpha(compact)) {
        std::string slash = compact.substr(0, 3) + "/" + compact.substr(3);
        if (!contains(slash)) variants.push_back(slash);
        if (!contains(compact)) variants.push_back(compact);
    }

    if (canonical.find(':') == std::string::npos) {
        for (const char *suffix : kProviderSuffixes) {
            std::string tagged = canonical + ":" + suffix;
            if (!contains(tagged)) variants.push_back(tagged);
        }
    }
    return variants;
}

bool isCanonical(const std::string &value) {
    return !value.empty() && canonicalPair(value) == value;
}

}  // namespace ctraderplus::util
