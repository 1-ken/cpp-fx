#pragma once

#include <string>
#include <vector>

// Single source of truth for pair-name canonicalization. Ported from
// app/utils/pair_normalizer.py in the original Python project.
//
// - Strip whitespace, uppercase.
// - Remove any ":<SUFFIX>" where SUFFIX is a known provider suffix.
// - For 6-letter forex symbols, collapse the slash form (EUR/USD -> EURUSD).
namespace ctraderplus::util {

// canonical_pair: returns the canonical spelling of value (empty if blank).
std::string canonicalPair(const std::string &value);

// pair_variants: every equivalent spelling for a DB lookup.
std::vector<std::string> pairVariants(const std::string &value);

bool isCanonical(const std::string &value);

}  // namespace ctraderplus::util
