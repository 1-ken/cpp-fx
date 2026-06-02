#include "ctrader/ProtoUtil.h"

namespace ctraderplus::ctrader {

std::string frameMessage(const std::string &serializedEnvelope) {
    uint32_t len = static_cast<uint32_t>(serializedEnvelope.size());
    char prefix[4];
    prefix[0] = static_cast<char>((len >> 24) & 0xFF);
    prefix[1] = static_cast<char>((len >> 16) & 0xFF);
    prefix[2] = static_cast<char>((len >> 8) & 0xFF);
    prefix[3] = static_cast<char>(len & 0xFF);
    std::string out;
    out.reserve(4 + serializedEnvelope.size());
    out.append(prefix, 4);
    out.append(serializedEnvelope);
    return out;
}

}  // namespace ctraderplus::ctrader
