#pragma once

#include <cstdint>
#include <string>

#include "OpenApiCommonMessages.pb.h"

namespace ctraderplus::ctrader {

// Wrap a concrete ProtoOA* message into a ProtoMessage envelope. The payload
// type is read from the message's own `payloadtype()` default. Returns the
// serialized ProtoMessage bytes (without the length prefix).
template <typename T>
std::string wrapMessage(const T &msg, const std::string &clientMsgId = "") {
    ProtoMessage envelope;
    envelope.set_payloadtype(static_cast<uint32_t>(msg.payloadtype()));
    envelope.set_payload(msg.SerializeAsString());
    if (!clientMsgId.empty()) envelope.set_clientmsgid(clientMsgId);
    return envelope.SerializeAsString();
}

// Prepend a 4-byte big-endian length prefix to a serialized ProtoMessage.
std::string frameMessage(const std::string &serializedEnvelope);

// Build a fully framed payload for a concrete message in one step.
template <typename T>
std::string frame(const T &msg, const std::string &clientMsgId = "") {
    return frameMessage(wrapMessage(msg, clientMsgId));
}

}  // namespace ctraderplus::ctrader
