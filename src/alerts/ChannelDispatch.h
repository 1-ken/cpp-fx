#pragma once

#include <iomanip>
#include <sstream>
#include <string>
#include <vector>

namespace ctraderplus::alerts {

struct ChannelReport {
    std::string channel;
    std::string status;
    std::string reason;
    bool retryable = false;
};

inline bool isExternalChannel(const std::string &channel) {
    return channel == "sms" || channel == "call" || channel == "email";
}

// Sound never decides the job. Only a failed external channel is retried.
inline std::vector<std::string> channelsToRetry(const std::vector<ChannelReport> &reports) {
    std::vector<std::string> out;
    for (const auto &report : reports) {
        if (!isExternalChannel(report.channel)) continue;
        if (report.retryable && report.status == "failed") out.push_back(report.channel);
    }
    return out;
}

// One call per user and phone number inside this window.
inline constexpr double kCallWindowSeconds = 60.0;

// True while a call to the same number is being placed, or one was accepted by the
// provider less than windowSeconds ago. A failed call never opens the window.
inline bool callWindowBlocks(bool inFlight, bool everPlaced, double secondsSincePlaced,
                             double windowSeconds = kCallWindowSeconds) {
    if (inFlight) return true;
    if (!everPlaced) return false;
    return secondsSincePlaced < windowSeconds;
}

// Short spoken line for an alert that rides on another alert's call,
// for example "GBPUSD sweep confirm at 1.27345: watch the open".
inline std::string callSnippet(const std::string &pair, const std::string &typeLabel,
                               double price, const std::string &customMessage) {
    std::string type = typeLabel;
    for (auto &ch : type) {
        if (ch == '_') ch = ' ';
    }
    std::ostringstream os;
    os << (pair.empty() ? std::string("alert") : pair);
    if (!type.empty() && type != "price") os << " " << type;
    if (price > 0) os << " at " << std::fixed << std::setprecision(price >= 100 ? 2 : 5) << price;
    if (!customMessage.empty()) os << ": " << customMessage;
    return os.str();
}

// Seconds until a held call group may be released: the rest of the minute after the last
// accepted call. Never negative. A failed call opened no minute, so release is immediate.
inline double callReleaseDelaySeconds(bool everPlaced, double secondsSincePlaced,
                                      double windowSeconds = kCallWindowSeconds) {
    if (!everPlaced) return 0.0;
    const double left = windowSeconds - secondsSincePlaced;
    return left > 0.0 ? left : 0.0;
}

// Spoken summary of the alerts held for the next call. Reads the first maxListed, then
// "and N more, check the app". The bell and in-app still list every alert.
inline std::string callDigestMessage(const std::vector<std::string> &snippets,
                                     std::size_t maxListed = 5) {
    std::string out;
    const std::size_t listed = snippets.size() < maxListed ? snippets.size() : maxListed;
    for (std::size_t i = 0; i < listed; ++i) {
        if (i) out += ". ";
        out += snippets[i];
    }
    if (snippets.size() > listed) {
        out += ". and " + std::to_string(snippets.size() - listed) + " more, check the app";
    }
    return out;
}

// Adds snippet to the call message once.
inline void appendCallSnippet(std::string &message, const std::string &snippet) {
    if (snippet.empty()) return;
    if (message.empty()) {
        message = snippet;
        return;
    }
    if (message.find(snippet) != std::string::npos) return;
    message += ". ";
    message += snippet;
}

}  // namespace ctraderplus::alerts
