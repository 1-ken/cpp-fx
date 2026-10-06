#pragma once

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

}  // namespace ctraderplus::alerts
