#pragma once

#include <functional>
#include <string>

#include "core/Config.h"

namespace ctraderplus::services {

// Outbound notification channels. Email -> SendGrid, SMS -> Africa's Talking,
// Call -> Twilio. Each send is async; the callback reports success/failure so
// the dispatcher can retry. Channels with missing credentials are no-ops that
// report success (so they are not retried), mirroring the Python behavior.
class Notifier {
  public:
    explicit Notifier(const core::Config &cfg);

    bool emailEnabled() const;
    bool smsEnabled() const;
    bool callEnabled() const;

    using DoneCb = std::function<void(bool success)>;

    void sendEmail(const std::string &toEmail, const std::string &subject,
                   const std::string &body, DoneCb cb);
    void sendSms(const std::string &toPhone, const std::string &text, DoneCb cb);
    void sendCall(const std::string &toPhone, const std::string &message, DoneCb cb);

    // Build a human-readable alert message body.
    static std::string formatAlertMessage(const std::string &pair, double targetPrice,
                                          double currentPrice, const std::string &condition,
                                          const std::string &customMessage,
                                          const std::string &alertType,
                                          const std::string &timeframe);

  private:
    const core::Config &cfg_;
};

}  // namespace ctraderplus::services
