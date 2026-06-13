#pragma once

#include <ctime>
#include <optional>
#include <string>
#include <vector>

#include <json/json.h>

namespace ctraderplus::services {

class PostgresService;

struct SubscriptionCheckResult {
    bool allowed = true;
    std::string code;
    std::string message;
};

struct SubscriptionState {
    std::string tier = "none";
    std::optional<std::time_t> trialStartedAt;
    std::optional<std::time_t> tourCompletedAt;
    int trialDaysRemaining = 0;
    bool trialExpired = false;
    bool paywallRequired = false;
    bool requiresPricingIntro = false;
    int dailySms = 0;
    int dailyCalls = 0;
    int trialSmsLimit = 10;
    int trialCallsLimit = 5;
    int freeMaxAlerts = 5;
};

class SubscriptionService {
  public:
    static constexpr int kTrialDays = 14;
    static constexpr int kTrialSmsLimit = 10;
    static constexpr int kTrialCallLimit = 5;
    static constexpr int kFreeMaxAlerts = 5;

    explicit SubscriptionService(PostgresService &pg);

    SubscriptionState getState(const std::string &userId);
    SubscriptionState completeTour(const std::string &userId);
    void dismissPaywall(const std::string &userId);
    bool selectTier(const std::string &userId, const std::string &tier, std::string &statusOut);

    SubscriptionCheckResult canSendNotification(const std::string &userId,
                                                const std::string &channel) const;
    SubscriptionCheckResult canCreateAlert(const std::string &userId,
                                           const std::vector<std::string> &channels,
                                           int activeAlertCount) const;

    bool reserveNotification(const std::string &userId, const std::string &channel);

    Json::Value toBootstrapJson(const SubscriptionState &state) const;

  private:
    PostgresService &pg_;

    int computeTrialDaysRemaining(std::optional<std::time_t> trialStartedAt) const;
    bool isTrialExpired(std::optional<std::time_t> trialStartedAt) const;
};

}  // namespace ctraderplus::services
