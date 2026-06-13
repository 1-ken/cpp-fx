#include "services/SubscriptionService.h"

#include <algorithm>
#include <cmath>

#include "services/PostgresService.h"
#include "util/TimeUtil.h"

namespace ctraderplus::services {

namespace {

bool isPaidTier(const std::string &tier) {
    return tier == "starter" || tier == "trader" || tier == "pro";
}

bool channelRequiresQuota(const std::string &channel) {
    return channel == "sms" || channel == "call";
}

bool channelsIncludeNonSound(const std::vector<std::string> &channels) {
    for (const auto &ch : channels) {
        if (ch != "sound") return true;
    }
    return false;
}

bool channelsIncludeSmsOrCall(const std::vector<std::string> &channels) {
    for (const auto &ch : channels) {
        if (ch == "sms" || ch == "call") return true;
    }
    return false;
}

}  // namespace

SubscriptionService::SubscriptionService(PostgresService &pg) : pg_(pg) {}

int SubscriptionService::computeTrialDaysRemaining(
    std::optional<std::time_t> trialStartedAt) const {
    if (!trialStartedAt) return 0;
    const std::time_t now = std::time(nullptr);
    const double elapsedSec = difftime(now, *trialStartedAt);
    const int elapsedDays = static_cast<int>(std::floor(elapsedSec / 86400.0));
    return std::max(0, kTrialDays - elapsedDays);
}

bool SubscriptionService::isTrialExpired(std::optional<std::time_t> trialStartedAt) const {
    if (!trialStartedAt) return false;
    return computeTrialDaysRemaining(trialStartedAt) <= 0 &&
           difftime(std::time(nullptr), *trialStartedAt) >= kTrialDays * 86400.0;
}

SubscriptionState SubscriptionService::getState(const std::string &userId) {
    pg_.autoDowngradeIfTrialExpired(userId);
    auto row = pg_.getUserStateFull(userId);
    auto usage = pg_.getDailyUsage(userId);

    SubscriptionState state;
    state.tier = row.subscriptionTier.empty() ? "none" : row.subscriptionTier;
    state.trialStartedAt = row.trialStartedAt;
    state.tourCompletedAt = row.tourCompletedAt;
    state.trialDaysRemaining = computeTrialDaysRemaining(row.trialStartedAt);
    state.trialExpired =
        row.trialStartedAt.has_value() &&
        (state.tier == "free" || state.trialDaysRemaining <= 0);
    state.dailySms = usage.smsSent;
    state.dailyCalls = usage.callsMade;
    state.trialSmsLimit = kTrialSmsLimit;
    state.trialCallsLimit = kTrialCallLimit;
    state.freeMaxAlerts = kFreeMaxAlerts;
    state.paywallRequired =
        state.tier == "free" && state.trialExpired && !row.paywallDismissedAt.has_value();
    state.requiresPricingIntro = row.pricingIntroRequired;
    return state;
}

SubscriptionState SubscriptionService::completeTour(const std::string &userId) {
    pg_.completeTour(userId);
    return getState(userId);
}

void SubscriptionService::dismissPaywall(const std::string &userId) {
    pg_.dismissPaywall(userId);
}

bool SubscriptionService::selectTier(const std::string &userId, const std::string &tier,
                                     std::string &statusOut) {
    if (tier == "free") {
        pg_.setSubscriptionTier(userId, "free");
        statusOut = "active";
        return true;
    }
    if (isPaidTier(tier)) {
        statusOut = "coming_soon";
        return true;
    }
    statusOut = "invalid_tier";
    return false;
}

SubscriptionCheckResult SubscriptionService::canSendNotification(
    const std::string &userId, const std::string &channel) const {
    SubscriptionCheckResult result;
    if (!channelRequiresQuota(channel)) {
        return result;
    }

    auto row = pg_.getUserStateFull(userId);
    const std::string tier = row.subscriptionTier.empty() ? "none" : row.subscriptionTier;

    if (row.pricingIntroRequired || (tier == "none" && row.onboardingCompletedAt.has_value())) {
        result.allowed = false;
        result.code = "subscription_required";
        result.message =
            row.pricingIntroRequired
                ? "Complete the product tour to start your free trial."
                : "Complete onboarding to activate your free trial.";
        return result;
    }

    if (tier == "free" || tier == "none") {
        result.allowed = false;
        result.code = "subscription_required";
        result.message = tier == "none"
                             ? "Complete onboarding to activate your free trial."
                             : "SMS and call notifications require a paid plan. Upgrade in Settings.";
        return result;
    }

    if (isPaidTier(tier)) {
        return result;
    }

    if (tier != "trial") {
        result.allowed = false;
        result.code = "subscription_required";
        result.message = "SMS and call notifications are not available on your current plan.";
        return result;
    }

    auto usage = pg_.getDailyUsage(userId);
    if (channel == "sms" && usage.smsSent >= kTrialSmsLimit) {
        result.allowed = false;
        result.code = "trial_limit";
        result.message = "Daily SMS limit reached (10/day). Resets at midnight UTC.";
        return result;
    }
    if (channel == "call" && usage.callsMade >= kTrialCallLimit) {
        result.allowed = false;
        result.code = "trial_limit";
        result.message = "Daily call limit reached (5/day). Resets at midnight UTC.";
        return result;
    }
    return result;
}

SubscriptionCheckResult SubscriptionService::canCreateAlert(
    const std::string &userId, const std::vector<std::string> &channels,
    int activeAlertCount) const {
    SubscriptionCheckResult result;
    auto row = pg_.getUserStateFull(userId);
    const std::string tier = row.subscriptionTier.empty() ? "none" : row.subscriptionTier;

    if (tier == "free") {
        if (activeAlertCount >= kFreeMaxAlerts) {
            result.allowed = false;
            result.code = "subscription_required";
            result.message = "Free plan allows up to 5 active alerts. Upgrade to add more.";
            return result;
        }
        if (channelsIncludeNonSound(channels)) {
            result.allowed = false;
            result.code = "subscription_required";
            result.message = "Free plan supports sound alerts only. Upgrade for SMS, call, or email.";
            return result;
        }
        return result;
    }

    if (row.pricingIntroRequired && channelsIncludeSmsOrCall(channels)) {
        result.allowed = false;
        result.code = "subscription_required";
        result.message = "Complete the product tour to start your free trial.";
        return result;
    }

    if (tier == "none" && channelsIncludeSmsOrCall(channels)) {
        result.allowed = false;
        result.code = "subscription_required";
        result.message = "Complete onboarding to activate your free trial and use SMS/call alerts.";
        return result;
    }

    if (tier == "trial" && activeAlertCount >= kFreeMaxAlerts) {
        // Trial has no alert count cap in plan — only daily SMS/call limits.
    }

    return result;
}

bool SubscriptionService::reserveNotification(const std::string &userId,
                                              const std::string &channel) {
    auto check = canSendNotification(userId, channel);
    if (!check.allowed) return false;
    if (channel == "sms") return pg_.incrementDailySms(userId);
    if (channel == "call") return pg_.incrementDailyCall(userId);
    return true;
}

Json::Value SubscriptionService::toBootstrapJson(const SubscriptionState &state) const {
    Json::Value v;
    v["subscriptionTier"] = state.tier;
    v["trialStartedAt"] = state.trialStartedAt
                              ? Json::Value(util::toIso8601(*state.trialStartedAt))
                              : Json::Value::null;
    v["tourCompletedAt"] = state.tourCompletedAt
                                 ? Json::Value(util::toIso8601(*state.tourCompletedAt))
                                 : Json::Value::null;
    v["trialDaysRemaining"] = state.trialDaysRemaining;
    v["trialExpired"] = state.trialExpired;
    v["paywallRequired"] = state.paywallRequired;
    v["requiresPricingIntro"] = state.requiresPricingIntro;

    Json::Value daily;
    daily["sms"] = state.dailySms;
    daily["smsLimit"] = state.trialSmsLimit;
    daily["calls"] = state.dailyCalls;
    daily["callsLimit"] = state.trialCallsLimit;
    v["dailyUsage"] = daily;

    Json::Value freeLimits;
    freeLimits["maxAlerts"] = state.freeMaxAlerts;
    Json::Value allowedChannels(Json::arrayValue);
    allowedChannels.append("sound");
    freeLimits["allowedChannels"] = allowedChannels;
    v["freeTierLimits"] = freeLimits;

    return v;
}

}  // namespace ctraderplus::services
