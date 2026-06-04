#include "services/Notifier.h"

#include <algorithm>
#include <cctype>
#include <sstream>

#include <drogon/HttpClient.h>
#include <drogon/HttpRequest.h>
#include <drogon/utils/Utilities.h>
#include <json/json.h>
#include <trantor/utils/Logger.h>

#include "util/TimeUtil.h"

using namespace drogon;

namespace ctraderplus::services {

namespace {

std::string escapeXml(const std::string &s) {
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        switch (c) {
            case '&': out += "&amp;"; break;
            case '<': out += "&lt;"; break;
            case '>': out += "&gt;"; break;
            case '"': out += "&quot;"; break;
            case '\'': out += "&apos;"; break;
            default: out += c;
        }
    }
    return out;
}

std::string upperTypeLabel(const std::string &alertType) {
    std::string label = alertType.empty() ? "price" : alertType;
    std::transform(label.begin(), label.end(), label.begin(),
                   [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    return label;
}

struct AlertMessageFields {
    std::string header;
    std::string pair;
    std::string type;
    std::string conditionLine;
    std::string triggeredKenya;
    std::string customMessage;
    std::string timeframeLine;
};

AlertMessageFields buildAlertFields(const std::string &pair, double targetPrice,
                                    double currentPrice, const std::string &condition,
                                    const std::string &customMessage,
                                    const std::string &alertType,
                                    const std::string &timeframe,
                                    const std::string &triggeredAtIso) {
    AlertMessageFields f;
    f.type = alertType.empty() ? "price" : alertType;
    f.header = upperTypeLabel(f.type) + " ALERT";
    f.pair = pair;
    std::ostringstream cond;
    cond << condition << " " << targetPrice;
    f.conditionLine = cond.str();
    (void)currentPrice;
    f.triggeredKenya =
        util::formatKenyaDateTime(triggeredAtIso);
    if (f.triggeredKenya.empty()) {
        f.triggeredKenya = util::formatKenyaDateTime(util::nowIso8601());
    }
    f.customMessage = customMessage;
    if (!timeframe.empty()) f.timeframeLine = timeframe;
    return f;
}

}  // namespace

Notifier::Notifier(const core::Config &cfg) : cfg_(cfg) {}

bool Notifier::emailEnabled() const { return !cfg_.sendgridApiKey.empty(); }
bool Notifier::smsEnabled() const {
    return !cfg_.smsGateUsername.empty() && !cfg_.smsGatePassword.empty();
}
bool Notifier::callEnabled() const {
    return !cfg_.twilioAccountSid.empty() && !cfg_.twilioAuthToken.empty() &&
           !cfg_.twilioFromNumber.empty();
}

std::string Notifier::formatAlertSubject(const std::string &pair,
                                         const std::string &alertType) {
    return upperTypeLabel(alertType) + " ALERT: " + pair;
}

std::string Notifier::formatAlertSms(const std::string &pair, double targetPrice,
                                     double currentPrice, const std::string &condition,
                                     const std::string &customMessage,
                                     const std::string &alertType,
                                     const std::string &timeframe,
                                     const std::string &triggeredAtIso) {
    AlertMessageFields f =
        buildAlertFields(pair, targetPrice, currentPrice, condition, customMessage,
                         alertType, timeframe, triggeredAtIso);
    std::ostringstream os;
    os << f.header << " | PAIR: " << f.pair << " | TYPE: " << f.type
       << " | CONDITION: " << f.conditionLine << " | TRIGGERED: " << f.triggeredKenya;
    if (!f.timeframeLine.empty()) os << " | TIMEFRAME: " << f.timeframeLine;
    if (!f.customMessage.empty()) os << " | MESSAGE: " << f.customMessage;
    return os.str();
}

std::string Notifier::formatAlertEmailBody(const std::string &pair, double targetPrice,
                                           double currentPrice, const std::string &condition,
                                           const std::string &customMessage,
                                           const std::string &alertType,
                                           const std::string &timeframe,
                                           const std::string &triggeredAtIso) {
    AlertMessageFields f =
        buildAlertFields(pair, targetPrice, currentPrice, condition, customMessage,
                         alertType, timeframe, triggeredAtIso);
    std::ostringstream os;
    os << f.header << "\n\n"
       << "PAIR: " << f.pair << "\n"
       << "TYPE: " << f.type << "\n"
       << "CONDITION: " << f.conditionLine << "\n"
       << "TRIGGERED: " << f.triggeredKenya << "\n";
    if (!f.timeframeLine.empty()) os << "TIMEFRAME: " << f.timeframeLine << "\n";
    if (!f.customMessage.empty()) os << "MESSAGE: " << f.customMessage << "\n";
    return os.str();
}

void Notifier::sendEmail(const std::string &toEmail, const std::string &subject,
                         const std::string &body, DoneCb cb) {
    if (!emailEnabled()) {
        LOG_WARN << "SendGrid email skipped: not configured";
        cb(false);
        return;
    }
    if (toEmail.empty()) {
        LOG_WARN << "SendGrid email skipped: empty recipient";
        cb(false);
        return;
    }
    auto client = HttpClient::newHttpClient("https://api.sendgrid.com");
    Json::Value payload;
    Json::Value personalization;
    Json::Value toArr(Json::arrayValue);
    Json::Value toObj;
    toObj["email"] = toEmail;
    toArr.append(toObj);
    personalization["to"] = toArr;
    Json::Value personalizations(Json::arrayValue);
    personalizations.append(personalization);
    payload["personalizations"] = personalizations;
    Json::Value from;
    from["email"] = cfg_.sendgridFromEmail;
    payload["from"] = from;
    payload["subject"] = subject;
    Json::Value content(Json::arrayValue);
    Json::Value c;
    c["type"] = "text/plain";
    c["value"] = body;
    content.append(c);
    payload["content"] = content;

    auto req = HttpRequest::newHttpJsonRequest(payload);
    req->setMethod(Post);
    req->setPath("/v3/mail/send");
    req->addHeader("Authorization", "Bearer " + cfg_.sendgridApiKey);
    client->sendRequest(req, [cb](ReqResult r, const HttpResponsePtr &resp) {
        bool ok = (r == ReqResult::Ok && resp &&
                   resp->getStatusCode() >= k200OK && resp->getStatusCode() < k300MultipleChoices);
        if (!ok) LOG_WARN << "SendGrid email failed";
        cb(ok);
    });
}

void Notifier::sendSms(const std::string &toPhone, const std::string &text, DoneCb cb) {
    if (!smsEnabled()) {
        LOG_WARN << "SMS Gate send skipped: SMS_GATE credentials not configured";
        cb(false);
        return;
    }
    if (toPhone.empty()) {
        LOG_WARN << "SMS Gate send skipped: empty phone number";
        cb(false);
        return;
    }
    auto client = HttpClient::newHttpClient("https://api.sms-gate.app");
    Json::Value payload;
    payload["message"] = text;
    Json::Value phones(Json::arrayValue);
    phones.append(toPhone);
    payload["phoneNumbers"] = phones;

    auto req = HttpRequest::newHttpJsonRequest(payload);
    req->setMethod(Post);
    req->setPath("/3rdparty/v1/message");
    std::string auth = drogon::utils::base64Encode(
        reinterpret_cast<const unsigned char *>(
            (cfg_.smsGateUsername + ":" + cfg_.smsGatePassword).c_str()),
        cfg_.smsGateUsername.size() + cfg_.smsGatePassword.size() + 1);
    req->addHeader("Authorization", "Basic " + auth);

    client->sendRequest(req, [cb, toPhone](ReqResult r, const HttpResponsePtr &resp) {
        bool ok = (r == ReqResult::Ok && resp &&
                   resp->getStatusCode() >= k200OK && resp->getStatusCode() < k300MultipleChoices);
        if (!ok) {
            int code = resp ? static_cast<int>(resp->getStatusCode()) : -1;
            std::string body = resp ? std::string(resp->getBody()) : "";
            LOG_WARN << "SMS Gate send failed phone=" << toPhone << " status=" << code
                     << " body=" << body;
        }
        cb(ok);
    });
}

void Notifier::sendCall(const std::string &toPhone, const std::string &message, DoneCb cb) {
    if (!callEnabled()) {
        LOG_WARN << "Twilio call skipped: TWILIO credentials not configured";
        cb(false);
        return;
    }
    if (toPhone.empty()) {
        LOG_WARN << "Twilio call skipped: empty phone number";
        cb(false);
        return;
    }
    auto client = HttpClient::newHttpClient("https://api.twilio.com");
    auto req = HttpRequest::newHttpRequest();
    req->setMethod(Post);
    req->setPath("/2010-04-01/Accounts/" + cfg_.twilioAccountSid + "/Calls.json");
    std::string auth = drogon::utils::base64Encode(
        reinterpret_cast<const unsigned char *>(
            (cfg_.twilioAccountSid + ":" + cfg_.twilioAuthToken).c_str()),
        cfg_.twilioAccountSid.size() + cfg_.twilioAuthToken.size() + 1);
    req->addHeader("Authorization", "Basic " + auth);
    req->setContentTypeCode(CT_APPLICATION_X_FORM);
    std::string twiml = "<Response><Say>" + escapeXml(message) + "</Say></Response>";
    std::ostringstream form;
    form << "To=" << drogon::utils::urlEncode(toPhone)
         << "&From=" << drogon::utils::urlEncode(cfg_.twilioFromNumber)
         << "&Twiml=" << drogon::utils::urlEncode(twiml);
    req->setBody(form.str());
    client->sendRequest(req, [cb, toPhone](ReqResult r, const HttpResponsePtr &resp) {
        bool ok = (r == ReqResult::Ok && resp && resp->getStatusCode() < k300MultipleChoices);
        if (!ok) {
            int code = resp ? static_cast<int>(resp->getStatusCode()) : -1;
            std::string body = resp ? std::string(resp->getBody()) : "";
            LOG_WARN << "Twilio call failed phone=" << toPhone << " status=" << code
                     << " body=" << body;
        }
        cb(ok);
    });
}

}  // namespace ctraderplus::services
