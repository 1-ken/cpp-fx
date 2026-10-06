#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "alerts/AlertManager.h"

namespace ctraderplus::alerts {

// In-memory stand-in for alert_events uniqueness: one row per firing, and a
// second delivery of the same alert_id + triggered_at is ignored.
class AlertEventLedger {
  public:
    struct Event {
        std::string alertId;
        std::string triggeredAt;
        std::string pair;
        std::string alertType;
        std::string timeframe;
        double price = 0;
        std::unordered_map<std::string, std::string> delivery;
    };

    struct ListResult {
        bool ok = false;
        std::string error;
        std::vector<Event> events;
        int unreadCount = 0;
    };

    static ListResult failedList(const std::string &error) {
        ListResult result;
        result.ok = false;
        result.error = error;
        return result;
    }

    static bool isEmptyInbox(const ListResult &result) { return result.ok && result.events.empty(); }

    static std::string keyFor(const std::string &alertId, const std::string &triggeredAt) {
        return alertId + "|" + triggeredAt;
    }

    bool record(const TriggeredAlert &t) {
        if (!t.alert.triggeredAt || t.alert.triggeredAt->empty() || t.alert.id.empty()) return false;
        const std::string key = keyFor(t.alert.id, *t.alert.triggeredAt);
        if (!seen_.insert(key).second) return false;
        Event ev;
        ev.alertId = t.alert.id;
        ev.triggeredAt = *t.alert.triggeredAt;
        ev.pair = t.alert.pair;
        ev.alertType = t.alert.alertType;
        ev.timeframe = t.timeframe;
        ev.price = t.currentPrice;
        events_.push_back(std::move(ev));
        return true;
    }

    bool recordDelivery(const std::string &alertId, const std::string &triggeredAt,
                        const std::string &channel, const std::string &status) {
        const std::string key = keyFor(alertId, triggeredAt);
        for (auto &event : events_) {
            if (keyFor(event.alertId, event.triggeredAt) != key) continue;
            event.delivery[channel] = status;
            return true;
        }
        return false;
    }

    std::size_t size() const { return events_.size(); }
    const std::vector<Event> &events() const { return events_; }

  private:
    std::unordered_set<std::string> seen_;
    std::vector<Event> events_;
};

}  // namespace ctraderplus::alerts
