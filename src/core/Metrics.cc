#include "core/Metrics.h"

#include <sstream>

namespace ctraderplus::core {

Metrics &Metrics::instance() {
    static Metrics m;
    return m;
}

std::string Metrics::jsonLine() const {
    std::ostringstream o;
    o << "{\"metric\":\"ctraderplus\""
      << ",\"ws_broadcasts\":" << wsBroadcasts.load()
      << ",\"ws_broadcast_duration_us\":" << wsBroadcastDurationUs.load()
      << ",\"ws_clients\":" << wsClients.load()
      << ",\"ws_dropped_slow\":" << wsDroppedSlow.load()
      << ",\"alert_eval_count\":" << alertEvalCount.load()
      << ",\"alert_eval_duration_us\":" << alertEvalDurationUs.load()
      << ",\"alert_trigger_total\":" << alertTriggerTotal.load()
      << ",\"ctrader_inflight_requests\":" << ctraderInflight.load()
      << ",\"pg_query_count\":" << pgQueryCount.load()
      << ",\"pg_query_duration_us\":" << pgQueryDurationUs.load()
      << ",\"notification_queue_depth\":" << notificationQueueDepth.load()
      << ",\"notification_dlq_depth\":" << notificationDlqDepth.load()
      << ",\"notification_retry_total\":" << notificationRetryTotal.load()
      << ",\"snapshot_seq\":" << snapshotSeq.load() << "}";
    return o.str();
}

}  // namespace ctraderplus::core
