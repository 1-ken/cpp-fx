#pragma once

#include <functional>

namespace ctraderplus::services {
class PostgresService;
}

namespace ctraderplus::core {

bool isDbReadyForAuth();

// Run fast user-facing DB work on the calling thread (bypasses dbExec queue).
bool withPostgres(const std::function<void(services::PostgresService &)> &fn);

// Queue work on the DB worker when one is configured. The function runs inline
// when dbExec is not set (tests and early startup).
void runOnDbWorker(std::function<void()> fn);

}  // namespace ctraderplus::core
