#pragma once

#include <functional>

namespace ctraderplus::services {
class PostgresService;
}

namespace ctraderplus::core {

bool isDbReadyForAuth();

// Run fast user-facing DB work on the calling thread (bypasses dbExec queue).
bool withPostgres(const std::function<void(services::PostgresService &)> &fn);

}  // namespace ctraderplus::core
