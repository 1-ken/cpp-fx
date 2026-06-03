#pragma once

#include <drogon/orm/DbClient.h>

namespace ctraderplus::services {

// Applies pending schema migrations. Returns the highest applied version.
// Throws on failure.
int runMigrations(const drogon::orm::DbClientPtr &client);

}  // namespace ctraderplus::services
