#include "core/DbReady.h"

#include <functional>

#include "core/AppContext.h"
#include "services/PostgresService.h"

namespace ctraderplus::core {

bool isDbReadyForAuth() {
    auto &app = AppContext::instance();
    return app.dbMigrationsReady.load() && app.postgres && app.postgres->available();
}

bool withPostgres(const std::function<void(services::PostgresService &)> &fn) {
    auto &app = AppContext::instance();
    if (!isDbReadyForAuth()) return false;
    fn(*app.postgres);
    return true;
}

}  // namespace ctraderplus::core
