#pragma once

#include <string>

namespace ctraderplus::controllers {

// Load postman/openapi.yaml once (relative to process cwd).
const std::string &openApiYaml();

extern const char *kSwaggerUiHtml;

}  // namespace ctraderplus::controllers
