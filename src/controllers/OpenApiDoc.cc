#include "controllers/OpenApiDoc.h"

#include <fstream>
#include <mutex>
#include <sstream>

namespace ctraderplus::controllers {

namespace {
std::once_flag g_once;
std::string g_openApiYaml;

void loadYaml() {
    std::ifstream f("postman/openapi.yaml");
    if (!f.is_open()) {
        g_openApiYaml =
            "openapi: 3.0.3\ninfo:\n  title: cTrader Plus\n  version: 0.0.0\n"
            "paths: {}\n";
        return;
    }
    std::ostringstream ss;
    ss << f.rdbuf();
    g_openApiYaml = ss.str();
}
}  // namespace

const std::string &openApiYaml() {
    std::call_once(g_once, loadYaml);
    return g_openApiYaml;
}

const char *kSwaggerUiHtml = R"HTML(<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>cTrader Plus API Docs</title>
<link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css"/>
<style>body{margin:0;background:#fafafa}</style>
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
<script>
SwaggerUIBundle({
  url: '/openapi.yaml',
  dom_id: '#swagger-ui',
  deepLinking: true,
  presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset],
  layout: 'StandaloneLayout'
});
</script>
</body>
</html>)HTML";

}  // namespace ctraderplus::controllers
