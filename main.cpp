#include <sstream>

#include <httpserver.hpp>

#include "dbGate.h"
#include "httpResource.h"

#define PORT 8080
#define CONN_TIMEOUT 180
#define MAX_THREADS 4

extern logger LOGGER;

int main() {
    LOGGER.info("[MAIN] Initializing database...");
    auto *db = new dbGate{};

    LOGGER.info("[MAIN] Initializing web server...");
    httpserver::webserver ws = httpserver::create_webserver(PORT)
            .connection_timeout(CONN_TIMEOUT)
            .start_method(httpserver::http::http_utils::INTERNAL_SELECT)
            .max_threads(MAX_THREADS)
            .debug();

    dbQueryResource dqr{db};
    ws.register_resource("/", &dqr);

    LOGGER.info("[MAIN] Web server started. Listening on port " + std::to_string(PORT) + ".");
    ws.start(true);

    return 0;
}
