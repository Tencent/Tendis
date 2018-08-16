#include <iostream>
#include <unistd.h>
#include <utility>
#include <memory>

#include "tendisplus/server/server_params.h"
#include "tendisplus/server/server_entry.h"
#include "glog/logging.h"

namespace tendisplus {

static std::shared_ptr<ServerEntry> gServer(nullptr);

void usage() {
    std::cout<< "./tendisplus configfile" << std::endl;
}


static void shutdown(int sigNum) {
    LOG(INFO) << "signal:" << sigNum << " caught, begin shutdown server";
    assert(gServer);
    gServer->stop();
}

static void waitForExit() {
    assert(gServer);
    gServer->waitStopComplete();
}

static void setupSignals() {
    struct sigaction ignore;
    memset(&ignore, 0, sizeof(ignore));
    ignore.sa_handler = SIG_IGN;
    sigemptyset(&ignore.sa_mask);

    assert(sigaction(SIGHUP, &ignore, nullptr) == 0);
    assert(sigaction(SIGUSR2, &ignore, nullptr) == 0);
    assert(sigaction(SIGPIPE, &ignore, nullptr) == 0);

    struct sigaction exits;
    memset(&exits, 0, sizeof(exits));
    exits.sa_handler = shutdown;
    sigemptyset(&ignore.sa_mask);

    assert(sigaction(SIGTERM, &exits, nullptr) == 0);
    assert(sigaction(SIGINT, &exits, nullptr) == 0);
}

}  // namespace tendisplus

int main(int argc, char *argv[]) {
    using namespace tendisplus;
    if (argc != 2) {
        usage();
        return 0;
    }
    auto params = std::make_shared<ServerParams>();
    auto s = params->parseFile(argv[1]);
    if (!s.ok()) {
        LOG(FATAL) << "parse config failed:" << s.toString();
    } else {
        LOG(INFO) << "start server with cfg:" << params->toString();
    }

    if (daemon(1 /*nochdir*/, 0 /*noclose*/) < 0) {
        // NOTE(deyukong): it should rarely fail.
        // but if code reaches here, cerr may have been redirected to
        // /dev/null and nothing printed.
        LOG(FATAL) << "daemonlize failed:" << errno;
    }

    // Log messages at or above this level. Again, the numbers of severity
    // levels INFO, WARNING, ERROR, and FATAL are 0, 1, 2, and 3,
    // respectively. refer to http://rpg.ifi.uzh.ch/docs/glog.html
    FLAGS_minloglevel = 0;
    if (params->logLevel == "debug" || params->logLevel == "verbose") {
        FLAGS_v = 1;
    } else {
        FLAGS_v = 0;
    }
    if (params->logDir != "") {
        FLAGS_log_dir = params->logDir;
    }
    ::google::InitGoogleLogging("tendisplus");

    gServer = std::make_shared<ServerEntry>();
    s = gServer->startup(params);
    if (!s.ok()) {
        LOG(FATAL) << "server startup failed:" << s.toString();
    }
    setupSignals();
    waitForExit();
    LOG(INFO) << "server exits";
}


