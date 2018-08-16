#include <iostream>
#include <unistd.h>

#include "tendisplus/server/server_params.h"
#include "tendisplus/server/server_entry.h"
#include "glog/logging.h"

namespace tendisplus {

static ServerEntry *gServer = nullptr;

void usage() {
    std::cout<< "./bin configfile" << std::endl;
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

int main(int argc, char *argv[]) {
    if (argc != 2) {
        usage();
        return 0;
    }
    if (daemon(1 /*nochdir*/, 0 /*noclose*/) < 0) {
        // NOTE(deyukong): it should rarely fail.
        // but if code reaches here, cerr may have been redirected to
        // /dev/null and nothing printed.
        LOG(FATAL) << "daemonlize failed:" << errno;
    }
    auto params = std::make_shared<ServerParams>();
    auto s = params->parseFile(argv[1]);
    if (!s.ok()) {
        LOG(FATAL) << "parse config failed:" << s.toString();
    }
    gServer = new ServerEntry();
    s = gServer->startup(params);
    if (!s.ok()) {
        LOG(FATAL) << "server startup failed:" << s.toString();
    }
    setupSignals();
    waitForExit();
    LOG(INFO) << "server exits";
}

}  // namespace tendisplus
