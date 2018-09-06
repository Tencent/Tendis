#include <unistd.h>
#include <iostream>
#include <utility>
#include <memory>

#include "tendisplus/server/server_params.h"
#include "tendisplus/server/server_entry.h"
#include "tendisplus/utils/invariant.h"
#include "glog/logging.h"

namespace tendisplus {

static std::shared_ptr<ServerEntry> gServer(nullptr);

}  // namespace tendisplus

static void shutdown(int sigNum) {
    LOG(INFO) << "signal:" << sigNum << " caught, begin shutdown server";
    INVARIANT(tendisplus::gServer != nullptr);
    tendisplus::gServer->stop();
}

static void waitForExit() {
    INVARIANT(tendisplus::gServer != nullptr);
    tendisplus::gServer->waitStopComplete();
}

static void setupSignals() {
    struct sigaction ignore;
    memset(&ignore, 0, sizeof(ignore));
    ignore.sa_handler = SIG_IGN;
    sigemptyset(&ignore.sa_mask);

    INVARIANT(sigaction(SIGHUP, &ignore, nullptr) == 0);
    INVARIANT(sigaction(SIGUSR2, &ignore, nullptr) == 0);
    INVARIANT(sigaction(SIGPIPE, &ignore, nullptr) == 0);

    struct sigaction exits;
    memset(&exits, 0, sizeof(exits));
    exits.sa_handler = shutdown;
    sigemptyset(&ignore.sa_mask);

    INVARIANT(sigaction(SIGTERM, &exits, nullptr) == 0);
    INVARIANT(sigaction(SIGINT, &exits, nullptr) == 0);
}

static void usage() {
    std::cout<< "./tendisplus [configfile]" << std::endl;
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        usage();
        return 0;
    }
    auto params = std::make_shared<tendisplus::ServerParams>();
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
    FLAGS_logbufsecs = 1;
    ::google::InitGoogleLogging("tendisplus");
    ::google::InstallFailureSignalHandler();
    ::google::InstallFailureWriter([](const char *data, int size) {
        LOG(ERROR) << "Failure:" << std::string(data, size);
        google::FlushLogFiles(google::INFO);
        google::FlushLogFiles(google::WARNING);
        google::FlushLogFiles(google::ERROR);
        google::FlushLogFiles(google::FATAL);
    });
    tendisplus::gServer = std::make_shared<tendisplus::ServerEntry>();
    s = tendisplus::gServer->startup(params);
    if (!s.ok()) {
        LOG(FATAL) << "server startup failed:" << s.toString();
    }
    setupSignals();
    waitForExit();
    LOG(INFO) << "server exits";
}


