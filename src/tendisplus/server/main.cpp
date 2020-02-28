#include <unistd.h>
#include <fstream>
#include <iostream>
#include <utility>
#include <memory>
#include <string>

#include "tendisplus/server/server_params.h"
#include "tendisplus/server/server_entry.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/portable.h"
#include "glog/logging.h"

namespace tendisplus {

static std::shared_ptr<ServerEntry> gServer(nullptr);

std::shared_ptr<ServerEntry> getGlobalServer() {
    return gServer;
}

}  // namespace tendisplus

static void shutdown(int sigNum) {
    LOG(INFO) << "signal:" << sigNum << " caught, begin shutdown server";
    INVARIANT(tendisplus::gServer != nullptr);
    tendisplus::gServer->handleShutdownCmd();
}

static void waitForExit() {
    INVARIANT(tendisplus::gServer != nullptr);
    tendisplus::gServer->waitStopComplete();
}

static void setupSignals() {
#ifndef _WIN32
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
#endif  // !_WIN32
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
        std::cout << "parse config failed:" << s.toString();
        //LOG(FATAL) << "parse config failed:" << s.toString();
        return -1;
    } else {
        std::cout << "start server with cfg:\n" << params->showAll() << std::endl;
        //LOG(INFO) << "start server with cfg:" << params->toString();
    }

    INVARIANT(sizeof(double) == 8);

#ifndef _WIN32
    if (daemon(1 /*nochdir*/, 0 /*noclose*/) < 0) {
        // NOTE(deyukong): it should rarely fail.
        // but if code reaches here, cerr may have been redirected to
        // /dev/null and nothing printed.
        LOG(FATAL) << "daemonlize failed:" << errno;
    }
#endif

    FLAGS_minloglevel = 0;
    if (params->logLevel == "debug" || params->logLevel == "verbose") {
        FLAGS_v = 1;
    } else {
        FLAGS_v = 0;
    }

    if (params->logDir != "") {
        FLAGS_log_dir = params->logDir;
        std::cout << "glog dir:" << FLAGS_log_dir << std::endl;
        if (!tendisplus::filesystem::exists(FLAGS_log_dir)) {
            std::error_code ec;
            if (!tendisplus::filesystem::create_directories(
                                    FLAGS_log_dir, ec)) {
                LOG(WARNING) << " create log path failed: " << ec.message();
            }
        }
    }

    FLAGS_logbufsecs = 1;
    ::google::InitGoogleLogging("tendisplus");
#ifndef _WIN32
    ::google::InstallFailureSignalHandler();
    ::google::InstallFailureWriter([](const char *data, int size) {
        LOG(ERROR) << "Failure:" << std::string(data, size);
        google::FlushLogFiles(google::INFO);
        google::FlushLogFiles(google::WARNING);
        google::FlushLogFiles(google::ERROR);
        google::FlushLogFiles(google::FATAL);
    });
#endif

    tendisplus::gServer = std::make_shared<tendisplus::ServerEntry>(params);
    s = tendisplus::gServer->startup(params);
    if (!s.ok()) {
        LOG(FATAL) << "server startup failed:" << s.toString();
    }
    setupSignals();

    // pid file
    std::ofstream pidfile(params->pidFile);
    pidfile << getpid();
    pidfile.close();

    waitForExit();
    LOG(INFO) << "server exits";

    remove(params->pidFile.c_str());
    return 0;
}
