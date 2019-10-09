#ifndef SRC_TENDISPLUS_UTILS_LOG_H_
#define SRC_TENDISPLUS_UTILS_LOG_H_

namespace tendisplus {

#include "glog/logging.h"
#include "tendisplus/server/server_params.h"

void initLog(const std::shared_ptr<ServerParams>& params)
{
    static bool isInitted = false;
    if (isInitted) {
        return;
    }
    isInitted = true;

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
}
}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_UTILS_TEST_UTIL_H_
