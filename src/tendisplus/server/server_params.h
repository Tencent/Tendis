#ifndef SRC_TENDISPLUS_SERVER_SERVER_PARAMS_H_
#define SRC_TENDISPLUS_SERVER_SERVER_PARAMS_H_

#include "tendisplus/utils/status.h"

namespace tendisplus {
class ServerParams {
 public:
    ServerParams();
    Status parseFile(const std::string& filename);
    std::string bindIp;
    std::string logLevel;
    std::string logFile;
    uint16_t port;
};
}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_SERVER_SERVER_PARAMS_H_
