#ifndef SRC_TENDISPLUS_SERVER_SERVER_PARAMS_H_
#define SRC_TENDISPLUS_SERVER_SERVER_PARAMS_H_

#include <string>

#include "tendisplus/utils/status.h"

namespace tendisplus {
class ServerParams {
 public:
    ServerParams();
    Status parseFile(const std::string& filename);
    std::string toString() const;
    std::string bindIp;
    uint16_t port;
    std::string logLevel;
    std::string logDir;
};
}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_SERVER_SERVER_PARAMS_H_
