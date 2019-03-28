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

    std::string storageEngine;
    std::string dbPath;
    std::string dumpPath;
    uint32_t  rocksBlockcacheMB;
    std::string requirepass;
    std::string masterauth;
    std::string pidFile;

    uint32_t scanCntIndexMgr;
    uint32_t scanJobCntIndexMgr;
    uint32_t delCntIndexMgr;
    uint32_t delJobCntIndexMgr;
    uint32_t pauseTimeIndexMgr;
};
}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_SERVER_SERVER_PARAMS_H_
