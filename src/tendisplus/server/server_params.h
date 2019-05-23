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
    bool versionIncrease;
    bool generalLog;
    // false: For command "set a b", it don't check the type of 
    // "a" and update it directly. It can make set() faster. 
    // Default false. Redis layer can guarantee that it's safe
    bool checkKeyTypeForSet;

    uint32_t chunkSize;
    uint32_t kvStoreCount;

    uint32_t scanCntIndexMgr;
    uint32_t scanJobCntIndexMgr;
    uint32_t delCntIndexMgr;
    uint32_t delJobCntIndexMgr;
    uint32_t pauseTimeIndexMgr;

    uint32_t protoMaxBulkLen;
    uint32_t dbNum;
};
}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_SERVER_SERVER_PARAMS_H_
