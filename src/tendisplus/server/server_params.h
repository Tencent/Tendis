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

    bool noexpire;
    uint32_t maxBinlogKeepNum;
    uint32_t minBinlogKeepSec;

    uint32_t maxClients;
    std::string slowlogPath;
    uint32_t slowlogLogSlowerThan;
    uint32_t slowlogMaxLen;
    uint32_t slowlogFlushInterval;
    uint32_t netIoThreadNum;
    uint32_t executorThreadNum;

    uint32_t binlogRateLimitMB;
    uint32_t timeoutSecBinlogSize1; // 2
    uint32_t timeoutSecBinlogSize2; // 10
    uint32_t timeoutSecBinlogSize3; // 10000
    uint32_t timeoutSecBinlogFileList; // 1000
    uint32_t timeoutSecBinlogFilename; // 10
    uint32_t timeoutSecBinlogBatch; // 100
    uint32_t timeoutSecBinlogWaitRsp; // 10
    uint32_t incrPushThreadnum;
    uint32_t fullPushThreadnum;
    uint32_t fullReceiveThreadnum;
    uint32_t logRecycleThreadnum;
    uint32_t truncateBinlogIntervalMs;
    uint32_t truncateBinlogNum;
    uint32_t binlogFileSizeMB;
    uint32_t binlogFileSecs;
    uint32_t binlogHeartbeatSecs;

    bool strictCapacityLimit;
    bool cacheIndexFilterblocks;
    int32_t maxOpenFiles;
private:
    bool caseEqual(const std::string& l, const std::string& r);
};
}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_SERVER_SERVER_PARAMS_H_
