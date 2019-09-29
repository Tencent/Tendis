#include <iostream>
#include <string>
#include <stdlib.h>
#include <typeinfo>
#include <algorithm>
#include <vector>
#include <fstream>
#include <sstream>

#include "glog/logging.h"
#include "tendisplus/utils/status.h"
#include "tendisplus/server/server_params.h"

namespace tendisplus {
using namespace std;


#define REGISTER_VARS(var) REGISTER_VARS2(#var, var)

#define REGISTER_VARS2(str, var) \
    if (typeid(var) == typeid(int) || typeid(var) == typeid(int32_t) \
        || typeid(var) == typeid(uint32_t) || typeid(var) == typeid(uint16_t)) \
        gMapServerParams.insert(make_pair(toLower(str), new IntVar(str, (void*)&var, NULL))); \
    else if (typeid(var) == typeid(float)) \
        gMapServerParams.insert(make_pair(toLower(str), new FloatVar(str, (void*)&var, NULL))); \
    else if (typeid(var) == typeid(string)) \
        gMapServerParams.insert(make_pair(toLower(str), new StringVar(str, (void*)&var, NULL))); \
    else if (typeid(var) == typeid(bool)) \
        gMapServerParams.insert(make_pair(toLower(str), new BoolVar(str, (void*)&var, NULL))); \
    else assert(false); // NOTE(takenliu): if other type is needed, change here.

ServerParams::ServerParams() {
    REGISTER_VARS2("bind", bindIp);
    REGISTER_VARS(port);
    REGISTER_VARS(logLevel); //todo check
    REGISTER_VARS(logDir);

    REGISTER_VARS2("storage", storageEngine);
    REGISTER_VARS2("dir", dbPath);
    REGISTER_VARS2("dumpdir", dumpPath);
    REGISTER_VARS2("rocks.blockcachemb", rocksBlockcacheMB);
    REGISTER_VARS(requirepass);
    REGISTER_VARS(masterauth);
    REGISTER_VARS(pidFile);
    REGISTER_VARS2("version-increase", versionIncrease);
    REGISTER_VARS(generalLog);
    // false: For command "set a b", it don't check the type of 
    // "a" and update it directly. It can make set() faster. 
    // Default false. Redis layer can guarantee that it's safe
    REGISTER_VARS2("checkkeytypeforsetcmd", checkKeyTypeForSet);

    REGISTER_VARS(chunkSize);
    REGISTER_VARS(kvStoreCount);

    REGISTER_VARS(scanCntIndexMgr);
    REGISTER_VARS(scanJobCntIndexMgr);
    REGISTER_VARS(delCntIndexMgr);
    REGISTER_VARS(delJobCntIndexMgr);
    REGISTER_VARS(pauseTimeIndexMgr);

    REGISTER_VARS2("proto-max-bulk-len", protoMaxBulkLen);
    REGISTER_VARS2("databases", dbNum);

    REGISTER_VARS(noexpire);
    REGISTER_VARS(maxBinlogKeepNum);
    REGISTER_VARS(minBinlogKeepSec);

    REGISTER_VARS(maxClients);
    REGISTER_VARS2("slowlog", slowlogPath);
    REGISTER_VARS2("slowlog-log-slower-than", slowlogLogSlowerThan);
    //REGISTER_VARS(slowlogMaxLen);
    REGISTER_VARS2("slowlog-flush-interval", slowlogFlushInterval);
    REGISTER_VARS(netIoThreadNum);
    REGISTER_VARS(executorThreadNum);

    REGISTER_VARS(binlogRateLimitMB);
    REGISTER_VARS(netBatchSize);
    REGISTER_VARS(netBatchTimeoutSec);
    REGISTER_VARS(timeoutSecBinlogWaitRsp);
    REGISTER_VARS(incrPushThreadnum);
    REGISTER_VARS(fullPushThreadnum);
    REGISTER_VARS(fullReceiveThreadnum);
    REGISTER_VARS(logRecycleThreadnum);
    REGISTER_VARS(truncateBinlogIntervalMs);
    REGISTER_VARS(truncateBinlogNum);
    REGISTER_VARS(binlogFileSizeMB);
    REGISTER_VARS(binlogFileSecs);
    REGISTER_VARS(binlogHeartbeatSecs);

    REGISTER_VARS(strictCapacityLimit);
    REGISTER_VARS(cacheIndexFilterblocks);
    REGISTER_VARS(maxOpenFiles);
};

ServerParams::~ServerParams() {
    for (auto iter : gMapServerParams) {
        delete iter.second;
    }
}

static std::string trim_left(const std::string& str) {
  const std::string pattern = " \f\n\r\t\v";
  auto iter = str.find_first_not_of(pattern);
  return str.substr(iter == string::npos ? 0 : iter);
}

static std::string trim_right(const std::string& str) {
  const std::string pattern = " \f\n\r\t\v";
  auto iter = str.find_last_not_of(pattern);
  return str.substr(0, iter == string::npos ? 0 : iter + 1);
}

static std::string trim(const std::string& str) {
  return trim_left(trim_right(str));
}

Status ServerParams::parseFile(const std::string& filename) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        LOG(ERROR) << "open file:" << filename << " failed";
        return {ErrorCodes::ERR_PARSEOPT, ""};
    }
    std::vector<std::string> tokens;
    std::string line;
    try {
        line.clear();
        while (std::getline(file, line)) {
            line = trim(line);
            if (line.size() == 0 || line[0] == '#') {
                continue;
            }
            std::stringstream ss(line);
            tokens.clear();
            std::string tmp;
            while (std::getline(ss, tmp, ' ')) {
                tokens.emplace_back(tmp);
            }

            if (tokens.size() == 2) {
                if (!setVar(tokens[0], tokens[1], NULL)) {
                    LOG(ERROR) << "err arg:" << tokens[0] << " " << tokens[1];
                    // return {ErrorCodes::ERR_PARSEOPT, ""}; // TODO(takenliu): return error
                }
            } else {
                LOG(ERROR) << "err arg:" << line;
                return {ErrorCodes::ERR_PARSEOPT, ""};
            }
        }
    } catch (const std::exception& ex) {
        LOG(ERROR) << "invalid " << tokens[0] << " config: " << ex.what() << " line:" << line;
        return {ErrorCodes::ERR_PARSEOPT, ""};
    }
    return {ErrorCodes::ERR_OK, ""};
}

bool ServerParams::setVar(string name, string value, string* errinfo) {
    auto iter = gMapServerParams.find(toLower(name));
    if (iter == gMapServerParams.end()){
        if (errinfo != NULL)
            *errinfo = "not found arg:" + name;
        return false;
    }
    return iter->second->set(value);
}


bool ServerParams::registerOnupdate(string name, funptr ptr){
    auto iter = gMapServerParams.find(toLower(name));
    if (iter == gMapServerParams.end()){
        return false;
    }
    iter->second->setUpdate(ptr);
    return true;
}

string ServerParams::showAll() {
    string ret;
    for (auto iter : gMapServerParams) {
        ret += iter.second->show() + "\n";
    }
    ret.resize(ret.size() - 1);
    return ret;
}
}  // namespace tendisplus

