#include <iostream>
#include <string>
#include <stdlib.h>
#include <typeinfo>
#include <algorithm>
#include <vector>
#include <fstream>
#include <sstream>

#include "tendisplus/utils/status.h"
#include "tendisplus/server/server_params.h"

namespace tendisplus {
using namespace std;

#define REGISTER_VARS_FULL(str, var, fun, allowDynamicSet) \
    if (typeid(var) == typeid(int) || typeid(var) == typeid(int32_t) \
        || typeid(var) == typeid(uint32_t) || typeid(var) == typeid(uint16_t)) \
        _mapServerParams.insert(make_pair(toLower(str), new IntVar(str, (void*)&var, fun, allowDynamicSet))); \
    else if (typeid(var) == typeid(float)) \
        _mapServerParams.insert(make_pair(toLower(str), new FloatVar(str, (void*)&var, fun, allowDynamicSet))); \
    else if (typeid(var) == typeid(string)) \
        _mapServerParams.insert(make_pair(toLower(str), new StringVar(str, (void*)&var, fun, allowDynamicSet))); \
    else if (typeid(var) == typeid(bool)) \
        _mapServerParams.insert(make_pair(toLower(str), new BoolVar(str, (void*)&var, fun, allowDynamicSet))); \
    else assert(false); // NOTE(takenliu): if other type is needed, change here.

#define REGISTER_VARS(var) REGISTER_VARS_FULL(#var, var, NULL, false)
#define REGISTER_VARS_DIFF_NAME(str, var) REGISTER_VARS_FULL(str, var, NULL, false)
#define REGISTER_VARS_ALLOW_DYNAMIC_SET(var) REGISTER_VARS_FULL(#var, var, NULL, true)

bool logLevelParamCheck(string& v) {
    v = toLower(v);
    if(v == "debug" || v == "verbose" || v == "notice" || v =="warning") {
        return true;
    }
    return false;
};

ServerParams::ServerParams() {
    REGISTER_VARS_DIFF_NAME("bind", bindIp);
    REGISTER_VARS(port);
    REGISTER_VARS_FULL("logLevel", logLevel, logLevelParamCheck, false);
    REGISTER_VARS(logDir);

    REGISTER_VARS_DIFF_NAME("storage", storageEngine);
    REGISTER_VARS_DIFF_NAME("dir", dbPath);
    REGISTER_VARS_DIFF_NAME("dumpdir", dumpPath);
    REGISTER_VARS_DIFF_NAME("rocks.blockcachemb", rocksBlockcacheMB);
    REGISTER_VARS(requirepass);
    REGISTER_VARS(masterauth);
    REGISTER_VARS(pidFile);
    REGISTER_VARS_DIFF_NAME("version-increase", versionIncrease);
    REGISTER_VARS(generalLog);
    // false: For command "set a b", it don't check the type of 
    // "a" and update it directly. It can make set() faster. 
    // Default false. Redis layer can guarantee that it's safe
    REGISTER_VARS_DIFF_NAME("checkkeytypeforsetcmd", checkKeyTypeForSet);

    REGISTER_VARS(chunkSize);
    REGISTER_VARS(kvStoreCount);

    REGISTER_VARS(scanCntIndexMgr);
    REGISTER_VARS(scanJobCntIndexMgr);
    REGISTER_VARS(delCntIndexMgr);
    REGISTER_VARS(delJobCntIndexMgr);
    REGISTER_VARS(pauseTimeIndexMgr);

    REGISTER_VARS_DIFF_NAME("proto-max-bulk-len", protoMaxBulkLen);
    REGISTER_VARS_DIFF_NAME("databases", dbNum);

    REGISTER_VARS(noexpire);
    REGISTER_VARS_ALLOW_DYNAMIC_SET(maxBinlogKeepNum);
    REGISTER_VARS_ALLOW_DYNAMIC_SET(minBinlogKeepSec);

    REGISTER_VARS_ALLOW_DYNAMIC_SET(maxClients);
    REGISTER_VARS_DIFF_NAME("slowlog", slowlogPath);
    REGISTER_VARS_FULL("slowlog-log-slower-than", slowlogLogSlowerThan, NULL, true);
    //REGISTER_VARS(slowlogMaxLen);
    REGISTER_VARS_FULL("slowlog-flush-interval", slowlogFlushInterval, NULL, true);
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
    REGISTER_VARS_ALLOW_DYNAMIC_SET(truncateBinlogIntervalMs);
    REGISTER_VARS_ALLOW_DYNAMIC_SET(truncateBinlogNum);
    REGISTER_VARS(binlogFileSizeMB);
    REGISTER_VARS(binlogFileSecs);
    REGISTER_VARS(binlogHeartbeatSecs);

    REGISTER_VARS(strictCapacityLimit);
    REGISTER_VARS(cacheIndexFilterblocks);
    REGISTER_VARS(maxOpenFiles);
};

ServerParams::~ServerParams() {
    for (auto iter : _mapServerParams) {
        delete iter.second;
    }
}

Status ServerParams::parseFile(const std::string& filename) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        LOG(ERROR) << "open file:" << filename << " failed";
        return {ErrorCodes::ERR_PARSEOPT, ""};
    }
    _setConfFile.insert(filename);
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
                if (toLower(tokens[0]) == "include") {
                    if (_setConfFile.find(tokens[1]) != _setConfFile.end()) {
                        LOG(ERROR) << "parseFile failed, include has recycle: " << tokens[1];
                        return {ErrorCodes::ERR_PARSEOPT, "include has recycle!"};
                    }
                    LOG(INFO) << "parseFile include file: " << tokens[1];
                    auto ret = parseFile(tokens[1]);
                    if (!ret.ok()) {
                        LOG(ERROR) << "parseFile include file failed: " << tokens[1];
                        return ret;
                    }
                }
                else if (!setVar(tokens[0], tokens[1], NULL)) {
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
    _confFile = filename;
    return {ErrorCodes::ERR_OK, ""};
}

bool ServerParams::setVar(string name, string value, string* errinfo, bool force) {
    auto iter = _mapServerParams.find(toLower(name));
    if (iter == _mapServerParams.end()){
        if (errinfo != NULL)
            *errinfo = "not found arg:" + name;
        return false;
    }
    if (!force) {
        LOG(INFO) << "ServerParams setVar dynamic," << name << " : " << value;
    }
    return iter->second->setVar(value, errinfo, force);
}


bool ServerParams::registerOnupdate(string name, funptr ptr){
    auto iter = _mapServerParams.find(toLower(name));
    if (iter == _mapServerParams.end()){
        return false;
    }
    iter->second->setUpdate(ptr);
    return true;
}

string ServerParams::showAll() {
    string ret;
    for (auto iter : _mapServerParams) {
        ret += "  " + iter.second->getName() + ":"+ iter.second->show() + "\n";
    }
    ret.resize(ret.size() - 1);
    return ret;
}

bool ServerParams::showVar(const string& key, string& info) {
    auto iter = _mapServerParams.find(key);
    if (iter == _mapServerParams.end()) {
        return false;
    }
    info = iter->second->show();
    return true;
}
}  // namespace tendisplus

