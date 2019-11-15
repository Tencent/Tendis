#include <iostream>
#include <string>
#include <stdlib.h>
#include <typeinfo>
#include <algorithm>
#include <vector>
#include <fstream>
#include <sstream>

#include "tendisplus/utils/status.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/server/server_params.h"
#include "tendisplus/utils/invariant.h"

namespace tendisplus {
using namespace std;

string gRenameCmdList = "";
string gMappingCmdList = "";

#define REGISTER_VARS_FULL(str, var, checkfun, prefun, allowDynamicSet) \
    if (typeid(var) == typeid(int) || typeid(var) == typeid(int32_t) \
        || typeid(var) == typeid(uint32_t) || typeid(var) == typeid(uint16_t)) \
        _mapServerParams.insert(make_pair(toLower(str), new IntVar(str, (void*)&var, checkfun, prefun, allowDynamicSet))); \
    else if (typeid(var) == typeid(int64_t) || typeid(var) == typeid(uint64_t)) \
        _mapServerParams.insert(make_pair(toLower(str), new Int64Var(str, (void*)&var, checkfun, prefun, allowDynamicSet))); \
    else if (typeid(var) == typeid(float)) \
        _mapServerParams.insert(make_pair(toLower(str), new FloatVar(str, (void*)&var, checkfun, prefun, allowDynamicSet))); \
    else if (typeid(var) == typeid(string)) \
        _mapServerParams.insert(make_pair(toLower(str), new StringVar(str, (void*)&var, checkfun, prefun, allowDynamicSet))); \
    else if (typeid(var) == typeid(bool)) \
        _mapServerParams.insert(make_pair(toLower(str), new BoolVar(str, (void*)&var, checkfun, prefun, allowDynamicSet))); \
    else INVARIANT(0); // NOTE(takenliu): if other type is needed, change here.

#define REGISTER_VARS(var) REGISTER_VARS_FULL(#var, var, NULL, NULL, false)
#define REGISTER_VARS_DIFF_NAME(str, var) REGISTER_VARS_FULL(str, var, NULL, NULL, false)
#define REGISTER_VARS_ALLOW_DYNAMIC_SET(var) REGISTER_VARS_FULL(#var, var, NULL, NULL, true)
#define REGISTER_VARS_DIFF_NAME_DYNAMIC(str, var) REGISTER_VARS_FULL(str, var, NULL, NULL, true)

bool logLevelParamCheck(const string& val) {
    auto v = toLower(val);
    if (v == "debug" || v == "verbose" || v == "notice" || v == "warning") {
        return true;
    }
    return false;
};

bool compressTypeParamCheck(const string& val) {
    auto v = toLower(val);
    if (v == "snappy" || v == "lz4" || v == "none") {
        return true;
    }
    return false;
};

string removeQuotes(const string& v) {
    if (v.size() < 2) {
        return v;
    }

    auto tmp = v;
    if (tmp[0] == '\"' && tmp[tmp.size() - 1] == '\"') {
        tmp = tmp.substr(1, tmp.size() - 2);
    }
    return tmp;
}

string  removeQuotesAndToLower(const string& v) {
    auto tmp = toLower(v);
    if (tmp.size() < 2) {
        return tmp;
    }

    if (tmp[0] == '\"' && tmp[tmp.size() - 1] == '\"') {
        tmp = tmp.substr(1, tmp.size() - 2);
    }
    return tmp;
}

ServerParams::ServerParams() {
    REGISTER_VARS_DIFF_NAME("bind", bindIp);
    REGISTER_VARS(port);
    REGISTER_VARS_FULL("logLevel", logLevel, logLevelParamCheck, removeQuotesAndToLower, false);
    REGISTER_VARS(logDir);

    REGISTER_VARS_DIFF_NAME("storage", storageEngine);
    REGISTER_VARS_DIFF_NAME("dir", dbPath);
    REGISTER_VARS_DIFF_NAME("dumpdir", dumpPath);
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
    REGISTER_VARS_ALLOW_DYNAMIC_SET(slaveBinlogKeepNum);

    REGISTER_VARS_ALLOW_DYNAMIC_SET(maxClients);
    REGISTER_VARS_DIFF_NAME("slowlog", slowlogPath);
    REGISTER_VARS_DIFF_NAME_DYNAMIC("slowlog-log-slower-than", slowlogLogSlowerThan);
    REGISTER_VARS_DIFF_NAME_DYNAMIC("slowlog-max-len", slowlogMaxLen);
    REGISTER_VARS_DIFF_NAME_DYNAMIC("slowlog-flush-interval", slowlogFlushInterval);
    REGISTER_VARS_DIFF_NAME_DYNAMIC("slowlog-file-enabled", slowlogFileEnabled);
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

    REGISTER_VARS_ALLOW_DYNAMIC_SET(keysDefaultLimit);
    REGISTER_VARS_ALLOW_DYNAMIC_SET(lockWaitTimeOut);

    REGISTER_VARS_DIFF_NAME("rocks.blockcachemb", rocksBlockcacheMB);
    REGISTER_VARS_DIFF_NAME("rocks.blockcache_strict_capacity_limit", rocksStrictCapacityLimit);
    REGISTER_VARS_DIFF_NAME_DYNAMIC("rocks.disable_wal", rocksDisableWAL);
    REGISTER_VARS_DIFF_NAME_DYNAMIC("rocks.flush_log_at_trx_commit", rocksFlushLogAtTrxCommit);
    REGISTER_VARS_DIFF_NAME("rocks.wal_dir", rocksWALDir);

    REGISTER_VARS_FULL("rocks.compress_type", rocksCompressType, compressTypeParamCheck, removeQuotesAndToLower, false);
    REGISTER_VARS_DIFF_NAME("rocks.level0_compress_enabled", level0Compress);
    REGISTER_VARS_DIFF_NAME("rocks.level1_compress_enabled", level1Compress);
    REGISTER_VARS(levelCompactionDynamicLevelBytes);

    REGISTER_VARS(migrateSenderThreadnum);
    REGISTER_VARS(migrateClearThreadnum);
    REGISTER_VARS(migrateReceiveThreadnum);
    REGISTER_VARS(migrateCheckThreadnum);
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

            if (tokens.size() == 3 && toLower(tokens[0]) == "rename-command") {
                gRenameCmdList += "," + tokens[1] + " " + tokens[2];
            } else if (tokens.size() == 3 && toLower(tokens[0]) == "mapping-command") {
                gMappingCmdList += "," + tokens[1] + " " + tokens[2];
            }
            else if (tokens.size() == 2) {
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
                } else if (!setVar(tokens[0], tokens[1], NULL)) {
                    LOG(ERROR) << "err arg:" << tokens[0] << " " << tokens[1];
                    return {ErrorCodes::ERR_PARSEOPT,
                        "invalid parameter " + tokens[0] + " value: " + tokens[1]}; 
                }
            } else {
                LOG(ERROR) << "err arg:" << line;
                return {ErrorCodes::ERR_PARSEOPT, "err arg: " + line};
            }
        }
    } catch (const std::exception& ex) {
        LOG(ERROR) << "invalid " << tokens[0] << " config: " << ex.what() << " line:" << line;
        return {ErrorCodes::ERR_PARSEOPT, ""};
    }
    _confFile = filename;
    return {ErrorCodes::ERR_OK, ""};
}

bool ServerParams::setVar(const string& name, const string& value, string* errinfo, bool force) {
    auto iter = _mapServerParams.find(toLower(name));
    if (iter == _mapServerParams.end()){
        if (name.substr(0,6) == "rocks.") {
            auto ed = tendisplus::stoll(value);
            if (!ed.ok()) {
                if (errinfo != NULL)
                    *errinfo = "invalid rocksdb options:" + name
                    + " value:" + value;

                return false;
            }

            _rocksdbOptions.insert(make_pair(toLower(name.substr(6, name.length())), ed.value()));
            return true;
        }

        if (errinfo != NULL)
            *errinfo = "not found arg:" + name;
        return false;
    }
    if (!force) {
        LOG(INFO) << "ServerParams setVar dynamic," << name << " : " << value;
    }
    return iter->second->setVar(value, errinfo, force);
}


bool ServerParams::registerOnupdate(const string& name, funptr ptr){
    auto iter = _mapServerParams.find(toLower(name));
    if (iter == _mapServerParams.end()){
        return false;
    }
    iter->second->setUpdate(ptr);
    return true;
}

string ServerParams::showAll() const {
    string ret;
    for (auto iter : _mapServerParams) {
        ret += "  " + iter.second->getName() + ":"+ iter.second->show() + "\n";
    }

    for (auto iter : _rocksdbOptions) {
        ret += "  rocks." + iter.first + ":" + std::to_string(iter.second) + "\n";
    }

    ret.resize(ret.size() - 1);
    return ret;
}

bool ServerParams::showVar(const string& key, string& info) const {
    auto iter = _mapServerParams.find(key);
    if (iter == _mapServerParams.end()) {
        return false;
    }
    info = iter->second->show();
    return true;
}

bool ServerParams::showVar(const string& key, vector<string>& info) const {
    for(auto iter = _mapServerParams.begin(); iter != _mapServerParams.end(); iter++) {
        if(redis_port::stringmatchlen(key.c_str(), key.size(), iter->first.c_str(), iter->first.size(), 1)) {
            info.push_back(iter->first);
            info.push_back(iter->second->show());
        }
    }
    if(info.empty())
        return false;
    return true;
}
}  // namespace tendisplus

