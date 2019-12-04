#ifndef SRC_TENDISPLUS_SERVER_SERVER_PARAMS_H_
#define SRC_TENDISPLUS_SERVER_SERVER_PARAMS_H_

#include <iostream>
#include <string>
#include <map>
#include <set>
#include <assert.h>
#include <stdlib.h>
#include <atomic>
#include "glog/logging.h"
#include "tendisplus/utils/status.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/redis_port.h"

namespace tendisplus {
using namespace std;

typedef void (*funptr) ();
typedef bool (*checkfunptr) (string&);

class BaseVar {
public:
    BaseVar(string s, void* v, checkfunptr ptr, bool allowDS) {
        if (v == NULL) {
            assert(false);
            return;
        }
        name = s;
        value = v;
        checkFun = ptr;
        allowDynamicSet = allowDS;
    };
    virtual ~BaseVar(){};
    bool setVar(string value, string* errinfo = NULL, bool force = true) {
        if (!allowDynamicSet && !force) {
            if (errinfo != NULL) {
                *errinfo = "not allow dynamic set";
            }
            return false;
        }
        return set(value);
    }
    virtual string show() = 0;
    void setUpdate(funptr f){
        Onupdate = f;
    }

    string getName(){
        return name;
    }
protected:
    virtual bool set(string value) = 0;
    virtual bool check(string& value) {
        if (checkFun != NULL) {
            return checkFun(value);
        }
        return true;
    };

    string name = "";
    void* value = NULL;
    funptr Onupdate = NULL;
    checkfunptr checkFun = NULL;
    bool allowDynamicSet = false;
};

class StringVar : public BaseVar {
public:
    StringVar(string name, void* v, checkfunptr ptr, bool allowDynamicSet) : BaseVar(name, v, ptr, allowDynamicSet){};
    virtual string show(){
        return "\"" + *(string*)value + "\"";
    };

private:
    bool set(string v) {
        preProcess(v);
        if(!check(v)) return false;

        *(string*)value = v;

        if (Onupdate != NULL) Onupdate();
        return true;
    }
    void preProcess(string& v) {
        if (v.size()<2) {
            return;
        }
        if (v[0] == '\"' && v[v.size()-1] == '\"') {
            v = v.substr(1, v.size()-2);
        }
    }
};

// support:int, uint32_t
class IntVar : public BaseVar {
public:
    IntVar(string name, void* v, checkfunptr ptr, bool allowDynamicSet) : BaseVar(name, v, ptr, allowDynamicSet){};
    virtual string show(){
        return std::to_string(*(int*)value);
    };

private:
    bool set(string v) {
        if(!check(v)) return false;

        try {
            *(int*)value = std::stoi(v);
        }
        catch (...) {
            LOG(ERROR) << "IntVar stoi err:" << v;
            return false;
        }

        if (Onupdate != NULL) Onupdate();
        return true;
    }

};

class FloatVar : public BaseVar {
public:
    FloatVar(string name, void* v, checkfunptr ptr, bool allowDynamicSet) : BaseVar(name, v, ptr, allowDynamicSet){};
    virtual string show(){
        return std::to_string(*(float*)value);
    };

private:
    bool set(string v) {
        if(!check(v)) return false;
        try {
            *(float*)value = std::stof(v);
        } catch (...) {
            LOG(ERROR) << "FloatVar stof err:" << v;
            return false;
        }
        if (Onupdate != NULL) Onupdate();
        return true;
    }
};

class BoolVar : public BaseVar {
public:
    BoolVar(string name, void* v, checkfunptr ptr, bool allowDynamicSet) : BaseVar(name, v, ptr, allowDynamicSet){};
    virtual string show(){
        return std::to_string(*(bool*)value);
    };

private:
    bool set(string v) {
        if(!check(v)) return false;

        *(bool*)value = isOptionOn(v);

        if (Onupdate != NULL) Onupdate();
        return true;
    }

    bool isOptionOn(const std::string& s) {
        auto x = toLower(s);
        if (x == "on" || x == "1" || x == "true") {
            return true;
        }
        return false;
    }
};

class ServerParams{
public:
    ServerParams();
    ~ServerParams();

    Status parseFile(const std::string& filename);
    bool registerOnupdate(string name, funptr ptr);
    string showAll();
    bool showVar(const string& key, string& info);
    bool setVar(string name, string value, string* errinfo, bool force = true);
    uint32_t paramsNum() {
        return _mapServerParams.size();
    }
    string getConfFile() {
        return _confFile;
    }
private:
    map<string, BaseVar*> _mapServerParams;
    std::string _confFile = "";
    std::set<std::string> _setConfFile;

public:
    std::string bindIp = "127.0.0.1";
    uint16_t port = 8903;
    std::string logLevel = "";
    std::string logDir = "./";

    std::string storageEngine = "rocks";
    std::string dbPath = "./db";
    std::string dumpPath = "./dump";
    uint32_t  rocksBlockcacheMB = 4096;
    std::string requirepass = "";
    std::string masterauth = "";
    std::string pidFile = "./tendisplus.pid";
    bool versionIncrease = true;
    bool generalLog = false;
    // false: For command "set a b", it don't check the type of 
    // "a" and update it directly. It can make set() faster. 
    // Default false. Redis layer can guarantee that it's safe
    bool checkKeyTypeForSet = false;

    uint32_t chunkSize = 0x4000;  // same as rediscluster
    uint32_t kvStoreCount = 10;

    uint32_t scanCntIndexMgr = 1000;
    uint32_t scanJobCntIndexMgr = 1;
    uint32_t delCntIndexMgr = 10000;
    uint32_t delJobCntIndexMgr = 1;
    uint32_t pauseTimeIndexMgr = 10;

    uint32_t protoMaxBulkLen = CONFIG_DEFAULT_PROTO_MAX_BULK_LEN;
    uint32_t dbNum = CONFIG_DEFAULT_DBNUM;

    bool noexpire = false;
    uint32_t maxBinlogKeepNum = 1000000;
    uint32_t minBinlogKeepSec = 0;

    uint32_t maxClients = CONFIG_DEFAULT_MAX_CLIENTS;
    std::string slowlogPath = "./slowlog";
    uint32_t slowlogLogSlowerThan = CONFIG_DEFAULT_SLOWLOG_LOG_SLOWER_THAN;
    //uint32_t slowlogMaxLen;
    uint32_t slowlogFlushInterval = CONFIG_DEFAULT_SLOWLOG_FLUSH_INTERVAL;
    uint32_t netIoThreadNum = 0;
    uint32_t executorThreadNum = 0;

    uint32_t binlogRateLimitMB = 64;
    uint32_t netBatchSize = 1024*1024;
    uint32_t netBatchTimeoutSec = 10;
    uint32_t timeoutSecBinlogWaitRsp = 10;
    uint32_t incrPushThreadnum = 50;
    uint32_t fullPushThreadnum = 4;
    uint32_t fullReceiveThreadnum = 4;
    uint32_t logRecycleThreadnum = 12;
    uint32_t truncateBinlogIntervalMs = 1000;
    uint32_t truncateBinlogNum = 50000;
    uint32_t binlogFileSizeMB = 64;
    uint32_t binlogFileSecs = 20*60;

    bool strictCapacityLimit = false;
    bool cacheIndexFilterblocks = false;
    int32_t maxOpenFiles = -1;
    int32_t keysDefaultLimit = 100;
};
}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_SERVER_SERVER_PARAMS_H_
