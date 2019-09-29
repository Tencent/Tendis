#ifndef SRC_TENDISPLUS_SERVER_SERVER_PARAMS_H_
#define SRC_TENDISPLUS_SERVER_SERVER_PARAMS_H_

#include <iostream>
#include <string>
#include <map>
#include <stdlib.h>
#include <atomic>
#include "tendisplus/utils/status.h"
#include "tendisplus/utils/redis_port.h"

namespace tendisplus {
using namespace std;

typedef void (*funptr) ();

char to_lower(char c)
{
    return tolower(c);
}
std::string toLower(std::string s) {
    std::string result = s;
    std::transform(result.begin(),
        result.end(),
        result.begin(),
        to_lower);
    return result;
}

class BaseVar {
public:
    BaseVar(string s, void* v, funptr ptr) {
        if (v == NULL) {
            assert(false);
            return;
        }
        name = s;
        value = v;
        Onupdate = ptr;
    };
    virtual ~BaseVar(){};
    virtual bool set(string value) = 0;
    virtual string show() = 0;
    void setUpdate(funptr f){
        Onupdate = f;
    }
protected:
    virtual bool check() = 0;

    string name;
    void* value;
    funptr Onupdate;
};

class StringVar : public BaseVar {
public:
    StringVar(string name, void* v, funptr ptr) : BaseVar(name, v, ptr){};
    bool set(string v) {
        if(!check()) return false;

        *(string*)value = v;

        if (Onupdate != NULL) Onupdate();
        return true;
    }
    virtual string show(){
        return "  " + name + ": \"" + *(string*)value + "\"";
    };
private:
    bool check() { return true; };
};

// support:int, uint32_t
class IntVar : public BaseVar {
public:
    IntVar(string name, void* v, funptr ptr) : BaseVar(name, v, ptr){};
    bool set(string v) {
        if(!check()) return false;

        *(int*)value = atoi(v.c_str());

        if (Onupdate != NULL) Onupdate();
        return true;
    }
    virtual string show(){
        return "  " + name + ": " + std::to_string(*(int*)value);
    };
private:
    bool check() { return true; };
};

class FloatVar : public BaseVar {
public:
    FloatVar(string name, void* v, funptr ptr) : BaseVar(name, v, ptr){};
    bool set(string v) {
        if(!check()) return false;

        *(float*)value = atof(v.c_str());

        if (Onupdate != NULL) Onupdate();
        return true;
    }
    virtual string show(){
        return "  " + name + ": " + std::to_string(*(float*)value);
    };
private:
    bool check() { return true; };
};

class BoolVar : public BaseVar {
public:
    BoolVar(string name, void* v, funptr ptr) : BaseVar(name, v, ptr){};
    bool set(string v) {
        if(!check()) return false;

        *(bool*)value = isOptionOn(v);

        if (Onupdate != NULL) Onupdate();
        return true;
    }
    virtual string show(){
        return "  " + name + ": " + std::to_string(*(bool*)value);
    };
private:
    bool check() { return true; };
    bool isOptionOn(const std::string& s) {
        auto x = toLower(s);
        if (x == "on" || x == "1" || x == "true") {
            return true;
        }
        return false;
    }
};

map<string, BaseVar*> gMapServerParams;

class ServerParams{
public:
    ServerParams();
    ~ServerParams();

    bool parseFile(const std::string& filename, string& errinfo);
    bool registerOnupdate(string name, funptr ptr);
    string showAll();
    bool setVar(string name, string value, string* errinfo);

public:
    int logSize = 0;
    float score = 2.0;
    uint32_t score2 = 2;
    bool open = false;
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
    uint32_t timeoutSecBinlogSize1 = 2;
    uint32_t timeoutSecBinlogSize2 = 10;
    uint32_t timeoutSecBinlogSize3 = 100;
    uint32_t timeoutSecBinlogFileList = 1000;
    uint32_t timeoutSecBinlogFilename = 10;
    uint32_t timeoutSecBinlogBatch = 100;
    uint32_t timeoutSecBinlogWaitRsp = 10;
    uint32_t incrPushThreadnum = 50;
    uint32_t fullPushThreadnum = 4;
    uint32_t fullReceiveThreadnum = 4;
    uint32_t logRecycleThreadnum = 12;
    uint32_t truncateBinlogIntervalMs = 1000;
    uint32_t truncateBinlogNum = 50000;
    uint32_t binlogFileSizeMB = 64;
    uint32_t binlogFileSecs = 20*60;
    uint32_t binlogHeartbeatSecs = 60;

    bool strictCapacityLimit = false;
    bool cacheIndexFilterblocks = false;
    int32_t maxOpenFiles = -1;
};
}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_SERVER_SERVER_PARAMS_H_
