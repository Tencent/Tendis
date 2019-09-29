#include <iostream>
#include <string>
#include <stdlib.h>
#include <typeinfo>
#include <assert.h>
#include <algorithm>
#include <vector>
#include <fstream>
#include <sstream>

#include "glog/logging.h"
#include "tendisplus/utils/status.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/server/sys_vars.h"

namespace tendisplus {
using namespace std;

#define REGISTER_VARS(v) \
    if (typeid(v) == typeid(int) || typeid(v) == typeid(uint32_t) || typeid(v) == typeid(uint16_t)) \
        gMapServerParams.insert(make_pair(toLower(#v), new IntVar(#v, (void*)&v, NULL))); \
    else if (typeid(v) == typeid(float)) \
        gMapServerParams.insert(make_pair(toLower(#v), new FloatVar(#v, (void*)&v, NULL))); \
    else if (typeid(v) == typeid(string)) \
        gMapServerParams.insert(make_pair(toLower(#v), new StringVar(#v, (void*)&v, NULL))); \
    else if (typeid(v) == typeid(bool)) \
        gMapServerParams.insert(make_pair(toLower(#v), new BoolVar(#v, (void*)&v, NULL))); \
    else assert(false); // NOTE(takenliu): if other type is needed, change here.

ServerParams::ServerParams() {
    REGISTER_VARS(logSize);
    REGISTER_VARS(score);
    REGISTER_VARS(score2);
    REGISTER_VARS(open);



    REGISTER_VARS(bindIp);
    REGISTER_VARS(port);
    REGISTER_VARS(logLevel);
    REGISTER_VARS(logDir);

    REGISTER_VARS(storageEngine);
    REGISTER_VARS(dbPath);
    REGISTER_VARS(dumpPath);
    REGISTER_VARS(rocksBlockcacheMB);
    REGISTER_VARS(requirepass);
    REGISTER_VARS(masterauth);
    REGISTER_VARS(pidFile);
    REGISTER_VARS(versionIncrease);
    REGISTER_VARS(generalLog);
    // false: For command "set a b", it don't check the type of 
    // "a" and update it directly. It can make set() faster. 
    // Default false. Redis layer can guarantee that it's safe
    REGISTER_VARS(checkKeyTypeForSet);

    REGISTER_VARS(chunkSize);
    REGISTER_VARS(kvStoreCount);

    REGISTER_VARS(scanCntIndexMgr);
    REGISTER_VARS(scanJobCntIndexMgr);
    REGISTER_VARS(delCntIndexMgr);
    REGISTER_VARS(delJobCntIndexMgr);
    REGISTER_VARS(pauseTimeIndexMgr);

    REGISTER_VARS(protoMaxBulkLen);
    REGISTER_VARS(dbNum);

    REGISTER_VARS(noexpire);
    REGISTER_VARS(maxBinlogKeepNum);
    REGISTER_VARS(minBinlogKeepSec);

    REGISTER_VARS(maxClients);
    REGISTER_VARS(slowlogPath);
    REGISTER_VARS(slowlogLogSlowerThan);
    REGISTER_VARS(slowlogMaxLen);
    REGISTER_VARS(slowlogFlushInterval);
    REGISTER_VARS(netIoThreadNum);
    REGISTER_VARS(executorThreadNum);

    REGISTER_VARS(binlogRateLimitMB);
    REGISTER_VARS(timeoutSecBinlogSize1);
    REGISTER_VARS(timeoutSecBinlogSize2);
    REGISTER_VARS(timeoutSecBinlogSize3);
    REGISTER_VARS(timeoutSecBinlogFileList);
    REGISTER_VARS(timeoutSecBinlogFilename);
    REGISTER_VARS(timeoutSecBinlogBatch);
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
  return str.substr(str.find_first_not_of(pattern));
}

static std::string trim_right(const std::string& str) {
  const std::string pattern = " \f\n\r\t\v";
  return str.substr(0, str.find_last_not_of(pattern) + 1);
}

static std::string trim(const std::string& str) {
  return trim_left(trim_right(str));
}

bool ServerParams::parseFile(const std::string& filename, string& errinfo) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        errinfo = "open file:" + filename + " failed";
        return false;
    }
    std::vector<std::string> tokens;
    try {
        std::string line;
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
                if (!setVar(tokens[0], tokens[1], &errinfo)) {
                    return false;
                }
            } else {
                errinfo = "err arg:" + line;
                return false;
            }
        }
    } catch (const std::exception& ex) {
        std::stringstream ss;
        errinfo = "invalid " + tokens[0] + " config: " + ex.what();
        return false;
    }

    return true;
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

/*
void testupdate(){
    std::cout << "testupdate" << endl;
}

int main(){
    SysVars sysVars;
    string errinfo;
    bool ret = sysVars.parseFile("./tendisplus.conf", errinfo);
    cout << ret << " " << errinfo << endl;
    sysVars.registerOnupdate("logDir", testupdate);

    //sysVars.setVar("logSize", " -1dir");
    //sysVars.setVar("score", "b3dir");
    //sysVars.setVar("logDir", "dir");

    auto infos = sysVars.showAll();
    cout << "allinfo:" << endl << infos << endl;

    std::cout << sysVars.logSize << endl;
    std::cout << sysVars.logDir << endl;
    return 0;
}*/
