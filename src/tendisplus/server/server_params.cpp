// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>
#include <cstdint>
#include <string>
#include <typeinfo>
#include <algorithm>
#include <vector>
#include <fstream>
#include <sstream>
#include <utility>
#include <memory>

#include "tendisplus/utils/status.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/server/server_entry.h"
#include "tendisplus/server/server_params.h"
#include "tendisplus/utils/invariant.h"

namespace tendisplus {
std::shared_ptr<tendisplus::ServerParams> gParams;
string gRenameCmdList = "";   // NOLINT
string gMappingCmdList = "";  // NOLINT

#define REGISTER_VARS_FULL(                                                   \
  str, var, checkfun, prefun, minval, maxval, allowDynamicSet)                \
  if (typeid(var) == typeid(int32_t) || typeid(var) == typeid(uint32_t))      \
    _mapServerParams.insert(                                                  \
      make_pair(toLower(str),                                                 \
                new IntVar(str,                                               \
                           reinterpret_cast<void*>(&var),                     \
                           checkfun,                                          \
                           prefun,                                            \
                           minval,                                            \
                           maxval,                                            \
                           allowDynamicSet)));                                \
  else if (typeid(var) == typeid(int64_t) || typeid(var) == typeid(uint64_t)) \
    _mapServerParams.insert(                                                  \
      make_pair(toLower(str),                                                 \
                new Int64Var(str,                                             \
                             reinterpret_cast<void*>(&var),                   \
                             checkfun,                                        \
                             prefun,                                          \
                             minval,                                          \
                             maxval,                                          \
                             allowDynamicSet)));                              \
  else if (typeid(var) == typeid(float))                                      \
    _mapServerParams.insert(                                                  \
      make_pair(toLower(str),                                                 \
                new FloatVar(str,                                             \
                             reinterpret_cast<void*>(&var),                   \
                             checkfun,                                        \
                             prefun,                                          \
                             allowDynamicSet)));                              \
  else if (typeid(var) == typeid(string))                                     \
    _mapServerParams.insert(                                                  \
      make_pair(toLower(str),                                                 \
                new StringVar(str,                                            \
                              reinterpret_cast<void*>(&var),                  \
                              checkfun,                                       \
                              prefun,                                         \
                              allowDynamicSet)));                             \
  else if (typeid(var) == typeid(bool))                                       \
    _mapServerParams.insert(                                                  \
      make_pair(toLower(str),                                                 \
                new BoolVar(str,                                              \
                            reinterpret_cast<void*>(&var),                    \
                            checkfun,                                         \
                            prefun,                                           \
                            allowDynamicSet)));                               \
  else                                                                        \
    INVARIANT(0);  // NOTE(takenliu): if other type is needed, change here.

#define REGISTER_VARS(var) \
  REGISTER_VARS_FULL(#var, var, NULL, NULL, 0, INT_MAX, false)
#define REGISTER_VARS_DIFF_NAME(str, var) \
  REGISTER_VARS_FULL(str, var, NULL, NULL, 0, INT_MAX, false)
#define REGISTER_VARS_ALLOW_DYNAMIC_SET(var) \
  REGISTER_VARS_FULL(#var, var, NULL, NULL, 0, INT_MAX, true)
#define REGISTER_VARS_DIFF_NAME_DYNAMIC(str, var) \
  REGISTER_VARS_FULL(str, var, NULL, NULL, 0, INT_MAX, true)
#define REGISTER_VARS_SAME_NAME(                          \
  var, checkfun, prefun, minval, maxval, allowDynamicSet) \
  REGISTER_VARS_FULL(                                     \
    #var, var, checkfun, prefun, minval, maxval, allowDynamicSet)

bool logLevelParamCheck(const string& val, bool startup, string* errinfo) {
  auto v = toLower(val);
  if (v == "debug" || v == "verbose" || v == "notice" || v == "warning") {
    return true;
  }
  return false;
}

bool compressTypeParamCheck(const string& val, bool startup, string* errinfo) {
  auto v = toLower(val);
  if (v == "snappy" || v == "lz4" || v == "none") {
    return true;
  }
  return false;
}

bool executorThreadNumCheck(
        const std::string& val, bool startup, string* errinfo) {
  auto num = std::strtoull(val.c_str(), nullptr, 10);
  if (startup || !gParams || gParams->executorWorkPoolSize == 0) {
    return true;
  }
  auto workPoolSize = gParams->executorWorkPoolSize;

  if (num % workPoolSize) {
    if (errinfo != NULL) {
      *errinfo = "need executorThreadNum % executorWorkPoolSize == 0";
    }
    return false;
  }
  return true;
}

bool binlogDelRangeCheck(
        const std::string& val, bool startup, string* errinfo) {
  auto num = std::strtoull(val.c_str(), nullptr, 10);
  if (startup || !gParams) {
    return true;
  }
  auto truncateBinlogNum = gParams->truncateBinlogNum;
  if (num > truncateBinlogNum) {
    if (errinfo != NULL) {
      *errinfo = "need binlogDelRange < truncateBinlogNum";
    }
    return false;
  }
  return true;
}

bool aofEnabledCheck(const std::string& val, bool startup, string* errinfo) {
  bool v = isOptionOn(val);
  if (startup || !gParams) {
    return true;
  }
  if (!v && gParams->psyncEnabled) {
    if (errinfo != NULL) {
      *errinfo = "can't disable aofEnabled when psyncEnabled is yes";
    }
    return false;
  }

  return true;
}

bool truncateBinlogNumCheck(
        const std::string& val, bool startup, string* errinfo) {
  auto num = std::strtoull(val.c_str(), nullptr, 10);
  if (startup || !gParams) {
    return true;
  }
  auto binlogDelRange = gParams->binlogDelRange;
  if (num < binlogDelRange) {
    if (errinfo != NULL) {
      *errinfo = "need binlogDelRange < truncateBinlogNum";
    }
    return false;
  }
  return true;
}

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

string removeQuotesAndToLower(const string& v) {
  auto tmp = toLower(v);
  if (tmp.size() < 2) {
    return tmp;
  }

  if (tmp[0] == '\"' && tmp[tmp.size() - 1] == '\"') {
    tmp = tmp.substr(1, tmp.size() - 2);
  }
  return tmp;
}

Status rewriteConfigState::rewriteConfigReadOldFile(
  const std::string& confFile) {
  Status s;
  std::ifstream file(confFile);
  if (!file.is_open()) {
    return {ErrorCodes::ERR_OK, ""};
  }
  int32_t linenum = -1;
  std::string line;
  std::vector<std::string> tokens;
  try {
    line.clear();
    while (std::getline(file, line)) {
      line = trim(line);
      linenum++;
      if (line[0] == '#' || line[0] == '\0') {
        if (!_hasTail && (line == _fixInfo))
          _hasTail = 1;
        _lines.emplace_back(line);
        line.clear();
        continue;
      }
      std::stringstream ss(line);
      tokens.clear();
      std::string tmp;
      while (std::getline(ss, tmp, ' ')) {
        tokens.emplace_back(tmp);
      }
      if (tokens.empty()) {
        std::string aux = "# ??? " + line;
        _lines.emplace_back(aux);
        line.clear();
        continue;
      }
      tokens[0] = toLower(tokens[0]);
      _lines.emplace_back(line);
      _optionToLine[tokens[0]].push_back(linenum);
      tokens.clear();
      line.clear();
    }
  } catch (const std::exception& ex) {
    LOG(ERROR) << "invalid " << tokens[0] << " config: " << ex.what()
               << " line:" << line;
    return {ErrorCodes::ERR_INTERNAL,
            "invalid " + tokens[0] + " config: " + ex.what() + " line:" + line};
  }
  return {ErrorCodes::ERR_OK, ""};
}

void rewriteConfigState::rewriteConfigRewriteLine(const std::string& option,
                                                  const std::string& line,
                                                  bool force) {
  if (_rewritten.find(option) == _rewritten.end()) {
    _rewritten.insert({option, std::list<uint64_t>()});
  }
  if (_optionToLine.find(option) == _optionToLine.end() && !force) {
    return;
  }
  if (_optionToLine.find(option) != _optionToLine.end()) {
    uint64_t linenum = *_optionToLine[option].begin();
    _optionToLine[option].erase(_optionToLine[option].begin());
    if (_optionToLine[option].empty()) {
      _optionToLine.erase(option);
    }
    _lines[linenum] = line;
  } else {
    if (!_hasTail) {
      _lines.emplace_back(_fixInfo);
      _hasTail = true;
    }
    _lines.emplace_back(line);
  }
}

void rewriteConfigState::rewriteConfigOption(const std::string& option,
                                             const std::string& value,
                                             const std::string& defvalue) {
  bool force = value != defvalue;
  std::string line;
  line += option + " " + value;

  rewriteConfigRewriteLine(option, line, force);
}

void rewriteConfigState::rewriteConfigRemoveOrphaned() {
  for (auto it = _optionToLine.begin(); it != _optionToLine.end(); it++) {
    std::string key = it->first;
    std::list<uint64_t> val = it->second;

    if (_rewritten.find(key) == _rewritten.end()) {
      continue;
    }

    for (auto lit = val.begin(); lit != val.end(); lit++) {
      int32_t linenum = *lit;
      _lines[linenum].clear();
    }
  }
}

std::string rewriteConfigState::rewriteConfigGetContentFromState() {
  std::string content;
  bool was_empty = false;
  uint32_t lsize = _lines.size();
  for (uint32_t i = 0; i < lsize; i++) {
    if (_lines[i].empty()) {
      if (was_empty)
        continue;
      was_empty = true;
    } else {
      was_empty = false;
    }
    content = content + _lines[i] + '\n';
  }
  return content;
}

Status rewriteConfigState::rewriteConfigOverwriteFile(
  const std::string& confFile, const std::string& content) {
  std::ofstream file(confFile);
  if (!file.is_open())
    return {ErrorCodes::ERR_INTERNAL, "fail to open file"};
  file << content;
  file.close();
  return {ErrorCodes::ERR_OK, ""};
}

ServerParams::ServerParams() {
  REGISTER_VARS_DIFF_NAME("bind", bindIp);
  REGISTER_VARS_FULL("port", port, nullptr, nullptr, 1, 65535, false);
  REGISTER_VARS_FULL("logLevel",
                     logLevel,
                     logLevelParamCheck,
                     removeQuotesAndToLower,
                     -1,
                     -1,
                     false);
  REGISTER_VARS(logDir);
  REGISTER_VARS(daemon);

  REGISTER_VARS_DIFF_NAME("storage", storageEngine);
  REGISTER_VARS_DIFF_NAME("dir", dbPath);
  REGISTER_VARS_DIFF_NAME("dumpdir", dumpPath);
  REGISTER_VARS(requirepass);
  REGISTER_VARS(masterauth);
  REGISTER_VARS(pidFile);
  REGISTER_VARS_DIFF_NAME_DYNAMIC("version-increase", versionIncrease);
  REGISTER_VARS_ALLOW_DYNAMIC_SET(generalLog);
  // false: For command "set a b", it don't check the type of
  // "a" and update it directly. It can make set() faster.
  // Default false. Redis layer can guarantee that it's safe
  REGISTER_VARS_DIFF_NAME_DYNAMIC("checkkeytypeforsetcmd", checkKeyTypeForSet);

  /* Disable parameter chunkSize because MACRO `CLUSTER_SLOTS` using everywhere
  REGISTER_VARS(chunkSize);
   */
  REGISTER_VARS(kvStoreCount);
  REGISTER_VARS_DIFF_NAME("chunkSize", fakeChunkSize);

  REGISTER_VARS_SAME_NAME(scanCntIndexMgr, nullptr, nullptr, 1, 1000000, true);
  REGISTER_VARS_SAME_NAME(scanJobCntIndexMgr, nullptr, nullptr, 1, 200, true);
  REGISTER_VARS_SAME_NAME(delCntIndexMgr, nullptr, nullptr, 1, 1000000, true);
  REGISTER_VARS_SAME_NAME(delJobCntIndexMgr, nullptr, nullptr, 1, 200, true);
  REGISTER_VARS_SAME_NAME(
    pauseTimeIndexMgr, nullptr, nullptr, 1, INT_MAX, true);

  REGISTER_VARS_DIFF_NAME("proto-max-bulk-len", protoMaxBulkLen);
  REGISTER_VARS_DIFF_NAME("databases", dbNum);

  REGISTER_VARS_ALLOW_DYNAMIC_SET(noexpire);
  REGISTER_VARS_SAME_NAME(
    maxBinlogKeepNum, nullptr, nullptr, 1, INT64_MAX, true);
  REGISTER_VARS_ALLOW_DYNAMIC_SET(minBinlogKeepSec);
  REGISTER_VARS_ALLOW_DYNAMIC_SET(slaveBinlogKeepNum);

  REGISTER_VARS_ALLOW_DYNAMIC_SET(maxClients);
  REGISTER_VARS_DIFF_NAME("slowlog", slowlogPath);
  REGISTER_VARS_DIFF_NAME_DYNAMIC("slowlog-log-slower-than",
                                  slowlogLogSlowerThan);
  REGISTER_VARS_DIFF_NAME_DYNAMIC("slowlog-max-len", slowlogMaxLen);
  REGISTER_VARS_DIFF_NAME_DYNAMIC("slowlog-flush-interval",
                                  slowlogFlushInterval);
  REGISTER_VARS_DIFF_NAME_DYNAMIC("slowlog-file-enabled", slowlogFileEnabled);

  // NOTE(pecochen): this two params should provide their own interface to
  // update.
  //              they don't use Workerpool, no need to use
  //              Workerpool::resize()
  REGISTER_VARS(netIoThreadNum);
  REGISTER_VARS_SAME_NAME(
    executorThreadNum, executorThreadNumCheck, nullptr, 1, 200, true);
  REGISTER_VARS_SAME_NAME(
    executorWorkPoolSize, nullptr, nullptr, 1, 200, false);

  REGISTER_VARS_ALLOW_DYNAMIC_SET(binlogRateLimitMB);
  // Only works on newly created connections(BlockingTcpClient)
  REGISTER_VARS_ALLOW_DYNAMIC_SET(netBatchSize);
  REGISTER_VARS_ALLOW_DYNAMIC_SET(netBatchTimeoutSec);
  REGISTER_VARS_ALLOW_DYNAMIC_SET(timeoutSecBinlogWaitRsp);
  REGISTER_VARS_SAME_NAME(incrPushThreadnum, nullptr, nullptr, 1, 200, true);
  REGISTER_VARS_SAME_NAME(fullPushThreadnum, nullptr, nullptr, 1, 200, true);
  REGISTER_VARS_SAME_NAME(fullReceiveThreadnum, nullptr, nullptr, 1, 200, true);
  REGISTER_VARS_SAME_NAME(logRecycleThreadnum, nullptr, nullptr, 1, 200, true);

  REGISTER_VARS_FULL("truncateBinlogIntervalMs",
                     truncateBinlogIntervalMs,
                     NULL,
                     NULL,
                     10,
                     5000,
                     true);
  REGISTER_VARS_SAME_NAME(
    truncateBinlogNum, truncateBinlogNumCheck, nullptr, 1, INT_MAX, true);
  REGISTER_VARS_ALLOW_DYNAMIC_SET(binlogFileSizeMB);
  REGISTER_VARS_ALLOW_DYNAMIC_SET(binlogFileSecs);
  REGISTER_VARS_SAME_NAME(
    binlogDelRange, binlogDelRangeCheck, nullptr, 1, 100000000, true);
  REGISTER_VARS_DIFF_NAME_DYNAMIC("binlog-send-batch", binlogSendBatch);
  REGISTER_VARS_DIFF_NAME_DYNAMIC("binlog-send-bytes", binlogSendBytes);

  REGISTER_VARS_ALLOW_DYNAMIC_SET(keysDefaultLimit);
  REGISTER_VARS_ALLOW_DYNAMIC_SET(lockWaitTimeOut);
  REGISTER_VARS_ALLOW_DYNAMIC_SET(lockDbXWaitTimeout);
  REGISTER_VARS_DIFF_NAME("binlog-using-defaultCF", binlogUsingDefaultCF);

  REGISTER_VARS_ALLOW_DYNAMIC_SET(scanDefaultLimit);
  REGISTER_VARS_SAME_NAME(
    scanDefaultMaxIterateTimes, nullptr, nullptr, 10, 10000, true);

  REGISTER_VARS_DIFF_NAME("rocks.blockcachemb", rocksBlockcacheMB);

  REGISTER_VARS_FULL("rocks.blockcache_num_shard_bits",
                    rocksBlockcacheNumShardBits,
                    nullptr, nullptr, -1, 19, false);
  REGISTER_VARS_DIFF_NAME("rocks.blockcache_strict_capacity_limit",
                          rocksStrictCapacityLimit);
  REGISTER_VARS_DIFF_NAME_DYNAMIC("rocks.disable_wal", rocksDisableWAL);
  REGISTER_VARS_DIFF_NAME_DYNAMIC("rocks.flush_log_at_trx_commit",
                                  rocksFlushLogAtTrxCommit);
  REGISTER_VARS_DIFF_NAME("rocks.wal_dir", rocksWALDir);

  REGISTER_VARS_FULL("rocks.compress_type",
                     rocksCompressType,
                     compressTypeParamCheck,
                     removeQuotesAndToLower,
                     -1,
                     -1,
                     false);
  REGISTER_VARS_DIFF_NAME("rocks.level0_compress_enabled", level0Compress);
  REGISTER_VARS_DIFF_NAME("rocks.level1_compress_enabled", level1Compress);

  REGISTER_VARS_SAME_NAME(
    migrateSenderThreadnum, nullptr, nullptr, 1, 200, true);
  REGISTER_VARS_SAME_NAME(
    migrateReceiveThreadnum, nullptr, nullptr, 1, 200, true);
  REGISTER_VARS_SAME_NAME(
    garbageDeleteThreadnum, nullptr, nullptr, 1, 100, true);

  REGISTER_VARS_DIFF_NAME("cluster-enabled", clusterEnabled);
  REGISTER_VARS_DIFF_NAME("domain-enabled", domainEnabled);
  REGISTER_VARS_FULL(
    "aof-enabled", aofEnabled, aofEnabledCheck, nullptr, 0, INT_MAX, true);
  REGISTER_VARS_DIFF_NAME_DYNAMIC("aof-psync-num", aofPsyncNum);

  REGISTER_VARS_DIFF_NAME_DYNAMIC("slave-migrate-enabled",
                                  slaveMigarateEnabled);
  REGISTER_VARS_DIFF_NAME_DYNAMIC("migrate-gc-enabled", enableGcInMigate);
  REGISTER_VARS_DIFF_NAME("cluster-single-node", clusterSingleNode);

  REGISTER_VARS_DIFF_NAME_DYNAMIC("cluster-require-full-coverage",
                                  clusterRequireFullCoverage);
  REGISTER_VARS_DIFF_NAME_DYNAMIC("cluster-slave-no-failover",
                                  clusterSlaveNoFailover);
  REGISTER_VARS_DIFF_NAME_DYNAMIC("cluster-node-timeout", clusterNodeTimeout);
  REGISTER_VARS_DIFF_NAME_DYNAMIC("cluster-migration-distance",
                                  migrateDistance);
  REGISTER_VARS_DIFF_NAME_DYNAMIC("garbage-delete-size", garbageDeleteSize);
  REGISTER_VARS_DIFF_NAME_DYNAMIC("cluster-migration-binlog-iters",
                                  migrateBinlogIter);
  REGISTER_VARS_DIFF_NAME_DYNAMIC("cluster-migration-slots-num-per-task",
                                  migrateTaskSlotsLimit);
  REGISTER_VARS_DIFF_NAME_DYNAMIC("migrate-snapshot-retry-num",
                                  snapShotRetryCnt);
  REGISTER_VARS_DIFF_NAME_DYNAMIC("cluster-migration-rate-limit",
                                  migrateRateLimitMB);
  REGISTER_VARS_DIFF_NAME_DYNAMIC("migrate-snapshot-retry-num",
                                  snapShotRetryCnt);
  REGISTER_VARS_DIFF_NAME_DYNAMIC("cluster-migration-barrier",
                                  clusterMigrationBarrier);
  REGISTER_VARS_DIFF_NAME_DYNAMIC("cluster-slave-validity-factor",
                                  clusterSlaveValidityFactor);
  REGISTER_VARS_DIFF_NAME_DYNAMIC("lua-time-limit", luaTimeLimit);
  REGISTER_VARS(luaStateMaxIdleTime);
  REGISTER_VARS_DIFF_NAME_DYNAMIC("jeprof-auto-dump", jeprofAutoDump);
  REGISTER_VARS_DIFF_NAME_DYNAMIC("compactrange-after-deleterange",
                                  compactRangeAfterDeleteRange);
}

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
      // TODO(takenliu) fix for multi space
      while (std::getline(ss, tmp, ' ')) {
        tokens.emplace_back(tmp);
      }

      if (tokens.size() == 3 && toLower(tokens[0]) == "rename-command") {
        gRenameCmdList += "," + tokens[1] + " " + tokens[2];
      } else if (tokens.size() == 3 &&
                 toLower(tokens[0]) == "mapping-command") {
        gMappingCmdList += "," + tokens[1] + " " + tokens[2];
      } else if (tokens.size() == 2) {
        if (toLower(tokens[0]) == "include") {
          if (_setConfFile.find(tokens[1]) != _setConfFile.end()) {
            LOG(ERROR) << "parseFile failed, include has recycle: "
                       << tokens[1];
            return {ErrorCodes::ERR_PARSEOPT, "include has recycle!"};
          }
          LOG(INFO) << "parseFile include file: " << tokens[1];
          auto ret = parseFile(tokens[1]);
          if (!ret.ok()) {
            LOG(ERROR) << "parseFile include file failed: " << tokens[1];
            return ret;
          }
        } else {
          auto s = setVar(tokens[0], tokens[1]);
          if (!s.ok()) {
            LOG(ERROR) << "invalid parameter:" << tokens[0] << " " << tokens[1]
                       << " " << s.toString();
            return {ErrorCodes::ERR_PARSEOPT,
                    "invalid parameter " + tokens[0] + " value: " + tokens[1]};
          }
        }
      } else {
        LOG(ERROR) << "err arg:" << line;
        return {ErrorCodes::ERR_PARSEOPT, "err arg: " + line};
      }
    }
  } catch (const std::exception& ex) {
    LOG(ERROR) << "invalid " << tokens[0] << " config: " << ex.what()
               << " line:" << line;
    return {ErrorCodes::ERR_PARSEOPT, ""};
  }

  auto s = checkParams();
  if (!s.ok()) {
    return s;
  }
  _confFile = filename;
  return {ErrorCodes::ERR_OK, ""};
}

// if need check, add in this function
Status ServerParams::checkParams() {
  if (executorWorkPoolSize != 0 && executorThreadNum % executorWorkPoolSize) {
    return {ErrorCodes::ERR_INTERNAL,
            "need executorThreadNum % executorWorkPoolSize == 0"};
  }

  if (binlogDelRange > truncateBinlogNum) {
    LOG(ERROR) << "not allow binlogDelRange > truncateBinlogNum : "
               << binlogDelRange << " > " << truncateBinlogNum;
    return {ErrorCodes::ERR_INTERNAL,
            "not allow binlogDelRange > truncateBinlogNum"};
  }

  if (psyncEnabled && !aofEnabled) {
    auto err = "psyncEnabled is not allowed when aofEnable is 0";
    LOG(ERROR) << err;
    return {ErrorCodes::ERR_INTERNAL, err};
  }

  if (scanJobCntIndexMgr > kvStoreCount) {
    LOG(INFO) << "`scanJobCntIndexMgr` is not allowed to be greater than "
                 "`kvstorecount`, set from "
              << scanJobCntIndexMgr << " to " << kvStoreCount;
    scanCntIndexMgr = kvStoreCount;
  }

  if (delJobCntIndexMgr > kvStoreCount) {
    LOG(INFO) << "`delJobCntIndexMgr` is not allowed to be greater than "
                 "`kvstorecount`, set from "
              << delJobCntIndexMgr << " to " << kvStoreCount;
    delJobCntIndexMgr = kvStoreCount;
  }

  if (incrPushThreadnum > kvStoreCount) {
    LOG(INFO) << "`incrPushThreadnum` is not allowed to be greater than "
                 "`kvstorecount`, set from "
              << incrPushThreadnum << " to " << kvStoreCount;
    incrPushThreadnum = kvStoreCount;
  }

  if (fullPushThreadnum > kvStoreCount) {
    LOG(INFO) << "`fullPushThreadnum` is not allowed to be greater than "
                 "`kvstorecount`, set from "
              << fullPushThreadnum << " to " << kvStoreCount;
    fullPushThreadnum = kvStoreCount;
  }

  if (fullReceiveThreadnum > kvStoreCount) {
    LOG(INFO) << "`fullReceiveThreadnum` is not allowed to be greater than "
                 "`kvstorecount`, set from "
              << fullReceiveThreadnum << " to " << kvStoreCount;
    fullReceiveThreadnum = kvStoreCount;
  }

  if (logRecycleThreadnum > kvStoreCount) {
    LOG(INFO) << "`logRecycleThreadnum` is not allowed to be greater than "
                 "`kvstorecount`, set from "
              << logRecycleThreadnum << " to " << kvStoreCount;
    logRecycleThreadnum = kvStoreCount;
  }

  return {ErrorCodes::ERR_OK, ""};
}

Status ServerParams::setVar(const string& name,
                            const string& value,
                            bool startup) {
  string errinfo;
  auto argname = toLower(name);
  auto iter = _mapServerParams.find(toLower(name));
  if (iter == _mapServerParams.end()) {
    if (argname.substr(0, 6) == "rocks.") {
      auto ed = tendisplus::stoll(value);
      if (!ed.ok()) {
        errinfo = "invalid rocksdb options:" + argname + " value:" + value +
          " " + ed.status().toString();
        return {ErrorCodes::ERR_PARSEOPT, errinfo};
      }

      if (startup) {
        _rocksdbOptions.insert(
          make_pair(toLower(argname.substr(6, argname.length())), ed.value()));
        return {ErrorCodes::ERR_OK, ""};
      } else {
        auto server = getGlobalServer();
        LocalSessionGuard sg(server.get());
        for (uint64_t i = 0; i < server->getKVStoreCount(); i++) {
          auto expStore = server->getSegmentMgr()->getDb(
            sg.getSession(), i, mgl::LockMode::LOCK_IS);
          RET_IF_ERR_EXPECTED(expStore);

          // change rocksdb options dynamically
          auto s = expStore.value().store->setOption(argname, ed.value());
          RET_IF_ERR(s);
        }

        return {ErrorCodes::ERR_OK, ""};
      }
    }

    errinfo = "not found arg:" + argname;
    return {ErrorCodes::ERR_PARSEOPT, errinfo};
  }
  if (!startup) {
    LOG(INFO) << "ServerParams setVar dynamic," << argname << " : " << value;
  }
  return iter->second->setVar(value, startup);
}

bool ServerParams::registerOnupdate(const string& name, funptr ptr) {
  auto iter = _mapServerParams.find(toLower(name));
  if (iter == _mapServerParams.end()) {
    return false;
  }
  iter->second->setUpdate(ptr);
  return true;
}

string ServerParams::showAll() const {
  string ret;
  for (auto iter : _mapServerParams) {
    if (iter.second->getName() == "requirepass" ||
        iter.second->getName() == "masterauth") {
      ret += "  " + iter.second->getName() + ":******\n";
      continue;
    }
    ret += "  " + iter.second->getName() + ":" + iter.second->show() + "\n";
  }

  for (auto iter : _rocksdbOptions) {
    ret += "  rocks." + iter.first + ":" + std::to_string(iter.second) + "\n";
  }

  ret.resize(ret.size() - 1);
  return ret;
}

bool ServerParams::showVar(const string& key, string* info) const {
  auto iter = _mapServerParams.find(key);
  if (iter == _mapServerParams.end()) {
    return false;
  }
  *info = iter->second->show();
  return true;
}

bool ServerParams::showVar(const string& key, vector<string>* info) const {
  for (auto iter = _mapServerParams.begin(); iter != _mapServerParams.end();
       iter++) {
    if (redis_port::stringmatchlen(key.c_str(),
                                   key.size(),
                                   iter->first.c_str(),
                                   iter->first.size(),
                                   1)) {
      info->push_back(iter->first);
      info->push_back(iter->second->show());
    }
  }
  if (info->empty())
    return false;
  return true;
}

Status ServerParams::rewriteConfig() const {
  auto rw = std::make_shared<rewriteConfigState>();
  Status s;

  s = rw->rewriteConfigReadOldFile(_confFile);
  if (!s.ok()) {
    return s;
  }

  for (auto it = _mapServerParams.begin(); it != _mapServerParams.end(); it++) {
    std::string name = it->first;
    BaseVar* var = it->second;

    if (!var->isallowDynamicSet())
      continue;

    rw->rewriteConfigOption(name, var->show(), var->default_show());
  }

  rw->rewriteConfigRemoveOrphaned();

  std::string content = rw->rewriteConfigGetContentFromState();

  s = rw->rewriteConfigOverwriteFile(_confFile, content);
  if (!s.ok()) {
    return s;
  }

  return {ErrorCodes::ERR_OK, ""};
}

}  // namespace tendisplus
