// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

// Copyright [2019] <eliotwang@tencent.com>
#ifndef SRC_TENDISPLUS_UTILS_TEST_UTIL_H_
#define SRC_TENDISPLUS_UTILS_TEST_UTIL_H_

#include <map>
#include <memory>
#include <vector>
#include <set>
#include <string>
#include <thread>  // NOLINT
#include <utility>

#include "asio.hpp"
#include "tendisplus/server/server_entry.h"
#include "tendisplus/server/server_params.h"
#include "tendisplus/cluster/cluster_manager.h"
#include "tendisplus/network/network.h"

// disable one test if needed
#define TEST_NO(a, b) void Test_no_##a##_##b()

#ifdef _WIN32
#define MYTEST TEST_NO
#include <windows.h>
#define usleep(us) Sleep(us)
#define be32toh(x) _byteswap_ulong(x)
#else
#define MYTEST TEST
#endif

namespace tendisplus {

using TestServer = std::shared_ptr<ServerEntry>;
using TestSession = std::shared_ptr<NetSession>;
using KeysWritten = std::set<std::string>;
using AllKeys = std::vector<KeysWritten>;

class NoSchedNetSession : public NetSession {
 public:
  NoSchedNetSession(std::shared_ptr<ServerEntry> server,
                    asio::ip::tcp::socket sock,
                    uint64_t connid,
                    bool initSock,
                    std::shared_ptr<NetworkMatrix> netMatrix,
                    std::shared_ptr<RequestMatrix> reqMatrix)
    : NetSession(
        server, std::move(sock), connid, initSock, netMatrix, reqMatrix) {
    // fake this flag as true, it can send nothing to the client
    _isSendRunning = true;
  }

 protected:
  virtual void schedule() {}

 public:
  // cmd using AOF format
  void setArgsFromAof(const std::string& cmd);
  virtual std::vector<std::string> getResponse();
};

bool setupEnv();
void destroyEnv();
bool setupEnv(const std::string& v);
void destroyEnv(const std::string& v);
std::string getBulkValue(const std::string& reply, uint32_t index);
std::shared_ptr<ServerParams> makeServerParam(uint32_t port = 8811,
                                              uint32_t storeCnt = 0,
                                              const std::string& dir = "",
                                              bool general_log = true);
std::shared_ptr<ServerEntry> makeServerEntry(
  const std::shared_ptr<ServerParams>& cfg);
std::shared_ptr<NetSession> makeSession(std::shared_ptr<ServerEntry> server,
                                        std::shared_ptr<asio::io_context> ctx);

void compareData(const std::shared_ptr<ServerEntry>& master,
                 const std::shared_ptr<ServerEntry>& slave,
                 bool comparebinlog = true);

/* remain api to get command string of primary key */
std::string getAofStr(const std::shared_ptr<ServerEntry>& svr,
                      const RecordKey& v);

bool setupReplEnv();
void destroyReplEnv();

int genRand();
std::string randomIp();
std::string randomStr(size_t s, bool maybeEmpty);
std::bitset<CLUSTER_SLOTS> genBitMap();

void testExpire1(std::shared_ptr<ServerEntry> svr);
void testExpire2(std::shared_ptr<ServerEntry> svr);
void testExpireCommandWhenNoexpireTrue(std::shared_ptr<ServerEntry> svr);
void testExpireKeyWhenGet(std::shared_ptr<ServerEntry> svr);
void testExpireKeyWhenCompaction(std::shared_ptr<ServerEntry> svr);
void testExpire(std::shared_ptr<ServerEntry> svr);
void testKV(std::shared_ptr<ServerEntry> svr);
void testMset(std::shared_ptr<ServerEntry> svr);
void testType(std::shared_ptr<ServerEntry> svr);
void testPf(std::shared_ptr<ServerEntry> svr);
void testZset(std::shared_ptr<ServerEntry> svr);
void testSet(std::shared_ptr<ServerEntry> svr);
void testZset3(std::shared_ptr<ServerEntry> svr);
void testZset4(std::shared_ptr<ServerEntry> svr);
void testZset2(std::shared_ptr<ServerEntry> svr);
void testHash1(std::shared_ptr<ServerEntry> svr);
void testHash2(std::shared_ptr<ServerEntry> svr);
void testList(std::shared_ptr<ServerEntry> svr);
void testSync(std::shared_ptr<ServerEntry> svr);

void testAll(std::shared_ptr<ServerEntry> svr);

class WorkLoad {
 public:
  WorkLoad(TestServer server, TestSession session)
    : _session(session), _max_key_len(32) {}

  void init() {
    std::srand((uint32_t)msSinceEpoch());
  }
  KeysWritten writeWork(RecordType,
                        uint32_t count,
                        uint32_t maxlen = 0,
                        bool sharename = true,
                        const char* key_suffix = NULL);
  void expireKeys(const AllKeys& keys, uint64_t ttl);
  void slaveof(const std::string& ip, uint32_t port);
  void flush();
  void delKeys(const KeysWritten& keys);
  void clusterMeet(const std::string& ip,
                   uint32_t port,
                   const uint32_t cport = 0);
  void clusterNodes();
  void addSlots(const std::string& slotsBuff);
  void replicate(const std::string& nodeName);
  bool manualFailover();
  void lockDb(mstime_t locktime);
  void stopMigrate(const std::string& taskid);
  void stopAllMigTasks();
  void restartAllMigTasks();
  void setMaxKeyLen(uint32_t max_key_len);
  Expected<uint64_t> getIntResult(const std::vector<std::string>& args);
  std::string getStringResult(const std::vector<std::string>& args);
  void addClusterSession(const string& addr, TestSession sess);

 private:
  Expected<string> runCommand(const std::vector<std::string>& args);

 private:
  TestSession _session;
  uint32_t _max_key_len;
  std::map<std::string, TestSession> _clusterSessions;
};

void waitSlaveCatchup(const std::shared_ptr<ServerEntry>& master,
                      const std::shared_ptr<ServerEntry>& slave);

std::string runCommand(std::shared_ptr<ServerEntry> svr,
                       std::vector<std::string> args);
void runBgCommand(std::shared_ptr<ServerEntry> svr);
}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_UTILS_TEST_UTIL_H_
