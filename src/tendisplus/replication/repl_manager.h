// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_REPLICATION_REPL_MANAGER_H_
#define SRC_TENDISPLUS_REPLICATION_REPL_MANAGER_H_

#include <fstream>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "tendisplus/network/blocking_tcp_client.h"
#include "tendisplus/replication/repl_util.h"
#include "tendisplus/server/server_entry.h"
#include "tendisplus/storage/catalog.h"
#include "tendisplus/utils/rate_limiter.h"

namespace tendisplus {

using SCLOCK = std::chrono::steady_clock;

const uint32_t gBinlogHeartbeatSecs = 1;
const uint32_t gBinlogHeartbeatTimeout = 10;

// slave's pov, sync status
struct SPovStatus {
  bool isRunning;
  uint64_t sessionId;
  SCLOCK::time_point nextSchedTime;
  SCLOCK::time_point lastSyncTime;
  uint64_t lastBinlogTs;  // in milliseconds
  uint32_t fullsyncSuccTimes;
};

enum class MPovClientType {
  repllogClient = 0,
  respClient = 1,  // resp = REdis Serialization Protocol
};

struct MPovStatus {
  bool isRunning = false;
  uint32_t dstStoreId = 0;
  // the greatest id that has been applied
  uint64_t binlogPos = 0;
  // the binlog timestamp that has been applied(milliseconds)
  uint64_t binlogTs = 0;
  SCLOCK::time_point nextSchedTime;
  SCLOCK::time_point lastSendBinlogTime;
  std::shared_ptr<BlockingTcpClient> client;
  uint64_t clientId = 0;
  string slave_listen_ip;
  uint16_t slave_listen_port = 0;
  MPovClientType clientType = MPovClientType::repllogClient;
};


enum class FullPushState {
  PUSHING = 0,
  SUCESS = 1,
  ERR = 2,
};

struct MPovFullPushStatus {
 public:
  std::string toString();

 public:
  uint32_t storeid;
  FullPushState state;
  // the greatest id that has been applied
  uint64_t binlogPos;
  SCLOCK::time_point startTime;
  SCLOCK::time_point endTime;
  std::shared_ptr<BlockingTcpClient> client;
  uint64_t clientId;
  string slave_listen_ip;
  uint16_t slave_listen_port;
};

struct RecycleBinlogStatus {
  bool isRunning;
  SCLOCK::time_point nextSchedTime;
  uint64_t firstBinlogId;
  uint64_t lastFlushBinlogId;
  uint32_t fileSeq;
  // the timestamp of last binlog in prev file or the first binlog in cur file
  uint64_t timestamp;
  SCLOCK::time_point fileCreateTime;
  uint64_t fileSize;
  std::unique_ptr<std::ofstream> fs;
  bool needNewFile;
  uint64_t saveBinlogId;
  std::string toString() const {
    std::stringstream ss;
    ss << "firstBinlogId:" << firstBinlogId << ",saveBinlogId:" << saveBinlogId
       << ",lastFlushBinlogId:" << lastFlushBinlogId << ",fileSeq:" << fileSeq
       << ",timestamp:" << timestamp;
    return ss.str();
  }
};

// 1) a new slave store's state is default to REPL_NONE
// when it receives a slaveof command, its state steps to
// REPL_CONNECT, when the scheduler sees the new state, it
// tries to connect master, auth and send initsync command.
// "connect/auth/initsync" are done in one schedule unit.
// if successes, it steps to REPL_TRANSFER state,
// otherwise, it keeps REPL_CONNECT state and wait for next
// schedule.

// 2) when slave's state steps into REPL_TRANSFER. a transfer
// file list is stores in _fetchStatus[i]. master keeps sending
// physical files to slaves.

// 3) REPL_TRANSFER wont be persisted, if the procedure fails,
// no matter network error or process crashes, slave will turn
// to REPL_CONNECT state and retry from 1)

// 4) on master side, each store can have only one slave copying
// physical data. the backup will be released after failure or
// procedure done.
enum class ReplState : std::uint8_t {
  REPL_NONE = 0,
  REPL_CONNECT = 1,
  REPL_TRANSFER = 2,   // initialsync, transfer whole db
  REPL_CONNECTED = 3,  // steadysync, transfer binlog steady
  REPL_ERR = 4
};

class ServerEntry;
class StoreMeta;

class ReplManager {
 public:
  explicit ReplManager(std::shared_ptr<ServerEntry> svr,
                       const std::shared_ptr<ServerParams> cfg);
  Status startup();
  Status stopStore(uint32_t storeId);
  void stop();
  void togglePauseState(bool isPaused) {
    _incrPaused = isPaused;
  }
  Status changeReplSource(Session* sess,
                          uint32_t storeId,
                          std::string ip,
                          uint32_t port,
                          uint32_t sourceStoreId);
  Status changeReplSourceInLock(uint32_t storeId,
                                std::string ip,
                                uint32_t port,
                                uint32_t sourceStoreId,
                                bool checkEmpty = true,
                                bool needLock = true,
                                bool incrSync = false);
  bool supplyFullSync(asio::ip::tcp::socket sock,
                      const std::string& storeIdArg,
                      const std::string& slaveIpArg,
                      const std::string& slavePortArg);
  bool registerIncrSync(asio::ip::tcp::socket sock,
                        const std::string& storeIdArg,
                        const std::string& dstStoreIdArg,
                        const std::string& binlogPosArg,
                        const std::string& listenIpArg,
                        const std::string& listenPortArg);

  bool supplyFullPsync(asio::ip::tcp::socket sock, const string& storeIdArg);

  void AddFakeFullPushStatus(
          uint64_t offset, const std::string& ip, uint64_t port);
  void DelFakeFullPushStatus(
          const std::string& ip, uint64_t port);

  Status replicationSetMaster(std::string ip,
                              uint32_t port,
                              bool checkEmpty = true,
                              bool incrSync = false);
  Status replicationSetMaster(std::string ip,
                              uint32_t port,
                              uint32_t storeid,
                              bool checkEmpty = true);
  Status replicationUnSetMaster();
  Status replicationUnSetMaster(uint32_t storeid);
#ifdef BINLOG_V1
  Status applyBinlogs(uint32_t storeId,
                      uint64_t sessionId,
                      const std::map<uint64_t, std::list<ReplLog>>& binlogs);
  Status applySingleTxn(uint32_t storeId,
                        uint64_t txnId,
                        const std::list<ReplLog>& ops);
#else
  Status applyRepllogV2(Session* sess,
                        uint32_t storeId,
                        const std::string& logKey,
                        const std::string& logValue);
#endif
  bool flushCurBinlogFs(uint32_t storeId);
  void appendJSONStat(rapidjson::PrettyWriter<rapidjson::StringBuffer>&) const;
  void getReplInfo(std::stringstream& ss) const;
  void onFlush(uint32_t storeId, uint64_t binlogid);
  bool hasSomeSlave(uint32_t storeId);
  bool isSlaveOfSomeone(uint32_t storeId);
  bool isSlaveOfSomeone();
  bool isSlaveFullSyncDone();
  Status resetRecycleState(uint32_t storeId);
  Expected<uint64_t> getSaveBinlogId(uint32_t storeId, uint32_t fileSeq);

  void fullPusherResize(size_t size);
  void fullReceiverResize(size_t size);
  void incrPusherResize(size_t size);
  void logRecyclerResize(size_t size);

  size_t fullPusherSize();
  size_t fullReceiverSize();
  size_t incrPusherSize();
  size_t logRecycleSize();

  std::string getRecycleBinlogStr(Session* sess) const;
  std::string getMasterHost() const;
  std::string getMasterHost(uint32_t storeid) const;
  std::vector<uint32_t> checkMasterHost(const std::string& hostname,
                                        uint32_t port);

  uint32_t getMasterPort() const;
  uint64_t getLastSyncTime() const;
  uint64_t replicationGetOffset() const;
  uint64_t replicationGetMaxBinlogIdFromRocks() const;
  uint64_t replicationGetMaxBinlogId() const;
  StoreMeta& getSyncMeta() const {
    return *_syncMeta[0];
  }

 protected:
  void controlRoutine();
  void supplyFullSyncRoutine(std::shared_ptr<BlockingTcpClient> client,
                             uint32_t storeId,
                             const string& slave_listen_ip,
                             uint16_t slave_listen_port);

  void supplyFullPsyncRoutine(std::shared_ptr<BlockingTcpClient> client,
                              uint32_t storeId);

  bool isFullSupplierFull() const;

  std::shared_ptr<BlockingTcpClient> createClient(const StoreMeta&,
                                                  uint64_t timeoutMs = 1000,
                                                  int64_t flags = 0);
  void slaveStartFullsync(const StoreMeta&);
  void slaveChkSyncStatus(const StoreMeta&);
  std::ofstream* getCurBinlogFs(uint32_t storeid);

#ifdef BINLOG_V1
  // binlogPos: the greatest id that has been applied
  Expected<uint64_t> masterSendBinlog(BlockingTcpClient*,
                                      uint32_t storeId,
                                      uint32_t dstStoreId,
                                      uint64_t binlogPos);
#else
  void updateCurBinlogFs(uint32_t storeId,
                         uint64_t written,
                         uint64_t ts,
                         bool changeNewFile = false);
#endif
  bool newBinlogFs(uint32_t storeId);

  void masterPushRoutine(uint32_t storeId, uint64_t clientId);
  void slaveSyncRoutine(uint32_t storeId);

  // truncate binlog in [start, end]
  void recycleBinlog(uint32_t storeId);

 private:
  void changeReplState(const StoreMeta& storeMeta, bool persist);
  void changeReplStateInLock(const StoreMeta&, bool persist);

  Expected<uint32_t> maxDumpFileSeq(uint32_t storeId);
#ifdef BINLOG_V1
  Status saveBinlogs(uint32_t storeId, const std::list<ReplLog>& logs);
#endif
  void getReplInfoSimple(std::stringstream& ss) const;
  void getReplInfoDetail(std::stringstream& ss) const;
  void recycleFullPushStatus();

 private:
  const std::shared_ptr<ServerParams> _cfg;
  mutable std::mutex _mutex;
  std::condition_variable _cv;
  std::atomic<bool> _isRunning;
  std::shared_ptr<ServerEntry> _svr;

  // slave's pov, meta data.
  std::vector<std::unique_ptr<StoreMeta>> _syncMeta;

  // slave's pov, sync status
  std::vector<std::unique_ptr<SPovStatus>> _syncStatus;

  // master's pov, living slave clients
#if defined(_WIN32) && _MSC_VER > 1900
  /* bugs for vs2017+, it can't use std::unique_ptr<> in std::pair
      https://stackoverflow.com/questions/44136073/stdvectorstdmapuint64-t-stdunique-ptrdouble-compilation-error-in-vs
      https://social.msdn.microsoft.com/Forums/windowsdesktop/en-US/15bfb88a-5fee-461c-b9c8-dc255148aad9/stdvectorltstdmapltuint64t-stduniqueptrltdoublegtgtgt-compilation-error-in?forum=vcgeneral
  */
  std::vector<std::map<uint64_t, MPovStatus*>> _pushStatus;
  std::vector<std::map<string, MPovFullPushStatus*>> _fullPushStatus;
#else
  std::vector<std::map<uint64_t, std::unique_ptr<MPovStatus>>> _pushStatus;
  std::vector<std::map<string, std::unique_ptr<MPovFullPushStatus>>>
    _fullPushStatus;
#endif

  // master and slave's pov, smallest binlogId, moves on when truncated
  std::vector<std::unique_ptr<RecycleBinlogStatus>> _logRecycStatus;

  // master's pov, workerpool of pushing full backup
  std::unique_ptr<WorkerPool> _fullPusher;

  // master's pov fullsync rate limiter
  std::unique_ptr<RateLimiter> _rateLimiter;

  // master's pov, workerpool of pushing incr backup
  std::unique_ptr<WorkerPool> _incrPusher;

  // master's pov, as its name
  bool _incrPaused;

  // slave's pov, workerpool of receiving full backup
  std::unique_ptr<WorkerPool> _fullReceiver;

  // slave's pov, periodly check incr-sync status
  std::unique_ptr<WorkerPool> _incrChecker;

  // master and slave's pov, log recycler
  std::unique_ptr<WorkerPool> _logRecycler;

  std::atomic<uint64_t> _clientIdGen;

  const std::string _dumpPath;

  std::unique_ptr<std::thread> _controller;

  std::shared_ptr<PoolMatrix> _fullPushMatrix;
  std::shared_ptr<PoolMatrix> _incrPushMatrix;
  std::shared_ptr<PoolMatrix> _fullReceiveMatrix;
  std::shared_ptr<PoolMatrix> _incrCheckMatrix;
  std::shared_ptr<PoolMatrix> _logRecycleMatrix;
  uint64_t _connectMasterTimeoutMs;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_REPLICATION_REPL_MANAGER_H_
