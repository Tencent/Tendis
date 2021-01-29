// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_CLUSTER_MIGRATE_SENDER_H_
#define SRC_TENDISPLUS_CLUSTER_MIGRATE_SENDER_H_

#include <bitset>
#include <list>
#include <memory>
#include <string>
#include "tendisplus/cluster/cluster_manager.h"
#include "tendisplus/cluster/migrate_manager.h"
#include "tendisplus/network/blocking_tcp_client.h"
#include "tendisplus/server/server_entry.h"
#include "tendisplus/server/server_params.h"
#include "tendisplus/utils/rate_limiter.h"
#include "tendisplus/utils/status.h"


namespace tendisplus {

class ClusterState;
class ClusterNode;
using myMutex = std::recursive_mutex;

enum class MigrateSenderStatus {
  NONE = 0,
  SNAPSHOT_BEGIN,
  SNAPSHOT_DONE,
  BINLOG_DONE,
  LASTBINLOG_DONE,
  SENDOVER_DONE,
  METACHANGE_DONE,
  DEL_DONE,
};


class ChunkMigrateSender {
 public:
  explicit ChunkMigrateSender(const std::bitset<CLUSTER_SLOTS>& slots,
                              const std::string& taskid,
                              std::shared_ptr<ServerEntry> svr,
                              std::shared_ptr<ServerParams> cfg,
                              bool is_fake = false);

  Status sendChunk();

  const std::bitset<CLUSTER_SLOTS>& getSlots() {
    return _slots;
  }

  void setStoreid(uint32_t storeid) {
    _storeid = storeid;
  }
  void setClient(std::shared_ptr<BlockingTcpClient> client) {
    _client = client;
  }

  void setDstAddr(const std::string& ip, uint32_t port) {
    _dstIp = ip;
    _dstPort = port;
  }

  void freeDbLock() {
    _dbWithLock.reset();
  }

  void setTaskId(const std::string& taskid) {
    _taskid = taskid;
  }

  void setDstStoreid(uint32_t dstStoreid) {
    _dstStoreid = dstStoreid;
  }
  void setDstNode(const std::string nodeid);

  uint32_t getStoreid() const {
    return _storeid;
  }
  uint64_t getSnapshotNum() const {
    return _snapshotKeyNum.load(std::memory_order_relaxed);
  }

  uint64_t getSnapShotStartTime() const {
    return _snapshotStartTime.load(std::memory_order_relaxed);
  }

  uint64_t getSnapShotEndTime() const {
    return _snapshotEndTime.load(std::memory_order_relaxed);
  }

  uint64_t getBinlogEndTime() const {
    return _binlogEndTime.load(std::memory_order_relaxed);
  }
  uint64_t getLockStartTime() const {
    return _lockStartTime.load(std::memory_order_relaxed);
  }

  uint64_t getLockEndTime() const {
    return _lockEndTime.load(std::memory_order_relaxed);
  }

  uint64_t getTaskStartTime() const {
    return _taskStartTime.load(std::memory_order_relaxed);
  }

  uint64_t getBinlogNum() const {
    return _binlogNum.load(std::memory_order_relaxed);
  }
  bool getConsistentInfo() const {
    return _consistency;
  }

  MigrateSenderStatus getSenderState() {
    std::lock_guard<std::mutex> lk(_mutex);
    return _sendstate;
  }

  std::string getStartTime() const {
    std::lock_guard<std::mutex> lk(_mutex);
    return _startTime;
  }

  void setStartTime(const std::string& str);
  void setTaskStartTime(uint64_t t);
  void setBinlogEndTime(uint64_t t);
  void setSnapShotStartTime(uint64_t t);
  void setSnapShotEndTime(uint64_t t);

  void setSenderStatus(MigrateSenderStatus s);
  bool checkSlotsBlongDst();

  uint64_t getProtectBinlogid() const {
    return _curBinlogid.load(std::memory_order_relaxed);
  }

  int64_t getBinlogDelay() const;

  Status lockChunks();
  void unlockChunks();
  bool needToWaitMetaChanged() const;
  Status sendVersionMeta();
  bool needToSendFail() const;
  void stop();
  void start();
  bool isRunning();
  std::string toString();

 private:
  Expected<std::unique_ptr<Transaction>> initTxn();
  Status sendBinlog();
  Expected<uint64_t> sendRange(Transaction* txn,
                               uint32_t begin,
                               uint32_t end,
                               uint32_t* totalNum);
  Status sendSnapshot();
  Status sendLastBinlog();
  Status catchupBinlog(uint64_t end);

  Status resetClient();
  Status sendOver();

 private:
  mutable std::mutex _mutex;

  std::bitset<CLUSTER_SLOTS> _slots;
  std::shared_ptr<ServerEntry> _svr;
  const std::shared_ptr<ServerParams> _cfg;
  std::atomic<bool> _isRunning;
  bool _isFake;

  std::unique_ptr<DbWithLock> _dbWithLock;
  std::shared_ptr<BlockingTcpClient> _client;
  std::string _taskid;
  std::shared_ptr<ClusterState> _clusterState;
  MigrateSenderStatus _sendstate;
  uint32_t _storeid;
  std::atomic<uint64_t> _snapshotKeyNum;
  std::atomic<uint64_t> _snapshotStartTime;
  std::atomic<uint64_t> _snapshotEndTime;
  std::atomic<uint64_t> _binlogEndTime;
  std::atomic<uint64_t> _binlogNum;
  std::atomic<uint64_t> _lockStartTime;
  std::atomic<uint64_t> _lockEndTime;
  std::atomic<uint64_t> _taskStartTime;
  uint64_t _binlogTimeStamp;
  std::string _startTime;
  bool _consistency;
  std::string _nodeid;
  std::atomic<uint64_t> _curBinlogid;
  string _dstIp;
  uint16_t _dstPort;
  uint32_t _dstStoreid;
  std::shared_ptr<ClusterNode> _dstNode;
  uint64_t getMaxBinLog(Transaction* ptxn) const;
  std::list<std::unique_ptr<ChunkLock>> _slotsLockList;
  std::string _OKSTR = "+OK";
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_CLUSTER_MIGRATE_SENDER_H_
