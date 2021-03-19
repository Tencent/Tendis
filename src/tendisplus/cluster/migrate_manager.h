// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_CLUSTER_MIGRATE_MANAGER_H_
#define SRC_TENDISPLUS_CLUSTER_MIGRATE_MANAGER_H_

#include <list>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include "tendisplus/cluster/cluster_manager.h"
#include "tendisplus/cluster/migrate_receiver.h"
#include "tendisplus/cluster/migrate_sender.h"
#include "tendisplus/network/blocking_tcp_client.h"
#include "tendisplus/server/server_entry.h"
#include "tendisplus/storage/catalog.h"
#include "tendisplus/utils/rate_limiter.h"

namespace tendisplus {

class ServerEntry;
class StoreMeta;
class ChunkMeta;
class ChunkMigrateSender;
class ClusterState;
enum class BinlogApplyMode;
using myMutex = std::recursive_mutex;

// Sender POV
enum class MigrateSendState { WAIT = 0, START, SUCC, CLEAR, HALF, ERR };

// enum clas MigrateState { ALL = 0, RUNNING, ERROR, WAITING, SUCCESS };

enum MigrateBinlogType { RECEIVE_START, RECEIVE_END, SEND_START, SEND_END };

using SlotsBitmap = std::bitset<CLUSTER_SLOTS>;

Expected<uint64_t> addMigrateBinlog(MigrateBinlogType type,
                                    string slots,
                                    uint32_t storeid,
                                    ServerEntry* svr,
                                    const string& nodeName);

/* PARENT TASK produce by cluster setslot importing command */
class pTask {
 public:
  explicit pTask(const std::string& taskid, const std::string& nodeid)
    : _taskid(taskid), _nodeid(nodeid), _taskNum(0) {}
  std::string _taskid;
  std::string _startTime;
  std::string getTaskid() {
    return _taskid;
  }
  uint64_t _migrateTime;
  std::string _nodeid;  // mark the dstNode of SrcNode, or SrcNode of dstNode
  std::atomic<uint64_t> _taskNum;
  std::map<std::string, std::string> _failStateMap;
  std::map<std::string, std::string> _succStateMap;

  std::uint64_t getTaskNum() {
    return _taskNum.load(std::memory_order_relaxed);
  }
  void addTaskNum() {
    _taskNum.fetch_add(1, std::memory_order_relaxed);
  }
};

class MigrateSendTask {
 public:
  explicit MigrateSendTask(uint32_t storeId,
                           const string& taskid_,
                           const SlotsBitmap& slots_,
                           std::shared_ptr<ServerEntry> svr,
                           const std::shared_ptr<ServerParams> cfg,
                           const std::shared_ptr<pTask> pTask_,
                           bool is_fake = false)
    : _storeid(storeId),
      _taskid(taskid_),
      _slots(slots_),
      _svr(svr),
      _isRunning(false),
      _state(MigrateSendState::WAIT),
      _isFake(is_fake),
      _pTask(pTask_) {
    _sender =
      std::make_unique<ChunkMigrateSender>(slots_, taskid_, svr, cfg, is_fake);
  }

  uint32_t _storeid;
  std::string _taskid;
  SlotsBitmap _slots;
  std::shared_ptr<ServerEntry> _svr;
  bool _isRunning;
  SCLOCK::time_point _nextSchedTime;
  MigrateSendState _state;
  bool _isFake;
  std::unique_ptr<ChunkMigrateSender> _sender;
  std::shared_ptr<pTask> _pTask;
  mutable std::mutex _mutex;
  void stopTask();
  void sendSlots();
  void deleteSenderChunks();
  std::string toString();
};


// receiver POV
enum class MigrateReceiveState {
  NONE = 0,
  RECEIVE_SNAPSHOT,
  RECEIVE_BINLOG,
  SUCC,
  ERR
};

class ChunkMigrateReceiver;

class MigrateReceiveTask {
 public:
  explicit MigrateReceiveTask(const SlotsBitmap& slots_,
                              uint32_t store_id,
                              const string& taskid_,
                              const string& ip,
                              uint16_t port,
                              std::shared_ptr<ServerEntry> svr,
                              const std::shared_ptr<ServerParams> cfg,
                              const std::shared_ptr<pTask> pTask_)
    : _slots(slots_),
      _taskid(taskid_),
      _storeid(store_id),
      _srcIp(ip),
      _srcPort(port),
      _svr(svr),
      _isRunning(false),
      _lastSyncTime(0),
      _state(MigrateReceiveState::RECEIVE_SNAPSHOT),
      _pTask(pTask_),
      _retryTime(0) {
    _receiver = std::make_unique<ChunkMigrateReceiver>(
      slots_, store_id, taskid_, svr, cfg);
  }
  SlotsBitmap _slots;
  std::string _taskid;
  uint32_t _storeid;
  string _srcIp;
  uint16_t _srcPort;
  std::shared_ptr<ServerEntry> _svr;
  bool _isRunning;
  SCLOCK::time_point _nextSchedTime;
  uint64_t _lastSyncTime;
  MigrateReceiveState _state;
  std::unique_ptr<ChunkMigrateReceiver> _receiver;
  std::shared_ptr<pTask> _pTask;
  mutable std::mutex _mutex;
  std::atomic<uint32_t> _retryTime;
  void stopTask();
  void checkMigrateStatus();
  void fullReceive();
  uint32_t getRetryCount() {
    return _retryTime.load(std::memory_order_relaxed);
  }
  std::string toString();
};

class MigrateManager {
 public:
  explicit MigrateManager(std::shared_ptr<ServerEntry> svr,
                          const std::shared_ptr<ServerParams> cfg);

  Status startup();
  Status stopStoreTask(uint32_t storid);
  void stop();

  Status stopTasks(const std::string& taskid);
  void stopAllTasks(bool saveSlots = true);
  // sender POV
  bool senderSchedule(const SCLOCK::time_point& now);

  Status migrating(const SlotsBitmap& slots,
                   const string& ip,
                   uint16_t port,
                   uint32_t storeid,
                   const std::string& taskid,
                   const std::shared_ptr<pTask> task);

  void dstReadyMigrate(asio::ip::tcp::socket sock,
                       const std::string& chunkidArg,
                       const std::string& StoreidArg,
                       const std::string& nodeidArg,
                       const std::string& taskidArg);

  void dstPrepareMigrate(asio::ip::tcp::socket sock,
                         const std::string& chunkidArg,
                         const std::string& nodeidArg,
                         const std::string& taskid,
                         uint32_t storeNum);

  // receiver POV
  bool receiverSchedule(const SCLOCK::time_point& now);

  Status importing(const SlotsBitmap& slots,
                   const string& ip,
                   uint16_t port,
                   uint32_t storeid,
                   const std::string& taskid,
                   const std::shared_ptr<pTask> task);

  Status startTask(const SlotsBitmap& taskmap,
                   const std::string& ip,
                   uint16_t port,
                   const std::string& taskid,
                   uint32_t storeid,
                   const std::shared_ptr<pTask> task,
                   bool import = false);

  void insertNodes(const SlotsBitmap& slots,
                   const std::string& nodeid,
                   bool import);
  std::string getNodeIdBySlot(uint32_t, bool import = true);

  void addImportPTask(std::shared_ptr<pTask> task);
  void addMigratePTask(std::shared_ptr<pTask> task);

  Status applyRepllog(Session* sess,
                      uint32_t storeid,
                      BinlogApplyMode mode,
                      const std::string& logKey,
                      const std::string& logValue);
  Status supplyMigrateEnd(const std::string& taskid, bool binlogDone = true);
  uint64_t getProtectBinlogid(uint32_t storeid);

  bool slotInTask(uint32_t slot);
  bool slotsInTask(const SlotsBitmap& bitMap);
  Expected<std::string> getMigrateInfo();
  Expected<std::string> getTaskInfo(bool noWait = true, bool noRunning = false);
  Expected<std::string> getSuccFailInfo(bool succ = false);
  uint64_t getWaitTaskNum(const std::string& pTaskid);
  Expected<std::string> getTaskInfo(const std::string& taskid,
                                    bool noWait = true,
                                    bool noRunning = false);
  Expected<std::string> getSuccFailInfo(const std::string& taskid,
                                        bool succ = false);
  Expected<std::string> getMigrateInfoStr(const SlotsBitmap& bitMap);
  Expected<std::string> getMigrateInfoStrSimple(const SlotsBitmap& bitMap);
  SlotsBitmap getSteadySlots(const SlotsBitmap& bitMap);
  Expected<uint64_t> applyMigrateBinlog(ServerEntry* svr,
                                        PStore store,
                                        MigrateBinlogType type,
                                        string slots,
                                        const string& nodeName);
  Status restoreMigrateBinlog(MigrateBinlogType type,
                              uint32_t storeid,
                              string slots);
  Status onRestoreEnd(uint32_t storeId);

  void requestRateLimit(uint64_t bytes);

  void migrateSenderResize(size_t size);
  void migrateReceiverResize(size_t size);

  size_t migrateSenderSize();
  size_t migrateReceiverSize();

  static constexpr int32_t SEND_RETRY_CNT = 3;

  std::string genPTaskid();
  uint64_t getAllTaskNum();
  uint64_t getTaskNum(const std::string& taskid,
                      bool noWait = false,
                      bool noRunning = false);

  void removeRestartSlots(const std::string& nodeid, const SlotsBitmap& slots);
  std::map<std::string, SlotsBitmap> getStopMap();
  bool existMigrateTask();
  bool existPtask(const std::string& taskid);

 private:
  std::unordered_map<uint32_t, std::unique_ptr<ChunkLock>> _lockMap;
  void controlRoutine();
  bool containSlot(const SlotsBitmap& slots1, const SlotsBitmap& slots2);
  bool checkSlotOK(const SlotsBitmap& bitMap,
                   const std::string& nodeid,
                   std::vector<uint32_t>* taskSlots);
  std::string genTaskid();
  // Status stopSrcNode(const std::string& nodeid, const std::string& taskid);
  Status stopSrcNode(const std::string& taskid,
                     const std::string& srcIp,
                     uint64_t port);

 private:
  const std::shared_ptr<ServerParams> _cfg;
  std::shared_ptr<ServerEntry> _svr;
  std::shared_ptr<ClusterState> _cluster;

  std::condition_variable _cv;
  std::atomic<bool> _isRunning;
  std::atomic<uint64_t> _taskIdGen;
  std::atomic<uint64_t> _pTaskIdGen;
  mutable myMutex _mutex;
  std::unique_ptr<std::thread> _controller;

  // TODO(wayenchen) takenliu add, change all std::bitset<CLUSTER_SLOTS> to
  // SlotsBitmap
  std::bitset<CLUSTER_SLOTS> _migrateSlots;
  std::bitset<CLUSTER_SLOTS> _importSlots;

  std::bitset<CLUSTER_SLOTS> _succMigrateSlots;
  std::bitset<CLUSTER_SLOTS> _failMigrateSlots;

  std::bitset<CLUSTER_SLOTS> _succImportSlots;
  std::bitset<CLUSTER_SLOTS> _failImportSlots;

  std::vector<std::string> _succSenderTask;
  std::vector<std::string> _failSenderTask;

  std::vector<std::string> _succReceTask;
  std::vector<std::string> _failReceTask;

  std::bitset<CLUSTER_SLOTS> _stopImportSlots;
  std::map<std::string, SlotsBitmap> _stopImportMap;

  std::map<uint32_t, std::list<SlotsBitmap>> _restoreMigrateTask;

  // sender's pov
  std::unordered_map<std::string, std::unique_ptr<MigrateSendTask>>
    _migrateSendTaskMap;

  std::unique_ptr<WorkerPool> _migrateSender;
  std::unique_ptr<WorkerPool> _migrateClear;
  std::shared_ptr<PoolMatrix> _migrateSenderMatrix;
  std::shared_ptr<PoolMatrix> _migrateClearMatrix;

  // receiver's pov
  std::unordered_map<std::string, std::unique_ptr<MigrateReceiveTask>>
    _migrateReceiveTaskMap;

  std::unique_ptr<WorkerPool> _migrateReceiver;
  std::shared_ptr<PoolMatrix> _migrateReceiverMatrix;

  uint16_t _workload;
  // mark dst node or source node
  std::unordered_map<uint32_t, std::string> _migrateNodes;
  std::unordered_map<uint32_t, std::string> _importNodes;

  // sender rate limiter
  std::unique_ptr<RateLimiter> _rateLimiter;

  // ptaskid map
  std::unordered_map<std::string, std::shared_ptr<pTask>> _importPtaskMap;
  std::unordered_map<std::string, std::shared_ptr<pTask>> _migratePtaskMap;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_CLUSTER_MIGRATE_MANAGER_H_
