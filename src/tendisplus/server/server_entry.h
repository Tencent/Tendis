// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_SERVER_SERVER_ENTRY_H_
#define SRC_TENDISPLUS_SERVER_SERVER_ENTRY_H_

#include <vector>
#include <utility>
#include <memory>
#include <map>
#include <string>
#include <list>
#include <deque>
#include <set>
#include <shared_mutex>

#include "glog/logging.h"
#include "tendisplus/network/network.h"
#include "tendisplus/network/worker_pool.h"
#include "tendisplus/server/server_params.h"
#include "tendisplus/server/segment_manager.h"
#include "tendisplus/storage/pessimistic.h"
#include "tendisplus/replication/repl_manager.h"
#include "tendisplus/cluster/migrate_manager.h"
#include "tendisplus/server/index_manager.h"
#include "tendisplus/storage/kvstore.h"
#include "tendisplus/storage/rocks/rocks_kvstore.h"
#include "tendisplus/storage/catalog.h"
#include "tendisplus/lock/mgl/mgl_mgr.h"
#include "tendisplus/cluster/cluster_manager.h"
#include "tendisplus/cluster/gc_manager.h"
#include "tendisplus/utils/cursor_map.h"
#include "tendisplus/script/script_manager.h"

#define SLOWLOG_ENTRY_MAX_ARGC 32;
#define SLOWLOG_ENTRY_MAX_STRING 128;

namespace tendisplus {
class Session;
class NetworkAsio;
class NetworkMatrix;
class PoolMatrix;
class RequestMatrix;
class Catalog;
class ReplManager;
class MigrateManager;
class IndexManager;
class ClusterManager;
class GCManager;
class ScriptManager;


/* Instantaneous metrics tracking. */
#define STATS_METRIC_SAMPLES 16   /* Number of samples per metric. */
#define STATS_METRIC_COMMAND 0    /* Number of commands executed. */
#define STATS_METRIC_NET_INPUT 1  /* Bytes read to network .*/
#define STATS_METRIC_NET_OUTPUT 2 /* Bytes written to network. */
#define STATS_METRIC_COUNT 3

std::shared_ptr<ServerEntry>& getGlobalServer();
bool checkKvstoreSlot(uint32_t kvstoreId, uint64_t slot);

class ServerStat {
 public:
  ServerStat();
  /* Add a sample to the operations per second array of samples. */
  void trackInstantaneousMetric(int metric, uint64_t current_reading);
  uint64_t getInstantaneousMetric(int metric) const;
  void reset();

  Atom<uint64_t> expiredkeys;    /* Number of expired keys */
  Atom<uint64_t> keyspaceHits;   /* Number of successful lookups of keys */
  Atom<uint64_t> keyspaceMisses; /* Number of failed lookups of keys */
  Atom<uint64_t> keyspaceIncorrectEp;  // Number of failed lookups of keys with
                                       // incorrect versionEp
  Atom<uint64_t> rejectedConn;   /* Clients rejected because of maxclients */
  Atom<uint64_t> syncFull;       /* Number of full resyncs with slaves. */
  Atom<uint64_t> syncPartialOk;  /* Number of accepted PSYNC requests. */
  Atom<uint64_t> syncPartialErr; /* Number of unaccepted PSYNC requests. */
  Atom<uint64_t> netInputBytes;  /* Bytes read from network. */
  Atom<uint64_t> netOutputBytes; /* Bytes written to network. */

  /* The following two are used to track instantaneous metrics, like
   * number of operations per second, network traffic. */
  struct {
    uint64_t lastSampleTime;  /* Timestamp of last sample in ms */
    uint64_t lastSampleCount; /* Count in last sample */
    uint64_t samples[STATS_METRIC_SAMPLES];
    int idx;
  } instMetric[STATS_METRIC_COUNT];


 private:
  mutable std::mutex _mutex;
};

class CompactionStat {
 public:
  CompactionStat();
  std::string curDBid;
  uint64_t startTime;
  bool isRunning;
  void reset();

 private:
  mutable std::mutex _mutex;
};

struct SlowlogEntry {
  std::vector<string> argv;
  int argc;
  uint64_t id;        /* Unique entry identifier. */
  uint64_t duration;  /* Time spent by the query, in microseconds. */
  uint64_t unix_time; /* Unix time at which the query was executed. */
  std::string cname;
  std::string peerID;
};

class SlowlogStat {
 public:
  SlowlogStat();
  uint64_t getSlowlogNum();
  uint64_t getSlowlogLen();
  void resetSlowlogData();
  void slowlogDataPushEntryIfNeeded(uint64_t time,
                                    uint64_t duration,
                                    Session* sess);
  std::list<SlowlogEntry> getSlowlogData(uint64_t count);
  Status initSlowlogFile(std::string logPath);
  void closeSlowlogFile();

 private:
  std::list<SlowlogEntry> _slowlogData;
  std::ofstream _slowLog;
  std::atomic<uint64_t> _slowlogId;
  mutable std::mutex _mutex;
};

#define THREAD_SLEEP(n_secs)                                \
  do {                                                      \
    uint64_t loop = 0;                                      \
    while (loop++ < n_secs) {                               \
      std::this_thread::sleep_for(std::chrono::seconds(1)); \
    }                                                       \
  } while (0)

class ServerEntry;

class ServerEntry : public std::enable_shared_from_this<ServerEntry> {
 public:
  explicit ServerEntry(const std::shared_ptr<ServerParams>& cfg);
  ServerEntry(const ServerEntry&) = delete;
  ServerEntry(ServerEntry&&) = delete;
  ~ServerEntry();
  Catalog* getCatalog();
  Status startup(const std::shared_ptr<ServerParams>& cfg);
  uint64_t getStartupTimeNs() const;
  template <typename fn>
  void schedule(fn&& task, uint32_t& ctxId) {  // NOLINT
    if (ctxId == UINT32_MAX || ctxId >= _executorList.size()) {
      ctxId = _scheduleNum.fetch_add(1, std::memory_order_relaxed) %
        _executorList.size();
    }
    _executorList[ctxId]->schedule(std::forward<fn>(task));
  }
  std::shared_ptr<ServerParams>& getParams() {
    return _cfg;
  }
  bool addSession(std::shared_ptr<Session> sess);
  std::shared_ptr<Session> getSession(uint64_t id) const;

  // NOTE(deyukong): be careful, currently, the callpath of
  // serverEntry::endSession is
  // NetSession.endSession -> ServerEntry.endSession
  // -> ServerEntry.eraseSession -> NetSession.~NetSession
  // this function is initially triggered by NetSession.
  // If you want to close a NetSession from serverside, do not
  // call ServerEntry.endSession, or the underlay's socket
  // may never have a chance to be destroyed.
  // Instead, you should call NetSession.cancel to close NetSession's
  // underlying socket and let itself trigger the whole path.
  void endSession(uint64_t connId);
  size_t getSessionCount();

  Status cancelSession(uint64_t connId);

  std::list<std::shared_ptr<Session>> getAllSessions() const;

  // returns true if NetSession should continue schedule
  bool processRequest(Session* sess);

  void installStoresInLock(const std::vector<PStore>&);
  void installSegMgrInLock(std::unique_ptr<SegmentMgr>);
  void installCatalog(std::unique_ptr<Catalog>);
  void installPessimisticMgrInLock(std::unique_ptr<PessimisticMgr>);
  void installMGLockMgrInLock(std::unique_ptr<mgl::MGLockMgr> o);

  void stop();
  void waitStopComplete();
  SegmentMgr* getSegmentMgr() const;
  ReplManager* getReplManager();
  MigrateManager* getMigrateManager();
  NetworkAsio* getNetwork();
  PessimisticMgr* getPessimisticMgr();
  mgl::MGLockMgr* getMGLockMgr();
  IndexManager* getIndexMgr();
  ClusterManager* getClusterMgr();
  GCManager* getGcMgr();
  ScriptManager* getScriptMgr();

  // TODO(takenliu) : args exist at two places, has better way?
  std::string requirepass() const;
  std::string masterauth() const;
  void setRequirepass(const std::string& v);
  void setMasterauth(const std::string& v);

  bool versionIncrease() const;
  bool checkKeyTypeForSet() const {
    return _cfg->checkKeyTypeForSet;
  }
  uint32_t protoMaxBulkLen() const {
    return _protoMaxBulkLen;
  }
  uint32_t dbNum() const {
    return _dbNum;
  }

  const std::vector<PStore>& getStores() const {
    return _kvstores;
  }

  std::shared_ptr<rocksdb::Cache> getBlockCache() const {
    return _blockCache;
  }

  void toggleFtmc(bool enable);
  void appendJSONStat(rapidjson::PrettyWriter<rapidjson::StringBuffer>&,
                      const std::set<std::string>& sections) const;
  void getStatInfo(std::stringstream& ss) const;
  ServerStat& getServerStat() const {
    return (ServerStat&)_serverStat;
  }
  void resetServerStat();
  void resetRocksdbStats(Session* sess);
  CompactionStat& getCompactionStat() const {
    return (CompactionStat&)_compactionStat;
  }
  SlowlogStat& getSlowlogStat() const {
    return (SlowlogStat&)_slowlogStat;
  }
  void logGeneral(Session* sess);
  void handleShutdownCmd();
  Status setStoreMode(PStore store, KVStore::StoreMode mode);
  Status destroyStore(Session* sess, uint32_t storeId, bool isForce);
  uint32_t getKVStoreCount() const;
  void setTsEp(uint64_t timestamp);
  uint64_t getTsEp() const;
  void AddMonitor(uint64_t sessId);
  static void logWarning(const std::string& str, Session* sess = nullptr);
  static void logError(const std::string& str, Session* sess = nullptr);
  void slowlogPushEntryIfNeeded(uint64_t time,
                                uint64_t duration,
                                Session* sess);
  void onBackupEnd() {
    _lastBackupTime.store(sinceEpoch(), std::memory_order_relaxed);
    _backupRunning.fetch_sub(1, std::memory_order_relaxed);
    _backupTimes.fetch_add(1, std::memory_order_relaxed);
  }
  void onBackupEndFailed(uint32_t storeid, const string& errinfo) {
    _lastBackupFailedTime.store(sinceEpoch(), std::memory_order_relaxed);
    _backupFailedTimes.fetch_add(1, std::memory_order_relaxed);
    _backupRunning.fetch_sub(1, std::memory_order_relaxed);
    std::lock_guard<std::mutex> lk(_mutex);
    _lastBackupFailedErr =
      "storeid " + std::to_string(storeid) + ",err:" + errinfo;
  }
  uint64_t getLastBackupTime() {
    return _lastBackupTime.load(std::memory_order_relaxed);
  }
  uint64_t getBackupTimes() {
    return _backupTimes.load(std::memory_order_relaxed);
  }
  uint64_t getLastBackupFailedTime() {
    return _lastBackupFailedTime.load(std::memory_order_relaxed);
  }
  uint64_t getBackupFailedTimes() {
    return _backupFailedTimes.load(std::memory_order_relaxed);
  }
  string getLastBackupFailedErr() {
    std::lock_guard<std::mutex> lk(_mutex);
    return _lastBackupFailedErr;
  }
  uint64_t getBackupRunning() {
    return _backupRunning.load(std::memory_order_relaxed);
  }
  void setBackupRunning();
  bool getTotalIntProperty(
    Session* sess, const std::string& property, uint64_t* value,
    ColumnFamilyNumber cf = ColumnFamilyNumber::ColumnFamily_Default) const;

  bool getAllProperty(Session* sess,
                      const std::string& property,
                      std::string* value) const;
  uint64_t getStatCountByName(Session* sess, const std::string& ticker) const;

  /* Note(wayenchen) fast judge if dbsize is zero or not*/
  bool isDbEmpty();

  bool isClusterEnabled() const {
    return _enableCluster;
  }
  bool isRunning() const {
    return _isRunning;
  }

  CursorMap& getCursorMap(int dbId) {
    return _cursorMaps[dbId];
  }

 private:
  ServerEntry();
  Status adaptSomeThreadNumByCpuNum(const std::shared_ptr<ServerParams>& cfg);
  void serverCron();
  void jeprofCron();
  void replyMonitors(Session* sess);
  void DelMonitorNoLock(uint64_t connId);
  void resizeExecutorThreadNum(uint64_t newThreadNum);
  void resizeIncrExecutorThreadNum(uint64_t newThreadNum);
  void resizeDecrExecutorThreadNum(uint64_t newThreadNum);

  // NOTE(deyukong): _isRunning = true -> running
  // _isRunning = false && _isStopped = false -> stopping in progress
  // _isRunning = false && _isStopped = true -> stop complete
  std::atomic<bool> _ftmcEnabled;
  std::atomic<bool> _isRunning;
  std::atomic<bool> _isStopped;
  // whether shutdown command is excuted
  std::atomic<bool> _isShutdowned;
  uint64_t _startupTime;
  mutable std::mutex _mutex;
  std::condition_variable _eventCV;
  std::unique_ptr<NetworkAsio> _network;

  // NOTE(takenliu) _mutex_session is used only for _sessions and _monitors.
  mutable std::mutex _mutex_session;
  std::map<uint64_t, std::shared_ptr<Session>> _sessions;
  std::list<std::shared_ptr<Session>> _monitors;

  std::vector<std::unique_ptr<WorkerPool>> _executorList;
  std::set<std::unique_ptr<WorkerPool>> _executorRecycleSet;
  std::unique_ptr<SegmentMgr> _segmentMgr;
  std::unique_ptr<ReplManager> _replMgr;
  std::unique_ptr<MigrateManager> _migrateMgr;
  std::unique_ptr<IndexManager> _indexMgr;
  std::unique_ptr<PessimisticMgr> _pessimisticMgr;
  std::unique_ptr<mgl::MGLockMgr> _mgLockMgr;
  std::unique_ptr<ClusterManager> _clusterMgr;
  std::unique_ptr<GCManager> _gcMgr;
  std::unique_ptr<ScriptManager> _scriptMgr;

  std::shared_ptr<rocksdb::Cache> _blockCache;
  std::vector<PStore> _kvstores;
  std::unique_ptr<Catalog> _catalog;

  std::shared_ptr<NetworkMatrix> _netMatrix;
  std::shared_ptr<PoolMatrix> _poolMatrix;
  std::shared_ptr<RequestMatrix> _reqMatrix;
  std::unique_ptr<std::thread> _cronThd;

  bool _enableCluster;
  // NOTE(deyukong):
  // return string's reference have race conditions if changed during
  // runtime. return by value is quite costive.
  std::string _requirepass;
  std::string _masterauth;
  uint32_t _protoMaxBulkLen;
  uint32_t _dbNum;
  std::atomic<uint64_t> _tsFromExtendedProtocol;
  std::deque<CursorMap> _cursorMaps;  // deque NOT vector ! ! !

  std::atomic<uint64_t> _scheduleNum;
  std::shared_ptr<ServerParams> _cfg;
  std::atomic<uint64_t> _lastBackupTime;
  std::atomic<uint64_t> _backupTimes;
  std::atomic<uint64_t> _lastBackupFailedTime;
  std::atomic<uint64_t> _backupFailedTimes;
  std::atomic<uint64_t> _backupRunning;
  string _lastBackupFailedErr;
  ServerStat _serverStat;
  CompactionStat _compactionStat;
  SlowlogStat _slowlogStat;
  uint32_t _lastJeprofDumpMemoryGB;
};
}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_SERVER_SERVER_ENTRY_H_
