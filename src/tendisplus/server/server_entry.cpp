// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <utility>
#include <memory>
#include <algorithm>
#include <chrono>  // NOLINT
#include <string>  // NOLINT
#include <list>
#include <mutex>  // NOLINT
#include "glog/logging.h"
#include "jemalloc/jemalloc.h"
#include "tendisplus/server/server_entry.h"
#include "tendisplus/server/server_params.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/time.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/storage/rocks/rocks_kvstore.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/lock/lock.h"

namespace tendisplus {

ServerStat::ServerStat() {
  memset(&instMetric, 0, sizeof(instMetric));
}

void ServerStat::reset() {
  std::lock_guard<std::mutex> lk(_mutex);
  expiredkeys = 0;
  keyspaceHits = 0;
  keyspaceMisses = 0;
  keyspaceIncorrectEp = 0;
  rejectedConn = 0;
  syncFull = 0;
  syncPartialOk = 0;
  syncPartialErr = 0;
  netInputBytes = 0;
  netOutputBytes = 0;
  memset(&instMetric, 0, sizeof(instMetric));
}

CompactionStat::CompactionStat()
  : curDBid(""), startTime(sinceEpoch()), isRunning(false) {}

void CompactionStat::reset() {
  std::lock_guard<std::mutex> lk(_mutex);
  isRunning = false;
  curDBid = "";
}

/* Return the mean of all the samples. */
uint64_t ServerStat::getInstantaneousMetric(int metric) const {
  std::lock_guard<std::mutex> lk(_mutex);
  int j;
  uint64_t sum = 0;

  for (j = 0; j < STATS_METRIC_SAMPLES; j++)
    sum += instMetric[metric].samples[j];
  return sum / STATS_METRIC_SAMPLES;
}

/* Add a sample to the operations per second array of samples. */
void ServerStat::trackInstantaneousMetric(int metric,
                                          uint64_t current_reading) {
  std::lock_guard<std::mutex> lk(_mutex);
  uint64_t t = msSinceEpoch() - instMetric[metric].lastSampleTime;
  uint64_t ops = current_reading - instMetric[metric].lastSampleCount;
  uint64_t ops_sec;

  ops_sec = t > 0 ? (ops * 1000 / t) : 0;

  instMetric[metric].samples[instMetric[metric].idx] = ops_sec;
  instMetric[metric].idx++;
  instMetric[metric].idx %= STATS_METRIC_SAMPLES;
  instMetric[metric].lastSampleTime = msSinceEpoch();
  instMetric[metric].lastSampleCount = current_reading;
}

SlowlogStat::SlowlogStat() {
  _slowlogId.store(0, std::memory_order_relaxed);
}

uint64_t SlowlogStat::getSlowlogNum() {
  return _slowlogId.load(std::memory_order_relaxed);
}

uint64_t SlowlogStat::getSlowlogLen() {
  std::lock_guard<std::mutex> lk(_mutex);
  return _slowlogData.size();
}

void SlowlogStat::resetSlowlogData() {
  std::lock_guard<std::mutex> lk(_mutex);
  _slowlogData.clear();
}

std::list<SlowlogEntry> SlowlogStat::getSlowlogData(uint64_t count) {
  std::lock_guard<std::mutex> lk(_mutex);
  std::list<SlowlogEntry> result;
  int size;
  size = std::min(count, _slowlogData.size());
  std::list<SlowlogEntry>::iterator it = _slowlogData.begin();
  for (int i = 0; i < size; i++) {
    result.push_back(*it);
    it++;
  }
  return result;
}

Status SlowlogStat::initSlowlogFile(std::string logPath) {
  _slowLog.open(logPath, std::ofstream::app);
  if (!_slowLog.is_open()) {
    std::stringstream ss;
    ss << "open:" << logPath << " failed";
    return {ErrorCodes::ERR_INTERNAL, ss.str()};
  }

  return {ErrorCodes::ERR_OK, ""};
}

void SlowlogStat::closeSlowlogFile() {
  _slowLog.close();
}

void SlowlogStat::slowlogDataPushEntryIfNeeded(uint64_t time,
                                               uint64_t duration,
                                               Session* sess) {
  std::lock_guard<std::mutex> lk(_mutex);
  auto server = sess->getServerEntry();
  auto& args = sess->getArgs();
  auto& cfgs = server->getParams();
  size_t max_argc = SLOWLOG_ENTRY_MAX_ARGC;
  size_t max_string = SLOWLOG_ENTRY_MAX_STRING;

  if (cfgs->slowlogFileEnabled) {
    _slowLog << "# Id: " << _slowlogId.load(std::memory_order_relaxed) << "\n";
    _slowLog << "# Timestamp: " << time << "\n";
    _slowLog << "# Time: " << epochToDatetime(time / 1000000) << "\n";
    _slowLog << "# Host: " << sess->getRemote() << "\n";
    _slowLog << "# Db: " << sess->getCtx()->getDbId() << "\n";
    _slowLog << "# Query_time: " << duration << "\n";

    uint64_t args_total_length = 0;
    uint64_t args_output_length = 0;
    for (size_t i = 0; i < args.size(); ++i) {
      args_total_length += args[i].length();
    }
    if (args_total_length > cfgs->slowlogMaxLen) {
      _slowLog << "[" << args_total_length << "] ";
    } else {
      _slowLog << "[] ";
    }
    for (size_t i = 0; i < args.size(); ++i) {
      if (args_output_length + args[i].length() <= max_string) {
        _slowLog << args[i] << " ";
        args_output_length += args[i].length();
      } else {
        _slowLog << args[i].substr(0, (max_string - args_output_length)) << " ";
        break;
      }
    }
    _slowLog << "\n\n";
    if ((_slowlogId.load(std::memory_order_relaxed) %
         cfgs->slowlogFlushInterval) == 0) {
      _slowLog.flush();
    }
  }

  SlowlogEntry new_entry;
  size_t slargc = std::min(args.size(), max_argc);
  new_entry.argc = slargc;
  for (size_t i = 0; i < slargc; i++) {
    if (slargc != args.size() && i == slargc - 1) {
      std::string remain_arg = "... (";
      remain_arg.append(to_string(args.size() - max_argc + 1));
      remain_arg.append(" more arguments)");
      new_entry.argv.push_back(std::move(remain_arg));
    } else {
      if (args[i].size() > max_string) {
        std::string brief_arg = std::move(args[i].substr(0, max_string));
        brief_arg.append("... (");
        brief_arg.append(to_string(args[i].size() - max_string));
        brief_arg.append(" more bytes)");
        new_entry.argv.push_back(std::move(brief_arg));
      } else {
        new_entry.argv.push_back(std::move(args[i]));
      }
    }
  }
  new_entry.peerID = sess->getRemote();
  new_entry.id = _slowlogId.load(std::memory_order_relaxed);
  new_entry.duration = duration;
  new_entry.cname = sess->getName();
  new_entry.unix_time = time / 1000000;
  _slowlogData.push_front(new_entry);

  while (_slowlogData.size() > cfgs->slowlogMaxLen) {
    _slowlogData.pop_back();
  }

  _slowlogId.fetch_add(1, std::memory_order_relaxed);
}

ServerEntry::ServerEntry()
  : _ftmcEnabled(false),
    _isRunning(false),
    _isStopped(true),
    _isShutdowned(false),
    _startupTime(nsSinceEpoch()),
    _network(nullptr),
    _segmentMgr(nullptr),
    _replMgr(nullptr),
    _migrateMgr(nullptr),
    _indexMgr(nullptr),
    _pessimisticMgr(nullptr),
    _mgLockMgr(nullptr),
    _clusterMgr(nullptr),
    _gcMgr(nullptr),
    _scriptMgr(nullptr),
    _catalog(nullptr),
    _netMatrix(std::make_shared<NetworkMatrix>()),
    _poolMatrix(std::make_shared<PoolMatrix>()),
    _reqMatrix(std::make_shared<RequestMatrix>()),
    _cronThd(nullptr),
    _enableCluster(false),
    _requirepass(""),
    _masterauth(""),
    _protoMaxBulkLen(CONFIG_DEFAULT_PROTO_MAX_BULK_LEN),
    _dbNum(CONFIG_DEFAULT_DBNUM),
    _scheduleNum(0),
    _cfg(nullptr),
    _lastBackupTime(0),
    _backupTimes(0),
    _lastBackupFailedTime(0),
    _backupFailedTimes(0),
    _backupRunning(0),
    _lastBackupFailedErr("") {}

ServerEntry::ServerEntry(const std::shared_ptr<ServerParams>& cfg)
  : ServerEntry() {
  _requirepass = cfg->requirepass;
  _masterauth = cfg->masterauth;
  _protoMaxBulkLen = cfg->protoMaxBulkLen;
  _enableCluster = cfg->clusterEnabled;
  _dbNum = cfg->dbNum;
  _cfg = cfg;
  _cfg->serverParamsVar("executorThreadNum")->setUpdate([this]() {
    resizeExecutorThreadNum(_cfg->executorThreadNum);
  });
}

ServerEntry::~ServerEntry() {
  stop();
}

void ServerEntry::resetServerStat() {
  std::lock_guard<std::mutex> lk(_mutex);

  _poolMatrix->reset();
  _netMatrix->reset();
  _reqMatrix->reset();

  _serverStat.reset();
}

void ServerEntry::installPessimisticMgrInLock(
  std::unique_ptr<PessimisticMgr> o) {
  _pessimisticMgr = std::move(o);
}

void ServerEntry::installMGLockMgrInLock(std::unique_ptr<mgl::MGLockMgr> o) {
  _mgLockMgr = std::move(o);
}

void ServerEntry::installStoresInLock(const std::vector<PStore>& o) {
  // TODO(deyukong): assert mutex held
  _kvstores = o;
}

void ServerEntry::installSegMgrInLock(std::unique_ptr<SegmentMgr> o) {
  // TODO(deyukong): assert mutex held
  _segmentMgr = std::move(o);
}

void ServerEntry::installCatalog(std::unique_ptr<Catalog> o) {
  _catalog = std::move(o);
}


Catalog* ServerEntry::getCatalog() {
  return _catalog.get();
}

void ServerEntry::logGeneral(Session* sess) {
  if (!_cfg->generalLog) {
    return;
  }
  LOG(INFO) << sess->getCmdStr();
}

void ServerEntry::logWarning(const std::string& str, Session* sess) {
  std::stringstream ss;
  if (sess) {
    ss << sess->id() << "cmd:" << sess->getCmdStr();
  }

  ss << ", warning:" << str;

  LOG(WARNING) << ss.str();
}

void ServerEntry::logError(const std::string& str, Session* sess) {
  std::stringstream ss;
  if (sess) {
    ss << "sessid:" << sess->id() << " cmd:" << sess->getCmdStr();
  }

  ss << ", error:" << str;

  LOG(ERROR) << ss.str();
}

uint32_t ServerEntry::getKVStoreCount() const {
  return _catalog->getKVStoreCount();
}

void ServerEntry::setBackupRunning() {
  _backupRunning.fetch_add(1, std::memory_order_relaxed);
}

Status ServerEntry::adaptSomeThreadNumByCpuNum(
  const std::shared_ptr<ServerParams>& cfg) {
  // request executePool
  uint32_t cpuNum = std::thread::hardware_concurrency();
  if (cpuNum == 0) {
    LOG(ERROR) << "ServerEntry::startup failed, cpuNum:" << cpuNum;
    return {ErrorCodes::ERR_INTERNAL, "cpu num cannot be detected"};
  }

  if (cfg->executorWorkPoolSize == 0) {
    cfg->executorWorkPoolSize = 4;
    if (cpuNum > 16) {
      cfg->executorWorkPoolSize = 8;
    }
    LOG(INFO) << "adaptSomeThreadNumByCpuNum executorWorkPoolSize:"
              << cfg->executorWorkPoolSize;
  }

  if (cfg->executorThreadNum == 0) {
    uint32_t threadnum = static_cast<uint32_t>(cpuNum * 1.5);
    threadnum = std::max(uint32_t(8), threadnum);
    threadnum = std::min(uint32_t(56), threadnum);

    if (threadnum % cfg->executorWorkPoolSize != 0) {
      threadnum =
        (threadnum / cfg->executorWorkPoolSize + 1) * cfg->executorWorkPoolSize;
    }

    cfg->executorThreadNum = threadnum;
    LOG(INFO) << "adaptSomeThreadNumByCpuNum executorThreadNum:"
              << cfg->executorThreadNum;
  }

  if (cfg->netIoThreadNum == 0) {
    uint32_t threadnum = static_cast<uint32_t>(cpuNum / 4);
    threadnum = std::max(uint32_t(2), threadnum);
    threadnum = std::min(uint32_t(12), threadnum);
    cfg->netIoThreadNum = threadnum;
    LOG(INFO) << "adaptSomeThreadNumByCpuNum netIoThreadNum:"
              << cfg->netIoThreadNum;
  }
  return {ErrorCodes::ERR_OK, ""};
}

extern string gRenameCmdList;
extern string gMappingCmdList;
Status ServerEntry::startup(const std::shared_ptr<ServerParams>& cfg) {
  std::lock_guard<std::mutex> lk(_mutex);

  LOG(INFO) << "ServerEntry::startup,,,";

  auto ret = adaptSomeThreadNumByCpuNum(cfg);
  if (!ret.ok()) {
    return ret;
  }

  uint32_t kvStoreCount = cfg->kvStoreCount;
  uint32_t chunkSize = cfg->chunkSize;
  _cursorMaps.resize(cfg->dbNum);

  // set command config
  Command::changeCommand(gRenameCmdList, "rename");
  Command::changeCommand(gMappingCmdList, "mapping");

  // catalog init
  auto catalog = std::make_unique<Catalog>(
    std::move(std::unique_ptr<KVStore>(
      new RocksKVStore(CATALOG_NAME,
                       cfg,
                       nullptr,
                       false,
                       KVStore::StoreMode::READ_WRITE,
                       RocksKVStore::TxnMode::TXN_PES))),
    kvStoreCount,
    chunkSize,
    cfg->binlogUsingDefaultCF);
  installCatalog(std::move(catalog));
  // forward compatibilty : binlogVersion is used to check whether binlog
  // column_family exists
  // ToDo : while starting server, if we process data produced by older
  // version, we need to migrate binlog from default columnf to binlog columnf
  uint32_t flag = 0;
  if (_catalog->getBinlogVersion() == BinlogVersion::BINLOG_VERSION_1) {
    if (cfg->binlogUsingDefaultCF == false) {
      // if data is produced by one-clolumn db, we want to start it with
      // two-column db, we need to transfer binlog from default_CF to
      // binlog_CF
      LOG(INFO) << "binlogVersion is "
                << std::to_string((uint64_t)_catalog->getBinlogVersion())
                << ", binlogUsingDefaultCF is false, we start transfering "
                   "binlog from default_CF to binlog_CF";
      // INVARIANT(0);
      flag |= ROCKS_FLAGS_BINLOGVERSION_CHANGED;
    }
  } else if (_catalog->getBinlogVersion() == BinlogVersion::BINLOG_VERSION_2) {
    if (cfg->binlogUsingDefaultCF == true) {
      LOG(FATAL) << "binlogVersion is "
                 << (uint64_t)_catalog->getBinlogVersion()
                 << ",we need to set binlogUsingDefaultCF to false";
      exit(-1);
    }
  } else {
    INVARIANT_D(0);
  }

  // kvstore init
  _blockCache = rocksdb::NewLRUCache(
    cfg->rocksBlockcacheMB * 1024 * 1024LL, cfg->rocksBlockcacheNumShardBits,
    cfg->rocksStrictCapacityLimit);
  std::vector<PStore> tmpStores;
  tmpStores.reserve(kvStoreCount);
  for (size_t i = 0; i < kvStoreCount; ++i) {
    auto meta = _catalog->getStoreMainMeta(i);
    KVStore::StoreMode mode = KVStore::StoreMode::READ_WRITE;

    if (meta.ok()) {
      mode = meta.value()->storeMode;
    } else if (meta.status().code() == ErrorCodes::ERR_NOTFOUND) {
      auto pMeta = std::unique_ptr<StoreMainMeta>(
        new StoreMainMeta(i, KVStore::StoreMode::READ_WRITE));
      Status s = _catalog->setStoreMainMeta(*pMeta);
      if (!s.ok()) {
        LOG(FATAL) << "catalog setStoreMainMeta error:" << s.toString();
        return s;
      }
    } else {
      LOG(FATAL) << "catalog getStoreMainMeta error:"
                 << meta.status().toString();
      return meta.status();
    }

    tmpStores.emplace_back(
      std::unique_ptr<KVStore>(new RocksKVStore(std::to_string(i),
                                                cfg,
                                                _blockCache,
                                                true,
                                                mode,
                                                RocksKVStore::TxnMode::TXN_PES,
                                                flag)));
  }

  // if binlogUsingDefaultCF is flase and binlog version is 1, we end up
  // initing kvstore with two cf.
  if (cfg->binlogUsingDefaultCF == false &&
      _catalog->getBinlogVersion() == BinlogVersion::BINLOG_VERSION_1) {
    auto pMeta = std::unique_ptr<MainMeta>(
      new MainMeta(kvStoreCount, chunkSize, BinlogVersion::BINLOG_VERSION_2));
    Status s = _catalog->setMainMeta(*pMeta);
    if (!s.ok()) {
      LOG(FATAL) << "catalog setMainMeta error:" << s.toString();
      INVARIANT(0);
    }
    LOG(INFO) << "we finish transfering "
                 "binlog from default_CF to binlog_CF";
  }

  installStoresInLock(tmpStores);
  INVARIANT_D(getKVStoreCount() == kvStoreCount);
  LOG(INFO) << "enable cluster flag is " << _enableCluster;

  auto tmpSegMgr =
    std::unique_ptr<SegmentMgr>(new SegmentMgrFnvHash64(_kvstores, chunkSize));
  installSegMgrInLock(std::move(tmpSegMgr));

  // pessimisticMgr
  auto tmpPessimisticMgr = std::make_unique<PessimisticMgr>(kvStoreCount);
  installPessimisticMgrInLock(std::move(tmpPessimisticMgr));

  auto tmpMGLockMgr = std::make_unique<mgl::MGLockMgr>();
  installMGLockMgrInLock(std::move(tmpMGLockMgr));

  for (uint32_t i = 0; i < _cfg->executorThreadNum;
       i += _cfg->executorWorkPoolSize) {
    // TODO(takenliu): make sure whether multi worker_pool is ok?
    // But each size of worker_pool should been not less than 8;
    // uint32_t i = 0;
    uint32_t curNum = i + _cfg->executorWorkPoolSize < _cfg->executorThreadNum
      ? _cfg->executorWorkPoolSize
      : _cfg->executorThreadNum - i;
    LOG(INFO) << "ServerEntry::startup WorkerPool thread num:" << curNum;
    auto executor = std::make_unique<WorkerPool>(
      "tx-worker-" + std::to_string(i), _poolMatrix);
    Status s = executor->startup(curNum);
    if (!s.ok()) {
      LOG(ERROR) << "ServerEntry::startup failed, executor->startup:"
                 << s.toString();
      return s;
    }
    _executorList.push_back(std::move(executor));
  }

  // set the executorThreadNum
  _cfg->executorThreadNum = 0;
  for (auto& pool : _executorList) {
    _cfg->executorThreadNum += pool->size();
  }

  // _cfg->executorThreadNum = _executorList.size() *
  // _executorList.back()->size(); network
  _network = std::make_unique<NetworkAsio>(
    shared_from_this(), _netMatrix, _reqMatrix, cfg);
  Status s = _network->prepare(cfg->bindIp, cfg->port, cfg->netIoThreadNum);
  if (!s.ok()) {
    LOG(ERROR) << "ServerEntry::startup failed, _network->prepare:"
               << s.toString() << " ip:" << cfg->bindIp
               << " port:" << cfg->port;
    return s;
  }
  LOG(INFO) << "_network->prepare ok. ip :" << cfg->bindIp
            << " port:" << cfg->port;

  // replication
  // replication relys on blocking-client
  // must startup after network prepares ok
  _replMgr = std::make_unique<ReplManager>(shared_from_this(), cfg);
  s = _replMgr->startup();
  if (!s.ok()) {
    LOG(ERROR) << "ServerEntry::startup failed, _replMgr->startup:"
               << s.toString();
    return s;
  }

  // cluster init
  /*(NOTE) wayenchen indexMgr need get task map size from migrateMgr, so init it
   * first */
  if (_enableCluster) {
    _clusterMgr = std::make_unique<ClusterManager>(shared_from_this());

    Status s = _clusterMgr->startup();
    if (!s.ok()) {
      LOG(WARNING) << "start up cluster manager failed!";
      return s;
    }

    _migrateMgr = std::make_unique<MigrateManager>(shared_from_this(), cfg);
    s = _migrateMgr->startup();
    if (!s.ok()) {
      LOG(WARNING) << "start up migrate manager failed!";
      return s;
    }

    _gcMgr = std::make_unique<GCManager>(shared_from_this());
    s = _gcMgr->startup();
    if (!s.ok()) {
      LOG(WARNING) << "start up gc manager failed";
      return s;
    }
  }

  _scriptMgr = std::make_unique<ScriptManager>(shared_from_this());
  // TODO(takenliu): change executorThreadNum dynamic
  s = _scriptMgr->startup(_cfg->executorThreadNum);
  if (!s.ok()) {
    LOG(WARNING) << "start up ScriptManager failed";
    return s;
  }

  _indexMgr = std::make_unique<IndexManager>(shared_from_this(), cfg);
  s = _indexMgr->startup();
  if (!s.ok()) {
    LOG(ERROR) << "ServerEntry::startup failed, _indexMgr->startup:"
               << s.toString();
    return s;
  }

  // listener should be the lastone to run.
  s = _network->run();
  if (!s.ok()) {
    LOG(ERROR) << "ServerEntry::startup failed, _network->run:" << s.toString();
    return s;
  } else {
    LOG(WARNING) << "ready to accept connections at " << cfg->bindIp << ":"
                 << cfg->port;
  }

  _isRunning.store(true, std::memory_order_relaxed);
  _isStopped.store(false, std::memory_order_relaxed);

  // server stats monitor
  _cronThd = std::make_unique<std::thread>([this] {
    INVARIANT(!pthread_setname_np(pthread_self(), "tx-svr-cron"));
    serverCron();
  });

  // init slowlog
  _slowlogStat.initSlowlogFile(cfg->slowlogPath);

  _lastJeprofDumpMemoryGB = 0;


  LOG(INFO) << "ServerEntry::startup sucess.";
  return {ErrorCodes::ERR_OK, ""};
}

uint64_t ServerEntry::getStartupTimeNs() const {
  return _startupTime;
}

NetworkAsio* ServerEntry::getNetwork() {
  return _network.get();
}

ReplManager* ServerEntry::getReplManager() {
  return _replMgr.get();
}

MigrateManager* ServerEntry::getMigrateManager() {
  return _migrateMgr.get();
}

SegmentMgr* ServerEntry::getSegmentMgr() const {
  return _segmentMgr.get();
}

PessimisticMgr* ServerEntry::getPessimisticMgr() {
  return _pessimisticMgr.get();
}

mgl::MGLockMgr* ServerEntry::getMGLockMgr() {
  return _mgLockMgr.get();
}

IndexManager* ServerEntry::getIndexMgr() {
  return _indexMgr.get();
}

ClusterManager* ServerEntry::getClusterMgr() {
  return _clusterMgr.get();
}

GCManager* ServerEntry::getGcMgr() {
  return _gcMgr.get();
}

ScriptManager* ServerEntry::getScriptMgr() {
  return _scriptMgr.get();
}

std::string ServerEntry::requirepass() const {
  std::lock_guard<std::mutex> lk(_mutex);
  return _requirepass;
}

void ServerEntry::setRequirepass(const string& v) {
  std::lock_guard<std::mutex> lk(_mutex);
  _requirepass = v;
}

std::string ServerEntry::masterauth() const {
  std::lock_guard<std::mutex> lk(_mutex);
  return _masterauth;
}

void ServerEntry::setMasterauth(const string& v) {
  std::lock_guard<std::mutex> lk(_mutex);
  _masterauth = v;
}

bool ServerEntry::versionIncrease() const {
  return _cfg->versionIncrease;
}

bool ServerEntry::addSession(std::shared_ptr<Session> sess) {
  std::lock_guard<std::mutex> lk(_mutex_session);
  if (!_isRunning.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "session:" << sess->id()
                 << " comes when stopping, ignore it";
    return false;
  }

  // NOTE(deyukong): first driving force
  sess->start();
  uint64_t id = sess->id();
  if (_sessions.find(id) != _sessions.end()) {
    INVARIANT_D(0);
    LOG(ERROR) << "add session:" << id << ",session id already exists";
  }
#ifdef TENDIS_DEBUG
  if (sess->getType() != Session::Type::LOCAL) {
    DLOG(INFO) << "ServerEntry addSession id:" << id
               << " addr:" << sess->getRemote()
               << " type:" << sess->getTypeStr();
  }
#endif
  _sessions[id] = std::move(sess);
  return true;
}

std::shared_ptr<Session> ServerEntry::getSession(uint64_t id) const {
  std::lock_guard<std::mutex> lk(_mutex_session);
  auto it = _sessions.find(id);
  if (it == _sessions.end()) {
    return nullptr;
  }

  return it->second;
}

size_t ServerEntry::getSessionCount() {
  std::lock_guard<std::mutex> lk(_mutex_session);
  return _sessions.size();
}

Status ServerEntry::cancelSession(uint64_t connId) {
  std::lock_guard<std::mutex> lk(_mutex_session);
  if (!_isRunning.load(std::memory_order_relaxed)) {
    return {ErrorCodes::ERR_BUSY, "server is shutting down"};
  }
  auto it = _sessions.find(connId);
  if (it == _sessions.end()) {
    return {ErrorCodes::ERR_NOTFOUND,
            "session not found:" + std::to_string(connId)};
  }
  LOG(INFO) << "ServerEntry cancelSession id:" << connId
            << " addr:" << it->second->getRemote();
  return it->second->cancel();
}
//
void ServerEntry::endSession(uint64_t connId) {
  std::lock_guard<std::mutex> lk(_mutex_session);
  if (!_isRunning.load(std::memory_order_relaxed)) {
    return;
  }
  auto it = _sessions.find(connId);
  if (it == _sessions.end()) {
    // NOTE(vinchen): ServerEntry::endSession() is called by
    // NetSession::endSession(), but it is not holding NetSession::_mutex
    // So here is possible now.
    LOG(ERROR) << "destroy conn:" << connId << ",not exists";
    return;
  }
  SessionCtx* pCtx = it->second->getCtx();
  INVARIANT(pCtx != nullptr);
  if (pCtx->getIsMonitor()) {
    DelMonitorNoLock(connId);
  }
#ifdef TENDIS_DEBUG
  if (it->second->getType() != Session::Type::LOCAL) {
    DLOG(INFO) << "ServerEntry endSession id:" << connId
               << " addr:" << it->second->getRemote()
               << " type:" << it->second->getTypeStr();
  }
#endif
  _sessions.erase(it);
}

std::list<std::shared_ptr<Session>> ServerEntry::getAllSessions() const {
  std::lock_guard<std::mutex> lk(_mutex_session);
  uint64_t start = nsSinceEpoch();
  std::list<std::shared_ptr<Session>> sesses;
  for (const auto& kv : _sessions) {
    sesses.push_back(kv.second);
  }
  uint64_t delta = (nsSinceEpoch() - start) / 1000000;
  if (delta >= 5) {
    LOG(WARNING) << "get sessions cost:" << delta << "ms"
                 << "length:" << sesses.size();
  }
  return sesses;
}

void ServerEntry::AddMonitor(uint64_t sessId) {
  std::lock_guard<std::mutex> lk(_mutex_session);
  for (const auto& monSess : _monitors) {
    if (monSess->id() == sessId) {
      return;
    }
  }
  auto it = _sessions.find(sessId);
  if (it == _sessions.end()) {
    LOG(ERROR) << "AddMonitor session not found:" << sessId;
    return;
  }

  _monitors.push_back(it->second);
}

void ServerEntry::DelMonitorNoLock(uint64_t connId) {
  for (auto it = _monitors.begin(); it != _monitors.end(); ++it) {
    if (it->get()->id() == connId) {
      _monitors.erase(it);
      break;
    }
  }
}

/**
 * @brief update func to resize workpool thread num
 * @note like WorkerPool::resize(), three cases
 *     operation on container should be thread-safe,
 *     use std::lock_guard to make thread safe
 */
void ServerEntry::resizeExecutorThreadNum(uint64_t newThreadNum) {
  std::lock_guard<std::mutex> lk(_mutex);
  auto threadSum = _executorList.size() * _executorList.back()->size();
  if (newThreadNum < threadSum) {
    resizeDecrExecutorThreadNum(newThreadNum);
  } else if (newThreadNum > threadSum) {
    resizeIncrExecutorThreadNum(newThreadNum);
  } else {
    return;
  }
}

/**
 * @brief decrease _executorList thread number
 * @note Because of resize() interface is async decrease operation,
 *      can't stop() directly, so move target workerpool to _executorSet first,
 * keep owning the std::unique_ptr. stop the workerpool eventually when call
 * another decrease operation， this design means: ergodic operation pressure is
 * on DECREASE OP, NOT INCREASE OP.
 */
void ServerEntry::resizeDecrExecutorThreadNum(uint64_t newThreadNum) {
  // calculate the threads sum -> threadNum, the list num to resize() ->
  // listNum
  auto threadSum = _executorList.size() * _executorList.back()->size();
  size_t listNum = (threadSum - newThreadNum) / _cfg->executorWorkPoolSize;
  INVARIANT_D(newThreadNum < threadSum);

  // call resize() op, move workerpool to _executorSet.
  for (size_t i = 0; i < listNum; ++i) {
    _executorList.back()->resize(0);
    _executorRecycleSet.emplace(std::move(_executorList.back().release()));
    _executorList.pop_back();
  }
}

/**
 * @brief increase _executorList thread number
 * @note maybe can reuse workerpool which in _executorSet,
 *      but may put the ergodic operation pressure on INCREASE OP.
 *      As we all known, DECREASE OP's pressure is less than INCREASE OP's.
 * @todo workerpool's name now may be repetitive, should fix it later.
 */
void ServerEntry::resizeIncrExecutorThreadNum(uint64_t newThreadNum) {
  auto threadSum = _executorList.size() * _executorList.back()->size();
  size_t listNum = (newThreadNum - threadSum) / _cfg->executorWorkPoolSize;
  INVARIANT_D(newThreadNum > threadSum);

  for (size_t i = 0; i < listNum; ++i) {
    auto executor = std::make_unique<WorkerPool>(
      "tx-worker-" + std::to_string(_executorList.size() + i), _poolMatrix);
    Status s = executor->startup(_executorList.back()->size());
    if (!s.ok()) {
      LOG(ERROR) << "ServerEntry::startup failed, executor->startup:"
                 << s.toString();
    }
    _executorList.push_back(std::move(executor));
  }
}

string catRepr(const string& val) {
  size_t len = val.length();
  size_t i = 0;
  std::stringstream s;
  s << "\"";
  char buf[5];
  while (i < len) {
    switch (val[i]) {
      case '\\':
      case '"':
        s << "\\" << val[i];
        break;
      case '\n': s << "\\n"; break;
      case '\r': s << "\\r"; break;
      case '\t': s << "\\t"; break;
      case '\a': s << "\\a"; break;
      case '\b': s << "\\b"; break;
      default:
        if (isprint(val[i])) {
          s << val[i];
        } else {
          snprintf(buf, sizeof(buf), "\\x%02x", val[i]);
          s << buf;
        }
        break;
    }
    i++;
  }
  s << "\"";
  return s.str();
}

// TODO(takenliu) add gtest
void ServerEntry::replyMonitors(Session* sess) {
  if (_monitors.size() <= 0) {
    return;
  }

  stringstream info;
  info << "+";

  auto timeNow = std::chrono::duration_cast<std::chrono::microseconds>(
    std::chrono::system_clock::now().time_since_epoch());
  uint64_t timestamp = timeNow.count();

  SessionCtx* pCtx = sess->getCtx();
  INVARIANT(pCtx != nullptr);
  uint32_t dbId = pCtx->getDbId();

  info << std::to_string(timestamp / 1000000) << "." <<
    std::to_string(timestamp % 1000000);
  info << " [" << std::to_string(dbId) << " " << sess->getRemote() << "] ";
  const auto& args = sess->getArgs();
  for (uint32_t i = 0; i < args.size(); ++i) {
    info << catRepr(args[i]);
    if (i != (args.size() - 1)) {
      info << " ";
    }
  }
  info << "\r\n";

  std::lock_guard<std::mutex> lk(_mutex_session);
  for (auto iter = _monitors.begin(); iter != _monitors.end();) {
    auto s = (*iter)->setResponse(info.str());
    if (!s.ok()) {
      iter = _monitors.erase(iter);
    } else {
      ++iter;
    }
  }
}

bool ServerEntry::processRequest(Session* sess) {
  if (!_isRunning.load(std::memory_order_relaxed)) {
    return false;
  }
  // general log if nessarry
  sess->getServerEntry()->logGeneral(sess);

  auto expCmd = Command::precheck(sess);
  if (!expCmd.ok()) {
    auto s =
      sess->setResponse(redis_port::errorReply(expCmd.status().toString()));
    if (!s.ok()) {
      return false;
    }
    return true;
  }

  replyMonitors(sess);

  if (expCmd.value()->isBgCmd()) {
    auto expCmdName = expCmd.value()->getName();
    if (expCmdName == "fullsync") {
      LOG(WARNING) << "[master] session id:" << sess->id()
                   << " socket borrowed";
      NetSession* ns = dynamic_cast<NetSession*>(sess);
      INVARIANT(ns != nullptr);
      std::vector<std::string> args = ns->getArgs();
      // we have called precheck, it should have 4 args
      INVARIANT(args.size() == 4);
      _replMgr->supplyFullSync(ns->borrowConn(), args[1], args[2], args[3]);
      ++_serverStat.syncFull;
      return false;
    } else if (expCmdName == "incrsync") {
      LOG(WARNING) << "[master] session id:" << sess->id()
                   << " socket borrowed";
      NetSession* ns = dynamic_cast<NetSession*>(sess);
      INVARIANT(ns != nullptr);
      std::vector<std::string> args = ns->getArgs();
      // we have called precheck, it should have 2 args
      INVARIANT(args.size() == 6);
      bool ret = _replMgr->registerIncrSync(
        ns->borrowConn(), args[1], args[2], args[3], args[4], args[5]);
      if (ret) {
        ++_serverStat.syncPartialOk;
      } else {
        ++_serverStat.syncPartialErr;
      }
      return false;
    } else if (expCmdName == "readymigrate") {
      LOG(WARNING) << "[source] session id:" << sess->id()
                   << " socket borrowed";
      NetSession* ns = dynamic_cast<NetSession*>(sess);
      INVARIANT(ns != nullptr);
      std::vector<std::string> args = ns->getArgs();
      // we have called precheck, it should have 2 args
      // INVARIANT(args.size() == 4);
      _migrateMgr->dstReadyMigrate(
        ns->borrowConn(), args[1], args[2], args[3], args[4]);
      return false;
    } else if (expCmdName == "preparemigrate") {
      LOG(INFO) << "prepare migrate command";
      NetSession* ns = dynamic_cast<NetSession*>(sess);
      INVARIANT(ns != nullptr);
      std::vector<std::string> args = ns->getArgs();
      auto esNum = ::tendisplus::stoul(args[4]);
      if (!esNum.ok())
        LOG(ERROR) << "Invalid store num:" << args[4];
      uint32_t storeNum = esNum.value();
      _migrateMgr->dstPrepareMigrate(
        ns->borrowConn(), args[1], args[2], args[3], storeNum);
      return false;
    } else if (expCmdName == "quit") {
      LOG(INFO) << "quit command";
      NetSession* ns = dynamic_cast<NetSession*>(sess);
      INVARIANT(ns != nullptr);
      ns->setCloseAfterRsp();
      auto s = ns->setResponse(Command::fmtOK());
      if (!s.ok()) {
        return false;
      }
      return true;
    } else if (expCmdName == "psync") {
      NetSession* ns = dynamic_cast<NetSession*>(sess);
      INVARIANT(ns != nullptr);

      if (!_cfg->aofEnabled) {
        ns->setResponse(Command::fmtErr("aof psync enable first"));
        return true;
      }

      std::vector<std::string> args = ns->getArgs();
      if (args[1] != "?" || args[2] != "-1") {
        ns->setResponse(Command::fmtErr("just support ? -1"));
        return true;
      }

      if (args[3].empty()) {
        ns->setResponse(Command::fmtErr("need rocksdb number"));
        return true;
      }

      _replMgr->supplyFullPsync(ns->borrowConn(), args[3]);
      return false;
    }
  }

  auto expect = Command::runSessionCmd(sess);
  if (!expect.ok()) {
    auto s = sess->setResponse(Command::fmtErr(expect.status().toString()));
    if (!s.ok()) {
      return false;
    }
    DLOG(ERROR) << "Command::runSessionCmd failed, cmd:" << sess->getCmdStr()
                << " err:" << expect.status().toString();
    return true;
  }
  auto s = sess->setResponse(expect.value());
  if (!s.ok()) {
    return false;
  }
  return true;
}

void ServerEntry::getStatInfo(std::stringstream& ss) const {
  ss << "total_connections_received:" << _netMatrix->connCreated.get()
     << "\r\n";
  ss << "total_connections_released:" << _netMatrix->connReleased.get()
     << "\r\n";
  auto executed = _reqMatrix->processed.get();
  ss << "total_commands_processed:" << executed << "\r\n";
  ss << "instantaneous_ops_per_sec:"
     << _serverStat.getInstantaneousMetric(STATS_METRIC_COMMAND) << "\r\n";

  auto allCost = _poolMatrix->executeTime.get() + _poolMatrix->queueTime.get() +
    _reqMatrix->sendPacketCost.get();
  ss << "total_commands_cost(ns):" << allCost << "\r\n";
  ss << "total_commands_workpool_queue_cost(ns):"
     << _poolMatrix->queueTime.get() << "\r\n";
  ss << "total_commands_workpool_execute_cost(ns):"
     << _poolMatrix->executeTime.get() << "\r\n";
  ss << "total_commands_send_packet_cost(ns):"
     << _reqMatrix->sendPacketCost.get() << "\r\n";
  ss << "total_commands_execute_cost(ns):" << _reqMatrix->processCost.get()
     << "\r\n";

  if (executed == 0)
    executed = 1;
  ss << "avg_commands_cost(ns):" << allCost / executed << "\r\n";
  ss << "avg_commands_workpool_queue_cost(ns):"
     << _poolMatrix->queueTime.get() / executed << "\r\n";
  ss << "avg_commands_workpool_execute_cost(ns):"
     << _poolMatrix->executeTime.get() / executed << "\r\n";
  ss << "avg_commands_send_packet_cost(ns):"
     << _reqMatrix->sendPacketCost.get() / executed << "\r\n";
  ss << "avg_commands_execute_cost(ns):"
     << _reqMatrix->processCost.get() / executed << "\r\n";

  ss << "commands_in_queue:" << _poolMatrix->inQueue.get() << "\r\n";
  ss << "commands_executed_in_workpool:" << _poolMatrix->executed.get()
     << "\r\n";

  ss << "total_stricky_packets:" << _netMatrix->stickyPackets.get() << "\r\n";
  ss << "total_invalid_packets:" << _netMatrix->invalidPackets.get() << "\r\n";

  ss << "total_net_input_bytes:" << _serverStat.netInputBytes.get() << "\r\n";
  ss << "total_net_output_bytes:" << _serverStat.netOutputBytes.get() << "\r\n";
  ss << "instantaneous_input_kbps:"
     << static_cast<float>(
          _serverStat.getInstantaneousMetric(STATS_METRIC_NET_INPUT)) /
      1024
     << "\r\n";
  ss << "instantaneous_output_kbps:"
     << static_cast<float>(
          _serverStat.getInstantaneousMetric(STATS_METRIC_NET_OUTPUT)) /
      1024
     << "\r\n";
  ss << "rejected_connections:" << _serverStat.rejectedConn.get() << "\r\n";
  ss << "sync_full:" << _serverStat.syncFull.get() << "\r\n";
  ss << "sync_partial_ok:" << _serverStat.syncPartialOk.get() << "\r\n";
  ss << "sync_partial_err:" << _serverStat.syncPartialErr.get() << "\r\n";
  ss << "keyspace_hits:" << _serverStat.keyspaceHits.get() << "\r\n";
  ss << "keyspace_misses:" << _serverStat.keyspaceMisses.get() << "\r\n";
  ss << "keyspace_wrong_versionep:" << _serverStat.keyspaceIncorrectEp.get()
     << "\r\n";
  ss << "scheduleNum:" << _scheduleNum << "\r\n";
}

void ServerEntry::appendJSONStat(
  rapidjson::PrettyWriter<rapidjson::StringBuffer>& w,
  const std::set<std::string>& sections) const {
  if (sections.find("network") != sections.end()) {
    w.Key("network");
    w.StartObject();
    w.Key("sticky_packets");
    w.Uint64(_netMatrix->stickyPackets.get());
    w.Key("conn_created");
    w.Uint64(_netMatrix->connCreated.get());
    w.Key("conn_released");
    w.Uint64(_netMatrix->connReleased.get());
    w.Key("invalid_packets");
    w.Uint64(_netMatrix->invalidPackets.get());
    w.EndObject();
  }
  if (sections.find("request") != sections.end()) {
    w.Key("request");
    w.StartObject();
    w.Key("processed");
    w.Uint64(_reqMatrix->processed.get());
    w.Key("process_cost");
    w.Uint64(_reqMatrix->processCost.get());
    w.Key("send_packet_cost");
    w.Uint64(_reqMatrix->sendPacketCost.get());
    w.EndObject();
  }
  if (sections.find("req_pool") != sections.end()) {
    w.Key("req_pool");
    w.StartObject();
    w.Key("in_queue");
    w.Uint64(_poolMatrix->inQueue.get());
    w.Key("executed");
    w.Uint64(_poolMatrix->executed.get());
    w.Key("queue_time");
    w.Uint64(_poolMatrix->queueTime.get());
    w.Key("execute_time");
    w.Uint64(_poolMatrix->executeTime.get());
    w.EndObject();
  }
}

bool ServerEntry::getTotalIntProperty(Session* sess,
                                      const std::string& property,
                                      uint64_t* value,
                                      ColumnFamilyNumber cf) const {
  *value = 0;
  for (uint64_t i = 0; i < getKVStoreCount(); i++) {
    auto expdb =
      getSegmentMgr()->getDb(sess, i, mgl::LockMode::LOCK_IS, false, 0);
    if (!expdb.ok()) {
      return false;
    }

    auto store = expdb.value().store;
    uint64_t tmp = 0;
    bool ok = store->getIntProperty(property, &tmp, cf);
    if (!ok) {
      return false;
    }
    *value += tmp;
  }

  return true;
}

uint64_t ServerEntry::getStatCountByName(Session* sess,
  const std::string& ticker) const {
  uint64_t value = 0;
  for (uint64_t i = 0; i < getKVStoreCount(); i++) {
    auto expdb =
      getSegmentMgr()->getDb(sess, i, mgl::LockMode::LOCK_IS, false, 0);
    if (!expdb.ok()) {
      return 0;
    }

    auto store = expdb.value().store;
    value += store->getStatCountByName(ticker);
  }

  return value;
}

bool ServerEntry::isDbEmpty() {
  for (uint32_t i = 0; i < getKVStoreCount(); ++i) {
    auto expdb = getSegmentMgr()->getDb(nullptr, i, mgl::LockMode::LOCK_IS);
    if (!expdb.ok()) {
      LOG(ERROR) << "get db lock fail:" << expdb.status().toString();
      return false;
    }

    auto kvstore = std::move(expdb.value().store);
    auto eTxn = kvstore->createTransaction(nullptr);
    if (!eTxn.ok()) {
      LOG(ERROR) << "createTransaction failed:" << eTxn.status().toString();
      return false;
    }

    auto cursor = eTxn.value()->createDataCursor();
    cursor->seek("");
    auto exptRcd = cursor->next();

    if (exptRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
      continue;
    } else {
      return false;
    }
  }
  return true;
}

bool ServerEntry::getAllProperty(Session* sess,
                                 const std::string& property,
                                 std::string* value) const {
  std::stringstream ss;
  for (uint64_t i = 0; i < getKVStoreCount(); i++) {
    auto expdb = getSegmentMgr()->getDb(sess, i, mgl::LockMode::LOCK_IS);
    if (!expdb.ok()) {
      return false;
    }

    auto store = expdb.value().store;
    std::string tmp;
    bool ok = store->getProperty(property, &tmp);
    if (!ok) {
      return false;
    }
    ss << "store_" << store->dbId() << ":" << tmp << "\r\n";
  }
  *value = ss.str();

  return true;
}

void ServerEntry::resetRocksdbStats(Session* sess) {
  std::stringstream ss;
  for (uint64_t i = 0; i < getKVStoreCount(); i++) {
    auto expdb = getSegmentMgr()->getDb(sess, i, mgl::LockMode::LOCK_IS);
    if (!expdb.ok()) {
      continue;
    }

    auto store = expdb.value().store;
    store->resetStatistics();
  }
}

Status ServerEntry::destroyStore(Session* sess,
                                 uint32_t storeId,
                                 bool isForce) {
  auto expdb = getSegmentMgr()->getDb(sess, storeId, mgl::LockMode::LOCK_X);
  if (!expdb.ok()) {
    return expdb.status();
  }

  auto store = expdb.value().store;
  if (!isForce) {
    if (!store->isEmpty()) {
      return {ErrorCodes::ERR_INTERNAL, "try to close an unempty store"};
    }
  }

  if (!store->isPaused()) {
    return {ErrorCodes::ERR_INTERNAL,
            "please pausestore first before destroystore"};
  }

  if (store->getMode() == KVStore::StoreMode::READ_WRITE) {
    // TODO(vinchen)
    // NOTE(vinchen): maybe it should create a binlog here to
    // destroy the store of slaves.
    // But it maybe hard to confirm whether all the slaves apply
    // this binlog before the master destroy. (check MPOVStatus?)
  }

  auto meta = getCatalog()->getStoreMainMeta(storeId);
  if (!meta.ok()) {
    LOG(WARNING) << "get store main meta:" << storeId
                 << " failed:" << meta.status().toString();
    return meta.status();
  }
  meta.value()->storeMode = KVStore::StoreMode::STORE_NONE;
  Status status = getCatalog()->setStoreMainMeta(*meta.value());
  if (!status.ok()) {
    LOG(WARNING) << "set store main meta:" << storeId
                 << " failed:" << status.toString();
    return status;
  }

  status = _migrateMgr->stopStoreTask(storeId);
  if (!status.ok()) {
    LOG(ERROR) << "migrateMgr stopStore :" << storeId
               << " failed:" << status.toString();
    return status;
  }

  status = store->destroy();
  if (!status.ok()) {
    LOG(ERROR) << "destroy store :" << storeId
               << " failed:" << status.toString();
    return status;
  }
  INVARIANT_D(store->getMode() == KVStore::StoreMode::STORE_NONE);

  status = _replMgr->stopStore(storeId);
  if (!status.ok()) {
    LOG(ERROR) << "replMgr stopStore :" << storeId
               << " failed:" << status.toString();
    return status;
  }

  status = _gcMgr->stopStoreTask(storeId);
  if (!status.ok()) {
    LOG(ERROR) << "gcMgr stopStore :" << storeId
               << " failed:" << status.toString();
    return status;
  }

  status = _scriptMgr->stopStore(storeId);
  if (!status.ok()) {
    LOG(ERROR) << "_scriptMgr stopStore :" << storeId
               << " failed:" << status.toString();
    return status;
  }

  if (_indexMgr) {
    status = _indexMgr->stopStore(storeId);
    if (!status.ok()) {
      LOG(ERROR) << "indexMgr stopStore :" << storeId
                 << " failed:" << status.toString();
      return status;
    }
  }

  return status;
}

Status ServerEntry::setStoreMode(PStore store, KVStore::StoreMode mode) {
  // assert held the X lock of store
  if (store->getMode() == mode) {
    return {ErrorCodes::ERR_OK, ""};
  }

  auto catalog = getCatalog();
  Status status = store->setMode(mode);
  if (!status.ok()) {
    LOG(FATAL) << "ServerEntry::setStoreMode error, " << status.toString();
    return status;
  }
  auto storeId = tendisplus::stoul(store->dbId());
  if (!storeId.ok()) {
    return storeId.status();
  }
  auto meta = catalog->getStoreMainMeta(storeId.value());
  meta.value()->storeMode = mode;

  return catalog->setStoreMainMeta(*meta.value());
}

#define run_with_period(_ms_) \
  if ((_ms_ <= 1000 / hz) || !(cronLoop % ((_ms_) / (1000 / hz))))

void ServerEntry::serverCron() {
  using namespace std::chrono_literals;  // NOLINT(build/namespaces)

  auto oldNetMatrix = *_netMatrix;
  auto oldPoolMatrix = *_poolMatrix;
  auto oldReqMatrix = *_reqMatrix;

  uint64_t cronLoop = 0;
  auto interval = 100ms;  // every 100ms execute one time
  uint64_t hz = 1000ms / interval;

  LOG(INFO) << "serverCron thread starts, hz:" << hz;
  while (_isRunning.load(std::memory_order_relaxed)) {
    std::unique_lock<std::mutex> lk(_mutex);

    bool ok = _eventCV.wait_for(lk, interval, [this] {
      return _isRunning.load(std::memory_order_relaxed) == false;
    });
    if (ok) {
      LOG(INFO) << "serverCron thread exits";
      return;
    }

    run_with_period(100) {
      _serverStat.trackInstantaneousMetric(STATS_METRIC_COMMAND,
                                           _reqMatrix->processed.get());
      _serverStat.trackInstantaneousMetric(STATS_METRIC_NET_INPUT,
                                           _serverStat.netInputBytes.get());
      _serverStat.trackInstantaneousMetric(STATS_METRIC_NET_OUTPUT,
                                           _serverStat.netOutputBytes.get());
    }

    run_with_period(1000) {
      // release idling workerpool, trigger 1s once time
      if (_executorRecycleSet.size()) {
        for (auto iter = _executorRecycleSet.begin();
             iter != _executorRecycleSet.end();) {
          if (!(*iter)->size()) {
            (*iter)->stop();
            iter = _executorRecycleSet.erase(iter);
          } else {
            iter++;
          }
        }
      }

      // full-time matrix collect
      if (_ftmcEnabled.load(std::memory_order_relaxed)) {
        auto tmpNetMatrix = *_netMatrix - oldNetMatrix;
        auto tmpPoolMatrix = *_poolMatrix - oldPoolMatrix;
        auto tmpReqMatrix = *_reqMatrix - oldReqMatrix;
        oldNetMatrix = *_netMatrix;
        oldPoolMatrix = *_poolMatrix;
        oldReqMatrix = *_reqMatrix;
        // TODO(vinchen): we should create a view here
        LOG(INFO) << "network matrix status:\n" << tmpNetMatrix.toString();
        LOG(INFO) << "pool matrix status:\n" << tmpPoolMatrix.toString();
        LOG(INFO) << "req matrix status:\n" << tmpReqMatrix.toString();
      }
    }

    run_with_period(1000) {
      _scriptMgr->cron();
    }

    if (_cfg->jeprofAutoDump) {
      run_with_period(1000) {
        jeprofCron();
      }
    }

    cronLoop++;
  }
}

void ServerEntry::jeprofCron() {
  size_t rss_human_size = 0;
#ifndef _WIN32
  ifstream file;
  file.open("/proc/self/status");
  if (file.is_open()) {
    string strline;
    while (getline(file, strline)) {
      auto v = stringSplit(strline, ":");
      if (v.size() != 2) {
        continue;
      }
      if (v[0] == "VmRSS") {  // physic memory
        string used_memory_rss_human = trim(v[1]);
        strDelete(used_memory_rss_human, ' ');
        auto s = getIntSize(used_memory_rss_human);
        if (s.ok()) {
          rss_human_size = s.value();
        } else {
          LOG(ERROR) << "getIntSize failed:" << s.status().toString();
        }
      }
    }
  }
#endif  // !_WIN32
  uint32_t memoryGb = rss_human_size/1024/1024/1024;
  if (memoryGb > _lastJeprofDumpMemoryGB) {
    _lastJeprofDumpMemoryGB = memoryGb;
    LOG(INFO) << "jeprof dump memoryGb:" << memoryGb;

    mallctl("prof.dump", NULL, NULL, NULL, 0);
  }
}

void ServerEntry::waitStopComplete() {
  using namespace std::chrono_literals;  // NOLINT(build/namespaces)
  bool shutdowned = false;
  while (_isRunning.load(std::memory_order_relaxed)) {
    std::unique_lock<std::mutex> lk(_mutex);
    bool ok = _eventCV.wait_for(lk, 1000ms, [this] {
      return _isRunning.load(std::memory_order_relaxed) == false &&
        _isStopped.load(std::memory_order_relaxed) == true;
    });
    if (ok) {
      return;
    }

    if (_isShutdowned.load(std::memory_order_relaxed)) {
      LOG(INFO) << "_isShutdowned is true";
      shutdowned = true;
      break;
    }
  }

  // NOTE(vinchen): it can't hold the _mutex before stop()
  if (shutdowned) {
    stop();
  }
}

void ServerEntry::handleShutdownCmd() {
  _isShutdowned.store(true, std::memory_order_relaxed);
}

void ServerEntry::stop() {
  // TODO(takenliu) check _isRunning and _mutex
  if (_isRunning.load(std::memory_order_relaxed) == false) {
    LOG(INFO) << "server is stopping, plz donot kill again";
    return;
  }
  LOG(INFO) << "server begins to stop...";
  _isRunning.store(false, std::memory_order_relaxed);
  _eventCV.notify_all();
  _network->stop();

  // NOTE(takenliu): _scriptMgr need stop earlier than _executorList
  _scriptMgr->stop();

  for (auto& executor : _executorList) {
    executor->stop();
  }
  for (auto& executor : _executorRecycleSet) {
    executor->stop();
  }
  _replMgr->stop();
  if (_migrateMgr)
    _migrateMgr->stop();
  if (_indexMgr)
    _indexMgr->stop();
  {
    std::lock_guard<std::mutex> lk(_mutex_session);
    _sessions.clear();
  }
  if (_clusterMgr) {
    _clusterMgr->stop();
  }
  if (_gcMgr) {
    _gcMgr->stop();
  }

  if (!_isShutdowned.load(std::memory_order_relaxed)) {
    // NOTE(vinchen): if it's not the shutdown command, it should reset the
    // workerpool to decr the referent count of share_ptr<server>
    _network.reset();
    for (auto& executor : _executorList) {
      executor.reset();
    }
    _replMgr.reset();
    _migrateMgr.reset();
    if (_indexMgr)
      _indexMgr.reset();
    _pessimisticMgr.reset();
    _mgLockMgr.reset();
    _segmentMgr.reset();
    _clusterMgr.reset();
    _gcMgr.reset();
    _scriptMgr.reset();
  }

  // stop the rocksdb
  std::stringstream ss;
  Status status = _catalog->stop();
  if (!status.ok()) {
    ss << "stop kvstore catalog failed: " << status.toString();
    LOG(ERROR) << ss.str();
  }

  for (auto& store : _kvstores) {
    Status status = store->stop();
    if (!status.ok()) {
      ss.clear();
      ss << "stop kvstore " << store->dbId() << "failed: " << status.toString();
      LOG(ERROR) << ss.str();
    }
  }

  _cronThd->join();
  _slowlogStat.closeSlowlogFile();
  LOG(INFO) << "server stops complete...";
  _isStopped.store(true, std::memory_order_relaxed);
  _eventCV.notify_all();
}

void ServerEntry::toggleFtmc(bool enable) {
  _ftmcEnabled.store(enable, std::memory_order_relaxed);
}

uint64_t ServerEntry::getTsEp() const {
  return _tsFromExtendedProtocol.load(std::memory_order_relaxed);
}

void ServerEntry::setTsEp(uint64_t timestamp) {
  _tsFromExtendedProtocol.store(timestamp, std::memory_order_relaxed);
}

/*
# Id: 3
# Timestamp: 1587107891128222
# Time: 200417 15:18:11
# Host: 127.0.0.1:51271
# Db: 0
# Query_time: 2001014
tendisadmin sleep 2
*/
// in ms
void ServerEntry::slowlogPushEntryIfNeeded(uint64_t time,
                                           uint64_t duration,
                                           Session* sess) {
  if (sess && duration >= _cfg->slowlogLogSlowerThan) {
    _slowlogStat.slowlogDataPushEntryIfNeeded(time, duration, sess);
  }
}

std::shared_ptr<ServerEntry>& getGlobalServer() {
  static std::shared_ptr<ServerEntry> gServer;
  return gServer;
}

/**
 * @brief check whether slot belong to this kvstore
 * @param kvstoreId kvstoreId which slots belong to
 * @param slot slot which need to check
 * @return boolean, show whether slot belong to this kvstore
 */
bool checkKvstoreSlot(uint32_t kvstoreId, uint64_t slot) {
  return (slot % getGlobalServer()->getParams()->kvStoreCount) == kvstoreId;
}

}  // namespace tendisplus
