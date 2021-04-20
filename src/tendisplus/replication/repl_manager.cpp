// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <chrono>  // NOLINT
#include <algorithm>
#include <fstream>
#include <limits>
#include <list>
#include <map>
#include <set>
#include <string>

#include "glog/logging.h"
#include "rapidjson/document.h"
#include "rapidjson/error/en.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/lock/lock.h"
#include "tendisplus/network/network.h"
#include "tendisplus/replication/repl_manager.h"
#include "tendisplus/server/session.h"
#include "tendisplus/storage/record.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/rate_limiter.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/string.h"

namespace tendisplus {

const char* ReplStateStr[] = {
  "none", "connecting", "send_bulk", "online", "error"};

const char* FullPushStateStr[] = {
  "send_bulk",
  "send_bulk_success",
  "send_bulk_err",
};

template <typename T>
std::string getEnumStr(T element) {
  size_t size = 0;
  const char** arr = nullptr;
  if (typeid(T) == typeid(FullPushState)) {
    arr = FullPushStateStr;
    size = sizeof(FullPushStateStr) / sizeof(FullPushStateStr[0]);
  } else if (typeid(T) == typeid(ReplState)) {
    arr = ReplStateStr;
    size = sizeof(ReplStateStr) / sizeof(ReplStateStr[0]);
  } else {
    INVARIANT_D(0);
    return "not support";
  }

  uint32_t index = static_cast<uint32_t>(element);
  if (index >= size) {
    INVARIANT_D(0);
    return "not support";
  }

  return arr[index];
}

std::string MPovFullPushStatus::toString() {
  stringstream ss_state;
  ss_state << "storeId:" << storeid << " node:" << slave_listen_ip << ":"
           << slave_listen_port << " state:" << getEnumStr(state)
           << " binlogPos:" << binlogPos
           << " startTime:" << startTime.time_since_epoch().count() / 1000000
           << " endTime:" << endTime.time_since_epoch().count() / 1000000;
  return ss_state.str();
}

ReplManager::ReplManager(std::shared_ptr<ServerEntry> svr,
                         const std::shared_ptr<ServerParams> cfg)
  : _cfg(cfg),
    _isRunning(false),
    _svr(svr),
    _rateLimiter(
      std::make_unique<RateLimiter>(cfg->binlogRateLimitMB * 1024 * 1024)),
    _incrPaused(false),
    _clientIdGen(0),
    _dumpPath(cfg->dumpPath),
    _fullPushMatrix(std::make_shared<PoolMatrix>()),
    _incrPushMatrix(std::make_shared<PoolMatrix>()),
    _fullReceiveMatrix(std::make_shared<PoolMatrix>()),
    _incrCheckMatrix(std::make_shared<PoolMatrix>()),
    _logRecycleMatrix(std::make_shared<PoolMatrix>()),
    _connectMasterTimeoutMs(1000) {
  _cfg->serverParamsVar("incrPushThreadnum")->setUpdate([this]() {
    incrPusherResize(_cfg->incrPushThreadnum);
  });
  _cfg->serverParamsVar("fullPushThreadnum")->setUpdate([this]() {
    fullPusherResize(_cfg->fullPushThreadnum);
  });
  _cfg->serverParamsVar("fullReceiveThreadnum")->setUpdate([this]() {
    fullReceiverResize(_cfg->fullReceiveThreadnum);
  });
  _cfg->serverParamsVar("logRecycleThreadnum")->setUpdate([this]() {
    logRecyclerResize(_cfg->logRecycleThreadnum);
  });
}

Status ReplManager::stopStore(uint32_t storeId) {
  std::lock_guard<std::mutex> lk(_mutex);

  INVARIANT_D(storeId < _svr->getKVStoreCount());

  _syncStatus[storeId]->nextSchedTime = SCLOCK::time_point::max();

  _logRecycStatus[storeId]->nextSchedTime = SCLOCK::time_point::max();

  for (auto& mpov : _pushStatus[storeId]) {
    mpov.second->nextSchedTime = SCLOCK::time_point::max();
  }
  _fullPushStatus[storeId].clear();

  return {ErrorCodes::ERR_OK, ""};
}

Status ReplManager::startup() {
  std::lock_guard<std::mutex> lk(_mutex);
  Catalog* catalog = _svr->getCatalog();
  INVARIANT(catalog != nullptr);

  for (uint32_t i = 0; i < _svr->getKVStoreCount(); i++) {
    Expected<std::unique_ptr<StoreMeta>> meta = catalog->getStoreMeta(i);
    if (meta.ok()) {
      _syncMeta.emplace_back(std::move(meta.value()));
    } else if (meta.status().code() == ErrorCodes::ERR_NOTFOUND) {
      auto pMeta = std::unique_ptr<StoreMeta>(new StoreMeta(
        i, "", 0, -1, Transaction::TXNID_UNINITED, ReplState::REPL_NONE));
      Status s = catalog->setStoreMeta(*pMeta);
      if (!s.ok()) {
        return s;
      }
      _syncMeta.emplace_back(std::move(pMeta));
    } else {
      return meta.status();
    }
  }

  INVARIANT(_syncMeta.size() == _svr->getKVStoreCount());

  for (size_t i = 0; i < _syncMeta.size(); ++i) {
    if (i != _syncMeta[i]->id) {
      std::stringstream ss;
      ss << "meta:" << i << " has id:" << _syncMeta[i]->id;
      return {ErrorCodes::ERR_INTERNAL, ss.str()};
    }
  }

  _incrPusher = std::make_unique<WorkerPool>("tx-repl-minc", _incrPushMatrix);
  Status s = _incrPusher->startup(_cfg->incrPushThreadnum);
  if (!s.ok()) {
    return s;
  }
  _fullPusher = std::make_unique<WorkerPool>("tx-repl-mfull", _fullPushMatrix);
  s = _fullPusher->startup(_cfg->fullPushThreadnum);
  if (!s.ok()) {
    return s;
  }

  _fullReceiver =
    std::make_unique<WorkerPool>("tx-repl-sfull", _fullReceiveMatrix);
  s = _fullReceiver->startup(_cfg->fullReceiveThreadnum);
  if (!s.ok()) {
    return s;
  }

  _incrChecker = std::make_unique<WorkerPool>("tx-repl-schk", _incrCheckMatrix);
  s = _incrChecker->startup(2);
  if (!s.ok()) {
    return s;
  }

  _logRecycler =
    std::make_unique<WorkerPool>("tx-log-recyc", _logRecycleMatrix);
  s = _logRecycler->startup(_cfg->logRecycleThreadnum);
  if (!s.ok()) {
    return s;
  }
  SCLOCK::time_point tp = getGmtUtcTime();
  DLOG(INFO) << "init last sync time:"
             << epochToDatetime(nsSinceEpoch(tp) / 1000000);

  for (uint32_t i = 0; i < _svr->getKVStoreCount(); i++) {
    // here we are starting up, dont acquire a storelock.
    auto expdb =
      _svr->getSegmentMgr()->getDb(nullptr, i, mgl::LockMode::LOCK_NONE, true);
    if (!expdb.ok()) {
      return expdb.status();
    }
    auto store = std::move(expdb.value().store);
    INVARIANT(store != nullptr);

    bool isOpen = store->isOpen();
    uint32_t fileSeq = std::numeric_limits<uint32_t>::max();

    if (!isOpen) {
      LOG(INFO) << "store:" << i << " is not opened";

      // NOTE(vinchen): Here the max timepiont value is tp used
      // to control the _syncStatus/_logRecycStatus
      // do nothing when the storeMode == STORE_NONE.
      // And it would be easier to reopen the closed store in the
      // future.
      tp = SCLOCK::time_point::max();
    }

    _syncStatus.emplace_back(std::unique_ptr<SPovStatus>(new SPovStatus{
      false,
      std::numeric_limits<uint64_t>::max(),
      tp,
      tp,
      0,
      0
    }));

    // NOTE(vinchen): if the mode == STORE_NONE, _pushStatus would do
    // nothing, more detailed in ReplManager::registerIncrSync()
    // init master's pov, incrpush status
#if defined(_WIN32) && _MSC_VER > 1900
    _pushStatus.emplace_back(std::map<uint64_t, MPovStatus*>());
    _fullPushStatus.emplace_back(std::map<string, MPovFullPushStatus*>());
#else
    _pushStatus.emplace_back(std::map<uint64_t, std::unique_ptr<MPovStatus>>());

    _fullPushStatus.emplace_back(
      std::map<string, std::unique_ptr<MPovFullPushStatus>>());
#endif

    Status status;

    if (isOpen) {
      if (_syncMeta[i]->syncFromHost == "") {
        status = _svr->setStoreMode(store, KVStore::StoreMode::READ_WRITE);
      } else {
        status = _svr->setStoreMode(store, KVStore::StoreMode::REPLICATE_ONLY);

        // NOTE(vinchen): the binlog of slave is sync from master,
        // when the slave startup, _syncMeta[i]->binlogId should depend
        // on store->getHighestBinlogId();
        _syncMeta[i]->binlogId = store->getHighestBinlogId();
      }
      if (!status.ok()) {
        return status;
      }

      Expected<uint32_t> efileSeq = maxDumpFileSeq(i);
      if (!efileSeq.ok()) {
        return efileSeq.status();
      }
      fileSeq = efileSeq.value();
    }

    auto recBinlogStat =
      std::unique_ptr<RecycleBinlogStatus>(new RecycleBinlogStatus{
        false,
        tp,
        Transaction::TXNID_UNINITED,
        Transaction::TXNID_UNINITED,
        fileSeq,
        0,
        tp,
        0,
        nullptr,
        Transaction::TXNID_UNINITED,
      });

    if (isOpen) {
      auto ptxn = store->createTransaction(nullptr);
      if (!ptxn.ok()) {
        return ptxn.status();
      }
      auto txn = std::move(ptxn.value());
      auto explog = RepllogCursorV2::getMinBinlog(txn.get());
      if (explog.ok()) {
        recBinlogStat->firstBinlogId = explog.value().getBinlogId();
        recBinlogStat->timestamp = explog.value().getTimestamp();
        recBinlogStat->lastFlushBinlogId = Transaction::TXNID_UNINITED;
        recBinlogStat->saveBinlogId = recBinlogStat->firstBinlogId;
        if (_cfg->binlogDelRange > 1) {
          uint64_t save = recBinlogStat->firstBinlogId;
          auto expid = getSaveBinlogId(i, fileSeq);
          if (!expid.ok() &&
              expid.status().code() != ErrorCodes::ERR_NOTFOUND) {
            LOG(ERROR) << "recycleBinlog get save binlog id failed:"
                       << expid.status().toString();
          }
          if (expid.ok()) {
            save = expid.value();
            save++;
            if (recBinlogStat->firstBinlogId > save) {
              LOG(ERROR) << "recycleBinlog get an incorrect save binlog "
                            "id."
                         << "save id:" << save
                         << " firstBinlogId:" << recBinlogStat->firstBinlogId;
              save = recBinlogStat->firstBinlogId;
            }
          }
          recBinlogStat->saveBinlogId = save;
        }
      } else {
        if (explog.status().code() == ErrorCodes::ERR_EXHAUST) {
          // void compiler ud-link about static constexpr
          // TODO(takenliu): fix the relative logic
          recBinlogStat->firstBinlogId = Transaction::MIN_VALID_TXNID;
          recBinlogStat->timestamp = 0;
          recBinlogStat->lastFlushBinlogId = Transaction::TXNID_UNINITED;
          recBinlogStat->saveBinlogId = recBinlogStat->firstBinlogId;
        } else {
          return explog.status();
        }
      }
    }
    _logRecycStatus.emplace_back(std::move(recBinlogStat));
    LOG(INFO) << "store:" << i
              << ",_firstBinlogId:" << _logRecycStatus.back()->firstBinlogId
              << ",_timestamp:" << _logRecycStatus.back()->timestamp;
  }

  INVARIANT(_logRecycStatus.size() == _svr->getKVStoreCount());

  _isRunning.store(true, std::memory_order_relaxed);
  _controller = std::make_unique<std::thread>(std::move([this]() {
    pthread_setname_np(pthread_self(), "tx-repl-loop");
    controlRoutine();
  }));

  return {ErrorCodes::ERR_OK, ""};
}

void ReplManager::changeReplStateInLock(const StoreMeta& storeMeta,
                                        bool persist) {
  if (persist) {
    Catalog* catalog = _svr->getCatalog();
    Status s = catalog->setStoreMeta(storeMeta);
    if (!s.ok()) {
      LOG(FATAL) << "setStoreMeta failed:" << s.toString();
    }
  }
  _syncMeta[storeMeta.id] = std::move(storeMeta.copy());
}

Expected<uint32_t> ReplManager::maxDumpFileSeq(uint32_t storeId) {
  std::string subpath = _dumpPath + "/" + std::to_string(storeId) + "/";
#ifdef _WIN32
  subpath = replaceAll(subpath, "/", "\\");
#endif
  try {
    if (!filesystem::exists(_dumpPath)) {
      filesystem::create_directory(_dumpPath);
    }
    if (!filesystem::exists(subpath)) {
      filesystem::create_directory(subpath);
    }
  } catch (const std::exception& ex) {
    LOG(ERROR) << "create dir:" << _dumpPath << " or " << subpath
               << " failed reason:" << ex.what();
    return {ErrorCodes::ERR_INTERNAL, ex.what()};
  }
  uint32_t maxFno = 0;
  try {
    for (auto& p : filesystem::recursive_directory_iterator(subpath)) {
      const filesystem::path& path = p.path();
      if (!filesystem::is_regular_file(p)) {
        LOG(INFO) << "maxDumpFileSeq ignore:" << p.path();
        continue;
      }
      // assert path with dir prefix
      INVARIANT(path.string().find(subpath) == 0);
      std::string relative = path.string().erase(0, subpath.size());
      if (relative.substr(0, 6) != "binlog") {
        LOG(INFO) << "maxDumpFileSeq ignore:" << relative;
      }
      size_t i = 0, start = 0, end = 0, first = 0;
      for (; i < relative.size(); ++i) {
        if (relative[i] == '-') {
          first += 1;
          if (first == 2) {
            start = i + 1;
          }
          if (first == 3) {
            end = i;
            break;
          }
        }
      }
      Expected<uint64_t> fno =
        ::tendisplus::stoul(relative.substr(start, end - start));
      if (!fno.ok()) {
        LOG(ERROR) << "parse fileno:" << relative
                   << " failed:" << fno.status().toString();
        return fno.value();
      }
      if (fno.value() >= std::numeric_limits<uint32_t>::max()) {
        LOG(ERROR) << "invalid fileno:" << fno.value();
        return {ErrorCodes::ERR_INTERNAL, "invalid fileno"};
      }
      maxFno = std::max(maxFno, (uint32_t)fno.value());
    }
  } catch (const std::exception& ex) {
    LOG(ERROR) << "store:" << storeId << " get fileno failed:" << ex.what();
    return {ErrorCodes::ERR_INTERNAL, "parse fileno failed"};
  }
  return maxFno;
}

void ReplManager::changeReplState(const StoreMeta& storeMeta, bool persist) {
  std::lock_guard<std::mutex> lk(_mutex);
  changeReplStateInLock(storeMeta, persist);
}

Status ReplManager::resetRecycleState(uint32_t storeId) {
  // set _logRecycStatus::firstBinlogId with MinBinlog get from rocksdb
  auto expdb =
    _svr->getSegmentMgr()->getDb(nullptr, storeId, mgl::LockMode::LOCK_NONE);
  if (!expdb.ok()) {
    return expdb.status();
  }
  auto store = std::move(expdb.value().store);
  INVARIANT(store != nullptr);
  auto exptxn = store->createTransaction(nullptr);
  if (!exptxn.ok()) {
    return exptxn.status();
  }

  Transaction* txn = exptxn.value().get();
  auto explog = RepllogCursorV2::getMinBinlog(txn);
  if (explog.ok()) {
    std::lock_guard<std::mutex> lk(_mutex);
    _logRecycStatus[storeId]->firstBinlogId = explog.value().getBinlogId();
    /* NOTE(wayenchen) saveBinlog should be reset */
    _logRecycStatus[storeId]->saveBinlogId = explog.value().getBinlogId();
    _logRecycStatus[storeId]->timestamp = explog.value().getTimestamp();
    _logRecycStatus[storeId]->lastFlushBinlogId = Transaction::TXNID_UNINITED;
    LOG(INFO) << "resetRecycleState"
              << " firstBinlogId:" << _logRecycStatus[storeId]->firstBinlogId
              << " saveBinlogId:" << _logRecycStatus[storeId]->saveBinlogId
              << " timestamp:" << _logRecycStatus[storeId]->timestamp
              << " lastFlushBinlogId:"
              << _logRecycStatus[storeId]->lastFlushBinlogId;
  } else {
    if (explog.status().code() == ErrorCodes::ERR_EXHAUST) {
      // void compiler ud-link about static constexpr
      // TODO(takenliu): fix the relative logic
      std::lock_guard<std::mutex> lk(_mutex);
      _logRecycStatus[storeId]->firstBinlogId = store->getHighestBinlogId() + 1;
      _logRecycStatus[storeId]->saveBinlogId = store->getHighestBinlogId() + 1;
      _logRecycStatus[storeId]->timestamp = 0;
      _logRecycStatus[storeId]->lastFlushBinlogId = Transaction::TXNID_UNINITED;
      LOG(INFO) << "resetRecycleState"
                << " firstBinlogId:" << _logRecycStatus[storeId]->firstBinlogId
                << " saveBinlogId:" << _logRecycStatus[storeId]->saveBinlogId
                << " timestamp:" << _logRecycStatus[storeId]->timestamp
                << " lastFlushBinlogId:"
                << _logRecycStatus[storeId]->lastFlushBinlogId;
    } else {
      LOG(ERROR) << "ReplManager::restart failed, storeid:" << storeId;
      return {ErrorCodes::ERR_INTERGER, "getMinBinlog failed."};
    }
  }

  return {ErrorCodes::ERR_OK, ""};
}

std::shared_ptr<BlockingTcpClient> ReplManager::createClient(
  const StoreMeta& metaSnapshot, uint64_t timeoutMs, int64_t flags) {
  std::shared_ptr<BlockingTcpClient> client =
    std::move(_svr->getNetwork()->createBlockingClient(64 * 1024 * 1024));
  client->setFlags(flags);
  Status s = client->connect(metaSnapshot.syncFromHost,
                             metaSnapshot.syncFromPort,
                             std::chrono::milliseconds(timeoutMs));
  if (!s.ok()) {
    LOG(WARNING) << "connect " << metaSnapshot.syncFromHost << ":"
                 << metaSnapshot.syncFromPort << " failed:" << s.toString()
                 << " storeid:" << metaSnapshot.id;
    return nullptr;
  }

  std::string masterauth = _svr->masterauth();
  if (masterauth != "") {
    std::stringstream ss;
    ss << "AUTH " << masterauth;
    client->writeLine(ss.str());
    Expected<std::string> s = client->readLine(std::chrono::seconds(10));
    if (!s.ok()) {
      LOG(WARNING) << "fullSync auth error:" << s.status().toString();
      return nullptr;
    }
    if (s.value().size() == 0 || s.value()[0] == '-') {
      LOG(INFO) << "fullSync auth failed:" << s.value();
      return nullptr;
    }
  }
  return std::move(client);
}

void ReplManager::controlRoutine() {
  using namespace std::chrono_literals;  // (NOLINT)
  auto schedSlaveInLock = [this](const SCLOCK::time_point& now) {
    // slave's POV
    bool doSth = false;
    for (size_t i = 0; i < _syncStatus.size(); i++) {
      if (_syncStatus[i]->isRunning || now < _syncStatus[i]->nextSchedTime ||
          _syncMeta[i]->replState == ReplState::REPL_NONE) {
        continue;
      }
      doSth = true;
      // NOTE(deyukong): we dispatch fullsync/incrsync jobs into
      // different pools.
      if (_syncMeta[i]->replState == ReplState::REPL_CONNECT) {
        _syncStatus[i]->isRunning = true;
        _fullReceiver->schedule([this, i]() { slaveSyncRoutine(i); });
      } else if (_syncMeta[i]->replState == ReplState::REPL_CONNECTED ||
                 _syncMeta[i]->replState == ReplState::REPL_ERR) {
        _syncStatus[i]->isRunning = true;
        _incrChecker->schedule([this, i]() { slaveSyncRoutine(i); });
      } else if (_syncMeta[i]->replState == ReplState::REPL_TRANSFER) {
        LOG(FATAL) << "sync store:" << i
                   << " REPL_TRANSFER should not be visitable";
      } else {  // REPL_NONE
                // nothing to do with REPL_NONE
      }
    }
    return doSth;
  };
  auto schedMasterInLock = [this](const SCLOCK::time_point& now) {
    // master's POV
    recycleFullPushStatus();

    bool doSth = false;
    for (size_t i = 0; i < _pushStatus.size(); i++) {
      for (auto& mpov : _pushStatus[i]) {
        if (mpov.second->isRunning || now < mpov.second->nextSchedTime) {
          continue;
        }

        doSth = true;
        mpov.second->isRunning = true;
        uint64_t clientId = mpov.first;
        _incrPusher->schedule(
          [this, i, clientId]() { masterPushRoutine(i, clientId); });
      }
    }
    return doSth;
  };
  auto schedRecycLogInLock = [this](const SCLOCK::time_point& now) {
    bool doSth = false;
    for (size_t i = 0; i < _logRecycStatus.size(); ++i) {
      if (_logRecycStatus[i]->isRunning ||
          now < _logRecycStatus[i]->nextSchedTime) {
        continue;
      }
      doSth = true;
      _logRecycStatus[i]->isRunning = true;

      _logRecycler->schedule([this, i]() { recycleBinlog(i); });
    }
    return doSth;
  };

  while (_isRunning.load(std::memory_order_relaxed)) {
    bool doSth = false;
    auto now = SCLOCK::now();
    {
      std::lock_guard<std::mutex> lk(_mutex);
      doSth = schedSlaveInLock(now);
      doSth = schedMasterInLock(now) || doSth;
      // TODO(takenliu): make recycLog work
      doSth = schedRecycLogInLock(now) || doSth;
    }
    if (doSth) {
      std::this_thread::yield();
    } else {
      std::this_thread::sleep_for(10ms);
    }
  }
  LOG(INFO) << "repl controller exits";
}

void ReplManager::recycleFullPushStatus() {
  auto now = SCLOCK::now();
  for (size_t i = 0; i < _fullPushStatus.size(); i++) {
    for (auto& mpov : _fullPushStatus[i]) {
      // if timeout, delte it.
      if (mpov.second->state == FullPushState::SUCESS &&
          now > mpov.second->endTime + std::chrono::seconds(600)) {
        LOG(ERROR) << "timeout, _fullPushStatus erase,"
                   << mpov.second->toString();
        _fullPushStatus[i].erase(mpov.first);
      }
    }
  }
}
void ReplManager::onFlush(uint32_t storeId, uint64_t binlogid) {
  std::lock_guard<std::mutex> lk(_mutex);
  auto& v = _logRecycStatus[storeId];
  LOG(INFO) << "ReplManager::onFlush before, storeId:" << storeId << " "
            << v->toString();
  v->firstBinlogId = binlogid;
  v->saveBinlogId = binlogid;
  v->lastFlushBinlogId = binlogid;
  INVARIANT_D(v->lastFlushBinlogId >= v->firstBinlogId);
  INVARIANT_D(v->lastFlushBinlogId >= v->saveBinlogId);
  LOG(INFO) << "ReplManager::onFlush, storeId:" << storeId << " "
            << v->toString();
}

bool ReplManager::hasSomeSlave(uint32_t storeId) {
  std::lock_guard<std::mutex> lk(_mutex);
  if (_pushStatus[storeId].size() != 0) {
    return true;
  }
  if (_fullPushStatus[storeId].size() != 0) {
    return true;
  }
  return false;
}

bool ReplManager::isSlaveOfSomeone(uint32_t storeId) {
  std::lock_guard<std::mutex> lk(_mutex);
  if (_syncMeta[storeId]->syncFromHost != "") {
    return true;
  }
  return false;
}

bool ReplManager::isSlaveOfSomeone() {
  for (size_t i = 0; i < _svr->getKVStoreCount(); ++i) {
    if (!isSlaveOfSomeone(i)) {
      return false;
    }
  }
  return true;
}

bool ReplManager::isSlaveFullSyncDone() {
  std::lock_guard<std::mutex> lk(_mutex);
  for (size_t i = 0; i < _svr->getKVStoreCount(); ++i) {
    if (_syncMeta[i]->syncFromHost == "") {
      return false;
    }
    auto rstate = _syncMeta[i]->replState;
    if (rstate != ReplState::REPL_CONNECTED) {
      return false;
    }
  }
  return true;
}

void ReplManager::recycleBinlog(uint32_t storeId) {
  SCLOCK::time_point nextSched = SCLOCK::now();
  float randRatio = redis_port::random() % 40 / 100.0 + 0.80;  // 0.80 to 1.20
  uint32_t nextSchedInterval = _cfg->truncateBinlogIntervalMs * randRatio;
  nextSched = nextSched + std::chrono::milliseconds(nextSchedInterval);

  uint64_t start = Transaction::MIN_VALID_TXNID;
  uint64_t end = Transaction::MIN_VALID_TXNID;
  uint64_t save = Transaction::MIN_VALID_TXNID;
  uint32_t fileSeq = 0;
  bool saveLogs;

  auto guard = MakeGuard([this, &nextSched, &start, &save, storeId] {
    std::lock_guard<std::mutex> lk(_mutex);
    auto& v = _logRecycStatus[storeId];
    INVARIANT_D(v->isRunning);
    v->isRunning = false;
    // v->nextSchedTime maybe time_point::max()
    if (v->nextSchedTime < nextSched) {
      v->nextSchedTime = nextSched;
    }
    if (start != Transaction::MIN_VALID_TXNID) {
      v->firstBinlogId = start;
    }
    if (save != Transaction::MIN_VALID_TXNID) {
      v->saveBinlogId = save;
    }
    // DLOG(INFO) << "_logRecycStatus[" << storeId << "].firstBinlogId
    // reset:" << start;

    // currently nothing waits for recycleBinlog's complete
    // _cv.notify_all();
  });
  LocalSessionGuard sg(_svr.get());

  auto segMgr = _svr->getSegmentMgr();
  INVARIANT(segMgr != nullptr);
  auto expdb = segMgr->getDb(sg.getSession(), storeId, mgl::LockMode::LOCK_IX);
  if (!expdb.ok()) {
    LOG(ERROR) << "recycleBinlog getDb failed:" << expdb.status().toString();
    return;
  }
  auto kvstore = std::move(expdb.value().store);
  if (!kvstore->isRunning()) {
    LOG(WARNING) << "dont need do recycleBinlog, kvstore is not running:"
                 << storeId;
    nextSched = SCLOCK::now() + std::chrono::seconds(1);
    return;
  }
  if (kvstore->getBgError() != "") {
    LOG(WARNING) << "dont need do recycleBinlog, " << kvstore->getBgError();
    nextSched = SCLOCK::now() + std::chrono::seconds(1);
    return;
  }

  updateCurBinlogFs(storeId, 0, 0);
  bool tailSlave = false;
  uint64_t highest = kvstore->getHighestBinlogId();
  end = highest;
  {
    std::unique_lock<std::mutex> lk(_mutex);

    start = _logRecycStatus[storeId]->firstBinlogId;
    save = _logRecycStatus[storeId]->saveBinlogId;
    fileSeq = _logRecycStatus[storeId]->fileSeq;

    saveLogs = _syncMeta[storeId]->syncFromHost != "";  // REPLICATE_ONLY
    // single node
    if (_syncMeta[storeId]->syncFromHost == "" &&
        _pushStatus[storeId].empty()) {
      saveLogs = true;
    }
    for (auto& mpov : _fullPushStatus[storeId]) {
      end = std::min(end, mpov.second->binlogPos);
    }
    for (auto& mpov : _pushStatus[storeId]) {
      end = std::min(end, mpov.second->binlogPos);
    }
    // NOTE(deyukong): we should keep at least 1 binlog
    uint64_t maxKeepLogs = _cfg->maxBinlogKeepNum;
    if (_syncMeta[storeId]->syncFromHost != "" &&
        _pushStatus[storeId].size() == 0) {
      tailSlave = true;
    }
    if (tailSlave) {
      maxKeepLogs = _cfg->slaveBinlogKeepNum;
    }

    maxKeepLogs = std::max((uint64_t)1, maxKeepLogs);
    if (highest >= maxKeepLogs && end > highest - maxKeepLogs) {
      end = highest - maxKeepLogs;
    }
    if (highest < maxKeepLogs) {
      end = 0;
    }
  }

  if (_svr->isClusterEnabled() && _svr->getMigrateManager() != nullptr) {
    end = std::min(end, _svr->getMigrateManager()->getProtectBinlogid(storeId));
  }
  // DLOG(INFO) << "recycleBinlog port:" << _svr->getParams()->port
  //      << " store: " << storeId << " " << start << " " << end;
  if (start > end) {
    return;
  }

  auto ptxn = kvstore->createTransaction(sg.getSession());
  if (!ptxn.ok()) {
    LOG(ERROR) << "recycleBinlog create txn failed:"
               << ptxn.status().toString();
    return;
  }
  auto txn = std::move(ptxn.value());

  uint64_t newStart = 0;
  {
    std::ofstream* fs = nullptr;
    int64_t maxWriteLen = 0;
    if (saveLogs) {
      fs = getCurBinlogFs(storeId);
      if (!fs) {
        LOG(ERROR) << "getCurBinlogFs() store;" << storeId << "failed:";
        return;
      }
      std::lock_guard<std::mutex> lk(_mutex);
      maxWriteLen = _cfg->binlogFileSizeMB * 1024 * 1024 -
        _logRecycStatus[storeId]->fileSize;
    }
    DLOG(INFO) << "store:" << storeId << " "
               << _logRecycStatus[storeId]->toString();
    auto s = kvstore->truncateBinlogV2(
      start, end, save, txn.get(), fs, maxWriteLen, tailSlave);
    if (!s.ok()) {
      LOG(ERROR) << "kvstore->truncateBinlogV2 store:" << storeId
                 << "failed:" << s.status().toString();
      return;
    }
    bool changeNewFile = s.value().ret < 0;
    updateCurBinlogFs(
      storeId, s.value().written, s.value().timestamp, changeNewFile);
    // TODO(vinchen): stat for binlog deleted
    newStart = s.value().newStart;
    save = s.value().newSave;
  }

  auto commitStat = txn->commit();
  if (!commitStat.ok()) {
    LOG(ERROR) << "truncate binlog store:" << storeId
               << "commit failed:" << commitStat.status().toString();
    return;
  }
  // DLOG(INFO) << "storeid:" << storeId << " truncate binlog from:" << start
  //    << " to end:" << newStart << " success."
  //    << "addr:" << _svr->getNetwork()->getIp()
  //    << ":" << _svr->getNetwork()->getPort();
  INVARIANT_COMPARE_D(newStart, <=, end + 1);
  start = newStart;
}

Expected<uint64_t> ReplManager::getSaveBinlogId(uint32_t storeId,
                                                uint32_t fileSeq) {
  if (fileSeq == 0) {
    return {ErrorCodes::ERR_NOTFOUND, ""};
  }
  std::string maxPath;

  std::string subpath = _dumpPath + "/" + std::to_string(storeId) + "/";
#ifdef _WIN32
  subpath = replaceAll(subpath, "/", "\\");
#endif
  try {
    for (auto& p : filesystem::recursive_directory_iterator(subpath)) {
      const filesystem::path& path = p.path();
      if (!filesystem::is_regular_file(p)) {
        LOG(INFO) << "maxDumpFileSeq ignore:" << p.path();
        continue;
      }
      // assert path with dir prefix
      INVARIANT(path.string().find(subpath) == 0);
      std::string relative = path.string().erase(0, subpath.size());
      if (relative.substr(0, 6) != "binlog") {
        LOG(INFO) << "maxDumpFileSeq ignore:" << relative;
        continue;
      }
      size_t i = 0, start = 0, end = 0, first = 0;
      for (; i < relative.size(); ++i) {
        if (relative[i] == '-') {
          first += 1;
          if (first == 2) {
            start = i + 1;
          }
          if (first == 3) {
            end = i;
            break;
          }
        }
      }
      Expected<uint64_t> fno =
        ::tendisplus::stoul(relative.substr(start, end - start));
      if (!fno.ok()) {
        LOG(ERROR) << "parse fileno:" << relative
                   << " failed:" << fno.status().toString();
        return fno.status();
      }
      if (fno.value() >= std::numeric_limits<uint32_t>::max()) {
        LOG(ERROR) << "invalid fileno:" << fno.value();
        return {ErrorCodes::ERR_INTERNAL, "invalid fileno"};
      }
      if (fileSeq == (uint32_t)fno.value()) {
        maxPath = path.string();
        break;
      }
    }
  } catch (const std::exception& ex) {
    LOG(ERROR) << "store:" << storeId << " get fileno failed:" << ex.what();
    return {ErrorCodes::ERR_INTERNAL, "parse fileno failed"};
  }

  std::ifstream fs(maxPath);
  if (!fs.is_open()) {
    LOG(ERROR) << "open file:" << maxPath << " for read failed";
    return {ErrorCodes::ERR_INTERNAL, "open file failed"};
  }
  std::string readBuf;
  readBuf.resize(4096);

  fs.read(&readBuf[0], strlen(BINLOG_HEADER_V2) + sizeof(uint32_t));
  if (!fs.good()) {
    if (fs.eof()) {
      return {ErrorCodes::ERR_NO_KEY, ""};
    }
    LOG(ERROR) << "read file:" << maxPath
               << " failed with err:" << strerror(errno);
    return {ErrorCodes::ERR_INTERNAL, "read file failed"};
  }

  std::string key;
  std::string value;
  // The last key may be incomplete, and you need to return the second last
  // key
  std::string lastKey;
  std::string lastValue;

  while (true) {
    uint32_t keylen = 0;
    fs.read(&readBuf[0], sizeof(uint32_t));
    if (fs.eof()) {
      break;
    }
    if (!fs.good()) {
      LOG(ERROR) << "read file:" << maxPath
                 << " failed with err:" << strerror(errno);
      return {ErrorCodes::ERR_INTERNAL, "read file failed"};
    }
    readBuf[sizeof(uint32_t)] = '\0';
    keylen = int32Decode(readBuf.c_str());

    std::string tempKey;
    tempKey.resize(keylen);
    fs.read(&tempKey[0], keylen);
    if (fs.eof()) {
      break;
    }
    if (!fs.good()) {
      LOG(ERROR) << "read file:" << maxPath
                 << " failed with err:" << strerror(errno);
      return {ErrorCodes::ERR_INTERNAL, "read file failed"};
    }

    uint32_t valuelen = 0;
    fs.read(&readBuf[0], sizeof(uint32_t));
    if (fs.eof()) {
      break;
    }
    if (!fs.good()) {
      LOG(ERROR) << "read file:" << maxPath
                 << " failed with err:" << strerror(errno);
      return {ErrorCodes::ERR_INTERNAL, "read file failed"};
    }
    readBuf[sizeof(uint32_t)] = '\0';
    valuelen = int32Decode(readBuf.c_str());

    std::string tempValue;
    tempValue.resize(valuelen);
    fs.read(&tempValue[0], valuelen);
    if (fs.eof()) {
      break;
    }
    if (!fs.good()) {
      LOG(ERROR) << "read file:" << maxPath
                 << " failed with err:" << strerror(errno);
      return {ErrorCodes::ERR_INTERNAL, "read file failed"};
    }

    lastKey = key;
    lastValue = value;
    key = tempKey;
    value = tempValue;
  }

  if (key != "" && value != "") {
    Expected<ReplLogKeyV2> logKey = ReplLogKeyV2::decode(key);
    Expected<ReplLogValueV2> logValue = ReplLogValueV2::decode(value);
    if (!logKey.ok()) {
      return logKey.status();
    } else if (!logValue.ok()) {
      return logValue.status();
    } else {
      return logKey.value().getBinlogId();
    }
  }
  if (lastKey != "" && lastValue != "") {
    Expected<ReplLogKeyV2> logLastKey = ReplLogKeyV2::decode(lastKey);
    Expected<ReplLogValueV2> logLastValue = ReplLogValueV2::decode(lastValue);
    if (!logLastKey.ok()) {
      return logLastKey.status();
    } else if (!logLastValue.ok()) {
      return logLastValue.status();
    } else {
      return logLastKey.value().getBinlogId();
    }
  }
  return {ErrorCodes::ERR_INTERNAL, "read file failed"};
}

bool ReplManager::flushCurBinlogFs(uint32_t storeId) {
  // TODO(takenliu): let truncateBinlogV2 return quickly.
  return newBinlogFs(storeId);
}

Status ReplManager::changeReplSource(Session* sess,
                                     uint32_t storeId,
                                     std::string ip,
                                     uint32_t port,
                                     uint32_t sourceStoreId) {
  auto expdb =
    _svr->getSegmentMgr()->getDb(sess, storeId, mgl::LockMode::LOCK_X, true);
  if (!expdb.ok()) {
    return expdb.status();
  }
  if (!expdb.value().store->isOpen()) {
    return {ErrorCodes::ERR_OK, ""};
  }
  if (ip != "" && !expdb.value().store->isEmpty(true)) {
    return {ErrorCodes::ERR_MANUAL, "store not empty"};
  }
  Status s = changeReplSourceInLock(storeId, ip, port, sourceStoreId);
  if (!s.ok()) {
    return s;
  }
  return {ErrorCodes::ERR_OK, ""};
}

// changeReplSource should be called with LOCK_X held
Status ReplManager::changeReplSourceInLock(uint32_t storeId,
                                           std::string ip,
                                           uint32_t port,
                                           uint32_t sourceStoreId,
                                           bool checkEmpty,
                                           bool needLock,
                                           bool incrSync) {
  uint64_t oldTimeout = _connectMasterTimeoutMs;
  if (ip != "") {
    _connectMasterTimeoutMs = 1000;
  } else {
    _connectMasterTimeoutMs = 1;
  }

  // NOTE(deyukong): we must wait for the target to stop before change meta,
  // or the meta may be rewrited
  if (needLock) {
    LOG(INFO) << "wait for store:" << storeId << " to yield work";
    std::unique_lock<std::mutex> lk(_mutex);
    if (!_cv.wait_for(
          lk, std::chrono::milliseconds(oldTimeout + 2000), [this, storeId] {
            return !_syncStatus[storeId]->isRunning;
          })) {
      return {ErrorCodes::ERR_TIMEOUT, "wait for yeild failed"};
    }
    LOG(INFO) << "wait for store:" << storeId << " to yield work succ";
    INVARIANT_D(!_syncStatus[storeId]->isRunning);
  } else {
    INVARIANT_D(_syncStatus[storeId]->isRunning);
  }

  if (storeId >= _syncMeta.size()) {
    return {ErrorCodes::ERR_INTERNAL, "invalid storeId"};
  }
  auto segMgr = _svr->getSegmentMgr();
  INVARIANT(segMgr != nullptr);
  auto expdb = segMgr->getDb(nullptr, storeId, mgl::LockMode::LOCK_NONE);
  if (!expdb.ok()) {
    return expdb.status();
  }
  auto kvstore = std::move(expdb.value().store);

  auto newMeta = _syncMeta[storeId]->copy();
  if (ip != "") {
    if (_syncMeta[storeId]->syncFromHost != "" && checkEmpty) {
      return {ErrorCodes::ERR_BUSY,
              "explicit set sync source empty before change it"};
    }
    _connectMasterTimeoutMs = 1000;

    Status s = _svr->setStoreMode(kvstore, KVStore::StoreMode::REPLICATE_ONLY);
    if (!s.ok()) {
      return s;
    }
    newMeta->syncFromHost = ip;
    newMeta->syncFromPort = port;
    newMeta->syncFromId = sourceStoreId;
    newMeta->replState = ReplState::REPL_CONNECT;
    newMeta->binlogId = Transaction::TXNID_UNINITED;
    if (incrSync) {
      newMeta->replState = ReplState::REPL_CONNECTED;
      newMeta->binlogId = kvstore->getHighestBinlogId();
    }
    LOG(INFO) << "change store:" << storeId << " syncSrc from no one to "
              << newMeta->syncFromHost << ":" << newMeta->syncFromPort << ":"
              << newMeta->syncFromId
              << " incrSync:" << incrSync
              << " replState:" << static_cast<int>(newMeta->replState)
              << " binlogId:" << newMeta->binlogId;
    changeReplStateInLock(*newMeta, true);

    return {ErrorCodes::ERR_OK, ""};
  } else {  // ip == ""
    if (newMeta->syncFromHost == "") {
      return {ErrorCodes::ERR_OK, ""};
    }
    LOG(INFO) << "change store:" << storeId
              << " syncSrc:" << newMeta->syncFromHost << " to no one";
    _connectMasterTimeoutMs = 1;

    Status closeStatus = _svr->cancelSession(_syncStatus[storeId]->sessionId);
    if (!closeStatus.ok()) {
      // this error does not affect much, just log and continue
      LOG(WARNING) << "cancel store:" << storeId
                   << " session failed:" << closeStatus.toString();
    }
    _syncStatus[storeId]->sessionId = std::numeric_limits<uint64_t>::max();

    Status s = _svr->setStoreMode(kvstore, KVStore::StoreMode::READ_WRITE);
    if (!s.ok()) {
      return s;
    }
    newMeta->syncFromHost = ip;
    newMeta->syncFromPort = port;
    newMeta->syncFromId = sourceStoreId;
    newMeta->replState = ReplState::REPL_NONE;
    newMeta->binlogId = Transaction::TXNID_UNINITED;
    changeReplStateInLock(*newMeta, true);

    return {ErrorCodes::ERR_OK, ""};
  }
}

Status ReplManager::replicationSetMaster(std::string ip,
                                         uint32_t port,
                                         bool checkEmpty,
                                         bool incrSync) {
  std::vector<uint32_t> storeVec;
  auto guard = MakeGuard([this, &storeVec] {
    std::lock_guard<std::mutex> lk(_mutex);
    for (auto& id : storeVec) {
      _syncStatus[id]->isRunning = false;
    }
  });

  for (uint32_t id = 0; id < _svr->getKVStoreCount(); ++id) {
    std::unique_lock<std::mutex> lk(_mutex);
    if (!_cv.wait_for(lk, std::chrono::milliseconds(2000), [this, id] {
          return !_syncStatus[id]->isRunning;
        })) {
      return {ErrorCodes::ERR_TIMEOUT, "wait for yeild failed"};
    }
    _syncStatus[id]->isRunning = true;
    storeVec.push_back(id);
  }

  LocalSessionGuard sg(_svr.get());
  // NOTE(takenliu): ensure automic operation for all store
  std::list<Expected<DbWithLock>> expdbList;
  for (uint32_t i = 0; i < _svr->getKVStoreCount(); ++i) {
    auto expdb =
      _svr->getSegmentMgr()->getDb(sg.getSession(), i, mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
      return expdb.status();
    }
    if (!expdb.value().store->isOpen()) {
      return {ErrorCodes::ERR_INTERNAL, "store not open"};
    }

    expdbList.push_back(std::move(expdb));
  }

  INVARIANT_D(expdbList.size() == _svr->getKVStoreCount());

  for (uint32_t i = 0; i < _svr->getKVStoreCount(); ++i) {
    Status s = changeReplSourceInLock(
      i, ip, port, i, checkEmpty, false, incrSync);
    if (!s.ok()) {
      return s;
    }
  }

  return {ErrorCodes::ERR_OK, ""};
}

Status ReplManager::replicationSetMaster(std::string ip,
                                         uint32_t port,
                                         uint32_t storeId,
                                         bool checkEmpty) {
  bool isStoreDone = false;
  auto guard = MakeGuard([this, storeId, &isStoreDone] {
    std::lock_guard<std::mutex> lk(_mutex);
    if (isStoreDone) {
      _syncStatus[storeId]->isRunning = false;
    }
  });

  {
    std::unique_lock<std::mutex> lk(_mutex);
    if (!_cv.wait_for(lk, std::chrono::milliseconds(2000), [this, storeId] {
          return !_syncStatus[storeId]->isRunning;
        })) {
      return {ErrorCodes::ERR_TIMEOUT, "wait for yeild failed"};
    }
    _syncStatus[storeId]->isRunning = true;
    isStoreDone = true;
  }

  LocalSessionGuard sg(_svr.get());
  auto expdb = _svr->getSegmentMgr()->getDb(
    sg.getSession(), storeId, mgl::LockMode::LOCK_X);
  if (!expdb.ok()) {
    return expdb.status();
  }
  if (!expdb.value().store->isOpen()) {
    return {ErrorCodes::ERR_INTERNAL, "store not open"};
  }

  Status s =
    changeReplSourceInLock(storeId, ip, port, storeId, checkEmpty, false);
  if (!s.ok()) {
    return s;
  }

  return {ErrorCodes::ERR_OK, ""};
}

Status ReplManager::replicationUnSetMaster() {
  std::vector<uint32_t> storeVec;
  auto guard = MakeGuard([this, &storeVec] {
    std::lock_guard<std::mutex> lk(_mutex);
    for (auto& id : storeVec) {
      _syncStatus[id]->isRunning = false;
    }
  });

  // NOTE(wayenchen): avoid dead lock in applyRepllogV2 or controlRoutine
  for (uint32_t id = 0; id < _svr->getKVStoreCount(); ++id) {
    std::unique_lock<std::mutex> lk(_mutex);
    if (!_cv.wait_for(lk, std::chrono::milliseconds(3000), [this, id] {
          return !_syncStatus[id]->isRunning;
        })) {
      return {ErrorCodes::ERR_TIMEOUT, "wait for yeild failed"};
    }
    _syncStatus[id]->isRunning = true;
    _syncStatus[id]->sessionId = std::numeric_limits<uint64_t>::max();
    storeVec.push_back(id);
  }

  LocalSessionGuard sg(_svr.get());
  // NOTE(takenliu): ensure automic operation for all store
  std::list<Expected<DbWithLock>> expdbList;
  for (uint32_t i = 0; i < _svr->getKVStoreCount(); ++i) {
    auto expdb =
      _svr->getSegmentMgr()->getDb(sg.getSession(), i, mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
      return expdb.status();
    }
    if (!expdb.value().store->isOpen()) {
      return {ErrorCodes::ERR_INTERNAL, "store not open"};
    }

    expdbList.push_back(std::move(expdb));
  }
  INVARIANT_D(expdbList.size() == _svr->getKVStoreCount());
  for (uint32_t i = 0; i < _svr->getKVStoreCount(); ++i) {
    Status s = changeReplSourceInLock(i, "", 0, i, true, false);
    if (!s.ok()) {
      return s;
    }
  }
  return {ErrorCodes::ERR_OK, ""};
}

Status ReplManager::replicationUnSetMaster(uint32_t storeId) {
  bool isStoreDone = false;
  auto guard = MakeGuard([this, storeId, &isStoreDone] {
    std::lock_guard<std::mutex> lk(_mutex);
    if (isStoreDone) {
      _syncStatus[storeId]->isRunning = false;
    }
  });
  // NOTE(wayenchen): avoid dead lock in applyRepllogV2 or controlRoutine
  {
    std::unique_lock<std::mutex> lk(_mutex);
    if (!_cv.wait_for(lk, std::chrono::milliseconds(3000), [this, storeId] {
          return !_syncStatus[storeId]->isRunning;
        })) {
      return {ErrorCodes::ERR_TIMEOUT, "wait for yeild failed"};
    }
    _syncStatus[storeId]->isRunning = true;
    _syncStatus[storeId]->sessionId = std::numeric_limits<uint64_t>::max();
    isStoreDone = true;
  }

  LocalSessionGuard sg(_svr.get());
  // NOTE(takenliu): ensure automic operation for all store
  auto expdb = _svr->getSegmentMgr()->getDb(
    sg.getSession(), storeId, mgl::LockMode::LOCK_X);

  if (!expdb.ok()) {
    return expdb.status();
  }
  if (!expdb.value().store->isOpen()) {
    return {ErrorCodes::ERR_INTERNAL, "store not open"};
  }

  Status s = changeReplSourceInLock(storeId, "", 0, storeId, true, false);
  if (!s.ok()) {
    return s;
  }

  return {ErrorCodes::ERR_OK, ""};
}


std::string ReplManager::getRecycleBinlogStr(Session* sess) const {
  stringstream ss;

  for (size_t i = 0; i < _svr->getKVStoreCount(); ++i) {
    auto expdb =
      _svr->getSegmentMgr()->getDb(sess, i, mgl::LockMode::LOCK_IS, false, 0);
    if (!expdb.ok())
      continue;

    PStore kvstore = expdb.value().store;
    ss << "rocksdb" + kvstore->dbId() << ":"
       << "min=" << _logRecycStatus[i]->firstBinlogId
       << ",save=" << _logRecycStatus[i]->saveBinlogId
       << ",BLWM=" << kvstore->getHighestBinlogId()
       << ",BHWM=" << kvstore->getNextBinlogSeq() << "\r\n";
  }
  return ss.str();
}

std::string ReplManager::getMasterHost() const {
  std::lock_guard<std::mutex> lk(_mutex);

  for (size_t i = 0; i < _svr->getKVStoreCount(); ++i) {
    if (_syncMeta[i]->syncFromHost != "") {
      INVARIANT_D(i == 0);
      return _syncMeta[i]->syncFromHost;
    }
  }
  return "";
}

std::string ReplManager::getMasterHost(uint32_t storeid) const {
  std::lock_guard<std::mutex> lk(_mutex);
  if (_syncMeta[storeid]->syncFromHost != "") {
    return _syncMeta[storeid]->syncFromHost;
  }
  return "";
}

std::vector<uint32_t> ReplManager::checkMasterHost(const std::string& hostname,
                                                   uint32_t port) {
  std::lock_guard<std::mutex> lk(_mutex);
  std::vector<uint32_t> errList;

  for (size_t i = 0; i < _svr->getKVStoreCount(); ++i) {
    if (_syncMeta[i]->syncFromHost != hostname ||
        _syncMeta[i]->syncFromPort != port) {
      errList.push_back(i);
      LOG(ERROR) << "replication meta is not right on store :" << i;
    }
  }

  return errList;
}

uint32_t ReplManager::getMasterPort() const {
  std::lock_guard<std::mutex> lk(_mutex);
  for (size_t i = 0; i < _svr->getKVStoreCount(); ++i) {
    if (_syncMeta[i]->syncFromPort != 0) {
      INVARIANT_D(i == 0);
      return _syncMeta[i]->syncFromPort;
    }
  }
  return 0;
}

uint64_t ReplManager::getLastSyncTime() const {
  std::lock_guard<std::mutex> lk(_mutex);
  uint64_t min = 0;
  for (size_t i = 0; i < _svr->getKVStoreCount(); ++i) {
    if (_syncMeta[i]->syncFromHost != "") {
      // it is a slave
      uint64_t last_sync_time =
        nsSinceEpoch(_syncStatus[i]->lastSyncTime) / 1000000;  // ms
      if (last_sync_time < min || min == 0) {
        min = last_sync_time;
      }
    }
  }

  return min;
}

uint64_t ReplManager::replicationGetOffset() const {
  uint64_t totalBinlog = 0;
  LocalSessionGuard sg(_svr.get());

  for (uint64_t i = 0; i < _svr->getKVStoreCount(); i++) {
    auto expdb = _svr->getSegmentMgr()->getDb(
      sg.getSession(), i, mgl::LockMode::LOCK_NONE);
    if (!expdb.ok()) {
      LOG(ERROR) << "slave offset get db error:" << expdb.status().toString();
      continue;
    }

    auto kvstore = std::move(expdb.value().store);
    uint64_t maxBinlog = kvstore->getHighestBinlogId();
    totalBinlog += maxBinlog;
  }
  return totalBinlog;
}

uint64_t ReplManager::replicationGetMaxBinlogIdFromRocks() const {
  uint64_t totalBinlog = 0;
  LocalSessionGuard sg(_svr.get());

  for (uint64_t i = 0; i < _svr->getKVStoreCount(); i++) {
    auto expdb =
      _svr->getSegmentMgr()->getDb(sg.getSession(), i, mgl::LockMode::LOCK_IS);
    if (!expdb.ok()) {
      LOG(ERROR) << "slave offset get db error:" << expdb.status().toString();
      continue;
    }
    auto kvstore = std::move(expdb.value().store);
    auto ptxn = kvstore->createTransaction(nullptr);

    if (!ptxn.ok()) {
      LOG(ERROR) << "offset create transaction fail:"
                 << ptxn.status().toString();
    }
    uint64_t maxBinlog = 0;
    auto expBinlogidMax = RepllogCursorV2::getMaxBinlogId(ptxn.value().get());
    if (!expBinlogidMax.ok()) {
      if (expBinlogidMax.status().code() != ErrorCodes::ERR_EXHAUST) {
        LOG(ERROR) << "slave offset getMaxBinlogId error:"
                   << expBinlogidMax.status().toString();
      }
    } else {
      maxBinlog = expBinlogidMax.value();
    }
    totalBinlog += maxBinlog;
  }
  return totalBinlog;
}

uint64_t ReplManager::replicationGetMaxBinlogId() const {
  uint64_t totalBinlog = 0;
  LocalSessionGuard sg(_svr.get());

  for (uint64_t i = 0; i < _svr->getKVStoreCount(); i++) {
    // NOTE(wayenchen) not necessary to lockdb if just get offset in memory
    auto expdb = _svr->getSegmentMgr()->getDb(
      sg.getSession(), i, mgl::LockMode::LOCK_NONE);
    if (!expdb.ok()) {
      LOG(ERROR) << "slave offset get db error:" << expdb.status().toString();
      continue;
    }
    auto kvstore = std::move(expdb.value().store);
    auto nextBinlog = kvstore->getNextBinlogSeq() - 1;
    totalBinlog += nextBinlog;
  }
  return totalBinlog;
}

void ReplManager::getReplInfo(std::stringstream& ss) const {
  getReplInfoSimple(ss);
  getReplInfoDetail(ss);
}

struct ReplMPovStatus {
  uint32_t dstStoreId = 0;
  uint64_t binlogpos = 0;
  uint64_t clientId = 0;
  string state;
  uint64_t lastBinlogTs = 0;
  string slave_listen_ip;
  uint16_t slave_listen_port = 0;
};

void ReplManager::getReplInfoSimple(std::stringstream& ss) const {
  // NOTE(takenliu), only consider slaveof all rockskvstores.
  string role = "master";
  uint64_t master_repl_offset = 0;
  string master_host = "";
  uint32_t master_port = 0;
  string master_link_status = "up";
  int64_t master_last_io_seconds_ago = 0;
  int32_t master_sync_in_progress = 0;
  uint64_t slave_repl_offset = 0;
  int32_t slave_priority = 100;
  int32_t slave_read_only = 1;
  SCLOCK::time_point minlastSyncTime = SCLOCK::now();
  SCLOCK::time_point mindownSyncTime = SCLOCK::now();
  for (size_t i = 0; i < _svr->getKVStoreCount(); ++i) {
    std::lock_guard<std::mutex> lk(_mutex);
    if (_syncMeta[i]->syncFromHost != "") {
      role = "slave";
      if (_syncStatus[i]->lastSyncTime < minlastSyncTime) {
        minlastSyncTime = _syncStatus[i]->lastSyncTime;
      }
      master_host = _syncMeta[i]->syncFromHost;
      master_port = _syncMeta[i]->syncFromPort;
      slave_repl_offset += _syncMeta[i]->binlogId;
      if (_syncMeta[i]->replState == ReplState::REPL_TRANSFER) {
        master_sync_in_progress = 1;
      } else if (_syncMeta[i]->replState == ReplState::REPL_ERR) {
        master_link_status = "down";
        if (_syncStatus[i]->lastSyncTime < mindownSyncTime) {
          mindownSyncTime = _syncStatus[i]->lastSyncTime;
        }
      }
    }
  }
  master_last_io_seconds_ago = sinceEpoch() - sinceEpoch(minlastSyncTime);
  ss << "role:" << role << "\r\n";
  if (role == "slave") {
    ss << "master_host:" << master_host << "\r\n";
    ss << "master_port:" << master_port << "\r\n";
    ss << "master_link_status:" << master_link_status << "\r\n";
    ss << "master_last_io_seconds_ago:" << master_last_io_seconds_ago << "\r\n";
    ss << "master_sync_in_progress:" << master_sync_in_progress << "\r\n";
    ss << "slave_repl_offset:" << slave_repl_offset << "\r\n";
    if (master_sync_in_progress == 1) {
      // TODO(takenliu):
      ss << "master_sync_left_bytes:" << -1 << "\r\n";
      ss << "master_sync_last_io_seconds_ago:" << master_last_io_seconds_ago
         << "\r\n";
    }
    if (master_link_status == "down") {
      auto master_link_down_since_seconds =
        sinceEpoch() - sinceEpoch(mindownSyncTime);
      ss << "master_link_down_since_seconds:" << master_link_down_since_seconds
         << "\r\n";
    }
    ss << "slave_priority:" << slave_priority << "\r\n";
    ss << "slave_read_only:" << slave_read_only << "\r\n";
  }
  // master point of view
  std::map<std::string, ReplMPovStatus> pstatus;
  for (size_t i = 0; i < _svr->getKVStoreCount(); ++i) {
    auto expdb =
      _svr->getSegmentMgr()->getDb(nullptr, i, mgl::LockMode::LOCK_IS, true, 0);
    if (!expdb.ok()) {
      continue;
    }
    uint64_t highestBinlogid = expdb.value().store->getHighestBinlogId();
    master_repl_offset += highestBinlogid;
    {
      std::lock_guard<std::mutex> lk(_mutex);
      for (auto& iter : _pushStatus[i]) {
        string key = iter.second->slave_listen_ip + "#" +
          std::to_string(iter.second->slave_listen_port);
        auto s = pstatus.find(key);
        if (s == pstatus.end()) {
          pstatus[key] = ReplMPovStatus();
          pstatus[key].slave_listen_ip = iter.second->slave_listen_ip;
          pstatus[key].slave_listen_port = iter.second->slave_listen_port;
          pstatus[key].state = "online";
          pstatus[key].lastBinlogTs = iter.second->binlogTs;
        }
        if (iter.second->binlogTs > pstatus[key].lastBinlogTs) {
          pstatus[key].lastBinlogTs = iter.second->binlogTs;
        }
        pstatus[key].binlogpos += iter.second->binlogPos;
      }
    }
    {
      std::lock_guard<std::mutex> lk(_mutex);
      for (auto& iter : _fullPushStatus[i]) {
        string key = iter.second->slave_listen_ip + "#" +
          std::to_string(iter.second->slave_listen_port);
        auto s = pstatus.find(key);
        if (s == pstatus.end()) {
          pstatus[key] = ReplMPovStatus();
          pstatus[key].slave_listen_ip = iter.second->slave_listen_ip;
          pstatus[key].slave_listen_port = iter.second->slave_listen_port;
          pstatus[key].state = "send_bulk";
        }
        pstatus[key].binlogpos += iter.second->binlogPos;
      }
    }
  }
  ss << "connected_slaves:" << pstatus.size() << "\r\n";
  if (pstatus.size() > 0) {
    int i = 0;
    for (auto& iter : pstatus) {
      ss << "slave" << i << ":ip=" << iter.second.slave_listen_ip
         << ",port=" << iter.second.slave_listen_port
         << ",state=" << iter.second.state
         << ",offset=" << iter.second.binlogpos
         << ",lag=" << (msSinceEpoch() - iter.second.lastBinlogTs) / 1000
         << ",binlog_lag="
         << (int64_t)master_repl_offset - (int64_t)iter.second.binlogpos
         << "\r\n";
    }
  }
  ss << "master_repl_offset:" << master_repl_offset << "\r\n";
}


void ReplManager::getReplInfoDetail(std::stringstream& ss) const {
  for (size_t i = 0; i < _svr->getKVStoreCount(); ++i) {
    std::lock_guard<std::mutex> lk(_mutex);
    if (_syncMeta[i]->syncFromHost != "") {
      std::string state = getEnumStr(_syncMeta[i]->replState);
      ss << "rocksdb" << i << "_master:";
      ss << "ip=" << _syncMeta[i]->syncFromHost;
      ss << ",port=" << _syncMeta[i]->syncFromPort;
      ss << ",src_store_id=" << _syncMeta[i]->syncFromId;
      ss << ",state=" << state;
      ss << ",fullsync_succ_times=" << _syncStatus[i]->fullsyncSuccTimes;
      ss << ",binlog_pos=" << _syncMeta[i]->binlogId;
      // lag in seconds
      ss << ",lag=" << (msSinceEpoch() - _syncStatus[i]->lastBinlogTs) / 1000;
      if (_syncMeta[i]->replState == ReplState::REPL_ERR) {
        ss << ",error=" << _syncMeta[i]->replErr;
      }
      ss << "\r\n";
    }
  }
  for (size_t i = 0; i < _svr->getKVStoreCount(); ++i) {
    auto expdb =
      _svr->getSegmentMgr()->getDb(nullptr, i, mgl::LockMode::LOCK_IS, true, 0);
    if (!expdb.ok()) {
      continue;
    }
    uint64_t highestBinlogid = expdb.value().store->getHighestBinlogId();
    std::lock_guard<std::mutex> lk(_mutex);
    size_t j = 0;
    for (auto iter = _pushStatus[i].begin(); iter != _pushStatus[i].end();
         ++iter) {
      ss << "rocksdb" << i << "_slave" << j++ << ":";
      ss << "ip=" << iter->second->slave_listen_ip;
      ss << ",port=" << iter->second->slave_listen_port;
      ss << ",dest_store_id=" << iter->second->dstStoreId;
      ss << ",state="
         << "online";  // TODO(vinchen)
      ss << ",binlog_pos=" << iter->second->binlogPos;
      // lag in seconds
      ss << ",lag=" << (msSinceEpoch() - iter->second->binlogTs) / 1000;
      ss << ",binlog_lag=" << highestBinlogid - iter->second->binlogPos;
      ss << "\r\n";
    }
    j = 0;
    for (auto iter = _fullPushStatus[i].begin();
         iter != _fullPushStatus[i].end();
         ++iter) {
      std::string state = getEnumStr(iter->second->state);
      ss << "rocksdb" << i << "_slave" << j++ << ":";
      ss << ",ip=" << iter->second->slave_listen_ip;
      ss << ",port=" << iter->second->slave_listen_port;
      ss << ",dest_store_id=" << iter->second->storeid;
      ss << ",state=" << state;
      ss << ",binlog_pos=" << iter->second->binlogPos;
      // duration in seconds
      ss << ",duration="
         << (sinceEpoch() - sinceEpoch(iter->second->startTime));
      ss << ",binlog_lag=" << highestBinlogid - iter->second->binlogPos;
      ss << "\r\n";
    }
  }
}

void ReplManager::appendJSONStat(
  rapidjson::PrettyWriter<rapidjson::StringBuffer>& w) const {
  std::lock_guard<std::mutex> lk(_mutex);
  INVARIANT(_pushStatus.size() == _svr->getKVStoreCount());
  INVARIANT(_syncStatus.size() == _svr->getKVStoreCount());
  for (size_t i = 0; i < _svr->getKVStoreCount(); ++i) {
    std::stringstream ss;
    ss << i;
    w.Key(ss.str().c_str());
    w.StartObject();

    w.Key("first_binlog");
    w.Uint64(_logRecycStatus[i]->firstBinlogId);

    w.Key("timestamp");
    w.Uint64(_logRecycStatus[i]->timestamp);

    w.Key("incr_paused");
    w.Uint64(_incrPaused);

    w.Key("sync_dest");
    w.StartObject();
    // sync to
    for (auto& mpov : _pushStatus[i]) {
      std::stringstream ss;
      ss << "client_" << mpov.second->clientId;
      w.Key(ss.str().c_str());
      w.StartObject();
      w.Key("is_running");
      w.Uint64(mpov.second->isRunning);
      w.Key("dest_store_id");
      w.Uint64(mpov.second->dstStoreId);
      w.Key("binlog_pos");
      w.Uint64(mpov.second->binlogPos);
      w.Key("remote_host");
      if (mpov.second->client != nullptr) {
        w.String(mpov.second->client->getRemoteRepr());
      } else {
        w.String("???");
      }
      w.EndObject();
    }
    w.EndObject();

    // sync from
    w.Key("sync_source");
    ss.str("");
    ss << _syncMeta[i]->syncFromHost << ":" << _syncMeta[i]->syncFromPort << ":"
       << _syncMeta[i]->syncFromId;
    w.String(ss.str().c_str());
    w.Key("binlog_id");
    w.Uint64(_syncMeta[i]->binlogId);
    w.Key("repl_state");
    w.Uint64(static_cast<uint64_t>(_syncMeta[i]->replState));
    w.Key("fullsync_succ_times");
    w.Uint64(static_cast<uint64_t>(_syncStatus[i]->fullsyncSuccTimes));
    w.Key("last_sync_time");
    w.String(timePointRepr(_syncStatus[i]->lastSyncTime));

    w.EndObject();
  }
}

void ReplManager::stop() {
  LOG(WARNING) << "repl manager begins stops...";
  _isRunning.store(false, std::memory_order_relaxed);
  _controller->join();

  // make sure all workpool has been stopped; otherwise calling
  // the destructor of a std::thread that is running will crash
  _fullPusher->stop();
  _incrPusher->stop();
  _fullReceiver->stop();
  _incrChecker->stop();
  _logRecycler->stop();

#if defined(_WIN32) && _MSC_VER > 1900
  for (size_t i = 0; i < _pushStatus.size(); i++) {
    for (auto& mpov : _pushStatus[i]) {
      delete mpov.second;
    }
    _pushStatus[i].clear();
  }

  for (size_t i = 0; i < _fullPushStatus.size(); i++) {
    for (auto& mpov : _fullPushStatus[i]) {
      delete mpov.second;
    }
    _fullPushStatus[i].clear();
  }
#endif
  // after _logRecycler->stop(), recycleBinlog() has quit,
  // so here can call fs->close().
  std::unique_lock<std::mutex> lk(_mutex);
  for (size_t i = 0; i < _logRecycStatus.size(); i++) {
    if (_logRecycStatus[i]->fs) {
      _logRecycStatus[i]->fs->close();
      _logRecycStatus[i]->fs.reset();
    }
  }
  LOG(WARNING) << "repl manager stops succ";
}

void ReplManager::fullPusherResize(size_t size) {
  if (size > _svr->getKVStoreCount()) {
    LOG(INFO) << "`fullPushThreadnum` is not allowed to be greater than "
                 "`kvstorecount`, set from "
              << size << " to " << _svr->getKVStoreCount();
    size = _svr->getKVStoreCount();
  }

  _fullPusher->resize(size);
}

void ReplManager::fullReceiverResize(size_t size) {
  if (size > _svr->getKVStoreCount()) {
    LOG(INFO) << "`fullReceiveThreadnum` is not allowed to be greater than "
                 "`kvstorecount`, set from "
              << size << " to " << _svr->getKVStoreCount();
    size = _svr->getKVStoreCount();
  }

  _fullReceiver->resize(size);
}

void ReplManager::incrPusherResize(size_t size) {
  if (size > _svr->getKVStoreCount()) {
    LOG(INFO) << "`incrPushThreadnum` is not allowed to be greater than "
                 "`kvstorecount`, set from "
              << size << " to " << _svr->getKVStoreCount();
    size = _svr->getKVStoreCount();
  }

  _incrPusher->resize(size);
}

void ReplManager::logRecyclerResize(size_t size) {
  if (size > _svr->getKVStoreCount()) {
    LOG(INFO) << "`logRecycleThreadnum` is not allowed to be greater than "
                 "`kvstorecount`, set from "
              << size << " to " << _svr->getKVStoreCount();
    size = _svr->getKVStoreCount();
  }

  _logRecycler->resize(size);
}

size_t ReplManager::fullPusherSize() {
  return _fullPusher->size();
}

size_t ReplManager::fullReceiverSize() {
  return _fullReceiver->size();
}

size_t ReplManager::incrPusherSize() {
  return _incrPusher->size();
}

size_t ReplManager::logRecycleSize() {
  return _logRecycler->size();
}

}  // namespace tendisplus
