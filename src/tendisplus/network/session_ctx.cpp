// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <utility>
#include <string>
#include <algorithm>
#include <list>
#include <vector>

#include "tendisplus/commands/command.h"
#include "tendisplus/network/session_ctx.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/string.h"

namespace tendisplus {

SessionCtx::SessionCtx(Session* sess)
  : _authed(false),
    _dbId(0),
    _waitlockStore(0),
    _waitlockChunk(0),
    _waitlockMode(mgl::LockMode::LOCK_NONE),
    _waitlockKey(""),
    _processPacketStart(0),
    _timestamp(TSEP_UNINITED),
    _version(VERSIONEP_UNINITED),
    _perfLevel(PerfLevel::kDisable),
    _perfLevelFlag(false),
    _txnVersion(-1),
    _extendProtocol(false),
    _replOnly(false),
    _session(sess),
    _isMonitor(false),
    _flags(0) {
  _perfContext.Reset();
  _ioContext.Reset();
}

void SessionCtx::setProcessPacketStart(uint64_t start) {
  _processPacketStart = start;
}

uint64_t SessionCtx::getProcessPacketStart() const {
  return _processPacketStart;
}

bool SessionCtx::authed() const {
  return _authed;
}

uint32_t SessionCtx::getDbId() const {
  return _dbId;
}

void SessionCtx::setDbId(uint32_t dbid) {
  _dbId = dbid;
}

void SessionCtx::setAuthed() {
  _authed = true;
}

void SessionCtx::addLock(ILock* lock) {
  std::lock_guard<std::mutex> lk(_mutex);
  _locks.push_back(lock);
}

void SessionCtx::removeLock(ILock* lock) {
  std::lock_guard<std::mutex> lk(_mutex);
  for (auto it = _locks.begin(); it != _locks.end(); ++it) {
    if (*it == lock) {
      _locks.erase(it);
      return;
    }
  }
  INVARIANT_D(0);
}

std::vector<std::string> SessionCtx::getArgsBrief() const {
  std::lock_guard<std::mutex> lk(_mutex);
  return _argsBrief;
}

void SessionCtx::setArgsBrief(const std::vector<std::string>& v) {
  std::lock_guard<std::mutex> lk(_mutex);
  _argsBrief.clear();
  constexpr size_t MAX_SIZE = 8;
  for (size_t i = 0; i < std::min(v.size(), MAX_SIZE); ++i) {
    _argsBrief.push_back(v[i]);
  }
}

void SessionCtx::clearRequestCtx() {
  std::lock_guard<std::mutex> lk(_mutex);
  _txnMap.clear();

  _argsBrief.clear();
  _timestamp = -1;
  _version = -1;
  if (_perfLevelFlag && _perfLevel >= PerfLevel::kEnableCount) {
    // NOTE(vinchen): rocksdb::get_perf_context() is thread local variables,
    // because of thread pool, "info rocksdbperfstat" can't use
    // rocksdb::get_perf_context() directly.
    _perfContext = *rocksdb::get_perf_context();
    _ioContext = *rocksdb::get_iostats_context();
  }
  _perfLevelFlag = false;
}

Expected<Transaction*> SessionCtx::createTransaction(const PStore& kvstore) {
  std::lock_guard<std::mutex> lk(_mutex);
  Transaction* txn = nullptr;
  if (_txnMap.count(kvstore->dbId()) > 0) {
    txn = _txnMap[kvstore->dbId()].get();
  } else {
    auto ptxn = kvstore->createTransaction(_session);
    if (!ptxn.ok()) {
      return ptxn.status();
    }
    _txnMap[kvstore->dbId()] = std::move(ptxn.value());
    txn = _txnMap[kvstore->dbId()].get();
  }

  return txn;
}

Expected<uint64_t> SessionCtx::commitTransaction(Transaction* txn) {
  std::lock_guard<std::mutex> lk(_mutex);
  INVARIANT_D(_txnMap.count(txn->getKVStoreId()) > 0);
  INVARIANT_D(_txnMap[txn->getKVStoreId()].get() == txn);
  auto eCmt = txn->commit();
  if (!eCmt.ok()) {
    return eCmt.status();
  }
  if (_txnMap.count(txn->getKVStoreId()) > 0
    && _txnMap[txn->getKVStoreId()].get() == txn) {
    _txnMap.erase(txn->getKVStoreId());
  } else {
    LOG(ERROR) << "what happend? has:" << _txnMap.count(txn->getKVStoreId())
      << " addr1:" << _txnMap[txn->getKVStoreId()].get()
      << " addr2:" << txn;
  }
  return eCmt;
}

Status SessionCtx::commitAll(const std::string& cmd) {
  std::lock_guard<std::mutex> lk(_mutex);

  Status s;
  for (auto& txn : _txnMap) {
    Expected<uint64_t> exptCommit = txn.second->commit();
    if (!exptCommit.ok()) {
      LOG(ERROR) << cmd << " commit error at kvstore " << txn.first
                 << ". It lead to partial success.";
      s = exptCommit.status();
    }
  }
  _txnMap.clear();
  return s;
}

Status SessionCtx::rollbackAll() {
  std::lock_guard<std::mutex> lk(_mutex);
  Status s = {ErrorCodes::ERR_OK, ""};
  for (auto& txn : _txnMap) {
    s = txn.second->rollback();
    if (!s.ok()) {
      LOG(ERROR) << "rollback error at kvstore " << txn.first
                 << ". It maybe lead to partial success.";
    }
  }
  _txnMap.clear();
  return s;
}

void SessionCtx::setWaitLock(uint32_t storeId,
                             uint32_t chunkId,
                             const std::string& key,
                             mgl::LockMode mode) {
  _waitlockStore = storeId;
  _waitlockChunk = chunkId;
  _waitlockMode = mode;
  _waitlockKey = key;
}

SLSP SessionCtx::getWaitlock() const {
  return std::tuple<uint32_t, uint32_t, std::string, mgl::LockMode>(
    _waitlockStore, _waitlockChunk, _waitlockKey, _waitlockMode);
}

std::list<SLSP> SessionCtx::getLockStates() const {
  std::lock_guard<std::mutex> lk(_mutex);
  std::list<SLSP> result;
  for (auto& lk : _locks) {
    result.push_back(
      SLSP(lk->getStoreId(), lk->getChunkId(), lk->getKey(), lk->getMode()));
  }
  return result;
}

void SessionCtx::setExtendProtocol(bool v) {
  _extendProtocol = v;
}
void SessionCtx::setExtendProtocolValue(uint64_t ts, uint64_t version) {
  _timestamp = ts;
  _version = version;
}

// reture false, mean error
bool SessionCtx::setPerfLevel(const std::string& level) {
  auto l = toLower(level);
  if (l == "disable") {
    _perfLevel = PerfLevel::kDisable;
  } else if (l == "enable_count") {
    _perfLevel = PerfLevel::kEnableCount;
  } else if (l == "enable_time_expect_for_mutex") {
    _perfLevel = PerfLevel::kEnableTimeExceptForMutex;
  } else if (l == "enable_time_and_cputime_expect_for_mutex") {
    _perfLevel = PerfLevel::kEnableTimeAndCPUTimeExceptForMutex;
  } else if (l == "enable_time") {
    _perfLevel = PerfLevel::kEnableTime;
  } else {
    return false;
  }
  return true;
}

bool SessionCtx::needResetPerLevel() {
  if (_perfLevelFlag) {
    return false;
  }

  // NOTE(vinchen): _perfLevelFlag mean that the the first time
  // call of SessionCtx::needResetPerLevel(). It would be reset
  // when SessionCtx::clearRequestCtx()
  _perfLevelFlag = true;

  if (_perfLevel < PerfLevel::kEnableCount) {
    return false;
  }

  return true;
}

std::string SessionCtx::getPerfContextStr() const {
  return _perfContext.ToString();
}

std::string SessionCtx::getIOstatsContextStr() const {
  return _ioContext.ToString();
}

uint32_t SessionCtx::getIsMonitor() const {
  return _isMonitor;
}

void SessionCtx::setIsMonitor(bool in) {
  _isMonitor = in;
}

void SessionCtx::setKeylock(const std::string& key, mgl::LockMode mode) {
  std::lock_guard<std::mutex> lk(_mutex);
  _keylockmap[key] = mode;
}

void SessionCtx::unsetKeylock(const std::string& key) {
  std::lock_guard<std::mutex> lk(_mutex);
  INVARIANT_D(_keylockmap.count(key) > 0);
  _keylockmap.erase(key);
}

bool SessionCtx::isLockedByMe(const std::string& key, mgl::LockMode mode) {
  std::lock_guard<std::mutex> lk(_mutex);
  if (_keylockmap.count(key) > 0) {
    // TODO(comboqiu): Here, lock can't upgrade or downgrade.
    // If a key lock twice in one session , it can't lock a bigger lock.
    // assert temporary.
    INVARIANT_D(mgl::enum2Int(mode) <= mgl::enum2Int(_keylockmap[key]));
    return true;
  }
  return false;
}

bool SessionCtx::verifyVersion(uint64_t keyVersion) {
  if (isInMulti()) {
    if (_txnVersion != _version) {
      // we use only one version inside the entire txn.
      return false;
    }
    // now we have _txnVersion eq to _version
    return !(_txnVersion < keyVersion && keyVersion != UINT64_MAX);
  }

  if (_version == UINT64_MAX) {
    // isolate tendis cmd cannot modify value of tendis with cache.
    // if (eValue.value().getVersionEP() != UINT64_MAX) {
    //    return {ErrorCodes::ERR_WRONG_VERSION_EP, ""};
    //}
  } else {
    // any command can modify value with versionEP = -1
    if (_version <= keyVersion && keyVersion != UINT64_MAX) {
      return false;
    }
  }
  return true;
}

}  // namespace tendisplus
