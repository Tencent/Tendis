// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <memory>
#include <utility>
#include <map>
#include <list>
#include <algorithm>
#include <limits>
#include "tendisplus/server/segment_manager.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/server/server_entry.h"


namespace tendisplus {

SegmentMgr::SegmentMgr(const std::string& name) : _name(name) {}

SegmentMgrFnvHash64::SegmentMgrFnvHash64(
  const std::vector<std::shared_ptr<KVStore>>& ins, const size_t chunkSize)
  : SegmentMgr("fnv_hash_64"),
    _instances(ins),
    _chunkSize(chunkSize),
    _movedNum(0) {}

uint32_t SegmentMgrFnvHash64::getStoreid(uint32_t chunkid) {
  return chunkid % _instances.size();
}

Expected<DbWithLock> SegmentMgrFnvHash64::getDbWithKeyLock(
  Session* sess, const std::string& key, mgl::LockMode mode) {
  uint32_t hash = uint32_t(redis_port::keyHashSlot(key.c_str(), key.size()));
  INVARIANT_D(hash < _chunkSize);
  uint32_t chunkId = hash % _chunkSize;
  INVARIANT(_chunkSize == CLUSTER_SLOTS);
  uint32_t segId = getStoreid(chunkId);

  // a duration of 49 days.
  uint64_t lockTimeoutMs = std::numeric_limits<uint32_t>::max();
  bool cluster_enabled = false;
  bool slave_readonly = true;
  std::shared_ptr<ClusterState> clusterState;
  if (sess && sess->getServerEntry()) {
    const auto& cfg = sess->getServerEntry()->getParams();
    lockTimeoutMs = (uint64_t)cfg->lockWaitTimeOut * 1000;
    cluster_enabled = sess->getServerEntry()->isClusterEnabled();
    clusterState = cluster_enabled
      ? sess->getServerEntry()->getClusterMgr()->getClusterState()
      : nullptr;
    /* NOTE(wayenchen) if aofPsync is enable and it is the master session, the
     * session should be able to run command */
    if (cfg->psyncEnabled && sess->getCtx()->isMaster()) {
      slave_readonly = false;
    }
  }

  if (!_instances[segId]->isOpen()) {
    _instances[segId]->stat.destroyedErrorCount.fetch_add(
      1, std::memory_order_relaxed);

    std::stringstream ss;
    ss << "store id " << segId << " is not opened";
    return {ErrorCodes::ERR_INTERNAL, ss.str()};
  }

  if (_instances[segId]->isPaused()) {
    _instances[segId]->stat.pausedErrorCount.fetch_add(
      1, std::memory_order_relaxed);

    std::stringstream ss;
    ss << "store id " << segId << " is paused";
    return {ErrorCodes::ERR_INTERNAL, ss.str()};
  }

  if (sess && sess->getCtx() &&
      _instances[segId]->getMode() == KVStore::StoreMode::REPLICATE_ONLY) {
    sess->getCtx()->setReplOnly(slave_readonly);
  }

  if (mode != mgl::LockMode::LOCK_NONE) {
    auto elk = KeyLock::AquireKeyLock(segId,
                                      chunkId,
                                      key,
                                      mode,
                                      sess,
                                      (sess && sess->getServerEntry())
                                        ? sess->getServerEntry()->getMGLockMgr()
                                        : nullptr,
                                      lockTimeoutMs);
    if (!elk.ok()) {
      return elk.status();
    }

    if (cluster_enabled && slave_readonly) {
      auto node = clusterState->clusterHandleRedirect(chunkId, sess);
      if (!node.ok())
        return node.status();
    }
    return DbWithLock{
      segId, chunkId, _instances[segId], nullptr, std::move(elk.value())};
  } else {
    if (cluster_enabled && slave_readonly) {
      auto node = clusterState->clusterHandleRedirect(chunkId, sess);
      if (!node.ok())
        return node.status();
    }
    return DbWithLock{segId, chunkId, _instances[segId], nullptr, nullptr};
  }
}

Expected<DbWithLock> SegmentMgrFnvHash64::getDbHasLocked(
  Session* sess, const std::string& key) {
  uint32_t hash = uint32_t(redis_port::keyHashSlot(key.c_str(), key.size()));
  INVARIANT_D(hash < _chunkSize);
  uint32_t chunkId = hash % _chunkSize;
  INVARIANT(_chunkSize == CLUSTER_SLOTS);
  uint32_t segId = chunkId % _instances.size();

  if (!_instances[segId]->isOpen()) {
    _instances[segId]->stat.destroyedErrorCount.fetch_add(
      1, std::memory_order_relaxed);

    std::stringstream ss;
    ss << "store id " << segId << " is not opened";
    return {ErrorCodes::ERR_INTERNAL, ss.str()};
  }

  if (_instances[segId]->isPaused()) {
    _instances[segId]->stat.pausedErrorCount.fetch_add(
      1, std::memory_order_relaxed);
    std::stringstream ss;
    ss << "store id " << segId << " is paused";
    return {ErrorCodes::ERR_INTERNAL, ss.str()};
  }

  if (sess && sess->getCtx() &&
      _instances[segId]->getMode() == KVStore::StoreMode::REPLICATE_ONLY) {
    bool slave_readonly = true;
    /* NOTE(wayenchen) if aofPsync is enable and it is the master session, the
     * session should be able to run command */
    if (sess->getServerEntry()->getParams()->psyncEnabled &&
        sess->getCtx()->isMaster())
      slave_readonly = false;
    sess->getCtx()->setReplOnly(slave_readonly);
  }

  return DbWithLock{segId, chunkId, _instances[segId], nullptr, nullptr};
}

Expected<std::list<std::unique_ptr<KeyLock>>>
SegmentMgrFnvHash64::getAllKeysLocked(Session* sess,
                                      const std::vector<std::string>& args,
                                      const std::vector<int>& index,
                                      mgl::LockMode mode) {
  std::list<std::unique_ptr<KeyLock>> locklist;

  if (mode == mgl::LockMode::LOCK_NONE) {
    return locklist;
  }
  // a duration of 49 days.
  uint64_t lockTimeoutMs = std::numeric_limits<uint32_t>::max();
  bool cluster_enabled = false;
  bool clusterSingle = false;
  bool slave_readonly = true;
  std::shared_ptr<ClusterState> clusterState;
  if (sess && sess->getServerEntry()) {
    const auto& cfg = sess->getServerEntry()->getParams();
    lockTimeoutMs = (uint64_t)cfg->lockWaitTimeOut * 1000;
    cluster_enabled = sess->getServerEntry()->isClusterEnabled();
    clusterSingle = cfg->clusterSingleNode;
    clusterState = cluster_enabled
      ? sess->getServerEntry()->getClusterMgr()->getClusterState()
      : nullptr;
    /* NOTE(wayenchen) if aofPsync is enable and it is the master session, the
     * session should be able to run command */
    if (cfg->psyncEnabled && sess->getCtx()->isMaster()) {
      slave_readonly = false;
    }
  }

  std::map<uint32_t, std::vector<std::pair<uint32_t, std::string>>> segList;
  uint32_t last_chunkId = -1;
  uint32_t last_segId = 0;
  for (auto iter = index.begin(); iter != index.end(); iter++) {
    auto key = args[*iter];
    uint32_t hash = redis_port::keyHashSlot(key.c_str(), key.size());
    INVARIANT_D(hash < _chunkSize);
    uint32_t chunkId = hash % _chunkSize;
    uint32_t segId = chunkId % _instances.size();
    segList[segId].emplace_back(std::make_pair(chunkId, std::move(key)));

    last_segId = segId;
    if (last_chunkId == (uint32_t)-1) {
      last_chunkId = chunkId;
    } else if (last_chunkId != chunkId) {
      if (cluster_enabled && !clusterSingle) {
        return {ErrorCodes::ERR_CLUSTER_REDIR_CROSS_SLOT, ""};
      }
      last_chunkId = chunkId;
    }
  }

  if (sess && sess->getCtx() &&
      _instances[last_segId]->getMode() == KVStore::StoreMode::REPLICATE_ONLY) {
    sess->getCtx()->setReplOnly(slave_readonly);
  }


  /* NOTE(vinchen): lock sequence
      lock kvstores from small to big(kvstore id)
          lock chunks from small to big(chunk id) in kvstore
              lock keys from small to big(key name) in chunk
  */
  for (auto& element : segList) {
    uint32_t segId = element.first;
    auto keysvec = element.second;
    std::sort(std::begin(keysvec),
              std::end(keysvec),
              [](const std::pair<uint32_t, std::string>& a,
                 const std::pair<uint32_t, std::string>& b) {
                return a.first < b.first ||
                  (a.first == b.first && a.second < b.second);
              });
    for (auto keyIter = keysvec.begin(); keyIter != keysvec.end(); keyIter++) {
      const auto& pair = *keyIter;
      uint32_t chunkId = pair.first;
      auto elk =
        KeyLock::AquireKeyLock(segId,
                               chunkId,
                               pair.second,
                               mode,
                               sess,
                               (sess && sess->getServerEntry())
                                 ? sess->getServerEntry()->getMGLockMgr()
                                 : nullptr,
                               lockTimeoutMs);
      if (!elk.ok()) {
        return elk.status();
      }
      locklist.emplace_back(std::move(elk.value()));
    }
  }

  if (last_chunkId != (uint32_t)-1 && sess &&
      clusterState != nullptr) {
    if (slave_readonly) {
      auto node = clusterState->clusterHandleRedirect(last_chunkId, sess);
      if (!node.ok()) {
        return node.status();
      }
    }
  }

  return locklist;
}

Expected<DbWithLock> SegmentMgrFnvHash64::getDb(Session* sess,
                                                uint32_t insId,
                                                mgl::LockMode mode,
                                                bool canOpenStoreNoneDB,
                                                uint64_t lock_wait_timeout) {
  if (insId >= _instances.size()) {
    return {ErrorCodes::ERR_INTERNAL, "invalid instance id"};
  }

  if (!canOpenStoreNoneDB) {
    if (!_instances[insId]->isOpen()) {
      _instances[insId]->stat.destroyedErrorCount.fetch_add(
        1, std::memory_order_relaxed);

      std::stringstream ss;
      ss << "store id " << insId << " is not opened";
      return {ErrorCodes::ERR_INTERNAL, ss.str()};
    }
  }

  // a duration of 49 days.
  uint64_t lockTimeoutMs = std::numeric_limits<uint32_t>::max();
  std::shared_ptr<ServerParams> cfg = (sess && sess->getServerEntry())
    ? sess->getServerEntry()->getParams()
    : nullptr;
  if (lock_wait_timeout == (uint64_t)-1) {
    if (mode == mgl::LockMode::LOCK_X) {
      /* *
       * If get db using LOCK_X, it can't wait
       * a long time. Otherwise, the waiting LOCK_X
       * would block all following the requests.
       */
      lockTimeoutMs =
        (cfg == nullptr) ? 1000 : (uint64_t)cfg->lockDbXWaitTimeout * 1000;
    } else if (cfg) {
      lockTimeoutMs = (uint64_t)cfg->lockWaitTimeOut * 1000;
    }
  } else {
    // NOTE(vinchen) : if lock_wait_timeout == 0, it means we don't wait
    // anything, such as running `info` when kvstore is restarting.
    lockTimeoutMs = lock_wait_timeout;
  }

  std::unique_ptr<StoreLock> lk = nullptr;
  if (mode != mgl::LockMode::LOCK_NONE) {
    auto elk = StoreLock::AquireStoreLock(
      insId,
      mode,
      sess,
      (sess && sess->getServerEntry()) ? sess->getServerEntry()->getMGLockMgr()
                                       : nullptr,
      lockTimeoutMs);
    if (!elk.ok()) {
      LOG(WARNING) << "store id " << insId
                   << " can't been opened:" << elk.status().toString();
      return elk.status();
    }
    lk = std::move(elk.value());
  }

  if (sess && sess->getCtx() &&
      _instances[insId]->getMode() == KVStore::StoreMode::REPLICATE_ONLY) {
    sess->getCtx()->setReplOnly(true);
  }
  return DbWithLock{insId, 0, _instances[insId], std::move(lk), nullptr};
}

}  // namespace tendisplus
