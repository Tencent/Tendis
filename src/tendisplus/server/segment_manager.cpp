// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <memory>
#include <utility>
#include <map>
#include <list>
#include <algorithm>
#include <limits>
#include <unordered_set>
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
  bool clusterEnabled = false;
  bool isSessionReplOnly = true;
  std::shared_ptr<ClusterState> clusterState;
  if (sess && sess->getServerEntry()) {
    const auto& cfg = sess->getServerEntry()->getParams();
    lockTimeoutMs = (uint64_t)cfg->lockWaitTimeOut * 1000;
    clusterEnabled = sess->getServerEntry()->isClusterEnabled();
    clusterState = clusterEnabled
      ? sess->getServerEntry()->getClusterMgr()->getClusterState()
      : nullptr;
    /* NOTE(wayenchen) if aofPsync is enable and it is the master session, the
     * session should be able to run command */
    if (cfg->psyncEnabled && sess->getCtx()->isMaster()) {
      isSessionReplOnly = false;
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
    sess->getCtx()->setReplOnly(isSessionReplOnly);
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

    if (clusterEnabled && isSessionReplOnly) {
      auto node = clusterState->clusterHandleRedirect(chunkId, sess);
      if (!node.ok())
        return node.status();
    }
    return DbWithLock{
      segId, chunkId, _instances[segId], nullptr, std::move(elk.value())};
  } else {
    if (clusterEnabled && isSessionReplOnly) {
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
  uint32_t segId = getStoreid(chunkId);

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
    bool isSessionReplOnly = true;
    /* NOTE(wayenchen) if aofPsync is enable and it is the master session, the
     * session should be able to run command */
    if (sess->getServerEntry()->getParams()->psyncEnabled &&
        sess->getCtx()->isMaster())
      isSessionReplOnly = false;
    sess->getCtx()->setReplOnly(isSessionReplOnly);
  }

  return DbWithLock{segId, chunkId, _instances[segId], nullptr, nullptr};
}

Expected<std::list<std::unique_ptr<KeyLock>>>
SegmentMgrFnvHash64::getAllKeysLocked(Session* sess,
                                      const std::vector<std::string>& args,
                                      const std::vector<int>& index,
                                      mgl::LockMode mode,
                                      int cmdFlag) {
  std::list<std::unique_ptr<KeyLock>> locklist;

  if (mode == mgl::LockMode::LOCK_NONE) {
    return locklist;
  }
  // a duration of 49 days.
  uint64_t lockTimeoutMs = std::numeric_limits<uint32_t>::max();
  bool clusterEnabled = false;
  bool clusterSingle = false;
  bool isSessionReplOnly = true;
  bool cmdAllowCrossSlot =
    sess->getServerEntry()->getParams()->allowCrossSlot &&
    ((cmdFlag & CMD_ALLOW_CROSS_SLOT) != 0);
  std::shared_ptr<ClusterState> clusterState;
  if (sess && sess->getServerEntry()) {
    const auto& cfg = sess->getServerEntry()->getParams();
    lockTimeoutMs = uint64_t(cfg->lockWaitTimeOut * 1000);
    clusterEnabled = sess->getServerEntry()->isClusterEnabled();
    clusterSingle = cfg->clusterSingleNode;
    clusterState = clusterEnabled
      ? sess->getServerEntry()->getClusterMgr()->getClusterState()
      : nullptr;
    /* NOTE(wayenchen) if aofPsync is enable and it is the master session, the
     * session should be able to run command */
    if (cfg->psyncEnabled && sess->getCtx()->isMaster()) {
      isSessionReplOnly = false;
    }
  }
  bool clusterHasMultiNodes = clusterEnabled && !clusterSingle;

  std::unordered_set<uint32_t> chunkSet;
  std::unordered_set<std::string> nodeSet;
  std::map<uint32_t, std::vector<std::pair<uint32_t, std::string>>> segList;
  for (const auto& it : index) {
    const auto& key = args[it];
    uint32_t hash = uint32_t(redis_port::keyHashSlot(key.c_str(), key.size()));
    INVARIANT_D(hash < _chunkSize);
    uint32_t chunkId = hash % _chunkSize;
    INVARIANT(_chunkSize == CLUSTER_SLOTS);
    uint32_t segId = getStoreid(chunkId);
    segList[segId].emplace_back(std::make_pair(chunkId, std::move(key)));
    chunkSet.insert(chunkId);

    // check if keys cross node if cluster has many nodes.
    if (clusterHasMultiNodes && cmdAllowCrossSlot) {
      auto node = clusterState->getNodeBySlot(chunkId);
      if (!node) {
        return {ErrorCodes::ERR_CLUSTER_REDIR_DOWN_UNBOUND, ""};
      }
      nodeSet.insert(node->getNodeName());
    }
  }

  // only need to check whether the first involved kvstore is REPLICATE_ONLY
  if (sess && sess->getCtx() && !chunkSet.empty() &&
      _instances[getStoreid(*chunkSet.begin())]->getMode() ==
      KVStore::StoreMode::REPLICATE_ONLY) {
      sess->getCtx()->setReplOnly(isSessionReplOnly);
  }

  if (clusterHasMultiNodes) {  // clusterEnabled && !clusterSingle
    if (cmdAllowCrossSlot) {
      if (nodeSet.size() > 1) {
        return {ErrorCodes::ERR_CLUSTER_REDIR_CROSS_SLOT, ""};
      }
    } else {
      if (chunkSet.size() > 1) {
        return {ErrorCodes::ERR_CLUSTER_REDIR_CROSS_SLOT, ""};
      }
    }
  }
  // from here, all slots should belong to single node.
  // then, lock all chunk and check if cmd need move.

  /* NOTE(vinchen): lock sequence
      lock kvstores from small to big(kvstore id)
          lock chunks from small to big(chunk id) in kvstore
              lock keys from small to big(key name) in chunk
  */
  for (const auto& element : segList) {
    uint32_t segId = element.first;
    auto keysvec = element.second;
    std::sort(keysvec.begin(),
              keysvec.end(),
              [](const std::pair<uint32_t, std::string>& a,
                 const std::pair<uint32_t, std::string>& b) {
                return a.first < b.first ||
                  (a.first == b.first && a.second < b.second);
              });
    for (const auto& pair : keysvec) {
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

  // Note: clusterHandleRedirect must been called after KeyLock
  // In case of data migration finished before all chunks locked, the nodeSet
  // info may be outdated, ensure all chunks belong to me here.
  if (clusterHasMultiNodes && isSessionReplOnly && !chunkSet.empty()) {
    for (const auto& chunk : chunkSet) {
      auto node =
        clusterState->clusterHandleRedirect(chunk, sess);
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
