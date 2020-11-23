// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_LOCK_MGL_MGL_MGR_H__
#define SRC_TENDISPLUS_LOCK_MGL_MGL_MGR_H__

#include <vector>
#include <list>
#include <unordered_map>
#include <mutex>  // NOLINT
#include <string>
#include <set>

#include "tendisplus/lock/mgl/lock_defines.h"

namespace tendisplus {
namespace mgl {

class MGLock;

// TODO(deyukong): this class should only be in mgl_mgr.cpp
// not thread safe, protected by LockShard's mutex
class LockSchedCtx {
 public:
  LockSchedCtx();
  LockSchedCtx(LockSchedCtx&&) = default;
  void lock(MGLock* core);
  bool unlock(MGLock* core);
  std::string toString();
  std::vector<std::string> getShardLocks();
 private:
  void schedPendingLocks();
  void incrPendingRef(LockMode mode);
  void incrRunningRef(LockMode mode);
  void decPendingRef(LockMode mode);
  void decRunningRef(LockMode mode);
  uint16_t _runningModes;
  uint16_t _pendingModes;
  std::vector<uint16_t> _runningRefCnt;
  std::vector<uint16_t> _pendingRefCnt;
  std::list<MGLock*> _runningList;
  std::list<MGLock*> _pendingList;
};

/* First come first lock
  for example:
  lock sequences(different sessions): IS IS X IX S IS S
  _runingModes = IS | IS
  _pendingModes = X | IX | S | IS
  _runningRefCnt = IS -> 2
  _pendingRefCnt = IS -> 1, IX -> 1, S -> 2, X -> 1
  _runningList = {IS, IS}
  _pendingList = {X, IX, S, IS, S}

  For same session: LOCK(IX), LOCK(X), it would lead to deadlock
*/
// class alignas(std::hardware_destructive_interference_size) LockShard {
// hardware_destructive_interference_size requires quite high version
// gcc. 128 should work for most cases
struct alignas(128) LockShard {
  std::mutex mutex;
  std::unordered_map<std::string, LockSchedCtx> map;
};

// TODO(vinchen): now there is a warning here, because the MGLockMgr change from
// a static object to a heap object of ServerEntry
// warning C4316: tendisplus::mgl::MGLockMgr
class MGLockMgr {
 public:
  MGLockMgr() = default;
  void lock(MGLock* core);
  void unlock(MGLock* core);
  static MGLockMgr& getInstance();
  std::string toString();
  std::vector<std::string> getLockList();

 private:
  static constexpr size_t SHARD_NUM = 32;
  LockShard _shards[SHARD_NUM];
};

}  // namespace mgl

}  // namespace tendisplus
#endif  // SRC_TENDISPLUS_LOCK_MGL_MGL_MGR_H__
