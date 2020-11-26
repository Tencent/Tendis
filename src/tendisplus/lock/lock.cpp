// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <memory>
#include <limits>
#include "tendisplus/lock/lock.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/server/server_entry.h"

namespace tendisplus {

const char StoresLock::_target[] = "stores";

ILock::ILock(ILock* parent, mgl::MGLock* lk, Session* sess)
  : _lockResult(mgl::LockRes::LOCKRES_WAIT),
    _parent(parent),
    _mgl(lk),
    _sess(sess) {}

ILock::~ILock() {
  if (_mgl) {
    _mgl->unlock();
  }
  if (_parent) {
    _parent.reset();
  }
  if (_sess && _lockResult == mgl::LockRes::LOCKRES_OK) {
    _sess->getCtx()->removeLock(this);
  }
}

mgl::LockMode ILock::getMode() const {
  if (_mgl == nullptr) {
    return mgl::LockMode::LOCK_NONE;
  }
  return _mgl->getMode();
}

mgl::LockRes ILock::getLockResult() const {
  return _lockResult;
}

uint32_t ILock::getStoreId() const {
  return 0;
}

uint32_t ILock::getChunkId() const {
  return 0;
}

std::string ILock::getKey() const {
  return "";
}

mgl::LockMode ILock::getParentMode(mgl::LockMode mode) {
  mgl::LockMode parentMode = mgl::LockMode::LOCK_NONE;
  switch (mode) {
    case mgl::LockMode::LOCK_IS:
    case mgl::LockMode::LOCK_S:
      parentMode = mgl::LockMode::LOCK_IS;
      break;
    case mgl::LockMode::LOCK_IX:
    case mgl::LockMode::LOCK_X:
      parentMode = mgl::LockMode::LOCK_IX;
      break;
    default:
      INVARIANT_D(0);
  }
  return parentMode;
}


StoresLock::StoresLock(mgl::LockMode mode,
                       Session* sess,
                       mgl::MGLockMgr* mgr,
                       uint64_t lockTimeoutMs)
  : ILock(nullptr, new mgl::MGLock(mgr), sess) {
  _lockResult = _mgl->lock(_target, mode, lockTimeoutMs);
}

Expected<std::unique_ptr<StoreLock>> StoreLock::AquireStoreLock(
  uint32_t storeId,
  mgl::LockMode mode,
  Session* sess,
  mgl::MGLockMgr* mgr,
  uint64_t lockTimeoutMs) {
  auto lock =
    std::make_unique<StoreLock>(storeId, mode, sess, mgr, lockTimeoutMs);
  if (lock->getLockResult() == mgl::LockRes::LOCKRES_OK) {
    return lock;
  } else if (lock->getLockResult() == mgl::LockRes::LOCKRES_TIMEOUT) {
    return {ErrorCodes::ERR_LOCK_TIMEOUT, "Lock wait timeout"};
  } else {
    INVARIANT_D(0);
    return {ErrorCodes::ERR_UNKNOWN, "unknown error"};
  }
}

StoreLock::StoreLock(uint32_t storeId,
                     mgl::LockMode mode,
                     Session* sess,
                     mgl::MGLockMgr* mgr,
                     uint64_t lockTimeoutMs)
  // NOTE(takenliu) : all request need get StoresLock, its a big cpu waste.
  //     then, you should not use StoresLock, you should process with every
  //     StoreLock.
  // :ILock(new StoresLock(getParentMode(mode), nullptr, mgr),
  : ILock(NULL, new mgl::MGLock(mgr), sess), _storeId(storeId) {
  // NOTE(takenliu): std::to_string() has performance issue in multi thread,
  //     because it will use std::locale(), so use snprintf instead.
  std::string target = "store_" + uitos(storeId);
  if (_sess) {
    _sess->getCtx()->setWaitLock(storeId, 0, "", mode);
  }
  _lockResult = _mgl->lock(target, mode, lockTimeoutMs);
  if (_sess) {
    _sess->getCtx()->setWaitLock(0, 0, "", mgl::LockMode::LOCK_NONE);
    if (_lockResult == mgl::LockRes::LOCKRES_OK) {
      _sess->getCtx()->addLock(this);
    }
  }
}

uint32_t StoreLock::getStoreId() const {
  return _storeId;
}

ChunkLock::ChunkLock(uint32_t storeId,
                     uint32_t chunkId,
                     mgl::LockMode mode,
                     Session* sess,
                     mgl::MGLockMgr* mgr,
                     uint64_t lockTimeoutMs)
  : ILock(
      new StoreLock(storeId, getParentMode(mode), nullptr, mgr, lockTimeoutMs),
      new mgl::MGLock(mgr),
      sess),
    _chunkId(chunkId) {
  // TODO(vinchen): chunklock only cluster_enabled?
  // if (!_sess || !_sess->getServerEntry() ||
  // _sess->getServerEntry()->isClusterEnabled()) {
  if (_parent->getLockResult() != mgl::LockRes::LOCKRES_OK) {
    _lockResult = _parent->getLockResult();
  } else {
    // NOTE(takenliu): std::to_string() has performance issue in multi thread,
    //     because it will use std::locale(), so use snprintf instead.
    std::string target = "chunk_" + uitos(chunkId);
    if (_sess) {
      _sess->getCtx()->setWaitLock(storeId, chunkId, "", mode);
    }
    _lockResult = _mgl->lock(target, mode, lockTimeoutMs);
    if (_sess) {
      _sess->getCtx()->setWaitLock(0, 0, "", mgl::LockMode::LOCK_NONE);
      if (_lockResult == mgl::LockRes::LOCKRES_OK) {
        _sess->getCtx()->addLock(this);
      }
    }
  }
}

Expected<std::unique_ptr<ChunkLock>> ChunkLock::AquireChunkLock(
  uint32_t storeId,
  uint32_t chunkId,
  mgl::LockMode mode,
  Session* sess,
  mgl::MGLockMgr* mgr,
  uint64_t lockTimeoutMs) {
  auto lock = std::make_unique<ChunkLock>(
    storeId, chunkId, mode, sess, mgr, lockTimeoutMs);
  if (lock->getLockResult() == mgl::LockRes::LOCKRES_OK) {
    return lock;
  } else if (lock->getLockResult() == mgl::LockRes::LOCKRES_TIMEOUT) {
    return {ErrorCodes::ERR_LOCK_TIMEOUT, "Lock wait timeout"};
  } else {
    INVARIANT_D(0);
    return {ErrorCodes::ERR_UNKNOWN, "unknown error"};
  }
}

uint32_t ChunkLock::getStoreId() const {
  return _parent->getStoreId();
}

uint32_t ChunkLock::getChunkId() const {
  return _chunkId;
}

Expected<std::unique_ptr<KeyLock>> KeyLock::AquireKeyLock(
  uint32_t storeId,
  uint32_t chunkId,
  const std::string& key,
  mgl::LockMode mode,
  Session* sess,
  mgl::MGLockMgr* mgr,
  uint64_t lockTimeoutMs) {
  if (sess->getCtx()->isLockedByMe(key, mode)) {
    return std::unique_ptr<KeyLock>(nullptr);
  } else {
    auto lock = std::make_unique<KeyLock>(
      storeId, chunkId, key, mode, sess, mgr, lockTimeoutMs);
    if (lock->getLockResult() == mgl::LockRes::LOCKRES_OK) {
      return lock;
    } else if (lock->getLockResult() == mgl::LockRes::LOCKRES_TIMEOUT) {
      return {ErrorCodes::ERR_LOCK_TIMEOUT, "Lock wait timeout"};
    } else {
      INVARIANT_D(0);
      return {ErrorCodes::ERR_UNKNOWN, "unknown error"};
    }
  }
}

KeyLock::KeyLock(uint32_t storeId,
                 uint32_t chunkId,
                 const std::string& key,
                 mgl::LockMode mode,
                 Session* sess,
                 mgl::MGLockMgr* mgr,
                 uint64_t lockTimeoutMs)
  // :ILock(new StoreLock(storeId, getParentMode(mode), nullptr, mgr,
  // lockTimeoutMs),
  : ILock(new ChunkLock(
            storeId, chunkId, getParentMode(mode), nullptr, mgr, lockTimeoutMs),
          new mgl::MGLock(mgr),
          sess),
    _key(key) {
  if (_parent->getLockResult() != mgl::LockRes::LOCKRES_OK) {
    _lockResult = _parent->getLockResult();
  } else {
    std::string target = "key_" + key;
    if (_sess) {
      _sess->getCtx()->setWaitLock(storeId, chunkId, key, mode);
    }
    _lockResult = _mgl->lock(target, mode, lockTimeoutMs);
    if (_sess) {
      _sess->getCtx()->setWaitLock(0, 0, "", mgl::LockMode::LOCK_NONE);
      if (_lockResult == mgl::LockRes::LOCKRES_OK) {
        _sess->getCtx()->addLock(this);
        _sess->getCtx()->setKeylock(key, mode);
      }
    }
  }
}

KeyLock::~KeyLock() {
  if (_sess && _lockResult == mgl::LockRes::LOCKRES_OK) {
    _sess->getCtx()->unsetKeylock(_key);
  }
}

uint32_t KeyLock::getStoreId() const {
  return _parent->getStoreId();
}

uint32_t KeyLock::getChunkId() const {
  return _parent->getChunkId();
}

std::string KeyLock::getKey() const {
  return _key;
}
}  // namespace tendisplus
