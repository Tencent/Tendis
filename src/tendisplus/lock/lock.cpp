#include <memory>
#include <limits>
#include "tendisplus/lock/lock.h"
#include "tendisplus/utils/invariant.h"

namespace tendisplus {

const char StoresLock::_target[] = "stores";

ILock::ILock(ILock* parent, mgl::MGLock* lk, Session* sess)
    :_parent(parent),
     _mgl(lk),
     _sess(sess) {
}

ILock::~ILock() {
    if (_mgl) {
        _mgl->unlock();
    }
    if (_parent) {
        _parent.reset();
    }
    if (_sess) {
        _sess->getCtx()->removeLock(this);
    }
}

mgl::LockMode ILock::getMode() const {
    if (_mgl == nullptr) { 
        return mgl::LockMode::LOCK_NONE;
    }
    return _mgl->getMode();
}

uint32_t ILock::getStoreId() const {
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
            INVARIANT(0);
    }
    return parentMode;
}

StoresLock::StoresLock(mgl::LockMode mode, Session* sess)
        :ILock(nullptr, new mgl::MGLock, sess) {
    // a duration of 49 days. If lock still not acquired, fail it
    uint64_t timeoutMs = std::numeric_limits<uint32_t>::max();
    auto lockResult = _mgl->lock(_target, mode, timeoutMs);
    INVARIANT(lockResult == mgl::LockRes::LOCKRES_OK);
}

StoreLock::StoreLock(uint32_t storeId, mgl::LockMode mode,
                     Session *sess)
        :ILock(new StoresLock(getParentMode(mode), nullptr), new mgl::MGLock, sess),
         _storeId(storeId) {
    std::string target = "store_" + std::to_string(storeId);
    // a duration of 49 days. If lock still not acquired, fail it
    uint64_t timeoutMs = std::numeric_limits<uint32_t>::max();
    if (_sess) {
        _sess->getCtx()->setWaitLock(storeId, "", mode);
    }
    auto lockResult = _mgl->lock(target, mode, timeoutMs);
    INVARIANT(lockResult == mgl::LockRes::LOCKRES_OK);
    if (_sess) {
        _sess->getCtx()->setWaitLock(0, "", mgl::LockMode::LOCK_NONE);
        _sess->getCtx()->addLock(this);
    }
}

uint32_t StoreLock::getStoreId() const {
    return _storeId;
}

KeyLock::KeyLock(uint32_t storeId, const std::string& key,
                mgl::LockMode mode, Session *sess)
        :ILock(new StoreLock(storeId, getParentMode(mode), nullptr), new mgl::MGLock, sess),
         _key(key) {
    std::string target = "key_" + key;
    uint64_t timeoutMs = std::numeric_limits<uint32_t>::max();
    if (_sess) {
        _sess->getCtx()->setWaitLock(storeId, key, mode);
    }
    auto lockResult = _mgl->lock(target, mode, timeoutMs);
    INVARIANT(lockResult == mgl::LockRes::LOCKRES_OK);
    if (_sess) {
        _sess->getCtx()->setWaitLock(0, "", mgl::LockMode::LOCK_NONE);
        _sess->getCtx()->addLock(this);
    }
}

uint32_t KeyLock::getStoreId() const {
    return _parent->getStoreId();
}

std::string KeyLock::getKey() const {
    return _key;
}
}  // namespace tendisplus
