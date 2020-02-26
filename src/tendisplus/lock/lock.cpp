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


StoresLock::StoresLock(mgl::LockMode mode, Session* sess, mgl::MGLockMgr* mgr,
                        uint64_t lockTimeoutMs)
        :ILock(nullptr, new mgl::MGLock(mgr), sess) {
    auto lockResult = _mgl->lock(_target, mode, lockTimeoutMs);
    INVARIANT(lockResult == mgl::LockRes::LOCKRES_OK);
}

StoreLock::StoreLock(uint32_t storeId, mgl::LockMode mode,
                     Session *sess, mgl::MGLockMgr* mgr,
                     uint64_t lockTimeoutMs)
        // NOTE(takenliu) : all request need get StoresLock, its a big cpu waste.
        //     then, you should not use StoresLock, you should process with every StoreLock.
        //:ILock(new StoresLock(getParentMode(mode), nullptr, mgr),
        :ILock(NULL,
                                new mgl::MGLock(mgr), sess),
         _storeId(storeId) {
    std::string target = "store_" + std::to_string(storeId);
    if (_sess) {
        _sess->getCtx()->setWaitLock(storeId, "", mode);
    }
    auto lockResult = _mgl->lock(target, mode, lockTimeoutMs);
    INVARIANT(lockResult == mgl::LockRes::LOCKRES_OK);
    if (_sess) {
        _sess->getCtx()->setWaitLock(0, "", mgl::LockMode::LOCK_NONE);
        _sess->getCtx()->addLock(this);
    }
}

uint32_t StoreLock::getStoreId() const {
    return _storeId;
}

std::unique_ptr<KeyLock> KeyLock::AquireKeyLock(uint32_t storeId,
        const std::string &key, mgl::LockMode mode,
        Session *sess, mgl::MGLockMgr* mgr, uint64_t lockTimeoutMs) {
    if (sess->getCtx()->isLockedByMe(key, mode)) {
        return std::unique_ptr<KeyLock>(nullptr);
    } else {
        return std::make_unique<KeyLock>(storeId, key, mode, sess, mgr, lockTimeoutMs);
    }
}

KeyLock::KeyLock(uint32_t storeId, const std::string& key,
    mgl::LockMode mode, Session *sess, mgl::MGLockMgr* mgr, uint64_t lockTimeoutMs)
        :ILock(new StoreLock(storeId, getParentMode(mode), nullptr, mgr, lockTimeoutMs),
                            new mgl::MGLock(mgr), sess),
         _key(key) {
    std::string target = "key_" + key;
    if (_sess) {
        _sess->getCtx()->setWaitLock(storeId, key, mode);
    }
    auto lockResult = _mgl->lock(target, mode, lockTimeoutMs);
    INVARIANT(lockResult == mgl::LockRes::LOCKRES_OK);
    if (_sess) {
        _sess->getCtx()->setWaitLock(0, "", mgl::LockMode::LOCK_NONE);
        _sess->getCtx()->addLock(this);
        _sess->getCtx()->setKeylock(key, mode);
    }
}

KeyLock::~KeyLock() {
    _sess->getCtx()->unsetKeylock(_key);
}

uint32_t KeyLock::getStoreId() const {
    return _parent->getStoreId();
}

std::string KeyLock::getKey() const {
    return _key;
}
}  // namespace tendisplus
