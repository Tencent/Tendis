#include "tendisplus/lock/lock.h"
#include "tendisplus/utils/invariant.h"

namespace tendisplus {

const std::string StoresLock::_target = "stores";

StoresLock::StoresLock(mgl::LockMode mode) {
    _mgl = std::make_unique<mgl::MGLock>();
    // a duration of 49 days. If lock still not acquired, fail it
    uint64_t timeoutMs = std::numeric_limits<uint32_t>::max();
    auto lockResult = _mgl->lock(_target, mode, timeoutMs);
    INVARIANT(lockResult == mgl::LockRes::LOCKRES_OK);
}

StoresLock::~StoresLock() {
    _mgl->unlock();
}

StoreLock::StoreLock(uint32_t storeId, mgl::LockMode mode) {
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
    _parent = std::make_unique<StoresLock>(parentMode);
    _mgl = std::make_unique<mgl::MGLock>();
    std::string target = "store_" + std::to_string(storeId);
    // a duration of 49 days. If lock still not acquired, fail it
    uint64_t timeoutMs = std::numeric_limits<uint32_t>::max();
    auto lockResult = _mgl->lock(target, mode, timeoutMs);
    INVARIANT(lockResult == mgl::LockRes::LOCKRES_OK);
}

StoreLock::~StoreLock() {
    _mgl->unlock();
    _parent.reset();
}
}  // namespace tendisplus
