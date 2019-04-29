#include <utility>
#include <string>
#include <algorithm>
#include <list>
#include <vector>

#include "tendisplus/network/session_ctx.h"
#include "tendisplus/utils/invariant.h"

namespace tendisplus {

SessionCtx::SessionCtx()
    :_authed(false),
     _dbId(0),
     _waitlockStore(0),
     _waitlockMode(mgl::LockMode::LOCK_NONE),
     _waitlockKey(""),
     _processPacketStart(0),
     _timestamp(-1),
     _version(-1),
     _extendProtocol(false) {
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

void SessionCtx::addLock(ILock *lock) {
    std::lock_guard<std::mutex> lk(_mutex);
    _locks.push_back(lock);
}

void SessionCtx::removeLock(ILock *lock) {
    std::lock_guard<std::mutex> lk(_mutex);
    for (auto it = _locks.begin(); it != _locks.end(); ++it) {
        if (*it == lock) {
            _locks.erase(it);
            return;
        }
    }
    INVARIANT(0);
}

std::vector<std::string> SessionCtx::getArgsBrief() const {
    std::lock_guard<std::mutex> lk(_mutex);
    return _argsBrief;
}

void SessionCtx::setArgsBrief(const std::vector<std::string>& v) {
    std::lock_guard<std::mutex> lk(_mutex);
    constexpr size_t MAX_SIZE = 8;
    for (size_t i = 0; i < std::min(v.size(), MAX_SIZE); ++i) {
        _argsBrief.push_back(v[i]);
    }
}

void SessionCtx::clearRequestCtx() {
    std::lock_guard<std::mutex> lk(_mutex);
    _argsBrief.clear();
    _timestamp = -1;
    _version = -1;
}

void SessionCtx::setWaitLock(uint32_t storeId, const std::string& key, mgl::LockMode mode) {
    _waitlockStore = storeId;
    _waitlockMode = mode;
    _waitlockKey = key;
}

SLSP SessionCtx::getWaitlock() const {
    return std::tuple<uint32_t, std::string, mgl::LockMode>(
            _waitlockStore, _waitlockKey, _waitlockMode);
}

std::list<SLSP> SessionCtx::getLockStates() const {
    std::lock_guard<std::mutex> lk(_mutex);
    std::list<SLSP> result;
    for (auto& lk : _locks) {
        result.push_back(
            SLSP(lk->getStoreId(), lk->getKey(), lk->getMode()));
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

}  // namespace tendisplus
