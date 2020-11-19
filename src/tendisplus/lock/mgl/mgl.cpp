#include "tendisplus/utils/invariant.h"
#include "tendisplus/lock/mgl/mgl.h"
#include "tendisplus/lock/mgl/mgl_mgr.h"
#include "tendisplus/utils/string.h"

namespace tendisplus {
namespace mgl {

std::atomic<uint64_t> MGLock::_idGen(0);
std::list<MGLock*> MGLock::_dummyList{};

MGLock::MGLock(MGLockMgr* mgr)
  : _id(_idGen.fetch_add(1, std::memory_order_relaxed)),
    _target(""),
    _targetHash(0),
    _mode(LockMode::LOCK_NONE),
    _res(LockRes::LOCKRES_UNINITED),
    _resIter(_dummyList.end()),
    _lockMgr(mgr),
    _threadId(getCurThreadId()) {}

MGLock::~MGLock() {
  INVARIANT_D(_res == LockRes::LOCKRES_UNINITED);
}

void MGLock::releaseLockResult() {
  std::lock_guard<std::mutex> lk(_mutex);
  _res = LockRes::LOCKRES_UNINITED;
  _resIter = _dummyList.end();
}

void MGLock::setLockResult(LockRes res, std::list<MGLock*>::iterator iter) {
  std::lock_guard<std::mutex> lk(_mutex);
  _res = res;
  _resIter = iter;
}

void MGLock::unlock() {
  LockRes status = getStatus();
  if (status != LockRes::LOCKRES_UNINITED) {
    INVARIANT_D(status == LockRes::LOCKRES_OK ||
                status == LockRes::LOCKRES_WAIT);
    if (!_lockMgr) {
      MGLockMgr::getInstance().unlock(this);
    } else {
      _lockMgr->unlock(this);
    }
    status = getStatus();
    INVARIANT_D(status == LockRes::LOCKRES_UNINITED);
  }
}

LockRes MGLock::lock(const std::string& target,
                     LockMode mode,
                     uint64_t timeoutMs) {
  _target = target;
  _mode = mode;
  INVARIANT_D(getStatus() == LockRes::LOCKRES_UNINITED);
  _resIter = _dummyList.end();
  if (_target != "") {
    _targetHash = static_cast<uint64_t>(std::hash<std::string>{}(_target));
  } else {
    _targetHash = 0;
  }
  if (!_lockMgr) {
    MGLockMgr::getInstance().lock(this);
  } else {
    _lockMgr->lock(this);
  }
  if (getStatus() == LockRes::LOCKRES_OK) {
    return LockRes::LOCKRES_OK;
  }
  if (waitLock(timeoutMs)) {
    return LockRes::LOCKRES_OK;
  } else {
    return LockRes::LOCKRES_TIMEOUT;
  }
}

std::list<MGLock*>::iterator MGLock::getLockIter() const {
  return _resIter;
}

void MGLock::notify() {
  _cv.notify_one();
}

bool MGLock::waitLock(uint64_t timeoutMs) {
  std::unique_lock<std::mutex> lk(_mutex);
  return _cv.wait_for(lk, std::chrono::milliseconds(timeoutMs), [this]() {
    return _res == LockRes::LOCKRES_OK;
  });
}

LockRes MGLock::getStatus() const {
  std::lock_guard<std::mutex> lk(_mutex);
  return _res;
}

std::string MGLock::toString() const {
  std::lock_guard<std::mutex> lk(_mutex);
  char buf[256];
  snprintf(buf,
           sizeof(buf),
           "id:%" PRIu64 " target:%s targetHash:%" PRIu64
           " LockMode:%s LockRes:%d threadId:0x%" PRIx64,
           _id,
           _target.c_str(),
           _targetHash,
           lockModeRepr(_mode),
           static_cast<int>(_res),
           _threadId);
  return std::string(buf);
}

}  // namespace mgl
}  // namespace tendisplus
