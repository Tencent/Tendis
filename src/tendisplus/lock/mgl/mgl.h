#ifndef SRC_TENDISPLUS_LOCK_MGL_MGL_H__
#define SRC_TENDISPLUS_LOCK_MGL_MGL_H__

#include <atomic>
#include <string>
#include <list>
#include <mutex>  // NOLINT
#include <condition_variable>  // NOLINT

#include "tendisplus/lock/mgl/lock_defines.h"
#include "tendisplus/lock/mgl/mgl_mgr.h"

namespace tendisplus {

namespace mgl {

class LockSchedCtx;

// multi granularity lock
// each lock can lock only one target, use multiple MGLocks
// if you want to lock different targets.
class MGLock {
 public:
    explicit MGLock(MGLockMgr* mgr);
    MGLock(const MGLock&) = delete;
    MGLock(MGLock&&) = delete;
    ~MGLock();
    LockRes lock(const std::string& target, LockMode mode,
                 uint64_t timeoutMs);
    void unlock();
    uint64_t getHash() const { return _targetHash; }
    LockMode getMode() const { return _mode; }
    LockRes getStatus() const;
    const std::string& getTarget() const { return _target; }
    std::string toString() const;
    uint64_t getThreadId() const { return _threadId; }

 private:
    friend class LockSchedCtx;
    void setLockResult(LockRes res, std::list<MGLock*>::iterator iter);
    void releaseLockResult();
    std::list<MGLock*>::iterator getLockIter() const;
    void notify();
    bool waitLock(uint64_t timeoutMs);

    const uint64_t _id;
    std::string _target;
    uint64_t _targetHash;
    LockMode _mode;

    // wrote by MGLockMgr
    mutable std::mutex _mutex;
    std::condition_variable _cv;
    LockRes _res;
    std::list<MGLock*>::iterator _resIter;
    MGLockMgr* _lockMgr;
    uint64_t _threadId;

    static std::atomic<uint64_t> _idGen;
    static std::list<MGLock*> _dummyList;
};

}  // namespace mgl

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_LOCK_MGL_MGL_H__
