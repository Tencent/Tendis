#ifndef SRC_TENDISPLUS_LOCK_LOCK_H__
#define SRC_TENDISPLUS_LOCK_LOCK_H__

#include <string>
#include <utility>
#include <memory>

#include "tendisplus/lock/mgl/mgl.h"
#include "tendisplus/server/session.h"
#include "tendisplus/network/session_ctx.h"

namespace tendisplus {

class Session;

class StoresLock {
 public:
    explicit StoresLock(mgl::LockMode mode);
    ~StoresLock();

 private:
    static const char _target[];
    std::unique_ptr<mgl::MGLock> _mgl;
};

class StoreLock {
 public:
    StoreLock(uint32_t storeId, mgl::LockMode mode,
              Session* sess);
    uint32_t getStoreId() const;
    mgl::LockMode getMode() const;
    ~StoreLock();

 private:
    std::unique_ptr<StoresLock> _parent;
    std::unique_ptr<mgl::MGLock> _mgl;
    uint32_t _storeId;
    mgl::LockMode _mode;
    // not owned
    Session* _sess;
};

}  // namespace tendisplus
#endif  //  SRC_TENDISPLUS_LOCK_LOCK_H__
