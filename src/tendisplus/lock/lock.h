#ifndef SRC_TENDISPLUS_LOCK_LOCK_H__
#define SRC_TENDISPLUS_LOCK_LOCK_H__

#include <string>
#include <utility>

#include "tendisplus/lock/mgl/mgl.h"

namespace tendisplus {

class StoresLock {
 public:
    StoresLock(mgl::LockMode mode);
    ~StoresLock();

 private:
    static const std::string _target;
    std::unique_ptr<mgl::MGLock> _mgl;
};

class StoreLock {
 public:
    StoreLock(uint32_t storeId, mgl::LockMode mode);
    ~StoreLock();

 private:
    std::unique_ptr<StoresLock> _parent;
    std::unique_ptr<mgl::MGLock> _mgl;
};

}  // namespace tendisplus
#endif  //  SRC_TENDISPLUS_LOCK_LOCK_H__
