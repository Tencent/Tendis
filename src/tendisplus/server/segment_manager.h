#ifndef SRC_TENDISPLUS_SERVER_SEGMENT_MANAGER_H_
#define SRC_TENDISPLUS_SERVER_SEGMENT_MANAGER_H_

#include <string>
#include <vector>
#include <memory>
#include "tendisplus/storage/kvstore.h"
#include "tendisplus/server/session.h"
#include "tendisplus/lock/lock.h"

namespace tendisplus {

struct DbWithLock {
    DbWithLock(const DbWithLock&) = delete;
    DbWithLock(DbWithLock&&) = default;
    uint32_t dbId;
    uint32_t chunkId;
    PStore store;
    std::unique_ptr<StoreLock> dbLock;
    std::unique_ptr<KeyLock> keyLock;
};

class SegmentMgr {
 public:
    explicit SegmentMgr(const std::string&);
    virtual ~SegmentMgr() = default;
    SegmentMgr(const SegmentMgr&) = delete;
    SegmentMgr(SegmentMgr&&) = delete;
    virtual Expected<DbWithLock> getDbWithKeyLock(Session* sess,
            const std::string& key, mgl::LockMode keyLockMode) = 0;
    virtual Expected<DbWithLock> getDb(Session* sess, uint32_t insId,
                                mgl::LockMode mode,
                                bool canOpenStoreNoneDB = false) = 0;

 private:
    const std::string _name;
};

class SegmentMgrFnvHash64: public SegmentMgr {
 public:
    SegmentMgrFnvHash64(const std::vector<PStore>& ins, const size_t hashSpace);
    virtual ~SegmentMgrFnvHash64() = default;
    SegmentMgrFnvHash64(const SegmentMgrFnvHash64&) = delete;
    SegmentMgrFnvHash64(SegmentMgrFnvHash64&&) = delete;
    Expected<DbWithLock> getDbWithKeyLock(Session* sess,
            const std::string& key, mgl::LockMode keyLockMode) final;
    Expected<DbWithLock> getDb(Session* sess, uint32_t insId,
                                mgl::LockMode mode,
                                bool canOpenStoreNoneDB = false) final;

 private:
    std::vector<PStore> _instances;
    size_t _chunkSize;
    static constexpr uint64_t FNV_64_INIT = 0xcbf29ce484222325ULL;
    static constexpr uint64_t FNV_64_PRIME = 0x100000001b3ULL;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_SERVER_SEGMENT_MANAGER_H_
