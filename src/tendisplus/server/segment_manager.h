#ifndef SRC_TENDISPLUS_SERVER_SEGMENT_MANAGER_H_
#define SRC_TENDISPLUS_SERVER_SEGMENT_MANAGER_H_

#include <string>
#include <vector>
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
    std::unique_ptr<StoreLock> lk;
};

class SegmentMgr {
 public:
    explicit SegmentMgr(const std::string&);
    virtual ~SegmentMgr() = default;
    SegmentMgr(const SegmentMgr&) = delete;
    SegmentMgr(SegmentMgr&&) = delete;
    virtual Expected<DbWithLock> getDb(Session* sess, const std::string& key, mgl::LockMode mode) = 0;
    virtual Expected<DbWithLock> getDb(Session* sess, uint32_t insId, mgl::LockMode mode) = 0;

 private:
    const std::string _name;
};

class SegmentMgrFnvHash64: public SegmentMgr {
 public:
    explicit SegmentMgrFnvHash64(const std::vector<PStore>& ins);
    virtual ~SegmentMgrFnvHash64() = default;
    SegmentMgrFnvHash64(const SegmentMgrFnvHash64&) = delete;
    SegmentMgrFnvHash64(SegmentMgrFnvHash64&&) = delete;
    Expected<DbWithLock> getDb(Session* sess, const std::string& key, mgl::LockMode mode) final;
    Expected<DbWithLock> getDb(Session* sess, uint32_t insId, mgl::LockMode mode) final;

 private:
    std::vector<PStore> _instances;
    static constexpr size_t HASH_SPACE = 420000U;
    static constexpr uint64_t FNV_64_INIT = 0xcbf29ce484222325ULL;
    static constexpr uint64_t FNV_64_PRIME = 0x100000001b3ULL;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_SERVER_SEGMENT_MANAGER_H_
