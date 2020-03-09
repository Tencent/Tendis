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
    std::unique_ptr<ChunkLock> chunkLock;
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
                                bool canOpenStoreNoneDB = false,
                                uint64_t lock_wait_timeout = -1) = 0;
    virtual Expected<DbWithLock> getDbHasLocked(Session* sess, const std::string& key) = 0;
    virtual Expected<std::list<std::unique_ptr<KeyLock>>> getAllKeysLocked(Session* sess,
            const std::vector<std::string>& args,
            const std::vector<int>& index,
            mgl::LockMode mode) = 0;
    virtual size_t getChunkSize() const = 0;
    virtual  uint32_t getStoreid(uint32_t chunkid)  = 0;
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
            bool canOpenStoreNoneDB = false, 
            uint64_t lock_wait_timeout = -1) final;
    Expected<DbWithLock> getDbHasLocked(Session* sess, const std::string& key) final;
    Expected<std::list<std::unique_ptr<KeyLock>>> getAllKeysLocked(Session* sess,
            const std::vector<std::string>& args,
            const std::vector<int>& index,
            mgl::LockMode mode) final;
    size_t getChunkSize() const final { return _chunkSize; }
    uint32_t getStoreid(uint32_t chunkid);
private:
    std::vector<PStore> _instances;

    size_t _chunkSize;
    static constexpr uint64_t FNV_64_INIT = 0xcbf29ce484222325ULL;
    static constexpr uint64_t FNV_64_PRIME = 0x100000001b3ULL;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_SERVER_SEGMENT_MANAGER_H_
