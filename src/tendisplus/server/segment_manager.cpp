#include <memory>
#include <utility>
#include "tendisplus/server/segment_manager.h"

namespace tendisplus {

SegmentMgr::SegmentMgr(const std::string& name)
        :_name(name) {
}

SegmentMgrFnvHash64::SegmentMgrFnvHash64(
            const std::vector<std::shared_ptr<KVStore>>& ins)
        :SegmentMgr("fnv_hash_64"),
         _instances(ins) {
}

Expected<DbWithLock> SegmentMgrFnvHash64::getDbWithKeyLock(Session *sess,
                    const std::string& key, mgl::LockMode mode) {
    uint32_t hash = static_cast<uint32_t>(FNV_64_INIT);
    for (auto v : key) {
        uint32_t val = static_cast<uint32_t>(v);
        hash ^= val;
        hash *= static_cast<uint32_t>(FNV_64_PRIME);
    }
    uint32_t chunkId = hash % HASH_SPACE;
    uint32_t segId = chunkId % _instances.size();

    if (!_instances[segId]->isOpen()) {
        std::stringstream ss;
        ss << "store id " << segId << " is not opened";
        return{ ErrorCodes::ERR_INTERNAL, ss.str() };
    }

    std::unique_ptr<KeyLock> lk = nullptr;
    if (mode != mgl::LockMode::LOCK_NONE) {
        lk = std::make_unique<KeyLock>(segId, key, mode, sess);
    }
    return DbWithLock{
            segId, chunkId, _instances[segId], nullptr, std::move(lk)
    };
}

Expected<DbWithLock> SegmentMgrFnvHash64::getDb(Session *sess, uint32_t insId,
                                            mgl::LockMode mode,
                                            bool canOpenStoreNoneDB) {
    if (insId >= KVStore::INSTANCE_NUM) {
        return {ErrorCodes::ERR_INTERNAL, "invalid instance id"};
    }

    if (!canOpenStoreNoneDB) {
        if (!_instances[insId]->isOpen()) {
            std::stringstream ss;
            ss << "store id " << insId << " is not opened";
            return{ ErrorCodes::ERR_INTERNAL, ss.str() };
        }
    }

    std::unique_ptr<StoreLock> lk = nullptr;
    if (mode != mgl::LockMode::LOCK_NONE) {
        lk = std::make_unique<StoreLock>(insId, mode, sess);
    }
    return DbWithLock{insId, 0, _instances[insId], std::move(lk), nullptr};
}

}  // namespace tendisplus
