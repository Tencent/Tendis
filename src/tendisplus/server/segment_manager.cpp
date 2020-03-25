#include <memory>
#include <utility>
#include <map>
#include "tendisplus/server/segment_manager.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/server/server_entry.h"

namespace tendisplus {

SegmentMgr::SegmentMgr(const std::string& name)
        :_name(name) {
}

SegmentMgrFnvHash64::SegmentMgrFnvHash64(
            const std::vector<std::shared_ptr<KVStore>>& ins,
            const size_t chunkSize)
        :SegmentMgr("fnv_hash_64"),
         _instances(ins),
         _chunkSize(chunkSize) {
}

Expected<DbWithLock> SegmentMgrFnvHash64::getDbWithKeyLock(Session *sess,
                    const std::string& key, mgl::LockMode mode) {
    uint32_t hash = uint32_t(redis_port::keyHashSlot(key.c_str(), key.size()));
    INVARIANT(hash < _chunkSize);
    uint32_t chunkId = hash % _chunkSize;
    uint32_t segId = chunkId % _instances.size();

    // a duration of 49 days.
    uint64_t lockTimeoutMs = std::numeric_limits<uint32_t>::max();
    if (sess && sess->getServerEntry()) {
        lockTimeoutMs = (uint64_t)sess->getServerEntry()->getParams()->lockWaitTimeOut * 1000;
    }

    if (!_instances[segId]->isOpen()) {
        _instances[segId]->stat.destroyedErrorCount.fetch_add(1,
            std::memory_order_relaxed);

        std::stringstream ss;
        ss << "store id " << segId << " is not opened";
        return{ ErrorCodes::ERR_INTERNAL, ss.str() };
    }

    if (_instances[segId]->isPaused()) {
        _instances[segId]->stat.pausedErrorCount.fetch_add(1,
            std::memory_order_relaxed);

        std::stringstream ss;
        ss << "store id " << segId << " is paused";
        return{ ErrorCodes::ERR_INTERNAL, ss.str() };
    }

    if (sess && sess->getCtx() &&
        _instances[segId]->getMode() == KVStore::StoreMode::REPLICATE_ONLY) {
        sess->getCtx()->setReplOnly(true);
    }

    if (mode != mgl::LockMode::LOCK_NONE) {
        auto elk = KeyLock::AquireKeyLock(segId, key, mode, sess,
            (sess && sess->getServerEntry()) ? sess->getServerEntry()->getMGLockMgr() : nullptr, lockTimeoutMs);
        if (!elk.ok()) {
            return elk.status();
        }

        return DbWithLock{
                segId, chunkId, _instances[segId], nullptr, std::move(elk.value())
                };
    } else {
        return DbWithLock{
                segId, chunkId, _instances[segId], nullptr, nullptr
                };
    }
}

Expected<DbWithLock> SegmentMgrFnvHash64::getDbHasLocked(Session *sess, const std::string& key) {
    uint32_t hash = uint32_t(redis_port::keyHashSlot(key.c_str(), key.size()));
    INVARIANT(hash < _chunkSize);
    uint32_t chunkId = hash % _chunkSize;
    uint32_t segId = chunkId % _instances.size();

    if (!_instances[segId]->isOpen()) {
        _instances[segId]->stat.destroyedErrorCount.fetch_add(1,
                                                              std::memory_order_relaxed);

        std::stringstream ss;
        ss << "store id " << segId << " is not opened";
        return{ ErrorCodes::ERR_INTERNAL, ss.str() };
    }

    if (_instances[segId]->isPaused()) {
        _instances[segId]->stat.pausedErrorCount.fetch_add(1,
                                                           std::memory_order_relaxed);

        std::stringstream ss;
        ss << "store id " << segId << " is paused";
        return{ ErrorCodes::ERR_INTERNAL, ss.str() };
    }

    if (sess && sess->getCtx() &&
        _instances[segId]->getMode() == KVStore::StoreMode::REPLICATE_ONLY) {
        sess->getCtx()->setReplOnly(true);
    }

    return DbWithLock {
        segId, chunkId, _instances[segId], nullptr, nullptr
    };
}

Expected<std::list<std::unique_ptr<KeyLock>>> SegmentMgrFnvHash64::getAllKeysLocked(Session* sess,
        const std::vector<std::string>& args,
        const std::vector<int>& index,
        mgl::LockMode mode) {
    std::list<std::unique_ptr<KeyLock>> locklist;

    if (mode == mgl::LockMode::LOCK_NONE) {
#ifdef _WIN32
        return std::move(locklist);
#else
        return locklist;
#endif
    }
    // a duration of 49 days.
    uint64_t lockTimeoutMs = std::numeric_limits<uint32_t>::max();
    if (sess && sess->getServerEntry()) {
        lockTimeoutMs = (uint64_t)sess->getServerEntry()->getParams()->lockWaitTimeOut * 1000;
    }
    std::map<uint32_t, std::vector<std::string>> segList;
    for (auto iter = index.begin(); iter != index.end(); iter++) {
        auto key = args[*iter];
        uint32_t hash = redis_port::keyHashSlot(key.c_str(), key.size());
        INVARIANT(hash < _chunkSize);
        uint32_t chunkId = hash % _chunkSize;
        uint32_t segId = chunkId % _instances.size();
        segList[segId].push_back(std::move(key));
    }

    for (auto& element : segList) {
        uint32_t segId = element.first;
        auto keysvec = element.second;
        std::sort(std::begin(keysvec), std::end(keysvec),
                [](const std::string& a, const std::string& b) { return a < b; });
        for (auto keyIter = keysvec.begin(); keyIter != keysvec.end(); keyIter++) {
            const std::string& key = *keyIter;
            auto elk = KeyLock::AquireKeyLock(segId, key, mode, sess,
                (sess && sess->getServerEntry()) ? sess->getServerEntry()->getMGLockMgr() : nullptr, lockTimeoutMs);
            if (!elk.ok()) {
                return elk.status();
            }
            locklist.emplace_back(std::move(elk.value()));
        }
    }

#ifdef _WIN32
    return std::move(locklist);
#else
    return locklist;
#endif
}

Expected<DbWithLock> SegmentMgrFnvHash64::getDb(Session *sess, uint32_t insId,
                                            mgl::LockMode mode,
                                            bool canOpenStoreNoneDB) {
    if (insId >= _instances.size()) {
        return {ErrorCodes::ERR_INTERNAL, "invalid instance id"};
    }

    if (!canOpenStoreNoneDB) {
        if (!_instances[insId]->isOpen()) {
            _instances[insId]->stat.destroyedErrorCount.fetch_add(1,
                        std::memory_order_relaxed);

            std::stringstream ss;
            ss << "store id " << insId << " is not opened";
            return{ ErrorCodes::ERR_INTERNAL, ss.str() };
        }
    }

    // a duration of 49 days.
    uint64_t lockTimeoutMs = std::numeric_limits<uint32_t>::max();
    if (sess && sess->getServerEntry()) {
        lockTimeoutMs = (uint64_t)sess->getServerEntry()->getParams()->lockWaitTimeOut * 1000;
    }

    std::unique_ptr<StoreLock> lk = nullptr;
    if (mode != mgl::LockMode::LOCK_NONE) {
        auto elk = StoreLock::AquireStoreLock(insId, mode, sess,
            (sess && sess->getServerEntry()) ? sess->getServerEntry()->getMGLockMgr() : nullptr, lockTimeoutMs);
        if (!elk.ok()) {
            return elk.status();
        }
        lk = std::move(elk.value());
    }

    if (sess && sess->getCtx() &&
        _instances[insId]->getMode() == KVStore::StoreMode::REPLICATE_ONLY) {
        sess->getCtx()->setReplOnly(true);
    }
    return DbWithLock{insId, 0, _instances[insId], std::move(lk), nullptr};
}

}  // namespace tendisplus
