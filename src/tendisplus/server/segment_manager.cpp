#include <memory>
#include <utility>
#include <map>
#include "tendisplus/server/segment_manager.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/redis_port.h"

namespace tendisplus {

SegmentMgr::SegmentMgr(const std::string& name)
        :_name(name) {
}

Expected<std::vector<DbWithLock>> SegmentMgr::getDbWithKeyLock(Session* sess,
        const std::vector<std::string>& args,
        const std::vector<int>& index,
        mgl::LockMode mode) {
    std::vector<std::pair<std::string, mgl::LockMode>> inputArgs;
    for (auto iter = index.begin(); iter != index.end(); iter++) {
        inputArgs.push_back(std::make_pair(args[*iter], mode));
    }

    return getDbWithKeyFineLocked(sess, inputArgs.begin(), inputArgs.end());
}

/*Expected<DbWithLock> SegmentMgr::getDbWithKeyLock(tendisplus::Session* sess, const std::string &key,
        tendisplus::mgl::LockMode mode) {
    std::vector<std::pair<std::string, mgl::LockMode>> inputArgs;
    auto tmpPair = std::make_pair(key, mode);
    inputArgs.push_back(std::move(tmpPair));

    auto expRet = getDbWithKeyFineLocked(sess, inputArgs.begin(), inputArgs.end());
    if (!expRet.ok()) {
        return expRet.status();
    }
    std::vector<DbWithLock>& refVec = expRet.value();
    INVARIANT(refVec.size() != 0);
    return std::move(refVec[0]);
}*/

Expected<std::vector<DbWithLock>> SegmentMgr::getDbWithKeyLock(Session* sess,
        std::vector<std::pair<std::string, mgl::LockMode>>::const_iterator start,
        std::vector<std::pair<std::string, mgl::LockMode>>::const_iterator end) {
    std::vector<std::pair<std::string, mgl::LockMode>> inputArgs;
    return getDbWithKeyFineLocked(sess, start, end);
}

Expected<std::vector<DbWithLock>> SegmentMgr::getDbWithKeyLock(Session* sess,
        std::vector<std::pair<std::string, mgl::LockMode>>& inputArgs) {
    return getDbWithKeyFineLocked(sess, inputArgs.begin(), inputArgs.end());
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

    std::unique_ptr<KeyLock> lk = nullptr;
    if (mode != mgl::LockMode::LOCK_NONE) {
        lk = KeyLock::AquireKeyLock(segId, key, mode, sess);
    }
    return DbWithLock{
            segId, chunkId, _instances[segId], nullptr, std::move(lk)
    };
}

Expected<std::vector<DbWithLock>> SegmentMgrFnvHash64::getDbWithKeyFineLocked(Session* sess,
        std::vector<std::pair<std::string, mgl::LockMode>>::const_iterator start,
        std::vector<std::pair<std::string, mgl::LockMode>>::const_iterator end) {
    std::map<std::string, std::tuple<uint32_t, uint32_t, mgl::LockMode>> keyList;
    std::map<uint32_t, std::vector<const std::string*>> segList;
    for (auto iter = start; iter != end; iter++) {
        auto* key = &(*iter).first;
        uint32_t hash = redis_port::keyHashSlot(key->c_str(), key->size());
        INVARIANT(hash < _chunkSize);
        uint32_t chunkId = hash % _chunkSize;
        uint32_t segId = chunkId % _instances.size();
        keyList[*key] = std::make_tuple(chunkId, segId, (*iter).second);
        segList[segId].push_back(key);
    }

    std::map<std::string, std::unique_ptr<KeyLock>> lockList;
    for (auto& element : segList) {
        uint32_t segId = element.first;
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
        auto keysvec = element.second;
        std::sort(std::begin(keysvec), std::end(keysvec), [](const std::string* a, const std::string* b) { return *a < *b; });
        for (auto keyIter = keysvec.begin(); keyIter != keysvec.end(); keyIter++) {
            const std::string& key = **keyIter;
            auto mode = std::get<2>(keyList[key]);
            if (mode != mgl::LockMode::LOCK_NONE) {
                auto lk = KeyLock::AquireKeyLock(segId, key, mode, sess);
                lockList[key] = std::move(lk);
            } else {
                lockList[key] = nullptr;
            }
        }
    }

    std::vector<DbWithLock> ret;
    for (auto iter = start; iter != end; iter++) {
        const auto& key = (*iter).first;
        const auto& attri = keyList[key];
        uint32_t chunkId = std::get<0>(attri);
        uint32_t segId = std::get<1>(attri);
        ret.push_back(std::move(DbWithLock{
            segId, chunkId, _instances[segId], nullptr, std::move(lockList[key])
        }));
    }

    return ret;
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

    std::unique_ptr<StoreLock> lk = nullptr;
    if (mode != mgl::LockMode::LOCK_NONE) {
        lk = std::make_unique<StoreLock>(insId, mode, sess);
    }
    return DbWithLock{insId, 0, _instances[insId], std::move(lk), nullptr};
}

}  // namespace tendisplus
