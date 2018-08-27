#include <memory>
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

uint32_t SegmentMgrFnvHash64::calcSegId(const std::string& key) const {
    uint32_t hash = static_cast<uint32_t>(FNV_64_INIT);
    for (auto v : key) {
        uint32_t val = static_cast<uint32_t>(v);
        hash ^= val;
        hash *= static_cast<uint32_t>(FNV_64_PRIME);
    }
    return hash % HASH_SPACE;
}

PStore SegmentMgrFnvHash64::calcInstance(const std::string& key) const {
    uint32_t segId = calcSegId(key);
    return _instances[segId % _instances.size()];
}

PStore SegmentMgrFnvHash64::getInstanceById(uint32_t id) const {
    return _instances[id];
}

std::vector<PStore> SegmentMgrFnvHash64::calcInstances(
        const std::string& beginKey, const std::string& endKey) const {
    return _instances;
}

}  // namespace tendisplus
