#ifndef SRC_TENDISPLUS_SERVER_SEGMENT_MANAGER_H_
#define SRC_TENDISPLUS_SERVER_SEGMENT_MANAGER_H_

#include <string>
#include <vector>
#include "tendisplus/storage/kvstore.h"

namespace tendisplus {

class SegmentMgr {
 public:
    explicit SegmentMgr(const std::string&);
    virtual ~SegmentMgr() = default;
    SegmentMgr(const SegmentMgr&) = delete;
    SegmentMgr(SegmentMgr&&) = delete;
    virtual uint32_t calcSegId(const std::string&) const = 0;
    virtual PStore calcInstance(const std::string&) const = 0;
    virtual PStore getInstanceById(uint32_t) const = 0;
    // instances intersecting with this range
    // it maybe meaningless useful in a range-based SegmentMgr
    virtual std::vector<PStore> calcInstances(
        const std::string& begin, const std::string& end) const = 0;

 private:
    const std::string _name;
};

class SegmentMgrFnvHash64: public SegmentMgr {
 public:
    explicit SegmentMgrFnvHash64(const std::vector<PStore>& ins);
    virtual ~SegmentMgrFnvHash64() = default;
    SegmentMgrFnvHash64(const SegmentMgrFnvHash64&) = delete;
    SegmentMgrFnvHash64(SegmentMgrFnvHash64&&) = delete;
    uint32_t calcSegId(const std::string&) const final;
    PStore calcInstance(const std::string&) const final;
    PStore getInstanceById(uint32_t) const final;
    std::vector<PStore> calcInstances(
        const std::string& begin, const std::string& end) const final;

 private:
    std::vector<PStore> _instances;
    static constexpr size_t HASH_SPACE = 420000U;
    static constexpr uint64_t FNV_64_INIT = 0xcbf29ce484222325ULL;
    static constexpr uint64_t FNV_64_PRIME = 0x100000001b3ULL;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_SERVER_SEGMENT_MANAGER_H_
