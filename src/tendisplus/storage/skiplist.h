#ifndef SRC_TENDISPLUS_STORAGE_SKIPLIST_H_
#define SRC_TENDISPLUS_STORAGE_SKIPLIST_H_

#include <limits>
#include <memory>
#include <string>
#include <vector>
#include <atomic>
#include <utility>
#include "tendisplus/storage/record.h"
#include "tendisplus/storage/kvstore.h"

namespace tendisplus {

class SkipList {
 public:
    SkipList(uint32_t dbId, const std::string& pk,
             const ZSlMetaValue& meta, PStore store);
    Status insert(uint64_t score, const std::string& subkey, Transaction* txn);
    Status remove(uint64_t score, const std::string& subkey, Transaction* txn);
    // Expected<uint32_t rank(const std::string& key, Transaction* txn);
    Status save(Transaction* txn);

 private:
    using PSE = std::unique_ptr<ZSlEleValue>;
    uint8_t randomLevel();
    Status saveNode(uint32_t pointer, const ZSlEleValue& val, Transaction* txn);
    Status delNode(uint32_t pointer, Transaction* txn);
    Expected<PSE> getNode(uint32_t pointer, Transaction* txn);
    std::pair<uint32_t, PSE> makeNode(uint64_t score, const std::string& subkey);
    const uint8_t _maxLevel;
    uint8_t _level;
    uint32_t _count;
    uint32_t _dbId;
    std::string _pk;
    PStore _store;
};

}  // namespace tendisplus
#endif  // SRC_TENDISPLUS_STORAGE_SKIPLIST_H_
