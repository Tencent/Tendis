#ifndef SRC_TENDISPLUS_STORAGE_SKIPLIST_H_
#define SRC_TENDISPLUS_STORAGE_SKIPLIST_H_

#include <limits>
#include <memory>
#include <string>
#include <vector>
#include <atomic>
#include "tendisplus/storage/record.h"
#include "tendisplus/storage/kvstore.h"

namespace tendisplus {

class SkipList {
 public:
    SkipList(uint32_t dbId, const std::string& pk,
             const ZSlMetaValue& meta, PStore store);
    Status insert(const std::string& key, const std::string& value);
    bool erase(const std::string& key);

 private:
    Expected<ZSlEleValue> getHead(Transaction* txn);
    const uint8_t _maxLevel;
    // current level
    uint8_t _level;
    uint32_t _dbId;
    std::string _pk;
    PStore _store;
};

}  // namespace tendisplus
#endif  // SRC_TENDISPLUS_STORAGE_SKIPLIST_H_
