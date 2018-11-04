#ifndef SRC_TENDISPLUS_STORAGE_SKIPLIST_H_
#define SRC_TENDISPLUS_STORAGE_SKIPLIST_H_

#include <map>
#include <limits>
#include <memory>
#include <string>
#include <list>
#include <vector>
#include <atomic>
#include <utility>
#include "tendisplus/storage/record.h"
#include "tendisplus/storage/kvstore.h"
#include "tendisplus/utils/redis_port.h"

namespace tendisplus {

using Zrangespec = redis_port::Zrangespec;
using Zlexrangespec = redis_port::Zlexrangespec;

class SkipList {
 public:
    using PSE = std::unique_ptr<ZSlEleValue>;
    SkipList(uint32_t dbId, const std::string& pk,
             const ZSlMetaValue& meta, PStore store);
    Status insert(uint64_t score, const std::string& subkey, Transaction* txn);
    Status remove(uint64_t score, const std::string& subkey, Transaction* txn);
    Expected<uint32_t> rank(uint64_t score,
                            const std::string& subkey, Transaction* txn);

    Expected<bool> isInRange(const Zrangespec& spec, Transaction* txn);
    Expected<bool> isInLexRange(const Zlexrangespec& spec, Transaction* txn);

    Expected<PSE> firstInRange(const Zrangespec& range, Transaction *txn);
    Expected<PSE> lastInRange(const Zrangespec& range, Transaction *txn);

    Expected<PSE> firstInLexRange(const Zlexrangespec& range, Transaction *txn);
    Expected<PSE> lastInLexRange(const Zlexrangespec& range, Transaction *txn);

    Expected<std::list<std::pair<uint64_t, std::string>>> scanByLex(
            const Zlexrangespec& range, int64_t offset, int64_t limit,
            bool rev, Transaction *txn);
    Expected<std::list<std::pair<uint64_t, std::string>>> scanByRank(
        int64_t start, int64_t len, bool rev, Transaction *txn);

    Status save(Transaction* txn);
    Status traverse(std::stringstream& ss, Transaction* txn);
    uint32_t getCount() const;
    uint64_t getAlloc() const;
    uint64_t getTail() const;
    uint8_t getLevel() const;

 private:
    uint8_t randomLevel();
    Status saveNode(uint64_t pointer, const ZSlEleValue& val, Transaction* txn);
    Status delNode(uint64_t pointer, Transaction* txn);
    Expected<ZSlEleValue*> getEleByRank(uint32_t rank,
                          std::map<uint64_t, SkipList::PSE>* cache,
                          Transaction* txn);
    Expected<ZSlEleValue*> getNode(uint64_t pointer,
                          std::map<uint64_t, SkipList::PSE>* cache,
                          Transaction* txn);
    std::pair<uint64_t, PSE> makeNode(uint64_t score,
                                      const std::string& subkey);
    const uint8_t _maxLevel;
    uint8_t _level;
    uint32_t _count;
    uint64_t _tail;
    uint64_t _posAlloc;
    uint32_t _dbId;
    std::string _pk;
    PStore _store;
};

}  // namespace tendisplus
#endif  // SRC_TENDISPLUS_STORAGE_SKIPLIST_H_
