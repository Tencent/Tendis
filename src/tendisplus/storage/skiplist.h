// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

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
const uint64_t SKIPLIST_INVALID_POS = (uint64_t)-1;
class SkipList {
 public:
  using PSE = std::unique_ptr<ZSlEleValue>;
  using PSE_MAP = std::map<uint64_t, SkipList::PSE>;
  SkipList(uint32_t chunkId,
           uint32_t dbId,
           const std::string& pk,
           const ZSlMetaValue& meta,
           PStore store);
  Status insert(double score, const std::string& subkey, Transaction* txn);
  Status remove(double score, const std::string& subkey, Transaction* txn);
  Expected<uint32_t> rank(double score,
                          const std::string& subkey,
                          Transaction* txn);

  Expected<bool> isInRange(const Zrangespec& spec, Transaction* txn);
  Expected<bool> isInLexRange(const Zlexrangespec& spec, Transaction* txn);

  Expected<uint64_t> firstInRange(const Zrangespec& range, Transaction* txn);
  Expected<uint64_t> lastInRange(const Zrangespec& range, Transaction* txn);

  Expected<uint64_t> firstInLexRange(const Zlexrangespec& range,
                                     Transaction* txn);
  Expected<uint64_t> lastInLexRange(const Zlexrangespec& range,
                                    Transaction* txn);

  Expected<std::list<std::pair<double, std::string>>> scanByLex(
    const Zlexrangespec& range,
    uint64_t offset,
    uint64_t limit,
    bool rev,
    Transaction* txn);
  Expected<std::list<std::pair<double, std::string>>> scanByRank(
    int64_t start, int64_t len, bool rev, Transaction* txn);

  Expected<std::list<std::pair<double, std::string>>> scanByScore(
    const Zrangespec& range,
    uint64_t offset,
    uint64_t limit,
    bool rev,
    Transaction* txn);

  Expected<std::list<std::pair<double, std::string>>> removeRangeByScore(
    const Zrangespec& range, Transaction* txn);

  Expected<std::list<std::pair<double, std::string>>> removeRangeByLex(
    const Zlexrangespec& range, Transaction* txn);

  // 1-based index
  Expected<std::list<std::pair<double, std::string>>> removeRangeByRank(
    uint32_t start, uint32_t end, Transaction* txn);


  Status save(Transaction* txn,
              const Expected<RecordValue>& oldValue,
              uint64_t versionEP);
  Status traverse(std::stringstream& ss, Transaction* txn);
  uint32_t getCount() const;
  uint64_t getAlloc() const;
  uint64_t getTail() const;
  uint8_t getLevel() const;
  ZSlEleValue* getCacheNode(uint64_t pos);

  uint32_t nGetFromCache;
  uint32_t nGetFromStore;
  uint32_t nInserted;
  uint32_t nUpdated;
  uint32_t nDeleted;

 private:
  uint8_t randomLevel();
  Status removeInternal(uint64_t pointer,
                        const std::vector<uint64_t>& update,
                        Transaction* txn);
  Status saveNode(uint64_t pointer, const ZSlEleValue& val, Transaction* txn);
  Status delNode(uint64_t pointer, Transaction* txn);
  Expected<ZSlEleValue*> getEleByRank(uint32_t rank, Transaction* txn);
  Expected<ZSlEleValue*> getNode(uint64_t pointer, Transaction* txn);
  std::pair<uint64_t, PSE> makeNode(double score, const std::string& subkey);
  const uint8_t _maxLevel;
  uint8_t _level;
  uint32_t _count;
  uint64_t _tail;
  uint64_t _posAlloc;
  uint32_t _chunkId;
  uint32_t _dbId;
  std::string _pk;
  PStore _store;
  PSE_MAP cache;
};

}  // namespace tendisplus
#endif  // SRC_TENDISPLUS_STORAGE_SKIPLIST_H_
