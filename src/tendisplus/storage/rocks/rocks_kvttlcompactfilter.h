#ifndef SRC_TENDISPLUS_STORAGE_ROCKS_ROCKS_KVTTLCOMPACTFILTER_H_
#define SRC_TENDISPLUS_STORAGE_ROCKS_ROCKS_KVTTLCOMPACTFILTER_H_

#include <memory>
#include "rocksdb/compaction_filter.h"
#include "rocksdb/env.h"
#include "tendisplus/storage/kvstore.h"
#include "tendisplus/storage/rocks/rocks_kvstore.h"

namespace tendisplus {
using rocksdb::CompactionFilter;
using rocksdb::CompactionFilterFactory;

struct KVTTLCompactionContext {
  bool is_manual_compaction;
};

class KVTtlCompactionFilterFactory : public CompactionFilterFactory {
 public:
  explicit KVTtlCompactionFilterFactory(KVStore* store) : _store(store) {}

  const char* Name() const override {
    return "KVTTLCompactionFilterFactory";
  }

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
    const CompactionFilter::Context& /*context*/) override;

 private:
  KVStore* _store;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_STORAGE_ROCKS_ROCKS_KVTTLCOMPACTFILTER_H_
