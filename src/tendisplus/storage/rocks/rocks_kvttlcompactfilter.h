// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

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
  explicit KVTtlCompactionFilterFactory(KVStore* store,
                                        const std::shared_ptr<ServerParams> cfg)
    : _store(store), _cfg(cfg) {}

  const char* Name() const override {
    return "KVTTLCompactionFilterFactory";
  }

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
    const CompactionFilter::Context& /*context*/) override;

 private:
  KVStore* _store;
  const std::shared_ptr<ServerParams> _cfg;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_STORAGE_ROCKS_ROCKS_KVTTLCOMPACTFILTER_H_
