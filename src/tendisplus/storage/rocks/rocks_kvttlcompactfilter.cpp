// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <string>
#include <memory>
#include <limits>
#include "rocksdb/compaction_filter.h"
#include "tendisplus/storage/rocks/rocks_kvttlcompactfilter.h"
#include "tendisplus/storage/record.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/time.h"
#include "tendisplus/utils/sync_point.h"
#include "glog/logging.h"

namespace tendisplus {
class KVTtlCompactionFilter : public CompactionFilter {
 public:
  explicit KVTtlCompactionFilter(KVStore* store,
                                 uint64_t current_time,
                                 const std::shared_ptr<ServerParams> cfg)
    : _store(store), _currentTime(current_time), _cfg(cfg) {}

  ~KVTtlCompactionFilter() override {
    TEST_SYNC_POINT_CALLBACK("InspectKvTtlExpiredCount", &_expiredCount);
    TEST_SYNC_POINT_CALLBACK("InspectKvTtlFilterCount", &_filterCount);

    // do something statistics here
    _store->stat.compactFilterCount.fetch_add(_filterCount,
                                              std::memory_order_relaxed);
    _store->stat.compactKvExpiredCount.fetch_add(_expiredCount,
                                                 std::memory_order_relaxed);
  }

  const char* Name() const override {
    return "KVTTLCompactionFilter";
  }

  virtual bool Filter(int /*level*/,
                      const rocksdb::Slice& key,
                      const rocksdb::Slice& existing_value,
                      std::string* /*new_value*/,
                      bool* /*value_changed*/) const {
    if (_cfg->noexpire) {
      return false;
    }

    RecordType type = RecordKey::decodeType(key.data(), key.size());
    RecordType vt;
    uint64_t ttl;
    _filterCount++;
    switch (type) {
      case RecordType::RT_DATA_META:
        vt =
          RecordValue::decodeType(existing_value.data(), existing_value.size());
        if (vt == RecordType::RT_KV) {
          ttl = RecordValue::decodeTtl(existing_value.data(),
                                       existing_value.size());
          if (ttl > 0 && ttl < _currentTime) {
            // Expired
            _expiredCount++;
            _expiredSize += key.size() + existing_value.size();

            return true;
          }
        }
        break;
      case RecordType::RT_INVALID:
        // TODO(vinchen): make sure
        INVARIANT_D(0);
        break;
      default:
        break;
    }
    return false;
  }

 private:
  KVStore* _store;
  // millisecond, same as ttl in the record
  const uint64_t _currentTime;
  const std::shared_ptr<ServerParams> _cfg;
  // It is safe to not using std::atomic since the compaction filter,
  // created from a compaction filter factory, will not be called
  // from multiple threads.
  mutable uint64_t _expiredCount = 0;
  mutable uint64_t _expiredSize = 0;
  mutable uint64_t _filterCount = 0;
};

std::unique_ptr<CompactionFilter>
KVTtlCompactionFilterFactory::CreateCompactionFilter(
  const CompactionFilter::Context& context) {
  if (_cfg->noexpire) {
    return nullptr;
  }

  uint64_t currentTs = 0;
  INVARIANT(_store != nullptr);
  // NOTE(vinchen): It can't get time = sinceEpoch () here, because it
  // should get the binlog time in slave point.
  currentTs = _store->getCurrentTime();

  if (currentTs == 0) {
    LOG(WARNING) << "The currentTs is 0, the kvttlcompaction would do nothing";
    currentTs = std::numeric_limits<uint64_t>::max();
  }

  return std::unique_ptr<CompactionFilter>(
    new KVTtlCompactionFilter(_store, currentTs, _cfg));
}

}  // namespace tendisplus
