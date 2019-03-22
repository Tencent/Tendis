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
    explicit KVTtlCompactionFilter(uint64_t current_time)
    : _currentTime(current_time) {}

    ~KVTtlCompactionFilter() override {
        // TODO(vinchen) do something statistics here
        TEST_SYNC_POINT_CALLBACK("InspectKvTtlExpiredCount", &_expiredCount);
        TEST_SYNC_POINT_CALLBACK("InspectKvTtlFilterCount", &_filterCount);
    }

    const char* Name() const override { return "KVTTLCompactionFilter"; }

    virtual bool Filter(int /*level*/, const rocksdb::Slice& key,
      const rocksdb::Slice& existing_value,
      std::string* /*new_value*/,
      bool* /*value_changed*/) const {
        RecordType type = RecordKey::getRecordTypeRaw(key.data(), key.size());
        uint64_t ttl;
        _filterCount++;
        switch (type) {
        case RecordType::RT_KV:
            ttl = RecordValue::getTtlRaw(existing_value.data(),
                existing_value.size());
            if (ttl > 0 && ttl < _currentTime) {
                // Expired
                _expiredCount++;
                _expiredSize += key.size() + existing_value.size();

                return true;
            }
            break;
        case RecordType::RT_INVALID:
            // TODO(vinchen): make sure
            INVARIANT(0);
            break;
        default:
            break;
        }
        return false;
    }

 private:
    // millisecond, same as ttl in the record
    const uint64_t _currentTime;
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

    uint64_t currentTs = 0;
    if (_store) {
        // NOTE(vinchen): It can't get time = sinceEpoch () here, because it
        // should get the binlog time in slave point.
        currentTs = ((uint64_t)_store->getCurrentTime()) * 1000;
    }

    if (currentTs == 0) {
        LOG(WARNING) << "The currentTs is 0, the kvttlcompaction would do nothing;"
            << "the _store is " << (_store ? "not NULL" : "NULL");
        currentTs = std::numeric_limits<uint64_t>::max();
    }

    return std::unique_ptr<CompactionFilter>(
        new KVTtlCompactionFilter(currentTs));
}

}  // namespace tendisplus

