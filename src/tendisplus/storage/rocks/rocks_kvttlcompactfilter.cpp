#include <string>
#include <memory>
#include <limits>
#include "rocksdb/compaction_filter.h"
#include "tendisplus/storage/rocks/rocks_kvttlcompactfilter.h"
#include "tendisplus/storage/record.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/time.h"

namespace tendisplus {
class KVTtlCompactionFilter : public CompactionFilter {
 public:
    explicit KVTtlCompactionFilter(uint64_t current_time)
    : _currentTime(current_time) {}

    ~KVTtlCompactionFilter() override {
        // TODO(vinchen) do something statistics here
    }

    const char* Name() const override { return "KVTTLCompactionFilter"; }

    virtual bool Filter(int /*level*/, const rocksdb::Slice& key,
      const rocksdb::Slice& existing_value,
      std::string* /*new_value*/,
      bool* /*value_changed*/) const {
        RecordType type = RecordKey::getRecordTypeRaw(key.data(), key.size());
        uint64_t ttl;
        switch (type) {
        case RecordType::RT_KV:
            ttl = RecordValue::getTtlRaw(existing_value.data(),
                existing_value.size());
            if (ttl < _currentTime) {
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
    const uint64_t _currentTime;
    // It is safe to not using std::atomic since the compaction filter,
    // created from a compaction filter factroy, will not be called
    // from multiple threads.
    mutable uint64_t _expiredCount = 0;
    mutable uint64_t _expiredSize = 0;
};

std::unique_ptr<CompactionFilter>
KVTtlCompactionFilterFactory::CreateCompactionFilter(
    const CompactionFilter::Context& context) {

    uint32_t currentTs = std::numeric_limits<uint32_t>::max();
    if (_store) {
        // NOTE(vinchen): It can't get time = sinceEpoch () here, because it
        // should get the binlog time in slave point.
        currentTs = _store->getCurrentTime();
    }

    INVARIANT(currentTs > 0);
    return std::unique_ptr<CompactionFilter>(
        new KVTtlCompactionFilter(currentTs));
}

}  // namespace tendisplus

