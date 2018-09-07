#ifndef SRC_TENDISPLUS_STORAGE_EXPIRABLE_H_
#define SRC_TENDISPLUS_STORAGE_EXPIRABLE_H_

#include "tendisplus/storage/kvstore.h"

namespace tendisplus {
class ExpirableDBWrapper {
 public:
    ExpirableDBWrapper(PStore store);
    ExpirableDBWrapper() = delete;
    ~ExpirableDBWrapper() = default;
    Expected<RecordValue> getKV(const RecordKey& key,
                                Transaction* txn);
 private:
    PStore _store;
};
}  // namespace tendisplus
#endif  // SRC_TENDISPLUS_STORAGE_EXPIRABLE_H_
