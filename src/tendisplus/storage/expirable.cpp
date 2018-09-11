#include "tendisplus/storage/expirable.h"
#include "tendisplus/utils/time.h"

namespace tendisplus {
ExpirableDBWrapper::ExpirableDBWrapper(PStore store)
    :_store(store) {
}

Expected<RecordValue> ExpirableDBWrapper::getKV(const RecordKey& key,
                                                Transaction *txn) {
    Expected<RecordValue> expv = _store->getKV(key, txn);
    if (!expv.ok()) {
        return expv;
    }
    uint64_t ttlVal = expv.value().getTtl();
    if (ttlVal == 0) {
        return expv.value();
    }
    uint64_t currentTs = nsSinceEpoch()/1000;
    if (currentTs < ttlVal) {
        return expv.value();
    }
    Status s = _store->delKV(key, txn);
    if (!s.ok()) {
        return s;
    }
    return {ErrorCodes::ERR_NOTFOUND, ""};
}

}  // namespace tendisplus
