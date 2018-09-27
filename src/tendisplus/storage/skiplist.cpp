#include "tendisplus/storage/skiplist.h"

namespace tendisplus {

SkipList::SkipList(uint32_t dbId, const std::string& pk,
                   const ZSlMetaValue& meta,
                   PStore store)
    :_maxLevel(meta.getMaxLevel()),
     _level(meta.getLevel()),
     _dbId(dbId),
     _pk(pk),
     _store(store) {
}

Expected<ZSlEleValue> SkipList::getNode(Transaction *txn, uint32_t pointer) {
    std::string pointerStr = std::to_string(pointer);
    RecordKey rk(_dbId, RecordType::RT_ZSET_S_ELE, _pk, pointerStr);
    Expected<RecordValue> rv = _store->getKV(rk, txn);
    if (!rv.ok()) {
        return rv.status();
    }
    const std::string& s = rv.value().getValue();
    return ZSlEleValue::decode(s);
}

Status SkipList::insert(const std::string& key, const std::string& val, Transaction *txn) {
    std::vector<ZSlEleValue> update(_maxLevel+1);
    Expected<ZSlEleValue> x = getNode(txn, ZSlMetaValue::HEAD_ID);
    if (!x.ok()) {
        return x.status();
    }
    for (size_t i = _level; i >= 1; --i) {
        uint32_t pointer = x.getForward(i);
        while (pointer != 0) {
            Expected<ZSlEleValue> next = getNode(txn, pointer);
            if (!next.ok()) {
                return next.status();
            }
            if (next.getKey() < key) {
                pointer = next.getForward(i);
                if (pointer != 0) {
                    x = getNode(txn, pointer);
                }
            } else {
                break;
            }
        }
        update[i] = x;
    }
    
    x = x->forward[1].get();
    if (x->key == key) {
        x->value = value;
    } else {
        uint8_t lvl = randomLevel();
        if (lvl > _level) {
            for (size_t i = _level+1; i <= lvl; i++) {
                update[i] = _head.get();
            }
        }
        auto p = SkipList::makeNode(lvl, key, value);
        for (size_t i = 1; i <= _level; ++i) {
            p->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = std::move(p);
        }
    }
}

}  // namespace tendisplus
