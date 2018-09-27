#include <chrono>
#include <random>
#include <map>
#include <utility>
#include "tendisplus/storage/skiplist.h"
#include "tendisplus/utils/invariant.h"

namespace tendisplus {

SkipList::SkipList(uint32_t dbId, const std::string& pk,
                   const ZSlMetaValue& meta,
                   PStore store)
    :_maxLevel(meta.getMaxLevel()),
     _level(meta.getLevel()),
     _count(meta.getCount()),
     _dbId(dbId),
     _pk(pk),
     _store(store) {
}

uint8_t SkipList::randomLevel() {
    static thread_local std::mt19937 generator(
        std::chrono::system_clock::now().time_since_epoch().count());
    std::uniform_int_distribution<int> distribution(0, 1);
    uint8_t lvl = 1;
    while (distribution(generator) && lvl < _maxLevel) {
        ++lvl;
    }
    return lvl;
}

std::pair<uint32_t, PSE> SkipList::makeNode(const std::string& key) {
    auto result = std::make_unique<ZSlEleValue>();
    result->setKey(key);
    return {++_count, std::move(result)};
}

Expected<PSE> SkipList::getNode(Transaction *txn, uint32_t pointer) {
    std::string pointerStr = std::to_string(pointer);
    RecordKey rk(_dbId, RecordType::RT_ZSET_S_ELE, _pk, pointerStr);
    Expected<RecordValue> rv = _store->getKV(rk, txn);
    if (!rv.ok()) {
        return rv.status();
    }
    const std::string& s = rv.value().getValue();
    auto result = ZSlEleValue::decode(s);
    if (!result.ok()) {
        return result.status();
    }
    return std::make_unique<ZSlEleValue>(std::move(result.value()));
}

Status SkipList::saveNode(uint32_t pointer, const ZSlEleValue& val) {
    return {ErrorCodes::ERR_OK, ""};
}

Status SkipList::save() {
    return {ErrorCodes::ERR_OK, ""};
}

Status SkipList::insert(const std::string& key, Transaction *txn) {
    std::vector<uint32_t> update(_maxLevel+1, 0);
    std::map<uint32_t, PSE> cache;
    Expected<PSE> expHead = getNode(txn, ZSlMetaValue::HEAD_ID);
    if (!expHead.ok()) {
        return expHead.status();
    }

    cache[ZSlMetaValue::HEAD_ID] = std::move(expHead.value());
    uint32_t pos = ZSlMetaValue::HEAD_ID;

    for (size_t i = _level; i >= 1; --i) {
        uint32_t tmpPos = cache[pos]->getForward(i);
        while (tmpPos != 0) {
            Expected<PSE> next = getNode(txn, tmpPos);
            if (!next.ok()) {
                return next.status();
            }

            // donot allow duplicate, check existence before insert,
            INVARIANT(next.value()->getKey() != key);
            INVARIANT(cache.find(tmpPos) == cache.end());

            cache[tmpPos] = std::move(next.value());
            if (next.value()->getKey() < key) {
                pos = tmpPos;
                tmpPos = next.value()->getForward(i);
            } else {
                break;
            }
        }
        update[i] = pos;
    }

    uint8_t lvl = randomLevel();
    if (lvl > _level) {
        for (size_t i = _level+1; i <= lvl; i++) {
            update[i] = ZSlMetaValue::HEAD_ID;
        }
        _level = lvl;
    }
    std::pair<uint32_t, PSE> p = SkipList::makeNode(key);
    cache[p.first] = std::move(p.second);
    for (size_t i = 1; i <= _level; ++i) {
        cache[p.first]->setForward(i, cache[update[i]]->getForward(i));
        cache[update[i]]->setForward(i, p.first);
    }
    for (size_t i = 1; i <= _level; ++i) {
        INVARIANT(update[i] >= ZSlMetaValue::HEAD_ID);
        INVARIANT(cache.find(update[i]) != cache.end());
        Status s = saveNode(update[i], *cache[update[i]]);
        if (!s.ok()) {
            return s;
        }
    }
    return save();
}

}  // namespace tendisplus
