#include <chrono>
#include <random>
#include <map>
#include <utility>
#include "tendisplus/storage/skiplist.h"
#include "tendisplus/utils/invariant.h"

namespace tendisplus {

// 0 eq
// 1 <
// 2 >
int slCmp(uint64_t score0, const std::string& subk0,
          uint64_t score1, const std::string& subk1) {
    if ((score0 == score1) && (subk0 == subk1)) {
        return 0;
    }
    if ((score0 < score1) || (score0 == score1 && subk0 < subk1)) {
        return -1;
    }
    if ((score0 > score1) || (score0 == score1 && subk0 > subk1)) {
        return 1;
    }
    INVARIANT(0);
}

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

std::pair<uint32_t, SkipList::PSE> SkipList::makeNode(uint64_t score, const std::string& subkey) {
    auto result = std::make_unique<ZSlEleValue>(score, subkey);
    return {++_count, std::move(result)};
}

Expected<SkipList::PSE> SkipList::getNode(uint32_t pointer, Transaction *txn) {
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

Status SkipList::delNode(uint32_t pointer, Transaction* txn) {
    return {ErrorCodes::ERR_OK, ""};
}

Status SkipList::saveNode(uint32_t pointer, const ZSlEleValue& val, Transaction* txn) {
    return {ErrorCodes::ERR_OK, ""};
}

Status SkipList::save(Transaction* txn) {
    return {ErrorCodes::ERR_OK, ""};
}

Status SkipList::remove(uint64_t score, const std::string& subkey, Transaction *txn) {
    std::vector<uint32_t> update(_maxLevel+1);
    std::map<uint32_t, SkipList::PSE> cache;
    Expected<SkipList::PSE> expHead = getNode(ZSlMetaValue::HEAD_ID, txn);
    if (!expHead.ok()) {
        return expHead.status();
    }

    cache[ZSlMetaValue::HEAD_ID] = std::move(expHead.value());
    uint32_t pos = ZSlMetaValue::HEAD_ID;

    for (size_t i = _level; i >= 1; --i) {
        uint32_t tmpPos = cache[pos]->getForward(i);
        while (tmpPos != 0) {
            Expected<SkipList::PSE> next = getNode(tmpPos, txn);
            if (!next.ok()) {
                return next.status();
            }

            INVARIANT(cache.find(tmpPos) == cache.end());
            ZSlEleValue *pRaw = next.value().get();
            cache[tmpPos] = std::move(next.value());
            if (slCmp(pRaw->getScore(), pRaw->getSubKey(), score, subkey) < 0) {
                pos = tmpPos;
                tmpPos = next.value()->getForward(i);
            } else {
                break;
            }
        }
        update[i] = pos;
    }
    pos = cache[pos]->getForward(1);
    // donot allow empty del, check existence before del
    INVARIANT(slCmp(cache[pos]->getScore(), cache[pos]->getSubKey(), score, subkey) == 0);

    for (size_t i = 1; i <= _level; ++i) {
        if (cache[update[i]]->getForward(i) != pos) {
            break;
        }
        cache[update[i]]->setForward(i, cache[pos]->getForward(i));
    }
    while (_level > 1 && cache[ZSlMetaValue::HEAD_ID]->getForward(_level) == 0) {
        --_level;
    }
    for (size_t i = 1; i <= _level; ++i) {
        INVARIANT(update[i] >= ZSlMetaValue::HEAD_ID);
        INVARIANT(cache.find(update[i]) != cache.end());
        Status s = saveNode(update[i], *cache[update[i]], txn);
        if (!s.ok()) {
            return s;
        }
    }
    return delNode(pos, txn);
}

Status SkipList::insert(uint64_t score, const std::string& subkey, Transaction *txn) {
    std::vector<uint32_t> update(_maxLevel+1, 0);
    std::map<uint32_t, SkipList::PSE> cache;
    Expected<SkipList::PSE> expHead = getNode(ZSlMetaValue::HEAD_ID, txn);
    if (!expHead.ok()) {
        return expHead.status();
    }

    cache[ZSlMetaValue::HEAD_ID] = std::move(expHead.value());
    uint32_t pos = ZSlMetaValue::HEAD_ID;

    for (size_t i = _level; i >= 1; --i) {
        uint32_t tmpPos = cache[pos]->getForward(i);
        while (tmpPos != 0) {
            Expected<SkipList::PSE> next = getNode(tmpPos, txn);
            if (!next.ok()) {
                return next.status();
            }

            // donot allow duplicate, check existence before insert,
            INVARIANT(next.value()->getSubKey() != subkey);
            INVARIANT(cache.find(tmpPos) == cache.end());
            ZSlEleValue *pRaw = next.value().get();
            cache[tmpPos] = std::move(next.value());
            if (slCmp(pRaw->getScore(), pRaw->getSubKey(), score, subkey) < 0) {
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
    std::pair<uint32_t, SkipList::PSE> p = SkipList::makeNode(score, subkey);
    cache[p.first] = std::move(p.second);
    for (size_t i = 1; i <= _level; ++i) {
        cache[p.first]->setForward(i, cache[update[i]]->getForward(i));
        cache[update[i]]->setForward(i, p.first);
    }
    for (size_t i = 1; i <= _level; ++i) {
        INVARIANT(update[i] >= ZSlMetaValue::HEAD_ID);
        INVARIANT(cache.find(update[i]) != cache.end());
        Status s = saveNode(update[i], *cache[update[i]], txn);
        if (!s.ok()) {
            return s;
        }
    }
    return save(txn);
}

}  // namespace tendisplus
