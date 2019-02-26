#include <chrono>
#include <random>
#include <map>
#include <utility>
#include "tendisplus/storage/skiplist.h"
#include "tendisplus/utils/invariant.h"

namespace tendisplus {

int compareStringObjectsForLexRange(const std::string& a,
                                    const std::string& b) {
    if (a == b) {
        return 0;
    }
    if (a == ZLEXMIN || b == ZLEXMAX) return -1;
    if (a == ZLEXMAX || b == ZLEXMIN) return 1;
    return a < b ? -1 : 1;
}

bool zslValueGteMin(uint64_t value, const Zrangespec& spec) {
    return spec.minex ? (value > spec.min) : (value >= spec.min);
}

bool zslValueLteMax(uint64_t value, const Zrangespec& spec) {
    return spec.maxex ? (value < spec.max) : (value <= spec.max);
}

bool zslLexValueGteMin(const std::string& value, const Zlexrangespec& spec) {
    return spec.minex ?
        (compareStringObjectsForLexRange(value,spec.min) > 0) :
        (compareStringObjectsForLexRange(value,spec.min) >= 0);
}

bool zslLexValueLteMax(const std::string& value, const Zlexrangespec& spec) {
    return spec.maxex ?
        (compareStringObjectsForLexRange(value,spec.max) < 0) :
        (compareStringObjectsForLexRange(value,spec.max) <= 0);
}

// 0 ==
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

SkipList::SkipList(uint32_t chunkId, uint32_t dbId, const std::string& pk,
                   const ZSlMetaValue& meta,
                   PStore store)
    :_maxLevel(meta.getMaxLevel()),
     _level(meta.getLevel()),
     _count(meta.getCount()),
     _tail(meta.getTail()),
     _posAlloc(meta.getPosAlloc()),
     _chunkId(chunkId),
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

std::pair<uint64_t, SkipList::PSE> SkipList::makeNode(
                                             uint64_t score,
                                             const std::string& subkey) {
    auto result = std::make_unique<ZSlEleValue>(score, subkey);
    return {++_posAlloc, std::move(result)};
}

Expected<ZSlEleValue*> SkipList::getEleByRank(uint32_t rank,
                                 std::map<uint64_t, SkipList::PSE>* pcache,
                                 Transaction *txn) {
    auto& cache = *pcache;
    Expected<ZSlEleValue*> expHead =
        getNode(ZSlMetaValue::HEAD_ID, pcache, txn);
    if (!expHead.ok()) {
        return expHead.status();
    }

    uint64_t pos = ZSlMetaValue::HEAD_ID;
    uint32_t traversed = 0;
    for (size_t i = _level; i >= 1; --i) {
        uint64_t tmpPos = cache[pos]->getForward(i);
        while (tmpPos != 0) {
            Expected<ZSlEleValue*> next = getNode(tmpPos, &cache, txn);
            if (!next.ok()) {
                return next.status();
            }

            ZSlEleValue *pRaw = next.value();
            if (traversed + cache[pos]->getSpan(i) <= rank) {
                traversed += cache[pos]->getSpan(i);
                pos = tmpPos;
                tmpPos = pRaw->getForward(i);
            } else {
                break;
            }
        }
        if (traversed == rank) {
            return cache[pos].get();
        }
    }
    return nullptr;
}

Expected<ZSlEleValue*> SkipList::getNode(uint64_t pointer,
                                 std::map<uint64_t, SkipList::PSE>* pcache,
                                 Transaction *txn) {
    auto& cache = *pcache;
    auto it = cache.find(pointer);
    if (it != cache.end()) {
        return it->second.get();
    }
    std::string pointerStr = std::to_string(pointer);
    RecordKey rk(_chunkId, _dbId, RecordType::RT_ZSET_S_ELE, _pk, pointerStr);
    Expected<RecordValue> rv = _store->getKV(rk, txn);
    if (!rv.ok()) {
        return rv.status();
    }
    const std::string& s = rv.value().getValue();
    auto result = ZSlEleValue::decode(s);
    if (!result.ok()) {
        return result.status();
    }
    auto ptr = std::make_unique<ZSlEleValue>(std::move(result.value()));
    ZSlEleValue* toReturn = ptr.get();
    cache[pointer] = std::move(ptr);
    return toReturn;
}

Status SkipList::delNode(uint64_t pointer, Transaction* txn) {
    RecordKey rk(_chunkId, _dbId,
                 RecordType::RT_ZSET_S_ELE,
                 _pk,
                 std::to_string(pointer));
    return _store->delKV(rk, txn);
}

Status SkipList::saveNode(uint64_t pointer,
                          const ZSlEleValue& val,
                          Transaction* txn) {
    RecordKey rk(_chunkId, _dbId,
                 RecordType::RT_ZSET_S_ELE,
                 _pk,
                 std::to_string(pointer));
    RecordValue rv(val.encode());
    return _store->setKV(rk, rv, txn);
}

Status SkipList::save(Transaction* txn) {
    RecordKey rk(_chunkId, _dbId, RecordType::RT_ZSET_META, _pk, "");
    ZSlMetaValue mv(_level, _maxLevel, _count, _tail, _posAlloc);
    RecordValue rv(mv.encode());
    return _store->setKV(rk, rv, txn);
}

Status SkipList::removeInternal(uint64_t pos,
                                const std::vector<uint64_t>& update,
                                std::map<uint64_t, SkipList::PSE>* pcache,
                                Transaction *txn) {
    auto& cache = *pcache;
    for (size_t i = 1; i <= _level; ++i) {
        auto& toupdate = cache[update[i]];
        if (toupdate->getForward(i) != pos) {
            toupdate->setSpan(i, toupdate->getSpan(i)-1);
        } else  {
            INVARIANT(update[i] >= ZSlMetaValue::HEAD_ID);
            toupdate->setSpan(i,
                              toupdate->getSpan(i) + cache[pos]->getSpan(i)-1);
            toupdate->setForward(i, cache[pos]->getForward(i));
        }
    }

    uint64_t btmFwd = cache[pos]->getForward(1);
    if (btmFwd) {
        auto node = getNode(btmFwd, &cache, txn);
        if (!node.ok()) {
            return node.status();
        }
        node.value()->setBackward(cache[pos]->getBackward());
        Status s = saveNode(btmFwd, *cache[btmFwd], txn);
        if (!s.ok()) {
            return s;
        }
    } else {
        _tail = cache[pos]->getBackward();
    }

    --_count;
    for (size_t i = 1; i <= _level; ++i) {
        Status s = saveNode(update[i], *cache[update[i]], txn);
        if (!s.ok()) {
            return s;
        }
    }
    while (_level > 1 &&
           cache[ZSlMetaValue::HEAD_ID]->getForward(_level) == 0) {
        --_level;
    }
    return delNode(pos, txn);
}

Expected<std::list<std::pair<uint64_t, std::string>>>
SkipList::removeRangeByRank(uint32_t start, uint32_t end, Transaction* txn) {
    std::vector<uint64_t> update(_maxLevel+1);
    std::map<uint64_t, SkipList::PSE> cache;
    Expected<ZSlEleValue*> expHead =
            getNode(ZSlMetaValue::HEAD_ID, &cache, txn);
    if (!expHead.ok()) {
        return expHead.status();
    }

    uint64_t pos = ZSlMetaValue::HEAD_ID;
    uint32_t traversed = 0;

    for (size_t i = _level; i >= 1; --i) {
        uint64_t tmpPos = cache[pos]->getForward(i);
        while (tmpPos != 0) {
            Expected<ZSlEleValue*> next = getNode(tmpPos, &cache, txn);
            if (!next.ok()) {
                return next.status();
            }

            ZSlEleValue *pRaw = next.value();
            if (traversed + cache[pos]->getSpan(i) < start) {
                traversed += cache[pos]->getSpan(i);
                pos = tmpPos;
                tmpPos = pRaw->getForward(i);
            } else {
                break;
            }
        }
        update[i] = pos;
    }
    traversed += 1;

    std::list<std::pair<uint64_t, std::string>> result;
    pos = cache[pos]->getForward(1);
    while (pos && traversed <= end) {
        result.push_back({cache[pos]->getScore(), cache[pos]->getSubKey()});
        traversed += 1;
        if (result.size() > 1000) {
            return {ErrorCodes::ERR_INTERNAL, "exceed batch limit"};
        }
        uint64_t nxt = cache[pos]->getForward(1);
        Status s = removeInternal(pos, update, &cache, txn);
        if (!s.ok()) {
            return s;
        }
        pos = nxt;
        if (pos != 0) {
            auto enxt = getNode(pos, &cache, txn);
            if (!enxt.ok()) {
                return enxt.status();
            }
        }
    }
    return result;
}

Expected<std::list<std::pair<uint64_t, std::string>>>
SkipList::removeRangeByLex(const Zlexrangespec& range, Transaction* txn) {
    std::vector<uint64_t> update(_maxLevel+1);
    std::map<uint64_t, SkipList::PSE> cache;
    Expected<ZSlEleValue*> expHead =
            getNode(ZSlMetaValue::HEAD_ID, &cache, txn);
    if (!expHead.ok()) {
        return expHead.status();
    }

    uint64_t pos = ZSlMetaValue::HEAD_ID;

    for (size_t i = _level; i >= 1; --i) {
        uint64_t tmpPos = cache[pos]->getForward(i);
        while (tmpPos != 0) {
            Expected<ZSlEleValue*> next = getNode(tmpPos, &cache, txn);
            if (!next.ok()) {
                return next.status();
            }

            ZSlEleValue *pRaw = next.value();
            if (!zslLexValueGteMin(pRaw->getSubKey(), range)) {
                pos = tmpPos;
                tmpPos = pRaw->getForward(i);
            } else {
                break;
            }
        }
        update[i] = pos;
    }

    std::list<std::pair<uint64_t, std::string>> result;
    pos = cache[pos]->getForward(1);

    while (pos != 0 && zslLexValueLteMax(cache[pos]->getSubKey(), range)) {
        result.push_back({cache[pos]->getScore(), cache[pos]->getSubKey()});
        if (result.size() > 1000) {
            return {ErrorCodes::ERR_INTERNAL, "exceed batch limit"};
        }
        uint64_t nxt = cache[pos]->getForward(1);
        Status s = removeInternal(pos, update, &cache, txn);
        if (!s.ok()) {
            return s;
        }
        pos = nxt;
        if (pos != 0) {
            auto enxt = getNode(pos, &cache, txn);
            if (!enxt.ok()) {
                return enxt.status();
            }
        }
    }
    return result;
}
 
Expected<std::list<std::pair<uint64_t, std::string>>>
SkipList::removeRangeByScore(const Zrangespec& range, Transaction* txn) {
    std::vector<uint64_t> update(_maxLevel+1);
    std::map<uint64_t, SkipList::PSE> cache;
    Expected<ZSlEleValue*> expHead =
            getNode(ZSlMetaValue::HEAD_ID, &cache, txn);
    if (!expHead.ok()) {
        return expHead.status();
    }

    uint64_t pos = ZSlMetaValue::HEAD_ID;

    for (size_t i = _level; i >= 1; --i) {
        uint64_t tmpPos = cache[pos]->getForward(i);
        while (tmpPos != 0) {
            Expected<ZSlEleValue*> next = getNode(tmpPos, &cache, txn);
            if (!next.ok()) {
                return next.status();
            }

            ZSlEleValue *pRaw = next.value();
            if ((range.minex && pRaw->getScore() <= range.min) ||
                ((!range.minex) && pRaw->getScore() < range.min)) {
                pos = tmpPos;
                tmpPos = pRaw->getForward(i);
            } else {
                break;
            }
        }
        update[i] = pos;
    }

    std::list<std::pair<uint64_t, std::string>> result;
    pos = cache[pos]->getForward(1);
    while (pos != 0 &&
        (range.maxex ? cache[pos]->getScore() < range.max : cache[pos]->getScore() <= range.max)) {
        result.push_back({cache[pos]->getScore(), cache[pos]->getSubKey()});
        if (result.size() > 1000) {
            return {ErrorCodes::ERR_INTERNAL, "exceed batch limit"};
        }
        uint64_t nxt = cache[pos]->getForward(1);
        Status s = removeInternal(pos, update, &cache, txn);
        if (!s.ok()) {
            return s;
        }
        pos = nxt;
        if (pos != 0) {
            auto enxt = getNode(pos, &cache, txn);
            if (!enxt.ok()) {
                return enxt.status();
            }
        }
    }
    return result;
}

Status SkipList::remove(uint64_t score,
                        const std::string& subkey,
                        Transaction *txn) {
    std::vector<uint64_t> update(_maxLevel+1);
    std::map<uint64_t, SkipList::PSE> cache;
    Expected<ZSlEleValue*> expHead =
            getNode(ZSlMetaValue::HEAD_ID, &cache, txn);
    if (!expHead.ok()) {
        return expHead.status();
    }

    uint64_t pos = ZSlMetaValue::HEAD_ID;

    for (size_t i = _level; i >= 1; --i) {
        uint64_t tmpPos = cache[pos]->getForward(i);
        while (tmpPos != 0) {
            Expected<ZSlEleValue*> next = getNode(tmpPos, &cache, txn);
            if (!next.ok()) {
                return next.status();
            }

            ZSlEleValue *pRaw = next.value();
            if (slCmp(pRaw->getScore(), pRaw->getSubKey(), score, subkey) < 0) {
                pos = tmpPos;
                tmpPos = pRaw->getForward(i);
            } else {
                break;
            }
        }
        update[i] = pos;
    }
    pos = cache[pos]->getForward(1);
    // donot allow empty del, check existence before del
    INVARIANT(slCmp(cache[pos]->getScore(),
                    cache[pos]->getSubKey(),
                    score, subkey) == 0);

    return removeInternal(pos, update, &cache, txn);
}

Expected<uint32_t> SkipList::rank(uint64_t score,
                                  const std::string& subkey,
                                  Transaction *txn) {
    uint32_t rank = 0;
    std::map<uint64_t, SkipList::PSE> cache;
    Expected<ZSlEleValue*> expHead =
            getNode(ZSlMetaValue::HEAD_ID, &cache, txn);
    if (!expHead.ok()) {
        return expHead.status();
    }
    ZSlEleValue* x = expHead.value();

    for (size_t i = _level; i >= 1; i--) {
        uint64_t tmpPos = x->getForward(i);
        while (tmpPos) {
            Expected<ZSlEleValue*> next = getNode(tmpPos, &cache, txn);
            if (!next.ok()) {
                return next.status();
            }
            ZSlEleValue *pRaw = next.value();
            int cmp = slCmp(pRaw->getScore(), pRaw->getSubKey(), score, subkey);
            if (cmp <= 0) {
                rank += x->getSpan(i);
                x = next.value();
                tmpPos = x->getForward(i);
            } else {
                break;
            }
        }
        if (x->getSubKey() == subkey) {
            return rank;
        }
    }
    INVARIANT(0);
    return {ErrorCodes::ERR_INTERNAL, "not reachable"};
}

/* Find the first node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
Expected<SkipList::PSE> SkipList::firstInRange(
        const Zrangespec& range,
        Transaction *txn) {
    Expected<bool> inrange = isInRange(range, txn);
    if (!inrange.ok()) {
        return inrange.status();
    }
    if (!inrange.value()) {
        return PSE(nullptr);
    }

    std::map<uint64_t, SkipList::PSE> cache;
    Expected<ZSlEleValue*> expHead =
        getNode(ZSlMetaValue::HEAD_ID, &cache, txn);
    if (!expHead.ok()) {
        return expHead.status();
    }

    uint64_t pos = ZSlMetaValue::HEAD_ID;
    for (size_t i = _level; i >= 1; --i) {
        uint64_t tmpPos = cache[pos]->getForward(i);
        while (tmpPos != 0) {
            Expected<ZSlEleValue*> next = getNode(tmpPos, &cache, txn);
            if (!next.ok()) {
                return next.status();
            }

            ZSlEleValue *pRaw = next.value();
            if (!zslValueGteMin(pRaw->getScore(), range)) {
                pos = tmpPos;
                tmpPos = pRaw->getForward(i);
            } else {
                break;
            }
        }
    }
    pos = cache[pos]->getForward(1);
    INVARIANT(pos != 0);
    if (!zslValueLteMax(cache[pos]->getScore(), range)) {
        return PSE(nullptr);
    }
    return std::move(cache[pos]);
}

/* Find the last node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
Expected<SkipList::PSE> SkipList::lastInRange(
        const Zrangespec& range,
        Transaction *txn) {
    Expected<bool> inrange = isInRange(range, txn);
    if (!inrange.ok()) {
        return inrange.status();
    }
    if (!inrange.value()) {
        return PSE(nullptr);
    }

    std::map<uint64_t, SkipList::PSE> cache;
    Expected<ZSlEleValue*> expHead =
        getNode(ZSlMetaValue::HEAD_ID, &cache, txn);
    if (!expHead.ok()) {
        return expHead.status();
    }

    uint64_t pos = ZSlMetaValue::HEAD_ID;
    for (size_t i = _level; i >= 1; --i) {
        uint64_t tmpPos = cache[pos]->getForward(i);
        while (tmpPos != 0) {
            Expected<ZSlEleValue*> next = getNode(tmpPos, &cache, txn);
            if (!next.ok()) {
                return next.status();
            }

            ZSlEleValue *pRaw = next.value();
            if (zslValueLteMax(pRaw->getScore(), range)) {
                pos = tmpPos;
                tmpPos = pRaw->getForward(i);
            } else {
                break;
            }
        }
    }
    INVARIANT(pos != 0);
    if (!zslValueGteMin(cache[pos]->getScore(), range)) {
        return PSE(nullptr);
    }
    return std::move(cache[pos]);
}

Expected<bool> SkipList::isInRange(const Zrangespec& range, Transaction *txn) {
    if (range.min > range.max ||
            (fabs(range.min - range.max) < 0.001 &&
                (range.minex || range.maxex))) {
        return false;
    }

    if (_tail == 0) {
        return false;
    }

    std::map<uint64_t, SkipList::PSE> cache;
    Expected<ZSlEleValue*> expTail = getNode(_tail, &cache, txn);
    if (!expTail.ok()) {
        return expTail.status();
    }
    if (!zslValueGteMin(expTail.value()->getScore(), range)) {
        return false;
    }

    Expected<ZSlEleValue*> expHead =
            getNode(ZSlMetaValue::HEAD_ID, &cache, txn);
    if (!expHead.ok()) {
        return expHead.status();
    }
    uint64_t first = expHead.value()->getForward(1);
    if (first == 0) {
        return false;
    }
    Expected<ZSlEleValue*> expNode = getNode(first, &cache, txn);
    if (!expNode.ok()) {
        return expNode.status();
    }
    if (!zslValueLteMax(expNode.value()->getScore(), range)) {
        return false;
    }
    return true;
}

Expected<bool> SkipList::isInLexRange(const Zlexrangespec& range, Transaction *txn) {
    if (compareStringObjectsForLexRange(range.min, range.max) > 0 ||
            (range.min == range.max && (range.minex || range.maxex))) {
        return false;
    }

    if (_tail == 0) {
        return false;
    }

    std::map<uint64_t, SkipList::PSE> cache;
    Expected<ZSlEleValue*> expTail = getNode(_tail, &cache, txn);
    if (!expTail.ok()) {
        return expTail.status();
    }
    if (!zslLexValueGteMin(expTail.value()->getSubKey(), range)) {
        return false;
    }

    Expected<ZSlEleValue*> expHead =
            getNode(ZSlMetaValue::HEAD_ID, &cache, txn);
    if (!expHead.ok()) {
        return expHead.status();
    }
    uint64_t first = expHead.value()->getForward(1);
    if (first == 0) {
        return false;
    }
    Expected<ZSlEleValue*> expNode = getNode(first, &cache, txn);
    if (!expNode.ok()) {
        return expNode.status();
    }
    if (!zslLexValueLteMax(expNode.value()->getSubKey(), range)) {
        return false;
    }
    return true;
}

Expected<SkipList::PSE> SkipList::firstInLexRange(const Zlexrangespec& range, Transaction *txn) {
    Expected<bool> inrange = isInLexRange(range, txn);
    if (!inrange.ok()) {
        return inrange.status();
    }
    if (!inrange.value()) {
        return PSE(nullptr);
    }

    std::map<uint64_t, SkipList::PSE> cache;
    Expected<ZSlEleValue*> expHead =
        getNode(ZSlMetaValue::HEAD_ID, &cache, txn);
    if (!expHead.ok()) {
        return expHead.status();
    }

    uint64_t pos = ZSlMetaValue::HEAD_ID;
    for (size_t i = _level; i >= 1; --i) {
        uint64_t tmpPos = cache[pos]->getForward(i);
        while (tmpPos != 0) {
            Expected<ZSlEleValue*> next = getNode(tmpPos, &cache, txn);
            if (!next.ok()) {
                return next.status();
            }

            ZSlEleValue *pRaw = next.value();
            if (!zslLexValueGteMin(pRaw->getSubKey(), range)) {
                pos = tmpPos;
                tmpPos = pRaw->getForward(i);
            } else {
                break;
            }
        }
    }
    pos = cache[pos]->getForward(1);
    INVARIANT(pos != 0);
    if (!zslLexValueLteMax(cache[pos]->getSubKey(), range)) {
        return PSE(nullptr);
    }
    return std::move(cache[pos]);
}

Expected<SkipList::PSE> SkipList::lastInLexRange(const Zlexrangespec& range, Transaction *txn) {
    Expected<bool> inrange = isInLexRange(range, txn);
    if (!inrange.ok()) {
        return inrange.status();
    }
    if (!inrange.value()) {
        return PSE(nullptr);
    }

    std::map<uint64_t, SkipList::PSE> cache;
    Expected<ZSlEleValue*> expHead =
        getNode(ZSlMetaValue::HEAD_ID, &cache, txn);
    if (!expHead.ok()) {
        return expHead.status();
    }

    uint64_t pos = ZSlMetaValue::HEAD_ID;
    for (size_t i = _level; i >= 1; --i) {
        uint64_t tmpPos = cache[pos]->getForward(i);
        while (tmpPos != 0) {
            Expected<ZSlEleValue*> next = getNode(tmpPos, &cache, txn);
            if (!next.ok()) {
                return next.status();
            }

            ZSlEleValue *pRaw = next.value();
            if (zslLexValueLteMax(pRaw->getSubKey(), range)) {
                pos = tmpPos;
                tmpPos = pRaw->getForward(i);
            } else {
                break;
            }
        }
    }
    INVARIANT(pos != 0);
    if (!zslLexValueGteMin(cache[pos]->getSubKey(), range)) {
        return PSE(nullptr);
    }
    return std::move(cache[pos]);
}

Expected<std::list<std::pair<uint64_t, std::string>>>
SkipList::scanByScore(const Zrangespec& range, int64_t offset,
        int64_t limit, bool rev, Transaction *txn) {
    PSE pse = nullptr;
    if (rev) {
        Expected<PSE> tmp = lastInRange(range, txn);
        if (!tmp.ok()) {
            return tmp.status();
        }
        pse = std::move(tmp.value());
    } else {
        Expected<PSE> tmp = firstInRange(range, txn);
        if (!tmp.ok()) {
            return tmp.status();
        }
        pse = std::move(tmp.value());
    }
    if (pse == nullptr) {
        return std::list<std::pair<uint64_t, std::string>>();
    }

    std::list<std::pair<uint64_t, std::string>> result;
    std::map<uint64_t, SkipList::PSE> cache;
    ZSlEleValue *ln = pse.get();

    while (ln != nullptr && offset--) {
        if (rev) {
            auto nxt = ln->getBackward();
            if (nxt == 0) {
                break;
            }
            auto tmp = getNode(nxt, &cache, txn);
            if (!tmp.ok()) {
                return tmp.status();
            }
            ln = tmp.value();
        } else {
            auto nxt = ln->getForward(1);
            if (nxt == 0) {
                break;
            }
            auto tmp = getNode(nxt, &cache, txn);
            if (!tmp.ok()) {
                return tmp.status();
            }
            ln = tmp.value();
        }
    }
    while (ln != nullptr && limit--) {
        if (rev) {
            if (!zslValueGteMin(ln->getScore(), range)) {
                break;
            }
        } else {
            if (!zslValueLteMax(ln->getScore(), range)) {
                break;
            }
        }
        result.push_back({ln->getScore(), ln->getSubKey()});
        if (rev) {
            auto nxt = ln->getBackward();
            if (nxt == 0) {
                break;
            }
            auto tmp = getNode(nxt, &cache, txn);
            if (!tmp.ok()) {
                return tmp.status();
            }
            ln = tmp.value();
        } else {
            auto nxt = ln->getForward(1);
            if (nxt == 0) {
                break;
            }
            auto tmp = getNode(nxt, &cache, txn);
            if (!tmp.ok()) {
                return tmp.status();
            }
            ln = tmp.value();
        }
    }
    return std::move(result);
}

Expected<std::list<std::pair<uint64_t, std::string>>>
SkipList::scanByLex(const Zlexrangespec& range, int64_t offset,
        int64_t limit, bool rev, Transaction *txn) {
    PSE pse = nullptr;
    if (rev) {
        Expected<PSE> tmp = lastInLexRange(range, txn);
        if (!tmp.ok()) {
            return tmp.status();
        }
        pse = std::move(tmp.value());
    } else {
        Expected<PSE> tmp = firstInLexRange(range, txn);
        if (!tmp.ok()) {
            return tmp.status();
        }
        pse = std::move(tmp.value());
    }
    if (pse == nullptr) {
        return std::list<std::pair<uint64_t, std::string>>();
    }
    std::list<std::pair<uint64_t, std::string>> result;
    std::map<uint64_t, SkipList::PSE> cache;
    ZSlEleValue *ln = pse.get();

    while (ln != nullptr && offset--) {
        if (rev) {
            auto nxt = ln->getBackward();
            if (nxt == 0) {
                break;
            }
            auto tmp = getNode(nxt, &cache, txn);
            if (!tmp.ok()) {
                return tmp.status();
            }
            ln = tmp.value();
        } else {
            auto nxt = ln->getForward(1);
            if (nxt == 0) {
                break;
            }
            auto tmp = getNode(nxt, &cache, txn);
            if (!tmp.ok()) {
                return tmp.status();
            }
            ln = tmp.value();
        }
    }
    while (ln != nullptr && limit--) {
        if (rev) {
            if (!zslLexValueGteMin(ln->getSubKey(), range)) {
                break;
            }
        } else {
            if (!zslLexValueLteMax(ln->getSubKey(), range)) {
                break;
            }
        }
        result.push_back({ln->getScore(), ln->getSubKey()});
        if (rev) {
            auto nxt = ln->getBackward();
            if (nxt == 0) {
                break;
            }
            auto tmp = getNode(nxt, &cache, txn);
            if (!tmp.ok()) {
                return tmp.status();
            }
            ln = tmp.value();
        } else {
            auto nxt = ln->getForward(1);
            if (nxt == 0) {
                break;
            }
            auto tmp = getNode(nxt, &cache, txn);
            if (!tmp.ok()) {
                return tmp.status();
            }
            ln = tmp.value();
        }
    }
    return std::move(result);
}

Expected<std::list<std::pair<uint64_t, std::string>>>
SkipList::scanByRank(int64_t start, int64_t len, bool rev, Transaction *txn) {
    // std::cout<< start << ' ' << len << ' ' << rev << ' ' << _count << std::endl;
    std::map<uint64_t, SkipList::PSE> cache;
    ZSlEleValue* ln = nullptr;
    if (rev) {
        Expected<ZSlEleValue*> expTail = getNode(_tail, &cache, txn);
        if (!expTail.ok()) {
            return expTail.status();
        }
        ln = expTail.value();
        if (start > 0) {
            auto tmp = getEleByRank(_count-1-start, &cache, txn);
            if (!tmp.ok()) {
                return tmp.status();
            }
            ln = tmp.value();
        }
    } else {
        Expected<ZSlEleValue*> expHead =
                getNode(ZSlMetaValue::HEAD_ID, &cache, txn);
        if (!expHead.ok()) {
            return expHead.status();
        }
        uint64_t first = expHead.value()->getForward(1);
        Expected<ZSlEleValue*> expNode = getNode(first, &cache, txn);
        if (!expNode.ok()) {
            return expNode.status();
        }
        ln = expNode.value();
        if (start > 0) {
            auto tmp = getEleByRank(start+1, &cache, txn);
            if (!tmp.ok()) {
                return tmp.status();
            }
            ln = tmp.value();
        }
    }
    std::list<std::pair<uint64_t, std::string>> result;
    while (len--) {
        INVARIANT(ln != nullptr);
        // std::cout << ln->getScore() << ' ' << ln->getSubKey() << std::endl;
        result.push_back({ln->getScore(), ln->getSubKey()});
        if (len == 0) {
            break;
        }
        if (rev) {
            auto tmp = getNode(ln->getBackward(), &cache, txn);
            if (!tmp.ok()) {
                return tmp.status();
            }
            ln = tmp.value();
        } else {
            auto tmp = getNode(ln->getForward(1), &cache, txn);
            if (!tmp.ok()) {
                return tmp.status();
            }
            ln = tmp.value();
        }
    }
    return std::move(result);
}

Status SkipList::insert(uint64_t score,
                        const std::string& subkey,
                        Transaction *txn) {
    if (_count >= std::numeric_limits<int32_t>::max() / 2) {
        return {ErrorCodes::ERR_INTERNAL, "zset count reach limit"};
    }
    std::vector<uint64_t> update(_maxLevel+1, 0);
    std::vector<uint32_t> rank(_maxLevel+1, 0);
    std::map<uint64_t, SkipList::PSE> cache;
    Expected<ZSlEleValue*> expHead =
            getNode(ZSlMetaValue::HEAD_ID, &cache, txn);
    if (!expHead.ok()) {
        return expHead.status();
    }

    uint64_t pos = ZSlMetaValue::HEAD_ID;

    for (size_t i = _level; i >= 1; --i) {
        uint64_t tmpPos = cache[pos]->getForward(i);
        if (i != _level) {
            // accumulate upper level's rank
            rank[i] = rank[i+1];
        }
        while (tmpPos != 0) {
            // TODO(deyukong): get from cache first
            Expected<ZSlEleValue*> next = getNode(tmpPos, &cache, txn);
            if (!next.ok()) {
                return next.status();
            }
            // donot allow duplicate, check existence before insert
            INVARIANT(next.value()->getSubKey() != subkey);
            ZSlEleValue *pRaw = next.value();
            if (slCmp(pRaw->getScore(), pRaw->getSubKey(), score, subkey) < 0) {
                rank[i] += cache[pos]->getSpan(i);
                pos = tmpPos;
                tmpPos = pRaw->getForward(i);
            } else {
                break;
            }
        }
        update[i] = pos;
    }

    uint8_t lvl = randomLevel();
    if (lvl > _level) {
        for (size_t i = _level+1; i <= lvl; i++) {
            rank[i] = 0;
            update[i] = ZSlMetaValue::HEAD_ID;
            // NOTE(deyukong): head node also affects _count, so here the span
            // should be _count -1, not _count.
            cache[update[i]]->setSpan(i, _count-1);
        }
        _level = lvl;
    }
    std::pair<uint64_t, SkipList::PSE> p = SkipList::makeNode(score, subkey);
    cache[p.first] = std::move(p.second);
    for (size_t i = 1; i <= lvl; ++i) {
        INVARIANT(update[i] >= ZSlMetaValue::HEAD_ID);
        INVARIANT(cache.find(update[i]) != cache.end());
        cache[p.first]->setForward(i, cache[update[i]]->getForward(i));
        cache[update[i]]->setForward(i, p.first);
        cache[p.first]->setSpan(i,
                    cache[update[i]]->getSpan(i) - (rank[1] - rank[i]));
        cache[update[i]]->setSpan(i, rank[1] - rank[i] + 1);
    }
    for (size_t i = lvl+1; i <= _level; ++i) {
        cache[update[i]]->setSpan(i, cache[update[i]]->getSpan(i)+1);
    }
    if (update[1] == ZSlMetaValue::HEAD_ID) {
        cache[p.first]->setBackward(0);
    } else {
        cache[p.first]->setBackward(update[1]);
    }

    uint64_t btmFwd = cache[p.first]->getForward(1);
    if (btmFwd != 0) {
        auto node = getNode(btmFwd, &cache, txn);
        if (!node.ok()) {
            return node.status();
        }
        cache[btmFwd]->setBackward(p.first);
        Status s = saveNode(btmFwd, *cache[btmFwd], txn);
        if (!s.ok()) {
            return s;
        }
    } else {
        _tail = p.first;
    }
    for (size_t i = 1; i <= _level; ++i) {
        Status s = saveNode(update[i], *cache[update[i]], txn);
        if (!s.ok()) {
            return s;
        }
    }
    ++_count;
    return saveNode(p.first, *cache[p.first], txn);
}

Status SkipList::traverse(std::stringstream& ss, Transaction *txn) {
    std::map<uint64_t, SkipList::PSE> cache;
    for (size_t i = _level; i >= 1; i--) {
        Expected<ZSlEleValue*> expNode =
            getNode(ZSlMetaValue::HEAD_ID, &cache, txn);
        if (!expNode.ok()) {
            return expNode.status();
        }
        ZSlEleValue* node = expNode.value();
        ss << "level:" << i << ":";
        while (node->getForward(i)) {
            uint64_t myId = node->getForward(i);
            Expected<ZSlEleValue*> expNode =
                getNode(node->getForward(i), &cache, txn);
            if (!expNode.ok()) {
                return expNode.status();
            }
            node = expNode.value();
            ss << "(" << myId
               << "," << node->getSubKey()
               << "," << node->getScore()
               << "," << node->getSpan(i)
               << "," << node->getForward(i)
               << "),";
        }
        ss << std::endl;
    }
    return {ErrorCodes::ERR_OK, ""};
}

uint32_t SkipList::getCount() const {
    return _count;
}

uint8_t SkipList::getLevel() const {
    return _level;
}

uint64_t SkipList::getAlloc() const {
    return _posAlloc;
}

uint64_t SkipList::getTail() const {
    return _tail;
}
}  // namespace tendisplus
