#include <string>
#include <utility>
#include <memory>
#include <algorithm>
#include <cctype>
#include <clocale>
#include <vector>
#include <map>
#include <cmath>
#include <type_traits>
#include "glog/logging.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/storage/varint.h"

namespace tendisplus {

struct MyAllocator {
    char* data;
    void* p;
    char* firstAlignedPtr;
    // left size
    std::size_t leftSize;
    std::size_t allocSize;
    MyAllocator(size_t N = 1024) : data(0), p(0), firstAlignedPtr(0),
        leftSize(N), allocSize(N) {
        data = reinterpret_cast<char*>(calloc(N, sizeof(char)));
        p = data;
    }

    ~MyAllocator() {
        if (data) {
            free(data);
            data = NULL;
            p = NULL;
        }
    }
    template <typename T>
    T* aligned_alloc(std::size_t a = alignof(T)) {
        // p and leftSize is a reference
        if (std::align(a, sizeof(T), p, leftSize)) {
            if (!firstAlignedPtr) {
                INVARIANT(reinterpret_cast<char*>(p) == data);
                firstAlignedPtr = reinterpret_cast<char*>(p);
            }
 
            T* result = reinterpret_cast<T*>(p);
            p = reinterpret_cast<char*>(p) + sizeof(T);
            leftSize -= sizeof(T);

           return result;
        }
        INVARIANT(0);
        return nullptr;
    }

    template <typename T>
    T* aligned_alloc(const char* ptr, size_t size,
        std::size_t a = alignof(T)) {
        // check whether the left size is enough
        if (leftSize < size) {
            return nullptr;
        }

        T* val = aligned_alloc<T>(a);
        if (!val) {
            INVARIANT(0);
            return val;
        }

        // check the left size after
        size_t left = (size_t)(data + allocSize - reinterpret_cast<char*>(val));
        if (left < size) {
            INVARIANT(0);
            return nullptr;
        }
        memcpy(reinterpret_cast<char*>(val), ptr, size);
        if (left - size < leftSize) {
            leftSize = left - size;
        }

        return val;
    }

    template <typename T>
    T* getFirstAlignedAddr() {
        INVARIANT(firstAlignedPtr!=NULL);
        return reinterpret_cast<T*>(firstAlignedPtr);
    }

    template <typename T>
    void resize(size_t n, std::size_t a = alignof(T)) {
        if (allocSize < n) {
            char* newdata = reinterpret_cast<char*>(calloc(n, sizeof(char)));
            memcpy(newdata, data, allocSize);

            free(data);
            data = newdata;
            p = data + (allocSize - leftSize);

            leftSize += n - allocSize;
            allocSize = n;

            size_t s;
            void* tmpp = data;
            if (std::align(a, sizeof(T), tmpp, s)) {
                INVARIANT(reinterpret_cast<char*>(tmpp) == data);
                firstAlignedPtr = reinterpret_cast<char*>(tmpp);
            }
        }
    }
};

class HPLLObject {
 public:
    HPLLObject(int type = HLL_SPARSE);
    HPLLObject(const std::string& v);
    HPLLObject(const HPLLObject&) = delete;
    HPLLObject(HPLLObject&&) = default;

    int add(const std::string& subkey);
    uint64_t getHllCount() const;
    // Note(vinchen): it is not const;
    uint64_t getHllCountFast();
    std::string encode() const;
    int merge(const HPLLObject* hpll);
    int getHdrEncoding() const { return _hdr->encoding; }
    int updateByRawHpll(const HPLLObject* rawHpll);

 private:
    void hllInvalidateCache();

 private:
    // NOTE(vinchen): _hdr is a hllhdr and followed an array, it's memory
    // should be alloced by an aligned allocator.
    // But in fact alignof(hllhdr) = 1, and aligned_alloc maybe more safe.
    redis_port::hllhdr* _hdr;
    size_t  _hdrSize;
    std::unique_ptr<MyAllocator> _buf;
};

HPLLObject::HPLLObject(const std::string& v) {
    INVARIANT(redis_port::isHLLObject(v));

    // TODO
    _buf = std::make_unique<MyAllocator>(v.size());
    // INVARIANT(_buf->allocSize > v.size());
    _hdr = _buf->aligned_alloc<redis_port::hllhdr>(v.c_str(), v.size());
    _hdrSize = v.size();
}

HPLLObject::HPLLObject(int type) {
    int ret;
    switch (type) {
    case HLL_RAW:
        _buf = std::make_unique<MyAllocator>(HLL_MAX_SIZE);
        _hdr = _buf->aligned_alloc<redis_port::hllhdr>();
        _hdr->encoding = HLL_RAW; /* Special internal-only encoding. */
        _hdrSize = _buf->allocSize;
        break;
    case HLL_SPARSE:
        _buf = std::make_unique<MyAllocator>();
        _hdr = _buf->aligned_alloc<redis_port::hllhdr>();
        _hdr = redis_port::createHLLObject(reinterpret_cast<const char*>(_hdr),
                                _buf->allocSize, &_hdrSize);
        break;
    case HLL_DENSE:
    {
        auto tmp = std::make_unique<MyAllocator>();
        auto tmphdr = tmp->aligned_alloc<redis_port::hllhdr>();
        size_t tmphdrSize = 0;
        tmphdr = redis_port::createHLLObject(
                        reinterpret_cast<const char*>(tmphdr),
                        tmp->allocSize, &tmphdrSize);

        _buf = std::make_unique<MyAllocator>(HLL_DENSE_SIZE + HLL_HDR_SIZE);
        _hdr = _buf->aligned_alloc<redis_port::hllhdr>();
        ret = redis_port::hllSparseToDense(tmphdr, tmphdrSize,
                        _hdr, &_hdrSize, _buf->allocSize);
        INVARIANT(_hdrSize == HLL_DENSE_SIZE);
        INVARIANT(ret == C_OK);
    }
    break;
    default:
        INVARIANT(0);
        break;
    }
}

std::string HPLLObject::encode() const {
    return std::string(reinterpret_cast<char*>(_hdr), _hdrSize);
}

// return:
// -1 : something wrong
// 0 : nothing changed
// 1 : success, and something changed
int HPLLObject::add(const std::string& subkey) {
    while (1) {
        int ret = redis_port::hllAdd(_hdr, &_hdrSize, _buf->allocSize,
            (unsigned char*)subkey.c_str(), subkey.size());
        switch (ret) {
        case 1:
            // changed
            hllInvalidateCache();
        case 0:
            // no change
        case -1:
            // error
            return ret;

        case HLL_ERROR_PROMOTE:
        {
            auto m = std::make_unique<MyAllocator>(
                                    HLL_DENSE_SIZE + HLL_HDR_SIZE);
            auto newhdr = m->aligned_alloc<redis_port::hllhdr>();
            ret = redis_port::hllSparseToDense(_hdr, _hdrSize,
                newhdr, &_hdrSize, m->allocSize);
            if (ret != C_OK) {
                return -1;
            }

            _buf = std::move(m);
            _hdr = newhdr;
            break;
        }

        case HLL_ERROR_MEMORY:
            _buf->resize<redis_port::hllhdr>(_buf->allocSize + 128);
            _hdr = _buf->getFirstAlignedAddr<redis_port::hllhdr>();
            break;

        default:
            INVARIANT(0);
            break;
        }
    }

    return 0;
}

// return:
// -1 : something wrong
// 1 : success
int HPLLObject::merge(const HPLLObject* hpll) {
    INVARIANT(getHdrEncoding() == HLL_RAW);
    INVARIANT(hpll->getHdrEncoding() != HLL_RAW);
    int ret = redis_port::hllMerge(_hdr->registers, hpll->_hdr, hpll->_hdrSize);
    if (ret == C_ERR) {
        return -1;
    }
    hllInvalidateCache();
    return 1;
}

uint64_t HPLLObject::getHllCount() const {
    int invalid = 0;
    auto count = redis_port::hllCount(_hdr, _hdrSize, &invalid);
    if (invalid) {
        return (uint64_t)-1;
    }
    return count;
}

uint64_t HPLLObject::getHllCountFast() {
    // NOTE(vinchen): pfcount should be a read only command,
    // so here it should not use hpll->getHllCountFast()
    INVARIANT(0);
    int invalid = 0;
    auto count = redis_port::hllCountFast(_hdr, _hdrSize, &invalid);
    if (invalid) {
        return (uint64_t)-1;
    }
    hllInvalidateCache();
    return count;
}

void HPLLObject::hllInvalidateCache() {
    HLL_INVALIDATE_CACHE(_hdr);
}

// return:
// -1 : something wrong
// 1 : success
int HPLLObject::updateByRawHpll(const HPLLObject* rawHpll) {
    INVARIANT(rawHpll->getHdrEncoding() == HLL_RAW);

    int ret = redis_port::hllUpdateByRawHpll(_hdr, &_hdrSize,
                        _buf->allocSize, rawHpll->_hdr);
    if (ret == C_ERR) {
        return -1;
    }
    hllInvalidateCache();

    return 1;
}


class PfAddCommand: public Command {
 public:
    PfAddCommand()
        :Command("pfadd") {
    }

    ssize_t arity() const {
        return -2;
    }

    int32_t firstkey() const {
        return 1;
    }

    int32_t lastkey() const {
        return 1;
    }

    int32_t keystep() const {
        return 1;
    }

    Expected<std::string> run(Session *sess) final {
        const std::vector<std::string>& args = sess->getArgs();

        const std::string& key = args[1];
        uint64_t ttl = 0;

        auto rv = Command::expireKeyIfNeeded(sess, key, RecordType::RT_KV);
        if (rv.status().code() != ErrorCodes::ERR_OK &&
            rv.status().code() != ErrorCodes::ERR_EXPIRED &&
            rv.status().code() != ErrorCodes::ERR_NOTFOUND) {
            return rv.status();
        }

        if (rv.ok()) {
            if (!redis_port::isHLLObject(rv.value().getValue())) {
                return{ ErrorCodes::ERR_WRONG_TYPE,
                    "-WRONGTYPE Key is not a valid HyperLogLog string value.\r\n" };    // NOLINT
            }
            ttl = rv.value().getTtl();
        }

        SessionCtx *pCtx = sess->getCtx();
        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key,
            mgl::LockMode::LOCK_X);
        if (!expdb.ok()) {
            return expdb.status();
        }

        // TODO(comboqiu): the recordValue get from Command::expireKeyIfNeeded()
        // is not reliable for write operation. Because the lock released after
        // expireKeyIfNeeded().
        // Maybe we should LOCK_X before expireKeyIfNeeded()
        // For LOCK_S, we should discuss later
        auto hpll = std::make_unique<HPLLObject>();
        if (rv.ok()) {
            auto tmp = std::make_unique<HPLLObject>(rv.value().getValue());
            hpll = std::move(tmp);
        }

        size_t updated = 0;
        for (size_t j = 2; j < args.size(); j ++) {
            auto& subkey = args[j];
            int retval = hpll->add(subkey);
            switch (retval) {
            case 1:
                updated++;
                break;
            case -1:
                return{ ErrorCodes::ERR_INVALID_HLL, "" };
            }
        }

        if (updated) {
            PStore kvstore = expdb.value().store;
            auto ptxn = kvstore->createTransaction();
            if (!ptxn.ok()) {
                return ptxn.status();
            }
            std::unique_ptr<Transaction> txn = std::move(ptxn.value());

            RecordKey rk(expdb.value().chunkId, pCtx->getDbId(),
                    RecordType::RT_KV, key, "");
            RecordValue value(hpll->encode(), RecordType::RT_KV, ttl, rv);

            Status s = kvstore->setKV(rk, value, txn.get());
            if (!s.ok()) {
                return s;
            }

            auto c = txn->commit();
            if (!c.ok()) {
                return c.status();
            }
        }
        return updated ? Command::fmtOne() : Command::fmtZero();
    }
} pfAddCmd;

class PfCountCommand : public Command {
 public:
    PfCountCommand()
        :Command("pfcount") {
    }

    ssize_t arity() const {
        return -2;
    }

    int32_t firstkey() const {
        return 1;
    }

    int32_t lastkey() const {
        return 1;
    }

    int32_t keystep() const {
        return 1;
    }

    Expected<std::string> run(Session *sess) final {
        const std::vector<std::string>& args = sess->getArgs();

        /* Case 1: multi-key keys, cardinality of the union.
        *
        * When multiple keys are specified, PFCOUNT actually computes
        * the cardinality of the merge of the N HLLs specified. */
        if (args.size() > 2) {
            auto hpll = std::make_unique<HPLLObject>(HLL_RAW);

            for (size_t j = 1; j < args.size(); j++) {
                auto& key = args[j];
                auto rv = Command::expireKeyIfNeeded(sess, key,
                                            RecordType::RT_KV);
                if (rv.status().code() != ErrorCodes::ERR_OK &&
                    rv.status().code() != ErrorCodes::ERR_EXPIRED &&
                    rv.status().code() != ErrorCodes::ERR_NOTFOUND) {
                    return rv.status();
                }

                if (rv.ok()) {
                    if (!redis_port::isHLLObject(rv.value().getValue())) {
                        return{ ErrorCodes::ERR_INVALID_HLL, "" };
                    }
                } else {
                    /* Assume empty HLL for non existing var.*/
                    continue;
                }

                auto keyHpll = std::make_unique<HPLLObject>(rv.value().getValue());  // NOLINT
                if (hpll->merge(keyHpll.get()) == -1) {
                    return{ ErrorCodes::ERR_INVALID_HLL, "" };
                }
            }

            auto count = hpll->getHllCount();
            return Command::fmtLongLong(count);
        }

        /* Case 2: cardinality of the single HLL.
        *
        * The user specified a single key. Either return the cached value
        * or compute one and update the cache. */
        const std::string& key = args[1];
        auto rv = Command::expireKeyIfNeeded(sess, key, RecordType::RT_KV);
        if (rv.status().code() != ErrorCodes::ERR_OK &&
            rv.status().code() != ErrorCodes::ERR_EXPIRED &&
            rv.status().code() != ErrorCodes::ERR_NOTFOUND) {
            return rv.status();
        }

        if (rv.ok()) {
            if (!redis_port::isHLLObject(rv.value().getValue())) {
                return{ ErrorCodes::ERR_INVALID_HLL, "" };
            }
        } else {
            /* No key? Cardinality is zero since no element was added, otherwise
            * we would have a key as HLLADD creates it as a side effect. */
            return Command::fmtZero();
        }

        auto hpll = std::make_unique<HPLLObject>(rv.value().getValue());

        // NOTE(vinchen): pfcount should be a read only command,
        // so here it should not use hpll->getHllCountFast()
        auto count = hpll->getHllCount();
        if (count == (uint64_t)-1) {
            return{ ErrorCodes::ERR_INVALID_HLL, "" };
        }

        return Command::fmtLongLong(count);
    }
} pfcountCmd;

class PfMergeCommand : public Command {
 public:
    PfMergeCommand()
        :Command("pfmerge") {
    }

    ssize_t arity() const {
        return -2;
    }

    int32_t firstkey() const {
        return 1;
    }

    int32_t lastkey() const {
        return 1;
    }

    int32_t keystep() const {
        return 1;
    }

    Expected<std::string> run(Session *sess) final {
        const std::vector<std::string>& args = sess->getArgs();

        bool useDense = false;
        auto hpll = std::make_unique<HPLLObject>(HLL_RAW);
        uint64_t ttl = 0;
        // TODO(comboqiu): In fact, it should lock first.
        for (size_t j = 1; j < args.size(); j++) {
            auto& okey = args[j];
            auto rv = Command::expireKeyIfNeeded(sess, okey, RecordType::RT_KV);
            if (rv.status().code() != ErrorCodes::ERR_OK &&
                rv.status().code() != ErrorCodes::ERR_EXPIRED &&
                rv.status().code() != ErrorCodes::ERR_NOTFOUND) {
                return rv.status();
            }

            if (rv.ok()) {
                if (!redis_port::isHLLObject(rv.value().getValue())) {
                    return{ ErrorCodes::ERR_INVALID_HLL, "" };
                }
                if (j == 1) {
                    ttl = rv.value().getTtl();
                }
            } else {
                /* Assume empty HLL for non existing var.*/
                continue;
            }

            auto keyHpll = std::make_unique<HPLLObject>(rv.value().getValue());
            if (keyHpll->getHdrEncoding() == HLL_DENSE) {
                useDense = true;
            }
            if (hpll->merge(keyHpll.get()) == -1) {
                return{ ErrorCodes::ERR_INVALID_HLL, "" };
            }
        }

        const std::string& key = args[1];

        /* Convert the destination object to dense representation if at least
        * one of the inputs was dense. */
        auto result = std::make_unique<HPLLObject>(useDense ? HLL_DENSE : HLL_SPARSE);   // NOLINT
        if (result->updateByRawHpll(hpll.get()) == -1) {
            return{ ErrorCodes::ERR_INVALID_HLL, "" };
        }

        SessionCtx *pCtx = sess->getCtx();
        auto server = sess->getServerEntry();
        // TODO(comboqiu): should lock first
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key,
            mgl::LockMode::LOCK_X);
        if (!expdb.ok()) {
            return expdb.status();
        }

        PStore kvstore = expdb.value().store;
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());

        RecordKey rk(expdb.value().chunkId, pCtx->getDbId(),
            RecordType::RT_KV, key, "");
        RecordValue value(result->encode(), RecordType::RT_KV, ttl);

        Status s = kvstore->setKV(rk, value, txn.get());
        if (!s.ok()) {
            return s;
        }

        auto c = txn->commit();
        if (!c.ok()) {
            return c.status();
        }

        return Command::fmtOK();
    }
} pfmergeCmd;

}  // namespace tendisplus
