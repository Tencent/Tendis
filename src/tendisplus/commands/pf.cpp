// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

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
  explicit MyAllocator(size_t N = 1024)
    : data(0), p(0), firstAlignedPtr(0), leftSize(N), allocSize(N) {
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
    INVARIANT_D(0);
    return nullptr;
  }

  template <typename T>
  T* aligned_alloc(const char* ptr, size_t size, std::size_t a = alignof(T)) {
    // check whether the left size is enough
    if (leftSize < size) {
      return nullptr;
    }

    T* val = aligned_alloc<T>(a);
    if (!val) {
      INVARIANT_D(0);
      return val;
    }

    // check the left size after
    size_t left = (size_t)(data + allocSize - reinterpret_cast<char*>(val));
    if (left < size) {
      INVARIANT_D(0);
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
    INVARIANT(firstAlignedPtr != NULL);
    return reinterpret_cast<T*>(firstAlignedPtr);
  }

  template <typename T>
  void resize(size_t n, std::size_t a = alignof(T)) {
    if (allocSize < n) {
      char* newdata = reinterpret_cast<char*>(calloc(n, sizeof(char)));

      // s should be initialized to be n
      size_t s = n;
      void* tmpp = newdata;
      if (std::align(a, sizeof(T), tmpp, s)) {
        INVARIANT(reinterpret_cast<char*>(tmpp) == newdata);
        firstAlignedPtr = reinterpret_cast<char*>(tmpp);
      }

      memcpy(newdata, data, allocSize);

      free(data);
      data = newdata;
      p = data + (allocSize - leftSize);

      leftSize += n - allocSize;
      allocSize = n;
    }
  }
};

class HPLLObject {
 public:
  explicit HPLLObject(int type = HLL_SPARSE);
  explicit HPLLObject(const std::string& v);
  HPLLObject(const HPLLObject&) = delete;
  HPLLObject(HPLLObject&&) = default;

  int add(const std::string& subkey);
  int add(const char* data, size_t size);
  uint64_t getHllCount() const;
  // Note(vinchen): it is not const;
  uint64_t getHllCountFast();
  std::string encode() const;
  int merge(const HPLLObject* hpll);
  int getHdrEncoding() const {
    return _hdr->encoding;
  }
  int updateByRawHpll(const HPLLObject* rawHpll);
  int hllSparseToDense();
  const redis_port::hllhdr* getHdr() const {
    return _hdr;
  }
  size_t getHdrSize() const {
    return _hdrSize;
  }

 private:
  void hllInvalidateCache();

 private:
  // NOTE(vinchen): _hdr is a hllhdr and followed an array, it's memory
  // should be alloced by an aligned allocator.
  // But in fact alignof(hllhdr) = 1, and aligned_alloc maybe more safe.
  redis_port::hllhdr* _hdr;
  size_t _hdrSize;
  std::unique_ptr<MyAllocator> _buf;
};

HPLLObject::HPLLObject(const std::string& v) {
#ifdef TENDIS_DEBUG
  INVARIANT_D(redis_port::isHLLObject(v.c_str(), v.size()));
#endif

  _buf = std::make_unique<MyAllocator>(v.size() + 128);
  INVARIANT(_buf->allocSize >= v.size());
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
      // _buf = std::make_unique<MyAllocator>(HLL_MAX_SIZE);
      _buf = std::make_unique<MyAllocator>();
      _hdr = _buf->aligned_alloc<redis_port::hllhdr>();
      _hdr = redis_port::createHLLObject(
        reinterpret_cast<const char*>(_hdr), _buf->allocSize, &_hdrSize);
      break;
    case HLL_DENSE: {
      auto tmp = std::make_unique<MyAllocator>();
      auto tmphdr = tmp->aligned_alloc<redis_port::hllhdr>();
      size_t tmphdrSize = 0;
      tmphdr = redis_port::createHLLObject(
        reinterpret_cast<const char*>(tmphdr), tmp->allocSize, &tmphdrSize);
      _buf = std::make_unique<MyAllocator>(HLL_DENSE_SIZE + HLL_HDR_SIZE);
      _hdr = _buf->aligned_alloc<redis_port::hllhdr>();
      ret = redis_port::hllSparseToDense(
        tmphdr, tmphdrSize, _hdr, &_hdrSize, _buf->allocSize);
      INVARIANT(_hdrSize == HLL_DENSE_SIZE);
      INVARIANT(ret == C_OK);
    } break;
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
    int ret = redis_port::hllAdd(_hdr,
                                 &_hdrSize,
                                 _buf->allocSize,
                                 (unsigned char*)subkey.c_str(),
                                 subkey.size());
    switch (ret) {
      case 1:
        // changed
        hllInvalidateCache();
      case 0:
        // no change
      case -1:
        // error
        return ret;

      case HLL_ERROR_PROMOTE: {
        ret = hllSparseToDense();
        if (ret == -1) {
          return ret;
        }
        break;
      }

      case HLL_ERROR_MEMORY:
        _buf->resize<redis_port::hllhdr>(_buf->allocSize + 128);
        _hdr = _buf->getFirstAlignedAddr<redis_port::hllhdr>();
#ifdef TENDIS_DEBUG
        INVARIANT_D(redis_port::isHLLObject((const char*)_hdr, _hdrSize));
#endif
        break;

      default:
        INVARIANT_D(0);
        return -1;
    }
  }

  return 0;
}

int HPLLObject::add(const char* data, size_t size) {
  std::string s(data, size);

  return add(s);
}

int HPLLObject::hllSparseToDense() {
  auto m = std::make_unique<MyAllocator>(HLL_DENSE_SIZE + HLL_HDR_SIZE);
  auto newhdr = m->aligned_alloc<redis_port::hllhdr>();
  int ret = redis_port::hllSparseToDense(
    _hdr, _hdrSize, newhdr, &_hdrSize, m->allocSize);
  if (ret != C_OK) {
    return -1;
  }

  _buf = std::move(m);
  _hdr = newhdr;

  return 0;
}

// return:
// -1 : something wrong
// 1 : success
int HPLLObject::merge(const HPLLObject* hpll) {
  INVARIANT_D(getHdrEncoding() == HLL_RAW);
  INVARIANT_D(hpll->getHdrEncoding() != HLL_RAW);
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
  INVARIANT_D(0);
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

  int ret = redis_port::hllUpdateByRawHpll(
    _hdr, &_hdrSize, _buf->allocSize, rawHpll->_hdr);
  if (ret == C_ERR) {
    return -1;
  }
  hllInvalidateCache();

  return 1;
}


class PfAddCommand : public Command {
 public:
  PfAddCommand() : Command("pfadd", "wmF") {}

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

  Expected<std::string> run(Session* sess) final {
    const std::vector<std::string>& args = sess->getArgs();

    const std::string& key = args[1];
    uint64_t ttl = 0;

    // NOTE(vinchen): LOCK before expired
    SessionCtx* pCtx = sess->getCtx();
    auto server = sess->getServerEntry();
    auto expdb = server->getSegmentMgr()->getDbWithKeyLock(
      sess, key, mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
      return expdb.status();
    }

    auto rv = Command::expireKeyIfNeeded(sess, key, RecordType::RT_KV);
    if (rv.status().code() != ErrorCodes::ERR_OK &&
        rv.status().code() != ErrorCodes::ERR_EXPIRED &&
        rv.status().code() != ErrorCodes::ERR_NOTFOUND) {
      return rv.status();
    }

    if (rv.ok()) {
      if (!redis_port::isHLLObject(rv.value().getValue().c_str(),
                                   rv.value().getValue().size())) {
        return {ErrorCodes::ERR_WRONG_TYPE,
                "-WRONGTYPE Key is not a valid HyperLogLog string "
                "value.\r\n"};  // NOLINT
      }
      ttl = rv.value().getTtl();
    }

    size_t updated = 0;
    auto hpll = std::make_unique<HPLLObject>();
    if (rv.ok()) {
      auto tmp = std::make_unique<HPLLObject>(rv.value().getValue());
      hpll = std::move(tmp);
    } else {
      // NOTE(vinchen): if hpll is not exists, it would always create a
      // new one
      updated++;
    }

    for (size_t j = 2; j < args.size(); j++) {
      auto& subkey = args[j];
      int retval = hpll->add(subkey);
      switch (retval) {
        case 1:
          updated++;
          break;
        case -1:
          return {ErrorCodes::ERR_INVALID_HLL, ""};
      }
    }

    if (updated) {
      PStore kvstore = expdb.value().store;
      auto ptxn = sess->getCtx()->createTransaction(kvstore);
      if (!ptxn.ok()) {
        return ptxn.status();
      }

      RecordKey rk(
        expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_KV, key, "");
      RecordValue value(
        hpll->encode(), RecordType::RT_KV, pCtx->getVersionEP(), ttl, rv);

      Status s = kvstore->setKV(rk, value, ptxn.value());
      if (!s.ok()) {
        return s;
      }

      auto c = sess->getCtx()->commitTransaction(ptxn.value());
      if (!c.ok()) {
        return c.status();
      }
    }
    return updated ? Command::fmtOne() : Command::fmtZero();
  }
} pfAddCmd;

class PfCountCommand : public Command {
 public:
  PfCountCommand() : Command("pfcount", "r") {}

  ssize_t arity() const {
    return -2;
  }

  int32_t firstkey() const {
    return 1;
  }

  int32_t lastkey() const {
    return -1;
  }

  int32_t keystep() const {
    return 1;
  }

  Expected<std::string> run(Session* sess) final {
    const std::vector<std::string>& args = sess->getArgs();

    auto server = sess->getServerEntry();
    auto index = getKeysFromCommand(args);

    auto locklist = server->getSegmentMgr()->getAllKeysLocked(
      sess, args, index, Command::RdLock());
    if (!locklist.ok()) {
      return locklist.status();
    }
    /* Case 1: multi-key keys, cardinality of the union.
     *
     * When multiple keys are specified, PFCOUNT actually computes
     * the cardinality of the merge of the N HLLs specified. */
    if (args.size() > 2) {
      auto hpll = std::make_unique<HPLLObject>(HLL_RAW);

      for (size_t j = 1; j < args.size(); j++) {
        auto& key = args[j];
        auto rv = Command::expireKeyIfNeeded(sess, key, RecordType::RT_KV);
        if (rv.status().code() != ErrorCodes::ERR_OK &&
            rv.status().code() != ErrorCodes::ERR_EXPIRED &&
            rv.status().code() != ErrorCodes::ERR_NOTFOUND) {
          return rv.status();
        }

        if (rv.ok()) {
          if (!redis_port::isHLLObject(rv.value().getValue().c_str(),
                                       rv.value().getValue().size())) {
            return {ErrorCodes::ERR_WRONG_TYPE, ""};
          }
        } else {
          /* Assume empty HLL for non existing var.*/
          continue;
        }

        auto keyHpll =
          std::make_unique<HPLLObject>(rv.value().getValue());  // NOLINT
        if (hpll->merge(keyHpll.get()) == -1) {
          return {ErrorCodes::ERR_INVALID_HLL, ""};
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
      if (!redis_port::isHLLObject(rv.value().getValue().c_str(),
                                   rv.value().getValue().size())) {
        return {ErrorCodes::ERR_WRONG_TYPE, ""};
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
      return {ErrorCodes::ERR_INVALID_HLL, ""};
    }

    return Command::fmtLongLong(count);
  }
} pfcountCmd;

class PfMergeCommand : public Command {
 public:
  PfMergeCommand() : Command("pfmerge", "wm") {}

  ssize_t arity() const {
    return -2;
  }

  int32_t firstkey() const {
    return 1;
  }

  int32_t lastkey() const {
    return -1;
  }

  int32_t keystep() const {
    return 1;
  }

  Expected<std::string> run(Session* sess) final {
    const std::vector<std::string>& args = sess->getArgs();

    auto server = sess->getServerEntry();
    SessionCtx* pCtx = sess->getCtx();
    auto index = getKeysFromCommand(args);

    auto locklist = server->getSegmentMgr()->getAllKeysLocked(
      sess, args, index, mgl::LockMode::LOCK_X);
    if (!locklist.ok()) {
      return locklist.status();
    }

    bool useDense = false;
    auto hpll = std::make_unique<HPLLObject>(HLL_RAW);
    uint64_t ttl = 0;
    for (size_t j = 1; j < args.size(); j++) {
      auto& okey = args[j];
      auto rv = Command::expireKeyIfNeeded(sess, okey, RecordType::RT_KV);
      if (rv.status().code() != ErrorCodes::ERR_OK &&
          rv.status().code() != ErrorCodes::ERR_EXPIRED &&
          rv.status().code() != ErrorCodes::ERR_NOTFOUND) {
        return rv.status();
      }

      if (rv.ok()) {
        if (!redis_port::isHLLObject(rv.value().getValue().c_str(),
                                     rv.value().getValue().size())) {
          return {ErrorCodes::ERR_WRONG_TYPE, ""};
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
        return {ErrorCodes::ERR_INVALID_HLL, ""};
      }
    }

    const std::string& key = args[1];

    /* Convert the destination object to dense representation if at least
     * one of the inputs was dense. */
    auto result = std::make_unique<HPLLObject>(
      useDense ? HLL_DENSE : HLL_SPARSE);  // NOLINT
    if (result->updateByRawHpll(hpll.get()) == -1) {
      return {ErrorCodes::ERR_INVALID_HLL, ""};
    }

    auto expdb = server->getSegmentMgr()->getDbHasLocked(sess, key);
    if (!expdb.ok()) {
      return expdb.status();
    }

    PStore kvstore = expdb.value().store;
    auto ptxn = sess->getCtx()->createTransaction(kvstore);
    if (!ptxn.ok()) {
      return ptxn.status();
    }

    RecordKey rk(
      expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_KV, key, "");
    RecordValue value(
      result->encode(), RecordType::RT_KV, pCtx->getVersionEP(), ttl);

    Status s = kvstore->setKV(rk, value, ptxn.value());

    if (!s.ok()) {
      return s;
    }

    auto c = sess->getCtx()->commitTransaction(ptxn.value());
    if (!c.ok()) {
      return c.status();
    }

    return Command::fmtOK();
  }
} pfmergeCmd;

/* PFSELFTEST
 * This command performs a self-test of the HLL registers implementation.
 * Something that is not easy to test from within the outside. */
#define HLL_TEST_CYCLES 1000
class PfSelfTestCommand : public Command {
 public:
  PfSelfTestCommand() : Command("pfselftest", "as") {}

  ssize_t arity() const {
    return 1;
  }

  int32_t firstkey() const {
    return 0;
  }

  int32_t lastkey() const {
    return 0;
  }

  int32_t keystep() const {
    return 0;
  }

  Expected<std::string> run(Session* sess) final {
    unsigned int j, i;
    // sds bitcounters = sdsnewlen(NULL, HLL_DENSE_SIZE);
    auto keyHpll = std::make_unique<HPLLObject>(HLL_DENSE);
    auto hdr = keyHpll->getHdr();
    // robj *o = NULL;
    uint8_t bytecounters[HLL_REGISTERS];

    /* Test 1: access registers.
     * The test is conceived to test that the different counters of our data
     * structure are accessible and that setting their values both result in
     * the correct value to be retained and not affect adjacent values. */
    for (j = 0; j < HLL_TEST_CYCLES; j++) {
      /* Set the HLL counters and an array of unsigned byes of the
       * same size to the same set of random values. */
      for (i = 0; i < HLL_REGISTERS; i++) {
        uint32_t seed = nsSinceEpoch();
        unsigned int r = rand_r(&seed) & HLL_REGISTER_MAX;

        bytecounters[i] = r;
        HLL_DENSE_SET_REGISTER(hdr->registers, i, r);
      }
      /* Check that we are able to retrieve the same values. */
      for (i = 0; i < HLL_REGISTERS; i++) {
        unsigned int val;

        HLL_DENSE_GET_REGISTER(val, hdr->registers, i);
        if (val != bytecounters[i]) {
          char buf[256];
          snprintf(buf,
                   sizeof(buf),
                   "TESTFAILED Register %d should be %d but is %d",
                   i,
                   static_cast<int>(bytecounters[i]),
                   static_cast<int>(val));

          return {ErrorCodes::ERR_INVALID_HLL, buf};
        }
      }
    }

    /* Test 2: approximation error.
     * The test adds unique elements and check that the estimated value
     * is always reasonable bounds.
     *
     * We check that the error is smaller than a few times than the expected
     * standard error, to make it very unlikely for the test to fail because
     * of a "bad" run.
     *
     * The test is performed with both dense and sparse HLLs at the same
     * time also verifying that the computed cardinality is the same. */
    memset((void*)hdr->registers, 0, HLL_DENSE_SIZE - HLL_HDR_SIZE);  // NOLINT
    // o = createHLLObject();
    auto o = std::make_unique<HPLLObject>();
    double relerr = 1.04 / sqrt(HLL_REGISTERS);
    int64_t checkpoint = 1;
    uint64_t seed = (uint64_t)rand() | (uint64_t)rand() << 32;
    uint64_t ele;
    for (j = 1; j <= 10000000; j++) {
      ele = j ^ seed;
      keyHpll->add(reinterpret_cast<char*>(&ele), sizeof(ele));
      // hllDenseAdd(hdr->registers, (unsigned char*)&ele, sizeof(ele));
      o->add(reinterpret_cast<char*>(&ele), sizeof(ele));
      // hllAdd(o, (unsigned char*)&ele, sizeof(ele));

      /* Make sure that for small cardinalities we use sparse
       * encoding. */
      if (j == checkpoint &&
          j < CONFIG_DEFAULT_HLL_SPARSE_MAX_BYTES / 2) {  // NOLINT
        if (o->getHdrEncoding() != HLL_SPARSE) {
          std::stringstream ss;
          ss << "TESTFAILED sparse encoding not used:"
             << "j=" << j << " encoding=" << o->getHdrEncoding();

          return {ErrorCodes::ERR_INVALID_HLL, ss.str()};
        }
      }

      /* Check that dense and sparse representations agree. */
      if (j == checkpoint && keyHpll->getHllCount() != o->getHllCount()) {
        return {ErrorCodes::ERR_INVALID_HLL,
                "TESTFAILED dense/sparse disagree"};
      }

      /* Check error. */
      if (j == checkpoint) {
        int64_t abserr = checkpoint - (int64_t)keyHpll->getHllCount();
        uint64_t maxerr = ceil(relerr * 6 * checkpoint);

        /* Adjust the max error we expect for cardinality 10
         * since from time to time it is statistically likely to get
         * much higher error due to collision, resulting into a false
         * positive. */
        if (j == 10)
          maxerr = 1;

        if (abserr < 0)
          abserr = -abserr;
        if (abserr > (int64_t)maxerr) {
          char buf[256];
          snprintf(buf,
                   sizeof(buf),
                   "TESTFAILED Too big error. card:%lu abserr:%lu",
                   (uint64_t)checkpoint,
                   (uint64_t)abserr);

          return {ErrorCodes::ERR_INVALID_HLL, buf};
        }
        checkpoint *= 10;
      }
    }

    /* Success! */
    return Command::fmtOK();
  }
} pfselftestCmd;

class PfDebugCommand : public Command {
 public:
  PfDebugCommand() : Command("pfdebug", "w") {}

  ssize_t arity() const {
    return -3;
  }

  int32_t firstkey() const {
    return 2;
  }

  int32_t lastkey() const {
    return 2;
  }

  int32_t keystep() const {
    return 1;
  }

  ///* PFDEBUG <subcommand> <key> ... args ..
  Expected<std::string> run(Session* sess) final {
    const std::vector<std::string>& args = sess->getArgs();

    auto& key = args[2];
    SessionCtx* pCtx = sess->getCtx();
    auto server = sess->getServerEntry();
    auto expdb = server->getSegmentMgr()->getDbWithKeyLock(
      sess, key, mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
      return expdb.status();
    }

    uint64_t ttl = 0;
    bool updated = false;
    auto rv = Command::expireKeyIfNeeded(sess, key, RecordType::RT_KV);
    if (!rv.ok()) {
      if (rv.status().code() == ErrorCodes::ERR_NOTFOUND ||
          rv.status().code() == ErrorCodes::ERR_EXPIRED) {
        return {ErrorCodes::ERR_INVALID_HLL,
                "The specified key does not exist"};
      }

      return rv.status();
    }

    if (!redis_port::isHLLObject(rv.value().getValue().c_str(),
                                 rv.value().getValue().size())) {
      return {ErrorCodes::ERR_WRONG_TYPE, ""};
    }
    ttl = rv.value().getTtl();

    auto keyHpll =
      std::make_unique<HPLLObject>(rv.value().getValue());  // NOLINT

    std::stringstream ss;
    auto cmd = toLower(args[1]);
    if (cmd == "getreg") {
      if (args.size() != 3) {
        return {ErrorCodes::ERR_WRONG_ARGS_SIZE, ""};
      }

      if (keyHpll->getHdrEncoding() == HLL_SPARSE) {
        if (keyHpll->hllSparseToDense() == -1) {
          return {ErrorCodes::ERR_INVALID_HLL, ""};
        }
        updated = true;
      }

      Command::fmtMultiBulkLen(ss, HLL_REGISTERS);
      for (size_t j = 0; j < HLL_REGISTERS; j++) {
        uint8_t val;

        HLL_DENSE_GET_REGISTER(val, keyHpll->getHdr()->registers, j);
        ss << Command::fmtLongLong(val);
      }

    } else if (cmd == "decode") {
      if (args.size() != 3) {
        return {ErrorCodes::ERR_WRONG_ARGS_SIZE, ""};
      }

      if (keyHpll->getHdrEncoding() != HLL_SPARSE) {
        return {ErrorCodes::ERR_WRONG_TYPE, "HLL encoding is not sparse"};
      }

      auto p = reinterpret_cast<const unsigned char*>(keyHpll->getHdr());
      auto end = p + keyHpll->getHdrSize();

      p += HLL_HDR_SIZE;
      std::stringstream decoded;
      char buf[256];
      while (p < end) {
        int runlen, regval;

        if (HLL_SPARSE_IS_ZERO(p)) {
          runlen = HLL_SPARSE_ZERO_LEN(p);
          p++;
          snprintf(buf, sizeof(buf), "z:%d ", runlen);
          decoded << buf;
        } else if (HLL_SPARSE_IS_XZERO(p)) {
          runlen = HLL_SPARSE_XZERO_LEN(p);
          p += 2;
          snprintf(buf, sizeof(buf), "Z:%d ", runlen);
          decoded << buf;
        } else {
          runlen = HLL_SPARSE_VAL_LEN(p);
          regval = HLL_SPARSE_VAL_VALUE(p);
          p++;
          snprintf(buf, sizeof(buf), "v:%d,%d ", regval, runlen);
          decoded << buf;
        }
      }
      auto s = decoded.str();
      sdstrim(s, " ");

      ss << Command::fmtBulk(s);
    } else if (cmd == "encoding") {
      const char* encodingstr[2] = {"dense", "sparse"};
      if (args.size() != 3) {
        return {ErrorCodes::ERR_WRONG_ARGS_SIZE, ""};
      }

      ss << Command::fmtStatus(encodingstr[keyHpll->getHdrEncoding()]);
    } else if (cmd == "todense") {
      if (args.size() != 3) {
        return {ErrorCodes::ERR_WRONG_ARGS_SIZE, ""};
      }

      if (keyHpll->getHdrEncoding() == HLL_SPARSE) {
        if (keyHpll->hllSparseToDense() == -1) {
          return {ErrorCodes::ERR_INVALID_HLL, ""};
        }

        updated = true;
      }

      updated ? ss << Command::fmtOne() : ss << Command::fmtZero();
    } else {
      ss << "Unknown PFDEBUG subcommand '" << cmd << "'";

      return {ErrorCodes::ERR_PARSEOPT, ss.str()};
    }

    if (updated) {
      PStore kvstore = expdb.value().store;
      auto ptxn = sess->getCtx()->createTransaction(kvstore);
      if (!ptxn.ok()) {
        return ptxn.status();
      }

      RecordKey rk(
        expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_KV, key, "");
      RecordValue value(
        keyHpll->encode(), RecordType::RT_KV, pCtx->getVersionEP(), ttl, rv);

      Status s = kvstore->setKV(rk, value, ptxn.value());
      if (!s.ok()) {
        return s;
      }

      auto c = sess->getCtx()->commitTransaction(ptxn.value());
      if (!c.ok()) {
        return c.status();
      }
    }

    return ss.str();
  }
} pfdebugCmd;

}  // namespace tendisplus
