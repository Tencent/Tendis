// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <algorithm>
#include <cctype>
#include <clocale>
#include <cmath>
#include <memory>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "tendisplus/commands/command.h"
#include "tendisplus/commands/dump.h"
#include "tendisplus/storage/varint.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/redis_port.h"

namespace tendisplus {

#define MAKE_CONTEXT(key)                                               \
  auto pctx = sess->getCtx();                                           \
  INVARIANT(pctx != nullptr);                                           \
  auto edb = sess->getServerEntry() -> getSegmentMgr()                  \
               -> getDbWithKeyLock(sess, (key), mgl::LockMode::LOCK_X); \
  RET_IF_ERR_EXPECTED(edb);                                             \
  auto kvstore = edb.value().store;                                     \
  auto eptxn = pctx->createTransaction(kvstore);                        \
  RET_IF_ERR_EXPECTED(eptxn);

#define GET_POS(param)                                       \
  auto epos = tendisplus::stoul(param);                      \
  RET_IF_ERR_EXPECTED(epos);                                 \
  uint64_t pos = epos.value();                               \
  if ((pos >> 3) >= CONFIG_DEFAULT_PROTO_MAX_BULK_LEN) {     \
    return {ErrorCodes::ERR_PARSEOPT,                        \
            "bit offset is not an integer or out of range"}; \
  }

#define GET_BIT(param, errmsg)                           \
  int8_t bit = param == "1" ? 1 : param == "0" ? 0 : -1; \
  if (bit == -1) {                                       \
    return {ErrorCodes::ERR_PARSEOPT, errmsg};           \
  }

#define GET_START_END(param)                              \
  int64_t start = 0, end = len - 1;                       \
  bool endGiven = false;                                  \
  if (args.size() == param - 1 || args.size() == param) { \
    auto estart = ::tendisplus::stoll(args[param - 2]);   \
    RET_IF_ERR_EXPECTED(estart);                          \
    start = estart.value();                               \
    if (args.size() == param) {                           \
      auto eend = ::tendisplus::stoll(args[param - 1]);   \
      RET_IF_ERR_EXPECTED(eend);                          \
      end = eend.value();                                 \
      endGiven = true;                                    \
    }                                                     \
    start += start < 0 ? len : 0;                         \
    end += end < 0 ? len : 0;                             \
    start = start < 0 ? 0 : start;                        \
    end = end < 0 ? 0 : end;                              \
    end = end >= len ? len - 1 : end;                     \
  }

/**
 * Big-endian encoding, the coding order is consistent with the numerical order
 */
std::string fragmentIdEncode(uint64_t fragmentId) {
  char buf[sizeof(uint64_t)] = {0};
  int64Encode(buf, fragmentId);

  return std::string(buf, sizeof(uint64_t));
}

Expected<uint64_t> fragmentIdDecode(const std::string& input) {
  if (input.size() != sizeof(uint64_t)) {
    return {ErrorCodes::ERR_INTERNAL, "invalid fragmentId"};
  }
  INVARIANT_D(input.size() == sizeof(uint64_t));
  return int64Decode(input.c_str());
}

Expected<int64_t> getBitOrOffset(const std::string& input, bool isBit) {
  static std::string bitError =
    "-ERR bit is not an integer or out of range\r\n";
  static std::string offsetError =
    "-ERR bit offset is not an integer or out of range\r\n";

  std::string error = isBit ? bitError : offsetError;

  if (input.find("-") != std::string::npos) {
    return {ErrorCodes::ERR_PARSEOPT, error};
  }

  int64_t val = -1;
  if (isBit) {
    auto ebit = tendisplus::stoul(input);
    RET_IF_ERR_EXPECTED(ebit);
    val = ebit.value();
    if (val != 0 && val != 1) {
      return {ErrorCodes::ERR_PARSEOPT, error};
    }
  } else {
    auto epos = tendisplus::stoul(input);
    RET_IF_ERR_EXPECTED(epos);
    val = epos.value();
    if ((val >> 3) >= CONFIG_DEFAULT_PROTO_MAX_BULK_LEN) {
      return {ErrorCodes::ERR_PARSEOPT, error};
    }
  }

  return val;
}

Status setBitmapEle(Transaction* txn,
                    PStore kvstore,
                    uint32_t chunkId,
                    uint32_t dbid,
                    const std::string& key,
                    uint64_t fragmentId,
                    const std::string& value) {
  // construct and set new value, RecordType::RT_TBITMAP_ELE sorted by
  // fragmentId
  RecordKey subRk(chunkId,
                  dbid,
                  RecordType::RT_TBITMAP_ELE,
                  key,
                  fragmentIdEncode(fragmentId));

  // remove tailing zeros
  int32_t i = -1;
  for (i = value.size() - 1; i >= 0; i--) {
    if (value[i] != 0) {
      break;
    }
  }

  std::string newValue;
  if (i >= 0) {
    newValue = value.substr(0, i + 1);
  }

  RecordValue subRv(newValue, RecordType::RT_TBITMAP_ELE, -1);
  auto s = kvstore->setKV(subRk, subRv, txn);

  return s;
}

Expected<std::string> getTBitmapString(uint32_t chunkId,
                                       uint32_t dbid,
                                       const TBitMapMetaValue& meta,
                                       const std::string& key,
                                       Transaction* txn) {
  uint32_t eleCnt = 1;
  std::string result(meta.byteAmount(), 0);

  // first fragment
  result.replace(0, meta.firstFragment().size(), meta.firstFragment());

  // construct the first subRk
  RecordKey rk(chunkId, dbid, RecordType::RT_TBITMAP_ELE, key, "");
  auto cursor = txn->createDataCursor();
  cursor->seek(rk.prefixPk());

  while (true) {
    auto ercd = cursor->next();
    if (ercd.status().code() == ErrorCodes::ERR_EXHAUST) {
      break;
    }
    RET_IF_ERR_EXPECTED(ercd);

    RecordKey subRk = ercd.value().getRecordKey();
    if (subRk.prefixPk() != rk.prefixPk()) {
      break;
    }

    eleCnt++;
    auto efragId = fragmentIdDecode(subRk.getSecondaryKey());
    RET_IF_ERR_EXPECTED(efragId);
    auto fragId = efragId.value();

    const std::string& value = ercd.value().getRecordValue().getValue();
    result.replace(fragId * meta.fragmentLen(), value.size(), value);
  }
  INVARIANT_D(eleCnt == meta.eleCount());

  return result;
}

Expected<std::string> getBitmap(Session* sess,
                                const std::string& key,
                                uint64_t* maxBitAmount) {
  auto pctx = sess->getCtx();
  INVARIANT(pctx != nullptr);
  // caller has locked all keys
  auto edb = sess->getServerEntry()->getSegmentMgr()->getDbHasLocked(sess, key);
  RET_IF_ERR_EXPECTED(edb);
  auto kvstore = edb.value().store;
  auto eptxn = pctx->createTransaction(kvstore);
  RET_IF_ERR_EXPECTED(eptxn);

  auto emetaRv =
    Command::expireKeyIfNeeded(sess, key, RecordType::RT_TBITMAP_META);
  if (emetaRv.status().code() == ErrorCodes::ERR_EXPIRED ||
      emetaRv.status().code() == ErrorCodes::ERR_NOTFOUND) {
    return std::string();
  } else if (emetaRv.status().code() == ErrorCodes::ERR_WRONG_TYPE) {
    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_KV);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
        rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return std::string();
    } else if (!rv.status().ok()) {
      return rv.status();
    }
    if (rv.value().getValue().size() << 3 > *maxBitAmount) {
      *maxBitAmount = rv.value().getValue().size() << 3;
    }
    return rv.value().getValue();
  } else if (!emetaRv.ok()) {
    return emetaRv.status();
  }

  auto eMeta = TBitMapMetaValue::decode(emetaRv.value().getValue());
  RET_IF_ERR_EXPECTED(eMeta);
  auto meta = std::move(eMeta.value());

  auto result = getTBitmapString(
    edb.value().chunkId, pctx->getDbId(), meta, key, eptxn.value());

  if (meta.bitAmount() > *maxBitAmount) {
    *maxBitAmount = meta.bitAmount();
  }
  return result;
}

class TSetBitCommand : public Command {
 public:
  TSetBitCommand() : Command("tsetbit", "wm") {}

  ssize_t arity() const {
    return 4;
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
    const auto& args = sess->getArgs();

    auto epos = getBitOrOffset(args[2], false);
    RET_IF_ERR_EXPECTED(epos);
    uint64_t pos = epos.value();

    auto ebit = getBitOrOffset(args[3], true);
    RET_IF_ERR_EXPECTED(ebit);
    uint64_t bit = ebit.value();

    const std::string& key = args[1];
    auto pctx = sess->getCtx();
    INVARIANT(pctx != nullptr);
    auto edb = sess->getServerEntry()->getSegmentMgr()->getDbWithKeyLock(
      sess, key, mgl::LockMode::LOCK_X);
    RET_IF_ERR_EXPECTED(edb);
    auto kvstore = edb.value().store;
    auto eptxn = pctx->createTransaction(kvstore);
    RET_IF_ERR_EXPECTED(eptxn);

    // get primary key value, mainly for meta data
    RecordKey metaRk(edb.value().chunkId,
                     pctx->getDbId(),
                     RecordType::RT_TBITMAP_META,
                     key,
                     "");
    auto emetaRv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_TBITMAP_META);
    uint64_t ttl = 0;  // for consttuct new value
    uint64_t fragmentLen =
      sess->getServerEntry()->getParams()->tbitmapFragmentSize;
    TBitMapMetaValue meta(fragmentLen);

    if (emetaRv.ok()) {
      ttl = emetaRv.value().getTtl();
      auto eMeta = TBitMapMetaValue::decode(emetaRv.value().getValue());
      RET_IF_ERR_EXPECTED(eMeta);
      meta = std::move(eMeta.value());
    } else if (emetaRv.status().code() == ErrorCodes::ERR_WRONG_TYPE) {
      auto it = commandMap().find("setbit");
      if (it == commandMap().end()) {
        LOG(FATAL) << "BUG: command:setbit not found!";
      }
      return it->second->run(sess);
    } else if (emetaRv.status().code() != ErrorCodes::ERR_EXPIRED &&
               emetaRv.status().code() != ErrorCodes::ERR_NOTFOUND) {
      return emetaRv.status();
    }

    // calculate partition and inside offset
    uint64_t fragmentId = pos / (8 * meta.fragmentLen());
    uint64_t offset = pos % (8 * meta.fragmentLen());
    std::string bitstr;  // corresponding partition bit string
    bool newElement = false;

    if (fragmentId == 0) {
      bitstr = meta.firstFragment();
    } else {
      // get sub key value
      RecordKey subRk(edb.value().chunkId,
                      pctx->getDbId(),
                      RecordType::RT_TBITMAP_ELE,
                      key,
                      fragmentIdEncode(fragmentId));
      auto esubRv = kvstore->getKV(subRk, eptxn.value());
      if (esubRv.ok()) {
        bitstr = esubRv.value().getValue();
        INVARIANT_D(bitstr.size() <= meta.fragmentLen());
      } else if (esubRv.status().code() != ErrorCodes::ERR_EXPIRED &&
                 esubRv.status().code() != ErrorCodes::ERR_NOTFOUND) {
        return emetaRv.status();
      } else {
        newElement = true;
      }
    }

    // set bit at offset
    uint64_t byte = (offset >> 3);
    if (bitstr.size() < byte + 1) {
      bitstr.resize(byte + 1, 0);
    }

    uint8_t byteval = static_cast<uint8_t>(bitstr[byte]);
    uint8_t p = 7 - (offset & 0x7);
    int before = (byteval >> p) & 1;
    byteval &= ~(1 << p);
    byteval |= ((bit & 0x1) << p);
    bitstr[byte] = byteval;

    if (fragmentId > 0) {
      auto s = setBitmapEle(eptxn.value(),
                            kvstore,
                            edb.value().chunkId,
                            pctx->getDbId(),
                            key,
                            fragmentId,
                            bitstr);
      RET_IF_ERR(s);
    } else {
      meta.setFirstFragment(bitstr);
    }

    if (pos + 1 > meta.bitAmount()) {
      meta.setBitAmount(pos + 1);
    }
    if (before == 0 && bit == 1) {
      meta.setCount(meta.count() + 1);
    }
    if (before == 1 && bit == 0) {
      meta.setCount(meta.count() - 1);
    }

    if (newElement) {
      meta.setEleCount(meta.eleCount() + 1);
    }

    auto s = kvstore->setKV(metaRk,
                            RecordValue(meta.encode(),
                                        RecordType::RT_TBITMAP_META,
                                        pctx->getVersionEP(),
                                        ttl,
                                        emetaRv),
                            eptxn.value());
    RET_IF_ERR(s);

    auto e = pctx->commitTransaction(eptxn.value());
    RET_IF_ERR_EXPECTED(e);

    return before ? Command::fmtOne() : Command::fmtZero();
  }
} tsetbitCmd;

class TGetBitCommand : public Command {
 public:
  TGetBitCommand() : Command("tgetbit", "rF") {}

  ssize_t arity() const {
    return 3;
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
    const auto& args = sess->getArgs();
    const std::string& key = args[1];

    auto epos = getBitOrOffset(args[2], false);
    RET_IF_ERR_EXPECTED(epos);
    uint64_t pos = epos.value();

    auto pctx = sess->getCtx();
    INVARIANT(pctx != nullptr);
    auto edb = sess->getServerEntry()->getSegmentMgr()->getDbWithKeyLock(
      sess, key, RdLock());
    RET_IF_ERR_EXPECTED(edb);
    auto kvstore = edb.value().store;
    auto eptxn = pctx->createTransaction(kvstore);
    RET_IF_ERR_EXPECTED(eptxn);

    // mainly for meta data
    auto emetaRv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_TBITMAP_META);
    if (emetaRv.status().code() == ErrorCodes::ERR_EXPIRED ||
        emetaRv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtZero();
    } else if (emetaRv.status().code() == ErrorCodes::ERR_WRONG_TYPE) {
      auto it = commandMap().find("getbit");
      if (it == commandMap().end()) {
        LOG(FATAL) << "BUG: command:getbit not found!";
      }
      return it->second->run(sess);
    } else if (!emetaRv.ok()) {
      return emetaRv.status();
    }

    auto eMeta = TBitMapMetaValue::decode(emetaRv.value().getValue());
    RET_IF_ERR_EXPECTED(eMeta);
    TBitMapMetaValue meta = std::move(eMeta.value());

    // get corresponding partition value
    uint64_t fragmentId = pos / (8 * meta.fragmentLen());
    uint64_t offset = pos % (8 * meta.fragmentLen());

    if (pos >= meta.bitAmount()) {
      return Command::fmtZero();
    }

    std::string bitstr;  // get bit at offset
    if (fragmentId == 0) {
      bitstr = meta.firstFragment();
    } else {
      RecordKey subRk(edb.value().chunkId,
                      pctx->getDbId(),
                      RecordType::RT_TBITMAP_ELE,
                      key,
                      fragmentIdEncode(fragmentId));
      auto esubRv = kvstore->getKV(subRk, eptxn.value());
      if (esubRv.status().code() == ErrorCodes::ERR_EXPIRED ||
          esubRv.status().code() == ErrorCodes::ERR_NOTFOUND) {
        return Command::fmtZero();
      } else if (!esubRv.ok()) {
        return esubRv.status();
      }
      bitstr = esubRv.value().getValue();
      INVARIANT_D(bitstr.size() <= meta.fragmentLen());
    }

    uint64_t byte = offset >> 3;
    uint8_t p = 7 - (offset & 0x7);
    if (byte >= bitstr.size()) {
      return Command::fmtZero();
    }
    uint8_t byteval = static_cast<uint8_t>(bitstr[byte]) & (1 << p);
    return byteval ? Command::fmtOne() : Command::fmtZero();
  }
} tgetbitCmd;

class TBitCountCommand : public Command {
 public:
  TBitCountCommand() : Command("tbitcount", "r") {}

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
    const auto& args = sess->getArgs();
    const std::string& key = args[1];

    auto pctx = sess->getCtx();
    INVARIANT(pctx != nullptr);
    auto edb = sess->getServerEntry()->getSegmentMgr()->getDbWithKeyLock(
      sess, (key), Command::RdLock());
    RET_IF_ERR_EXPECTED(edb);
    auto kvstore = edb.value().store;
    auto eptxn = pctx->createTransaction(kvstore);
    RET_IF_ERR_EXPECTED(eptxn);

    // mainly for meta data
    auto emetaRv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_TBITMAP_META);
    if (emetaRv.status().code() == ErrorCodes::ERR_EXPIRED ||
        emetaRv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtZero();
    } else if (emetaRv.status().code() == ErrorCodes::ERR_WRONG_TYPE) {
      auto it = commandMap().find("bitcount");
      if (it == commandMap().end()) {
        LOG(FATAL) << "BUG: command:bitcount not found!";
      }
      return it->second->run(sess);
    } else if (!emetaRv.ok()) {
      return emetaRv.status();
    }
    auto eMeta = TBitMapMetaValue::decode(emetaRv.value().getValue());
    RET_IF_ERR_EXPECTED(eMeta);
    TBitMapMetaValue meta = std::move(eMeta.value());

    // get start and end
    int64_t len = meta.byteAmount();
    uint64_t argc = 4;
    GET_START_END(argc)

    if (start > end) {
      return Command::fmtZero();
    }

    // the whole bitmap
    if (start == 0 && end >= len - 1) {
      return Command::fmtLongLong(meta.count());
    }

    // whole bitmap except the first element
    if ((uint64_t)start < meta.fragmentLen() && end >= len - 1) {
      INVARIANT_D(start >= 1);
      auto cnt = redis_port::popCount(meta.firstFragment(), 0, start - 1);
      return Command::fmtLongLong(meta.count() - cnt);
    }

    uint64_t bitcount = 0;
    auto firstFrag = start / meta.fragmentLen();
    auto offset = start;

    if ((uint64_t)start < meta.fragmentLen()) {
      bitcount += redis_port::popCount(meta.firstFragment(), start, end);

      offset = meta.fragmentLen();
      INVARIANT_D(firstFrag == 0);
      firstFrag++;

      // only need to get bitcount from bitmap metadata
      if (offset > end) {
        return Command::fmtLongLong(bitcount);
      }
    }

    // get bitcount from bitmap elements
    RecordKey rk(edb.value().chunkId,
                 pctx->getDbId(),
                 RecordType::RT_TBITMAP_ELE,
                 key,
                 fragmentIdEncode(firstFrag));
    auto cursor = eptxn.value()->createDataCursor();
    cursor->seek(rk.prefixPk());

    while (offset < end) {
      auto ercd = cursor->next();
      if (ercd.status().code() == ErrorCodes::ERR_EXHAUST) {
        break;
      }
      RET_IF_ERR_EXPECTED(ercd);
      RecordKey subRk = ercd.value().getRecordKey();
      if (subRk.prefixPk() != rk.prefixPk() ||
          subRk.getRecordType() != RecordType::RT_TBITMAP_ELE) {
        break;
      }

      auto efragId = fragmentIdDecode(subRk.getSecondaryKey());
      RET_IF_ERR_EXPECTED(efragId);
      auto fragId = efragId.value();
      if (fragId * meta.fragmentLen() > (uint64_t)end) {
        break;
      }

      if ((uint64_t)offset < fragId * meta.fragmentLen()) {
        offset = fragId * meta.fragmentLen();
      }

      bitcount += redis_port::popCount(ercd.value().getRecordValue().getValue(),
                                       offset % meta.fragmentLen(),
                                       end - fragId * meta.fragmentLen());
      offset = (fragId + 1) * meta.fragmentLen();
    }

    return Command::fmtLongLong(bitcount);
  }
} tbitcntCmd;

class TBitPosCommand : public Command {
 public:
  TBitPosCommand() : Command("tbitpos", "r") {}

  ssize_t arity() const {
    return -3;
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
    const auto& args = sess->getArgs();
    const std::string& key = args[1];

    auto ebit = getBitOrOffset(args[2], true);
    // same as redis
    if (!ebit.ok()) {
      ebit = {ErrorCodes::ERR_PARSEOPT,
              "-ERR The bit argument must be 1 or 0.\r\n"};
    }
    RET_IF_ERR_EXPECTED(ebit);
    auto bit = ebit.value();

    auto pctx = sess->getCtx();
    INVARIANT(pctx != nullptr);
    auto edb = sess->getServerEntry()->getSegmentMgr()->getDbWithKeyLock(
      sess, (key), Command::RdLock());
    RET_IF_ERR_EXPECTED(edb);
    auto kvstore = edb.value().store;
    auto eptxn = pctx->createTransaction(kvstore);
    RET_IF_ERR_EXPECTED(eptxn);

    // mainly for meta data
    auto emetaRv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_TBITMAP_META);
    if (emetaRv.status().code() == ErrorCodes::ERR_EXPIRED ||
        emetaRv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return bit ? Command::fmtLongLong(-1) : Command::fmtZero();
    } else if (emetaRv.status().code() == ErrorCodes::ERR_WRONG_TYPE) {
      auto it = commandMap().find("bitpos");
      if (it == commandMap().end()) {
        LOG(FATAL) << "BUG: command:bitpos not found!";
      }
      return it->second->run(sess);
    } else if (!emetaRv.ok()) {
      return emetaRv.status();
    }
    auto eMeta = TBitMapMetaValue::decode(emetaRv.value().getValue());
    RET_IF_ERR_EXPECTED(eMeta);
    TBitMapMetaValue meta = std::move(eMeta.value());

    // get start and end
    int64_t len = meta.byteAmount();
    uint64_t argc = 5;
    GET_START_END(argc)

    if (start > end) {
      return Command::fmtLongLong(-1);
    }

    auto firstFrag = start / meta.fragmentLen();
    auto offset = start;

    if ((uint64_t)start < meta.fragmentLen()) {
      auto pos = redis_port::bitPos(meta.firstFragment(), start, end, bit);
      if (pos != -1) {
        /* If we are looking for clear bits, and the user specified an exact
         * range with start-end, we can't consider the right of the range as
         * zero padded (as we do when no explicit end is given).
         *
         * So if redisBitpos() returns the first bit outside the range,
         * we return -1 to the caller, to mean, in the specified range there
         * is not a single "0" bit. */
        if (endGiven && bit == 0 && pos == (end + 1) * 8) {
          pos = -1;
        }
        return Command::fmtLongLong(pos);
      }

      offset = meta.fragmentLen();
      INVARIANT_D(firstFrag == 0);
      firstFrag++;

      // only need to get bitpos from bitmap metadata
      if (offset > end) {
        return Command::fmtLongLong(-1);
      }
    }

    // get bitcount from bitmap elements
    RecordKey rk(edb.value().chunkId,
                 pctx->getDbId(),
                 RecordType::RT_TBITMAP_ELE,
                 key,
                 fragmentIdEncode(firstFrag));
    auto cursor = eptxn.value()->createDataCursor();
    cursor->seek(rk.prefixPk());

    while (offset < end) {
      auto ercd = cursor->next();
      if (ercd.status().code() == ErrorCodes::ERR_EXHAUST) {
        break;
      }
      RET_IF_ERR_EXPECTED(ercd);
      RecordKey subRk = ercd.value().getRecordKey();
      if (subRk.prefixPk() != rk.prefixPk() ||
          subRk.getRecordType() != RecordType::RT_TBITMAP_ELE) {
        break;
      }

      auto efragId = fragmentIdDecode(subRk.getSecondaryKey());
      RET_IF_ERR_EXPECTED(efragId);
      auto fragId = efragId.value();
      if (fragId * meta.fragmentLen() > (uint64_t)end) {
        break;
      }

      if ((uint64_t)offset < fragId * meta.fragmentLen()) {
        offset = fragId * meta.fragmentLen();
      }

      auto pos = redis_port::bitPos(ercd.value().getRecordValue().getValue(),
                                    offset % meta.fragmentLen(),
                                    end - fragId * meta.fragmentLen(),
                                    bit);
      if (pos != -1) {
        if (endGiven && bit == 0 && pos == (end + 1) * 8) {
          pos = -1;
        }
        return Command::fmtLongLong(pos + fragId * meta.fragmentLen() * 8);
      }

      offset = (fragId + 1) * meta.fragmentLen();
    }

    return Command::fmtLongLong(-1);
  }
} tbitposCmd;

class TBitFieldCommand : public Command {
 public:
  TBitFieldCommand() : Command("tbitfield", "wm") {}

  enum class FieldOpType {
    BITFIELDOP_GET,
    BITFIELDOP_SET,
    BITFIELDOP_INCRBY,
  };

  enum class BFOverFlowType {
    BFOVERFLOW_WRAP,
    BFOVERFLOW_SAT,
    BFOVERFLOW_FAIL,
  };

  struct BitfieldOp {
    uint64_t offset;
    int64_t i64;
    FieldOpType opcode;
    BFOverFlowType owtype;
    int32_t bits;
    int sign;
  };

  struct BitFragment {
    std::string value;
    bool newFrag;  // true: created by this operation
  };

  typedef std::unordered_map<uint64_t, BitFragment> FragMap;

  Expected<std::string> getBitfield(const uint32_t chunkId,
                                    const uint32_t dbId,
                                    Transaction* ptxn,
                                    PStore kvstore,
                                    const TBitMapMetaValue& meta,
                                    const std::string& key,
                                    uint64_t* offset,
                                    const uint64_t bits,
                                    FragMap* fragMap) {
    uint64_t fragId = *offset / (8 * meta.fragmentLen());
    uint64_t cross = fragId;
    if ((8 * meta.fragmentLen()) - *offset % (8 * meta.fragmentLen()) < bits) {
      cross += 1;
    }

    std::string value;
    bool newEle = false;
    for (uint64_t i = fragId; i <= cross; ++i) {
      std::string str;
      if (fragMap->count(i)) {
        str = (*fragMap)[i].value;
      } else {
        if (i == 0) {
          str = meta.firstFragment();
        } else {
          RecordKey subRk(chunkId,
                          dbId,
                          RecordType::RT_TBITMAP_ELE,
                          key,
                          fragmentIdEncode(i));
          auto esubRv = kvstore->getKV(subRk, ptxn);

          if (esubRv.status().code() == ErrorCodes::ERR_EXPIRED ||
              esubRv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            str = "";
            newEle = true;
          } else if (!esubRv.ok()) {
            return esubRv.status();
          } else {
            str = esubRv.value().getValue();
          }
        }
        (*fragMap)[i] = {str, newEle};
      }
      str.resize(meta.fragmentLen());
      value += str;
    }
    *offset -= fragId * (8 * meta.fragmentLen());
    return value;
  }

  Expected<uint64_t> getUnsignedBitfield(const uint32_t chunkId,
                                         const uint32_t dbId,
                                         Transaction* ptxn,
                                         PStore kvstore,
                                         const TBitMapMetaValue& meta,
                                         const std::string& key,
                                         uint64_t offset,
                                         const uint64_t bits,
                                         FragMap* fragMap) {
    auto ev = getBitfield(
      chunkId, dbId, ptxn, kvstore, meta, key, &offset, bits, fragMap);
    RET_IF_ERR_EXPECTED(ev);
    std::string value = ev.value();

    uint64_t oldVal(0);
    for (size_t i = 0; i < bits; i++) {
      uint64_t byte = offset >> 3;
      uint64_t bit = 7 - (offset & 0x7);
      uint64_t byteval = static_cast<uint8_t>(value[byte]);
      uint64_t bitval = (byteval >> bit) & 1;
      oldVal = (oldVal << 1) | bitval;
      offset++;
    }
    return oldVal;
  }

  Expected<int64_t> getSignedBitfield(const uint32_t chunkId,
                                      const uint32_t dbId,
                                      Transaction* ptxn,
                                      PStore kvstore,
                                      const TBitMapMetaValue& meta,
                                      const std::string& key,
                                      uint64_t offset,
                                      const uint64_t bits,
                                      FragMap* fragMap) {
    int64_t ret;
    union {
      uint64_t u;
      int64_t i;
    } conv;

    auto ev = getUnsignedBitfield(
      chunkId, dbId, ptxn, kvstore, meta, key, offset, bits, fragMap);
    RET_IF_ERR_EXPECTED(ev);
    conv.u = ev.value();
    ret = conv.i;

    if (ret & ((uint64_t)1 << (bits - 1)))
      ret |= ((uint64_t)-1) << bits;

    return ret;
  }

  int checkUnsignedBitfieldOverflow(uint64_t value,
                                    int64_t incr,
                                    uint64_t bits,
                                    BFOverFlowType owtype,
                                    uint64_t* newVal) {
    uint64_t max = (bits == 64) ? UINT64_MAX : (((uint64_t)1 << bits) - 1);
    int64_t maxincr = max - value;
    int64_t minincr = -value;
    auto handleWrap = [&]() {
      uint64_t mask = ((uint64_t)-1) << bits;
      uint64_t res = value + incr;
      res &= ~mask;
      *newVal = res;
      return 1;
    };

    if (value > max || (incr > 0 && incr > maxincr)) {
      if (owtype == BFOverFlowType::BFOVERFLOW_WRAP) {
        return handleWrap();
      } else if (owtype == BFOverFlowType::BFOVERFLOW_SAT) {
        *newVal = max;
      }
      return 1;
    } else if (incr < 0 && incr < minincr) {
      if (owtype == BFOverFlowType::BFOVERFLOW_WRAP) {
        return handleWrap();
      } else if (owtype == BFOverFlowType::BFOVERFLOW_SAT) {
        *newVal = 0;
      }
      return -1;
    }
    return 0;
  }

  int checkSignedBitfieldOverflow(int64_t value,
                                  int64_t incr,
                                  uint64_t bits,
                                  BFOverFlowType owtype,
                                  int64_t* newVal) {
    int64_t max = (bits == 64) ? INT64_MAX : (((int64_t)1 << (bits - 1)) - 1);
    int64_t min = (-max) - 1;

    int64_t maxincr = max - value;
    int64_t minincr = min - value;

    auto handleWrap = [&]() {
      uint64_t mask = ((uint64_t)-1) << bits;
      uint64_t msb = (uint64_t)1 << (bits - 1);
      uint64_t a = value, b = incr, c;
      c = a + b;

      if (c & msb) {
        c |= mask;
      } else {
        c &= ~mask;
      }
      *newVal = c;
      return 1;
    };

    if (value > max || (bits != 64 && incr > maxincr) ||
        (value >= 0 && incr > 0 && incr > maxincr)) {
      if (owtype == BFOverFlowType::BFOVERFLOW_WRAP) {
        return handleWrap();
      } else if (owtype == BFOverFlowType::BFOVERFLOW_SAT) {
        *newVal = max;
      }
      return 1;
    } else if (value < min || (bits != 64 && incr < minincr) ||
               (value < 0 && incr < 0 && incr < minincr)) {
      if (owtype == BFOverFlowType::BFOVERFLOW_WRAP) {
        return handleWrap();
      } else if (owtype == BFOverFlowType::BFOVERFLOW_SAT) {
        *newVal = min;
      }
      return -1;
    }
    return 0;
  }

  Status setUnsignedBitfield(const uint32_t chunkId,
                             const uint32_t dbId,
                             Transaction* ptxn,
                             PStore kvstore,
                             SessionCtx* pctx,
                             TBitMapMetaValue& meta,  // NOLINT
                             const std::string& key,
                             uint64_t offset,
                             const uint64_t bits,
                             const uint64_t value,
                             FragMap* fragMap) {
    uint64_t fragId = offset / (8 * meta.fragmentLen());
    uint64_t cross = fragId;
    if ((8 * meta.fragmentLen()) - offset % (8 * meta.fragmentLen()) < bits) {
      cross += 1;
    }

    auto ebuf = getBitfield(
      chunkId, dbId, ptxn, kvstore, meta, key, &offset, bits, fragMap);
    RET_IF_ERR_EXPECTED(ebuf);
    std::string buf = ebuf.value();

    for (size_t i = 0; i < bits; i++) {
      uint64_t bitval = (value & ((uint64_t)1 << (bits - 1 - i))) != 0;
      uint64_t byte = offset >> 3;
      uint64_t bit = 7 - (offset & 0x7);
      uint8_t byteval = static_cast<uint8_t>(buf[byte]);
      int before = (byteval >> bit) & 1;
      byteval &= ~(1 << bit);
      byteval |= bitval << bit;
      buf[byte] = byteval & 0xff;
      offset++;

      if (before == 0 && bitval == 1) {
        meta.setCount(meta.count() + 1);
      }
      if (before == 1 && bitval == 0) {
        meta.setCount(meta.count() - 1);
      }
      if (offset + fragId * (8 * meta.fragmentLen()) > meta.bitAmount()) {
        meta.setBitAmount(offset + fragId * (8 * meta.fragmentLen()));
      }
    }

    for (uint64_t i = fragId; i <= cross; ++i) {
      auto value =
        buf.substr((i - fragId) * meta.fragmentLen(), meta.fragmentLen());

      if (i == 0) {
        meta.setFirstFragment(value);
      } else {
        auto s = setBitmapEle(ptxn, kvstore, chunkId, dbId, key, i, value);
        RET_IF_ERR(s);

        if ((*fragMap)[i].newFrag) {
          // It means that the element is "NOT_FOUND" in storage.
          meta.setEleCount(meta.eleCount() + 1);
        }
      }

      (*fragMap)[i] = {value, false};
    }

    return {ErrorCodes::ERR_OK, ""};
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

  Expected<std::string> run(Session* sess) final {
    const auto& args = sess->getArgs();
    const std::string& key = args[1];

    std::queue<BitfieldOp> ops;
    bool readonly(1);
    size_t highestOffset(0);
    BFOverFlowType owtype(BFOverFlowType::BFOVERFLOW_WRAP);
    for (size_t i = 2; i < args.size(); i++) {
      int remaining = args.size() - i - 1;
      FieldOpType opcode;
      int64_t i64(0);
      int sign;

      if (!::strcasecmp(args[i].c_str(), "get") && remaining >= 2) {
        opcode = FieldOpType::BITFIELDOP_GET;
      } else if (!::strcasecmp(args[i].c_str(), "set") && remaining >= 3) {
        opcode = FieldOpType::BITFIELDOP_SET;
      } else if (!::strcasecmp(args[i].c_str(), "incrby") && remaining >= 3) {
        opcode = FieldOpType::BITFIELDOP_INCRBY;
      } else if (!::strcasecmp(args[i].c_str(), "overflow") && remaining >= 1) {
        ++i;
        if (!::strcasecmp(args[i].c_str(), "wrap")) {
          owtype = BFOverFlowType::BFOVERFLOW_WRAP;
        } else if (!::strcasecmp(args[i].c_str(), "sat")) {
          owtype = BFOverFlowType::BFOVERFLOW_SAT;
        } else if (!::strcasecmp(args[i].c_str(), "fail")) {
          owtype = BFOverFlowType::BFOVERFLOW_FAIL;
        } else {
          return {ErrorCodes::ERR_PARSEOPT, "syntax error"};
        }
        continue;
      } else {
        return {ErrorCodes::ERR_PARSEOPT, "syntax error"};
      }

      ++i;
      // type
      if (args[i].c_str()[0] == 'i') {
        sign = 1;
      } else if (args[i].c_str()[0] == 'u') {
        sign = 0;
      } else {
        return {ErrorCodes::ERR_PARSEOPT, "syntax error"};
      }

      const std::string bitvals(args[i].c_str() + 1);
      Expected<int32_t> eBits = tendisplus::stol(bitvals);
      if (!eBits.ok() || eBits.value() < 1 ||
          (sign == 1 && eBits.value() > 64) ||
          (sign == 0 && eBits.value() > 63)) {
        return {ErrorCodes::ERR_PARSEOPT,
                "-ERR Invalid bitfield type. Use something like i16 u8. "
                "Note that u64 is not supported buf i64 is."};
      }
      int32_t bits = eBits.value();

      ++i;
      // offset
      int usehash(0);
      if (args[i].c_str()[0] == '#')
        usehash = 1;
      const std::string offsetval(args[i].c_str() + usehash);
      Expected<int64_t> eOffset = tendisplus::stoll(offsetval);
      if (!eOffset.ok()) {
        return {ErrorCodes::ERR_PARSEPKT,
                "-ERR bit offset is not an integer or out of range"};
      }
      uint64_t offset = usehash ? eOffset.value() * bits : eOffset.value();
      if ((offset >> 3) >= 512 * 1024 * 1024) {
        return {ErrorCodes::ERR_PARSEPKT,
                "-ERR bit offset is not an integer or out of range"};
      }

      if (opcode != FieldOpType::BITFIELDOP_GET) {
        // value or increment
        ++i;
        readonly = 0;
        if (highestOffset < static_cast<size_t>(offset) + bits - 1)
          highestOffset = offset + bits - 1;
        Expected<int64_t> eI64 = tendisplus::stoll(args[i]);
        if (!eI64.ok()) {
          return eI64.status();
        }
        i64 = eI64.value();
      }

      ops.emplace(BitfieldOp{
        static_cast<size_t>(offset), i64, opcode, owtype, bits, sign});
    }

    auto pctx = sess->getCtx();
    INVARIANT(pctx != nullptr);
    auto edb = sess->getServerEntry()->getSegmentMgr()->getDbWithKeyLock(
      sess, (key), readonly ? RdLock() : mgl::LockMode::LOCK_X);
    RET_IF_ERR_EXPECTED(edb);
    auto kvstore = edb.value().store;
    auto eptxn = pctx->createTransaction(kvstore);
    RET_IF_ERR_EXPECTED(eptxn);
    auto txn = eptxn.value();
    auto chunkId = edb.value().chunkId;

    bool metaExists = true;

    auto emetaRv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_TBITMAP_META);
    uint64_t fragmentLen =
      sess->getServerEntry()->getParams()->tbitmapFragmentSize;
    TBitMapMetaValue meta(fragmentLen);
    if (emetaRv.status().code() == ErrorCodes::ERR_EXPIRED ||
        emetaRv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      metaExists = false;
    } else if (emetaRv.status().code() == ErrorCodes::ERR_WRONG_TYPE) {
      auto it = commandMap().find("bitfield");
      if (it == commandMap().end()) {
        LOG(FATAL) << "BUG: command:bitfield not found!";
      }
      return it->second->run(sess);
    } else if (!emetaRv.ok()) {
      return emetaRv.status();
    } else {
      auto eMeta = TBitMapMetaValue::decode(emetaRv.value().getValue());
      RET_IF_ERR_EXPECTED(eMeta);
      meta = std::move(eMeta.value());
    }

    /* Cache all the fragments, because it need to get fragments(get) after
     * the uncommitted operations(set/incr). It also speedup the get operation.
     */
    FragMap fragMap;
    std::stringstream ss;
    Command::fmtMultiBulkLen(ss, ops.size());
    size_t size = ops.size();
    for (size_t i = 0; i < size; i++) {
      BitfieldOp op = ops.front();
      ops.pop();
      if (op.opcode == FieldOpType::BITFIELDOP_SET ||
          op.opcode == FieldOpType::BITFIELDOP_INCRBY) {
        if (op.sign) {
          int64_t oldval, newval, retval, wrapped(0);
          int overflow(0);
          auto ev = getSignedBitfield(chunkId,
                                      pctx->getDbId(),
                                      txn,
                                      kvstore,
                                      meta,
                                      key,
                                      op.offset,
                                      op.bits,
                                      &fragMap);
          RET_IF_ERR_EXPECTED(ev);
          oldval = ev.value();

          if (op.opcode == FieldOpType::BITFIELDOP_INCRBY) {
            newval = oldval + op.i64;
            overflow = checkSignedBitfieldOverflow(
              oldval, op.i64, op.bits, op.owtype, &wrapped);
            if (overflow)
              newval = wrapped;
            retval = newval;
          } else {
            newval = op.i64;
            overflow = checkSignedBitfieldOverflow(
              op.i64, 0, op.bits, op.owtype, &wrapped);
            if (overflow)
              newval = wrapped;
            retval = oldval;
          }

          if (!(overflow && op.owtype == BFOverFlowType::BFOVERFLOW_FAIL)) {
            Command::fmtLongLong(ss, retval);
            uint64_t uv = newval;
            auto s = setUnsignedBitfield(chunkId,
                                         pctx->getDbId(),
                                         txn,
                                         kvstore,
                                         pctx,
                                         meta,
                                         key,
                                         op.offset,
                                         op.bits,
                                         uv,
                                         &fragMap);
            RET_IF_ERR(s);

          } else {
            Command::fmtNull(ss);
          }
        } else {
          uint64_t oldval, newval, retval, wrapped(0);
          int overflow(0);
          auto ev = getUnsignedBitfield(chunkId,
                                        pctx->getDbId(),
                                        txn,
                                        kvstore,
                                        meta,
                                        key,
                                        op.offset,
                                        op.bits,
                                        &fragMap);
          RET_IF_ERR_EXPECTED(ev);
          oldval = ev.value();

          if (op.opcode == FieldOpType::BITFIELDOP_INCRBY) {
            newval = oldval + op.i64;
            overflow = checkUnsignedBitfieldOverflow(
              oldval, op.i64, op.bits, op.owtype, &wrapped);
            if (overflow)
              newval = wrapped;
            retval = newval;
          } else {
            newval = op.i64;
            overflow = checkUnsignedBitfieldOverflow(
              op.i64, 0, op.bits, op.owtype, &wrapped);
            if (overflow)
              newval = wrapped;
            retval = oldval;
          }
          if (!(overflow && op.owtype == BFOverFlowType::BFOVERFLOW_FAIL)) {
            Command::fmtLongLong(ss, retval);
            auto s = setUnsignedBitfield(chunkId,
                                         pctx->getDbId(),
                                         txn,
                                         kvstore,
                                         pctx,
                                         meta,
                                         key,
                                         op.offset,
                                         op.bits,
                                         newval,
                                         &fragMap);
            RET_IF_ERR(s);
          } else {
            Command::fmtNull(ss);
          }
        }
      } else {
        if (op.sign) {
          auto ev = getSignedBitfield(chunkId,
                                      pctx->getDbId(),
                                      txn,
                                      kvstore,
                                      meta,
                                      key,
                                      op.offset,
                                      op.bits,
                                      &fragMap);
          RET_IF_ERR_EXPECTED(ev);
          int64_t val = ev.value();
          Command::fmtLongLong(ss, val);
        } else {
          auto ev = getUnsignedBitfield(chunkId,
                                        pctx->getDbId(),
                                        txn,
                                        kvstore,
                                        meta,
                                        key,
                                        op.offset,
                                        op.bits,
                                        &fragMap);
          RET_IF_ERR_EXPECTED(ev);
          uint64_t val = ev.value();
          Command::fmtLongLong(ss, val);
        }
      }
    }

    if (!metaExists || !readonly) {
      RecordKey metaRk(
        chunkId, pctx->getDbId(), RecordType::RT_TBITMAP_META, key, "");
      Status s = kvstore->setKV(metaRk,
                                RecordValue(meta.encode(),
                                            RecordType::RT_TBITMAP_META,
                                            sess->getCtx()->getVersionEP()),
                                txn);

      RET_IF_ERR(s);
      auto eCmt = pctx->commitTransaction(txn);
      RET_IF_ERR_EXPECTED(eCmt);
    }
    return ss.str();
  }
} tbitfieldCmd;

class TBitopCommand : public Command {
 public:
  TBitopCommand() : Command("tbitop", "wm") {}

  enum class Op {
    BITOP_AND,
    BITOP_OR,
    BITOP_XOR,
    BITOP_NOT,
  };

  ssize_t arity() const {
    return -4;
  }

  int32_t firstkey() const {
    return 2;
  }

  int32_t lastkey() const {
    return -1;
  }

  int32_t keystep() const {
    return 1;
  }

  Expected<std::string> run(Session* sess) final {
    const auto& args = sess->getArgs();
    const std::string& opName = toLower(args[1]);
    const std::string& targetKey = args[2];

    Op op;
    if (opName == "and") {
      op = Op::BITOP_AND;
    } else if (opName == "or") {
      op = Op::BITOP_OR;
    } else if (opName == "xor") {
      op = Op::BITOP_XOR;
    } else if (opName == "not") {
      op = Op::BITOP_NOT;
    } else {
      return {ErrorCodes::ERR_PARSEPKT, "syntax error"};
    }
    if (op == Op::BITOP_NOT && args.size() != 4) {
      return {ErrorCodes::ERR_PARSEPKT,
              "-ERR BITOP NOT must be called with a single source key."};
    }

    auto index = getKeysFromCommand(args);
    auto locklist = sess->getServerEntry()->getSegmentMgr()->getAllKeysLocked(
      sess, args, index, mgl::LockMode::LOCK_X);
    if (!locklist.ok()) {
      return locklist.status();
    }

    auto pctx = sess->getCtx();
    INVARIANT(pctx != nullptr);
    auto edb =
      sess->getServerEntry()->getSegmentMgr()->getDbHasLocked(sess, targetKey);
    RET_IF_ERR_EXPECTED(edb);
    auto kvstore = edb.value().store;
    auto eptxn = pctx->createTransaction(kvstore);
    RET_IF_ERR_EXPECTED(eptxn);

    size_t numKeys = args.size() - 3;
    std::vector<std::string> vals;

    uint64_t maxBitAmount = 0;

    for (size_t j = 0; j < numKeys; ++j) {
      const std::string& key = args[j + 3];
      auto eval = getBitmap(sess, key, &maxBitAmount);
      RET_IF_ERR_EXPECTED(eval);
      vals.emplace_back(eval.value());
    }

    RecordKey metaRk(edb.value().chunkId,
                     pctx->getDbId(),
                     RecordType::RT_TBITMAP_META,
                     targetKey,
                     "");

    if (maxBitAmount > 0) {
      if (maxBitAmount % 8 != 0) {
        maxBitAmount += 8 - (maxBitAmount % 8);
      }
    }
    uint64_t fragmentLen =
      sess->getServerEntry()->getParams()->tbitmapFragmentSize;
    TBitMapMetaValue meta(maxBitAmount, 0, 1, fragmentLen);

    auto maxlen = meta.byteAmount();
    std::string result(maxlen, 0);
    for (size_t i = 0; i < maxlen; ++i) {
      unsigned char output = (vals[0].size() <= i) ? 0 : vals[0][i];
      if (op == Op::BITOP_NOT)
        output = ~output;
      for (size_t j = 1; j < numKeys; ++j) {
        bool skip = false;
        unsigned char byte = (vals[j].size() <= i) ? 0 : vals[j][i];
        switch (op) {
          case Op::BITOP_AND:
            output &= byte;
            skip = (output == 0);
            break;
          case Op::BITOP_OR:
            output |= byte;
            skip = (output == 0xff);
            break;
          case Op::BITOP_XOR:
            output ^= byte;
            break;
          default:
            INVARIANT_D(0);
        }

        if (skip) {
          break;
        }
      }
      result[i] = output;
    }

    // delete the old key
    auto s = Command::delKey(
      sess, targetKey, RecordType::RT_TBITMAP_META, eptxn.value());
    RET_IF_ERR(s);

    auto eleCnt = 1;
    for (uint64_t i = 0; i < maxlen; i += meta.fragmentLen()) {
      std::string val;
      auto valLen = meta.fragmentLen();
      if (i + valLen > maxlen) {
        valLen = maxlen - i;
      }
      val.assign(result, i, valLen);

      auto valBitCount = redis_port::popCount(val.c_str(), val.size());

      auto fragId = i / meta.fragmentLen();
      if (fragId == 0) {
        meta.setFirstFragment(val);
      } else if (valBitCount > 0) {
        auto s = setBitmapEle(eptxn.value(),
                              kvstore,
                              metaRk.getChunkId(),
                              metaRk.getDbId(),
                              targetKey,
                              fragId,
                              val);
        RET_IF_ERR(s);

        eleCnt++;
      }
    }

    if (maxlen) {
      meta.setCount(redis_port::popCount(result.c_str(), result.size()));
      meta.setEleCount(eleCnt);

      Status s = kvstore->setKV(metaRk,
                                RecordValue(meta.encode(),
                                            RecordType::RT_TBITMAP_META,
                                            pctx->getVersionEP()),
                                eptxn.value());
      RET_IF_ERR(s);
    }

    auto eCmt = pctx->commitTransaction(eptxn.value());
    RET_IF_ERR_EXPECTED(eCmt);

    return Command::fmtLongLong(meta.byteAmount());
  }
} tbitopCmd;

}  // namespace tendisplus
