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
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/utils/scopeguard.h"

namespace tendisplus {

Expected<bool> delGeneric(Session* sess, const std::string& key,
        Transaction* txn);

Expected<std::string> genericSRem(Session* sess,
                                  PStore kvstore,
                                  Transaction* txn,
                                  const RecordKey& metaRk,
                                  const Expected<RecordValue>& rv,
                                  const std::vector<std::string>& args) {
  SetMetaValue sm;
  uint64_t ttl = 0;

  if (rv.ok()) {
    ttl = rv.value().getTtl();
    Expected<SetMetaValue> exptSm = SetMetaValue::decode(rv.value().getValue());
    INVARIANT_D(exptSm.ok());
    if (!exptSm.ok()) {
      return exptSm.status();
    }
    sm = std::move(exptSm.value());
  } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND ||
             rv.status().code() == ErrorCodes::ERR_EXPIRED) {
    return Command::fmtZero();
  } else {
    return rv.status();
  }

  uint64_t cnt = 0;
  for (size_t i = 0; i < args.size(); ++i) {
    RecordKey subRk(metaRk.getChunkId(),
                    metaRk.getDbId(),
                    RecordType::RT_SET_ELE,
                    metaRk.getPrimaryKey(),
                    args[i]);
    Expected<RecordValue> rv = kvstore->getKV(subRk, txn);
    if (rv.ok()) {
      cnt += 1;
    } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      continue;
    } else {
      return rv.status();
    }
    Status s = kvstore->delKV(subRk, txn);
    if (!s.ok()) {
      return s;
    }
  }
  INVARIANT_D(sm.getCount() >= cnt);
  Status s;
  if (sm.getCount() <= cnt) {
    if (sm.getCount() < cnt) {
      LOG(ERROR) << "invalid set:"
                 << rcd_util::makeInvalidErrStr(metaRk.getRecordValueType(),
                                                metaRk.getPrimaryKey(),
                                                sm.getCount(),
                                                cnt);
    }
    s = Command::delKeyAndTTL(sess, metaRk, rv.value(), txn);
  } else {
    sm.setCount(sm.getCount() - cnt);
    s = kvstore->setKV(metaRk,
                       RecordValue(sm.encode(),
                                   RecordType::RT_SET_META,
                                   sess->getCtx()->getVersionEP(),
                                   ttl,
                                   rv),
                       txn);
  }
  if (!s.ok()) {
    return s;
  }
  return Command::fmtLongLong(cnt);
}

Expected<std::string> genericSAdd(Session* sess,
                                  PStore kvstore,
                                  Transaction* txn,
                                  const RecordKey& metaRk,
                                  const Expected<RecordValue>& rv,
                                  const std::vector<std::string>& args) {
  SetMetaValue sm;
  uint64_t ttl = 0;

  if (args.size() <= 2) {
    return Command::fmtZero();
  }

  if (rv.ok()) {
    ttl = rv.value().getTtl();
    Expected<SetMetaValue> exptSm = SetMetaValue::decode(rv.value().getValue());
    INVARIANT_D(exptSm.ok());
    if (!exptSm.ok()) {
      return exptSm.status();
    }
    sm = std::move(exptSm.value());
  } else if (rv.status().code() != ErrorCodes::ERR_NOTFOUND &&
             rv.status().code() != ErrorCodes::ERR_EXPIRED) {
    return rv.status();
  }

  uint64_t cnt = 0;
  for (size_t i = 2; i < args.size(); ++i) {
    RecordKey subRk(metaRk.getChunkId(),
                    metaRk.getDbId(),
                    RecordType::RT_SET_ELE,
                    metaRk.getPrimaryKey(),
                    args[i]);

      Expected<RecordValue> subrv = kvstore->getKV(subRk, txn);
      if (subrv.ok()) {
        continue;
      } else if (subrv.status().code() == ErrorCodes::ERR_NOTFOUND) {
        cnt += 1;
      } else {
        return subrv.status();
      }

    RecordValue subRv("", RecordType::RT_SET_ELE, -1);
    Status s = kvstore->setKV(subRk, subRv, txn);
    if (!s.ok()) {
      return s;
    }
  }
  sm.setCount(sm.getCount() + cnt);
  Status s = kvstore->setKV(metaRk,
                            RecordValue(sm.encode(),
                                        RecordType::RT_SET_META,
                                        sess->getCtx()->getVersionEP(),
                                        ttl,
                                        rv),
                            txn);
  if (!s.ok()) {
    return s;
  }
  return Command::fmtLongLong(cnt);
}

class SMembersCommand : public Command {
 public:
  SMembersCommand() : Command("smembers", "rS") {}

  ssize_t arity() const {
    return 2;
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

    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);

    auto server = sess->getServerEntry();
    auto expdb =
      server->getSegmentMgr()->getDbWithKeyLock(sess, key, Command::RdLock());
    if (!expdb.ok()) {
      return expdb.status();
    }

    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_SET_META);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
      return Command::fmtZeroBulkLen();
    } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtZeroBulkLen();
    } else if (!rv.ok()) {
      return rv.status();
    }

    RecordKey metaRk(
      expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_SET_META, key, "");

    PStore kvstore = expdb.value().store;
    auto ptxn = sess->getCtx()->createTransaction(kvstore);
    if (!ptxn.ok()) {
      return ptxn.status();
    }

    ssize_t ssize = 0, cnt = 0;
    Expected<SetMetaValue> exptSm = SetMetaValue::decode(rv.value().getValue());
    INVARIANT_D(exptSm.ok());
    if (!exptSm.ok()) {
      return exptSm.status();
    }
    ssize = exptSm.value().getCount();

    std::stringstream ss;
    Command::fmtMultiBulkLen(ss, ssize);
    auto cursor = ptxn.value()->createDataCursor();
    RecordKey fake = {
      expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_SET_ELE, key, ""};
    cursor->seek(fake.prefixPk());
    while (true) {
      Expected<Record> exptRcd = cursor->next();
      if (exptRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
        break;
      }
      if (!exptRcd.ok()) {
        return exptRcd.status();
      }
      Record& rcd = exptRcd.value();
      const RecordKey& rcdkey = rcd.getRecordKey();
      if (rcdkey.prefixPk() != fake.prefixPk()) {
        break;
      }
      cnt += 1;
      Command::fmtBulk(ss, rcdkey.getSecondaryKey());
    }
    INVARIANT_D(cnt == ssize);
    if (cnt != ssize) {
      return {ErrorCodes::ERR_DECODE,
              rcd_util::makeInvalidErrStr(
                metaRk.getRecordValueType(), key, ssize, cnt)};
    }
    return ss.str();
  }
} smemberscmd;

class SIsMemberCommand : public Command {
 public:
  SIsMemberCommand() : Command("sismember", "rF") {}

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
    const std::vector<std::string>& args = sess->getArgs();
    const std::string& key = args[1];
    const std::string& subkey = args[2];

    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);

    auto server = sess->getServerEntry();
    auto expdb =
      server->getSegmentMgr()->getDbWithKeyLock(sess, key, Command::RdLock());
    if (!expdb.ok()) {
      return expdb.status();
    }

    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_SET_META);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
      return fmtZero();
    } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return fmtZero();
    } else if (!rv.ok()) {
      return rv.status();
    }

    RecordKey metaRk(
      expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_SET_META, key, "");

    PStore kvstore = expdb.value().store;
    auto ptxn = sess->getCtx()->createTransaction(kvstore);
    if (!ptxn.ok()) {
      return ptxn.status();
    }

    RecordKey subRk(expdb.value().chunkId,
                    pCtx->getDbId(),
                    RecordType::RT_SET_ELE,
                    key,
                    subkey);
    Expected<RecordValue> eSubVal = kvstore->getKV(subRk, ptxn.value());
    if (eSubVal.ok()) {
      return Command::fmtOne();
    } else if (eSubVal.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtZero();
    } else {
      return eSubVal.status();
    }
  }
} sIsMemberCmd;

class SrandMemberCommand : public Command {
 public:
  SrandMemberCommand() : Command("srandmember", "rR") {}

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

  bool sameWithRedis() const {
    return false;
  }

  Expected<std::string> run(Session* sess) final {
    const std::string& key = sess->getArgs()[1];
    bool explictBulk = false;
    int64_t bulk = 1;
    bool negative = false;
    if (sess->getArgs().size() >= 3) {
      Expected<int64_t> ebulk = ::tendisplus::stoll(sess->getArgs()[2]);
      if (!ebulk.ok()) {
        return ebulk.status();
      }
      bulk = ebulk.value();
      if (bulk < 0) {
        negative = true;
        bulk = -bulk;
      }

      // bulk = 0 explictly, return empty list
      if (!bulk) {
        return Command::fmtZeroBulkLen();
      }
      explictBulk = true;
    }
    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);

    auto server = sess->getServerEntry();
    auto expdb =
      server->getSegmentMgr()->getDbWithKeyLock(sess, key, Command::RdLock());

    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_SET_META);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
        rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      if (bulk == 1 && !explictBulk) {
        return Command::fmtNull();
      } else {
        return Command::fmtZeroBulkLen();
      }
    } else if (!rv.status().ok()) {
      return rv.status();
    }

    if (!expdb.ok()) {
      return expdb.status();
    }

    PStore kvstore = expdb.value().store;
    auto ptxn = sess->getCtx()->createTransaction(kvstore);
    if (!ptxn.ok()) {
      return ptxn.status();
    }

    ssize_t ssize = 0;
    Expected<SetMetaValue> exptSm = SetMetaValue::decode(rv.value().getValue());
    INVARIANT_D(exptSm.ok());
    if (!exptSm.ok()) {
      return {ErrorCodes::ERR_DECODE, "invalid set meta" + key};
    }
    ssize = exptSm.value().getCount();
    INVARIANT_D(ssize != 0);
    if (ssize == 0) {
      return {ErrorCodes::ERR_DECODE, "invalid set meta" + key};
    }

    auto cursor = ptxn.value()->createDataCursor();
    uint32_t beginIdx = 0;
    uint32_t cnt = 0;
    uint32_t peek = 0;
    uint32_t remain = 0;
    std::vector<std::string> vals;
    if (bulk > ssize) {
      beginIdx = 0;
      if (!negative) {
        remain = ssize;
      } else {
        remain = bulk;
      }
    } else {
      remain = bulk;

      uint32_t offset = ssize - remain + 1;
      int r = redis_port::random();
      // TODO(vinchen): max scan count should be configable
      beginIdx = r % (offset > 1024 * 16 ? 1024 * 16 : offset);
    }

    if (remain > 1024 * 16) {
      // TODO(vinchen):  should be configable
      return {ErrorCodes::ERR_INTERNAL, "bulk too big"};
    }
    RecordKey fake = {
      expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_SET_ELE, key, ""};
    cursor->seek(fake.prefixPk());
    while (true) {
      Expected<Record> exptRcd = cursor->next();
      if (exptRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
        break;
      }
      if (!exptRcd.ok()) {
        return exptRcd.status();
      }
      if (cnt++ < beginIdx) {
        continue;
      }
      if (cnt > ssize) {
        break;
      }
      if (peek < remain) {
        vals.emplace_back(exptRcd.value().getRecordKey().getSecondaryKey());
        peek++;
      } else {
        break;
      }
    }
    // TODO(vinchen): vals should be shuffle here
    INVARIANT_D(vals.size() != 0);
    if (vals.size() == 0) {
      return {ErrorCodes::ERR_DECODE, "invalid set meta" + key};
    }
    if (bulk == 1 && !explictBulk) {
      return Command::fmtBulk(vals[0]);
    } else {
      std::stringstream ss;
      INVARIANT_D(remain == vals.size() || negative);
      Command::fmtMultiBulkLen(ss, remain);
      while (remain) {
        for (auto& v : vals) {
          if (!remain)
            break;
          Command::fmtBulk(ss, v);
          remain--;
        }
      }
      return ss.str();
    }
  }
} srandmembercmd;

class SpopCommand : public Command {
 public:
  SpopCommand() : Command("spop", "wRsF") {}

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
    uint32_t count(1);
    if (args.size() > 3) {
      return {ErrorCodes::ERR_PARSEOPT, "syntax error"};
    }
    if (args.size() == 3) {
      auto eCnt = tendisplus::stoul(args[2]);
      if (!eCnt.ok()) {
        return eCnt.status();
      }
      count = eCnt.value();
    }

    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);

    auto server = sess->getServerEntry();
    auto expdb = server->getSegmentMgr()->getDbWithKeyLock(
      sess, key, mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
      return expdb.status();
    }

    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_SET_META);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
        rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtNull();
    } else if (!rv.ok()) {
      return rv.status();
    }

    Expected<SetMetaValue> expSm = SetMetaValue::decode(rv.value().getValue());
    if (!expSm.ok()) {
      return expSm.status();
    }
    SetMetaValue sm = std::move(expSm.value());

    RecordKey metaRk(
      expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_SET_META, key, "");
    PStore kvstore = expdb.value().store;

    auto ptxn = sess->getCtx()->createTransaction(kvstore);
    if (!ptxn.ok()) {
      return ptxn.status();
    }
    RecordKey fake = {
      expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_SET_ELE, key, ""};
    auto batch = Command::scan(fake.prefixPk(), "0", count, ptxn.value());
    if (!batch.ok()) {
      return batch.status();
    }
    const auto& rcds = batch.value().second;
    if (rcds.size() == 0) {
      return Command::fmtNull();
    }

    bool deleteMeta(false);
    INVARIANT_D(rcds.size() <= sm.getCount());
    if (rcds.size() >= sm.getCount()) {
      deleteMeta = true;
    }
    INVARIANT_D(rcds.size() == count || rcds.size() == sm.getCount());

    for (uint32_t i = 0; i < RETRY_CNT; ++i) {
      std::stringstream ss;
      if (rcds.size() > 1) {
        Command::fmtMultiBulkLen(ss, rcds.size());
      }
      auto ptxn = sess->getCtx()->createTransaction(kvstore);
      if (!ptxn.ok()) {
        return ptxn.status();
      }

      // avoid string copy, directly delete elements according to rcds.
      Status s;
      for (auto iter = rcds.begin(); iter != rcds.end(); iter++) {
        const RecordKey& subRk = iter->getRecordKey();
        s = kvstore->delKV(subRk, ptxn.value());
        if (!s.ok()) {
          return s;
        }
        Command::fmtBulk(ss, subRk.getSecondaryKey());
      }

      if (deleteMeta) {
        s = Command::delKeyAndTTL(sess, metaRk, rv.value(), ptxn.value());
        if (!s.ok()) {
          return s;
        }
      } else {
        sm.setCount(sm.getCount() - rcds.size());
        s = kvstore->setKV(metaRk,
                           RecordValue(sm.encode(),
                                       RecordType::RT_SET_META,
                                       pCtx->getVersionEP(),
                                       rv.value().getTtl(),
                                       rv),
                           ptxn.value());
        if (!s.ok()) {
          return s;
        }
      }

      auto expCmt = sess->getCtx()->commitTransaction(ptxn.value());
      if (expCmt.ok()) {
        return ss.str();
      }
      if (expCmt.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
        return expCmt.status();
      }
      if (i == RETRY_CNT - 1) {
        return expCmt.status();
      } else {
        continue;
      }
    }

    INVARIANT_D(0);
    return {ErrorCodes::ERR_INTERNAL, "not reachable"};
  }
} spopcmd;

class SaddCommand : public Command {
 public:
  SaddCommand() : Command("sadd", "wmF") {}

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
    const std::vector<std::string>& args = sess->getArgs();
    const std::string& key = args[1];

    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);

    auto server = sess->getServerEntry();
    auto expdb = server->getSegmentMgr()->getDbWithKeyLock(
      sess, key, mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
      return expdb.status();
    }

    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_SET_META);
    if (rv.status().code() != ErrorCodes::ERR_OK &&
        rv.status().code() != ErrorCodes::ERR_EXPIRED &&
        rv.status().code() != ErrorCodes::ERR_NOTFOUND) {
      return rv.status();
    }

    // uint32_t storeId = expdb.value().dbId;
    RecordKey metaRk(
      expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_SET_META, key, "");
    PStore kvstore = expdb.value().store;

    for (uint32_t i = 0; i < RETRY_CNT; ++i) {
      auto ptxn = sess->getCtx()->createTransaction(kvstore);
      if (!ptxn.ok()) {
        return ptxn.status();
      }

      Expected<std::string> s =
        genericSAdd(sess, kvstore, ptxn.value(), metaRk, rv, args);
      if (s.ok()) {
        auto s1 = sess->getCtx()->commitTransaction(ptxn.value());
        if (!s1.ok()) {
          return s1.status();
        }
        return s.value();
      }
      if (s.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
        return s.status();
      }
      if (i == RETRY_CNT - 1) {
        return s.status();
      } else {
        continue;
      }
    }

    INVARIANT_D(0);
    return {ErrorCodes::ERR_INTERNAL, "not reachable"};
  }
} saddCommand;

class ScardCommand : public Command {
 public:
  ScardCommand() : Command("scard", "rF") {}

  ssize_t arity() const {
    return 2;
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
    const std::string& key = sess->getArgs()[1];

    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);

    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_SET_META);

    if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
      return fmtZero();
    } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return fmtZero();
    } else if (!rv.status().ok()) {
      return rv.status();
    }

    Expected<SetMetaValue> exptSetMeta =
      SetMetaValue::decode(rv.value().getValue());
    if (!exptSetMeta.ok()) {
      return exptSetMeta.status();
    }
    return fmtLongLong(exptSetMeta.value().getCount());
  }
} scardCommand;

class SRemCommand : public Command {
 public:
  SRemCommand() : Command("srem", "wF") {}

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
    const std::vector<std::string>& args = sess->getArgs();
    const std::string& key = args[1];

    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);

    auto server = sess->getServerEntry();
    auto expdb = server->getSegmentMgr()->getDbWithKeyLock(
      sess, key, mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
      return expdb.status();
    }

    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_SET_META);
    if (rv.status().code() != ErrorCodes::ERR_OK &&
        rv.status().code() != ErrorCodes::ERR_EXPIRED &&
        rv.status().code() != ErrorCodes::ERR_NOTFOUND) {
      return rv.status();
    }

    RecordKey metaRk(
      expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_SET_META, key, "");

    PStore kvstore = expdb.value().store;

    std::vector<std::string> valArgs;
    for (uint32_t i = 2; i < args.size(); ++i) {
      valArgs.push_back(args[i]);
    }
    for (uint32_t i = 0; i < RETRY_CNT; ++i) {
      auto ptxn = sess->getCtx()->createTransaction(kvstore);
      if (!ptxn.ok()) {
        return ptxn.status();
      }
      Expected<std::string> s =
        genericSRem(sess, kvstore, ptxn.value(), metaRk, rv, valArgs);
      if (s.ok()) {
        auto s1 = sess->getCtx()->commitTransaction(ptxn.value());
        if (!s1.ok()) {
          return s1.status();
        }
        return s.value();
      }
      if (s.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
        return s.status();
      }
      if (i == RETRY_CNT - 1) {
        return s.status();
      } else {
        continue;
      }
    }

    INVARIANT_D(0);
    return {ErrorCodes::ERR_INTERNAL, "not reachable"};
  }
} sremCommand;

class SdiffgenericCommand : public Command {
 public:
  SdiffgenericCommand(const std::string& name, const char* sflags, bool store)
    : Command(name, sflags), _store(store) {}

  Expected<std::string> run(Session* sess) final {
    const std::vector<std::string>& args = sess->getArgs();
    size_t startkey = _store ? 2 : 1;
    std::set<std::string> result;
    auto server = sess->getServerEntry();
    SessionCtx* pCtx = sess->getCtx();

    std::vector<int> index = getKeysFromCommand(args);
    auto lock = server->getSegmentMgr()->getAllKeysLocked(
      sess, args, index, _store ? mgl::LockMode::LOCK_X : Command::RdLock());
    if (!lock.ok()) {
      return lock.status();
    }

    for (size_t i = startkey; i < args.size(); ++i) {
      Expected<RecordValue> rv =
        Command::expireKeyIfNeeded(sess, args[i], RecordType::RT_SET_META);
      if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
        continue;
      } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
        continue;
      } else if (!rv.ok()) {
        return rv.status();
      }

      auto expdb = server->getSegmentMgr()->getDbHasLocked(sess, args[i]);
      if (!expdb.ok()) {
        return expdb.status();
      }

      PStore kvstore = expdb.value().store;
      auto ptxn = sess->getCtx()->createTransaction(kvstore);
      if (!ptxn.ok()) {
        return ptxn.status();
      }
      auto cursor = ptxn.value()->createDataCursor();
      RecordKey fake = {expdb.value().chunkId,
                        pCtx->getDbId(),
                        RecordType::RT_SET_ELE,
                        args[i],
                        ""};
      cursor->seek(fake.prefixPk());
      while (true) {
        Expected<Record> exptRcd = cursor->next();
        if (exptRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
          break;
        }
        if (!exptRcd.ok()) {
          return exptRcd.status();
        }
        Record& rcd = exptRcd.value();
        const RecordKey& rcdkey = rcd.getRecordKey();
        if (rcdkey.prefixPk() != fake.prefixPk()) {
          break;
        }
        if (i == startkey) {
          result.insert(rcdkey.getSecondaryKey());
        } else {
          result.erase(rcdkey.getSecondaryKey());
        }
      }
    }

    if (!_store) {
      std::stringstream ss;
      Command::fmtMultiBulkLen(ss, result.size());
      for (auto& v : result) {
        Command::fmtBulk(ss, v);
      }
      return ss.str();
    }

    const std::string& storeKey = args[1];
    auto expdb = server->getSegmentMgr()->getDbHasLocked(sess, storeKey);
    if (!expdb.ok()) {
      return expdb.status();
    }
    PStore kvstore = expdb.value().store;
    auto ptxn = sess->getCtx()->createTransaction(kvstore);
    if (!ptxn.ok()) {
      return ptxn.status();
    }

    Expected<bool> deleted = delGeneric(sess, storeKey, ptxn.value());
    // Expected<bool> deleted = Command::delKeyChkExpire(sess, storeKey,
    // RecordType::RT_SET_META);
    if (!deleted.ok()) {
      return deleted.status();
    }

    std::vector<std::string> newKeys(2);
    for (auto& v : result) {
      newKeys.push_back(std::move(v));
    }

    RecordKey storeRk(expdb.value().chunkId,
                      pCtx->getDbId(),
                      RecordType::RT_SET_META,
                      storeKey,
                      "");
    for (uint32_t i = 0; i < RETRY_CNT; ++i) {
      Expected<std::string> addStore = genericSAdd(
        sess,
        kvstore,
        ptxn.value(),
        storeRk,
        {ErrorCodes::ERR_NOTFOUND, ""}, /* storeKey has been deleted */
        newKeys);
      if (addStore.ok()) {
        auto s1 = sess->getCtx()->commitTransaction(ptxn.value());
        if (!s1.ok()) {
          return s1.status();
        }
        return addStore.value();
      }
      if (addStore.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
        return addStore.status();
      }
      if (i == RETRY_CNT - 1) {
        return addStore.status();
      } else {
        continue;
      }
    }
    return {ErrorCodes::ERR_INTERNAL, "currently unrechable"};
  }

 private:
  bool _store;
};

class SdiffCommand : public SdiffgenericCommand {
 public:
  SdiffCommand() : SdiffgenericCommand("sdiff", "rS", false) {}

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
} sdiffcmd;

class SdiffStoreCommand : public SdiffgenericCommand {
 public:
  SdiffStoreCommand() : SdiffgenericCommand("sdiffstore", "wm", true) {}

  ssize_t arity() const {
    return -3;
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
} sdiffstoreCommand;

// Implement O(n*m) intersection
// which n is the cardinality of the smallest set
// m is the num of sets input
class SintergenericCommand : public Command {
 public:
  SintergenericCommand(const std::string& name, const char* sflags, bool store)
    : Command(name, sflags), _store(store) {}

  Expected<std::string> run(Session* sess) final {
    const std::vector<std::string>& args = sess->getArgs();
    size_t startkey = _store ? 2 : 1;
    std::set<std::string> result;
    auto server = sess->getServerEntry();
    SessionCtx* pCtx = sess->getCtx();

    std::vector<int> index = getKeysFromCommand(args);
    auto lock = server->getSegmentMgr()->getAllKeysLocked(
      sess, args, index, _store ? mgl::LockMode::LOCK_X : Command::RdLock());
    if (!lock.ok()) {
      return lock.status();
    }

    // stored all sets sorted by their length
    std::vector<std::pair<size_t, uint64_t>> setList;
    for (size_t i = startkey; i < args.size(); i++) {
      Expected<RecordValue> rv =
        Command::expireKeyIfNeeded(sess, args[i], RecordType::RT_SET_META);

      // if one set is empty, their intersection is empty set, so just
      // return it.
      if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
          rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
        if (_store) {
          continue;
        }
        return Command::fmtNull();
      } else if (!rv.ok()) {
        return rv.status();
      }

      Expected<SetMetaValue> expSetMeta =
        SetMetaValue::decode(rv.value().getValue());

      uint64_t setLength = expSetMeta.value().getCount();
      if (setLength == 0) {
        if (_store) {
          continue;
        }
        return Command::fmtNull();
      }
      setList.push_back(std::make_pair(i, setLength));
    }
    std::sort(setList.begin(), setList.end(), [](auto& left, auto& right) {
      return left.second < right.second;
    });

    for (size_t i = 0; i < setList.size(); i++) {
      const std::string& key = args[setList[i].first];
      auto expdb = server->getSegmentMgr()->getDbHasLocked(sess, key);
      if (!expdb.ok()) {
        return expdb.status();
      }
      PStore kvstore = expdb.value().store;
      auto ptxn = sess->getCtx()->createTransaction(kvstore);
      if (!ptxn.ok()) {
        return ptxn.status();
      }
      if (i == 0) {
        auto cursor = ptxn.value()->createDataCursor();
        RecordKey fakeRk(expdb.value().chunkId,
                         pCtx->getDbId(),
                         RecordType::RT_SET_ELE,
                         key,
                         "");
        cursor->seek(fakeRk.prefixPk());
        while (true) {
          Expected<Record> expRcd = cursor->next();
          if (expRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
            break;
          }
          if (!expRcd.ok()) {
            return expRcd.status();
          }
          Record& rcd = expRcd.value();
          const RecordKey& rcdKey = rcd.getRecordKey();
          if (rcdKey.prefixPk() != fakeRk.prefixPk()) {
            break;
          }
          result.insert(rcdKey.getSecondaryKey());
        }
        // for the smallest set
        // input all its keys into set, then goto next loop;
        continue;
      }
      if (result.size() == 0) {
        if (_store) {
          // we must del the storeKey before we return
          break;
        }
        // has no intersection, just return empty set
        return Command::fmtNull();
      }

      for (auto iter = result.begin(); iter != result.end();) {
        RecordKey subRk(expdb.value().chunkId,
                        pCtx->getDbId(),
                        RecordType::RT_SET_ELE,
                        key,
                        *iter);
        Expected<RecordValue> subValue = kvstore->getKV(subRk, ptxn.value());
        // if key not found, erase it
        if (!subValue.ok() ||
            subValue.status().code() == ErrorCodes::ERR_NOTFOUND) {
          // then the old iterator will be invalid
          // new value is the iterator to the next element
          iter = result.erase(iter);
        } else {
          // otherwise move forward manually
          iter++;
        }
      }
    }

    if (!_store) {
      std::stringstream ss;
      Command::fmtMultiBulkLen(ss, result.size());
      for (auto& v : result) {
        Command::fmtBulk(ss, v);
      }
      return ss.str();
    }

    const std::string& storeKey = args[1];
    auto expdb = server->getSegmentMgr()->getDbHasLocked(sess, storeKey);
    if (!expdb.ok()) {
      return expdb.status();
    }
    PStore kvstore = expdb.value().store;
    auto ptxn = sess->getCtx()->createTransaction(kvstore);
    if (!ptxn.ok()) {
      return ptxn.status();
    }

    Expected<bool> deleted = delGeneric(sess, storeKey, ptxn.value());
    // Expected<bool> deleted = Command::delKeyChkExpire(sess, storeKey,
    // RecordType::RT_SET_META);
    if (!deleted.ok()) {
      return deleted.status();
    }

    std::vector<std::string> newKeys(2);
    for (auto& v : result) {
      newKeys.push_back(std::move(v));
    }


    RecordKey storeRk(expdb.value().chunkId,
                      pCtx->getDbId(),
                      RecordType::RT_SET_META,
                      storeKey,
                      "");
    for (uint32_t i = 0; i < RETRY_CNT; ++i) {
      Expected<std::string> addStore =
        genericSAdd(sess,
                    kvstore,
                    ptxn.value(),
                    storeRk,
                    {ErrorCodes::ERR_NOTFOUND, ""},
                    newKeys);
      if (addStore.ok()) {
        auto s1 = sess->getCtx()->commitTransaction(ptxn.value());
        if (!s1.ok()) {
          return s1.status();
        }
        return addStore.value();
      }
      return addStore.status();
    }
    return {ErrorCodes::ERR_INTERNAL, "currently unrechable"};
  }

 private:
  bool _store;
};

class SinterCommand : public SintergenericCommand {
 public:
  SinterCommand() : SintergenericCommand("sinter", "rS", false) {}

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
} sinterCommand;

class SinterStoreCommand : public SintergenericCommand {
 public:
  SinterStoreCommand() : SintergenericCommand("sinterstore", "wm", true) {}

  ssize_t arity() const {
    return -3;
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
} sinterstoreCommand;

class SmoveCommand : public Command {
 public:
  SmoveCommand() : Command("smove", "wF") {}

  ssize_t arity() const {
    return 4;
  }

  int32_t firstkey() const {
    return 1;
  }

  int32_t lastkey() const {
    return 2;
  }

  int32_t keystep() const {
    return 1;
  }

  Expected<std::string> run(Session* sess) final {
    const std::vector<std::string>& args = sess->getArgs();
    const std::string& source = args[1];
    const std::string& dest = args[2];
    const std::string& member = args[3];

    SessionCtx* pCtx = sess->getCtx();
    auto server = sess->getServerEntry();

    std::vector<int> index = getKeysFromCommand(args);
    auto lock = server->getSegmentMgr()->getAllKeysLocked(
      sess, args, index, mgl::LockMode::LOCK_X);
    if (!lock.ok()) {
      return lock.status();
    }

    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, source, RecordType::RT_SET_META);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
        rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtZero();
    } else if (!rv.ok()) {
      return rv.status();
    }

    Expected<RecordValue> destRv =
      Command::expireKeyIfNeeded(sess, dest, RecordType::RT_SET_META);
    if (!destRv.ok() && destRv.status().code() != ErrorCodes::ERR_EXPIRED &&
        destRv.status().code() != ErrorCodes::ERR_NOTFOUND) {
      return destRv.status();
    }

    auto srcDb = server->getSegmentMgr()->getDbHasLocked(sess, source);
    if (!srcDb.ok()) {
      return srcDb.status();
    }
    auto destDb = server->getSegmentMgr()->getDbHasLocked(sess, dest);
    if (!destDb.ok()) {
      return destDb.status();
    }

    PStore srcStore = srcDb.value().store;
    PStore destStore = destDb.value().store;

    auto etxn = pCtx->createTransaction(srcStore);
    if (!etxn.ok()) {
      return etxn.status();
    }
    bool rollback = true;
    const auto guard = MakeGuard([&rollback, &pCtx] {
      if (rollback) {
        pCtx->rollbackAll();
      }
    });

    RecordKey remRk(srcDb.value().chunkId,
                    pCtx->getDbId(),
                    RecordType::RT_SET_META,
                    source,
                    "");
    // directly remove member from source
    const std::string& formatRet = Command::fmtLongLong(1);
    for (uint32_t i = 0; i < RETRY_CNT; ++i) {
      Expected<std::string> remRet =
        genericSRem(sess, srcStore, etxn.value(), remRk, rv, {member});
      if (remRet.ok()) {
        if (remRet.value() == formatRet) {
          break;
        } else {
          return Command::fmtZero();
        }
      }
      if (remRet.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
        return remRet.status();
      }
      if (i == RETRY_CNT - 1) {
        return remRet.status();
      } else {
        continue;
      }
    }

    if (source == dest) {
      // NOTE(vinchen): if source == dest, do nothing and return 1.
      return Command::fmtOne();
    } else {
      auto etxn2 = pCtx->createTransaction(destStore);
      if (!etxn2.ok()) {
        return etxn2.status();
      }

      RecordKey addRk(destDb.value().chunkId,
                      pCtx->getDbId(),
                      RecordType::RT_SET_META,
                      dest,
                      "");
      // add member to dest
      for (uint32_t i = 0; i < RETRY_CNT; ++i) {
        Expected<std::string> addRet = genericSAdd(
          sess, destStore, etxn2.value(), addRk, destRv, {"", "", member});
        if (addRet.ok()) {
          pCtx->commitAll("smove");
          rollback = false;
          return addRet.value();
        }
        if (addRet.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
          return addRet.status();
        }
        if (i == RETRY_CNT - 1) {
          return addRet.status();
        } else {
          continue;
        }
      }
    }

    return {ErrorCodes::ERR_INTERNAL, "not reachable"};
  }
} smoveCommand;

class SuniongenericCommand : public Command {
 public:
  SuniongenericCommand(const std::string& name, const char* sflags, bool store)
    : Command(name, sflags), _store(store) {}

  Expected<std::string> run(Session* sess) final {
    const std::vector<std::string>& args = sess->getArgs();
    size_t startkey = _store ? 2 : 1;
    std::unordered_set<std::string> result;
    auto server = sess->getServerEntry();
    SessionCtx* pCtx = sess->getCtx();

    std::vector<int> index = getKeysFromCommand(args);
    auto lock = server->getSegmentMgr()->getAllKeysLocked(
      sess, args, index, _store ? mgl::LockMode::LOCK_X : Command::RdLock());
    if (!lock.ok()) {
      return lock.status();
    }

    for (size_t i = startkey; i < args.size(); ++i) {
      Expected<RecordValue> rv =
        Command::expireKeyIfNeeded(sess, args[i], RecordType::RT_SET_META);
      if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
          rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
        continue;
      } else if (!rv.ok()) {
        return rv.status();
      }

      auto expdb = server->getSegmentMgr()->getDbHasLocked(sess, args[i]);
      if (!expdb.ok()) {
        return expdb.status();
      }
      PStore kvstore = expdb.value().store;
      auto ptxn = sess->getCtx()->createTransaction(kvstore);
      if (!ptxn.ok()) {
        return ptxn.status();
      }
      auto cursor = ptxn.value()->createDataCursor();
      RecordKey fakeRk(expdb.value().chunkId,
                       pCtx->getDbId(),
                       RecordType::RT_SET_ELE,
                       args[i],
                       "");
      cursor->seek(fakeRk.prefixPk());
      while (true) {
        Expected<Record> exptRcd = cursor->next();
        if (exptRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
          break;
        }
        if (!exptRcd.ok()) {
          return exptRcd.status();
        }

        Record& rcd = exptRcd.value();
        const RecordKey& rcdkey = rcd.getRecordKey();
        if (rcdkey.prefixPk() != fakeRk.prefixPk()) {
          break;
        }
        result.insert(rcdkey.getSecondaryKey());
      }
    }

    if (!_store) {
      std::stringstream ss;
      Command::fmtMultiBulkLen(ss, result.size());
      for (const auto& v : result) {
        Command::fmtBulk(ss, v);
      }
      return ss.str();
    }

    const std::string& storeKey = args[1];
    auto expdb = server->getSegmentMgr()->getDbHasLocked(sess, storeKey);
    if (!expdb.ok()) {
      return expdb.status();
    }
    PStore kvstore = expdb.value().store;
    auto ptxn = sess->getCtx()->createTransaction(kvstore);
    if (!ptxn.ok()) {
      return ptxn.status();
    }
    Expected<bool> deleted = delGeneric(sess, storeKey, ptxn.value());
    if (!deleted.ok()) {
      return deleted.status();
    }

    std::vector<std::string> newKeys(2);
    for (const auto& v : result) {
      newKeys.push_back(std::move(v));
    }

    RecordKey storeRk(expdb.value().chunkId,
                      pCtx->getDbId(),
                      RecordType::RT_SET_META,
                      storeKey,
                      "");
    for (uint32_t i = 0; i < RETRY_CNT; ++i) {
      Expected<std::string> addStore =
        genericSAdd(sess,
                    kvstore,
                    ptxn.value(),
                    storeRk,
                    {ErrorCodes::ERR_NOTFOUND, ""},
                    newKeys);
      if (addStore.ok()) {
        auto s1 = sess->getCtx()->commitTransaction(ptxn.value());
        if (!s1.ok()) {
          return s1.status();
        }
        return addStore.value();
      }
      if (addStore.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
        return addStore.status();
      }
      if (i == RETRY_CNT - 1) {
        return addStore.status();
      } else {
        continue;
      }
    }
    return {ErrorCodes::ERR_INTERNAL, "not reachable"};
  }

 private:
  bool _store;
};

class SunionCommand : public SuniongenericCommand {
 public:
  SunionCommand() : SuniongenericCommand("sunion", "rS", false) {}

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
} sunionCommand;

class SunionStoreCommand : public SuniongenericCommand {
 public:
  SunionStoreCommand() : SuniongenericCommand("sunionstore", "wm", true) {}

  ssize_t arity() const {
    return -3;
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
} sunionstoreCommand;

}  // namespace tendisplus
