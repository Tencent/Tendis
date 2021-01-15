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
#include "glog/logging.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/storage/skiplist.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/storage/varint.h"

namespace tendisplus {
Expected<bool> delGeneric(Session* sess, const std::string& key,
        Transaction* txn);
Expected<std::string> genericZrem(Session* sess,
                                  PStore kvstore,
                                  const RecordKey& mk,
                                  const Expected<RecordValue>& eMeta,
                                  const std::vector<std::string>& subkeys) {
  SessionCtx* pCtx = sess->getCtx();
  INVARIANT(pCtx != nullptr);
  auto ptxn = sess->getCtx()->createTransaction(kvstore);
  if (!ptxn.ok()) {
    return ptxn.status();
  }
  if (!eMeta.ok()) {
    if (eMeta.status().code() == ErrorCodes::ERR_NOTFOUND ||
        eMeta.status().code() == ErrorCodes::ERR_EXPIRED) {
      return Command::fmtZero();
    } else {
      return eMeta.status();
    }
  }
  auto eMetaContent = ZSlMetaValue::decode(eMeta.value().getValue());
  if (!eMetaContent.ok()) {
    return eMetaContent.status();
  }
  ZSlMetaValue meta = eMetaContent.value();
  SkipList sl(mk.getChunkId(), mk.getDbId(), mk.getPrimaryKey(), meta, kvstore);

  uint32_t cnt = 0;
  for (const auto& subkey : subkeys) {
    RecordKey hk(mk.getChunkId(),
                 pCtx->getDbId(),
                 RecordType::RT_ZSET_H_ELE,
                 mk.getPrimaryKey(),
                 subkey);
    Expected<RecordValue> eValue = kvstore->getKV(hk, ptxn.value());
    if (!eValue.ok() && eValue.status().code() != ErrorCodes::ERR_NOTFOUND) {
      return eValue.status();
    }
    if (eValue.status().code() == ErrorCodes::ERR_NOTFOUND) {
      continue;
    } else {
      cnt += 1;
      Expected<double> oldScore =
        ::tendisplus::doubleDecode(eValue.value().getValue());
      if (!oldScore.ok()) {
        return oldScore.status();
      }
      Status s = sl.remove(oldScore.value(), subkey, ptxn.value());
      if (!s.ok()) {
        return s;
      }
      s = kvstore->delKV(hk, ptxn.value());
      if (!s.ok()) {
        return s;
      }
    }
  }
  Status s;
  if (sl.getCount() > 1) {
    s = sl.save(ptxn.value(), eMeta, pCtx->getVersionEP());
  } else {
    INVARIANT(sl.getCount() == 1);
    s = Command::delKeyAndTTL(sess, mk, eMeta.value(), ptxn.value());
    if (!s.ok()) {
      return s;
    }
    RecordKey head(mk.getChunkId(),
                   pCtx->getDbId(),
                   RecordType::RT_ZSET_S_ELE,
                   mk.getPrimaryKey(),
                   std::to_string(ZSlMetaValue::HEAD_ID));
    s = kvstore->delKV(head, ptxn.value());
  }
  if (!s.ok()) {
    return s;
  }
  Expected<uint64_t> commitStatus = sess->getCtx()->commitTransaction(
          ptxn.value());
  if (!commitStatus.ok()) {
    return commitStatus.status();
  }
  return Command::fmtLongLong(cnt);
}

Expected<std::string> genericZadd(Session* sess,
                                  PStore kvstore,
                                  const RecordKey& mk,
                                  const Expected<RecordValue>& eMeta,
                                  const std::map<std::string, double>& subKeys,
                                  int flags,
                                  Transaction* txn) {
  /* The following vars are used in order to track what the command actually
   * did during the execution, to reply to the client and to trigger the
   * notification of keyspace change. */
  int added = 0;   /* Number of new elements added. */
  int updated = 0; /* Number of elements with updated score. */
  /* Number of elements processed, may remain zero with options like XX. */
  int processed = 0;
  /* Turn options into simple to check vars. */
  bool incr = (flags & ZADD_INCR) != 0;
  bool nx = (flags & ZADD_NX) != 0;
  bool xx = (flags & ZADD_XX) != 0;
  bool ch = (flags & ZADD_CH) != 0;

  /* Check for incompatible options. */
  if (nx && xx) {
    return {ErrorCodes::ERR_PARSEPKT,
            "XX and NX options at the same time are not compatible"};
  }

  if (incr && subKeys.size() > 1) {
    return {ErrorCodes::ERR_PARSEPKT,
            "INCR option supports a single increment-element pair"};
  }

  SessionCtx* pCtx = sess->getCtx();

  ZSlMetaValue meta;

  if (eMeta.ok()) {
    auto eMetaContent = ZSlMetaValue::decode(eMeta.value().getValue());
    if (!eMetaContent.ok()) {
      return eMetaContent.status();
    }
    meta = eMetaContent.value();
  } else {
    INVARIANT_D(eMeta.status().code() == ErrorCodes::ERR_NOTFOUND ||
                eMeta.status().code() == ErrorCodes::ERR_EXPIRED);
    // head node also included into the count
    ZSlMetaValue tmp(1 /*lvl*/, 1 /*count*/, 0 /*tail*/);
    RecordValue rv(
      tmp.encode(), RecordType::RT_ZSET_META, pCtx->getVersionEP());
    Status s = kvstore->setKV(mk, rv, txn);
    if (!s.ok()) {
      return s;
    }
    RecordKey head(mk.getChunkId(),
                   pCtx->getDbId(),
                   RecordType::RT_ZSET_S_ELE,
                   mk.getPrimaryKey(),
                   std::to_string(ZSlMetaValue::HEAD_ID));
    ZSlEleValue headVal;
    RecordValue subRv(headVal.encode(), RecordType::RT_ZSET_S_ELE, -1);
    s = kvstore->setKV(head, subRv, txn);
    if (!s.ok()) {
      return s;
    }
    Expected<RecordValue> eMeta = kvstore->getKV(mk, txn);
    if (!eMeta.ok()) {
      return eMeta.status();
    }
    auto eMetaContent = ZSlMetaValue::decode(eMeta.value().getValue());
    if (!eMetaContent.ok()) {
      return eMetaContent.status();
    }
    meta = eMetaContent.value();
  }

  SkipList sl(mk.getChunkId(), mk.getDbId(), mk.getPrimaryKey(), meta, kvstore);
  std::stringstream ss;
  double newScore = 0;
  // sl.traverse(ss, ptxn.value());
  for (const auto& entry : subKeys) {
    RecordKey hk(mk.getChunkId(),
                 pCtx->getDbId(),
                 RecordType::RT_ZSET_H_ELE,
                 mk.getPrimaryKey(),
                 entry.first);
    newScore = entry.second;
    if (std::isnan(newScore)) {
      return {ErrorCodes::ERR_NAN, ""};
    }
    Expected<RecordValue> eValue = kvstore->getKV(hk, txn);
    if (!eValue.ok() && eValue.status().code() != ErrorCodes::ERR_NOTFOUND) {
      return eValue.status();
    }
    if (eValue.status().code() == ErrorCodes::ERR_NOTFOUND) {
      if (xx) {
        continue;
      }
      added++;
      processed++;
      Status s = sl.insert(entry.second, entry.first, txn);
      if (!s.ok()) {
        return s;
      }
      RecordValue hv(newScore, RecordType::RT_ZSET_H_ELE);
      s = kvstore->setKV(hk, hv, txn);
      if (!s.ok()) {
        return s;
      }
    } else {
      if (nx) {
        continue;
      }
      Expected<double> oldScore =
        ::tendisplus::doubleDecode(eValue.value().getValue());
      if (!oldScore.ok()) {
        return oldScore.status();
      }
      if (incr) {
        newScore += oldScore.value();
        if (std::isnan(newScore)) {
          return {ErrorCodes::ERR_NAN, ""};
        }
      }

      if (newScore == oldScore.value()) {
        continue;
      }
      updated++;
      processed++;
      // change score
      Status s = sl.remove(oldScore.value(), entry.first, txn);
      if (!s.ok()) {
        return s;
      }
      s = sl.insert(newScore, entry.first, txn);
      if (!s.ok()) {
        return s;
      }
      RecordValue hv(newScore, RecordType::RT_ZSET_H_ELE);
      s = kvstore->setKV(hk, hv, txn);
      if (!s.ok()) {
        return s;
      }
    }
  }
  // NOTE(vinchen): skiplist save one time
  Status s = sl.save(txn, eMeta, sess->getCtx()->getVersionEP());
  if (!s.ok()) {
    return s;
  }

  if (incr) { /* ZINCRBY or INCR option. */
    if (processed)
      return Command::fmtBulk(::tendisplus::dtos(newScore));
    else
      return Command::fmtNull();
  } else { /* ZADD */
    return Command::fmtLongLong(ch ? added + updated : added);
  }
}

Expected<std::string> genericZRank(Session* sess,
                                   PStore kvstore,
                                   const RecordKey& mk,
                                   const RecordValue& mv,
                                   const std::string& subkey,
                                   bool reverse) {
  auto ptxn = sess->getCtx()->createTransaction(kvstore);
  if (!ptxn.ok()) {
    return ptxn.status();
  }

  RecordKey hk(mk.getChunkId(),
               mk.getDbId(),
               RecordType::RT_ZSET_H_ELE,
               mk.getPrimaryKey(),
               subkey);
  Expected<RecordValue> eValue =
    kvstore->getKV(hk, ptxn.value(), RecordType::RT_ZSET_H_ELE);
  if (!eValue.ok()) {
    if (eValue.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtNull();
    }
    return eValue.status();
  }
  Expected<double> score =
    ::tendisplus::doubleDecode(eValue.value().getValue());
  if (!score.ok()) {
    return score.status();
  }

  auto eMetaContent = ZSlMetaValue::decode(mv.getValue());
  if (!eMetaContent.ok()) {
    return eMetaContent.status();
  }
  const ZSlMetaValue& meta = eMetaContent.value();
  SkipList sl(mk.getChunkId(), mk.getDbId(), mk.getPrimaryKey(), meta, kvstore);
  Expected<uint32_t> rank = sl.rank(score.value(), subkey, ptxn.value());
  if (!rank.ok()) {
    return rank.status();
  }
  int64_t r = rank.value() - 1;
  if (reverse) {
    r = meta.getCount() - 2 - r;
  }
  INVARIANT_D(r >= 0);
  return Command::fmtLongLong(r);
}

class ZRemByRangeGenericCommand : public Command {
 public:
  enum class Type {
    RANK,
    SCORE,
    LEX,
  };

  ZRemByRangeGenericCommand(const std::string& name, const char* sflags)
    : Command(name, sflags) {
    if (name == "zremrangebyscore") {
      _type = Type::SCORE;
    } else if (name == "zremrangebylex") {
      _type = Type::LEX;
    } else if (name == "zremrangebyrank") {
      _type = Type::RANK;
    } else {
      INVARIANT_D(0);
    }
  }

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

  Expected<std::string> genericZremrange(Session* sess,
                                         PStore kvstore,
                                         const RecordKey& mk,
                                         const Expected<RecordValue>& eMeta,
                                         const Zrangespec& range,
                                         const Zlexrangespec& lexrange,
                                         int64_t start,
                                         int64_t end) {
    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    auto ptxn = sess->getCtx()->createTransaction(kvstore);
    if (!ptxn.ok()) {
      return ptxn.status();
    }

    if (!eMeta.ok()) {
      if (eMeta.status().code() == ErrorCodes::ERR_NOTFOUND ||
          eMeta.status().code() == ErrorCodes::ERR_EXPIRED) {
        return Command::fmtZero();
      } else {
        return eMeta.status();
      }
    }
    auto eMetaContent = ZSlMetaValue::decode(eMeta.value().getValue());
    if (!eMetaContent.ok()) {
      return eMetaContent.status();
    }
    ZSlMetaValue meta = eMetaContent.value();
    SkipList sl(
      mk.getChunkId(), mk.getDbId(), mk.getPrimaryKey(), meta, kvstore);

    if (_type == Type::RANK) {
      int64_t llen = sl.getCount() - 1;
      if (start < 0) {
        start = llen + start;
      }
      if (end < 0) {
        end = llen + end;
      }
      if (start < 0) {
        start = 0;
      }
      if (start > end || start >= llen) {
        return Command::fmtZero();
      }
      if (end >= llen) {
        end = llen - 1;
      }
    }

    std::list<std::pair<double, std::string>> result;
    if (_type == Type::RANK) {
      auto tmp = sl.removeRangeByRank(start + 1, end + 1, ptxn.value());
      if (!tmp.ok()) {
        return tmp.status();
      }
      result = std::move(tmp.value());
    } else if (_type == Type::SCORE) {
      auto tmp = sl.removeRangeByScore(range, ptxn.value());
      if (!tmp.ok()) {
        return tmp.status();
      }
      result = std::move(tmp.value());
    } else if (_type == Type::LEX) {
      auto tmp = sl.removeRangeByLex(lexrange, ptxn.value());
      if (!tmp.ok()) {
        return tmp.status();
      }
      result = std::move(tmp.value());
    }
    for (const auto& v : result) {
      RecordKey hk(mk.getChunkId(),
                   pCtx->getDbId(),
                   RecordType::RT_ZSET_H_ELE,
                   mk.getPrimaryKey(),
                   v.second);
      auto s = kvstore->delKV(hk, ptxn.value());
      if (!s.ok()) {
        return s;
      }
    }

    Status s;
    if (sl.getCount() > 1) {
      s = sl.save(ptxn.value(), eMeta, pCtx->getVersionEP());
    } else {
      INVARIANT(sl.getCount() == 1);
      s = Command::delKeyAndTTL(sess, mk, eMeta.value(), ptxn.value());
      if (!s.ok()) {
        return s;
      }
      RecordKey head(mk.getChunkId(),
                     pCtx->getDbId(),
                     RecordType::RT_ZSET_S_ELE,
                     mk.getPrimaryKey(),
                     std::to_string(ZSlMetaValue::HEAD_ID));
      s = kvstore->delKV(head, ptxn.value());
    }
    if (!s.ok()) {
      return s;
    }
    Expected<uint64_t> commitStatus = sess->getCtx()->commitTransaction(
            ptxn.value());
    if (!commitStatus.ok()) {
      return commitStatus.status();
    }
    return Command::fmtLongLong(result.size());
  }

  Expected<std::string> run(Session* sess) final {
    const std::vector<std::string>& args = sess->getArgs();
    const std::string& key = args[1];
    Zrangespec range;
    Zlexrangespec lexrange;
    int64_t start(0), end(0);
    if (_type == Type::RANK) {
      Expected<int64_t> estart = ::tendisplus::stoll(args[2]);
      if (!estart.ok()) {
        return estart.status();
      }
      Expected<int64_t> eend = ::tendisplus::stoll(args[3]);
      if (!eend.ok()) {
        return eend.status();
      }
      start = estart.value();
      end = eend.value();
    } else if (_type == Type::SCORE) {
      if (zslParseRange(args[2].c_str(), args[3].c_str(), &range) != 0) {
        return {ErrorCodes::ERR_ZSLPARSERANGE, ""};
      }
    } else if (_type == Type::LEX) {
      if (zslParseLexRange(args[2].c_str(), args[3].c_str(), &lexrange) !=
          0) {  // NOLINT:whitespace/line_length
        return {ErrorCodes::ERR_ZSLPARSELEXRANGE, ""};
      }
    }

    auto server = sess->getServerEntry();
    auto expdb = server->getSegmentMgr()->getDbWithKeyLock(
      sess, key, mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
      return expdb.status();
    }
    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_ZSET_META);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
        rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtZero();
    } else if (!rv.ok()) {
      return rv.status();
    }

    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);

    RecordKey metaRk(expdb.value().chunkId,
                     pCtx->getDbId(),
                     RecordType::RT_ZSET_META,
                     key,
                     "");
    PStore kvstore = expdb.value().store;

    for (int32_t i = 0; i < RETRY_CNT; ++i) {
      Expected<std::string> s = genericZremrange(
        sess, kvstore, metaRk, rv, range, lexrange, start, end);
      if (s.ok()) {
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

 private:
  Type _type;
};

class ZRemRangeByRankCommand : public ZRemByRangeGenericCommand {
 public:
  ZRemRangeByRankCommand()
    : ZRemByRangeGenericCommand("zremrangebyrank", "w") {}
} zremrangebyrankCmd;

class ZRemRangeByScoreCommand : public ZRemByRangeGenericCommand {
 public:
  ZRemRangeByScoreCommand()
    : ZRemByRangeGenericCommand("zremrangebyscore", "w") {}
} zremrangebyscoreCmd;

class ZRemRangeByLexCommand : public ZRemByRangeGenericCommand {
 public:
  ZRemRangeByLexCommand() : ZRemByRangeGenericCommand("zremrangebylex", "w") {}
} zremrangebylexCmd;

class ZRemCommand : public Command {
 public:
  ZRemCommand() : Command("zrem", "wF") {}

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
    std::vector<std::string> subkeys;
    for (size_t i = 2; i < args.size(); ++i) {
      subkeys.push_back(args[i]);
    }

    auto server = sess->getServerEntry();
    auto expdb = server->getSegmentMgr()->getDbWithKeyLock(
      sess, key, mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
      return expdb.status();
    }

    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_ZSET_META);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
        rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtZero();
    } else if (!rv.ok()) {
      return rv.status();
    }

    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);

    RecordKey metaRk(expdb.value().chunkId,
                     pCtx->getDbId(),
                     RecordType::RT_ZSET_META,
                     key,
                     "");
    PStore kvstore = expdb.value().store;

    for (int32_t i = 0; i < RETRY_CNT; ++i) {
      Expected<std::string> s = genericZrem(sess, kvstore, metaRk, rv, subkeys);
      if (s.ok()) {
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
} zremCmd;

class ZCardCommand : public Command {
 public:
  ZCardCommand() : Command("zcard", "rF") {}

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

    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_ZSET_META);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
        rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtZero();
    } else if (!rv.ok()) {
      return rv.status();
    }
    auto eMetaContent = ZSlMetaValue::decode(rv.value().getValue());
    if (!eMetaContent.ok()) {
      return eMetaContent.status();
    }
    ZSlMetaValue meta = eMetaContent.value();
    INVARIANT_D(meta.getCount() > 1);
    return Command::fmtLongLong(meta.getCount() - 1);
  }
} zcardCmd;

class ZRankCommand : public Command {
 public:
  ZRankCommand() : Command("zrank", "rF") {}

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

    auto server = sess->getServerEntry();
    auto expdb =
      server->getSegmentMgr()->getDbWithKeyLock(sess, key, Command::RdLock());
    if (!expdb.ok()) {
      return expdb.status();
    }
    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_ZSET_META);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
        rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtNull();
    } else if (!rv.ok()) {
      return rv.status();
    }

    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    RecordKey metaRk(expdb.value().chunkId,
                     pCtx->getDbId(),
                     RecordType::RT_ZSET_META,
                     key,
                     "");
    PStore kvstore = expdb.value().store;

    return genericZRank(sess, kvstore, metaRk, rv.value(), subkey, false);
  }
} zrankCmd;


class ZRevRankCommand : public Command {
 public:
  ZRevRankCommand() : Command("zrevrank", "rF") {}

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

    auto server = sess->getServerEntry();
    auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key, RdLock());
    if (!expdb.ok()) {
      return expdb.status();
    }

    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_ZSET_META);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
        rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtNull();
    } else if (!rv.ok()) {
      return rv.status();
    }

    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    RecordKey metaRk(expdb.value().chunkId,
                     pCtx->getDbId(),
                     RecordType::RT_ZSET_META,
                     key,
                     "");
    PStore kvstore = expdb.value().store;

    return genericZRank(sess, kvstore, metaRk, rv.value(), subkey, true);
  }
} zrevrankCmd;

class ZIncrCommand : public Command {
 public:
  ZIncrCommand() : Command("zincrby", "wmF") {}

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
    const std::vector<std::string>& args = sess->getArgs();
    const std::string& key = args[1];
    Expected<double> score = ::tendisplus::stod(args[2]);
    if (!score.ok()) {
      return score.status();
    }
    const std::string& subkey = args[3];

    auto server = sess->getServerEntry();
    auto expdb = server->getSegmentMgr()->getDbWithKeyLock(
      sess, key, mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
      return expdb.status();
    }

    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_ZSET_META);
    if (rv.status().code() != ErrorCodes::ERR_OK &&
        rv.status().code() != ErrorCodes::ERR_EXPIRED &&
        rv.status().code() != ErrorCodes::ERR_NOTFOUND) {
      return rv.status();
    }

    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);

    RecordKey metaRk(expdb.value().chunkId,
                     pCtx->getDbId(),
                     RecordType::RT_ZSET_META,
                     key,
                     "");
    PStore kvstore = expdb.value().store;
    int flag = ZADD_INCR;
    for (int32_t i = 0; i < RETRY_CNT; ++i) {
      auto ptxn = sess->getCtx()->createTransaction(kvstore);
      if (!ptxn.ok()) {
        return ptxn.status();
      }
      Expected<std::string> s =
        genericZadd(sess, kvstore, metaRk, rv, {{subkey, score.value()}},
                flag, ptxn.value());
      if (!s.ok()) {
        return s.status();
      }
      auto eCmt = sess->getCtx()->commitTransaction(ptxn.value());
      if (!eCmt.ok()) {
        return eCmt.status();
      }
      return s.value();
    }

    INVARIANT_D(0);
    return {ErrorCodes::ERR_INTERNAL, "not reachable"};
  }
} zincrbyCommand;

class ZCountCommand : public Command {
 public:
  ZCountCommand() : Command("zcount", "rF") {}

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
    const std::vector<std::string>& args = sess->getArgs();
    const std::string& key = args[1];

    Zrangespec range;
    if (zslParseRange(args[2].c_str(), args[3].c_str(), &range) != 0) {
      return {ErrorCodes::ERR_ZSLPARSERANGE, ""};
    }

    auto server = sess->getServerEntry();
    auto expdb =
      server->getSegmentMgr()->getDbWithKeyLock(sess, key, Command::RdLock());
    if (!expdb.ok()) {
      return expdb.status();
    }

    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_ZSET_META);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
        rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtZero();
    } else if (!rv.ok()) {
      return rv.status();
    }

    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    PStore kvstore = expdb.value().store;
    auto ptxn = sess->getCtx()->createTransaction(kvstore);
    if (!ptxn.ok()) {
      return ptxn.status();
    }

    auto eMetaContent = ZSlMetaValue::decode(rv.value().getValue());
    if (!eMetaContent.ok()) {
      return eMetaContent.status();
    }
    ZSlMetaValue meta = eMetaContent.value();
    SkipList sl(expdb.value().chunkId, pCtx->getDbId(), key, meta, kvstore);
    auto f = sl.firstInRange(range, ptxn.value());
    if (!f.ok()) {
      return f.status();
    }
    if (f.value() == SKIPLIST_INVALID_POS) {
      return Command::fmtZero();
    }
    auto first = sl.getCacheNode(f.value());
    Expected<uint32_t> rank =
      sl.rank(first->getScore(), first->getSubKey(), ptxn.value());
    if (!rank.ok()) {
      return rank.status();
    }
    // sl.getCount()-1 : total skiplist nodes exclude head
    uint32_t count = (sl.getCount() - 1 - (rank.value() - 1));
    auto l = sl.lastInRange(range, ptxn.value());
    if (!l.ok()) {
      return l.status();
    }
    if (l.value() == SKIPLIST_INVALID_POS) {
      return Command::fmtLongLong(count);
    }
    auto last = sl.getCacheNode(l.value());
    rank = sl.rank(last->getScore(), last->getSubKey(), ptxn.value());
    if (!rank.ok()) {
      return rank.status();
    }
    return Command::fmtLongLong(count - (sl.getCount() - 1 - rank.value()));
  }
} zcountCommand;

class ZlexCountCommand : public Command {
 public:
  ZlexCountCommand() : Command("zlexcount", "rF") {}

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
    const std::vector<std::string>& args = sess->getArgs();
    const std::string& key = args[1];

    Zlexrangespec range;
    if (zslParseLexRange(args[2].c_str(), args[3].c_str(), &range) != 0) {
      return {ErrorCodes::ERR_ZSLPARSELEXRANGE, ""};
    }

    auto server = sess->getServerEntry();
    auto expdb =
      server->getSegmentMgr()->getDbWithKeyLock(sess, key, Command::RdLock());
    if (!expdb.ok()) {
      return expdb.status();
    }

    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_ZSET_META);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
        rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtZero();
    } else if (!rv.ok()) {
      return rv.status();
    }

    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    PStore kvstore = expdb.value().store;
    auto ptxn = sess->getCtx()->createTransaction(kvstore);
    if (!ptxn.ok()) {
      return ptxn.status();
    }

    auto eMetaContent = ZSlMetaValue::decode(rv.value().getValue());
    if (!eMetaContent.ok()) {
      return eMetaContent.status();
    }
    ZSlMetaValue meta = eMetaContent.value();
    SkipList sl(expdb.value().chunkId, pCtx->getDbId(), key, meta, kvstore);

    auto f = sl.firstInLexRange(range, ptxn.value());
    if (!f.ok()) {
      return f.status();
    }
    if (f.value() == SKIPLIST_INVALID_POS) {
      return Command::fmtZero();
    }
    auto first = sl.getCacheNode(f.value());
    Expected<uint32_t> rank =
      sl.rank(first->getScore(), first->getSubKey(), ptxn.value());
    if (!rank.ok()) {
      return rank.status();
    }
    // sl.getCount()-1 : total skiplist nodes exclude head
    uint32_t count = (sl.getCount() - 1 - (rank.value() - 1));
    auto l = sl.lastInLexRange(range, ptxn.value());
    if (!l.ok()) {
      return l.status();
    }
    if (l.value() == SKIPLIST_INVALID_POS) {
      return Command::fmtLongLong(count);
    }
    auto last = sl.getCacheNode(l.value());
    rank = sl.rank(last->getScore(), last->getSubKey(), ptxn.value());
    if (!rank.ok()) {
      return rank.status();
    }
    return Command::fmtLongLong(count - (sl.getCount() - 1 - rank.value()));
  }
} zlexCntCmd;

class ZRangeByScoreGenericCommand : public Command {
 public:
  ZRangeByScoreGenericCommand(const std::string& name, const char* sflags)
    : Command(name, sflags) {
    if (name == "zrangebyscore") {
      _rev = false;
    } else {
      _rev = true;
    }
  }

  ssize_t arity() const {
    return -4;
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
    uint64_t offset = 0;
    uint64_t limit = (uint64_t)-1;
    int withscore = 0;
    int minidx, maxidx;
    Zrangespec range;
    if (_rev) {
      maxidx = 2;
      minidx = 3;
    } else {
      minidx = 2;
      maxidx = 3;
    }
    if (zslParseRange(args[minidx].c_str(), args[maxidx].c_str(), &range) !=
        0) {  // NOLINT:whitespace/line_length
      return {ErrorCodes::ERR_ZSLPARSERANGE, ""};
    }

    if (args.size() > 4) {
      int remaining = args.size() - 4;
      int pos = 4;
      while (remaining) {
        if (remaining >= 1 && toLower(args[pos]) == "withscores") {
          pos++;
          remaining--;
          withscore = 1;
        } else if (remaining >= 3 && toLower(args[pos]) == "limit") {
          Expected<int64_t> eoffset = ::tendisplus::stoll(args[pos + 1]);
          if (!eoffset.ok()) {
            return eoffset.status();
          }
          offset = (uint64_t)eoffset.value();
          Expected<int64_t> elimit = ::tendisplus::stoll(args[pos + 2]);
          if (!elimit.ok()) {
            return elimit.status();
          }
          limit = (uint64_t)elimit.value();
          pos += 3;
          remaining -= 3;
        } else {
          return {ErrorCodes::ERR_PARSEOPT, "syntax error"};
        }
      }
    }

    auto server = sess->getServerEntry();
    auto expdb =
      server->getSegmentMgr()->getDbWithKeyLock(sess, key, Command::RdLock());
    if (!expdb.ok()) {
      return expdb.status();
    }

    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_ZSET_META);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
        rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtZeroBulkLen();
    } else if (!rv.ok()) {
      return rv.status();
    }

    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    PStore kvstore = expdb.value().store;
    auto ptxn = sess->getCtx()->createTransaction(kvstore);
    if (!ptxn.ok()) {
      return ptxn.status();
    }

    auto eMetaContent = ZSlMetaValue::decode(rv.value().getValue());
    if (!eMetaContent.ok()) {
      return eMetaContent.status();
    }
    ZSlMetaValue meta = eMetaContent.value();
    SkipList sl(expdb.value().chunkId, pCtx->getDbId(), key, meta, kvstore);
    auto arr = sl.scanByScore(range, offset, limit, _rev, ptxn.value());
    if (!arr.ok()) {
      return arr.status();
    }
    std::stringstream ss;
    if (withscore) {
      Command::fmtMultiBulkLen(ss, arr.value().size() * 2);
    } else {
      Command::fmtMultiBulkLen(ss, arr.value().size());
    }
    for (const auto& v : arr.value()) {
      Command::fmtBulk(ss, v.second);
      if (withscore) {
        Command::fmtBulk(ss, ::tendisplus::dtos(v.first));
      }
    }
    return ss.str();
  }

 private:
  bool _rev;
};

class ZRangeByScoreCommand : public ZRangeByScoreGenericCommand {
 public:
  ZRangeByScoreCommand() : ZRangeByScoreGenericCommand("zrangebyscore", "r") {}
} zrangebyscoreCmd;

class ZRevRangeByScoreCommand : public ZRangeByScoreGenericCommand {
 public:
  ZRevRangeByScoreCommand()
    : ZRangeByScoreGenericCommand("zrevrangebyscore", "r") {}
} zrevrangebyscoreCmd;

class ZRangeByLexGenericCommand : public Command {
 public:
  ZRangeByLexGenericCommand(const std::string& name, const char* sflags)
    : Command(name, sflags) {
    if (name == "zrangebylex") {
      _rev = false;
    } else {
      _rev = true;
    }
  }

  ssize_t arity() const {
    return -4;
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
    uint64_t offset = 0;
    uint64_t limit = -1;
    int minidx, maxidx;
    if (_rev) {
      maxidx = 2;
      minidx = 3;
    } else {
      minidx = 2;
      maxidx = 3;
    }

    Zlexrangespec range;
    if (zslParseLexRange(args[minidx].c_str(),
                         args[maxidx].c_str(),
                         &range) != 0) {  // NOLINT:whitespace/line_length
      return {ErrorCodes::ERR_ZSLPARSELEXRANGE, ""};
    }

    if (args.size() > 4) {
      int remaining = args.size() - 4;
      int pos = 4;
      while (remaining) {
        if (remaining >= 3 && toLower(args[pos]) == "limit") {
          Expected<int64_t> eoffset = ::tendisplus::stoll(args[pos + 1]);
          if (!eoffset.ok()) {
            return eoffset.status();
          }
          offset = (uint64_t)eoffset.value();
          Expected<int64_t> elimit = ::tendisplus::stoll(args[pos + 2]);
          if (!elimit.ok()) {
            return elimit.status();
          }
          limit = (uint64_t)elimit.value();
          pos += 3;
          remaining -= 3;
        } else {
          return {ErrorCodes::ERR_PARSEOPT, "syntax error"};
        }
      }
    }

    auto server = sess->getServerEntry();
    auto expdb =
      server->getSegmentMgr()->getDbWithKeyLock(sess, key, Command::RdLock());
    if (!expdb.ok()) {
      return expdb.status();
    }

    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_ZSET_META);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
        rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtZeroBulkLen();
    } else if (!rv.ok()) {
      return rv.status();
    }

    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    PStore kvstore = expdb.value().store;
    auto ptxn = sess->getCtx()->createTransaction(kvstore);
    if (!ptxn.ok()) {
      return ptxn.status();
    }

    auto eMetaContent = ZSlMetaValue::decode(rv.value().getValue());
    if (!eMetaContent.ok()) {
      return eMetaContent.status();
    }
    ZSlMetaValue meta = eMetaContent.value();
    SkipList sl(expdb.value().chunkId, pCtx->getDbId(), key, meta, kvstore);
    auto arr = sl.scanByLex(range, offset, limit, _rev, ptxn.value());
    if (!arr.ok()) {
      return arr.status();
    }
    std::stringstream ss;
    Command::fmtMultiBulkLen(ss, arr.value().size());
    for (const auto& v : arr.value()) {
      Command::fmtBulk(ss, v.second);
    }
    return ss.str();
  }

 private:
  bool _rev;
};

class ZRangeByLexCommand : public ZRangeByLexGenericCommand {
 public:
  ZRangeByLexCommand() : ZRangeByLexGenericCommand("zrangebylex", "r") {}
} zrangebylexCmd;

class ZRevRangeByLexCommand : public ZRangeByLexGenericCommand {
 public:
  ZRevRangeByLexCommand() : ZRangeByLexGenericCommand("zrevrangebylex", "r") {}
} zrevrangebylexCmd;

class ZRangeGenericCommand : public Command {
 public:
  ZRangeGenericCommand(const std::string& name, const char* sflags)
    : Command(name, sflags) {
    if (name == "zrange") {
      _rev = false;
    } else {
      _rev = true;
    }
  }

  ssize_t arity() const {
    return -4;
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
    Expected<int64_t> estart = ::tendisplus::stoll(args[2]);
    if (!estart.ok()) {
      return estart.status();
    }
    Expected<int64_t> eend = ::tendisplus::stoll(args[3]);
    if (!eend.ok()) {
      return eend.status();
    }
    int64_t start = estart.value();
    int64_t end = eend.value();
    bool withscore = (args.size() == 5 && toLower(args[4]) == "withscores");
    if (args.size() > 5) {
      return {ErrorCodes::ERR_PARSEOPT, "syntax error"};
    }

    auto server = sess->getServerEntry();
    auto expdb =
      server->getSegmentMgr()->getDbWithKeyLock(sess, key, Command::RdLock());
    if (!expdb.ok()) {
      return expdb.status();
    }

    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_ZSET_META);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
        rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtZeroBulkLen();
    } else if (!rv.ok()) {
      return rv.status();
    }

    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    PStore kvstore = expdb.value().store;
    auto ptxn = sess->getCtx()->createTransaction(kvstore);
    if (!ptxn.ok()) {
      return ptxn.status();
    }
    auto eMetaContent = ZSlMetaValue::decode(rv.value().getValue());
    if (!eMetaContent.ok()) {
      return eMetaContent.status();
    }
    ZSlMetaValue meta = eMetaContent.value();
    SkipList sl(expdb.value().chunkId, pCtx->getDbId(), key, meta, kvstore);
    int64_t len = sl.getCount() - 1;
    if (start < 0) {
      start = len + start;
    }
    if (end < 0) {
      end = len + end;
    }
    if (start < 0) {
      start = 0;
    }
    if (start > end || start >= len) {
      return Command::fmtZeroBulkLen();
    }
    if (end >= len) {
      end = len - 1;
    }
    int64_t rangeLen = end - start + 1;
    auto arr = sl.scanByRank(start, rangeLen, _rev, ptxn.value());
    if (!arr.ok()) {
      return arr.status();
    }
    std::stringstream ss;
    if (withscore) {
      Command::fmtMultiBulkLen(ss, arr.value().size() * 2);
    } else {
      Command::fmtMultiBulkLen(ss, arr.value().size());
    }
    for (const auto& v : arr.value()) {
      Command::fmtBulk(ss, v.second);
      if (withscore) {
        Command::fmtBulk(ss, ::tendisplus::dtos(v.first));
      }
    }
    return ss.str();
  }

 private:
  bool _rev;
};

class ZRangeCommand : public ZRangeGenericCommand {
 public:
  ZRangeCommand() : ZRangeGenericCommand("zrange", "r") {}
} zrangeCmd;

class ZRevRangeCommand : public ZRangeGenericCommand {
 public:
  ZRevRangeCommand() : ZRangeGenericCommand("zrevrange", "r") {}
} zrevrangeCmd;

class ZScoreCommand : public Command {
 public:
  ZScoreCommand() : Command("zscore", "rF") {}

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

    auto server = sess->getServerEntry();
    auto expdb =
      server->getSegmentMgr()->getDbWithKeyLock(sess, key, Command::RdLock());
    if (!expdb.ok()) {
      return expdb.status();
    }

    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_ZSET_META);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
        rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtNull();
    } else if (!rv.ok()) {
      return rv.status();
    }

    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);

    RecordKey metaRk(expdb.value().chunkId,
                     pCtx->getDbId(),
                     RecordType::RT_ZSET_META,
                     key,
                     "");
    PStore kvstore = expdb.value().store;

    auto ptxn = sess->getCtx()->createTransaction(kvstore);
    if (!ptxn.ok()) {
      return ptxn.status();
    }

    RecordKey hk(expdb.value().chunkId,
                 pCtx->getDbId(),
                 RecordType::RT_ZSET_H_ELE,
                 key,
                 subkey);
    Expected<RecordValue> eValue = kvstore->getKV(hk, ptxn.value());
    if (!eValue.ok() && eValue.status().code() != ErrorCodes::ERR_NOTFOUND) {
      return eValue.status();
    }
    if (eValue.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtNull();
    }
    Expected<double> oldScore =
      ::tendisplus::doubleDecode(eValue.value().getValue());
    if (!oldScore.ok()) {
      return oldScore.status();
    }
    return Command::fmtBulk(::tendisplus::dtos(oldScore.value()));
  }
} zscoreCmd;

class ZAddCommand : public Command {
 public:
  ZAddCommand() : Command("zadd", "wmF") {}

  ssize_t arity() const {
    return -4;
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

  /* Add a new element or update the score of an existing element in a sorted
   * set, regardless of its encoding.
   *
   * The set of flags change the command behavior. They are passed with an
   * integer pointer since the function will clear the flags and populate them
   * with other flags to indicate different conditions.
   *
   * The input flags are the following:
   *
   * ZADD_INCR: Increment the current element score by 'score' instead of
   * updating the current element score. If the element does not exist, we
   *            assume 0 as previous score.
   * ZADD_NX:   Perform the operation only if the element does not exist.
   * ZADD_XX:   Perform the operation only if the element already exist.
   *
   * When ZADD_INCR is used, the new score of the element is stored in
   * '*newscore' if 'newscore' is not NULL.
   *
   * The returned flags are the following:
   *
   * ZADD_NAN:     The resulting score is not a number.
   * ZADD_ADDED:   The element was added (not present before the call).
   * ZADD_UPDATED: The element score was updated.
   * ZADD_NOP:     No operation was performed because of NX or XX.
   *
   * Return value:
   *
   * The function returns 1 on success, and sets the appropriate flags
   * ADDED or UPDATED to signal what happened during the operation (note that
   * none could be set if we re-added an element using the same score it used
   * to have, or in the case a zero increment is used).
   *
   * The function returns 0 on erorr, currently only when the increment
   * produces a NAN condition, or when the 'score' value is NAN since the
   * start.
   *
   * The commad as a side effect of adding a new element may convert the
   * sorted set internal encoding from ziplist to hashtable+skiplist.
   *
   * Memory managemnet of 'ele':
   *
   * The function does not take ownership of the 'ele' SDS string, but copies
   * it if needed. */
  Expected<std::string> run(Session* sess) final {
    const std::vector<std::string>& args = sess->getArgs();

    int flag = ZADD_NONE;

    size_t i = 2;
    while (i < args.size()) {
      if (toLower(args[i]) == "nx") {
        flag |= ZADD_NX;
      } else if (toLower(args[i]) == "xx") {
        flag |= ZADD_XX;
      } else if (toLower(args[i]) == "ch") {
        flag |= ZADD_CH;
      } else if (toLower(args[i]) == "incr") {
        flag |= ZADD_INCR;
      } else {
        break;
      }
      i++;
    }

    if ((args.size() - i) % 2 != 0 || args.size() - i == 0) {
      return {ErrorCodes::ERR_PARSEOPT, ""};
    }
    const std::string& key = args[1];
    std::map<std::string, double> scoreMap;
    for (; i < args.size(); i += 2) {
      const std::string& subkey = args[i + 1];
      Expected<double> score = ::tendisplus::stod(args[i]);
      if (!score.ok()) {
        return score.status();
      }
      scoreMap[subkey] = score.value();
    }

    auto server = sess->getServerEntry();
    auto expdb = server->getSegmentMgr()->getDbWithKeyLock(
      sess, key, mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
      return expdb.status();
    }

    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_ZSET_META);
    if (rv.status().code() != ErrorCodes::ERR_OK &&
        rv.status().code() != ErrorCodes::ERR_EXPIRED &&
        rv.status().code() != ErrorCodes::ERR_NOTFOUND) {
      return rv.status();
    }

    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);

    RecordKey metaRk(expdb.value().chunkId,
                     pCtx->getDbId(),
                     RecordType::RT_ZSET_META,
                     key,
                     "");
    PStore kvstore = expdb.value().store;
    for (int32_t i = 0; i < RETRY_CNT; ++i) {
      auto ptxn = sess->getCtx()->createTransaction(kvstore);
      if (!ptxn.ok()) {
        return ptxn.status();
      }
      Expected<std::string> s =
        genericZadd(sess, kvstore, metaRk, rv, scoreMap, flag, ptxn.value());
      if (!s.ok()) {
        return s.status();
      }
      auto eCmt = sess->getCtx()->commitTransaction(ptxn.value());
      if (!eCmt.ok()) {
        return eCmt.status();
      }
      return s.value();
    }

    INVARIANT_D(0);
    return {ErrorCodes::ERR_INTERNAL, "not reachable"};
  }
} zaddCommand;

class ZSetCountCommand : public Command {
 public:
  ZSetCountCommand() : Command("zsetcount", "r") {}

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

  bool sameWithRedis() const {
    return false;
  }

  Expected<std::string> run(Session* sess) final {
    // const std::vector<std::string>& args = sess->getArgs();

    // TODO(vinchen): support it later
    return Command::fmtOK();
  }
} zsetcountCmd;

class ZUnionInterGenericCommand : public Command {
 public:
  enum class ZsetOp {
    SET_OP_UNION,
    SET_OP_INTER,
  };
  enum class Aggregate {
    REDIS_AGGR_SUM,
    REDIS_AGGR_MIN,
    REDIS_AGGR_MAX,
  };
  explicit ZUnionInterGenericCommand(const std::string& name)
    : Command(name, "wm") {
    if (name == "zunionstore") {
      _op = ZsetOp::SET_OP_UNION;
    } else {
      _op = ZsetOp::SET_OP_INTER;
    }
  }

  ssize_t arity() const {
    return -4;
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

  std::vector<int> getKeysFromCommand(
    const std::vector<std::string>& argv) final {
    Expected<int32_t> expLen = tendisplus::stol(argv[2]);
    if (!expLen.ok()) {
      return std::vector<int>();
    }
    int32_t len = expLen.value();
    if (len > static_cast<int32_t>(argv.size()) - 3) {
      return std::vector<int>();
    }

    std::vector<int> keyindex;
    keyindex.reserve(len + 1);
    for (int i = 0; i < len; i++) {
      keyindex.push_back(3 + i);
    }
    keyindex.push_back(1);
    return keyindex;
  }

  Expected<std::string> run(Session* sess) final {
    const std::vector<std::string>& args = sess->getArgs();
    Expected<int32_t> expLen = tendisplus::stol(args[2]);
    if (!expLen.ok()) {
      return expLen.status();
    }
    int32_t len = expLen.value();
    if (len < 1) {
      return {ErrorCodes::ERR_PARSEPKT,
              "at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE"};
    }
    if (len > static_cast<int32_t>(sess->getArgs().size()) - 3) {
      return {ErrorCodes::ERR_PARSEOPT, ""};
    }

    auto server = sess->getServerEntry();
    SessionCtx* pCtx = sess->getCtx();
    std::vector<int> keyindex = getKeysFromCommand(args);

    // Parse optional extra arguments
    std::vector<double> weights(len, 1);
    Aggregate aggr = Aggregate::REDIS_AGGR_SUM;
    for (size_t j = 3 + len; j < args.size(); j++) {
      int remaining = args.size() - j;

      while (remaining) {
        if (remaining >= len + 1 && !::strcasecmp(args[j].c_str(), "weights")) {
          j++;
          remaining--;
          for (int i = 0; i < len; i++, j++, remaining--) {
            Expected<double> expScore = tendisplus::stod(args[j]);
            if (expScore.status().code() == ErrorCodes::ERR_FLOAT) {
              return {ErrorCodes::ERR_PARSEPKT, "weight value is not a float"};
            }
            weights[i] = expScore.value();
          }
        } else if (remaining >= 2 &&
                   !::strcasecmp(args[j].c_str(), "aggregate")) {
          j++;
          remaining--;
          std::string aggtype(args[j]);
          std::transform(
            aggtype.begin(), aggtype.end(), aggtype.begin(), ::tolower);
          if (aggtype == "sum") {
            aggr = Aggregate::REDIS_AGGR_SUM;
          } else if (aggtype == "min") {
            aggr = Aggregate::REDIS_AGGR_MIN;
          } else if (aggtype == "max") {
            aggr = Aggregate::REDIS_AGGR_MAX;
          } else {
            return {ErrorCodes::ERR_PARSEOPT, ""};
          }
          j++;
          remaining--;
        } else {
          return {ErrorCodes::ERR_PARSEOPT, ""};
        }
      }
    }

    auto lock = server->getSegmentMgr()->getAllKeysLocked(
      sess, sess->getArgs(), keyindex, mgl::LockMode::LOCK_X);
    if (!lock.ok()) {
      return lock.status();
    }

    std::vector<std::pair<size_t, uint32_t>> sortList;
    std::vector<std::pair<uint32_t, RecordValue>> zsetList;
    zsetList.reserve(keyindex.size() - 1);
    for (size_t i = 0; i < keyindex.size() - 1; i++) {
      Expected<RecordValue> exprv = Command::expireKeyIfNeeded(
        sess, args[keyindex[i]], RecordType::RT_DATA_META);
      if (exprv.status().code() == ErrorCodes::ERR_EXPIRED ||
          exprv.status().code() == ErrorCodes::ERR_NOTFOUND) {
        if (_op == ZsetOp::SET_OP_INTER)
          Command::fmtZero();
        continue;
      } else if (!exprv.ok()) {
        return exprv.status();
      }
      if (exprv.value().getRecordType() == RecordType::RT_ZSET_META) {
        Expected<ZSlMetaValue> zslMeta =
          ZSlMetaValue::decode(exprv.value().getValue());
        uint32_t len = zslMeta.value().getCount();
        sortList.push_back(std::make_pair(i, len));
        zsetList.emplace_back(std::make_pair(i, std::move(exprv.value())));
      } else if (exprv.value().getRecordType() == RecordType::RT_SET_META) {
        Expected<SetMetaValue> eSetMeta =
          SetMetaValue::decode(exprv.value().getValue());
        uint32_t len = eSetMeta.value().getCount();
        sortList.push_back(std::make_pair(i, len));
        zsetList.emplace_back(std::make_pair(i, std::move(exprv.value())));
      }
    }

    // sort set according to its op type.
    bool (*comp)(size_t, size_t);
    if (_op == ZsetOp::SET_OP_INTER) {
      comp = [](size_t a, size_t b) { return a < b; };
    } else {
      comp = [](size_t a, size_t b) { return a > b; };
    }
    std::sort(
      sortList.begin(), sortList.end(), [comp](auto& left, auto& right) {
        return comp(left.second, right.second);
      });

    std::map<std::string, double> scoreMap;

    for (size_t fakei = 0; fakei < sortList.size(); fakei++) {
      size_t i = sortList[fakei].first;
      double w = weights[i];
      const std::string& key = args[keyindex[i]];
      auto expdb = server->getSegmentMgr()->getDbHasLocked(sess, key);
      if (!expdb.ok()) {
        return expdb.status();
      }
      PStore kvstore = expdb.value().store;
      auto ptxn = sess->getCtx()->createTransaction(kvstore);
      if (!ptxn.ok()) {
        return ptxn.status();
      }

      RecordType keyType = zsetList[i].second.getRecordType();
      if (fakei == 0 || _op == ZsetOp::SET_OP_UNION) {
        if (keyType == RecordType::RT_ZSET_META) {
          Expected<ZSlMetaValue> zslMeta =
            ZSlMetaValue::decode(zsetList[i].second.getValue());
          SkipList sl(expdb.value().chunkId,
                      pCtx->getDbId(),
                      key,
                      zslMeta.value(),
                      kvstore);
          auto arr = sl.scanByRank(0, sl.getCount() - 1, false, ptxn.value());
          if (!arr.ok()) {
            return arr.status();
          }
          for (const auto& v : arr.value()) {
            double value = v.first * w;
            if (std::isnan(value))
              value = 0.0;
            if (!scoreMap.count(v.second)) {
              scoreMap[v.second] = value;
              continue;
            }
            zunionInterAggregate(&scoreMap[v.second], value, aggr);
          }
        } else if (keyType == RecordType::RT_SET_META) {
          auto cursor = ptxn.value()->createDataCursor();
          RecordKey rk(expdb.value().chunkId,
                       pCtx->getDbId(),
                       RecordType::RT_SET_ELE,
                       key,
                       "");
          cursor->seek(rk.prefixPk());
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
            if (rcdKey.prefixPk() != rk.prefixPk()) {
              break;
            }
            const std::string& subkey = rcdKey.getSecondaryKey();
            if (!scoreMap.count(subkey)) {
              scoreMap[subkey] = 1 * w;
              continue;
            }
            zunionInterAggregate(&scoreMap[subkey], 1 * w, aggr);
          }
        }
        continue;
      } else if (_op == ZsetOp::SET_OP_INTER) {
        RecordType eleType = keyType == RecordType::RT_ZSET_META
          ? RecordType::RT_ZSET_H_ELE
          : RecordType::RT_SET_ELE;
        for (auto iter = scoreMap.begin(); iter != scoreMap.end();) {
          const std::string& subkey = iter->first;
          RecordKey rk(
            expdb.value().chunkId, pCtx->getDbId(), eleType, key, subkey);
          auto eVal = kvstore->getKV(rk, ptxn.value());

          if (!eVal.ok() || eVal.status().code() == ErrorCodes::ERR_NOTFOUND) {
            iter = scoreMap.erase(iter);
            continue;
          }
          double value = 1;
          if (keyType == RecordType::RT_ZSET_META) {
            Expected<double> eScore =
              tendisplus::doubleDecode(eVal.value().getValue());
            if (!eScore.ok()) {
              return eScore.status();
            }
            value = eScore.value();
          }
          value = value * w;
          if (std::isnan(value))
            value = 0;
          zunionInterAggregate(&(iter->second), value, aggr);
          iter++;
        }
      }
    }

    // delete before store
    const std::string& storeKey = args[1];
    auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, storeKey,
            mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
      return expdb.status();
    }
    PStore kvstore = expdb.value().store;
    auto ptxn = sess->getCtx()->createTransaction(kvstore);
    if (!ptxn.ok()) {
      return ptxn.status();
    }

    Expected<bool> eRes = delGeneric(sess, storeKey, ptxn.value());
    if (!eRes.ok()) {
      return eRes.status();
    }
    if (scoreMap.size() == 0) {
      auto eCmt = sess->getCtx()->commitTransaction(ptxn.value());
      if (!eCmt.ok()) {
        return eCmt.status();
      }
      return Command::fmtZero();
    }

    RecordKey storeRk(expdb.value().chunkId,
                      pCtx->getDbId(),
                      RecordType::RT_ZSET_META,
                      storeKey,
                      "");
    for (int32_t i = 0; i < RETRY_CNT; ++i) {
      Expected<std::string> s = genericZadd(sess,
                                            kvstore,
                                            storeRk,
                                            {ErrorCodes::ERR_NOTFOUND, ""},
                                            scoreMap,
                                            ZADD_NONE,
                                            ptxn.value());
      if (!s.ok()) {
        return s.status();
      }
      auto eCmt = sess->getCtx()->commitTransaction(ptxn.value());
      if (!eCmt.ok()) {
        return eCmt.status();
      }
      return s.value();
    }

    return {ErrorCodes::ERR_INTERNAL, "Not reachable"};
  }

 private:
  void zunionInterAggregate(double* oldScore,
                            double value,
                            const Aggregate& aggr) {
    switch (aggr) {
      case Aggregate::REDIS_AGGR_SUM:
        *oldScore = *oldScore + value;
        if (std::isnan(*oldScore))
          *oldScore = 0.0;
        break;
      case Aggregate::REDIS_AGGR_MIN:
        *oldScore = value < *oldScore ? value : *oldScore;
        break;
      case Aggregate::REDIS_AGGR_MAX:
        *oldScore = value > *oldScore ? value : *oldScore;
        break;
      default:
        INVARIANT_D(0);
    }
  }
  ZsetOp _op;
};

class ZUnionStoreCommand : public ZUnionInterGenericCommand {
 public:
  ZUnionStoreCommand() : ZUnionInterGenericCommand("zunionstore") {}
} zunionstoreCommand;

class ZInterStoreCommand : public ZUnionInterGenericCommand {
 public:
  ZInterStoreCommand() : ZUnionInterGenericCommand("zinterstore") {}
} zinterstoreCommand;

}  // namespace tendisplus
