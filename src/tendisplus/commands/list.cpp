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
#include "glog/logging.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

constexpr uint64_t MAXSEQ = 9223372036854775807ULL;
constexpr uint64_t INITSEQ = MAXSEQ / 2ULL;
constexpr uint64_t MINSEQ = 1024;

enum class ListPos {
  LP_HEAD,
  LP_TAIL,
};

Expected<std::string> genericPop(Session* sess,
                                 PStore kvstore,
                                 Transaction* txn,
                                 const RecordKey& metaRk,
                                 const Expected<RecordValue>& rv,
                                 ListPos pos) {
  ListMetaValue lm(INITSEQ, INITSEQ);
  if (!rv.ok()) {
    return rv.status();
  }

  uint64_t ttl = 0;
  ttl = rv.value().getTtl();
  Expected<ListMetaValue> exptLm = ListMetaValue::decode(rv.value().getValue());
  INVARIANT_D(exptLm.ok());
  if (!exptLm.ok()) {
    return exptLm.status();
  }
  lm = std::move(exptLm.value());

  uint64_t head = lm.getHead();
  uint64_t tail = lm.getTail();
  INVARIANT_D(head != tail);
  if (head == tail) {
    return {ErrorCodes::ERR_INTERNAL, "invalid head or tail of list"};
  }
  uint64_t idx;
  if (pos == ListPos::LP_HEAD) {
    idx = head++;
  } else {
    idx = --tail;
  }
  RecordKey subRk(metaRk.getChunkId(),
                  metaRk.getDbId(),
                  RecordType::RT_LIST_ELE,
                  metaRk.getPrimaryKey(),
                  std::to_string(idx));
  Expected<RecordValue> subRv = kvstore->getKV(subRk, txn);
  if (!subRv.ok()) {
    return subRv.status();
  }
  Status s = kvstore->delKV(subRk, txn);
  if (!s.ok()) {
    return s;
  }
  if (head == tail) {
    s = Command::delKeyAndTTL(sess, metaRk, rv.value(), txn);
  } else {
    lm.setHead(head);
    lm.setTail(tail);
    s = kvstore->setKV(metaRk,
                       RecordValue(lm.encode(),
                                   RecordType::RT_LIST_META,
                                   sess->getCtx()->getVersionEP(),
                                   ttl,
                                   rv),
                       txn);
  }
  if (!s.ok()) {
    return s;
  }
  return subRv.value().getValue();
}

Expected<std::string> genericPush(Session* sess,
                                  PStore kvstore,
                                  Transaction* txn,
                                  const RecordKey& metaRk,
                                  const Expected<RecordValue>& rv,
                                  const std::vector<std::string>& args,
                                  ListPos pos,
                                  bool needExist) {
  ListMetaValue lm(INITSEQ, INITSEQ);
  uint64_t ttl = 0;

  if (rv.ok()) {
    ttl = rv.value().getTtl();
    Expected<ListMetaValue> exptLm =
      ListMetaValue::decode(rv.value().getValue());
    INVARIANT_D(exptLm.ok());
    if (!exptLm.ok()) {
      return exptLm.status();
    }
    lm = std::move(exptLm.value());
  } else if (rv.status().code() != ErrorCodes::ERR_NOTFOUND &&
             rv.status().code() != ErrorCodes::ERR_EXPIRED) {
    return rv.status();
  } else if (needExist) {
    return Command::fmtZero();
  }

  uint64_t head = lm.getHead();
  uint64_t tail = lm.getTail();
  for (size_t i = 0; i < args.size(); ++i) {
    uint64_t idx;
    if (pos == ListPos::LP_HEAD) {
      idx = --head;
    } else {
      idx = tail++;
    }
    RecordKey subRk(metaRk.getChunkId(),
                    metaRk.getDbId(),
                    RecordType::RT_LIST_ELE,
                    metaRk.getPrimaryKey(),
                    std::to_string(idx));
    RecordValue subRv(args[i], RecordType::RT_LIST_ELE, -1);
    Status s = kvstore->setKV(subRk, subRv, txn);
    if (!s.ok()) {
      return s;
    }
  }
  lm.setHead(head);
  lm.setTail(tail);
  Status s = kvstore->setKV(metaRk,
                            RecordValue(lm.encode(),
                                        RecordType::RT_LIST_META,
                                        sess->getCtx()->getVersionEP(),
                                        ttl,
                                        rv),
                            txn);
  if (!s.ok()) {
    return s;
  }
  return Command::fmtLongLong(lm.getTail() - lm.getHead());
}

class LLenCommand : public Command {
 public:
  LLenCommand() : Command("llen", "rF") {}

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
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_LIST_META);

    if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
      return fmtZero();
    } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return fmtZero();
    } else if (!rv.status().ok()) {
      return rv.status();
    }

    Expected<ListMetaValue> exptListMeta =
      ListMetaValue::decode(rv.value().getValue());
    if (!exptListMeta.ok()) {
      return exptListMeta.status();
    }
    uint64_t tail = exptListMeta.value().getTail();
    uint64_t head = exptListMeta.value().getHead();
    return fmtLongLong(tail - head);
  }
} llenCommand;

class ListPopWrapper : public Command {
 public:
  explicit ListPopWrapper(ListPos pos, const char* sflags)
    : Command(pos == ListPos::LP_HEAD ? "lpop" : "rpop", sflags), _pos(pos) {}

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
    auto expdb = server->getSegmentMgr()->getDbWithKeyLock(
      sess, key, mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
      return expdb.status();
    }
    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_LIST_META);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
        rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtNull();
    } else if (!rv.ok()) {
      return rv.status();
    }

    // record exists
    RecordKey metaRk(expdb.value().chunkId,
                     pCtx->getDbId(),
                     RecordType::RT_LIST_META,
                     key,
                     "");
    PStore kvstore = expdb.value().store;

    for (uint32_t i = 0; i < RETRY_CNT; ++i) {
      auto ptxn = sess->getCtx()->createTransaction(kvstore);
      if (!ptxn.ok()) {
        return ptxn.status();
      }
      Expected<std::string> s1 =
        genericPop(sess, kvstore, ptxn.value(), metaRk, rv, _pos);
      if (s1.status().code() == ErrorCodes::ERR_NOTFOUND) {
        return Command::fmtNull();
      } else if (!s1.ok()) {
        return s1.status();
      }
      auto s = sess->getCtx()->commitTransaction(ptxn.value());
      if (s.ok()) {
        return Command::fmtBulk(s1.value());
      } else if (s.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
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
  ListPos _pos;
};

class LPopCommand : public ListPopWrapper {
 public:
  LPopCommand() : ListPopWrapper(ListPos::LP_HEAD, "wF") {}
} LPopCommand;

class RPopCommand : public ListPopWrapper {
 public:
  RPopCommand() : ListPopWrapper(ListPos::LP_TAIL, "wF") {}
} rpopCommand;

class ListPushWrapper : public Command {
 public:
  explicit ListPushWrapper(const std::string& name,
                           const char* sflags,
                           ListPos pos,
                           bool needExist)
    : Command(name, sflags), _pos(pos), _needExist(needExist) {}

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
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_LIST_META);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
        rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      if (_needExist) {
        return Command::fmtZero();
      }
    } else if (!rv.ok()) {
      return rv.status();
    }
    INVARIANT(rv.ok() || !_needExist);

    RecordKey metaRk(expdb.value().chunkId,
                     pCtx->getDbId(),
                     RecordType::RT_LIST_META,
                     key,
                     "");
    PStore kvstore = expdb.value().store;

    std::vector<std::string> valargs;
    for (size_t i = 2; i < args.size(); ++i) {
      valargs.push_back(args[i]);
    }
    for (uint32_t i = 0; i < RETRY_CNT; ++i) {
      auto ptxn = sess->getCtx()->createTransaction(kvstore);
      if (!ptxn.ok()) {
        return ptxn.status();
      }
      Expected<std::string> s1 = genericPush(
        sess, kvstore, ptxn.value(), metaRk, rv, valargs, _pos, _needExist);
      if (!s1.ok()) {
        return s1.status();
      }
      auto s = sess->getCtx()->commitTransaction(ptxn.value());
      if (s.ok()) {
        return s1.value();
      } else if (s.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
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
  ListPos _pos;
  bool _needExist;
};

class LPushCommand : public ListPushWrapper {
 public:
  LPushCommand() : ListPushWrapper("lpush", "wmF", ListPos::LP_HEAD, false) {}
} lpushCommand;

class RPushCommand : public ListPushWrapper {
 public:
  RPushCommand() : ListPushWrapper("rpush", "wmF", ListPos::LP_TAIL, false) {}
} rpushCommand;

class LPushXCommand : public ListPushWrapper {
 public:
  LPushXCommand() : ListPushWrapper("lpushx", "wmF", ListPos::LP_HEAD, true) {}
} lpushxCommand;

class RPushXCommand : public ListPushWrapper {
 public:
  RPushXCommand() : ListPushWrapper("rpushx", "wmF", ListPos::LP_TAIL, true) {}
} rpushxCommand;

// NOTE(deyukong): atomic is not guaranteed
class RPopLPushCommand : public Command {
 public:
  RPopLPushCommand() : Command("rpoplpush", "wm") {}

  ssize_t arity() const {
    return 3;
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
    const std::string& key1 = args[1];
    const std::string& key2 = args[2];

    SessionCtx* pCtx = sess->getCtx();
    auto server = sess->getServerEntry();
    INVARIANT(pCtx != nullptr);

    auto index = getKeysFromCommand(args);
    auto locklist = server->getSegmentMgr()->getAllKeysLocked(
      sess, args, index, mgl::LockMode::LOCK_X);
    if (!locklist.ok()) {
      return locklist.status();
    }

    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key1, RecordType::RT_LIST_META);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
        rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtNull();
    } else if (!rv.ok()) {
      return rv.status();
    }

    auto expdb1 = server->getSegmentMgr()->getDbHasLocked(sess, key1);
    if (!expdb1.ok()) {
      return expdb1.status();
    }
    RecordKey metaRk1(expdb1.value().chunkId,
                      pCtx->getDbId(),
                      RecordType::RT_LIST_META,
                      key1,
                      "");
    PStore kvstore1 = expdb1.value().store;
    auto etxn = pCtx->createTransaction(kvstore1);
    if (!etxn.ok()) {
      return etxn.status();
    }
    bool rollback = true;
    const auto guard = MakeGuard([&rollback, &pCtx] {
      if (rollback) {
        pCtx->rollbackAll();
      }
    });

    std::string val = "";
    for (uint32_t i = 0; i < RETRY_CNT; ++i) {
      Expected<std::string> s =
        genericPop(sess, kvstore1, etxn.value(), metaRk1, rv, ListPos::LP_TAIL);
      if (s.ok()) {
        val = std::move(s.value());
        break;
      }
      if (s.status().code() == ErrorCodes::ERR_NOTFOUND) {
        return Command::fmtNull();
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

    if (key1 == key2) {
      // NOTE(vinchen): if key1 == key2, it should getkv of rv2 using
      // etxn, because key1 has be pop() by etxn. Otherwise if rv2 =
      // Command::expireKeyIfNeeded(), it would get the old value.
      auto rv2 = kvstore1->getKV(metaRk1, etxn.value());
      // Only means that former pop has removed this meta key.
      // if (!rv2.ok()) {
      // INVARIANT(0);
      // return Command::fmtNull();
      // }

      for (uint32_t i = 0; i < RETRY_CNT; ++i) {
        auto s = genericPush(sess,
                             kvstore1,
                             etxn.value(),
                             metaRk1,
                             rv2,
                             {val},
                             ListPos::LP_HEAD,
                             false /*need_exist*/);
        if (s.ok()) {
          pCtx->commitAll("rpoplpush");
          rollback = false;
          return Command::fmtBulk(val);
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
    } else {
      auto expdb2 = server->getSegmentMgr()->getDbHasLocked(sess, key2);
      if (!expdb2.ok()) {
        return expdb2.status();
      }
      RecordKey metaRk2(expdb2.value().chunkId,
                        pCtx->getDbId(),
                        RecordType::RT_LIST_META,
                        key2,
                        "");
      PStore kvstore2 = expdb2.value().store;

      auto etxn2 = pCtx->createTransaction(kvstore2);
      if (!etxn2.ok()) {
        return etxn2.status();
      }

      Expected<RecordValue> rv2 =
        Command::expireKeyIfNeeded(sess, key2, RecordType::RT_LIST_META);
      if (rv2.status().code() != ErrorCodes::ERR_OK &&
          rv2.status().code() != ErrorCodes::ERR_EXPIRED &&
          rv2.status().code() != ErrorCodes::ERR_NOTFOUND) {
        return rv2.status();
      }

      for (uint32_t i = 0; i < RETRY_CNT; ++i) {
        auto s = genericPush(sess,
                             kvstore2,
                             etxn2.value(),
                             metaRk2,
                             rv2,
                             {val},
                             ListPos::LP_HEAD,
                             false /*need_exist*/);
        if (s.ok()) {
          pCtx->commitAll("rpoplpush");
          rollback = false;
          return Command::fmtBulk(val);
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
    }
    INVARIANT_D(0);
    return {ErrorCodes::ERR_INTERNAL, "not reachable"};
  }
} rpoplpushCmd;

class LtrimCommand : public Command {
 public:
  LtrimCommand() : Command("ltrim", "w") {}

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

  Status trimListPessimistic(Session* sess,
                             PStore kvstore,
                             const RecordKey& mk,
                             const ListMetaValue& lm,
                             int64_t start,
                             int64_t end,
                             const Expected<RecordValue>& rv) {
    auto ptxn = sess->getCtx()->createTransaction(kvstore);
    if (!ptxn.ok()) {
      return ptxn.status();
    }
    if (!rv.ok()) {
      return rv.status();
    }
    uint64_t head = lm.getHead();
    uint64_t cnt = 0;
    auto functor = [kvstore, sess, &cnt, &ptxn, &mk](int64_t start,
                                                    int64_t end) -> Status {
      SessionCtx* pCtx = sess->getCtx();
      for (int64_t i = start; i < end; ++i) {
        RecordKey subRk(mk.getChunkId(),
                        pCtx->getDbId(),
                        RecordType::RT_LIST_ELE,
                        mk.getPrimaryKey(),
                        std::to_string(i));
        Status s = kvstore->delKV(subRk, ptxn.value());
        if (!s.ok()) {
          return s;
        }
        cnt += 1;
        if (cnt % 1000 == 0) {
          auto v = sess->getCtx()->commitTransaction(ptxn.value());
          if (!v.ok()) {
            return v.status();
          }
          auto ptxn2 = sess->getCtx()->createTransaction(kvstore);
          if (!ptxn2.ok()) {
            return ptxn2.status();
          }
          ptxn = std::move(ptxn2.value());
        }
      }
      return {ErrorCodes::ERR_OK, ""};
    };
    auto st = functor(head, start + head);
    if (!st.ok()) {
      return st;
    }
    st = functor(head + end + 1, lm.getTail());
    if (!st.ok()) {
      return st;
    }
    if (cnt >= lm.getTail() - lm.getHead()) {
      st = Command::delKeyAndTTL(sess, mk, rv.value(), ptxn.value());
      if (!st.ok()) {
        return st;
      }
    } else {
      ListMetaValue newLm(start + head, end + 1 + head);
      RecordValue metarcd(newLm.encode(),
                          RecordType::RT_LIST_META,
                          sess->getCtx()->getVersionEP(),
                          rv.value().getTtl(),
                          rv);
      st = kvstore->setKV(mk, metarcd, ptxn.value());
      if (!st.ok()) {
        return st;
      }
    }
    auto commitstatus = sess->getCtx()->commitTransaction(ptxn.value());
    return commitstatus.status();
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

    SessionCtx* pCtx = sess->getCtx();
    auto server = sess->getServerEntry();
    auto expdb = server->getSegmentMgr()->getDbWithKeyLock(
      sess, key, mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
      return expdb.status();
    }
    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_LIST_META);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
        rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtOK();
    } else if (!rv.ok()) {
      return rv.status();
    }

    RecordKey metaRk(expdb.value().chunkId,
                     pCtx->getDbId(),
                     RecordType::RT_LIST_META,
                     key,
                     "");
    PStore kvstore = expdb.value().store;

    Expected<ListMetaValue> exptLm =
      ListMetaValue::decode(rv.value().getValue());
    if (!exptLm.ok()) {
      return exptLm.status();
    }

    const ListMetaValue& lm = exptLm.value();
    uint64_t head = lm.getHead();
    uint64_t tail = lm.getTail();
    int64_t len = tail - head;
    INVARIANT_D(len > 0);
    if (len <= 0) {
      return {ErrorCodes::ERR_INTERNAL, "invalid list"};
    }

    int64_t start = estart.value();
    int64_t end = eend.value();
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
      start = len;
      end = len;
    }
    if (end >= len) {
      end = len - 1;
    }
    Status st = trimListPessimistic(sess, kvstore, metaRk, lm, start, end, rv);
    if (!st.ok()) {
      return st;
    }
    return Command::fmtOK();
  }
} ltrimCmd;

class LRangeCommand : public Command {
 public:
  LRangeCommand() : Command("lrange", "r") {}

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
    Expected<int64_t> estart = ::tendisplus::stoll(args[2]);
    if (!estart.ok()) {
      return estart.status();
    }
    Expected<int64_t> eend = ::tendisplus::stoll(args[3]);
    if (!eend.ok()) {
      return eend.status();
    }

    auto server = sess->getServerEntry();
    auto expdb =
      server->getSegmentMgr()->getDbWithKeyLock(sess, key, Command::RdLock());
    if (!expdb.ok()) {
      return expdb.status();
    }

    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_LIST_META);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
      return fmtZeroBulkLen();
    } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return fmtZeroBulkLen();
    } else if (!rv.ok()) {
      return rv.status();
    }

    SessionCtx* pCtx = sess->getCtx();

    PStore kvstore = expdb.value().store;
    auto ptxn = sess->getCtx()->createTransaction(kvstore);
    if (!ptxn.ok()) {
      return ptxn.status();
    }

    Expected<ListMetaValue> exptLm =
      ListMetaValue::decode(rv.value().getValue());
    INVARIANT_D(exptLm.ok());
    if (!exptLm.ok()) {
      return exptLm.status();
    }

    const ListMetaValue& lm = exptLm.value();
    uint64_t head = lm.getHead();
    uint64_t tail = lm.getTail();
    int64_t len = tail - head;
    INVARIANT_D(len > 0);
    if (len <= 0) {
      return {ErrorCodes::ERR_INTERNAL, "invalid list"};
    }

    int64_t start = estart.value();
    int64_t end = eend.value();
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
    int64_t rangelen = (end - start) + 1;
    start += head;
    std::stringstream ss;
    Command::fmtMultiBulkLen(ss, rangelen);
    while (rangelen--) {
      RecordKey subRk(expdb.value().chunkId,
                      pCtx->getDbId(),
                      RecordType::RT_LIST_ELE,
                      key,
                      std::to_string(start));
      Expected<RecordValue> eSubVal = kvstore->getKV(subRk, ptxn.value());
      if (eSubVal.ok()) {
        Command::fmtBulk(ss, eSubVal.value().getValue());
      } else {
        return eSubVal.status();
      }
      start++;
    }
    return ss.str();
  }
} lrangeCmd;

class LIndexCommand : public Command {
 public:
  LIndexCommand() : Command("lindex", "r") {}

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
    int64_t idx = 0;
    try {
      idx = static_cast<int64_t>(std::stoll(args[2]));
    } catch (std::exception& ex) {
      return {ErrorCodes::ERR_PARSEOPT, ex.what()};
    }

    SessionCtx* pCtx = sess->getCtx();
    auto server = sess->getServerEntry();
    auto expdb =
      server->getSegmentMgr()->getDbWithKeyLock(sess, key, Command::RdLock());
    if (!expdb.ok()) {
      return expdb.status();
    }

    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_LIST_META);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
      return fmtNull();
    } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return fmtNull();
    } else if (!rv.ok()) {
      return rv.status();
    }

    PStore kvstore = expdb.value().store;
    // uint32_t storeId = expdb.value().dbId;
    RecordKey metaRk(expdb.value().chunkId,
                     pCtx->getDbId(),
                     RecordType::RT_LIST_META,
                     key,
                     "");

    auto ptxn = sess->getCtx()->createTransaction(kvstore);
    if (!ptxn.ok()) {
      return ptxn.status();
    }

    Expected<ListMetaValue> exptLm =
      ListMetaValue::decode(rv.value().getValue());
    INVARIANT_D(exptLm.ok());
    if (!exptLm.ok()) {
      return exptLm.status();
    }

    const ListMetaValue& lm = exptLm.value();
    uint64_t head = lm.getHead();
    uint64_t tail = lm.getTail();
    uint64_t mappingIdx = 0;
    if (idx >= 0) {
      mappingIdx = idx + head;
    } else {
      mappingIdx = idx + tail;
    }
    if (mappingIdx < head || mappingIdx >= tail) {
      return fmtNull();
    }
    RecordKey subRk(expdb.value().chunkId,
                    pCtx->getDbId(),
                    RecordType::RT_LIST_ELE,
                    key,
                    std::to_string(mappingIdx));
    Expected<RecordValue> eSubVal = kvstore->getKV(subRk, ptxn.value());
    if (eSubVal.ok()) {
      return fmtBulk(eSubVal.value().getValue());
    } else {
      return eSubVal.status();
    }
  }
} lindexCommand;

class LSetCommand : public Command {
 public:
  LSetCommand() : Command("lset", "wm") {}

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
    Expected<int64_t> expIndex = tendisplus::stoll(args[2]);
    if (!expIndex.ok()) {
      return expIndex.status();
    }
    int64_t index = expIndex.value();
    const std::string& value = args[3];

    auto server = sess->getServerEntry();
    auto pCtx = sess->getCtx();
    auto expdb = server->getSegmentMgr()->getDbWithKeyLock(
      sess, key, mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
      return expdb.status();
    }

    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_LIST_META);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
        rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return {ErrorCodes::ERR_NO_KEY, ""};
    } else if (!rv.ok()) {
      return rv.status();
    }

    Expected<ListMetaValue> expLm =
      ListMetaValue::decode(rv.value().getValue());
    ListMetaValue lm = std::move(expLm.value());
    uint64_t head = lm.getHead();
    uint64_t tail = lm.getTail();
    uint64_t realIndex(0);
    if (index < 0) {
      realIndex = tail + index;
    } else {
      realIndex = head + index;
    }
    if (realIndex < head || realIndex >= tail) {
      return {ErrorCodes::ERR_OUT_OF_RANGE, ""};
    }

    PStore kvstore = expdb.value().store;
    for (uint32_t i = 0; i < RETRY_CNT; ++i) {
      auto ptxn = sess->getCtx()->createTransaction(kvstore);
      if (!ptxn.ok()) {
        return ptxn.status();
      }

      RecordKey subRk(expdb.value().chunkId,
                      pCtx->getDbId(),
                      RecordType::RT_LIST_ELE,
                      key,
                      std::to_string(realIndex));
      RecordValue subRv(value, RecordType::RT_LIST_ELE, -1);
      Status s = kvstore->setKV(subRk, subRv, ptxn.value());
      if (!s.ok()) {
        return s;
      }
      // update meta key's revision
      RecordKey metaRk(expdb.value().chunkId,
                       pCtx->getDbId(),
                       RecordType::RT_LIST_META,
                       key,
                       "");
      s = kvstore->setKV(metaRk,
                         RecordValue(lm.encode(),
                                     RecordType::RT_LIST_META,
                                     sess->getCtx()->getVersionEP(),
                                     rv.value().getTtl(),
                                     rv),
                         ptxn.value());
      if (!s.ok()) {
        return s;
      }

      Expected<uint64_t> expCmt = sess->getCtx()->commitTransaction(
              ptxn.value());
      if (expCmt.ok()) {
        return Command::fmtOK();
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
} lsetCmd;

class LRemCommand : public Command {
 public:
  LRemCommand() : Command("lrem", "w") {}

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

  Expected<std::string> run(Session* sess) {
    const std::string& key = sess->getArgs()[1];
    Expected<int32_t> expCnt = tendisplus::stol(sess->getArgs()[2]);
    if (!expCnt.ok()) {
      return expCnt.status();
    }
    int32_t count = expCnt.value();
    const std::string& value = sess->getArgs()[3];

    auto server = sess->getServerEntry();
    auto pCtx = sess->getCtx();
    auto expdb = server->getSegmentMgr()->getDbWithKeyLock(
      sess, key, mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
      return expdb.status();
    }

    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_LIST_META);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
        rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtZero();
    } else if (!rv.ok()) {
      return rv.status();
    }

    ListPos pos(ListPos::LP_HEAD);
    if (count < 0) {
      pos = ListPos::LP_TAIL;
      count = -count;
    }

    Expected<ListMetaValue> expLm =
      ListMetaValue::decode(rv.value().getValue());
    INVARIANT_D(expLm.ok());
    if (!expLm.ok()) {
      return expLm.status();
    }
    ListMetaValue lm = std::move(expLm.value());
    uint64_t head = lm.getHead();
    uint64_t tail = lm.getTail();

    size_t len = tail - head;
    uint64_t index = pos == ListPos::LP_HEAD ? head : tail - 1;
    std::vector<uint64_t> hole;
    hole.push_back(head - 1);

    PStore kvstore = expdb.value().store;
    auto ptxn = sess->getCtx()->createTransaction(kvstore);
    if (!ptxn.ok()) {
      return ptxn.status();
    }

    for (size_t i = 0; i < len; i++) {
      RecordKey subRk(expdb.value().chunkId,
                      pCtx->getDbId(),
                      RecordType::RT_LIST_ELE,
                      key,
                      std::to_string(index));
      Expected<RecordValue> expRv = kvstore->getKV(subRk, ptxn.value());
      if (!expRv.ok()) {
        return expRv.status();
      }
      if (expRv.value().getValue() == value) {
        hole.push_back(index);
        Status s = kvstore->delKV(subRk, ptxn.value());
        if (!s.ok()) {
          return s;
        }
        if (count >= 0 && hole.size() - 1 == static_cast<uint32_t>(count)) {
          break;
        }
      }
      if (pos == ListPos::LP_HEAD)
        index++;
      else
        index--;
    }
    hole.push_back(tail);
    if (hole.size() == 2) {
      return Command::fmtZero();
    }

    size_t lasthole(0);
    uint64_t largest(0);
    std::pair<uint64_t, uint64_t> seg;
    if (pos == ListPos::LP_TAIL) {
      std::sort(hole.begin(), hole.end());
    }
    for (size_t i = 1; i < hole.size(); i++) {
      if (hole[i] - hole[lasthole] > largest) {
        largest = hole[i] - hole[lasthole];
        seg = std::make_pair(lasthole, i);
      }
      lasthole = i;
    }

    uint64_t oldHead(head), oldTail(tail);
    uint64_t lBorder, rBorder;
    lBorder = seg.first;
    rBorder = seg.second;
    INVARIANT(hole[rBorder] > hole[lBorder]);

    uint64_t destPos(hole[lBorder]);
    for (ssize_t i = lBorder - 1; i >= 0; i--) {
      uint64_t pos = hole[i + 1] - 1;
      uint64_t nextHole = hole[i];
      for (; pos > nextHole; pos--) {
        RecordKey subRk(expdb.value().chunkId,
                        pCtx->getDbId(),
                        RecordType::RT_LIST_ELE,
                        key,
                        std::to_string(pos));
        Expected<RecordValue> eSubVal = kvstore->getKV(subRk, ptxn.value());
        if (!eSubVal.ok()) {
          return eSubVal.status();
        }
        RecordKey newRk(expdb.value().chunkId,
                        pCtx->getDbId(),
                        RecordType::RT_LIST_ELE,
                        key,
                        std::to_string(destPos));
        Status s = kvstore->setKV(newRk, eSubVal.value(), ptxn.value());
        if (!s.ok()) {
          return s;
        }
        destPos--;
      }
    }
    head = destPos + 1;

    destPos = hole[rBorder];
    for (size_t i = rBorder + 1; i < hole.size(); i++) {
      uint64_t pos = hole[i - 1] + 1;
      uint64_t nextHole = hole[i];
      for (; pos < nextHole; pos++) {
        RecordKey subRk(expdb.value().chunkId,
                        pCtx->getDbId(),
                        RecordType::RT_LIST_ELE,
                        key,
                        std::to_string(pos));
        Expected<RecordValue> eSubVal = kvstore->getKV(subRk, ptxn.value());
        if (!eSubVal.ok()) {
          return eSubVal.status();
        }
        RecordKey newRk(expdb.value().chunkId,
                        pCtx->getDbId(),
                        RecordType::RT_LIST_ELE,
                        key,
                        std::to_string(destPos));
        Status s = kvstore->setKV(newRk, eSubVal.value(), ptxn.value());
        if (!s.ok()) {
          return s;
        }
        destPos++;
      }
    }
    tail = destPos;

    // explicit delete useless records between old end and new end.
    while (true) {
      uint64_t delPos;
      if (oldHead != head) {
        delPos = oldHead++;
      } else if (oldTail != tail) {
        delPos = --oldTail;
      } else {
        break;
      }

      RecordKey subRk(expdb.value().chunkId,
                      pCtx->getDbId(),
                      RecordType::RT_LIST_ELE,
                      key,
                      std::to_string(delPos));
      Status s = kvstore->delKV(subRk, ptxn.value());
      if (!s.ok()) {
        return s;
      }
    }

    lm.setHead(head);
    lm.setTail(tail);
    RecordKey metaRk(expdb.value().chunkId,
                     pCtx->getDbId(),
                     RecordType::RT_LIST_META,
                     key,
                     "");
    Status s;
    if (head == tail) {
      s = Command::delKeyAndTTL(sess, metaRk, rv.value(), ptxn.value());
    } else {
      s = kvstore->setKV(metaRk,
                         RecordValue(lm.encode(),
                                     RecordType::RT_LIST_META,
                                     sess->getCtx()->getVersionEP(),
                                     rv.value().getTtl(),
                                     rv),
                         ptxn.value());
    }
    if (!s.ok()) {
      return s;
    }

    Expected<uint64_t> expCmt = sess->getCtx()->commitTransaction(ptxn.value());
    if (!expCmt.ok()) {
      return expCmt.status();
    }

    return Command::fmtLongLong(static_cast<int64_t>(hole.size() - 2));
  }
} lremCmd;

class LInsertCommand : public Command {
 public:
  LInsertCommand() : Command("linsert", "wm") {}

  ssize_t arity() const {
    return 5;
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
    int32_t step;
    const std::string& pivot = args[3];
    const std::string& value = args[4];
    if (!::strcasecmp(args[2].c_str(), "before")) {
      step = 1;
    } else if (!::strcasecmp(args[2].c_str(), "after")) {
      step = -1;
    } else {
      return {ErrorCodes::ERR_PARSEOPT, "syntax error"};
    }
    auto server = sess->getServerEntry();
    auto pCtx = sess->getCtx();
    auto expdb = server->getSegmentMgr()->getDbWithKeyLock(
      sess, key, mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
      return expdb.status();
    }

    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_LIST_META);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
        rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtZero();
    } else if (!rv.ok()) {
      return rv.status();
    }

    Expected<ListMetaValue> expLm =
      ListMetaValue::decode(rv.value().getValue());
    INVARIANT_D(expLm.ok());
    if (!expLm.ok()) {
      return expLm.status();
    }

    ListMetaValue lm = std::move(expLm.value());
    uint64_t head = lm.getHead();
    uint64_t tail = lm.getTail();
    uint64_t len = tail - head;
    uint64_t index = head;

    PStore kvstore = expdb.value().store;
    auto ptxn = sess->getCtx()->createTransaction(kvstore);
    if (!ptxn.ok()) {
      return ptxn.status();
    }
    while (len > 0) {
      RecordKey subRk(expdb.value().chunkId,
                      pCtx->getDbId(),
                      RecordType::RT_LIST_ELE,
                      key,
                      std::to_string(index));
      Expected<RecordValue> eSubRv = kvstore->getKV(subRk, ptxn.value());
      if (!eSubRv.ok()) {
        return eSubRv.status();
      }
      RecordValue& subRv = eSubRv.value();
      if (subRv.getValue() == pivot) {
        break;
      }
      len--;
      index++;
    }
    if (len <= 0) {
      return Command::fmtLongLong(-1);
    }
    // we traverse the list in reverse order,
    // so here need to subtract the step.
    int32_t leftEle = index - head;
    int32_t rightEle = tail - index - 1;

    int32_t moveLen(0);
    if (step < 0) {
      leftEle += 1;
    } else if (step > 0) {
      rightEle += 1;
    }

    if (leftEle < rightEle) {
      step = 1;
      head = head - 1;
      index = head;
      moveLen = leftEle;
    } else {
      step = -1;
      tail = tail + 1;
      index = tail - 1;
      moveLen = rightEle;
    }

    while (moveLen > 0) {
      RecordKey subRk(expdb.value().chunkId,
                      pCtx->getDbId(),
                      RecordType::RT_LIST_ELE,
                      key,
                      std::to_string(index + step));
      Expected<RecordValue> eSubVal = kvstore->getKV(subRk, ptxn.value());
      if (!eSubVal.ok()) {
        return eSubVal.status();
      }

      RecordKey newRk(subRk.getChunkId(),
                      subRk.getDbId(),
                      RecordType::RT_LIST_ELE,
                      key,
                      std::to_string(index));
      Status s = kvstore->setKV(newRk, eSubVal.value(), ptxn.value());
      if (!s.ok()) {
        return s;
      }
      index += step;
      moveLen--;
    }
    RecordKey targRk(expdb.value().chunkId,
                     pCtx->getDbId(),
                     RecordType::RT_LIST_ELE,
                     key,
                     std::to_string(index));
    RecordValue targRv(value, RecordType::RT_LIST_ELE, -1);
    Status s = kvstore->setKV(targRk, targRv, ptxn.value());
    if (!s.ok()) {
      return s;
    }

    lm.setHead(head);
    lm.setTail(tail);
    RecordKey metaRk(expdb.value().chunkId,
                     pCtx->getDbId(),
                     RecordType::RT_LIST_META,
                     key,
                     "");
    s = kvstore->setKV(metaRk,
                       RecordValue(lm.encode(),
                                   RecordType::RT_LIST_META,
                                   pCtx->getVersionEP(),
                                   rv.value().getTtl(),
                                   rv),
                       ptxn.value());
    if (!s.ok()) {
      return s;
    }

    Expected<uint64_t> expCmt = sess->getCtx()->commitTransaction(ptxn.value());
    if (!expCmt.ok()) {
      return expCmt.status();
    }

    return Command::fmtLongLong(lm.getTail() - lm.getHead());
  }
} linsertCmd;

}  // namespace tendisplus
