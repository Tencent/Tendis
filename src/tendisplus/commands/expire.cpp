// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <string>
#include <utility>
#include <memory>
#include <algorithm>
#include <cctype>
#include <clocale>
#include <map>
#include "glog/logging.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/time.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

Expected<bool> expireBeforeNow(Session* sess,
                               RecordType type,
                               const std::string& key,
                               Transaction* txn) {
  // pass txn to params, and commit only by top caller.
  return Command::delKeyChkExpire(sess, key, type, txn);
}

// return true if exists
// return false if not exists
// return error if has error
Expected<bool> expireAfterNow(Session* sess,
                              RecordType type,
                              const std::string& key,
                              uint64_t expireAt,
                              Transaction* txn) {
  Expected<RecordValue> rv = Command::expireKeyIfNeeded(sess, key, type);
  if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
    return false;
  } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
    return false;
  } else if (!rv.status().ok()) {
    return rv.status();
  }

  INVARIANT_D(type == RecordType::RT_DATA_META);
  // record exists and not expired
  auto server = sess->getServerEntry();
  auto expdb =
    server->getSegmentMgr()->getDbWithKeyLock(sess, key, mgl::LockMode::LOCK_X);
  if (!expdb.ok()) {
    return expdb.status();
  }
  // uint32_t storeId = expdb.value().dbId;
  PStore kvstore = expdb.value().store;
  SessionCtx* pCtx = sess->getCtx();
  RecordKey rk(expdb.value().chunkId, pCtx->getDbId(), type, key, "");
  // if (Command::isKeyLocked(sess, storeId, rk.encode())) {
  //     return {ErrorCodes::ERR_BUSY, "key locked"};
  // }

  for (uint32_t i = 0; i < Command::RETRY_CNT; ++i) {
    Expected<RecordValue> eValue = kvstore->getKV(rk, txn);
    if (eValue.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return false;
    } else if (!eValue.ok()) {
      return eValue.status();
    }
    auto rv = eValue.value();
    auto vt = rv.getRecordType();
    Status s;

    if (vt != RecordType::RT_KV) {
      // delete old index entry
      auto oldTTL = rv.getTtl();
      if (oldTTL != 0) {
        TTLIndex o_ictx(key, vt, pCtx->getDbId(), oldTTL);

        s = txn->delKV(o_ictx.encode());
        if (!s.ok()) {
          return s;
        }
      }

      // add new index entry
      TTLIndex n_ictx(key, vt, pCtx->getDbId(), expireAt);
      s = txn->setKV(n_ictx.encode(),
                     RecordValue(RecordType::RT_TTL_INDEX).encode());
      if (!s.ok()) {
        return s;
      }
    }

    // update
    rv.setTtl(expireAt);
    rv.setVersionEP(pCtx->getVersionEP());
    s = kvstore->setKV(rk, rv, txn);
    if (!s.ok()) {
      return s;
    }
    return true;
  }

  INVARIANT_D(0);
  return {ErrorCodes::ERR_INTERNAL, "not reachable"};
}

// NOTE(takenliu) txn not commited, caller need commit it.
Expected<std::string> expireGeneric(Session* sess,
                                    int64_t expireAt,
                                    const std::string& key,
                                    Transaction* txn) {
  if (expireAt >= (int64_t)msSinceEpoch() ||
      sess->getServerEntry()->getParams()->noexpire) {
    bool atLeastOne = false;
    for (auto type : {RecordType::RT_DATA_META}) {
      auto done = expireAfterNow(sess, type, key, expireAt, txn);
      if (!done.ok()) {
        return done.status();
      }
      atLeastOne |= done.value();
    }
    return atLeastOne ? Command::fmtOne() : Command::fmtZero();
  } else {
    bool atLeastOne = false;
    for (auto type : {RecordType::RT_DATA_META}) {
      auto done = expireBeforeNow(sess, type, key, txn);
      DLOG(INFO) << " expire before " << key << " " << rt2Char(type);
      if (!done.ok()) {
        return done.status();
      }
      atLeastOne |= done.value();
    }
    return atLeastOne ? Command::fmtOne() : Command::fmtZero();
  }
  INVARIANT_D(0);
  return {ErrorCodes::ERR_INTERNAL, "not reachable"};
}

class GeneralExpireCommand : public Command {
 public:
  GeneralExpireCommand(const std::string& name, const char* sflags)
    : Command(name, sflags) {}

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
    const std::string& key = sess->getArgs()[1];
    auto expt = ::tendisplus::stoll(sess->getArgs()[2]);
    if (!expt.ok()) {
      return expt.status();
    }
    auto expdb = sess->getServerEntry()->getSegmentMgr()->getDbWithKeyLock(
      sess, key, mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
      LOG(ERROR) << "getDbWithKeyLock failed, key" << key
                 << " err:" << expdb.status().toString();
      return expdb.status();
    }
    PStore kvstore = expdb.value().store;
    auto ptxn = sess->getCtx()->createTransaction(kvstore);

    int64_t millsecs = 0;
    if (Command::getName() == "expire") {
      millsecs = msSinceEpoch() + expt.value() * 1000;
    } else if (Command::getName() == "pexpire") {
      millsecs = msSinceEpoch() + expt.value();
    } else if (Command::getName() == "expireat") {
      millsecs = expt.value() * 1000;
    } else if (Command::getName() == "pexpireat") {
      millsecs = expt.value();
    } else {
      INVARIANT_D(0);
    }

    auto s = expireGeneric(sess, millsecs, key, ptxn.value());
    if (!s.ok()) {
      return s.status();
    }
    auto expCmt = sess->getCtx()->commitTransaction(ptxn.value());
    if (!expCmt.ok()) {
      return expCmt.status();
    }
    return s;
  }
};

class ExpireCommand : public GeneralExpireCommand {
 public:
  ExpireCommand() : GeneralExpireCommand("expire", "wF") {}
} expireCmd;

class PExpireCommand : public GeneralExpireCommand {
 public:
  PExpireCommand() : GeneralExpireCommand("pexpire", "wF") {}
} pexpireCmd;

class ExpireAtCommand : public GeneralExpireCommand {
 public:
  ExpireAtCommand() : GeneralExpireCommand("expireat", "wF") {}
} expireatCmd;

class PExpireAtCommand : public GeneralExpireCommand {
 public:
  PExpireAtCommand() : GeneralExpireCommand("pexpireat", "wF") {}
} pexpireatCmd;

class GenericTtlCommand : public Command {
 public:
  GenericTtlCommand(const std::string& name, const char* sflags)
    : Command(name, sflags) {}

  Expected<std::string> run(Session* sess) final {
    const std::string& key = sess->getArgs()[1];

    for (auto type : {RecordType::RT_DATA_META}) {
      Expected<RecordValue> rv = Command::expireKeyIfNeeded(sess, key, type);
      if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
        continue;
      } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
        continue;
      } else if (!rv.ok()) {
        return rv.status();
      }
      if (rv.value().getTtl() == 0) {
        return Command::fmtLongLong(-1);
      }
      int64_t ms = rv.value().getTtl() - msSinceEpoch();
      if (ms < 0) {
        ms = 1;
      }
      if (Command::getName() == "ttl") {
        return Command::fmtLongLong((ms + 500) / 1000);
      } else if (Command::getName() == "pttl") {
        return Command::fmtLongLong(ms);
      } else {
        INVARIANT_D(0);
      }
    }
    return Command::fmtLongLong(-2);
  }
};

class TtlCommand : public GenericTtlCommand {
 public:
  TtlCommand() : GenericTtlCommand("ttl", "rF") {}

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
} ttlCmd;

class PTtlCommand : public GenericTtlCommand {
 public:
  PTtlCommand() : GenericTtlCommand("pttl", "rF") {}

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
} pttlCmd;

class ExistsCommand : public Command {
 public:
  ExistsCommand() : Command("exists", "rF") {}

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
    auto& args = sess->getArgs();
    size_t count = 0;
    auto server = sess->getServerEntry();
    INVARIANT(server != nullptr);

    auto index = getKeysFromCommand(args);
    auto locklist = server->getSegmentMgr()->getAllKeysLocked(
      sess, args, index, Command::RdLock());
    if (!locklist.ok()) {
      return locklist.status();
    }

    for (size_t j = 1; j < args.size(); j++) {
      const std::string& key = args[j];

      Expected<RecordValue> rv =
        Command::expireKeyIfNeeded(sess, key, RecordType::RT_DATA_META);
      if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
        continue;
      } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
        continue;
      } else if (!rv.ok()) {
        return rv.status();
      }
      count++;
    }
    return Command::fmtLongLong(count);
  }
} existsCmd;

class TypeCommand : public Command {
 public:
  TypeCommand() : Command("type", "rF") {}

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

    const std::map<RecordType, std::string> lookup = {
      {RecordType::RT_KV, "string"},
      {RecordType::RT_LIST_META, "list"},
      {RecordType::RT_HASH_META, "hash"},
      {RecordType::RT_SET_META, "set"},
      {RecordType::RT_ZSET_META, "zset"},
    };

    auto server = sess->getServerEntry();
    auto expdb =
      server->getSegmentMgr()->getDbWithKeyLock(sess, key, Command::RdLock());
    if (!expdb.ok()) {
      return expdb.status();
    }

    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_DATA_META);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
        rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtStatus("none");
    } else if (!rv.ok()) {
      LOG_STATUS(rv.status());
      return Command::fmtStatus("unknown");
    }
    auto vt = rv.value().getRecordType();
    return Command::fmtStatus(lookup.at(vt));
  }
} typeCmd;

class PersistCommand : public Command {
 public:
  PersistCommand() : Command("persist", "wF") {}

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

    auto server = sess->getServerEntry();
    auto expdb = server->getSegmentMgr()->getDbWithKeyLock(
      sess, key, mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
      return expdb.status();
    }

    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_DATA_META);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
        rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtZero();
    } else if (!rv.ok()) {
      return rv.status();
    }

    // change the ttl of rv
    rv.value().setTtl(0);
    auto vt = rv.value().getRecordType();
    RecordKey mk(expdb.value().chunkId, sess->getCtx()->getDbId(), vt, key, "");

    PStore kvstore = expdb.value().store;
    auto ptxn = sess->getCtx()->createTransaction(kvstore);
    if (!ptxn.ok()) {
      return ptxn.status();
    }

    auto s = kvstore->setKV(mk, rv.value(), ptxn.value());
    if (!s.ok()) {
      return s;
    }

    auto s1 = sess->getCtx()->commitTransaction(ptxn.value());
    if (!s1.ok()) {
      return s1.status();
    }

    return Command::fmtOne();
  }
} persistCmd;

class RevisionCommand : public Command {
 public:
  RevisionCommand() : Command("revision", "was") {}

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

  // REVISION key revision [timestamp(expire time)]
  Expected<std::string> run(Session* sess) final {
    const auto& args = sess->getArgs();
    const auto& key = args[1];

    auto expvs = tendisplus::stoul(args[2]);
    if (!expvs.ok()) {
      return expvs.status();
    }

    int64_t expire = 0;
    if (args.size() > 3) {
      auto emillsecs = tendisplus::stoul(args[3]);
      if (!emillsecs.ok()) {
        return emillsecs.status();
      }
      expire = emillsecs.value();
    }
    auto expdb = sess->getServerEntry()->getSegmentMgr()->getDbWithKeyLock(
      sess, key, mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
      return expdb.status();
    }
    PStore kvstore = expdb.value().store;
    auto ptxn = sess->getCtx()->createTransaction(kvstore);

    Expected<RecordValue> exprv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_DATA_META);
    // already expired, should return here.
    if (exprv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      auto expCmt = sess->getCtx()->commitTransaction(ptxn.value());
      if (!expCmt.ok()) {
        return expCmt.status();
      }
      return Command::fmtOK();
    }

    // other errorcode
    if (!exprv.ok()) {
      return exprv.status();
    }

    // set versionEP
    RecordKey rk(expdb.value().chunkId,
                 sess->getCtx()->getDbId(),
                 RecordType::RT_DATA_META,
                 key,
                 "");
    auto rv = std::move(exprv.value());
    rv.setVersionEP(expvs.value());
    // set expire time
    if (expire > 0) {
      if (rv.getTtl() != 0) {
        LOG(ERROR) << "revision expire not empty,key:" << key
                   << " old ttl:" << rv.getTtl()
                   << " versionep:" << rv.getVersionEP()
                   << " new ttl:" << expire << " versionep:" << expvs.value();
        return {ErrorCodes::ERR_INTERGER,
                "expire not empty " + to_string(rv.getTtl())};
      }
      rv.setTtl(expire);
    }

    auto s = kvstore->setKV(rk, rv, ptxn.value());
    if (!s.ok()) {
      return s;
    }

    auto expCmt = sess->getCtx()->commitTransaction(ptxn.value());
    if (!expCmt.ok()) {
      return expCmt.status();
    }
    return Command::fmtOK();
  }
} revisionCmd;

}  // namespace tendisplus
