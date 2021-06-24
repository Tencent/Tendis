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
#include <queue>
#include <cmath>
#include "glog/logging.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/utils/scopeguard.h"

namespace tendisplus {

constexpr int32_t REDIS_SET_NO_FLAGS = 0;

// set if not exists
constexpr int32_t REDIS_SET_NX = (1 << 0);

// set if exists
constexpr int32_t REDIS_SET_XX = (1 << 1);

// set and expire if not exists
constexpr int32_t REDIS_SET_NXEX = (1 << 2);

struct SetParams {
  SetParams() : key(""), value(""), flags(REDIS_SET_NO_FLAGS), expire(0) {}
  std::string key;
  std::string value;
  int32_t flags;
  int64_t expire;
};

Expected<std::string> setGeneric(Session* sess,
                                 PStore store,
                                 Transaction* txn,
                                 int32_t flags,
                                 const RecordKey& key,
                                 const RecordValue& val,
                                 bool checkType,
                                 bool endTxn,
                                 const std::string& okReply,
                                 const std::string& abortReply) {
  bool diffType = false;
  bool needExpire = false;
  bool notExist = false;
  if ((flags & REDIS_SET_NX) || (flags & REDIS_SET_XX) ||
      (flags & REDIS_SET_NXEX)) {
    Expected<RecordValue> eValue = store->getKV(key, txn);
    if ((!eValue.ok()) && eValue.status().code() != ErrorCodes::ERR_NOTFOUND) {
      return eValue.status();
    }

    uint64_t currentTs = 0;
    uint64_t targetTtl = 0;
    checkType = false;
    if (eValue.ok()) {
      currentTs = msSinceEpoch();
      targetTtl = eValue.value().getTtl();
      if (eValue.value().getRecordType() != RecordType::RT_KV) {
        diffType = true;
      }
    } else if (eValue.status().code() == ErrorCodes::ERR_NOTFOUND) {
      notExist = true;
    } else {
      return eValue.status();
    }

    needExpire = (targetTtl != 0 && currentTs >= targetTtl);
    bool exists =
      (eValue.status().code() == ErrorCodes::ERR_OK) && (!needExpire);
    if ((flags & REDIS_SET_NX && exists) ||
        (flags & REDIS_SET_XX && (!exists)) ||
        (flags & REDIS_SET_NXEX && exists)) {
      return abortReply == "" ? Command::fmtNull() : abortReply;
    }
  }

  /** NOTE(vinchen):
   * For better performance, set() directly without get() is more fast.
   * But because of RT_DATA_META, the string set is possible to
   * override other meta type. It would lead to some garbage in rocksdb.
   * In fact, because of the redis layer in hybrid storage, the override problem
   * would never happen. So keep the set() directly when `checkKeyTypeForSet` is
   * false. */
  if (checkType) {
    Expected<RecordValue> eValue = store->getKV(key, txn);
    if (eValue.ok()) {
      if (eValue.value().getRecordType() != RecordType::RT_KV) {
        diffType = true;
      }
    } else if (eValue.status().code() == ErrorCodes::ERR_NOTFOUND) {
      notExist = true;
    } else {
      return eValue.status();
    }
  }

  /**
   * Only delete key when key type is changed.
   */
  if (diffType) {
    auto s =
      Command::delKey(sess, key.getPrimaryKey(), RecordType::RT_DATA_META, txn);
    if (!s.ok() && s.code() != ErrorCodes::ERR_NOTFOUND) {
      return s;
    }
  }

  // here we have no need to check expire since we will overwrite it
  Status status = store->setKV(key, val, txn);
  TEST_SYNC_POINT("setGeneric::SetKV::1");
  if (!status.ok()) {
    return status;
  }
  if (endTxn) {
    Expected<uint64_t> exptCommit = sess->getCtx()->commitTransaction(txn);
    if (!exptCommit.ok()) {
      return exptCommit.status();
    }
  }
  return okReply == "" ? Command::fmtOK() : okReply;
}

class SetCommand : public Command {
 public:
  SetCommand() : Command("set", "wm") {}

  Expected<SetParams> parse(Session* sess) const {
    const auto& args = sess->getArgs();
    SetParams result;
    if (args.size() < 3) {
      return {ErrorCodes::ERR_PARSEPKT, "invalid set params"};
    }
    result.key = args[1];
    result.value = args[2];
    try {
      for (size_t i = 3; i < args.size(); i++) {
        const std::string& s = toLower(args[i]);
        if (s == "nx") {
          result.flags |= REDIS_SET_NX;
        } else if (s == "xx") {
          result.flags |= REDIS_SET_XX;
        } else if (s == "ex" && i + 1 < args.size()) {
          result.expire = std::stoll(args[i + 1]) * 1000ULL;
          if (result.expire <= 0) {
            return {ErrorCodes::ERR_PARSEPKT, "invalid expire time"};
          }
          i++;
        } else if (s == "px" && i + 1 < args.size()) {
          result.expire = std::stoll(args[i + 1]);
          if (result.expire <= 0) {
            return {ErrorCodes::ERR_PARSEPKT, "invalid expire time"};
          }
          i++;
        } else {
          return {ErrorCodes::ERR_PARSEPKT, "syntax error"};
        }
      }
    } catch (std::exception& ex) {
      return {ErrorCodes::ERR_PARSEPKT,
              "value is not an integer or out of range"};
    }
    return result;
  }

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
    Expected<SetParams> exptParams = parse(sess);
    if (!exptParams.ok()) {
      return exptParams.status();
    }

    // NOTE(deyukong): no need to do a expireKeyIfNeeded
    // on a simple kv. We will overwrite it.
    const SetParams& params = exptParams.value();
    auto server = sess->getServerEntry();
    INVARIANT(server != nullptr);
    auto expdb = server->getSegmentMgr()->getDbWithKeyLock(
      sess, params.key, mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
      return expdb.status();
    }

    PStore kvstore = expdb.value().store;
    auto ptxn = sess->getCtx()->createTransaction(kvstore);
    if (!ptxn.ok()) {
      return ptxn.status();
    }

    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);

    RecordKey rk(expdb.value().chunkId,
                 pCtx->getDbId(),
                 RecordType::RT_KV,
                 params.key,
                 "");

    uint64_t ts = 0;
    if (params.expire != 0) {
      ts = msSinceEpoch() + params.expire;
    }
    RecordValue rv(params.value, RecordType::RT_KV, pCtx->getVersionEP(), ts);

    bool checkKeyTypeForSet = server->checkKeyTypeForSet();
    for (int32_t i = 0; i < RETRY_CNT - 1; ++i) {
      auto result = setGeneric(sess,
                               kvstore,
                               ptxn.value(),
                               params.flags,
                               rk,
                               rv,
                               checkKeyTypeForSet,
                               true,
                               "",
                               "");
      if (result.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
        return result;
      }
    }
    return setGeneric(sess,
                      kvstore,
                      ptxn.value(),
                      params.flags,
                      rk,
                      rv,
                      checkKeyTypeForSet,
                      true,
                      "",
                      "");
  }
} setCommand;

class SetexGeneralCommand : public Command {
 public:
  explicit SetexGeneralCommand(const std::string& name, const char* flags)
    : Command(name, flags) {}

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

  Expected<std::string> runGeneral(Session* sess,
                                   const std::string& key,
                                   const std::string& val,
                                   uint64_t ttl) {
    auto server = sess->getServerEntry();
    INVARIANT(server != nullptr);
    auto expdb = server->getSegmentMgr()->getDbWithKeyLock(
      sess, key, mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
      return expdb.status();
    }
    PStore kvstore = expdb.value().store;
    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);

    RecordKey rk(
      expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_KV, key, "");
    RecordValue rv(val, RecordType::RT_KV, pCtx->getVersionEP(), ttl);

    bool checkKeyTypeForSet = server->checkKeyTypeForSet();
    for (int32_t i = 0; i < RETRY_CNT; ++i) {
      auto ptxn = sess->getCtx()->createTransaction(kvstore);
      if (!ptxn.ok()) {
        return ptxn.status();
      }
      auto result = setGeneric(sess,
                               kvstore,
                               ptxn.value(),
                               REDIS_SET_NO_FLAGS,
                               rk,
                               rv,
                               checkKeyTypeForSet,
                               true,
                               "",
                               "");
      if (result.ok()) {
        return result.value();
      }
      if (result.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
        return result.status();
      }
      if (i == RETRY_CNT - 1) {
        return result.status();
      } else {
        continue;
      }
    }

    INVARIANT_D(0);
    return {ErrorCodes::ERR_INTERNAL, "not reachable"};
  }
};

class SetExCommand : public SetexGeneralCommand {
 public:
  SetExCommand() : SetexGeneralCommand("setex", "wm") {}

  Expected<std::string> run(Session* sess) final {
    const std::string& key = sess->getArgs()[1];
    const std::string& val = sess->getArgs()[3];
    Expected<int64_t> eexpire = ::tendisplus::stoll(sess->getArgs()[2]);
    if (!eexpire.ok()) {
      return eexpire.status();
    }
    if (eexpire.value() <= 0) {
      return {ErrorCodes::ERR_PARSEPKT, "invalid expire time"};
    }
    return runGeneral(sess, key, val, msSinceEpoch() + eexpire.value() * 1000);
  }
} setexCmd;

class PSetExCommand : public SetexGeneralCommand {
 public:
  PSetExCommand() : SetexGeneralCommand("psetex", "wm") {}

  Expected<std::string> run(Session* sess) final {
    const std::string& key = sess->getArgs()[1];
    const std::string& val = sess->getArgs()[3];
    Expected<int64_t> eexpire = ::tendisplus::stoll(sess->getArgs()[2]);
    if (!eexpire.ok()) {
      return eexpire.status();
    }
    if (eexpire.value() <= 0) {
      return {ErrorCodes::ERR_PARSEPKT, "invalid expire time"};
    }
    return runGeneral(sess, key, val, msSinceEpoch() + eexpire.value());
  }
} psetexCmd;

class SetNxCommand : public Command {
 public:
  SetNxCommand() : Command("setnx", "wmF") {}

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
    const std::string& val = sess->getArgs()[2];

    auto server = sess->getServerEntry();
    INVARIANT(server != nullptr);
    auto expdb = server->getSegmentMgr()->getDbWithKeyLock(
      sess, key, mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
      return expdb.status();
    }
    PStore kvstore = expdb.value().store;
    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);

    RecordKey rk(
      expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_KV, key, "");
    RecordValue rv(val, RecordType::RT_KV, pCtx->getVersionEP());

    bool checkKeyTypeForSet = server->checkKeyTypeForSet();
    for (int32_t i = 0; i < RETRY_CNT; ++i) {
      auto ptxn = sess->getCtx()->createTransaction(kvstore);
      if (!ptxn.ok()) {
        return ptxn.status();
      }
      auto result = setGeneric(sess,
                               kvstore,
                               ptxn.value(),
                               REDIS_SET_NX,
                               rk,
                               rv,
                               checkKeyTypeForSet,
                               true,
                               Command::fmtOne(),
                               Command::fmtZero());
      if (result.ok()) {
        return result.value();
      }
      if (result.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
        return result.status();
      }
      if (i == RETRY_CNT - 1) {
        return result.status();
      } else {
        continue;
      }
    }

    INVARIANT_D(0);
    return {ErrorCodes::ERR_INTERNAL, "not reachable"};
  }
} setnxCmd;

class SetNxExCommand : public Command {
 public:
  SetNxExCommand() : Command("setnxex", "wm") {}

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
    const std::string& key = sess->getArgs()[1];
    const std::string& val = sess->getArgs()[3];
    Expected<int64_t> eexpire = ::tendisplus::stoll(sess->getArgs()[2]);
    if (!eexpire.ok()) {
      return eexpire.status();
    }
    if (eexpire.value() <= 0) {
      return {ErrorCodes::ERR_PARSEPKT, "invalid expire time"};
    }
    auto ttl = msSinceEpoch() + eexpire.value() * 1000;

    auto server = sess->getServerEntry();
    INVARIANT(server != nullptr);
    auto expdb = server->getSegmentMgr()->getDbWithKeyLock(
      sess, key, mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
      return expdb.status();
    }
    PStore kvstore = expdb.value().store;
    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);

    RecordKey rk(
      expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_KV, key, "");
    RecordValue rv(val, RecordType::RT_KV, pCtx->getVersionEP(), ttl);

    bool checkKeyTypeForSet = server->checkKeyTypeForSet();
    for (int32_t i = 0; i < RETRY_CNT; ++i) {
      auto ptxn = sess->getCtx()->createTransaction(kvstore);
      if (!ptxn.ok()) {
        return ptxn.status();
      }
      auto result = setGeneric(sess,
                               kvstore,
                               ptxn.value(),
                               REDIS_SET_NX,
                               rk,
                               rv,
                               checkKeyTypeForSet,
                               true,
                               Command::fmtOne(),
                               Command::fmtZero());
      if (result.ok()) {
        return result.value();
      }
      if (result.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
        return result.status();
      }
      if (i == RETRY_CNT - 1) {
        return result.status();
      } else {
        continue;
      }
    }

    INVARIANT_D(0);
    return {ErrorCodes::ERR_INTERNAL, "not reachable"};
  }
} setnxexCmd;

class StrlenCommand : public Command {
 public:
  StrlenCommand() : Command("strlen", "rF") {}

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
    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    const std::string& key = sess->getArgs()[1];
    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_KV);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
      return Command::fmtZero();
    } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtZero();
    } else if (!rv.status().ok()) {
      return rv.status();
    } else {
      return Command::fmtLongLong(rv.value().getValue().size());
    }
  }
} strlenCmd;

class BitPosCommand : public Command {
 public:
  BitPosCommand() : Command("bitpos", "r") {}

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
    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    const std::vector<std::string>& args = sess->getArgs();
    const std::string& key = args[1];
    uint32_t bit = 0;
    bool endGiven = false;
    if (args[2] == "0") {
      bit = 0;
    } else if (args[2] == "1") {
      bit = 1;
    } else {
      return {ErrorCodes::ERR_PARSEOPT, "The bit argument must be 1 or 0."};
    }
    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_KV);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
        rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      /* If the key does not exist, from our point of view it is an
       * infinite array of 0 bits. If the user is looking for the fist
       * clear bit return 0, If the user is looking for the first set bit,
       * return -1. */
      if (bit) {
        return Command::fmtLongLong(-1);
      } else {
        return Command::fmtLongLong(0);
      }
    } else if (!rv.status().ok()) {
      return rv.status();
    }
    int64_t start = 0;
    const std::string& target = rv.value().getValue();
    int64_t end = target.size() - 1;
    if (args.size() == 4 || args.size() == 5) {
      Expected<int64_t> estart = ::tendisplus::stoll(args[3]);
      if (!estart.ok()) {
        return estart.status();
      }
      start = estart.value();
      if (args.size() == 5) {
        Expected<int64_t> eend = ::tendisplus::stoll(args[4]);
        if (!eend.ok()) {
          return eend.status();
        }
        end = eend.value();
        endGiven = true;
      }

      ssize_t len = rv.value().getValue().size();
      if (start < 0) {
        start = len + start;
      }
      if (end < 0) {
        end = len + end;
      }
      if (start < 0) {
        start = 0;
      }
      if (end < 0) {
        end = 0;
      }
      if (end >= len) {
        end = len - 1;
      }
    } else if (args.size() == 3) {
      // nothing
    } else {
      return {ErrorCodes::ERR_PARSEOPT, "syntax error"};
    }
    if (start > end) {
      return Command::fmtLongLong(-1);
    }
    int64_t result =
      redis_port::bitPos(target.c_str() + start, end - start + 1, bit);
    if (endGiven && bit == 0 && result == (end - start + 1) * 8) {
      return Command::fmtLongLong(-1);
    }
    if (result != -1) {
      result += start * 8;
    }
    return Command::fmtLongLong(result);
  }
} bitposCmd;

class BitCountCommand : public Command {
 public:
  BitCountCommand() : Command("bitcount", "r") {}

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
    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    const std::string& key = sess->getArgs()[1];
    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_KV);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
      return Command::fmtZero();
    } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtZero();
    } else if (!rv.status().ok()) {
      return rv.status();
    }
    int64_t start = 0;
    const std::string& target = rv.value().getValue();
    int64_t end = target.size() - 1;
    if (sess->getArgs().size() == 4) {
      Expected<int64_t> estart = ::tendisplus::stoll(sess->getArgs()[2]);
      Expected<int64_t> eend = ::tendisplus::stoll(sess->getArgs()[3]);
      if (!estart.ok() || !eend.ok()) {
        return estart.ok() ? eend.status() : estart.status();
      }
      start = estart.value();
      end = eend.value();
      if (start < 0 && end < 0 && start > end) {
        return Command::fmtZero();
      }
      ssize_t len = rv.value().getValue().size();
      if (start < 0) {
        start = len + start;
      }
      if (end < 0) {
        end = len + end;
      }
      if (start < 0) {
        start = 0;
      }
      if (end < 0) {
        end = 0;
      }
      if (end >= len) {
        end = len - 1;
      }
    } else if (sess->getArgs().size() == 2) {
      // nothing
    } else {
      return {ErrorCodes::ERR_PARSEOPT, "syntax error"};
    }
    if (start > end) {
      return Command::fmtZero();
    }
    return Command::fmtLongLong(
      redis_port::popCount(target.c_str() + start, end - start + 1));
  }
} bitcntCmd;

class GetGenericCmd : public Command {
 public:
  GetGenericCmd(const std::string& name, const char* sflags)
    : Command(name, sflags) {}

  virtual Expected<std::string> run(Session* sess) {
    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    const std::string& key = sess->getArgs()[1];
    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_KV);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
        rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return rv.status();
    } else if (!rv.status().ok()) {
      return rv.status();
    } else {
      return rv.value().getValue();
    }
  }
};

class GetVsnCommand : public Command {
 public:
  GetVsnCommand() : Command("getvsn", "rF") {}

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
    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    const std::string& key = sess->getArgs()[1];
    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_KV);

    std::stringstream ss;
    Command::fmtMultiBulkLen(ss, 2);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
        rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      Command::fmtLongLong(ss, -1);
      Command::fmtNull(ss);
      return ss.str();
    } else if (!rv.status().ok()) {
      return rv.status();
    } else {
      Command::fmtLongLong(ss, rv.value().getCas());
      Command::fmtBulk(ss, rv.value().getValue());
      return ss.str();
    }
  }
} getvsnCmd;

class GetCommand : public GetGenericCmd {
 public:
  GetCommand() : GetGenericCmd("get", "rF") {}

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
    auto v = GetGenericCmd::run(sess);
    if (v.status().code() == ErrorCodes::ERR_EXPIRED ||
        v.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtNull();
    }
    if (!v.ok()) {
      return v.status();
    }
    return Command::fmtBulk(v.value());
  }
} getCommand;

class GetRangeGenericCommand : public GetGenericCmd {
 public:
  GetRangeGenericCommand(const std::string& name, const char* sflags)
    : GetGenericCmd(name, sflags) {}

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
    Expected<int64_t> estart = ::tendisplus::stoll(sess->getArgs()[2]);
    if (!estart.ok()) {
      return estart.status();
    }
    int64_t start = estart.value();

    Expected<int64_t> eend = ::tendisplus::stoll(sess->getArgs()[3]);
    if (!eend.ok()) {
      return eend.status();
    }
    int64_t end = eend.value();

    auto v = GetGenericCmd::run(sess);
    if (v.status().code() == ErrorCodes::ERR_EXPIRED ||
        v.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtBulk("");
    } else if (!v.ok()) {
      return v.status();
    }
    std::string s = std::move(v.value());
    if (start < 0) {
      start = s.size() + start;
    }
    if (end < 0) {
      end = s.size() + end;
    }
    if (start < 0) {
      start = 0;
    }
    if (end < 0) {
      end = 0;
    }
    if (end >= static_cast<ssize_t>(s.size())) {
      end = s.size() - 1;
    }
    if (start > end || s.size() == 0) {
      return Command::fmtBulk("");
    }
    return Command::fmtBulk(s.substr(start, end - start + 1));
  }
};

class GetRangeCommand : public GetRangeGenericCommand {
 public:
  GetRangeCommand() : GetRangeGenericCommand("getrange", "r") {}
} getrangeCmd;

class Substrcommand : public GetRangeGenericCommand {
 public:
  Substrcommand() : GetRangeGenericCommand("substr", "r") {}
} substrCmd;

class GetSetGeneral : public Command {
 public:
  explicit GetSetGeneral(const std::string& name, const char* sflags)
    : Command(name, sflags) {}

  virtual bool replyNewValue() const {
    return true;
  }

  virtual Expected<RecordValue> newValueFromOld(
    Session* sess, const Expected<RecordValue>& oldValue) const = 0;

  Expected<RecordValue> runGeneral(Session* sess) {
    const std::string& key = sess->getArgs()[firstkey()];
    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);

    auto server = sess->getServerEntry();
    INVARIANT(server != nullptr);
    auto expdb = server->getSegmentMgr()->getDbWithKeyLock(
      sess, key, mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
      return expdb.status();
    }

    // expire if possible
    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_KV);
    if (rv.status().code() != ErrorCodes::ERR_OK &&
        rv.status().code() != ErrorCodes::ERR_EXPIRED &&
        rv.status().code() != ErrorCodes::ERR_NOTFOUND) {
      return rv.status();
    }

    PStore kvstore = expdb.value().store;
    RecordKey rk(
      expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_KV, key, "");

    for (int32_t i = 0; i < RETRY_CNT; ++i) {
      auto ptxn = sess->getCtx()->createTransaction(kvstore);
      if (!ptxn.ok()) {
        return ptxn.status();
      }
      const Expected<RecordValue>& newValue = newValueFromOld(sess, rv);
      if (newValue.status().code() == ErrorCodes::ERR_NOTFOUND) {
        return RecordValue(
          "", RecordType::RT_KV, sess->getCtx()->getVersionEP());
      }
      if (!newValue.ok()) {
        return newValue.status();
      }
      auto result = setGeneric(sess,
                               kvstore,
                               ptxn.value(),
                               REDIS_SET_NO_FLAGS,
                               rk,
                               newValue.value(),
                               false,  //  check_type has be done by
                                       //  Command::expireKeyIfNeeded()
                               true,
                               "",
                               "");
      if (result.ok()) {
        if (replyNewValue()) {
          return std::move(newValue.value());
        } else {
          return rv.ok() ? std::move(rv.value())
                         : RecordValue("",
                                       RecordType::RT_KV,
                                       sess->getCtx()->getVersionEP());
        }
      }
      if (result.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
        return result.status();
      }
      if (i == RETRY_CNT - 1) {
        return result.status();
      } else {
        continue;
      }
    }

    INVARIANT_D(0);
    return {ErrorCodes::ERR_INTERNAL, "not reachable"};
  }
};

class CasCommand : public GetSetGeneral {
 public:
  CasCommand() : GetSetGeneral("cas", "wm") {}

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

  Expected<RecordValue> newValueFromOld(
    Session* sess, const Expected<RecordValue>& oldValue) const {
    Expected<uint64_t> ecas = ::tendisplus::stoul(sess->getArgs()[2]);
    if (!ecas.ok()) {
      return ecas.status();
    }

    uint64_t ttl = 0;
    if (oldValue.ok()) {
      ttl = oldValue.value().getTtl();
    }
    RecordValue ret(sess->getArgs()[3],
                    RecordType::RT_KV,
                    sess->getCtx()->getVersionEP(),
                    ttl,
                    oldValue);
    if (!oldValue.ok()) {
      ret.setCas(ecas.value() + 1);
      return ret;
    }

    if ((int64_t)ecas.value() != oldValue.value().getCas() &&
        oldValue.value().getCas() != -1) {
      return {ErrorCodes::ERR_CAS, "cas unmatch"};
    }

    ret.setCas(ecas.value() + 1);
    return std::move(ret);
  }

  Expected<std::string> run(Session* sess) final {
    const Expected<RecordValue>& rv = runGeneral(sess);
    if (!rv.ok()) {
      return rv.status();
    }
    return Command::fmtOK();
  }
} casCommand;

class AppendCommand : public GetSetGeneral {
 public:
  AppendCommand() : GetSetGeneral("append", "wm") {}

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

  Expected<RecordValue> newValueFromOld(
    Session* sess, const Expected<RecordValue>& oldValue) const {
    const std::string& val = sess->getArgs()[2];
    std::string cat;
    if (!oldValue.ok()) {
      cat = val;
    } else {
      cat = oldValue.value().getValue();
      cat.insert(cat.end(), val.begin(), val.end());
    }

    uint64_t ttl = 0;
    RecordType type = RecordType::RT_KV;
    if (oldValue.ok()) {
      ttl = oldValue.value().getTtl();
      type = oldValue.value().getRecordType();
    }
    return std::move(RecordValue(
      std::move(cat), type, sess->getCtx()->getVersionEP(), ttl, oldValue));
  }

  Expected<std::string> run(Session* sess) final {
    const Expected<RecordValue>& rv = runGeneral(sess);
    if (!rv.ok()) {
      return rv.status();
    }
    return Command::fmtLongLong(rv.value().getValue().size());
  }
} appendCmd;

class SetRangeCommand : public GetSetGeneral {
 public:
  SetRangeCommand() : GetSetGeneral("setrange", "wm") {}

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

  Expected<RecordValue> newValueFromOld(
    Session* sess, const Expected<RecordValue>& oldValue) const {
    const std::string& val = sess->getArgs()[3];
    if (oldValue.status().code() == ErrorCodes::ERR_NOTFOUND ||
        oldValue.status().code() == ErrorCodes::ERR_EXPIRED) {
      if (val.size() == 0) {
        return {ErrorCodes::ERR_NOTFOUND, ""};
      }
    }
    Expected<int64_t> eoffset = ::tendisplus::stoll(sess->getArgs()[2]);
    if (!eoffset.ok()) {
      return eoffset.status();
    }
    if (eoffset.value() < 0) {
      return {ErrorCodes::ERR_PARSEOPT, "offset is out of range"};
    }
    uint32_t offset = eoffset.value();
    if (offset + val.size() > 512 * 1024 * 1024) {
      return {ErrorCodes::ERR_PARSEOPT,
              "string exceeds maximum allowed size (512MB)"};
    }
    std::string cat;
    if (oldValue.ok()) {
      cat = oldValue.value().getValue();
    }
    if (offset + val.size() > cat.size()) {
      cat.resize(offset + val.size(), 0);
    }
    for (size_t i = offset; i < offset + val.size(); ++i) {
      cat[i] = val[i - offset];
    }
    uint64_t ttl = 0;
    RecordType type = RecordType::RT_KV;
    if (oldValue.ok()) {
      ttl = oldValue.value().getTtl();
      type = oldValue.value().getRecordType();
    }
    return RecordValue(
      std::move(cat), type, sess->getCtx()->getVersionEP(), ttl, oldValue);
  }

  Expected<std::string> run(Session* sess) final {
    const Expected<RecordValue>& rv = runGeneral(sess);
    if (!rv.ok()) {
      return rv.status();
    }
    return Command::fmtLongLong(rv.value().getValue().size());
  }
} setrangeCmd;

class SetBitCommand : public GetSetGeneral {
 public:
  SetBitCommand() : GetSetGeneral("setbit", "wm") {}

  bool replyNewValue() const final {
    return false;
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

  Expected<RecordValue> newValueFromOld(
    Session* sess, const Expected<RecordValue>& oldValue) const {
    Expected<uint64_t> epos = ::tendisplus::stoul(sess->getArgs()[2]);
    if (!epos.ok()) {
      return epos.status();
    }
    uint64_t pos = epos.value();
    int on = 0;
    std::string tomodify;
    if (oldValue.ok()) {
      tomodify = oldValue.value().getValue();
    }
    if ((pos >> 3) >= (512 * 1024 * 1024)) {
      return {ErrorCodes::ERR_PARSEOPT,
              "bit offset is not an integer or out of range"};
    }
    if ((pos >> 3) > 4 * 1024 * 1024) {
      LOG(WARNING) << "meet large bitpos:" << pos;
    }
    if (sess->getArgs()[3] == "1") {
      on = 1;
    } else if (sess->getArgs()[3] == "0") {
      on = 0;
    } else {
      return {ErrorCodes::ERR_PARSEOPT,
              "bit is not an integer or out of range"};
    }

    uint64_t byte = (pos >> 3);
    if (tomodify.size() < byte + 1) {
      tomodify.resize(byte + 1, 0);
    }
    uint8_t byteval = static_cast<uint8_t>(tomodify[byte]);
    uint8_t bit = 7 - (pos & 0x7);
    byteval &= ~(1 << bit);
    byteval |= ((on & 0x1) << bit);
    tomodify[byte] = byteval;

    // incrby wont clear ttl
    uint64_t ttl = 0;
    RecordType type = RecordType::RT_KV;
    if (oldValue.ok()) {
      ttl = oldValue.value().getTtl();
      type = oldValue.value().getRecordType();
    }
    return RecordValue(
      std::move(tomodify), type, sess->getCtx()->getVersionEP(), ttl, oldValue);
  }

  Expected<std::string> run(Session* sess) final {
    const Expected<RecordValue>& rv = runGeneral(sess);
    if (!rv.ok()) {
      return rv.status();
    }

    Expected<uint64_t> epos = ::tendisplus::stoul(sess->getArgs()[2]);
    if (!epos.ok()) {
      return epos.status();
    }
    uint64_t pos = epos.value();
    std::string toreturn = rv.value().getValue();

    uint64_t byte = (pos >> 3);
    if (toreturn.size() < byte + 1) {
      toreturn.resize(byte + 1, 0);
    }
    uint8_t byteval = static_cast<uint8_t>(toreturn[byte]);
    uint8_t bit = 7 - (pos & 0x7);
    uint8_t bitval = byteval & (1 << bit);
    return bitval ? Command::fmtOne() : Command::fmtZero();
  }
} setbitCmd;

class GetSetCommand : public GetSetGeneral {
 public:
  GetSetCommand() : GetSetGeneral("getset", "wm") {}

  bool replyNewValue() const final {
    return false;
  }

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

  Expected<RecordValue> newValueFromOld(
    Session* sess, const Expected<RecordValue>& oldValue) const {
    // getset overwrites ttl
    return RecordValue(sess->getArgs()[2],
                       RecordType::RT_KV,
                       sess->getCtx()->getVersionEP(),
                       0,
                       oldValue);
  }

  Expected<std::string> run(Session* sess) final {
    const Expected<RecordValue>& rv = runGeneral(sess);
    if (!rv.ok()) {
      return rv.status();
    }
    const std::string& v = rv.value().getValue();
    if (v.size()) {
      return Command::fmtBulk(v);
    }
    return Command::fmtNull();
  }
} getsetCmd;

class IncrDecrGeneral : public GetSetGeneral {
 public:
  explicit IncrDecrGeneral(const std::string& name, const char* sflags)
    : GetSetGeneral(name, sflags) {}

  Expected<int64_t> sumIncr(const Expected<RecordValue>& esum,
                            int64_t incr) const {
    int64_t sum = 0;
    if (esum.ok()) {
      Expected<int64_t> val = ::tendisplus::stoll(esum.value().getValue());
      if (!val.ok()) {
        return {ErrorCodes::ERR_DECODE,
                "value is not an integer or out of range"};
      }
      sum = val.value();
    }

    if ((incr < 0 && sum < 0 && incr < (LLONG_MIN - sum)) ||
        (incr > 0 && sum > 0 && incr > (LLONG_MAX - sum))) {
      return {ErrorCodes::ERR_OVERFLOW,
              "increment or decrement would overflow"};
    }
    sum += incr;
    return sum;
  }

  virtual Expected<std::string> run(Session* sess) {
    const Expected<RecordValue>& rv = runGeneral(sess);
    if (!rv.ok()) {
      return rv.status();
    }
    Expected<int64_t> val = ::tendisplus::stoll(rv.value().getValue());
    if (!val.ok()) {
      return val.status();
    }
    return Command::fmtLongLong(val.value());
  }
};

class IncrbyfloatCommand : public GetSetGeneral {
 public:
  IncrbyfloatCommand() : GetSetGeneral("incrbyfloat", "wmF") {}

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

  Expected<long double> sumIncr(const Expected<RecordValue>& esum,
                                long double incr) const {
    long double sum = 0;
    if (esum.ok()) {
      Expected<long double> val = ::tendisplus::stold(esum.value().getValue());
      if (!val.ok()) {
        return {ErrorCodes::ERR_DECODE, "value is not a valid float"};
      }
      sum = val.value();
    }

    sum += incr;
    return sum;
  }

  virtual Expected<std::string> run(Session* sess) {
    const Expected<RecordValue>& rv = runGeneral(sess);
    if (!rv.ok()) {
      return rv.status();
    }
    Expected<long double> val = ::tendisplus::stold(rv.value().getValue());
    if (!val.ok()) {
      return val.status();
    }
    return Command::fmtBulk(::tendisplus::ldtos(val.value(), true));
  }

  Expected<RecordValue> newValueFromOld(
    Session* sess, const Expected<RecordValue>& oldValue) const {
    const std::string& val = sess->getArgs()[2];
    Expected<long double> eInc = ::tendisplus::stold(val);
    if (!eInc.ok()) {
      return eInc.status();
    }
    if (std::isnan(eInc.value()) || std::isinf(eInc.value())) {
      return {ErrorCodes::ERR_NAN, "increment would produce NaN or Infinity"};
    }
    Expected<long double> newSum = sumIncr(oldValue, eInc.value());
    if (!newSum.ok()) {
      return newSum.status();
    }

    // incrby wont clear ttl
    uint64_t ttl = 0;
    RecordType type = RecordType::RT_KV;
    if (oldValue.ok()) {
      ttl = oldValue.value().getTtl();
      type = oldValue.value().getRecordType();
    }
    return RecordValue(::tendisplus::ldtos(newSum.value(), true),
                       type,
                       sess->getCtx()->getVersionEP(),
                       ttl,
                       oldValue);
  }
} incrbyfloatCmd;

class IncrbyCommand : public IncrDecrGeneral {
 public:
  IncrbyCommand() : IncrDecrGeneral("incrby", "wmF") {}

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

  Expected<RecordValue> newValueFromOld(
    Session* sess, const Expected<RecordValue>& oldValue) const {
    const std::string& val = sess->getArgs()[2];
    Expected<int64_t> eInc = ::tendisplus::stoll(val);
    if (!eInc.ok()) {
      return eInc.status();
    }
    Expected<int64_t> newSum = sumIncr(oldValue, eInc.value());
    if (!newSum.ok()) {
      return newSum.status();
    }

    // incrby wont clear ttl
    uint64_t ttl = 0;
    RecordType type = RecordType::RT_KV;
    if (oldValue.ok()) {
      ttl = oldValue.value().getTtl();
      type = oldValue.value().getRecordType();
    }
    return RecordValue(std::to_string(newSum.value()),
                       type,
                       sess->getCtx()->getVersionEP(),
                       ttl,
                       oldValue);
  }
} incrbyCmd;

class IncrexCommand : public IncrDecrGeneral {
 public:
  IncrexCommand() : IncrDecrGeneral("increx", "wmF") {}

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

  Expected<RecordValue> newValueFromOld(
    Session* sess, const Expected<RecordValue>& oldValue) const {
    const std::string& val = sess->getArgs()[2];
    Expected<int64_t> ettl = ::tendisplus::stoll(val);
    if (!ettl.ok()) {
      return ettl.status();
    }
    Expected<int64_t> newSum = sumIncr(oldValue, 1);
    if (!newSum.ok()) {
      return newSum.status();
    }

    uint64_t ttl = ettl.value() * 1000 + msSinceEpoch();
    RecordType type = RecordType::RT_KV;
    if (oldValue.ok()) {
      type = oldValue.value().getRecordType();
      if (oldValue.value().getTtl() != 0) {
        ttl = oldValue.value().getTtl();
      }
    }
    return RecordValue(std::to_string(newSum.value()),
                       type,
                       sess->getCtx()->getVersionEP(),
                       ttl,
                       oldValue);
  }
} increxCmd;


class IncrCommand : public IncrDecrGeneral {
 public:
  IncrCommand() : IncrDecrGeneral("incr", "wmF") {}
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

  Expected<RecordValue> newValueFromOld(
    Session* sess, const Expected<RecordValue>& oldValue) const {
    Expected<int64_t> newSum = sumIncr(oldValue, 1);
    if (!newSum.ok()) {
      return newSum.status();
    }

    // incrby wont clear ttl
    uint64_t ttl = 0;
    RecordType type = RecordType::RT_KV;
    if (oldValue.ok()) {
      ttl = oldValue.value().getTtl();
      type = oldValue.value().getRecordType();
    }
    return RecordValue(std::to_string(newSum.value()),
                       type,
                       sess->getCtx()->getVersionEP(),
                       ttl,
                       oldValue);
  }
} incrCmd;

class DecrbyCommand : public IncrDecrGeneral {
 public:
  DecrbyCommand() : IncrDecrGeneral("decrby", "wmF") {}

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

  Expected<RecordValue> newValueFromOld(
    Session* sess, const Expected<RecordValue>& oldValue) const {
    const std::string& val = sess->getArgs()[2];
    Expected<int64_t> eInc = ::tendisplus::stoll(val);
    if (!eInc.ok()) {
      return eInc.status();
    }
    Expected<int64_t> newSum = sumIncr(oldValue, -eInc.value());
    if (!newSum.ok()) {
      return newSum.status();
    }

    // incrby wont clear ttl
    uint64_t ttl = 0;
    RecordType type = RecordType::RT_KV;
    if (oldValue.ok()) {
      ttl = oldValue.value().getTtl();
      type = oldValue.value().getRecordType();
    }
    // LOG(INFO) << "decr new val:" << newSum.value() << ' ' << val;
    return RecordValue(std::to_string(newSum.value()),
                       type,
                       sess->getCtx()->getVersionEP(),
                       ttl,
                       oldValue);
  }
} decrbyCmd;

class DecrCommand : public IncrDecrGeneral {
 public:
  DecrCommand() : IncrDecrGeneral("decr", "wmF") {}

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

  Expected<RecordValue> newValueFromOld(
    Session* sess, const Expected<RecordValue>& oldValue) const {
    Expected<int64_t> newSum = sumIncr(oldValue, -1);
    if (!newSum.ok()) {
      return newSum.status();
    }

    // incrby wont clear ttl
    uint64_t ttl = 0;
    RecordType type = RecordType::RT_KV;
    if (oldValue.ok()) {
      ttl = oldValue.value().getTtl();
      type = oldValue.value().getRecordType();
    }
    return RecordValue(std::to_string(newSum.value()),
                       type,
                       sess->getCtx()->getVersionEP(),
                       ttl,
                       oldValue);
  }
} decrCmd;

class MGetCommand : public Command {
 public:
  MGetCommand() : Command("mget", "rF") {}

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
    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    auto server = sess->getServerEntry();
    INVARIANT(server != nullptr);
    const auto& args = sess->getArgs();

    auto index = getKeysFromCommand(args);
    auto locklist = server->getSegmentMgr()->getAllKeysLocked(
      sess, args, index, Command::RdLock());
    if (!locklist.ok()) {
      return locklist.status();
    }

    std::stringstream ss;
    Command::fmtMultiBulkLen(ss, sess->getArgs().size() - 1);
    for (size_t i = 1; i < sess->getArgs().size(); ++i) {
      const std::string& key = sess->getArgs()[i];
      Expected<RecordValue> rv =
        Command::expireKeyIfNeeded(sess, key, RecordType::RT_KV);
      if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
          rv.status().code() == ErrorCodes::ERR_NOTFOUND ||
          rv.status().code() == ErrorCodes::ERR_WRONG_TYPE) {
        Command::fmtNull(ss);
        continue;
      } else if (!rv.status().ok()) {
        return rv.status();
      }
      Command::fmtBulk(ss, rv.value().getValue());
    }
    return ss.str();
  }
} mgetCmd;

class BitopCommand : public Command {
 public:
  BitopCommand() : Command("bitop", "wm") {}

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
      return {
        ErrorCodes::ERR_PARSEPKT,
        "BITOP NOT must be called with a single source key."};  // NOLINT(whitespace/line_length)
    }

    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    auto server = sess->getServerEntry();
    INVARIANT(server != nullptr);

    auto index = getKeysFromCommand(args);
    auto locklist = server->getSegmentMgr()->getAllKeysLocked(
      sess, args, index, mgl::LockMode::LOCK_X);
    if (!locklist.ok()) {
      return locklist.status();
    }

    size_t numKeys = args.size() - 3;
    size_t maxLen = 0;
    std::vector<std::string> vals;
    for (size_t j = 0; j < numKeys; ++j) {
      Expected<RecordValue> rv =
        Command::expireKeyIfNeeded(sess, args[j + 3], RecordType::RT_KV);
      if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
        vals.push_back("");
      } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
        vals.push_back("");
      } else if (!rv.status().ok()) {
        return rv.status();
      } else {
        vals.push_back(rv.value().getValue());
        if (vals[j].size() > maxLen) {
          maxLen = vals[j].size();
        }
      }
    }
    if (maxLen == 0) {
      auto expdb = server->getSegmentMgr()->getDbWithKeyLock(
              sess, targetKey, mgl::LockMode::LOCK_X);
      if (!expdb.ok()) {
        return expdb.status();
      }
      PStore kvstore = expdb.value().store;

      auto ptxn = sess->getCtx()->createTransaction(kvstore);
      if (!ptxn.ok()) {
        return ptxn.status();
      }
      Command::delKeyChkExpire(sess, targetKey, RecordType::RT_KV,
        ptxn.value());
      auto eCmt = sess->getCtx()->commitTransaction(ptxn.value());
      if (!eCmt.ok()) {
        return eCmt.status();
      }
      return Command::fmtZero();
    }
    std::string result(maxLen, 0);
    for (size_t i = 0; i < maxLen; ++i) {
      unsigned char output = (vals[0].size() <= i) ? 0 : vals[0][i];
      if (op == Op::BITOP_NOT)
        output = ~output;
      for (size_t j = 1; j < numKeys; ++j) {
        unsigned char byte = (vals[j].size() <= i) ? 0 : vals[j][i];
        switch (op) {
          case Op::BITOP_AND:
            output &= byte;
            break;
          case Op::BITOP_OR:
            output |= byte;
            break;
          case Op::BITOP_XOR:
            output ^= byte;
            break;
          default:
            INVARIANT_D(0);
        }
      }
      result[i] = output;
    }


    auto expdb = server->getSegmentMgr()->getDbWithKeyLock(
            sess, targetKey, mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
      return expdb.status();
    }
    PStore kvstore = expdb.value().store;

    RecordKey rk(
      expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_KV, targetKey, "");
    RecordValue rv(result, RecordType::RT_KV, pCtx->getVersionEP());
    bool checkKeyTypeForSet = server->checkKeyTypeForSet();
    for (int32_t i = 0; i < RETRY_CNT; ++i) {
      auto ptxn = sess->getCtx()->createTransaction(kvstore);
      if (!ptxn.ok()) {
        return ptxn.status();
      }
      auto setRes = setGeneric(sess,
                               kvstore,
                               ptxn.value(),
                               REDIS_SET_NO_FLAGS,
                               rk,
                               rv,
                               checkKeyTypeForSet,
                               true,
                               "",
                               "");
      if (setRes.ok()) {
        return Command::fmtLongLong(result.size());
      }
      if (setRes.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
        return setRes.status();
      }
      if (i == RETRY_CNT - 1) {
        return setRes.status();
      } else {
        continue;
      }
    }
    INVARIANT_D(0);
    return {ErrorCodes::ERR_INTERNAL, "not reachable"};
  }
} bitopCmd;

class MSetGenericCommand : public Command {
 public:
  MSetGenericCommand(const std::string& name, const char* sflags, int flags)
    : Command(name, sflags), _myflags(flags) {}

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
    return 2;
  }

  Expected<std::string> run(Session* sess) final {
    auto& args = sess->getArgs();
    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);

    if (args.size() % 2 == 0) {
      return {ErrorCodes::ERR_PARSEPKT, "wrong number of arguments for MSET"};
    }

    auto server = sess->getServerEntry();
    auto index = getKeysFromCommand(args);

    auto locklist = server->getSegmentMgr()->getAllKeysLocked(
      sess, args, index, mgl::LockMode::LOCK_X);
    if (!locklist.ok()) {
      return locklist.status();
    }

    bool checkKeyTypeForSet = server->checkKeyTypeForSet();
    // NOTE(vinchen): commit or rollback in one time
    bool failed = false;

    for (size_t i = 1; i < sess->getArgs().size(); i += 2) {
      const std::string& key = sess->getArgs()[i];
      const std::string& val = sess->getArgs()[i + 1];
      INVARIANT(server != nullptr);
      auto expdb = server->getSegmentMgr()->getDbHasLocked(sess, key);
      if (!expdb.ok()) {
        return expdb.status();
      }
      PStore kvstore = expdb.value().store;

      RecordKey rk(
        expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_KV, key, "");
      RecordValue rv(val, RecordType::RT_KV, pCtx->getVersionEP());
      for (int32_t i = 0; i < RETRY_CNT; ++i) {
        auto etxn = pCtx->createTransaction(kvstore);
        if (!etxn.ok()) {
          failed = true;
          break;
        }

        // NOTE(vinchen): commit one by one is not corect
        auto result = setGeneric(sess,
                                 kvstore,
                                 etxn.value(),
                                 _myflags,
                                 rk,
                                 rv,
                                 checkKeyTypeForSet,
                                 false,
                                 "ok",
                                 "abort");
        if (result.ok()) {
          if (result.value() == "ok") {
            break;
          } else {
            failed = true;
            break;
          }
        } else if (result.status().code() !=
                   ErrorCodes::ERR_COMMIT_RETRY) {  // NOLINT
          failed = true;
          break;
        } else {
          if (i == RETRY_CNT - 1) {
            failed = true;
            break;
          } else {
            continue;
          }
        }
      }

      if (failed) {
        break;
      }
    }
    if (!failed) {
      auto s = pCtx->commitAll("mset(nx)");
      if (!s.ok()) {
        failed = true;
      }
    } else {
      pCtx->rollbackAll();
    }
    if (_myflags == REDIS_SET_NO_FLAGS) {
      // mset
      return Command::fmtOK();
    } else if (_myflags == REDIS_SET_NX) {
      // msetnx
      return failed ? Command::fmtZero() : Command::fmtOne();
    }
    INVARIANT_D(0);
    return Command::fmtOK();
  }

 private:
  int _myflags;
};

class MSetCommand : public MSetGenericCommand {
 public:
  MSetCommand() : MSetGenericCommand("mset", "wm", REDIS_SET_NO_FLAGS) {}
} msetCmd;

class MSetNXCommand : public MSetGenericCommand {
 public:
  MSetNXCommand() : MSetGenericCommand("msetnx", "wm", REDIS_SET_NX) {}
} msetNxCmd;

class RenameGenericCommand : public Command {
 public:
  RenameGenericCommand(const std::string& name, const char* sflags, bool nx)
    : Command(name, sflags), _flagnx(nx) {}

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
    auto& args = sess->getArgs();
    const std::string& src = args[1];
    const std::string& dst = args[2];
    bool samekey(false);
    if (src == dst) {
      samekey = true;
    }

    auto server = sess->getServerEntry();
    auto pCtx = sess->getCtx();
    std::vector<int32_t> keyidx = getKeysFromCommand(args);
    auto locklist = server->getSegmentMgr()->getAllKeysLocked(
      sess, args, keyidx, mgl::LockMode::LOCK_X);
    if (!locklist.ok()) {
      return locklist.status();
    }

    auto srcdb = server->getSegmentMgr()->getDbHasLocked(sess, src);
    RET_IF_ERR_EXPECTED(srcdb);
    auto dstdb = server->getSegmentMgr()->getDbHasLocked(sess, dst);
    RET_IF_ERR_EXPECTED(dstdb);
    PStore dststore = dstdb.value().store;
    auto dptxn = pCtx->createTransaction(dststore);
    if (!dptxn.ok()) {
      return dptxn.status();
    }
    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, src, RecordType::RT_DATA_META);
    if (rv.status().code() == ErrorCodes::ERR_NOTFOUND ||
        rv.status().code() == ErrorCodes::ERR_EXPIRED) {
      return {ErrorCodes::ERR_NO_KEY, ""};
    }

    if (samekey) {
      return _flagnx ? Command::fmtZero() : Command::fmtOK();
    }

    Expected<RecordValue> dstrv =
      Command::expireKeyIfNeeded(sess, dst, RecordType::RT_DATA_META);
    if (dstrv.status().code() != ErrorCodes::ERR_NOTFOUND &&
        dstrv.status().code() != ErrorCodes::ERR_EXPIRED) {
      if (!dstrv.ok()) {
        return dstrv.status();
      }
      if (_flagnx) {
        return Command::fmtZero();
      }

      Status deleted = Command::delKey(sess, dst, RecordType::RT_DATA_META,
        dptxn.value());
      if (!deleted.ok()) {
        return deleted.toString();
      }
    }

    RecordKey rk(srcdb.value().chunkId,
                 pCtx->getDbId(),
                 RecordType::RT_DATA_META,
                 src,
                 "");
    RecordKey dstRk(dstdb.value().chunkId,
                    pCtx->getDbId(),
                    RecordType::RT_DATA_META,
                    dst,
                    "");
    PStore srcstore = srcdb.value().store;
    auto sptxn = pCtx->createTransaction(srcstore);
    if (!sptxn.ok()) {
      return sptxn.status();
    }
    bool rollback = true;
    const auto guard = MakeGuard([&rollback, &pCtx] {
      if (rollback) {
        pCtx->rollbackAll();
      }
    });

    // del old meta k/v
    Status s = Command::delKeyAndTTL(sess, rk, rv.value(), sptxn.value());
    if (!s.ok()) {
      return s;
    }

    // set new meta k/v
    s = dststore->setKV(dstRk, rv.value(), dptxn.value());
    if (!s.ok()) {
      return s;
    }
    if (rv.value().getRecordType() != RecordType::RT_KV &&
        rv.value().getTtl() > 0) {
      TTLIndex ictx(dst,
                    rv.value().getRecordType(),
                    sess->getCtx()->getDbId(),
                    rv.value().getTtl());
      if (ictx.getType() != RecordType::RT_KV) {
        s = dptxn.value()->setKV(
          ictx.encode(), RecordValue(RecordType::RT_TTL_INDEX).encode());
        if (!s.ok()) {
          return s;
        }
      }
    }

    if (rv.value().getRecordType() == RecordType::RT_KV) {
      pCtx->commitAll("rename");
      rollback = false;
      return _flagnx ? Command::fmtOne() : Command::fmtOK();
    }

    auto cnt = rcd_util::getSubKeyCount(rk, rv.value());
    if (!cnt.ok()) {
      return cnt.status();
    }

    std::vector<std::string> prefixes =
      getEleType(rk, rv.value().getRecordType());
    std::vector<Record> pending;
    pending.reserve(cnt.value());
    for (const auto& prefix : prefixes) {
      auto cursor = sptxn.value()->createDataCursor();
      cursor->seek(prefix);

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
        if (rcdKey.prefixPk() != prefix) {
          break;
        }
        pending.emplace_back(std::move(expRcd.value()));
      }
    }
    // add new
    for (auto& ele : pending) {
      const RecordKey& srcRk = ele.getRecordKey();
      RecordKey rk(dstRk.getChunkId(),
                   dstRk.getDbId(),
                   srcRk.getRecordType(),
                   dst,
                   srcRk.getSecondaryKey());
      const RecordValue& rv = ele.getRecordValue();
      Status s = dststore->setKV(rk, rv, dptxn.value());
      if (!s.ok()) {
        return s;
      }
    }

    // delete
    for (auto& ele : pending) {
      Status s = srcstore->delKV(ele.getRecordKey(), sptxn.value());
      if (!s.ok()) {
        return s;
      }
    }

    pCtx->commitAll("rename");
    rollback = false;

    return _flagnx ? Command::fmtOne() : Command::fmtOK();
  }

 private:
  bool _flagnx;
  std::vector<std::string> getEleType(const RecordKey& rk,
                                      const RecordType& type) {
    std::vector<std::string> ret;
    if (type == RecordType::RT_HASH_META) {
      RecordKey fakeRk(rk.getChunkId(),
                       rk.getDbId(),
                       RecordType::RT_HASH_ELE,
                       rk.getPrimaryKey(),
                       "");
      ret.push_back(fakeRk.prefixPk());
    } else if (type == RecordType::RT_LIST_META) {
      RecordKey fakeRk(rk.getChunkId(),
                       rk.getDbId(),
                       RecordType::RT_LIST_ELE,
                       rk.getPrimaryKey(),
                       "");
      ret.push_back(fakeRk.prefixPk());
    } else if (type == RecordType::RT_SET_META) {
      RecordKey fakeRk(rk.getChunkId(),
                       rk.getDbId(),
                       RecordType::RT_SET_ELE,
                       rk.getPrimaryKey(),
                       "");
      ret.push_back(fakeRk.prefixPk());
    } else if (type == RecordType::RT_ZSET_META) {
      RecordKey fakeRk(rk.getChunkId(),
                       rk.getDbId(),
                       RecordType::RT_ZSET_S_ELE,
                       rk.getPrimaryKey(),
                       "");
      ret.push_back(fakeRk.prefixPk());
      RecordKey fakeRk2(rk.getChunkId(),
                        rk.getDbId(),
                        RecordType::RT_ZSET_H_ELE,
                        rk.getPrimaryKey(),
                        "");
      ret.push_back(fakeRk2.prefixPk());
    }
    return ret;
  }
};

class RenameCommand : public RenameGenericCommand {
 public:
  RenameCommand() : RenameGenericCommand("rename", "w", false) {}
} renameCmd;

class RenamenxCommand : public RenameGenericCommand {
 public:
  RenamenxCommand() : RenameGenericCommand("renamenx", "wF", true) {}
} renamenxCmd;

class GetBitCommand : public GetGenericCmd {
 public:
  GetBitCommand() : GetGenericCmd("getbit", "rF") {}

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
    Expected<uint64_t> epos = ::tendisplus::stoul(sess->getArgs()[2]);
    if (!epos.ok()) {
      return epos.status();
    }
    auto pos = epos.value();
    if ((pos >> 3) >= (512 * 1024 * 1024)) {
      return {ErrorCodes::ERR_PARSEOPT,
              "bit offset is not an integer or out of range"};
    }
    auto v = GetGenericCmd::run(sess);
    if (v.status().code() == ErrorCodes::ERR_EXPIRED ||
        v.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return Command::fmtZero();
    }
    if (!v.ok()) {
      return v.status();
    }

    std::string bitValue = v.value();
    size_t byte, bit;
    uint8_t bitval = 0;

    byte = pos >> 3;
    bit = 7 - (pos & 0x7);
    if (byte > bitValue.size()) {
      return Command::fmtZero();
    }
    bitval = static_cast<uint8_t>(bitValue[byte]) & (1 << bit);
    return bitval ? Command::fmtOne() : Command::fmtZero();
  }
} getbitCommand;

class BitFieldCommand : public Command {
 public:
  BitFieldCommand() : Command("bitfield", "wm") {}

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

  uint64_t getUnsignedBitfield(const std::string& value,
                               uint64_t offset,
                               uint64_t bits) {
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

  int64_t getSignedBitfield(const std::string& value,
                            uint64_t offset,
                            uint64_t bits) {
    int64_t ret;
    union {
      uint64_t u;
      int64_t i;
    } conv;

    conv.u = getUnsignedBitfield(value, offset, bits);
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

  void setUnsignedBitfield(std::string* buf,
                           uint64_t offset,
                           uint64_t bits,
                           uint64_t value) {
    for (size_t i = 0; i < bits; i++) {
      uint64_t bitval = (value & ((uint64_t)1 << (bits - 1 - i))) != 0;
      uint64_t byte = offset >> 3;
      uint64_t bit = 7 - (offset & 0x7);
      uint8_t byteval = static_cast<uint8_t>((*buf)[byte]);
      byteval &= ~(1 << bit);
      byteval |= bitval << bit;
      (*buf)[byte] = byteval & 0xff;
      offset++;
    }
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
    auto& args = sess->getArgs();
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
                "Invalid bitfield type. Use something like i16 u8. "
                "Note that u64 is not supported buf i64 is."};
      }
      int32_t bits = eBits.value();

      ++i;
      int usehash(0);
      if (args[i].c_str()[0] == '#')
        usehash = 1;
      const std::string offsetval(args[i].c_str() + usehash);
      Expected<int64_t> eOffset = tendisplus::stoll(offsetval);
      if (!eOffset.ok()) {
        return {ErrorCodes::ERR_PARSEPKT,
                "bit offset is not an integer or out of range"};
      }
      int64_t offset = usehash ? eOffset.value() * bits : eOffset.value();
      if (offset < 0 || (offset >> 3) >= (512 * 1024 * 1024)) {
        return {ErrorCodes::ERR_PARSEPKT,
                "bit offset is not an integer or out of range"};
      }

      ++i;
      if (opcode != FieldOpType::BITFIELDOP_GET) {
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

    std::string value;
    RecordValue rv(RecordType::RT_KV);
    const std::string& key = args[1];
    auto server = sess->getServerEntry();
    auto pCtx = sess->getCtx();
    auto expdb = server->getSegmentMgr()->getDbWithKeyLock(
      sess, key, mgl::LockMode::LOCK_X);
    Expected<RecordValue> eRv =
      Command::expireKeyIfNeeded(sess, key, RecordType::RT_KV);
    if (eRv.status().code() == ErrorCodes::ERR_EXPIRED ||
        eRv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      if (readonly)
        return Command::fmtZeroBulkLen();
    } else if (!eRv.ok()) {
      return eRv.status();
    } else {
      rv = std::move(eRv.value());
      value = rv.getValue();
    }

    if (!readonly) {
      uint64_t maxbyte = highestOffset >> 3;
      if (value.size() < maxbyte + 1)
        value.resize(maxbyte + 1);
    }
    bool changes(false);

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
          oldval = getSignedBitfield(value, op.offset, op.bits);

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
            setUnsignedBitfield(&value, op.offset, op.bits, uv);
          } else {
            Command::fmtNull(ss);
          }
        } else {
          uint64_t oldval, newval, retval, wrapped(0);
          int overflow(0);
          oldval = getUnsignedBitfield(value, op.offset, op.bits);

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
            setUnsignedBitfield(&value, op.offset, op.bits, newval);
          } else {
            Command::fmtNull(ss);
          }
        }  // end of usnigned SET/INCR
        changes = true;
      } /*end of SET/INCR*/ else {
        std::vector<unsigned char> buf(9, 0);
        size_t byte = op.offset >> 3;
        for (int i = 0; i < 9; i++) {
          if (i + byte >= value.size())
            break;
          buf[i] = value[i + byte];
        }

        if (op.sign) {
          int64_t val = getSignedBitfield(
            std::string(buf.begin(), buf.end()), op.offset, op.bits);
          Command::fmtLongLong(ss, val);
        } else {
          uint64_t val = getUnsignedBitfield(
            std::string(buf.begin(), buf.end()), op.offset, op.bits);
          Command::fmtLongLong(ss, val);
        }
      }
    }  // end of ops' loop
    if (changes) {
      RecordKey rk(
        expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_KV, key, "");
      RecordValue newrv(
        value, RecordType::RT_KV, pCtx->getVersionEP(), rv.getTtl(), rv);
      PStore kvstore = expdb.value().store;
      auto ptxn = sess->getCtx()->createTransaction(kvstore);
      if (!ptxn.ok()) {
        return ptxn.status();
      }
      Status s = kvstore->setKV(rk, newrv, ptxn.value());
      if (!s.ok()) {
        return s;
      }
      auto eCmt = sess->getCtx()->commitTransaction(ptxn.value());
      if (!eCmt.ok()) {
        return eCmt.status();
      }
    }

    return ss.str();
  }
} bitfieldCommand;

}  // namespace tendisplus
