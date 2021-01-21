// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <string>
#include <utility>
#include <memory>
#include <algorithm>
#include <cctype>
#include <clocale>
#include "glog/logging.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/time.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

// return false if not exists
// return true if exists and del ok
// return error on error
Expected<bool> delGeneric(Session* sess, const std::string& key,
  Transaction* txn) {
  SessionCtx* pCtx = sess->getCtx();
  INVARIANT(pCtx != nullptr);
  bool atLeastOne = false;
  Expected<bool> done =
    Command::delKeyChkExpire(sess, key, RecordType::RT_DATA_META, txn);
  if (!done.ok()) {
    return done.status();
  }
  atLeastOne |= done.value();
  return atLeastOne;
}

class DelCommand : public Command {
 public:
  DelCommand() : Command("del", "w") {}

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
    const auto& args = sess->getArgs();

    auto index = getKeysFromCommand(args);
    auto locklist = sess->getServerEntry()->getSegmentMgr()->getAllKeysLocked(
      sess, args, index, mgl::LockMode::LOCK_X);
    if (!locklist.ok()) {
      return locklist.status();
    }

    uint64_t total = 0;
    for (size_t i = 1; i < args.size(); ++i) {
      auto server = sess->getServerEntry();
      auto expdb = server->getSegmentMgr()->getDbHasLocked(
              sess, args[i]);
      if (!expdb.ok()) {
        return expdb.status();
      }

      PStore kvstore = expdb.value().store;
      auto ptxn = sess->getCtx()->createTransaction(kvstore);
      if (!ptxn.ok()) {
        return ptxn.status();
      }
      Expected<bool> done = delGeneric(sess, args[i], ptxn.value());
      if (!done.ok()) {
        return done.status();
      }
      total += done.value() ? 1 : 0;
    }
    auto s = sess->getCtx()->commitAll("del");
    if (!s.ok()) {
      LOG(ERROR) << "UnlinkCommand commitAll failed:"<< s.toString();
    }
    return Command::fmtLongLong(total);
  }
} delCommand;

class UnlinkCommand : public Command {
 public:
  UnlinkCommand() : Command("unlink", "wF") {}

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

    auto index = getKeysFromCommand(args);
    auto locklist = sess->getServerEntry()->getSegmentMgr()->getAllKeysLocked(
      sess, args, index, mgl::LockMode::LOCK_X);
    if (!locklist.ok()) {
      return locklist.status();
    }

    std::vector<std::string> validKeys;
    validKeys.reserve(args.size());
    uint64_t elesNum = 0;
    for (size_t i = 1; i < args.size(); ++i) {
      const std::string& key = args[i];
      Expected<RecordValue> rv =
        Command::expireKeyIfNeeded(sess, key, RecordType::RT_DATA_META);
      if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
          rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
        continue;
      } else if (!rv.status().ok()) {
        return rv.status();
      }
      elesNum += rv.value().getEleCnt();
      validKeys.emplace_back(std::move(args[i]));
    }
    uint64_t size = validKeys.size();
    auto delKeyInTranscation =
      [](Session* sess,
         std::vector<std::string>&& keys,
         std::list<std::unique_ptr<KeyLock>>&& locklist) {
        for (size_t i = 0; i < keys.size(); ++i) {
          auto server = sess->getServerEntry();
          auto expdb = server->getSegmentMgr()->getDbHasLocked(
                  sess, keys[i]);
          if (!expdb.ok()) {
            return;
          }

          PStore kvstore = expdb.value().store;
          auto ptxn = sess->getCtx()->createTransaction(kvstore);
          if (!ptxn.ok()) {
            LOG(ERROR) << "UnlinkCommand createTransaction failed:"
              << ptxn.status().toString();
            return;
          }
          delKey(sess, keys[i], RecordType::RT_DATA_META, ptxn.value());
        }
        auto s = sess->getCtx()->commitAll("mset(nx)");
        if (!s.ok()) {
          LOG(ERROR) << "UnlinkCommand commitAll failed:" << s.toString();
        }
      };
    if (elesNum > 1024) {
      std::thread unlink(delKeyInTranscation,
                         sess,
                         std::move(validKeys),
                         std::move(locklist.value()));
      unlink.detach();
    } else {
      delKeyInTranscation(
        sess, std::move(validKeys), std::move(locklist.value()));
    }
    return Command::fmtLongLong(size);
  }
} unlinkCmd;

}  // namespace tendisplus
