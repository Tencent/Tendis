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
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/storage/varint.h"
#include "tendisplus/storage/skiplist.h"

namespace tendisplus {
class ScanGenericCommand : public Command {
 public:
  ScanGenericCommand(const std::string& name, const char* sflags)
    : Command(name, sflags) {}

  virtual RecordType getRcdType() const = 0;

  virtual RecordKey genFakeRcd(uint32_t chunkId,
                               uint32_t dbId,
                               const std::string& key) const = 0;

  virtual Expected<std::string> genResult(const std::string& cursor,
                                          const std::list<Record>& rcds) = 0;

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
    const std::string& cursor = args[2];
    size_t i = 3;
    int j;
    std::string pat;
    int usePatten = 0;
    uint64_t count = 10;
    while (i < args.size()) {
      j = args.size() - i;
      if (toLower(args[i]) == "count" && j >= 2) {
        Expected<uint64_t> ecnt = ::tendisplus::stoul(args[i + 1]);
        if (!ecnt.ok()) {
          return ecnt.status();
        }
        if (ecnt.value() < 1) {
          return {ErrorCodes::ERR_PARSEOPT, "syntax error"};
        }
        count = ecnt.value();
        i += 2;
      } else if (toLower(args[i]) == "match" && j >= 2) {
        pat = args[i + 1];
        usePatten = !(pat[0] == '*' && pat.size() == 1);
        i += 2;
      } else {
        return {ErrorCodes::ERR_PARSEOPT, "syntax error"};
      }
    }

    Expected<RecordValue> rv =
      Command::expireKeyIfNeeded(sess, key, getRcdType());
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
        rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      std::stringstream ss;
      Command::fmtMultiBulkLen(ss, 2);
      Command::fmtBulk(ss, "0");
      Command::fmtMultiBulkLen(ss, 0);
      return ss.str();
    } else if (!rv.ok()) {
      return rv.status();
    }

    auto server = sess->getServerEntry();
    auto expdb = server->getSegmentMgr()->getDbWithKeyLock(
      sess, key, mgl::LockMode::LOCK_S);
    if (!expdb.ok()) {
      return expdb.status();
    }
    SessionCtx* pCtx = sess->getCtx();
    RecordKey metaRk(
      expdb.value().chunkId, pCtx->getDbId(), getRcdType(), key, "");
    PStore kvstore = expdb.value().store;

    auto ptxn = sess->getCtx()->createTransaction(kvstore);
    if (!ptxn.ok()) {
      return ptxn.status();
    }

    std::string name = getName();
    if (name == "zscanbyscore") {
      auto eMetaContent = ZSlMetaValue::decode(rv.value().getValue());
      if (!eMetaContent.ok()) {
        return eMetaContent.status();
      }
      ZSlMetaValue meta = eMetaContent.value();
      SkipList sl(expdb.value().chunkId, pCtx->getDbId(), key, meta, kvstore);
      Zrangespec range;
      if (zslParseRange(cursor.c_str(), maxscore.c_str(), &range) != 0) {
        return {ErrorCodes::ERR_ZSLPARSERANGE, ""};
      }
      auto arr = sl.scanByScore(range, 0, count + 1, false, ptxn.value());
      if (!arr.ok()) {
        return arr.status();
      }
      std::stringstream ss;
      Command::fmtMultiBulkLen(ss, 2);
      if (arr.value().size() == count + 1) {
        Command::fmtBulk(ss, ::tendisplus::dtos(arr.value().back().first));
        arr.value().pop_back();
      } else {
        Command::fmtNull(ss);
      }
      Command::fmtMultiBulkLen(ss, arr.value().size() * 2);
      for (const auto& v : arr.value()) {
        Command::fmtBulk(ss, v.second);
        Command::fmtBulk(ss, ::tendisplus::dtos(v.first));
      }
      return ss.str();
    }

    RecordKey fake = genFakeRcd(expdb.value().chunkId, pCtx->getDbId(), key);

    auto batch = Command::scan(fake.prefixPk(), cursor, count, ptxn.value());
    if (!batch.ok()) {
      return batch.status();
    }
    const bool NOCASE = false;
    for (std::list<Record>::iterator it = batch.value().second.begin();
         it != batch.value().second.end();) {
      if (usePatten &&
          !redis_port::stringmatchlen(
            pat.c_str(),
            pat.size(),
            it->getRecordKey().getSecondaryKey().c_str(),
            it->getRecordKey().getSecondaryKey().size(),
            NOCASE)) {
        it = batch.value().second.erase(it);
      } else {
        ++it;
      }
    }
    return genResult(batch.value().first, batch.value().second);
  }

 private:
  std::string maxscore = "9223372036854775806";
};

class ZScanCommand : public ScanGenericCommand {
 public:
  ZScanCommand() : ScanGenericCommand("zscan", "rR") {}

  RecordType getRcdType() const final {
    return RecordType::RT_ZSET_META;
  }

  RecordKey genFakeRcd(uint32_t chunkId,
                       uint32_t dbId,
                       const std::string& key) const final {
    return {chunkId, dbId, RecordType::RT_ZSET_H_ELE, key, ""};
  }

  Expected<std::string> genResult(const std::string& cursor,
                                  const std::list<Record>& rcds) {
    std::stringstream ss;
    Command::fmtMultiBulkLen(ss, 2);
    Command::fmtBulk(ss, cursor);
    Command::fmtMultiBulkLen(ss, rcds.size() * 2);
    for (const auto& v : rcds) {
      Command::fmtBulk(ss, v.getRecordKey().getSecondaryKey());
      auto d = tendisplus::doubleDecode(v.getRecordValue().getValue());
      INVARIANT_D(d.ok());
      if (!d.ok()) {
        return d.status();
      }
      Command::fmtBulk(ss, tendisplus::dtos(d.value()));
    }
    return ss.str();
  }
} zscanCmd;

class ZScanbyscoreCommand : public ScanGenericCommand {
 public:
  ZScanbyscoreCommand() : ScanGenericCommand("zscanbyscore", "rR") {}

  RecordType getRcdType() const final {
    return RecordType::RT_ZSET_META;
  }

  RecordKey genFakeRcd(uint32_t chunkId,
                       uint32_t dbId,
                       const std::string& key) const final {
    return {chunkId,
            dbId,
            RecordType::RT_ZSET_S_ELE,
            key,
            std::to_string(ZSlMetaValue::HEAD_ID)};
  }

  Expected<std::string> genResult(const std::string& cursor,
                                  const std::list<Record>& rcds) {
    std::list<Record> sortRcds = rcds;
    // sortRcds.sort([](Record& a, Record& b) -> bool {
    //     auto da =
    //     tendisplus::doubleDecode(a.getRecordValue().getValue()); auto db
    //     = tendisplus::doubleDecode(b.getRecordValue().getValue());
    //     if(!da.ok() || !db.ok()) {
    //         return false;
    //     }
    //     return da.value() < db.value();
    // });
    std::stringstream ss;
    Command::fmtMultiBulkLen(ss, 2);
    Command::fmtBulk(ss, cursor);
    Command::fmtMultiBulkLen(ss, rcds.size() * 2);
    for (const auto& v : rcds) {
      auto str = v.toString() + " ";
      std::cout << str;
      if (v.getRecordKey().getSecondaryKey() ==
          std::to_string(ZSlMetaValue::HEAD_ID)) {
        continue;
      }
      Command::fmtBulk(ss, v.getRecordValue().getValue());
      auto d = tendisplus::doubleDecode(v.getRecordKey().getSecondaryKey());
      INVARIANT_D(d.ok());
      if (!d.ok()) {
        return d.status();
      }
      Command::fmtBulk(ss, tendisplus::dtos(d.value()));
    }
    return ss.str();
  }
} zscanbyscoreCmd;

class SScanCommand : public ScanGenericCommand {
 public:
  SScanCommand() : ScanGenericCommand("sscan", "rR") {}

  RecordType getRcdType() const final {
    return RecordType::RT_SET_META;
  }

  RecordKey genFakeRcd(uint32_t chunkId,
                       uint32_t dbId,
                       const std::string& key) const final {
    return {chunkId, dbId, RecordType::RT_SET_ELE, key, ""};
  }

  Expected<std::string> genResult(const std::string& cursor,
                                  const std::list<Record>& rcds) {
    std::stringstream ss;
    Command::fmtMultiBulkLen(ss, 2);
    Command::fmtBulk(ss, cursor);
    Command::fmtMultiBulkLen(ss, rcds.size());
    for (const auto& v : rcds) {
      Command::fmtBulk(ss, v.getRecordKey().getSecondaryKey());
    }
    return ss.str();
  }
} sscanCmd;

class HScanCommand : public ScanGenericCommand {
 public:
  HScanCommand() : ScanGenericCommand("hscan", "rR") {}

  RecordType getRcdType() const final {
    return RecordType::RT_HASH_META;
  }

  RecordKey genFakeRcd(uint32_t chunkId,
                       uint32_t dbId,
                       const std::string& key) const final {
    return {chunkId, dbId, RecordType::RT_HASH_ELE, key, ""};
  }

  Expected<std::string> genResult(const std::string& cursor,
                                  const std::list<Record>& rcds) final {
    std::stringstream ss;
    Command::fmtMultiBulkLen(ss, 2);
    Command::fmtBulk(ss, cursor);
    Command::fmtMultiBulkLen(ss, rcds.size() * 2);
    for (const auto& v : rcds) {
      Command::fmtBulk(ss, v.getRecordKey().getSecondaryKey());
      Command::fmtBulk(ss, v.getRecordValue().getValue());
    }
    return ss.str();
  }
} hscanCmd;

class ScanCommand : public Command {
 public:
  ScanCommand() : Command("scan", "rR") {}

  ssize_t arity() const {
    return -2;
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

  // NOTE(deyukong): tendis did not impl this api
  Expected<std::string> run(Session* sess) final {
    return Command::fmtZeroBulkLen();
  }
} scanCmd;

}  // namespace tendisplus
