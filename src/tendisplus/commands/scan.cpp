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
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/storage/varint.h"
#include "tendisplus/storage/skiplist.h"
#include "tendisplus/storage/record.h"

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
        RET_IF_ERR_EXPECTED(ecnt);
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
    RET_IF_ERR_EXPECTED(expdb);

    SessionCtx* pCtx = sess->getCtx();
    RecordKey metaRk(
      expdb.value().chunkId, pCtx->getDbId(), getRcdType(), key, "");
    PStore kvstore = expdb.value().store;

    auto ptxn = sess->getCtx()->createTransaction(kvstore);
    RET_IF_ERR_EXPECTED(ptxn);

    std::string name = getName();
    if (name == "zscanbyscore") {
      auto eMetaContent = ZSlMetaValue::decode(rv.value().getValue());
      RET_IF_ERR_EXPECTED(eMetaContent);
      ZSlMetaValue meta = eMetaContent.value();
      SkipList sl(expdb.value().chunkId, pCtx->getDbId(), key, meta, kvstore);
      Zrangespec range;
      if (zslParseRange(cursor.c_str(), maxscore.c_str(), &range) != 0) {
        return {ErrorCodes::ERR_ZSLPARSERANGE, ""};
      }
      auto arr = sl.scanByScore(range, 0, count + 1, false, ptxn.value());
      RET_IF_ERR_EXPECTED(arr);
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
    RET_IF_ERR_EXPECTED(batch);
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
      RET_IF_ERR_EXPECTED(d);
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
      RET_IF_ERR_EXPECTED(d);
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

  ssize_t arity() const final {
    return -2;
  }

  int32_t firstkey() const final {
    return 0;
  }

  int32_t lastkey() const final {
    return 0;
  }

  int32_t keystep() const final {
    return 0;
  }

  bool sameWithRedis() const final {
    return false;
  }

  static Status parseSessionArgs(const std::vector<std::string> &args,
                                 std::bitset<CLUSTER_SLOTS> *slots,
                                 std::string *pattern,
                                 std::string *type,
                                 uint64_t *count,
                                 uint64_t *cursor,
                                 int64_t *seq,
                                 bool *enableSlots) {
    size_t index = 2;                             // used for parse args
    auto cursorCnt = tendisplus::stoul(args[1]);
    RET_IF_ERR_EXPECTED(cursorCnt);
    *cursor = cursorCnt.value();

    while (index < args.size()) {
      if (toLower(args[index]) == "count" && index + 1 < args.size()) {
        auto cnt = tendisplus::stoul(args[index + 1]);
        RET_IF_ERR_EXPECTED(cnt);
        if (cnt.value() < 1) {
          return {ErrorCodes::ERR_PARSEOPT, "Syntax Error"};
        }
        *count = cnt.value();
        index += 2;
      } else if (toLower(args[index]) == "match" && index + 1 < args.size()) {
        pattern->assign(args[index + 1]);
        index += 2;
      } else if (toLower(args[index]) == "type" && index + 1 < args.size()) {
        type->assign(toLower(args[index + 1]));
        index += 2;
      } else if (toLower(args[index]) == "slots" && index + 1 < args.size()) {
        *enableSlots = true;
        auto expSlots = ClusterNode::parseClusterNodesInfo(args[index + 1]);
        RET_IF_ERR_EXPECTED(expSlots);

        // get the intersection of slots and SLOTS result
        // may bitwise or faster than logic or
        // if (slots->test(i) && expSlots.value().test(i)) {
        for (size_t i = 0; i < CLUSTER_SLOTS; ++i) {
          if ((*slots)[i] & expSlots.value()[i]) {
            slots->set(i, true);
          }
        }
        index += 2;
      } else if (toLower(args[index]) == "seq" && index + 1 <= args.size()) {
        auto expNode = tendisplus::stoll(args[index + 1]);
        RET_IF_ERR_EXPECTED(expNode);
        if (expNode.value() < 0) {
          return {ErrorCodes::ERR_INTERGER, "Invalid Node"};
        }
        *seq = expNode.value();
        index += 2;
      } else {
        return {ErrorCodes::ERR_PARSEOPT,
                "Unknown Subcommand " + toUpper(args[index])};
      }
    }

    return {ErrorCodes::ERR_OK, ""};
  }

  /**
   * @brief scanCmd core impl
   * @param sess user session
   * @return Expected<std::string>
   */
  Expected<std::string> run(Session* sess) final {
    std::string pattern;                      // subcommand "MATCH"
    uint64_t count = sess->getServerEntry()
            ->getParams()->scanDefaultLimit;  // subcommand "COUNT"
    std::string type;                         // subcommand "TYPE"
    std::bitset<CLUSTER_SLOTS> slots;         // subcommand "SLOTS"
    int64_t seqId{0};                         // subcommand "SEQ"
    uint64_t cursor{0};                       // used for arg "cursor"
    bool enableSlots{false};                  // check whether has "SLOTS"
    std::list<RecordKey> batch;

    if (sess->getServerEntry()->getParams()->clusterEnabled) {
      auto myself = sess->getServerEntry()
                      ->getClusterMgr()
                      ->getClusterState()
                      ->getMyselfNode();
      slots = myself->nodeIsMaster() ? myself->getSlots()
                : myself->getMaster()->getSlots();
    } else {
      slots.set();
    }

    // Step 1: get args and parse options. COUNT, MATCH, TYPE, SLOTS
    auto status = parseSessionArgs(sess->getArgs(), &slots, &pattern, &type,
                                   &count, &cursor, &seqId, &enableSlots);
    RET_IF_ERR(status);

    // init _filter config.
    _filter.setPattern(pattern);
    _filter.setType(type);

    // Step 2: check if this scan command cursor has been stored in cursorMap_
    uint64_t kvstoreId{0};
    std::string lastScanKey;
    auto &cursorMap = sess->getServerEntry()
                      ->getCursorMap(sess->getCtx()->getDbId());
    auto expMapping = cursorMap.getMapping(cursor);
    if (!expMapping.ok()) {
      if (expMapping.status().code() == ErrorCodes::ERR_NOTFOUND) {
        kvstoreId = 0;
        cursor = 0;          // invalid cursor, should reset as 0, back to start
                             // lastScanKey is still empty string
      } else {
        return expMapping.status();
      }
    } else {
      kvstoreId = expMapping.value().kvstoreId;
      lastScanKey = expMapping.value().lastScanKey;
    }

    // use lambda return value to decode
    // because RecordKey's assign operator= function has been deleted
    // RecordKey::decode will fault when string is empty
    auto expRecordKey = [&]() -> Expected<RecordKey> {
      if (lastScanKey.empty()) {
        return genRecordKey(0, sess->getCtx()->getDbId(), "");
      } else {
        auto expLastScanRecordKey = RecordKey::decode(lastScanKey);
        RET_IF_ERR_EXPECTED(expLastScanRecordKey);
        return expLastScanRecordKey.value();
      }
    }();

    RET_IF_ERR_EXPECTED(expRecordKey);
    auto lastScanRecordKey = expRecordKey.value();

    /*
     * scan primary keys
     * NOTE(pecochen): for Tendis, there is no need to make ScanCommand generic,
     *            Just scan primary key is Ok.
     */
    auto server = sess->getServerEntry();
    auto params = server->getParams();
    uint64_t seq{0};
    uint64_t scanTimes{0};
    uint64_t scanMaxTimes = params->scanDefaultMaxIterateTimes;

    count += 1;          // in order to add mapping, scan one more key.

    // Step 3: scan data across all kv-stores one by one
    size_t id = kvstoreId;
    for (; id < server->getKVStoreCount(); ++id) {
      if (batch.size() >= count || scanTimes >= scanMaxTimes) {
        break;
      }

      // use scope to control expdb life-time. (RAII)
      {
        auto expdb = server->getSegmentMgr()->getDb(
                sess, id, mgl::LockMode::LOCK_IS);
        RET_IF_ERR_EXPECTED(expdb);
        auto *pCtx = sess->getCtx();
        auto kvstore = expdb.value().store;
        auto ptxn = sess->getCtx()->createTransaction(kvstore);
        RET_IF_ERR_EXPECTED(ptxn);

        /**
         * set recordKey policy:
         * 1. id == kvstoreId
         *   means: start scan this kv-store, use lastScanRecordKey from mapping
         * 2. id != kvstoreId
         *   means: for new kv-store, should scan from its first keys
         */
        auto recordKey = [&]() -> RecordKey {
          if (id == kvstoreId) {
            return lastScanRecordKey;
          } else {
            return genRecordKey(0, pCtx->getDbId() , "");
          }
        }();
        auto expRecordKeys = scanKvstore(slots, recordKey, pCtx->getDbId(),
                                         id, count - batch.size(),
                                         &seq, &scanTimes, scanMaxTimes,
                                         ptxn.value());
        RET_IF_ERR_EXPECTED(expRecordKeys);
        auto recordKeys = expRecordKeys.value();
        for (const auto &key : recordKeys) {
          batch.emplace_back(key);
        }
      }
    }

    id -= 1;                        // id need operator-- after loop
    _filter.reset();                // reset _filter condition
    cursor += seq;                  // seq is real cursor position.

    // means ERR_EXHAUST, should set cursor to 0
    if (batch.size() < count && scanTimes < scanMaxTimes) {
      cursor = 0;
    }

    if (cursor && !batch.empty()) {
      cursorMap.addMapping(
              cursor, {static_cast<int>(id), batch.back().encode()});
      batch.pop_back();
    }

    /**
     * for series tendis nodes
     *          |<---16bit--->|<---48bit--->|
     * cursor = |   seqId    | real-cursor |
     */
    cursor = (cursor | (seqId << 48U));

    return genResult(cursor, batch);
  }

 private:
  class Filter {
   public:
    // RT_DATA_META means RT_ALL here.
    Filter() : _type(RecordType::RT_DATA_META) {}
    ~Filter() = default;

    Filter(const Filter &) = delete;
    Filter &operator=(const Filter &) = delete;
    Filter(Filter &&) = delete;
    Filter &&operator=(Filter &&) = delete;

    /**
     * @brief filter recordkey by pattern and type etc.
     * @param record record to filter
     * @return boolean shows whether it's needed
     */
    bool filter(const Record &record) {
      if (enablePattern() &&
          !redis_port::stringmatchlen(
                  _pattern.c_str(),
                  _pattern.size(),
                  record.getRecordKey().getPrimaryKey().c_str(),
                  record.getRecordKey().getPrimaryKey().size(),
                  false)) {
          return false;
      }
      if (enableType() &&
          record.getRecordValue().getRecordType() != _type) {
        return false;
      }
      // add any sub filter you need here.

      return true;
    }

    /**
     * @brief reset after scan calls
     * @return void
     */
    void reset() {
      _pattern.clear();
      _type = RecordType::RT_DATA_META;
    }

    /**
     * @brief set filter _pattern
     * @param pattern MATCH "pattern"
     */
    void setPattern(const std::string &pattern) {
      if (pattern != "*") {
        _pattern.assign(pattern);
      } else {
        _pattern.clear();
      }
    }

    /**
     * @brief set filter _type
     * @param type TYPE "type" (after toLower() operation)
     */
    void setType(std::string &type) {
      if (recordTypeMap.count(type)) {
        _type = recordTypeMap[type];
      } else if (!type.empty()) {
        _type = RecordType::RT_INVALID;
      }
    }

   private:
    /**
     * @brief check whether need pattern match
     * @return boolean
     */
    inline bool enablePattern() {
      return !_pattern.empty();
    }

    /**
     * @brief check whether need type match
     * @return boolean
     * @note RT_DATA_META means all data is needed.
     *      RT_INVALID means not support type
     */
    inline bool enableType() {
      return _type != RecordType::RT_DATA_META;
    }

    std::string _pattern;
    RecordType _type;

    std::map<std::string, RecordType> recordTypeMap = {
            {"string", RecordType::RT_KV},
            {"list", RecordType::RT_LIST_META},
            {"hash", RecordType::RT_HASH_META},
            {"set", RecordType::RT_SET_META},
            {"zset", RecordType::RT_ZSET_META},
            // TODO(pecochen): unsupport type stream now (since redis 5.0)
    };
  }_filter;

  /**
   * @brief get next slot which fit the slots condition
   * @param slots slot collection impl by bitset
   * @param slot current slot
   * @return slots or -1 (means none)
   */
  static int32_t getNextSlot(const std::bitset<CLUSTER_SLOTS> &slots,
                              int32_t slot) {
    for (size_t i = slot + 1; i < CLUSTER_SLOTS; ++i) {
      if (slots.test(i)) {
        return i;
      }
    }
    return -1;
  }

  /**
   * @brief generate RecordKey to seek by cursor
   * @param slotsId aka chunkId, get by
   * @param dbId
   * @param lastScanKey
   * @return
   */
  inline static RecordKey genRecordKey(uint32_t slotsId, uint32_t dbId,
                                const std::string &primaryKey) {
    return {slotsId, dbId, RecordType::RT_DATA_META, primaryKey, ""};
  }

  /**
   * @brief generate result string
   * @param cursor input cursor
   * @param recordKeys scan result
   * @return string return to session
   */
  static Expected<std::string> genResult(uint64_t cursor,
                                  const std::list<RecordKey>& recordKeys) {
    std::stringstream ss;
    Command::fmtMultiBulkLen(ss, 2);
    Command::fmtBulk(ss, std::to_string(cursor));
    Command::fmtMultiBulkLen(ss, recordKeys.size());
    for (const auto &v : recordKeys) {
      Command::fmtBulk(ss, v.getPrimaryKey());
    }
    return ss.str();
  }

  /**
   * @brief get slots need to scan
   * @param slots subcommand "SLOTS" args
   * @return slots. belongs to this kvsotre && belongs to "slots"
   */
  static std::bitset<CLUSTER_SLOTS> getKvstoreSlots(
          uint32_t kvstoreId, const std::bitset<CLUSTER_SLOTS> &slots) {
    std::bitset<CLUSTER_SLOTS> kvstoreSlots;
    for (size_t i = 0; i < CLUSTER_SLOTS; ++i) {
      if (slots[i] & checkKvstoreSlot(kvstoreId, i)) {
        kvstoreSlots.set(i, true);
      } else {
        kvstoreSlots.set(i, false);
      }
    }

    return kvstoreSlots;
  }

  /**
   * @brief scan one kvstore
   * @param slots slots get from subcommand "SLOTS", maybe & with cluster slots
   * @param dbId session dbid for DBID in RecordKey
   * @param kvstoreId for kvstoreSlots
   * @param lastScanKey lastScanKey from cursorMap_
   * @param count need scan number
   * @param txn transaction
   * @return {cursor, key-list}
   */
  auto scanKvstore(const std::bitset<CLUSTER_SLOTS> &slots,
                   const RecordKey &lastScanRecordKey,
                   uint32_t dbId,
                   int kvstoreId,
                   uint64_t count,
                   uint64_t *seq,
                   uint64_t *scanTimes,
                   uint64_t scanMaxTimes,
                   Transaction *txn)
            -> Expected<std::list<RecordKey>> {
    std::list<RecordKey> result;
    auto kvstoreSlots = getKvstoreSlots(kvstoreId, slots);
    auto cursor = txn->createDataCursor();
    cursor->seek(lastScanRecordKey.encode());

    while (result.size() < count && *scanTimes < scanMaxTimes) {
      // get cursor current record and iterate next cursor
      auto expRecord = cursor->next();
      (*scanTimes)++;

      // ERR_EXHAUST means this kv-store has been iterated over
      if (expRecord.status().code() == ErrorCodes::ERR_EXHAUST) {
        break;
      }
      RET_IF_ERR_EXPECTED(expRecord);
      auto record = expRecord.value();

      const auto &guard = MakeGuard([&]() {
        // record the scan position when iterate each kv-store over
        // or we'll lost record position
        // BEHAVIOR: scan no keys suitable (NOT EMPTY)  => result.second.empty()
        //           scan keys not suit filter          => back() != recordKey
        // CASE: scan this kv-store over         => no need add this record
        //       scanTimes equal to MaxScanTimes => need this record
        //       scan "COUNT" num record         => no need this record
        // NOTE: call back() on empty std::list is UB
        // NOTE: no need increase seq. If it meets the criteria,
        //     it must have been increased seq.
        // WARNING: scan this kvstore over no need this record ! ! !
        if (*scanTimes == scanMaxTimes &&
            expRecord.status().code() != ErrorCodes::ERR_EXHAUST) {
          if (result.empty() ||
              (result.back() != record.getRecordKey())) {
            result.emplace_back(record.getRecordKey());
          }
        }
      });

      // check if this record match scan condition
      auto recordSlotId = record.getRecordKey().getChunkId();
      auto recordDbId = record.getRecordKey().getDbId();
      auto recordType = record.getRecordKey().getRecordType();
      if (!kvstoreSlots.test(recordSlotId)
          || recordDbId != dbId
          || recordType != RecordType::RT_DATA_META) {
        // seq: this key in DBID db's position
        // means, recordDbId == dbId && recordType == RT_DATA_META
        // seq should NOT include this key.
        // INVALID SLOT => this record shouldn't in this db,
        // maybe slots import/migrate
        //
        // if (!kvstoreSlots.test(recordSlotId) &&
        //     recordDbId == dbId &&
        //     recordType == RecordType::RT_DATA_META) {
        //   (*seq)++;
        // }
        auto nextSlot = getNextSlot(slots, recordSlotId);
        // nextSlot == -1 means this kv-store has been iterated over
        if (nextSlot == -1) {
          break;
        }

        // construct new record key to seek
        auto nextRecordKey = genRecordKey(nextSlot, dbId, "");
        cursor->seek(nextRecordKey.encode());
        continue;
      }

      /**
       * check if this record has expired
       * expired key shouldn't increase scanTimes,
       * they only haven't been removed yet.
       */
      if (record.getRecordValue().getTtl() > 0
          && record.getRecordValue().getTtl() < msSinceEpoch()) {
        continue;
      }

      // this record should increase seq.
      // SLOTS, DBID, TYPE, TTL
      // NOTE: lastScanKey has increased seq.
      if (record.getRecordKey() != lastScanRecordKey) {
        (*seq)++;
      }

      // filter
      if (!_filter.filter(record)) {
        continue;
      }

      result.emplace_back(record.getRecordKey());
    }

    return result;
  }
} scanCmd;
}  // namespace tendisplus
