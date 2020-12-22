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

  /**
   * @brief scanCmd core impl
   * @param sess user session
   * @return Expected<std::string>
   */
  Expected<std::string> run(Session* sess) final {
    size_t index = 2;                        // used for parse args
    std::string pattern;                     // subcommand "MATCH"
    uint64_t count = sess->getServerEntry()
            ->getParams()->scanDefaultLimit; // subcommand "COUNT"
    std::string type;                        // subcommand "TYPE"
    std::bitset<CLUSTER_SLOTS> slots;        // subcommand "SLOTS"
    int64_t node{0};                        // subcommand "NODE"
    bool enableSlots{false};                 // check whether has "SLOTS"
    std::pair<uint64_t, std::list<RecordKey>> batch{0, {}};

    // Step 1: get args and parse options. COUNT, MATCH, TYPE, SLOTS
    uint64_t cursor{0};
    auto &args = sess->getArgs();
    auto cursorCnt = tendisplus::stoul(args[1]);
    if (!cursorCnt.ok()) {
      return cursorCnt.status();
    } else {
      cursor = cursorCnt.value();
    }

    while (index < args.size()) {
      if (toLower(args[index]) == "count" && index + 1 <= args.size()) {
        auto cnt = tendisplus::stoul(args[index + 1]);
        if (!cnt.ok()) {
          return cnt.status();
        }
        if (cnt.value() < 1) {
          return {ErrorCodes::ERR_PARSEOPT, "Syntax Error"};
        }
        count = cnt.value();
        index += 2;
      } else if (toLower(args[index]) == "match" && index + 1 <= args.size()) {
        pattern = args[index + 1];
        index += 2;
      } else if (toLower(args[index]) == "type" && index + 1 <= args.size()) {
        type = toLower(args[index + 1]);
        index += 2;
      } else if (toLower(args[index]) == "slots" && index + 1 <= args.size()) {
        auto slotsArg = args[index + 1];
        auto slotsMeta = tendisplus::stringSplit(slotsArg, ",");
        enableSlots = true;

        for (const auto &v : slotsMeta) {
          auto slotPair = tendisplus::stringSplit(v, "-");
          if (slotPair.size() == 1) {
            auto slot = tendisplus::stol(slotPair[0]);
            if (!slot.ok()) {
              return slot.status();
            }
            if (slot.value() < 0) {
              return {ErrorCodes::ERR_PARSEOPT, "Wrong Slots"};
            }
            // set single bit as true
            slots.set(slot.value());
          } else {
            auto slotStart = tendisplus::stol(slotPair[0]);
            auto slotEnd = tendisplus::stol(slotPair[1]);
            if (!slotStart.ok()) {
              return slotStart.status();
            }
            if (slotStart.value() < 0 || slotStart.value() >= CLUSTER_SLOTS) {
              return {ErrorCodes::ERR_PARSEOPT, "Wrong Slots"};
            }
            if (!slotEnd.ok()) {
              return slotEnd.status();
            }
            if (slotEnd.value() < 0 || slotEnd.value() >= CLUSTER_SLOTS) {
              return {ErrorCodes::ERR_PARSEOPT, "Wrong Slots"};
            }

            // set bit as true from start to end
            for (int i = slotStart.value(); i <= slotEnd.value(); ++i) {
              slots.set(i);
            }
          }
        }
        index += 2;
      } else if (toLower(args[index]) == "node" && index + 1 <= args.size()) {
        auto expNode = tendisplus::stoll(args[index + 1]);
        if (!expNode.ok()) {
          return expNode.status();
        }
        if (expNode.value() < 0) {
          return {ErrorCodes::ERR_INTERGER, "Invalid Node"};
        }
        node = expNode.value();
        index += 2;
      } else {
        return {ErrorCodes::ERR_PARSEOPT, "Unknown Subcommand"};
      }
    }

    // init _filter config.
    _filter.setPattern(pattern);
    _filter.setType(type);

    // if no SLOTS, scan all slots in rocksdb
    if (!enableSlots) {
      for (size_t i = 0; i < slots.size(); ++i) {
        slots.set(i);
      }
    }

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
      kvstoreId = expMapping.value()._kvstoreId;
      lastScanKey = expMapping.value()._lastScanKey;
    }

    // use lambda return value to decode
    // because RecordKey's assign operator= function has been deleted
    // RecordKey::decode will fault when string is empty
    auto expRecordKey = [&]() -> Expected<RecordKey> {
      if (lastScanKey.empty()) {
        return genRecordKey(0, sess->getCtx()->getDbId(), "");
      } else {
        auto expLastScanRecordKey = RecordKey::decode(lastScanKey);
        if (!expLastScanRecordKey.ok()) {
          return expLastScanRecordKey.status();
        } else {
          return expLastScanRecordKey.value();
        }
      }
    }();

    if (!expRecordKey.ok()) {
      return expRecordKey.status();
    }
    auto lastScanRecordKey = expRecordKey.value();

    /*
     * scan primary keys
     * NOTE(pecochen): for Tendis, there is no need to make ScanCommand generic,
     *            Just scan primary key is Ok.
     */
    auto server = sess->getServerEntry();
    auto params = server->getParams();
    uint64_t scanTimes{0};
    uint64_t scanMaxTimes = count * params->scanDefaultCoefficient >
                            params->scanDefaultMaxIterateTimes ?
                            params->scanDefaultMaxIterateTimes :
                            count * params->scanDefaultCoefficient;
    count += 1;          // in order to add mapping, scan one more key.

    // Step 3: scan data across all kv-stores one by one
    size_t id = kvstoreId;
    for (; id < server->getKVStoreCount(); ++id) {
      if (batch.second.size() >= count || scanTimes >= scanMaxTimes) {
        break;
      }

      // use scope to control expdb life-time. (RAII)
      {
        auto expdb = server->getSegmentMgr()->getDb(
                sess, id, mgl::LockMode::LOCK_IS);
        if (!expdb.ok()) {
          return expdb.status();
        }
        auto *pCtx = sess->getCtx();
        auto kvstore = expdb.value().store;
        auto ptxn = kvstore->createTransaction(sess);
        if (!ptxn.ok()) {
          return ptxn.status();
        }
        auto txn = std::move(ptxn.value());

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
                                         id, count - batch.first,
                                         scanTimes, scanMaxTimes, txn.get());
        if (!expRecordKeys.ok()) {
          return expRecordKeys.status();
        }
        auto recordKeys = expRecordKeys.value();
        batch.first += recordKeys.first;
        for (const auto &key : recordKeys.second) {
          batch.second.emplace_back(key);
        }
      }
    }

    id -= 1;                        // id need operator-- after loop
    _filter.reset();                // reset _filter condition
    cursor += scanTimes;           // scanTimes is real cursor position.

    // means ERR_EXHAUST, should set cursor to 0
    if (batch.second.size() < count &&
        scanTimes < scanMaxTimes) {
      cursor = 0;
    }

    if (cursor && !batch.second.empty()) {
      cursorMap.addMapping(
              cursor, {static_cast<int>(id), batch.second.back().encode()});
      batch.second.pop_back();
    }

    /**
     * for series tendis nodes
     *          |<---16bit--->|<---48bit--->|
     * cursor = |   nodeId    | real-cursor |
     */
    cursor = (cursor | (node << 48U));

    return genResult(cursor, batch.second);
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
      } else if (!type.empty()){
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
   * @brief check whether slot belong to this kvstore
   * @param kvstoreId
   * @return boolean, show whether slot belong to this kvstore
   */
  inline static bool checkSlotsBelongToKvstore(uint32_t kvstoreId,
                                               uint32_t slot) {
    return (slot % 10) == kvstoreId;
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
      if (slots[i] & checkSlotsBelongToKvstore(kvstoreId, i)) {
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
                   uint32_t dbId, int kvstoreId, uint64_t count,
                   uint64_t &scanTimes, uint64_t scanMaxTimes, Transaction *txn)
            -> Expected<std::pair<uint64_t, std::list<RecordKey>>> {
    std::pair<uint64_t, std::list<RecordKey>> result{0, {}};
    auto kvstoreSlots = getKvstoreSlots(kvstoreId, slots);
    auto cursor = txn->createDataCursor();
    cursor->seek(lastScanRecordKey.encode());

    while (result.first < count && scanTimes < scanMaxTimes) {
      // get cursor current record and iterate next cursor
      auto expRecord = cursor->next();

      // ERR_EXHAUST means this kv-store has been iterated over
      if (expRecord.status().code() == ErrorCodes::ERR_EXHAUST) {
        break;
      }
      if (!expRecord.ok()) {
        return expRecord.status();
      }
      auto record = expRecord.value();

      if (record.getRecordKey() == lastScanRecordKey) {
        scanTimes -= 1;             // otherwise, calculate one more time
      }

      // check if this record match scan condition
      auto recordSlotId = record.getRecordKey().getChunkId();
      auto recordDbId = record.getRecordKey().getDbId();
      auto recordType = record.getRecordKey().getRecordType();
      if (!kvstoreSlots.test(recordSlotId)
          || recordDbId != dbId
          || recordType != RecordType::RT_DATA_META) {
        auto nextSlot = getNextSlot(slots, recordSlotId);
        // nextSlot == -1 means this kv-store has been iterated over
        if (nextSlot == -1) {
          break;
        }

        // construct new record key to seek
        auto nextRecordKey = genRecordKey(nextSlot, dbId, "");
        cursor->seek(nextRecordKey.encode());
        scanTimes++;
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

      const auto &guard = MakeGuard([&]() {
        if (!(result.first < count &&
              scanTimes < scanMaxTimes)) {
          /**
           * record the scan position
           * especially when iterate each kv-store over
           * or we'll lost keys
           * NOTE: call back() on empty list is UB
           */
          if (result.second.empty() ||
             (result.second.back() != record.getRecordKey())) {
            result.second.emplace_back(record.getRecordKey());
          }
        }
      });

      // filter
      if (!_filter.filter(record)) {
        scanTimes++;
        continue;
      }

      // this record matches all conditions
      scanTimes++;
      result.first++;
      result.second.emplace_back(record.getRecordKey());
    }

    return result;
  }

} scanCmd;

}  // namespace tendisplus
