// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_STORAGE_KVSTORE_H_
#define SRC_TENDISPLUS_STORAGE_KVSTORE_H_

#include <limits>
#include <string>
#include <utility>
#include <memory>
#include <iostream>
#include <list>
#include <map>
#include <vector>
#include <atomic>

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include "rocksdb/db.h"
#include "tendisplus/utils/status.h"
#include "tendisplus/storage/record.h"
#include "tendisplus/server/session.h"

#define CATALOG_NAME "catalog"

namespace tendisplus {

class KVStore;
class Record;
class ReplLogValueEntryV2;
class ReplLogRawV2;
class ReplLogV2;
class Transaction;
class TTLIndex;
class RecordKey;
class RecordValue;
class VersionMeta;
enum class RecordType;

enum class BinlogVersion : uint8_t {
  //== "1" represents that data and binlogs are stored in one cloumn family
  BINLOG_VERSION_1 = 1,
  //== "2" represents that data and binlogs are stored in two column family
  BINLOG_VERSION_2,
};

#define ROCKSDB_NUM_LEVELS 7

using PStore = std::shared_ptr<KVStore>;

enum class ColumnFamilyNumber { ColumnFamily_Default = 0, ColumnFamily_Binlog };

class Cursor {
 public:
  Cursor() = default;
  virtual ~Cursor() = default;
  virtual void seek(const std::string& prefix) = 0;
  // seek to last of the collection, Not the prefix
  virtual void seekToLast() = 0;
  virtual Expected<Record> next() = 0;
  virtual Status prev() = 0;
  virtual Expected<std::string> key() = 0;
};

class BasicDataCursor {
 public:
  BasicDataCursor() = delete;
  explicit BasicDataCursor(std::unique_ptr<Cursor>);
  ~BasicDataCursor() = default;
  void seek(const std::string& prefix);
  // void seekToLast();
  Expected<Record> next();
  Status prev();
  Expected<std::string> key();

 protected:
  std::unique_ptr<Cursor> _baseCursor;
};

class AllDataCursor {
 public:
  AllDataCursor() = delete;
  explicit AllDataCursor(std::unique_ptr<Cursor>);
  ~AllDataCursor() = default;
  void seek(const std::string& prefix);
  void seekToLast();
  Expected<Record> next();
  Status prev();
  Expected<std::string> key();

 protected:
  std::unique_ptr<Cursor> _baseCursor;
};

class BinlogCursor {
 public:
  BinlogCursor() = delete;
  explicit BinlogCursor(std::unique_ptr<Cursor>);
  ~BinlogCursor() = default;
  void seek(const std::string& prefix);
  void seekToLast();
  Expected<Record> next();
  Status prev();
  Expected<std::string> key();

 protected:
  std::unique_ptr<Cursor> _baseCursor;
};

class TTLIndexCursor {
 public:
  TTLIndexCursor() = delete;
  TTLIndexCursor(std::unique_ptr<Cursor> cursor, std::uint64_t until);
  ~TTLIndexCursor() = default;
  Expected<TTLIndex> next();
  void prev();
  void seek(const std::string& target);
  Expected<std::string> key();

 private:
  const uint64_t _until;

 protected:
  std::unique_ptr<Cursor> _baseCursor;
};

class VersionMetaCursor {
 public:
  VersionMetaCursor() = delete;
  explicit VersionMetaCursor(std::unique_ptr<Cursor> cursor);
  ~VersionMetaCursor() = default;
  Expected<VersionMeta> next();
  void prev();
  void seek(const std::string& target);
  Expected<std::string> key();

 private:
 protected:
  std::unique_ptr<Cursor> _baseCursor;
};

class SlotCursor {
 public:
  SlotCursor() = delete;
  SlotCursor(std::unique_ptr<Cursor> cursor, uint32_t slot);
  ~SlotCursor() = default;
  Expected<Record> next();

 private:
  const uint32_t _slot;

 protected:
  std::unique_ptr<Cursor> _baseCursor;
};

class SlotsCursor {
 public:
  SlotsCursor() = delete;
  SlotsCursor(std::unique_ptr<Cursor> cursor, uint32_t start, uint32_t end);
  ~SlotsCursor() = default;
  Expected<Record> next();

 private:
  const uint32_t _startSlot;
  const uint32_t _endSlot;

 protected:
  std::unique_ptr<Cursor> _baseCursor;
};

class RepllogCursorV2 {
 public:
  RepllogCursorV2() = delete;
  // NOTE(vinchen): in range of [begin, end], be careful both close interval
  RepllogCursorV2(Transaction* txn, uint64_t begin, uint64_t end);
  ~RepllogCursorV2() = default;
  Expected<ReplLogRawV2> next();
  Expected<ReplLogV2> nextV2();
  Status seekToLast();
  static Expected<uint64_t> getMinBinlogId(Transaction* txn);
  static Expected<uint64_t> getMaxBinlogId(Transaction* txn);
  static Expected<ReplLogRawV2> getMinBinlog(Transaction* txn);
  static Expected<ReplLogRawV2> getMaxBinlog(Transaction* txn);

 protected:
  Transaction* _txn;
  std::unique_ptr<BinlogCursor> _baseCursor;

 private:
  uint64_t _start;
  uint64_t _cur;
  const uint64_t _end;
};

class Transaction {
 public:
  Transaction() = default;
  Transaction(const Transaction&) = delete;
  Transaction(Transaction&&) = delete;
  virtual ~Transaction() = default;
  virtual Expected<uint64_t> commit() = 0;
  virtual Status rollback() = 0;
  virtual Status flushall() = 0;
  virtual Status migrate(const std::string& logKey,
                         const std::string& logValue) = 0;

  virtual std::unique_ptr<RepllogCursorV2> createRepllogCursorV2(
    uint64_t begin, bool ignoreReadBarrier = false) = 0;
  virtual Status applyBinlog(const ReplLogValueEntryV2& logEntry) = 0;
  virtual Status setBinlogKV(uint64_t binlogId,
                             const std::string& logKey,
                             const std::string& logValue) = 0;
  virtual Status setBinlogKV(const std::string& logKey,
                             const std::string& logValue) = 0;
  virtual Status delBinlog(const ReplLogRawV2& log) = 0;
  virtual uint64_t getBinlogId() const = 0;
  virtual void setBinlogId(uint64_t binlogId) = 0;
  virtual uint32_t getChunkId() const = 0;
  virtual std::string getKVStoreId() const = 0;
  virtual void setChunkId(uint32_t chunkId) = 0;
  virtual void SetSnapshot() = 0;

  virtual std::unique_ptr<TTLIndexCursor> createTTLIndexCursor(
    std::uint64_t until) = 0;
  virtual std::unique_ptr<SlotCursor> createSlotCursor(uint32_t slot) = 0;
  virtual std::unique_ptr<SlotsCursor> createSlotsCursor(uint32_t start,
                                                         uint32_t end) = 0;
  virtual std::unique_ptr<VersionMetaCursor> createVersionMetaCursor() = 0;
  virtual std::unique_ptr<BasicDataCursor> createDataCursor() = 0;
  virtual std::unique_ptr<AllDataCursor> createAllDataCursor() = 0;
  virtual std::unique_ptr<BinlogCursor> createBinlogCursor() = 0;

  virtual Expected<std::string> getKV(const std::string& key) = 0;
  virtual Status setKV(const std::string& key,
                       const std::string& val,
                       const uint64_t ts = 0) = 0;
  virtual Status delKV(const std::string& key, const uint64_t ts = 0) = 0;
  virtual Status addDeleteRangeBinlog(const std::string& begin,
                                      const std::string& end) = 0;
  virtual uint64_t getBinlogTime() = 0;
  virtual void setBinlogTime(uint64_t timestamp) = 0;
  virtual bool isReplOnly() const = 0;
  virtual uint64_t getTxnId() const = 0;

 protected:
  virtual std::unique_ptr<Cursor> createCursor(ColumnFamilyNumber cf,
          const std::string* iterate_upper_bound = NULL) = 0;

 public:
  static constexpr uint64_t MAX_VALID_TXNID =
    std::numeric_limits<uint64_t>::max() / 2;
  static constexpr uint64_t MIN_VALID_TXNID = 1;
  static constexpr uint64_t TXNID_UNINITED = 0;

  static_assert(TXNID_UNINITED + 1 == MIN_VALID_TXNID,
                "invalid TXNID_UNINITED");


  static constexpr uint32_t CHUNKID_UNINITED = 0xFFFFFFFF;
  static constexpr uint32_t CHUNKID_MULTI = 0xFFFFFFFE;
  static constexpr uint32_t CHUNKID_FLUSH = 0xFFFFFFFD;
  static constexpr uint32_t CHUNKID_MIGRATE = 0xFFFFFFFC;
  static constexpr uint32_t CHUNKID_DEL_RANGE = 0xFFFFFFFB;
};

class BackupInfo {
 public:
  BackupInfo();
  const std::map<std::string, uint64_t>& getFileList() const;
  void setFileList(const std::map<std::string, uint64_t>&);
  void setBinlogPos(uint64_t);
  void setBackupMode(uint8_t);
  void setStartTimeSec(uint64_t);
  void setEndTimeSec(uint64_t);
  void setBinlogVersion(BinlogVersion binlogversion);
  uint64_t getBinlogPos() const;
  uint8_t getBackupMode() const;
  uint64_t getStartTimeSec() const;
  uint64_t getEndTimeSec() const;
  BinlogVersion getBinlogVersion() const;
  void addFile(const std::string& file, uint64_t size);

 private:
  std::map<std::string, uint64_t> _fileList;
  uint64_t _binlogPos;
  uint8_t _backupMode;
  uint64_t _startTimeSec;
  uint64_t _endTimeSec;
  BinlogVersion _binlogVersion;
};

class BinlogObserver {
 public:
  virtual ~BinlogObserver() = default;
};

struct KVStoreStat {
  std::atomic<uint64_t> compactFilterCount;
  std::atomic<uint64_t> compactKvExpiredCount;
  // number of request when store is paused
  std::atomic<uint64_t> pausedErrorCount;
  // number of request when store is destroyed
  std::atomic<uint64_t> destroyedErrorCount;
};

#define BINLOG_HEADER_V2 "BINLOG_V2\r\n"
#define BINLOG_HEADER_V2_LEN (strlen(BINLOG_HEADER_V2) + sizeof(uint32_t))

struct TruncateBinlogResult {
  TruncateBinlogResult()
    : newStart(0), newSave(0), timestamp(0), written(0), deleten(0), ret(0) {}

  uint64_t newStart;
  uint64_t newSave;
  uint64_t timestamp;
  uint64_t written;
  uint64_t deleten;
  int32_t ret;
};

class KVStore {
 public:
  enum class StoreMode { READ_WRITE = 0, REPLICATE_ONLY = 1, STORE_NONE = 2 };

  enum class BackupMode { BACKUP_COPY, BACKUP_CKPT, BACKUP_CKPT_INTER };


  explicit KVStore(const std::string& id, const std::string& path);
  virtual ~KVStore() = default;
  const std::string& dbPath() const {
    return _dbPath;
  }
  const std::string& dbId() const {
    return _id;
  }
  const std::string dftBackupDir() const {
    return _backupDir;
  }
  virtual Expected<std::unique_ptr<Transaction>> createTransaction(
    Session* sess) = 0;
  virtual Expected<RecordValue> getKV(const RecordKey& key,
                                      Transaction* txn) = 0;
  virtual Expected<RecordValue> getKV(const RecordKey& key,
                                      Transaction* txn,
                                      RecordType valueType) = 0;
  virtual Status setKV(const RecordKey&, const RecordValue&, Transaction*) = 0;
  virtual Status setKV(const Record& kv, Transaction* txn) = 0;
  // TODO(eliotwang) deprecate this member function
  virtual Status setKV(const std::string& key,
                       const std::string& val,
                       Transaction* txn) = 0;
  virtual Status delKV(const RecordKey& key, Transaction* txn) = 0;
  // [begin, end)
  virtual Status deleteRange(const std::string& begin,
                             const std::string& end) = 0;
  virtual Status deleteRangeBinlog(uint64_t begin, uint64_t end) = 0;

  virtual Status assignBinlogIdIfNeeded(Transaction* txn) = 0;
  virtual void setNextBinlogSeq(uint64_t binlogId, Transaction* txn) = 0;
  virtual uint64_t getNextBinlogSeq() const = 0;
  static std::ofstream* createBinlogFile(const std::string& name,
                                         uint32_t storeId);
  virtual Expected<TruncateBinlogResult> truncateBinlogV2(uint64_t start,
                                                          uint64_t end,
                                                          uint64_t save,
                                                          Transaction* txn,
                                                          std::ofstream* fs,
                                                          int64_t maxWritelen,
                                                          bool tailSlave) = 0;
  virtual Expected<uint64_t> getBinlogCnt(Transaction* txn) const = 0;
  virtual Expected<bool> validateAllBinlog(Transaction* txn) const = 0;

  virtual Status setLogObserver(std::shared_ptr<BinlogObserver>) = 0;
  virtual Status compactRange(ColumnFamilyNumber cf,
                              const std::string* begin,
                              const std::string* end) = 0;
  virtual Status fullCompact() = 0;

  // remove all data in db
  virtual Status clear() = 0;

  virtual bool isRunning() const = 0;
  virtual bool isOpen() const = 0;
  virtual bool isEmpty(bool ignoreBinlog = false) const = 0;
  virtual bool isPaused() const = 0;
  virtual Status stop() = 0;
  virtual Status pause() = 0;
  virtual Status resume() = 0;
  virtual Status destroy() = 0;
  virtual bool getIntProperty(const std::string& property, uint64_t* value,
    ColumnFamilyNumber cf = ColumnFamilyNumber::ColumnFamily_Default) const = 0;
  virtual bool getProperty(const std::string& property,
                           std::string* value) const = 0;
  virtual std::string getAllProperty() const = 0;
  virtual std::string getStatistics() const = 0;
  virtual uint64_t getStatCountById(uint32_t id) const = 0;
  virtual uint64_t getStatCountByName(const std::string& name) const = 0;
  virtual std::string getBgError() const = 0;
  virtual Status recoveryFromBgError() = 0;
  virtual void resetStatistics() = 0;

  virtual Expected<VersionMeta> getVersionMeta() = 0;
  virtual Expected<VersionMeta> getVersionMeta(const std::string& name) = 0;
  virtual Expected<std::vector<VersionMeta>>
          getAllVersionMeta(Transaction *txn) = 0;
  virtual Status setVersionMeta(const std::string& name,
                                uint64_t ts,
                                uint64_t version) = 0;

  virtual Status setMode(StoreMode mode) = 0;
  virtual KVStore::StoreMode getMode() = 0;
  virtual uint64_t getHighestBinlogId() const = 0;

  // return the greatest commitId
  virtual Expected<uint64_t> restart(
    bool restore = false,
    uint64_t nextBinlogid = Transaction::MIN_VALID_TXNID,
    uint64_t maxBinlogid = Transaction::TXNID_UNINITED,
    uint32_t flag = 0) = 0;
  virtual Expected<uint64_t> flush(Session* sess, uint64_t nextBinlogid) = 0;

  // backup related apis, allows only one backup at a time
  // backup and return the filename<->filesize pair
  virtual Expected<BackupInfo> backup(const std::string&,
                                      BackupMode,
                                      BinlogVersion) = 0;
  virtual Expected<std::string> restoreBackup(const std::string& dir) = 0;
  virtual Expected<BackupInfo> getBackupMeta(const std::string& dir) = 0;
  virtual Status releaseBackup() = 0;

  virtual void appendJSONStat(
    rapidjson::PrettyWriter<rapidjson::StringBuffer>&) const = 0;

  uint64_t getBinlogTime();
  void setBinlogTime(uint64_t timestamp);
  uint64_t getCurrentTime();
  virtual Status setOption(const std::string& option, int64_t value) = 0;
  virtual int64_t getOption(const std::string& option) = 0;

  KVStoreStat stat;

 private:
  const std::string _id;
  const std::string _dbPath;
  const std::string _backupDir;
  std::atomic<uint64_t> _binlogTimeSpov;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_STORAGE_KVSTORE_H_
