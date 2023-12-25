// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_STORAGE_ROCKS_ROCKS_KVSTORE_H_
#define SRC_TENDISPLUS_STORAGE_ROCKS_ROCKS_KVSTORE_H_

#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"

#include "tendisplus/server/server_params.h"
#include "tendisplus/storage/kvstore.h"

namespace tendisplus {

class RocksKVStore;
class RocksdbEnv;
class BackgroundErrorListener;

enum class TxnMode {
  TXN_OPT = 0,
  TXN_PES = 1,
  TXN_WB = 2,
};

class RocksTxn : public Transaction {
 public:
  RocksTxn(RocksKVStore* store,
           uint64_t txnId,
           bool replOnly,
           std::shared_ptr<BinlogObserver> logob,
           Session* sess,
           TxnMode txnMode = TxnMode::TXN_WB,
           uint64_t binlogId = Transaction::TXNID_UNINITED,
           uint32_t chunkId = Transaction::CHUNKID_UNINITED);
  RocksTxn(const RocksTxn&) = delete;
  RocksTxn(RocksTxn&&) = delete;
  virtual ~RocksTxn();
  std::unique_ptr<RepllogCursorV2> createRepllogCursorV2(
    uint64_t begin, bool ignoreReadBarrier) final;
  std::unique_ptr<TTLIndexCursor> createTTLIndexCursor(uint64_t until) final;
  std::unique_ptr<SlotCursor> createSlotCursor(uint32_t slot) final;
  std::unique_ptr<SlotsCursor> createSlotsCursor(uint32_t start,
                                                 uint32_t end) final;
  std::unique_ptr<VersionMetaCursor> createVersionMetaCursor() final;
  std::unique_ptr<BasicDataCursor> createDataCursor() final;
  std::unique_ptr<AllDataCursor> createAllDataCursor() final;
  std::unique_ptr<BinlogCursor> createBinlogCursor() final;

  Expected<uint64_t> commit() final;
  virtual Status rollback();
  // getKV: get data from chosen column family
  Expected<std::string> getKV(const std::string& key) final;
  Status setKV(const std::string& key,
               const std::string& val,
               const uint64_t ts = 0) final;
  Status delKV(const std::string& key, const uint64_t ts = 0) final;
  Status setKVWithoutBinlog(const std::string& key,
                            const std::string& val) final;
  Status addDeleteRangeBinlog(const std::string& begin,
                              const std::string& end) final;
  Status addDeleteFilesInRangeBinlog(const std::string& begin,
                                     const std::string& end,
                                     bool include_end = false) final;
  Status flushall() final;
  Status migrate(const std::string& logKey, const std::string& logValue) final;

  Status applyBinlog(const ReplLogValueEntryV2& logEntry) final;
  Status setBinlogKV(uint64_t binlogId,
                     const std::string& logKey,
                     const std::string& logValue) final;
  Status setBinlogKV(const std::string& logKey,
                     const std::string& logValue) final;
  Status delBinlog(const ReplLogRawV2& log) final;
  uint64_t getBinlogId() const final;
  void setBinlogId(uint64_t binlogId) final;
  uint32_t getChunkId() const final {
    return _chunkId;
  }
  void setChunkId(uint32_t chunkId) final;
  uint64_t getTxnId() const final;
  bool isReplOnly() const {
    return _replOnly;
  }
  std::string getKVStoreId() const;
  const std::unique_ptr<rocksdb::Transaction>& getRocksdbTxn() const {
    return _txn;
  }
  void setTxnType(TxnMode type) {
    _txnMode = type;
  }

  // Transaction API
  // put data to default column family
  virtual rocksdb::Status put(const std::string& key, const std::string& val);
  virtual rocksdb::Status put(rocksdb::ColumnFamilyHandle* columnFamily,
                              const std::string& key,
                              const std::string& val);
  virtual rocksdb::Status get(const rocksdb::ReadOptions& options,
                              rocksdb::ColumnFamilyHandle* columnFamily,
                              const std::string& key,
                              std::string* value);
  virtual rocksdb::Status del(rocksdb::ColumnFamilyHandle* columnFamily,
                              const std::string& key);
  virtual const rocksdb::Snapshot* getSnapshot();
  virtual rocksdb::Iterator* getIterator(
    rocksdb::ReadOptions readOpts, rocksdb::ColumnFamilyHandle* columnFamily);

 protected:
  virtual void ensureTxn() {}
  std::unique_ptr<Cursor> createCursor(
    ColumnFamilyNumber cf, const std::string* iterate_upper_bound = NULL) final;
  virtual rocksdb::Status txnCommit();

  uint64_t _txnId;
  uint64_t _binlogId;
  uint32_t _chunkId;

  std::atomic<TxnMode> _txnMode{TxnMode::TXN_PES};
  rocksdb::WriteOptions _writeOpts;

  // NOTE(deyukong): I believe rocksdb does clean job in txn's destructor
  std::unique_ptr<rocksdb::Transaction> _txn;
  std::string _strUpperBound;
  rocksdb::Slice _upperBound;

  // NOTE(deyukong): not owned by me
  RocksKVStore* _store;

  std::vector<ReplLogValueEntryV2> _replLogValues;

  // if rollback/commit has been explicitly called
  bool _done;

  bool _replOnly;

  std::shared_ptr<BinlogObserver> _logOb;
  Session* _session;
};

// TODO(deyukong): donot modify store's unCommittedTxn list if
// its only a read-transaction

// NOTE(deyukong): RocksOptTxn does not guarantee thread-safety
// Do not use one RocksOptTxn to do parallel things.
class RocksOptTxn : public RocksTxn {
 public:
  RocksOptTxn(RocksKVStore* store,
              uint64_t txnId,
              bool replOnly,
              std::shared_ptr<BinlogObserver> logob,
              Session* sess);
  RocksOptTxn(const RocksOptTxn&) = delete;
  RocksOptTxn(RocksOptTxn&&) = delete;
  virtual ~RocksOptTxn() = default;

 protected:
  void ensureTxn() final;
  void SetSnapshot() final;
};

class RocksPesTxn : public RocksTxn {
 public:
  RocksPesTxn(RocksKVStore* store,
              uint64_t txnId,
              bool replOnly,
              std::shared_ptr<BinlogObserver> logob,
              Session* sess);
  RocksPesTxn(const RocksPesTxn&) = delete;
  RocksPesTxn(RocksPesTxn&&) = delete;
  virtual ~RocksPesTxn() = default;

 protected:
  void ensureTxn() final;
  void SetSnapshot() final;
};

class RocksWBTxn : public RocksTxn {
 public:
  RocksWBTxn(RocksKVStore* store,
             uint64_t txnId,
             bool replOnly,
             std::shared_ptr<BinlogObserver> logob,
             Session* sess);
  RocksWBTxn(const RocksWBTxn&) = delete;
  RocksWBTxn(RocksWBTxn&&) = delete;
  virtual ~RocksWBTxn();

 protected:
  void ensureTxn() final;
  void SetSnapshot() final;

  rocksdb::WriteBatchWithIndex* getWriteBatch() {
    return _writeBatch;
  }

  // Transaction API
  // put data into default column family
  rocksdb::Status put(const std::string& key, const std::string& val) final;
  rocksdb::Status put(rocksdb::ColumnFamilyHandle* columnFamily,
                      const std::string& key,
                      const std::string& val) final;
  rocksdb::Status get(const rocksdb::ReadOptions& options,
                      rocksdb::ColumnFamilyHandle* columnFamily,
                      const std::string& key,
                      std::string* value) final;
  rocksdb::Status del(rocksdb::ColumnFamilyHandle* columnFamily,
                      const std::string& key) final;
  rocksdb::Status txnCommit() final;
  Status rollback() final;
  const rocksdb::Snapshot* getSnapshot() final;
  rocksdb::Iterator* getIterator(
    rocksdb::ReadOptions readOpts,
    rocksdb::ColumnFamilyHandle* columnFamily) final;

 private:
  rocksdb::WriteBatchWithIndex* _writeBatch;
  rocksdb::Snapshot* _snapshot;
};

class RocksKVCursor : public Cursor {
 public:
  explicit RocksKVCursor(std::unique_ptr<rocksdb::Iterator>);
  virtual ~RocksKVCursor() = default;
  void seek(const std::string& prefix) final;
  void seekToLast() final;
  Expected<Record> next() final;
  Status prev() final;
  Expected<std::string> key() final;

 private:
  std::unique_ptr<rocksdb::Iterator> _it;
  bool _seeked;
};

typedef struct sstMetaData {
  uint64_t size = 0;
  uint64_t num_entries = 0;
  uint64_t num_deletions = 0;
} sstMetaData;

#define ROCKS_FLAGS_BINLOGVERSION_CHANGED (1 << 0)

class RocksKVStore : public KVStore {
 public:
  RocksKVStore(
    const std::string& id,
    const std::shared_ptr<ServerParams>& cfg,
    std::shared_ptr<rocksdb::Cache> blockCache,
    std::shared_ptr<rocksdb::Cache> rowCache = nullptr,
    std::shared_ptr<rocksdb::Cache> blobCache = nullptr,
    std::shared_ptr<rocksdb::RateLimiter> rateLimiter = nullptr,
    std::shared_ptr<rocksdb::SstFileManager> sstFileManager = nullptr,
    bool enableRepllog = true,
    KVStore::StoreMode mode = KVStore::StoreMode::READ_WRITE,
    TxnMode txnMode = TxnMode::TXN_WB,
    uint32_t flag = 0);
  virtual ~RocksKVStore() {
    stop();
  }
  Expected<std::unique_ptr<Transaction>> createTransaction(Session* sess) final;
  Expected<RecordValue> getKV(const RecordKey&, Transaction*) final;
  Status setKV(const RecordKey&, const RecordValue&, Transaction*) final;
  Status delKV(const RecordKey&, Transaction*) final;

  // [begin, end)
  Status deleteRange(const std::string& begin,
                     const std::string& end,
                     bool deleteFilesInRangeBeforeDeleteRange = false,
                     bool compactRangeAfterDeleteRange = false) override;
  Status deleteRangeBinlog(uint64_t begin, uint64_t end) override;
  Status deleteRangeWithoutBinlog(rocksdb::ColumnFamilyHandle* column_family,
                                  const std::string& begin,
                                  const std::string& end) override;

  // [begin, end) if include_end = false
  // [begin, end] if include_end = true
  Status deleteFilesInRange(const std::string& begin,
                            const std::string& end,
                            bool include_end = false) override;
  Status deleteFilesInRangeBinlog(uint64_t begin,
                                  uint64_t end,
                                  bool include_end = false) override;
  Status deleteFilesInRangeWithoutBinlog(ColumnFamilyNumber cf,
                                         const std::string& begin,
                                         const std::string& end,
                                         bool include_end = false) override;
  Status deleteFilesInRangeWithoutBinlog(
    rocksdb::ColumnFamilyHandle* column_family,
    const std::string& begin,
    const std::string& end,
    bool include_end = false) override;

  // [begin, end]
  // [nullptr, nullptr] -> [-inf, +inf]
  Status compactRange(ColumnFamilyNumber cf,
                      const std::string* begin,
                      const std::string* end) override;
  Status fullCompact() override;
  void bgCompact() override;

  Expected<uint64_t> GetApproximateSizes(ColumnFamilyNumber cf,
                                         const std::string* begin,
                                         const std::string* end,
                                         bool incl_mem) override;
  Status saveMinBinlogId(uint64_t id, uint64_t ts);
  Status handleRocksdbError(rocksdb::Status s) const;

  Status assignBinlogIdIfNeeded(Transaction* txn) final;
  void setNextBinlogSeq(uint64_t binlogId, Transaction* txn) final;
  uint64_t getNextBinlogSeq() const final;
  Expected<TruncateBinlogResult> truncateBinlogV2(std::ofstream* fs,
                                                  uint64_t start,
                                                  uint64_t end,
                                                  uint64_t dump,
                                                  uint64_t maxWriteLen) final;
  int64_t dumpBinlogV2(std::ofstream* fs, const ReplLogRawV2& log);
  Expected<uint64_t> getBinlogCnt(Transaction* txn) const final;
  Expected<bool> validateAllBinlog(Transaction* txn) const final;
  Status setLogObserver(std::shared_ptr<BinlogObserver>) final;
  // RocksDB::DestroyDB
  Status clear() final;
  bool isRunning() const final;
  Status stop() final;

  Status setMode(StoreMode mode) final;
  KVStore::StoreMode getMode() const final {
    return _mode;
  }

  bool isOpen() const final {
    return _mode != KVStore::StoreMode::STORE_NONE;
  }

  // check whether there is any data in the store
  bool isEmpty(bool ignoreBinlog = false) const final;
  // check whether the store get do get/set operations
  bool isPaused() const final;
  bool enableRepllog() const {
    return _enableRepllog;
  }
  bool recoveryMode() const {
    return _cfg->forceRecovery != 0;
  }
  Status pause() final;
  Status resume() final;
  // stop() && clear()
  Status destroy() final;

  TxnMode getTxnMode() const;

  Expected<uint64_t> restart(
    bool restore = false,
    uint64_t nextBinlogid = Transaction::MIN_VALID_TXNID,
    uint64_t maxBinlogid = UINT64_MAX,
    uint32_t flag = 0) final;
  Expected<uint64_t> flush(Session* sess, uint64_t nextBinlogid) final;

  Expected<BackupInfo> backup(const std::string&,
                              KVStore::BackupMode,
                              BinlogVersion binlogVersion) final;
  Expected<std::string> restoreBackup(const std::string& dir) final;
  Expected<BackupInfo> getBackupMeta(const std::string& dir) final;

  Status releaseBackup() final;

  void appendJSONStat(
    rapidjson::PrettyWriter<rapidjson::StringBuffer>&) const final;

  // if binlogTxnId == Transaction::TXNID_UNINITED, it mean rollback
  void markCommitted(uint64_t txnId, uint64_t binlogTxnId);
  rocksdb::OptimisticTransactionDB* getUnderlayerOptDB();
  rocksdb::TransactionDB* getUnderlayerPesDB();

  uint64_t getHighestBinlogId() const final;

  // NOTE(deyukong): this api is only for debug
  std::set<uint64_t> getUncommittedTxns() const;

  const std::shared_ptr<ServerParams>& getCfg() const {
    return _cfg;
  }

  bool getIntProperty(
    const std::string& property,
    uint64_t* value,
    ColumnFamilyNumber cf = ColumnFamilyNumber::ColumnFamily_Default) const;
  bool getProperty(
    const std::string& property,
    std::string* value,
    ColumnFamilyNumber cf = ColumnFamilyNumber::ColumnFamily_Default) const;
  std::string getAllProperty() const override;
  std::string getStatistics() const override;
  uint64_t getStatCountById(uint32_t id) const override;
  uint64_t getStatCountByName(const std::string& name) const override;
  std::string getBgError() const override;
  Status recoveryFromBgError() override;
  void resetStatistics();
  Status setOptionDynamic(const std::string& option,
                          const std::string& value) override;
  Status setCompactOnDeletionCollectorFactory(
    const std::string& option, const std::string& value) override;
  int64_t getOption(const std::string& option) override;
  const rocksdb::Snapshot* getSnapshot();
  rocksdb::Iterator* newIterator(const rocksdb::ReadOptions& readOptions,
                                 rocksdb::ColumnFamilyHandle* columnFamily);

  Expected<VersionMeta> getVersionMeta() override;
  Expected<VersionMeta> getVersionMeta(const std::string& name) override;
  Expected<std::vector<VersionMeta>> getAllVersionMeta(
    Transaction* txn) override;
  Status setVersionMeta(const std::string& name,
                        uint64_t ts,
                        uint64_t version) override;
  rocksdb::ColumnFamilyHandle* getDataColumnFamilyHandle() {
    return _cfHandles[0];
  }
  rocksdb::ColumnFamilyHandle* getBinlogColumnFamilyHandle() {
    if (_cfg->binlogUsingDefaultCF == true) {
      return _cfHandles[0];
    } else {
      return _cfHandles[1];
    }
  }
  rocksdb::ColumnFamilyHandle* getColumnFamilyHandle(
    ColumnFamilyNumber cfNum) const {
    if (cfNum == ColumnFamilyNumber::ColumnFamily_Default) {
      return _cfHandles[0];
    } else if (cfNum == ColumnFamilyNumber::ColumnFamily_Binlog) {
      if (_cfg->binlogUsingDefaultCF == true) {
        return _cfHandles[0];
      } else {
        return _cfHandles[1];
      }
    } else {
      return _cfHandles[0];
    }
  }

  rocksdb::ColumnFamilyHandle* getColumnFamilyHandleByRecordType(
    RecordType type) {
    if (type == RecordType::RT_BINLOG) {
      return getBinlogColumnFamilyHandle();
    } else {
      return getDataColumnFamilyHandle();
    }
  }

  ColumnFamilyNumber getBinlogColumnFamilyNumber() {
    if (_cfg->binlogUsingDefaultCF == true) {
      return ColumnFamilyNumber::ColumnFamily_Default;
    } else {
      return ColumnFamilyNumber::ColumnFamily_Binlog;
    }
  }
  rocksdb::DB* getBaseDB() const;
  rocksdb::WriteOptions writeOptions();

 private:
  void addUnCommitedTxnInLock(uint64_t txnId);
  void markCommittedInLock(uint64_t txnId, uint64_t binlogTxnId);
  rocksdb::Options options(const std::string cf = "");
  rocksdb::Options binlogColumnOptions();
  rocksdb::Options defaultColumnOptions();
  Expected<bool> deleteBinlog(uint64_t start);
  void initRocksProperties();
  Expected<std::string> saveBackupMeta(const std::string& dir,
                                       BackupInfo* result);
  Expected<std::string> loadCopy(const std::string& dir);
  Expected<std::string> copyCkpt(const std::string& dir);

 private:
  mutable std::mutex _mutex;

  const std::shared_ptr<ServerParams> _cfg;
  // Protected by _mutex
  std::atomic<bool> _isRunning;
  // _isPaused = true, it means that the rocksdb can't do any
  // get/set operations. But the rocksdb is running. It can be
  // reopen again.
  std::atomic<bool> _isPaused;
  std::atomic<bool> _hasBackup;
  bool _enableRepllog;

  std::atomic<KVStore::StoreMode> _mode;

  const TxnMode _txnMode;

  std::unique_ptr<rocksdb::OptimisticTransactionDB> _optdb;
  std::unique_ptr<rocksdb::TransactionDB> _pesdb;

  std::shared_ptr<rocksdb::Statistics> _stats;
  std::shared_ptr<rocksdb::Cache> _blockCache;
  std::shared_ptr<rocksdb::Cache> _rowCache;
  std::shared_ptr<rocksdb::Cache> _blobCache;
  std::shared_ptr<rocksdb::RateLimiter> _rateLimiter;
  std::shared_ptr<rocksdb::SstFileManager> _sstFileManager;

  uint64_t _nextTxnSeq;
  // Protected by _mutex
  // NOTE(deyukong): sorted data-structure is required here.
  // we rely on the data order to maintain active txns' watermark.
  // txnid -> committed|uncommited
  // --------------------------------------------------------------
  // when creating txns, a {txnId, {false, 0}} pair is inserted
  // when txn commits or destroys, the txnId is marked as true
  // means it is committed. As things run parallely, there will
  // be false-holes in _aliveTxns, fortunely, when _aliveTxns.begin()
  // changes from uncommitted to committed, we have a chance to
  // remove all the continous committed txnIds follows it, and
  // push _highestVisible forward.
  uint64_t _nextBinlogSeq;  // high water level for binlog id
  // <txnId, <commit_or_not, binlogId>>
  std::unordered_map<uint64_t, std::pair<bool, uint64_t>> _aliveTxns;

  // As things run parallel, there will be false-holes in _aliveBinlogs.
  // Fortunely, when _aliveBinlogs.begin() changes from uncommitted to
  // committed, we have a chance to remove all the continuous committed
  // binlogIds follows it, and push _highestVisible forward.
  // <binlogId, <commit_or_not, txnId>>
  std::map<uint64_t, std::pair<bool, uint64_t>> _aliveBinlogs;

  // NOTE(deyukong): _highestVisible is the largest committed binlog
  // before _aliveTxns.begin()
  // TOD0(vinchen) : make it actomic?
  uint64_t _highestVisible;  // low water level for binlog id

  std::shared_ptr<BinlogObserver> _logOb;
  std::shared_ptr<RocksdbEnv> _env;
  std::map<std::string, std::string> _rocksIntProperties;
  std::map<std::string, std::string> _rocksStringProperties;
  std::vector<rocksdb::ColumnFamilyHandle*> _cfHandles;
  std::vector<rocksdb::ColumnFamilyDescriptor> _cfDescs;
};

class RocksdbEnv {
 public:
  RocksdbEnv();
  uint64_t getErrorCnt() const {
    return _errCnt.load(std::memory_order_relaxed);
  }
  std::string getErrorString() const;
  void clear();
  void resetError();
  void setError(rocksdb::BackgroundErrorReason reason, rocksdb::Status* error);

 private:
  mutable std::mutex _mutex;
  std::atomic<uint64_t> _errCnt;
  rocksdb::BackgroundErrorReason _reason;
  std::string _bgError;
  rocksdb::Status _rocksbgError;
};

class BackgroundErrorListener : public rocksdb::EventListener {
 private:
  std::shared_ptr<RocksdbEnv> _env;

 public:
  explicit BackgroundErrorListener(std::shared_ptr<RocksdbEnv> env)
    : _env(env) {}

  void OnBackgroundError(rocksdb::BackgroundErrorReason reason,
                         rocksdb::Status* bg_error) override;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_STORAGE_ROCKS_ROCKS_KVSTORE_H_
