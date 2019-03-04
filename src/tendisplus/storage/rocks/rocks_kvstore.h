#ifndef SRC_TENDISPLUS_STORAGE_ROCKS_ROCKS_KVSTORE_H_
#define SRC_TENDISPLUS_STORAGE_ROCKS_ROCKS_KVSTORE_H_

#include <memory>
#include <string>
#include <iostream>
#include <set>
#include <mutex>
#include <map>
#include <vector>
#include <utility>
#include <list>
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "tendisplus/server/server_params.h"
#include "tendisplus/storage/kvstore.h"

namespace tendisplus {

class RocksKVStore;

class RocksTxn: public Transaction {
 public:
    RocksTxn(RocksKVStore *store, uint64_t txnId, bool replOnly, std::shared_ptr<BinlogObserver> logob);
    RocksTxn(const RocksTxn&) = delete;
    RocksTxn(RocksTxn&&) = delete;
    virtual ~RocksTxn();
    std::unique_ptr<Cursor> createCursor() final;
    std::unique_ptr<BinlogCursor> createBinlogCursor(
                                    uint64_t begin,
                                    bool ignoreReadBarrier) final;
    Expected<uint64_t> commit() final;
    Status rollback() final;
    Expected<std::string> getKV(const std::string&) final;
    Status setKV(const std::string& key,
                 const std::string& val) final;
    Status delKV(const std::string& key) final;
    Status applyBinlog(const std::list<ReplLog>& txnLog) final;
    Status truncateBinlog(const std::list<ReplLog>& txnLog) final;
    uint64_t getTxnId() const;

 protected:
    virtual void ensureTxn() {};

    uint64_t _txnId;

    // NOTE(deyukong): I believe rocksdb does clean job in txn's destructor
    std::unique_ptr<rocksdb::Transaction> _txn;

    // NOTE(deyukong): not owned by me
    RocksKVStore *_store;

    // TODO(deyukong): it's double buffered in rocks, optimize
    std::vector<ReplLog> _binlogs;

    // if rollback/commit has been explicitly called
    bool _done;

    bool _replOnly;

    std::shared_ptr<BinlogObserver> _logOb;
};

// TODO(deyukong): donot modify store's unCommittedTxn list if
// its only a read-transaction

// NOTE(deyukong): RocksOptTxn does not guarantee thread-safety
// Do not use one RocksOptTxn to do parallel things.
class RocksOptTxn: public RocksTxn {
 public:
    RocksOptTxn(RocksKVStore *store, uint64_t txnId, bool replOnly, std::shared_ptr<BinlogObserver> logob);
    RocksOptTxn(const RocksOptTxn&) = delete;
    RocksOptTxn(RocksOptTxn&&) = delete;
    virtual ~RocksOptTxn() = default;

 protected:
    void ensureTxn() final;
};

class RocksPesTxn: public RocksTxn {
 public:
    RocksPesTxn(RocksKVStore *store, uint64_t txnId, bool replOnly, std::shared_ptr<BinlogObserver> logob);
    RocksPesTxn(const RocksPesTxn&) = delete;
    RocksPesTxn(RocksPesTxn&&) = delete;
    virtual ~RocksPesTxn() = default;

 protected:
    void ensureTxn() final;
};

class RocksKVCursor: public Cursor {
 public:
    explicit RocksKVCursor(std::unique_ptr<rocksdb::Iterator>);
    virtual ~RocksKVCursor() = default;
    void seek(const std::string& prefix) final;
    void seekToLast() final;
    Expected<Record> next() final;

 private:
    std::unique_ptr<rocksdb::Iterator> _it;
};

class RocksKVStore: public KVStore {
 public:
    enum class TxnMode {
        TXN_OPT,
        TXN_PES,
    };

 public:
    RocksKVStore(const std::string& id,
        const std::shared_ptr<ServerParams>& cfg,
        std::shared_ptr<rocksdb::Cache> blockCache,
        TxnMode txnMode = TxnMode::TXN_PES,
        uint64_t maxKeepLogs = 1000000);
    virtual ~RocksKVStore() = default;
    Expected<std::unique_ptr<Transaction>> createTransaction() final;
    Expected<RecordValue> getKV(const RecordKey& key, Transaction* txn) final;
    Status setKV(const Record& kv, Transaction* txn) final;
    Status setKV(const RecordKey& key, const RecordValue& val,
                 Transaction* txn) final;
    Status setKV(const std::string& key, const std::string& val,
                 Transaction *txn) final;
    Status delKV(const RecordKey& key, Transaction* txn) final;
    Status applyBinlog(const std::list<ReplLog>& txnLog,
                       Transaction *txn) final;
    Expected<std::pair<uint64_t, std::list<ReplLog>>> getTruncateLog(
        uint64_t start, uint64_t end, Transaction *txn) final;
    Status truncateBinlog(const std::list<ReplLog>&, Transaction *txn) final;
    Status setLogObserver(std::shared_ptr<BinlogObserver>) final;

    Status clear() final;
    bool isRunning() const final;
    Status stop() final;

    Status setMode(StoreMode mode) final;

    TxnMode getTxnMode() const;

    Expected<uint64_t> restart(bool restore = false) final;

    Expected<BackupInfo> backup(const std::string&, KVStore::BackupMode) final;
    Status releaseBackup() final;

    void appendJSONStat(
            rapidjson::Writer<rapidjson::StringBuffer>&) const final;

    void markCommitted(uint64_t txnId, uint64_t binlogTxnId);
    rocksdb::OptimisticTransactionDB* getUnderlayerOptDB();
    rocksdb::TransactionDB* getUnderlayerPesDB();

    uint64_t getHighestBinlogId() const;

    // NOTE(deyukong): this api is only for debug
    std::set<uint64_t> getUncommittedTxns() const;

 private:
    rocksdb::DB* getBaseDB() const;
    void addUnCommitedTxnInLock(uint64_t txnId);
    void markCommittedInLock(uint64_t txnId, uint64_t binlogTxnId);
    rocksdb::Options options();
    mutable std::mutex _mutex;

    bool _isRunning;
    bool _hasBackup;

    KVStore::StoreMode _mode;

    const TxnMode _txnMode;

    std::unique_ptr<rocksdb::OptimisticTransactionDB> _optdb;
    std::unique_ptr<rocksdb::TransactionDB> _pesdb;

    std::shared_ptr<rocksdb::Statistics> _stats;
    std::shared_ptr<rocksdb::Cache> _blockCache;

    uint64_t _nextTxnSeq;

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
    std::map<uint64_t, std::pair<bool, uint64_t>> _aliveTxns;

    // NOTE(deyukong): _highestVisible is the largest committed binlog
    // before _aliveTxns.begin()
    uint64_t _highestVisible;

    std::shared_ptr<BinlogObserver> _logOb;

    // NOTE(deyukong): currently, we cant get the exact log count by
    // _highestVisible - startLogId, because "readonly" txns also
    // occupy txnIds, and in each txnId, there are more sub operations.
    // So, _maxKeepLogs is not named precisely.
    const uint64_t _maxKeepLogs;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_STORAGE_ROCKS_ROCKS_KVSTORE_H_
