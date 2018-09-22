#ifndef SRC_TENDISPLUS_STORAGE_ROCKS_ROCKS_KVSTORE_H_
#define SRC_TENDISPLUS_STORAGE_ROCKS_ROCKS_KVSTORE_H_

#include <memory>
#include <string>
#include <iostream>
#include <set>
#include <mutex>
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "tendisplus/server/server_params.h"
#include "tendisplus/storage/kvstore.h"

namespace tendisplus {

class RocksKVStore;

// TODO(deyukong): donot modify store's unCommittedTxn list if
// its only a read-transaction

// NOTE(deyukong): RocksOptTxn does not guarantee thread-safety
// Do not use one RocksOptTxn to do parallel things.
class RocksOptTxn: public Transaction {
 public:
    explicit RocksOptTxn(RocksKVStore *store, uint64_t txnId);
    RocksOptTxn(const RocksOptTxn&) = delete;
    RocksOptTxn(RocksOptTxn&&) = delete;
    virtual ~RocksOptTxn();
    std::unique_ptr<Cursor> createCursor() final;
    std::unique_ptr<BinlogCursor> createBinlogCursor(uint64_t begin) final;
    Expected<uint64_t> commit() final;
    Status rollback() final;
    Expected<std::string> getKV(const std::string&) final;
    Status setKV(const std::string& key,
                 const std::string& val,
                 bool withLog) final;
    Status delKV(const std::string& key, bool withLog) final;
    uint64_t getTxnId() const;

 private:
    void ensureTxn();

    uint64_t _txnId;

    // NOTE(deyukong): I believe rocksdb does clean job in txn's destructor
    std::unique_ptr<rocksdb::Transaction> _txn;

    // NOTE(deyukong): not owned by RocksOptTxn
    RocksKVStore *_store;

    // TODO(deyukong): it's double buffered in rocks, optimize 
    std::vector<ReplLog> _binlogs;

    // if rollback/commit has been explicitly called
    bool _done;
};

class RocksKVCursor: public Cursor {
 public:
    explicit RocksKVCursor(std::unique_ptr<rocksdb::Iterator>);
    virtual ~RocksKVCursor() = default;
    void seek(const std::string& prefix) final;
    Expected<Record> next() final;

    // seekToLast currently is not Curosr interface's requirement
    void seekToLast();

 private:
    std::unique_ptr<rocksdb::Iterator> _it;
};

class RocksKVStore: public KVStore {
 public:
    RocksKVStore(const std::string& id,
        const std::shared_ptr<ServerParams>& cfg,
        std::shared_ptr<rocksdb::Cache> blockCache);
    virtual ~RocksKVStore() = default;
    Expected<std::unique_ptr<Transaction>> createTransaction() final;
    Expected<RecordValue> getKV(const RecordKey& key, Transaction* txn) final;
    Status setKV(const Record& kv, Transaction* txn,
                 bool withLog = true) final;
    Status setKV(const RecordKey& key,
                 const RecordValue& val,
                 Transaction* txn,
                 bool withLog = true) final;
    Status setKV(const std::string& key,
                 const std::string& val,
                 Transaction *txn,
                 bool withLog = true) final;
    Status delKV(const RecordKey& key,
                 Transaction* txn,
                 bool withLog = true) final;

    Status clear() final;
    bool isRunning() const final;
    Status stop() final;
    Expected<uint64_t> restart(bool restore = false) final;

    Expected<BackupInfo> backup() final;
    Status releaseBackup() final;

    void appendJSONStat(rapidjson::Writer<rapidjson::StringBuffer>&) const final;

    void markCommitted(uint64_t txnId);
    rocksdb::OptimisticTransactionDB* getUnderlayerDB();

    uint64_t getHighestVisibleTxnId() const;

    // NOTE(deyukong): this api is only for debug
    std::set<uint64_t> getUncommittedTxns() const;

 private:
    void addUnCommitedTxnInLock(uint64_t txnId);
    void markCommittedInLock(uint64_t txnId);
    rocksdb::Options options();
    mutable std::mutex _mutex;

    bool _isRunning;
    bool _hasBackup;
    std::unique_ptr<rocksdb::OptimisticTransactionDB> _db;
    std::shared_ptr<rocksdb::Statistics> _stats;
    std::shared_ptr<rocksdb::Cache> _blockCache;

    uint64_t _nextTxnSeq;

    // NOTE(deyukong): sorted data-structure is required here.
    // we rely on the data order to maintain active txns' watermark.
    // txnid -> committed|uncommited
    // --------------------------------------------------------------
    // when creating txns, a {txnId, false} pair is inserted
    // when txn commits or destroys, the txnId is marked as true
    // means it is committed. As things run parallely, there will
    // be false-holes in _aliveTxns, fortunely, when _aliveTxns.begin()
    // changes from uncommitted to committed, we have a chance to
    // remove all the continous committed txnIds follows it, and
    // push _highestVisible forward.
    std::map<uint64_t, bool> _aliveTxns;

    // NOTE(deyukong): _highestVisible is the largest committedTxnId that
    // for any txn with Id = b is committed, and b > committedTxnId
    // there must exists a uncommitted txn with Id = c, and
    // _highestVisible < c < b.
    // For simple, _highestVisible is the highest TxnId that before this
    // txnId, there wont be any "commit-hole"
    uint64_t _highestVisible;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_STORAGE_ROCKS_ROCKS_KVSTORE_H_
