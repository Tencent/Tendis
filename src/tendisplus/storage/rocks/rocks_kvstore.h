#ifndef SRC_TENDISPLUS_STORAGE_ROCKS_ROCKS_KVSTORE_H_
#define SRC_TENDISPLUS_STORAGE_ROCKS_ROCKS_KVSTORE_H_

#include <memory>
#include <string>
#include <iostream>
#include <set>
#include <mutex>
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "tendisplus/server/server_params.h"
#include "tendisplus/storage/kvstore.h"

namespace tendisplus {

class RocksKVStore;
class RocksOptTxn: public Transaction {
 public:
    explicit RocksOptTxn(RocksKVStore *store, uint64_t txnId);
    RocksOptTxn(const RocksOptTxn&) = delete;
    RocksOptTxn(RocksOptTxn&&) = delete;
    virtual ~RocksOptTxn();
    Expected<CommitId> commit() final;
    Status rollback() final;
    Expected<std::string> getKV(const std::string&) final;
    Status setKV(const std::string& key, const std::string& val) final;
    Status delKV(const std::string& key) final;
    uint64_t getTxnId() const;

 private:
    void ensureTxn();

    uint64_t _txnId;

    // NOTE(deyukong): I believe rocksdb does clean job in txn's destructor
    std::unique_ptr<rocksdb::Transaction> _txn;

    // NOTE(deyukong): not owned by RocksOptTxn
    RocksKVStore *_store;

    // if rollback/commit has been explicitly called
    bool _done;
};

class RocksKVStore: public KVStore {
 public:
    RocksKVStore(const std::string& id,
        const std::shared_ptr<ServerParams>& cfg,
        std::shared_ptr<rocksdb::Cache> blockCache);
    virtual ~RocksKVStore() = default;
    Expected<std::unique_ptr<Transaction>> createTransaction() final;
    Expected<RecordValue> getKV(const RecordKey& key, Transaction* txn) final;
    Status setKV(const Record& kv, Transaction* txn) final;
    Status setKV(const RecordKey& key, const RecordValue& val, Transaction* txn) final;
    Status delKV(const RecordKey& key, Transaction* txn) final;

    Status clear() final;
    bool isRunning() const final;
    Status stop() final;
    Status restart(bool restore = false) final;

    Expected<BackupInfo> backup() final;
    Status releaseBackup() final;

    void removeUncommited(uint64_t txnId);
    rocksdb::OptimisticTransactionDB* getUnderlayerDB();
    std::set<uint64_t> getUncommittedTxns() const;

 private:
    void addUnCommitedTxnInLock(uint64_t txnId);
    void removeUncommitedInLock(uint64_t txnId);
    rocksdb::Options options();
    mutable std::mutex _mutex;

    bool _isRunning;
    bool _hasBackup;
    std::unique_ptr<rocksdb::OptimisticTransactionDB> _db;
    std::shared_ptr<rocksdb::Statistics> _stats;
    std::shared_ptr<rocksdb::Cache> _blockCache;

    std::atomic<uint64_t> _nextTxnSeq;
    // NOTE(deyukong): sorted data-structure is required here.
    // we rely on the data order to maintain active txns' watermark.
    std::set<uint64_t> _uncommitted_txns;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_STORAGE_ROCKS_ROCKS_KVSTORE_H_
