#ifndef SRC_TENDISPLUS_STORAGE_ROCKS_ROCKS_KVSTORE_H_
#define SRC_TENDISPLUS_STORAGE_ROCKS_ROCKS_KVSTORE_H_

#include <memory>
#include <string>
#include <iostream>
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "tendisplus/server/server_params.h"
#include "tendisplus/storage/kvstore.h"

namespace tendisplus {

class RocksOptTxn: public Transaction {
 public:
    explicit RocksOptTxn(rocksdb::OptimisticTransactionDB *db);
    RocksOptTxn(const RocksOptTxn&) = delete;
    RocksOptTxn(RocksOptTxn&&) = delete;
    virtual ~RocksOptTxn() = default;
    Status commit() final;
    Status rollback() final;
    Expected<std::string> getKV(const std::string&) final;
    Status setKV(const std::string& key, const std::string& val) final;
    Status delKV(const std::string& key) final;

 private:
    void ensureTxn();
    // NOTE(deyukong): I believe rocksdb does clean job in txn's destructor
    std::unique_ptr<rocksdb::Transaction> _txn;

    // NOTE(deyukong): not owned by RocksOptTxn
    rocksdb::OptimisticTransactionDB* _db;
};

class RocksKVStore: public KVStore {
 public:
    RocksKVStore(const std::string& id,
        const std::shared_ptr<ServerParams>& cfg,
        std::shared_ptr<rocksdb::Cache> blockCache);
    virtual ~RocksKVStore() = default;
    Expected<std::unique_ptr<Transaction>> createTransaction() final;
    Expected<std::string> getKV(const std::string& key, Transaction* txn) final;
    Status setKV(const std::string& key, const std::string& val,
        Transaction* txn) final;
    Status delKV(const std::string& key, Transaction* txn) final;

 private:
    std::unique_ptr<rocksdb::OptimisticTransactionDB> _db;
    std::shared_ptr<rocksdb::Statistics> _stats;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_STORAGE_ROCKS_ROCKS_KVSTORE_H_
