#include <memory>
#include <utility>
#include <string>
#include "tendisplus/storage/rocks/rocks_kvstore.h"

namespace tendisplus {
RocksOptTxn::RocksOptTxn(rocksdb::OptimisticTransactionDB *db)
        :_txn(nullptr),
         _db(db) {
}

Status RocksOptTxn::commit() {
    if (_txn == nullptr) {
        return {ErrorCodes::ERR_OK, ""};
    }
    auto s = _txn->Commit();
    if (s.ok()) {
        return {ErrorCodes::ERR_OK, ""};
    } else if (s.IsBusy() || s.IsTryAgain()) {
        return {ErrorCodes::ERR_COMMIT_RETRY, s.ToString()};
    } else {
        return {ErrorCodes::ERR_INTERNAL, s.ToString()};
    }
}

Status RocksOptTxn::rollback() {
    if (_txn == nullptr) {
        return {ErrorCodes::ERR_OK, ""};
    }
    auto s = _txn->Rollback();
    if (s.ok()) {
        return {ErrorCodes::ERR_OK, ""};
    } else {
        return {ErrorCodes::ERR_INTERNAL, s.ToString()};
    }
}

void RocksOptTxn::ensureTxn() {
    if (_txn != nullptr) {
        return;
    }
    rocksdb::WriteOptions writeOpts;
    rocksdb::OptimisticTransactionOptions txnOpts;

    // NOTE(deyukong): the optimistic_txn won't save a snapshot
    // (mainly for read in our cases) automaticly.
    // We must set_snapshot manually.
    // if set_snapshot == false, the RC-level is guaranteed.
    // if set_snapshot == true, the RR-level is guaranteed.

    // refer to rocks' document, even if set_snapshot is set,
    // the uncommitted data in this txn's writeBatch are still
    // visible to reads.
    txnOpts.set_snapshot = true;
    _txn.reset(_db->BeginTransaction(writeOpts, txnOpts));
    assert(_txn);
}

RocksKVStore::RocksKVStore(const std::string& id,
            const std::shared_ptr<ServerParams>& cfg)
        :KVStore(id) {
    (void)cfg;
}

Expected<std::string> RocksOptTxn::getKV(const std::string& key) {
    ensureTxn();
    rocksdb::ReadOptions readOpts;
    std::string value;
    auto s = _txn->Get(readOpts, key, &value);
    if (s.ok()) {
        return value;
    }
    if (s.IsNotFound()) {
        return {ErrorCodes::ERR_NOTFOUND, s.ToString()};
    }
    return {ErrorCodes::ERR_INTERNAL, s.ToString()};
}

Status RocksOptTxn::setKV(const std::string& key, const std::string& val) {
    ensureTxn();
    auto s = _txn->Put(key, val);
    if (s.ok()) {
        return {ErrorCodes::ERR_OK, ""};
    }
    return {ErrorCodes::ERR_INTERNAL, s.ToString()};
}

Status RocksOptTxn::delKV(const std::string& key) {
    ensureTxn();
    auto s = _txn->Delete(key);
    if (s.ok()) {
        return {ErrorCodes::ERR_OK, ""};
    }
    return {ErrorCodes::ERR_INTERNAL, s.ToString()};
}

Expected<std::unique_ptr<Transaction>> RocksKVStore::createTransaction() {
    return std::unique_ptr<Transaction>(new RocksOptTxn(_db.get()));
}

Expected<std::string> RocksKVStore::getKV(const std::string& key,
        Transaction* txn) {
    // TODO(deyukong): statstics
    return txn->getKV(key);
}

Status RocksKVStore::setKV(const std::string& key, const std::string& val,
        Transaction* txn) {
    // TODO(deyukong): statstics and inmemory-accumulative counter
    return txn->setKV(key, val);
}

Status RocksKVStore::delKV(const std::string& key, Transaction *txn) {
    // TODO(deyukong): statstics and inmemory-accumulative counter
    return txn->delKV(key);
}

}  // namespace tendisplus
