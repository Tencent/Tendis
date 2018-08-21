#include <memory>
#include <utility>
#include <string>
#include <sstream>
#include "glog/logging.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
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
    // Of course we need RR-level, not RC-level.

    // refer to rocks' document, even if set_snapshot == true,
    // the uncommitted data in this txn's writeBatch are still
    // visible to reads, and this behavior is what we need.
    txnOpts.set_snapshot = true;
    _txn.reset(_db->BeginTransaction(writeOpts, txnOpts));
    assert(_txn);
}

RocksKVStore::RocksKVStore(const std::string& id,
            const std::shared_ptr<ServerParams>& cfg,
            std::shared_ptr<rocksdb::Cache> blockCache)
        :KVStore(id),
         _db(nullptr),
         _stats(rocksdb::CreateDBStatistics()) {
    rocksdb::Options options;
    rocksdb::BlockBasedTableOptions table_options;
    table_options.block_cache = blockCache;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
    table_options.block_size = 16 * 1024;  // 16KB
    table_options.format_version = 2;
    // let index and filters pining in mem forever
    table_options.cache_index_and_filter_blocks = false;
    options.table_factory.reset(
        rocksdb::NewBlockBasedTableFactory(table_options));
    options.write_buffer_size = 64 * 1024 * 1024;  // 64MB
    // level_0 max size: 8*64MB = 512MB
    options.level0_slowdown_writes_trigger = 8;
    options.max_write_buffer_number = 4;
    options.max_background_compactions = 8;
    options.max_background_flushes = 2;
    options.target_file_size_base = 64 * 1024 * 1024;  // 64MB
    options.level_compaction_dynamic_level_bytes = true;
    // level_1 max size: 512MB, in fact, things are more complex
    // since we set level_compaction_dynamic_level_bytes = true
    options.max_bytes_for_level_base = 512 * 1024 * 1024;  // 512 MB
    options.max_open_files = -1;
    // if we have no 'empty reads', we can disable bottom
    // level's bloomfilters
    options.optimize_filters_for_hits = false;
    // TODO(deyukong): we should have our own compaction factory
    // options.compaction_filter_factory.reset(
    //     new PrefixDeletingCompactionFilterFactory(this));
    options.enable_thread_tracking = true;
    options.compression_per_level.resize(7);
    options.compression_per_level[0] = rocksdb::kNoCompression;
    options.compression_per_level[1] = rocksdb::kNoCompression;
    options.compression_per_level[2] = rocksdb::kSnappyCompression;
    options.compression_per_level[3] = rocksdb::kSnappyCompression;
    options.compression_per_level[4] = rocksdb::kSnappyCompression;
    options.compression_per_level[5] = rocksdb::kSnappyCompression;
    options.compression_per_level[6] = rocksdb::kSnappyCompression;
    options.statistics = _stats;
    options.create_if_missing = true;

    std::stringstream ss;
    ss << cfg->dbPath << "/" << id;
    std::string dbname = ss.str();
    options.wal_dir = dbname + "/journal";
    options.max_total_wal_size = uint64_t(4294967296);  // 4GB
    rocksdb::OptimisticTransactionDB *tmpDb;
    auto status = rocksdb::OptimisticTransactionDB::Open(
        options, dbname, &tmpDb);
    if (!status.ok()) {
        LOG(FATAL) << "opendb:" << dbname
                    << ", failed info:" << status.ToString();
    }
    _db.reset(tmpDb);
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

Expected<RecordValue> RocksKVStore::getKV(const RecordKey& key,
        Transaction *txn) {
    Expected<std::string> s = txn->getKV(key.encode());
    if (!s.ok()) {
        return {s.status().code(), s.status().toString()};
    }
    return RecordValue::decode(s.value());
}

Status RocksKVStore::setKV(const Record& kv, Transaction* txn) {
    // TODO(deyukong): statstics and inmemory-accumulative counter
    Record::KV pair = kv.encode();
    return txn->setKV(pair.first, pair.second);
}

Status RocksKVStore::delKV(const RecordKey& key, Transaction *txn) {
    // TODO(deyukong): statstics and inmemory-accumulative counter
    return txn->delKV(key.encode());
}

}  // namespace tendisplus
