#include <memory>
#include <utility>
#include <string>
#include <sstream>
#include "glog/logging.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/utilities/backupable_db.h"
#include "rocksdb/options.h"
#include "tendisplus/storage/rocks/rocks_kvstore.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/scopeguard.h"

namespace tendisplus {
RocksOptTxn::RocksOptTxn(RocksKVStore* store, uint64_t txnId)
        :_txnId(txnId),
         _txn(nullptr),
         _store(store),
         _done(false) {
}

Expected<Transaction::CommitId> RocksOptTxn::commit() {
    if (_done) {
        LOG(FATAL) << "BUG: reusing RocksOptTxn";
    }
    _done = true;

    const auto guard = MakeGuard([this] {
        _store->removeUncommited(_txnId);
    });

    if (_txn == nullptr) {
        return {ErrorCodes::ERR_OK, ""};
    }
    TEST_SYNC_POINT("RocksOptTxn::commit()::1");
    TEST_SYNC_POINT("RocksOptTxn::commit()::2");
    auto s = _txn->Commit();
    if (s.ok()) {
        return _txnId;
    } else if (s.IsBusy() || s.IsTryAgain()) {
        return {ErrorCodes::ERR_COMMIT_RETRY, s.ToString()};
    } else {
        return {ErrorCodes::ERR_INTERNAL, s.ToString()};
    }
}

Status RocksOptTxn::rollback() {
    if (_done) {
        LOG(FATAL) << "BUG: reusing RocksOptTxn";
    }
    _done = true;

    const auto guard = MakeGuard([this] {
        _store->removeUncommited(_txnId);
    });

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
    auto db = _store->getUnderlayerDB();
    if (!db) {
        LOG(FATAL) << "BUG: rocksKVStore underLayerDB nil";
    }
    _txn.reset(db->BeginTransaction(writeOpts, txnOpts));
    assert(_txn);
}

uint64_t RocksOptTxn::getTxnId() const {
    return _txnId;
}

RocksOptTxn::~RocksOptTxn() {
    if (_done) {
        return;
    }
    _store->removeUncommited(_txnId);
}

rocksdb::Options RocksKVStore::options() {
    rocksdb::Options options;
    rocksdb::BlockBasedTableOptions table_options;
    table_options.block_cache = _blockCache;
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

    options.max_total_wal_size = uint64_t(4294967296);  // 4GB
    return options;
}

bool RocksKVStore::isRunning() const {
    std::lock_guard<std::mutex> lk(_mutex); 
    return _isRunning;
}

Status RocksKVStore::stop() {
    std::lock_guard<std::mutex> lk(_mutex); 
    if (_uncommitted_txns.size() != 0) {
        return {ErrorCodes::ERR_INTERNAL,
            "it's upperlayer's duty to guarantee no pinning txns alive"};
    }
    _isRunning = false;
    _db.reset();
    return {ErrorCodes::ERR_OK, ""};
}

Status RocksKVStore::clear() {
    std::lock_guard<std::mutex> lk(_mutex); 
    if (_isRunning) {
        return {ErrorCodes::ERR_INTERNAL, "should stop before clear"};
    }
    auto n = filesystem::remove_all(dbPath() + "/" + dbId());
    LOG(INFO) << "dbId:" << dbId() << "cleared " << n << " files/dirs";
    return {ErrorCodes::ERR_OK, ""};
}

Status RocksKVStore::restart(bool restore) {
    // TODO(deyukong): initialize _nextTxnSeq
    std::lock_guard<std::mutex> lk(_mutex); 
    if (_isRunning) {
        return {ErrorCodes::ERR_INTERNAL, "already running"};
    }
    std::string dbname = dbPath() + "/" + dbId();
    if (restore) {
        rocksdb::BackupEngine* bkEngine = nullptr;
        auto s = rocksdb::BackupEngine::Open(rocksdb::Env::Default(),
            rocksdb::BackupableDBOptions(backupDir()),
            &bkEngine);
        if (!s.ok()) {
            return {ErrorCodes::ERR_INTERNAL, s.ToString()};
        }
        std::unique_ptr<rocksdb::BackupEngine> pBkEngine;
        pBkEngine.reset(bkEngine);
        std::vector<rocksdb::BackupInfo> backupInfo;
        pBkEngine->GetBackupInfo(&backupInfo);
        if (backupInfo.size() != 1) {
            LOG(FATAL) << "BUG: backup cnt:" << backupInfo.size()
                << " != 1";
        }
        s = pBkEngine->RestoreDBFromLatestBackup(dbname, dbname);
        if (!s.ok()) {
            return {ErrorCodes::ERR_INTERNAL, s.ToString()};
        }
    }

    try {
        if (filesystem::exists(backupDir())) {
            LOG(WARNING) << backupDir() << " exists, remove it";
            filesystem::remove_all(backupDir());
        }
    } catch (const std::exception& ex) {
        return {ErrorCodes::ERR_INTERNAL, ex.what()};
    }

    rocksdb::OptimisticTransactionDB *tmpDb;
    rocksdb::Options dbOpts = options();
    auto status = rocksdb::OptimisticTransactionDB::Open(
        dbOpts, dbname, &tmpDb);
    if (!status.ok()) {
        return {ErrorCodes::ERR_INTERNAL, status.ToString()};
    }
    _db.reset(tmpDb);
    _isRunning = true;
    return {ErrorCodes::ERR_OK, ""};
}

RocksKVStore::RocksKVStore(const std::string& id,
            const std::shared_ptr<ServerParams>& cfg,
            std::shared_ptr<rocksdb::Cache> blockCache)
        :KVStore(id, cfg->dbPath),
         _isRunning(false),
         _hasBackup(false),
         _db(nullptr),
         _stats(rocksdb::CreateDBStatistics()),
         _blockCache(blockCache) {
    Status s = restart(false);
    if (!s.ok()) {
        LOG(FATAL) << "opendb:" << cfg->dbPath << "/" << id
                    << ", failed info:" << s.toString();
    }
}

Status RocksKVStore::releaseBackup() {
    try {
        if (!filesystem::exists(backupDir())) {
            return {ErrorCodes::ERR_OK, ""};
        }
        filesystem::remove_all(backupDir());
    } catch (const std::exception& ex) {
        LOG(FATAL) << "remove " << backupDir() << " ex:" << ex.what();
    }
    {
        std::lock_guard<std::mutex> lk(_mutex);
        if (_hasBackup) {
            _hasBackup = false;
        }
        return {ErrorCodes::ERR_OK, ""};
    }
}

// this function guarantees that if backup is failed, 
// there should be no remaining dirs left to clean.
Expected<BackupInfo> RocksKVStore::backup() {
    {
        std::lock_guard<std::mutex> lk(_mutex);
        if (_hasBackup) {
            return {ErrorCodes::ERR_INTERNAL, "already have backup"};
        }
        _hasBackup = true;
    }
    bool succ = false;
    rocksdb::BackupEngine* bkEngine;
    auto guard = MakeGuard([this, &succ]() {
        if (succ) {
            return;
        }
        try {
            if (!filesystem::exists(backupDir())) {
                return;
            }
            filesystem::remove_all(backupDir());
        } catch (const std::exception& ex) {
            LOG(FATAL) << "remove " << backupDir() << " ex:" << ex.what();
        }
    });
    auto s = rocksdb::BackupEngine::Open(rocksdb::Env::Default(),
            rocksdb::BackupableDBOptions(backupDir()), &bkEngine);
    if (!s.ok()) {
        return {ErrorCodes::ERR_INTERNAL, s.ToString()};
    }
    std::unique_ptr<rocksdb::BackupEngine> pBkEngine;
    pBkEngine.reset(bkEngine);
    s = pBkEngine->CreateNewBackup(_db->GetBaseDB());
    if (!s.ok()) {
        return {ErrorCodes::ERR_INTERNAL, s.ToString()};
    }
    BackupInfo result;
    std::map<std::string, uint64_t> flist;
    try {
        for (auto& p: filesystem::recursive_directory_iterator(backupDir())) {
            const filesystem::path& path = p.path();
            size_t filesize = filesystem::file_size(path);
            // assert path with backupDir prefix
            assert(path.string().find(backupDir()) == 0);
            std::string relative = path.string().erase(0, backupDir().size());
            flist[relative] = filesize;
        }
    } catch (const std::exception& ex) {
        return {ErrorCodes::ERR_INTERNAL, ex.what()};
    }
    succ = true;
    result.setFileList(flist);
    result.setCommitId(0);
    // TODO(deyukong): fulfill commitId
    return result;
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
    uint64_t txnId = _nextTxnSeq.fetch_add(1);
    auto ret = std::unique_ptr<Transaction>(new RocksOptTxn(this, txnId));
    std::lock_guard<std::mutex> lk(_mutex);
    if (!_isRunning) {
        return {ErrorCodes::ERR_INTERNAL, "db stopped!"};
    }
    addUnCommitedTxnInLock(txnId);
    return std::move(ret);
}

rocksdb::OptimisticTransactionDB* RocksKVStore::getUnderlayerDB() {
    return _db.get();
}

void RocksKVStore::addUnCommitedTxnInLock(uint64_t txnId) {
    // TODO(deyukong): need a better mutex mechnism to assert held
    if (_uncommitted_txns.find(txnId) != _uncommitted_txns.end()) {
        LOG(FATAL) << "BUG: txnid:" << txnId << " double add uncommitted";
    }
    _uncommitted_txns.insert(txnId);
}

void RocksKVStore::removeUncommited(uint64_t txnId) {
    std::lock_guard<std::mutex> lk(_mutex);
    removeUncommitedInLock(txnId);
}

std::set<uint64_t> RocksKVStore::getUncommittedTxns() const {
    std::lock_guard<std::mutex> lk(_mutex);
    return _uncommitted_txns;
}

void RocksKVStore::removeUncommitedInLock(uint64_t txnId) {
    // TODO(deyukong): need a better mutex mechnism to assert held
    if (_uncommitted_txns.find(txnId) == _uncommitted_txns.end()) {
        LOG(FATAL) << "BUG: txnid:" << txnId << " not in uncommitted";
    }
    if (!_isRunning) {
        LOG(FATAL) << "BUG: _uncommitted_txns not empty after stopped";
    }
    _uncommitted_txns.erase(txnId);
}

Expected<RecordValue> RocksKVStore::getKV(const RecordKey& key,
        Transaction *txn) {
    Expected<std::string> s = txn->getKV(key.encode());
    if (!s.ok()) {
        return s.status();
    }
    return RecordValue::decode(s.value());
}

Status RocksKVStore::setKV(const RecordKey& key, const RecordValue& value,
        Transaction *txn) {
    return txn->setKV(key.encode(), value.encode());
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
