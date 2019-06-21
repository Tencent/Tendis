#include <memory>
#include <utility>
#include <string>
#include <sstream>
#include <map>
#include <vector>
#include <list>
#include <limits>
#include <algorithm>
#include "glog/logging.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/utilities/backupable_db.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/options.h"
#include "tendisplus/storage/rocks/rocks_kvstore.h"
#include "tendisplus/storage/rocks/rocks_kvttlcompactfilter.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/time.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/server/session.h"
#include "tendisplus/server/server_entry.h"
#include "tendisplus/storage/varint.h"

namespace tendisplus {

RocksKVCursor::RocksKVCursor(std::unique_ptr<rocksdb::Iterator> it)
        :Cursor(),
         _it(std::move(it)) {
    _it->Seek("");
}

void RocksKVCursor::seek(const std::string& prefix) {
    _it->Seek(rocksdb::Slice(prefix.c_str(), prefix.size()));
}

void RocksKVCursor::seekToLast() {
    _it->SeekToLast();
}

Expected<Record> RocksKVCursor::next() {
    if (!_it->status().ok()) {
        return {ErrorCodes::ERR_INTERNAL, _it->status().ToString()};
    }
    if (!_it->Valid()) {
        return {ErrorCodes::ERR_EXHAUST, "no more data"};
    }
    const std::string& key = _it->key().ToString();
    const std::string& val = _it->value().ToString();
    auto result = Record::decode(key, val);
    _it->Next();
    if (result.ok()) {
        return std::move(result.value());
    } else {
        LOG(WARNING) << result.status().toString();
    }
    return result.status();
}

Status RocksKVCursor::prev() {
    if (!_it->status().ok()) {
        return {ErrorCodes::ERR_INTERNAL, _it->status().ToString()};
    }

    if (!_it->Valid()) {
        return {ErrorCodes::ERR_EXHAUST, "no more data"};
    }

    _it->Prev();

    return {ErrorCodes::ERR_OK, ""};
}

Expected<std::string> RocksKVCursor::key() {
    if (!_it->status().ok()) {
        return {ErrorCodes::ERR_INTERNAL, _it->status().ToString()};
    }

    if (!_it->Valid()) {
        return {ErrorCodes::ERR_EXHAUST, "no more data"};
    }

    return _it->key().ToString();
}

RocksTxn::RocksTxn(RocksKVStore* store, uint64_t txnId, bool replOnly,
                   std::shared_ptr<tendisplus::BinlogObserver> ob,
                   uint64_t binlogId, uint32_t chunkId)
        :_txnId(txnId),
         _binlogId(binlogId),
         _chunkId(chunkId),
         _txn(nullptr),
         _store(store),
         _done(false),
         _replOnly(replOnly),
         _logOb(ob) {
}

#ifdef BINLOG_V1
std::unique_ptr<BinlogCursor> RocksTxn::createBinlogCursor(
                                uint64_t begin,
                                bool ignoreReadBarrier) {
    auto cursor = createCursor();

    uint64_t hv = Transaction::MAX_VALID_TXNID;
    if (!ignoreReadBarrier) {
        hv = _store->getHighestBinlogId();
    }
    return std::make_unique<BinlogCursor>(std::move(cursor), begin, hv);
}
#else
std::unique_ptr<RepllogCursorV2> RocksTxn::createRepllogCursorV2(
                                uint64_t begin,
                                bool ignoreReadBarrier) {
    uint64_t hv = 0;
    if (!ignoreReadBarrier) {
        hv = _store->getHighestBinlogId();
    } else {
        hv = _store->getNextBinlogSeq() - 1;
    }

    if (begin <= Transaction::MIN_VALID_TXNID) {
        auto k = RepllogCursorV2::getMinBinlogId(this);
        if (!k.ok()) {
            if (k.status().code() == ErrorCodes::ERR_EXHAUST) {
                begin = hv + 1;
            } else {
                LOG(ERROR) << "RepllogCursorV2::getMinBinlogId() ERROR: "
                    << k.status().toString();
                begin = Transaction::TXNID_UNINITED;
            }
        } else {
            begin = k.value();
        }
    }
    return std::make_unique<RepllogCursorV2>(this, begin, hv);
}
#endif

std::unique_ptr<TTLIndexCursor> RocksTxn::createTTLIndexCursor(
    uint64_t until) {
    auto cursor = createCursor();

    return std::make_unique<TTLIndexCursor>(std::move(cursor), until);
}

std::unique_ptr<Cursor> RocksTxn::createCursor() {
    rocksdb::ReadOptions readOpts;
    readOpts.snapshot =  _txn->GetSnapshot();
    rocksdb::Iterator* iter = _txn->GetIterator(readOpts);
    return std::unique_ptr<Cursor>(
        new RocksKVCursor(
            std::move(std::unique_ptr<rocksdb::Iterator>(iter))));
}

Expected<uint64_t> RocksTxn::commit() {
    INVARIANT(!_done);
    _done = true;

    uint64_t binlogTxnId = Transaction::TXNID_UNINITED;
    const auto guard = MakeGuard([this, &binlogTxnId] {
        _txn.reset();
        // for non-replonly mode, we should have binlogTxnId == _txnId
        if (!_replOnly) {
            INVARIANT(binlogTxnId == _txnId
                      || binlogTxnId == Transaction::TXNID_UNINITED);
        }
        _store->markCommitted(_txnId, binlogTxnId);
    });

    if (_txn == nullptr) {
        return {ErrorCodes::ERR_OK, ""};
    }

#ifdef BINLOG_V1
    if (_binlogs.size() != 0) {
        ReplLogKey& key = _binlogs[_binlogs.size()-1].getReplLogKey();
        uint16_t oriFlag = static_cast<uint16_t>(key.getFlag());
        oriFlag |= static_cast<uint16_t>(ReplFlag::REPL_GROUP_END);
        key.setFlag(static_cast<ReplFlag>(oriFlag));
    }
    for (auto& v : _binlogs) {
        auto strPair = v.encode();
        if (binlogTxnId == Transaction::TXNID_UNINITED) {
            binlogTxnId = v.getReplLogKey().getTxnId();
        } else {
            if (binlogTxnId != v.getReplLogKey().getTxnId()) {
                binlogTxnId = Transaction::TXNID_UNINITED;
                return {ErrorCodes::ERR_INTERNAL,
                        "binlogTxnId in one txn not same"};
            }
        }
        auto s = _txn->Put(strPair.first, strPair.second);
        if (!s.ok()) {
            binlogTxnId = Transaction::TXNID_UNINITED;
            return {ErrorCodes::ERR_INTERNAL, s.ToString()};
        }
    }
#else
    if (_replLogValues.size() != 0) {
        // NOTE(vinchen): for repl, binlog should be inserted by setBinlogKV()
        INVARIANT(!isReplOnly());

        _store->assignBinlogIdIfNeeded(this);
        INVARIANT(_binlogId != Transaction::TXNID_UNINITED);

        uint32_t chunkId = getChunkId();

        // TODO(vinchen): Now, one transaction one repllog, so the flag is
        // (START | END)
        uint16_t oriFlag = static_cast<uint16_t>(ReplFlag::REPL_GROUP_START)
            | static_cast<uint16_t>(ReplFlag::REPL_GROUP_END);

        ReplLogKeyV2 key(_binlogId);
        // TODO(vinchen): versionEp should get from session
        ReplLogValueV2 val(chunkId, static_cast<ReplFlag>(oriFlag), _txnId,
            _replLogValues.back().getTimestamp(),
            0,  // versionEp
            nullptr, 0);

        binlogTxnId = _txnId;
        auto s = _txn->Put(key.encode(), val.encode(_replLogValues));
        if (!s.ok()) {
            binlogTxnId = Transaction::TXNID_UNINITED;
            return{ ErrorCodes::ERR_INTERNAL, s.ToString() };
        }
    }
    if (isReplOnly() && _binlogId != Transaction::TXNID_UNINITED) {
        // NOTE(vinchen): for slave, binlog form master store directly
        binlogTxnId = _txnId;
    }
#endif

    TEST_SYNC_POINT("RocksTxn::commit()::1");
    TEST_SYNC_POINT("RocksTxn::commit()::2");
    auto s = _txn->Commit();
    if (s.ok()) {
#ifdef BINLOG_V1
        if (_logOb) {
            _logOb->onCommit(_binlogs);
        }
#endif
        return _txnId;
    } else {
        binlogTxnId = Transaction::TXNID_UNINITED;
        if (s.IsBusy() || s.IsTryAgain()) {
            return {ErrorCodes::ERR_COMMIT_RETRY, s.ToString()};
        } else {
            return {ErrorCodes::ERR_INTERNAL, s.ToString()};
        }
    }
}

Status RocksTxn::rollback() {
    INVARIANT(!_done);
    _done = true;

    const auto guard = MakeGuard([this] {
        _txn.reset();
        _store->markCommitted(_txnId, Transaction::TXNID_UNINITED);
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

uint64_t RocksTxn::getTxnId() const {
    return _txnId;
}

void RocksTxn::setChunkId(uint32_t chunkId) {
    if (_chunkId == Transaction::CHUNKID_UNINITED) {
        _chunkId = chunkId;
    } else if (_chunkId != chunkId) {
        _chunkId = Transaction::CHUNKID_MULTI;
    }
}

Expected<std::string> RocksTxn::getKV(const std::string& key) {
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

Status RocksTxn::setKV(const std::string& key,
                       const std::string& val,
                       const uint64_t ts) {
    if (_replOnly) {
        return {ErrorCodes::ERR_INTERNAL, "txn is replOnly"};
    }

    auto s = _txn->Put(key, val);
    if (!s.ok()) {
        return {ErrorCodes::ERR_INTERNAL, s.ToString()};
    }

#ifdef BINLOG_V1
    if (_binlogs.size() >= std::numeric_limits<uint16_t>::max()) {
        return {ErrorCodes::ERR_BUSY, "txn max ops reached"};
    }
    ReplLogKey logKey(_txnId, _binlogs.size(),
                ReplFlag::REPL_GROUP_MID, ts ? ts : msSinceEpoch());
    ReplLogValue logVal(ReplOp::REPL_OP_SET, key, val);
    if (_binlogs.size() == 0) {
        uint16_t oriFlag = static_cast<uint16_t>(logKey.getFlag());
        oriFlag |= static_cast<uint16_t>(ReplFlag::REPL_GROUP_START);
        logKey.setFlag(static_cast<ReplFlag>(oriFlag));
    }
    _binlogs.emplace_back(
            std::move(
                ReplLog(std::move(logKey), std::move(logVal))));
#else
    setChunkId(RecordKey::decodeChunkId(key));
    if (_replLogValues.size() >= std::numeric_limits<uint16_t>::max()) {
        // TODO(vinchen): if too large, it can flush to rocksdb first, and get
        // another binlogid using assignBinlogIdIfNeeded()

        LOG(WARNING) << "too big binlog size";
    }

    ReplLogValueEntryV2 logVal(ReplOp::REPL_OP_SET, ts ? ts : msSinceEpoch(),
                key, val);
    _replLogValues.emplace_back(std::move(logVal));

#endif
    return {ErrorCodes::ERR_OK, ""};
}

Status RocksTxn::delKV(const std::string& key, const uint64_t ts) {
    if (_replOnly) {
        return {ErrorCodes::ERR_INTERNAL, "txn is replOnly"};
    }
    auto s = _txn->Delete(key);
    if (!s.ok()) {
        return {ErrorCodes::ERR_INTERNAL, s.ToString()};
    }

#ifdef BINLOG_V1
    if (_binlogs.size() >= std::numeric_limits<uint16_t>::max()) {
        return {ErrorCodes::ERR_BUSY, "txn max ops reached"};
    }
    ReplLogKey logKey(_txnId, _binlogs.size(),
                ReplFlag::REPL_GROUP_MID, ts ? ts : msSinceEpoch());
    ReplLogValue logVal(ReplOp::REPL_OP_DEL, key, "");
    if (_binlogs.size() == 0) {
        uint16_t oriFlag = static_cast<uint16_t>(logKey.getFlag());
        oriFlag |= static_cast<uint16_t>(ReplFlag::REPL_GROUP_START);
        logKey.setFlag(static_cast<ReplFlag>(oriFlag));
    }
    _binlogs.emplace_back(
        std::move(
            ReplLog(std::move(logKey), std::move(logVal))));
#else
    setChunkId(RecordKey::decodeChunkId(key));
    if (_replLogValues.size() >= std::numeric_limits<uint16_t>::max()) {
        // TODO(vinchen): if too large, it can flush to rocksdb first, and get
        // another binlogid using assignBinlogIdIfNeeded()

        LOG(WARNING) << "too big binlog size";
    }
    ReplLogValueEntryV2 logVal(ReplOp::REPL_OP_DEL, ts ? ts : msSinceEpoch(),
                key, "");
    _replLogValues.emplace_back(std::move(logVal));
#endif
    return {ErrorCodes::ERR_OK, ""};
}

#ifdef BINLOG_V1
Status RocksTxn::truncateBinlog(const std::list<ReplLog>& ops) {
    for (const auto& v : ops) {
        auto s = _txn->Delete(v.getReplLogKey().encode());
        if (!s.ok()) {
            return {ErrorCodes::ERR_INTERNAL, s.ToString()};
        }
    }
    return {ErrorCodes::ERR_OK, ""};
}

Status RocksTxn::applyBinlog(const std::list<ReplLog>& ops) {
    if (!_replOnly) {
        return {ErrorCodes::ERR_INTERNAL, "txn is not replOnly"};
    }
    for (const auto& log : ops) {
        const ReplLogValue& logVal = log.getReplLogValue();
        uint64_t timestamp = log.getReplLogKey().getTimestamp();
        this->setBinlogTime(timestamp);

        Expected<RecordKey> expRk = RecordKey::decode(logVal.getOpKey());
        if (!expRk.ok()) {
            return expRk.status();
        }

        auto strPair = log.encode();
        // write binlog
        _binlogs.emplace_back(log);
        switch (logVal.getOp()) {
            case (ReplOp::REPL_OP_SET): {
                Expected<RecordValue> expRv =
                    RecordValue::decode(logVal.getOpValue());
                if (!expRv.ok()) {
                    return expRv.status();
                }
                auto s = _txn->Put(expRk.value().encode(),
                                   expRv.value().encode());
                if (!s.ok()) {
                    return {ErrorCodes::ERR_INTERNAL, s.ToString()};
                } else {
                    break;
                }
            }
            case (ReplOp::REPL_OP_DEL): {
                auto s = _txn->Delete(expRk.value().encode());
                if (!s.ok()) {
                    return {ErrorCodes::ERR_INTERNAL, s.ToString()};
                } else {
                    break;
                }
            }
            default: {
                LOG(FATAL) << "invalid binlogOp:"
                            << static_cast<uint8_t>(logVal.getOp());
            }
        }
    }
    return {ErrorCodes::ERR_OK, ""};
}

#else
Status RocksTxn::applyBinlog(const ReplLogValueEntryV2& logEntry) {
    if (!_replOnly) {
        return{ ErrorCodes::ERR_INTERNAL, "txn is not replOnly" };
    }
    switch (logEntry.getOp()) {
    case ReplOp::REPL_OP_SET: {
        // TODO(vinchen): RecordKey::validate()
        auto s = _txn->Put(logEntry.getOpKey(), logEntry.getOpValue());
        if (!s.ok()) {
            return{ ErrorCodes::ERR_INTERNAL, s.ToString() };
        }
        break;
    }
    case ReplOp::REPL_OP_DEL: {
        auto s = _txn->Delete(logEntry.getOpKey());
        if (!s.ok()) {
            return{ ErrorCodes::ERR_INTERNAL, s.ToString() };
        }
        break;
    }
    default:
        INVARIANT(0);
        return{ ErrorCodes::ERR_DECODE, "not a valid binlog" };
    }

    return{ ErrorCodes::ERR_OK, "" };
}

Status RocksTxn::setBinlogKV(uint64_t binlogId,
    const std::string& logKey, const std::string& logValue) {
    if (!_replOnly) {
        return{ ErrorCodes::ERR_INTERNAL, "txn is not replOnly" };
    }

    // NOTE(vinchen): Because the (logKey, logValue) from the master store in
    // slave's rocksdb directly, we should change the _nextBinlogSeq.
    // BTW, the txnid of logValue is different from _txnId. But it's ok.
    _store->setNextBinlogSeq(binlogId, this);
    INVARIANT(_binlogId != Transaction::TXNID_UNINITED);

    auto s = _txn->Put(logKey, logValue);
    if (!s.ok()) {
        return{ ErrorCodes::ERR_INTERNAL, s.ToString() };
    }

    return{ ErrorCodes::ERR_OK, "" };
}

Status RocksTxn::delBinlog(const ReplLogRawV2& log) {
    auto s = _txn->Delete(log.getReplLogKey());
    if (!s.ok()) {
        return{ ErrorCodes::ERR_INTERNAL, s.ToString() };
    }

    return{ ErrorCodes::ERR_OK, "" };
}

uint64_t RocksTxn::getBinlogId() const {
    return _binlogId;
}

void RocksTxn::setBinlogId(uint64_t binlogId) {
    INVARIANT(_binlogId == Transaction::TXNID_UNINITED);
    _binlogId = binlogId;
}

#endif

void RocksTxn::setBinlogTime(uint64_t timestamp) {
    INVARIANT(_store->getMode() == KVStore::StoreMode::REPLICATE_ONLY);

    _binlogTimeSpov = timestamp > _binlogTimeSpov ?
        timestamp : _binlogTimeSpov;
}

RocksTxn::~RocksTxn() {
    if (_done) {
        return;
    }
    _txn.reset();
    _store->markCommitted(_txnId, Transaction::TXNID_UNINITED);
}

RocksOptTxn::RocksOptTxn(RocksKVStore* store, uint64_t txnId, bool replOnly,
            std::shared_ptr<tendisplus::BinlogObserver> ob)
    :RocksTxn(store, txnId, replOnly, ob) {
    // NOTE(deyukong): the rocks-layer's snapshot should be opened in
    // RocksKVStore::createTransaction, with the guard of RocksKVStore::_mutex,
    // or, we are not able to guarantee the oplog order is the same as the
    // local commit,
    // In other words, to the same key, a txn with greater id can be committed
    // before a txn with smaller id, and they have no conflicts, it's wrong.
    // so ensureTxn() should be done in RocksOptTxn's constructor
    ensureTxn();
}

void RocksOptTxn::ensureTxn() {
    INVARIANT(!_done);
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
    auto db = _store->getUnderlayerOptDB();
    if (!db) {
        LOG(FATAL) << "BUG: rocksKVStore underLayerDB nil";
    }
    _txn.reset(db->BeginTransaction(writeOpts, txnOpts));
    INVARIANT(_txn != nullptr);
}

RocksPesTxn::RocksPesTxn(RocksKVStore *store, uint64_t txnId, bool replOnly,
            std::shared_ptr<BinlogObserver> ob)
    :RocksTxn(store, txnId, replOnly, ob) {
    // NOTE(deyukong): the rocks-layer's snapshot should be opened in
    // RocksKVStore::createTransaction, with the guard of RocksKVStore::_mutex,
    // or, we are not able to guarantee the oplog order is the same as the
    // local commit,
    // In other words, to the same key, a txn with greater id can be committed
    // before a txn with smaller id, and they have no conflicts, it's wrong.
    // so ensureTxn() should be done in RocksOptTxn's constructor
    ensureTxn();
}

void RocksPesTxn::ensureTxn() {
    INVARIANT(!_done);
    if (_txn != nullptr) {
        return;
    }
    rocksdb::WriteOptions writeOpts;
    rocksdb::TransactionOptions txnOpts;

    // NOTE(deyukong): the txn won't set a snapshot automaticly.
    // if set_snapshot == false, the RC-level is guaranteed.
    // if set_snapshot == true, the SI-level is guaranteed.
    // due to server-layer's keylock, RC-level can satisfy our
    // requirements. so here set_snapshot = false
    txnOpts.set_snapshot = false;
    auto db = _store->getUnderlayerPesDB();
    if (!db) {
        LOG(FATAL) << "BUG: rocksKVStore underLayerDB nil";
    }
    _txn.reset(db->BeginTransaction(writeOpts, txnOpts));
    INVARIANT(_txn != nullptr);
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
    options.max_write_buffer_number_to_maintain = 1;
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

    if (_enableFilter && dbId() != CATALOG_NAME) {
        // setup the ttlcompactionfilter expect "catalog" db
        options.compaction_filter_factory.reset(
            new KVTtlCompactionFilterFactory(this));
    }
    return options;
}

bool RocksKVStore::isRunning() const {
    std::lock_guard<std::mutex> lk(_mutex);
    return _isRunning;
}

bool RocksKVStore::isPaused() const {
    // std::lock_guard<std::mutex> lk(_mutex);
    return _isPaused;
}

bool RocksKVStore::isEmpty() const {
    std::lock_guard<std::mutex> lk(_mutex);

    // TODO(vinchen)
    return false;
}

Status RocksKVStore::pause() {
    std::lock_guard<std::mutex> lk(_mutex);
    if (_aliveTxns.size() != 0) {
        return{ ErrorCodes::ERR_INTERNAL,
            "it's upperlayer's duty to guarantee no pinning txns alive" };
    }

    _isPaused = true;
    return {ErrorCodes::ERR_OK, ""};
}

Status RocksKVStore::resume() {
    std::lock_guard<std::mutex> lk(_mutex);
    if (_aliveTxns.size() != 0) {
        return{ ErrorCodes::ERR_INTERNAL,
            "it's upperlayer's duty to guarantee no pinning txns alive" };
    }

    _isPaused = false;
    return{ ErrorCodes::ERR_OK, "" };
}

Status RocksKVStore::stop() {
    std::lock_guard<std::mutex> lk(_mutex);
    if (_aliveTxns.size() != 0) {
        return {ErrorCodes::ERR_INTERNAL,
            "it's upperlayer's duty to guarantee no pinning txns alive"};
    }
    _isRunning = false;
    _optdb.reset();
    _pesdb.reset();
    return {ErrorCodes::ERR_OK, ""};
}

Status RocksKVStore::destroy() {
    Status status;
    if (_isRunning) {
        status = stop();
        if (!status.ok()) {
            return status;
        }
    }

    _mode = KVStore::StoreMode::STORE_NONE;
    status = clear();

    return status;
}

Status RocksKVStore::setMode(StoreMode mode) {
    std::lock_guard<std::mutex> lk(_mutex);
    if (_aliveTxns.size() != 0) {
        return {ErrorCodes::ERR_INTERNAL,
            "it's upperlayer's duty to guarantee no pinning txns alive"};
    }
    if (_mode == mode) {
        return {ErrorCodes::ERR_OK, ""};
    }
    uint64_t oldSeq = _nextTxnSeq;
    switch (mode) {
    case KVStore::StoreMode::READ_WRITE:
        INVARIANT(_mode == KVStore::StoreMode::REPLICATE_ONLY);
        // in READ_WRITE mode, the binlog's key is identified by _nextTxnSeq,
        // in REPLICATE_ONLY mode, the binlog is same as the sync-source's
        // when changing from REPLICATE_ONLY to READ_WRITE mode, we shrink
        // _nextTxnSeq so that binlog's wont' be duplicated.
        if (_nextTxnSeq <= _highestVisible) {
            _nextTxnSeq = _highestVisible+1;
        }
        break;

    case KVStore::StoreMode::REPLICATE_ONLY:
    case KVStore::StoreMode::STORE_NONE:
        INVARIANT(_mode == KVStore::StoreMode::READ_WRITE);
        break;
    default:
        INVARIANT(0);
    }

    LOG(INFO) << "store:" << dbId()
              << ",mode:" << static_cast<uint32_t>(_mode)
              << ",changes to:" << static_cast<uint32_t>(mode)
              << ",_nextTxnSeq:" << oldSeq
              << ",changes to:" << _nextTxnSeq;
    _mode = mode;
    return {ErrorCodes::ERR_OK, ""};
}

RocksKVStore::TxnMode RocksKVStore::getTxnMode() const {
    return _txnMode;
}

#ifdef BINLOG_V1
Expected<std::pair<uint64_t, std::list<ReplLog>>>
RocksKVStore::getTruncateLog(uint64_t start, uint64_t end,
                             Transaction *txn) {
    // NOTE(deyukong): precheck non-io operations to reduce io
    uint64_t gap = getHighestBinlogId() - start;
    if (gap < _maxKeepLogs) {
        return std::pair<uint64_t, std::list<ReplLog>>(start, {});
    }

    std::unique_ptr<BinlogCursor> cursor =
        txn->createBinlogCursor(Transaction::MIN_VALID_TXNID);
    Expected<ReplLog> explog = cursor->next();
    if (!explog.ok()) {
        if (explog.status().code() == ErrorCodes::ERR_EXHAUST) {
            // NOTE(deyukong): perhaps INVARIANT here is better
            if (start > Transaction::TXNID_UNINITED) {
                return {ErrorCodes::ERR_INTERNAL, "invalid start binlog"};
            }
            return std::pair<uint64_t, std::list<ReplLog>>(
                Transaction::TXNID_UNINITED, {});
        }
        return explog.status();
    }
    if (start != explog.value().getReplLogKey().getTxnId() &&
        start != Transaction::TXNID_UNINITED) {
        return {ErrorCodes::ERR_INTERNAL, "invalid start binlog"};
    }
    start = explog.value().getReplLogKey().getTxnId();

    // TODO(deyukong): put 10000 into configuration.
    uint64_t cnt = std::min((uint64_t)10000, gap - _maxKeepLogs);
    std::list<ReplLog> toDelete;
    toDelete.push_back(std::move(explog.value()));
    uint64_t nowTxnId = start;
    uint64_t nextStart = start;
    while (true) {
        Expected<ReplLog> explog = cursor->next();
        if (!explog.ok()) {
            if (explog.status().code() == ErrorCodes::ERR_EXHAUST) {
                break;
            }
            return explog.status();
        }
        nextStart = explog.value().getReplLogKey().getTxnId();
        if (nextStart > end) {
            break;
        }
        if (nextStart == nowTxnId) {
            toDelete.push_back(std::move(explog.value()));
        } else if (toDelete.size() >= cnt) {
            break;
        } else {
            nowTxnId = nextStart;
            toDelete.push_back(std::move(explog.value()));
        }
    }
    if (nextStart == start) {
        return std::pair<uint64_t, std::list<ReplLog>>(start, {});
    }
    for (auto v = toDelete.begin(); v != toDelete.end();) {
        if (v->getReplLogKey().getTxnId() == nextStart) {
            v = toDelete.erase(v);
        } else {
            ++v;
        }
    }
    return std::pair<uint64_t, std::list<ReplLog>>(nextStart,
        std::move(toDelete));
}

Status RocksKVStore::truncateBinlog(const std::list<ReplLog>& toDelete,
                                    Transaction *txn) {
    return txn->truncateBinlog(toDelete);
}

Status RocksKVStore::applyBinlog(const std::list<ReplLog>& txnLog,
                                 Transaction *txn) {
    return txn->applyBinlog(txnLog);
}
#else
// keylen(4) + key + vallen(4) + value
uint64_t RocksKVStore::saveBinlogV2(std::ofstream* fs,
                const ReplLogRawV2& log) {
    uint64_t written = 0;
    INVARIANT_D(fs != nullptr);

    uint32_t keyLen = log.getReplLogKey().size();
    uint32_t keyLenTrans = int32Encode(keyLen);
    fs->write(reinterpret_cast<char*>(&keyLenTrans), sizeof(keyLenTrans));
    fs->write(log.getReplLogKey().c_str(), keyLen);

    uint32_t valLen = log.getReplLogValue().size();
    uint32_t valLenTrans = int32Encode(valLen);
    fs->write(reinterpret_cast<char*>(&valLenTrans), sizeof(valLenTrans));
    fs->write(log.getReplLogValue().c_str(), valLen);
    written += keyLen + valLen + sizeof(keyLen) + sizeof(valLen);

    INVARIANT_D(fs->good());

    return written;
}

Expected<TruncateBinlogResult> RocksKVStore::truncateBinlogV2(uint64_t start,
    uint64_t end, Transaction *txn, std::ofstream *fs) {
    // not precise, but fast (gap >= getBinlogCnt())
    uint64_t gap = getHighestBinlogId() - start + 1;
    TruncateBinlogResult result;
    uint64_t ts = 0;
    uint64_t written = 0;
    uint64_t deleten = 0;
    if (gap < _maxKeepLogs) {
        result.newStart = start;
        return result;
    }

    INVARIANT_D(RepllogCursorV2::getMinBinlogId(txn).value() == start);

    auto cursor = txn->createRepllogCursorV2(start);

    // TODO(deyukong): put 1000 into configuration.
    uint64_t cnt = std::min((uint64_t)1000, gap - _maxKeepLogs);
    uint64_t size = 0;
    uint64_t nextStart = start;
    while (true) {
        auto explog = cursor->next();
        if (!explog.ok()) {
            if (explog.status().code() == ErrorCodes::ERR_EXHAUST) {
                break;
            }
            return explog.status();
        }
        nextStart = explog.value().getBinlogId();
        if (nextStart > end ||
            size >= cnt) {
            break;
        }

        ts = explog.value().getTimestamp();
        size++;
        if (fs) {
            // save binlog
            written += saveBinlogV2(fs, explog.value());
        }

        // TODO(vinchen): compactrange or compactfilter should be better
        auto s = txn->delBinlog(explog.value());
        if (!s.ok()) {
            // NOTE(vinchen): if error here, binlog would be wrong because
            // saveBinlogV2() can't be rollbacked;
            LOG(ERROR) << "delbinlog error:" << s.toString();
            return s;
        }
        deleten++;
    }

    result.deleten = deleten;
    result.written = written;
    result.timestamp = ts;
    result.newStart = nextStart;

    return result;
}

Expected<uint64_t> RocksKVStore::getBinlogCnt(Transaction* txn) const {
    auto bcursor = txn->createRepllogCursorV2(Transaction::MIN_VALID_TXNID,
        true);
    uint64_t cnt = 0;
    while (true) {
        auto v = bcursor->next();
        if (!v.ok()) {
            if (v.status().code() == ErrorCodes::ERR_EXHAUST)
                break;

            return v.status();
        }
        cnt += 1;
    }
    return cnt;
}
Expected<bool> RocksKVStore::validateAllBinlog(Transaction* txn) const {
    auto bcursor = txn->createRepllogCursorV2(Transaction::MIN_VALID_TXNID,
        true);
    while (true) {
        auto v = bcursor->nextV2();
        if (!v.ok()) {
            if (v.status().code() == ErrorCodes::ERR_EXHAUST)
                break;

            return v.status();
        }
    }
    return true;
}

#endif

Status RocksKVStore::setLogObserver(std::shared_ptr<BinlogObserver> ob) {
    std::lock_guard<std::mutex> lk(_mutex);
    if (!_isRunning) {
        return {ErrorCodes::ERR_OK, ""};
    }
    if (_logOb != nullptr) {
        return {ErrorCodes::ERR_INTERNAL, "logOb already exists"};
    }
    _logOb = ob;
    return {ErrorCodes::ERR_OK, ""};
}

Status RocksKVStore::compactRange(const std::string* begin,
    const std::string* end) {
    // TODO(vinchen): need lock_guard?
    auto compactionOptions = rocksdb::CompactRangeOptions();
    auto db =  getBaseDB();

    rocksdb::Slice* sbegin = nullptr;
    rocksdb::Slice* send = nullptr;
    const auto guard = MakeGuard([&] {
        if (sbegin) {
            delete sbegin;
        }
        if (send) {
            delete send;
        }
    });
    if (begin != nullptr) {
        sbegin = new rocksdb::Slice(*begin);
    }
    if (end != nullptr) {
        send = new rocksdb::Slice(*end);
    }
    rocksdb::Status status =  db->CompactRange(compactionOptions,
                sbegin, send);
    if (!status.ok()) {
        return {ErrorCodes::ERR_INTERNAL, status.getState()};
    }
    return {ErrorCodes::ERR_OK, ""};
}

Status RocksKVStore::fullCompact() {
    return compactRange(nullptr, nullptr);
}

Status RocksKVStore::clear() {
    std::lock_guard<std::mutex> lk(_mutex);
    if (_isRunning) {
        return {ErrorCodes::ERR_INTERNAL, "should stop before clear"};
    }
    try {
        const std::string path = dbPath() + "/" + dbId();
        if (!filesystem::exists(path)) {
            return {ErrorCodes::ERR_OK, ""};
        }
        auto n = filesystem::remove_all(dbPath() + "/" + dbId());
        LOG(INFO) << "dbId:" << dbId() << " cleared " << n << " files/dirs";
    } catch(std::exception& ex) {
        LOG(WARNING) << "dbId:" << dbId()
                     << " clear failed:" << ex.what();
        return {ErrorCodes::ERR_INTERNAL, ex.what()};
    }
    return {ErrorCodes::ERR_OK, ""};
}

Expected<uint64_t> RocksKVStore::restart(bool restore) {
    std::lock_guard<std::mutex> lk(_mutex);
    if (_isRunning) {
        return {ErrorCodes::ERR_INTERNAL, "already running"};
    }

    // NOTE(vinchen): if stateMode is STORE_NONE, the store no need
    // to open in rocksdb layer.
    if (getMode() == KVStore::StoreMode::STORE_NONE) {
        return {ErrorCodes::ERR_OK, "" };
    }

    std::string dbname = dbPath() + "/" + dbId();
    if (restore) {
        try {
            const std::string path = dbPath() + "/" + dbId();
            if (filesystem::exists(path)) {
                std::stringstream ss;
                ss << "path:" << path
                    << " should not exist when restore";
                return {ErrorCodes::ERR_INTERNAL, ss.str()};
            }
            if (!filesystem::exists(dftBackupDir())) {
                std::stringstream ss;
                ss << "recover path:" << dftBackupDir()
                    << " not exist when restore";
                return {ErrorCodes::ERR_INTERNAL, ss.str()};
            }
            filesystem::rename(dftBackupDir(), path);
        } catch(std::exception& ex) {
            LOG(WARNING) << "dbId:" << dbId()
                        << "restore exception" << ex.what();
            return {ErrorCodes::ERR_INTERNAL, ex.what()};
        }
    }

    try {
        // this happens due to a bad terminate
        if (filesystem::exists(dftBackupDir())) {
            LOG(WARNING) << dftBackupDir() << " exists, remove it";
            filesystem::remove_all(dftBackupDir());
        }
    } catch (const std::exception& ex) {
        return {ErrorCodes::ERR_INTERNAL, ex.what()};
    }

    std::unique_ptr<rocksdb::Iterator> iter = nullptr;
    if (_txnMode == TxnMode::TXN_OPT) {
        rocksdb::OptimisticTransactionDB *tmpDb;
        rocksdb::Options dbOpts = options();
        auto status = rocksdb::OptimisticTransactionDB::Open(
            dbOpts, dbname, &tmpDb);
        if (!status.ok()) {
            return {ErrorCodes::ERR_INTERNAL, status.ToString()};
        }
        rocksdb::ReadOptions readOpts;
        iter.reset(tmpDb->GetBaseDB()->NewIterator(readOpts));
        _optdb.reset(tmpDb);
    } else {
        rocksdb::TransactionDB* tmpDb;
        rocksdb::TransactionDBOptions txnDbOptions;
        // txnDbOptions.max_num_locks unlimit
        // txnDbOptions.transaction_lock_timeout 1sec
        // txnDbOptions.default_lock_timeout 1sec
        // txnDbOptions.write_policy WRITE_COMMITTED
        rocksdb::Options dbOpts = options();
        auto status = rocksdb::TransactionDB::Open(
            dbOpts, txnDbOptions, dbname, &tmpDb);
        if (!status.ok()) {
            return {ErrorCodes::ERR_INTERNAL, status.ToString()};
        }
        rocksdb::ReadOptions readOpts;
        iter.reset(tmpDb->GetBaseDB()->NewIterator(readOpts));
        _pesdb.reset(tmpDb);
    }
    // NOTE(deyukong): during starttime, mutex is held and
    // no need to consider visibility
    // TODO(deyukong): use BinlogCursor to rewrite
    RocksKVCursor cursor(std::move(iter));

    cursor.seekToLast();
    Expected<Record> expRcd = cursor.next();

    uint64_t maxCommitId = Transaction::TXNID_UNINITED;
#ifdef BINLOG_V1
    if (expRcd.ok()) {
        const RecordKey& rk = expRcd.value().getRecordKey();
        if (rk.getRecordType() == RecordType::RT_BINLOG) {
            auto explk = ReplLogKey::decode(rk);
            if (!explk.ok()) {
                return explk.status();
            } else {
                LOG(INFO) << "store:" << dbId()
                          << " nextSeq change from:" << _nextTxnSeq
                          << " to:" << explk.value().getTxnId() + 1;
                maxCommitId = explk.value().getTxnId();
                _nextTxnSeq = maxCommitId+1;
                _highestVisible = maxCommitId;
            }
        } else {
            _nextTxnSeq = Transaction::MIN_VALID_TXNID;
            LOG(INFO) << "store:" << dbId() << ' ' << rk.getPrimaryKey()
                    << " have no binlog, set nextSeq to " << _nextTxnSeq;
            _highestVisible = Transaction::TXNID_UNINITED;
            INVARIANT(_highestVisible < _nextTxnSeq);
        }
    } else if (expRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
        _nextTxnSeq = Transaction::MIN_VALID_TXNID;
        LOG(INFO) << "store:" << dbId()
                  << " all empty, set nextSeq to " << _nextTxnSeq;
        _highestVisible = Transaction::TXNID_UNINITED;
        INVARIANT(_highestVisible < _nextTxnSeq);
    } else {
        return expRcd.status();
    }
#else
    if (expRcd.ok()) {
        const RecordKey& rk = expRcd.value().getRecordKey();
        if (rk.getRecordType() == RecordType::RT_BINLOG) {
            auto explk = ReplLogKeyV2::decode(rk);
            if (!explk.ok()) {
                return explk.status();
            } else {
                auto binlogId = explk.value().getBinlogId();
                LOG(INFO) << "store:" << dbId()
                    << " nextSeq change from:" << _nextTxnSeq
                    << " to:" << binlogId + 1;
                maxCommitId = binlogId;
                _nextTxnSeq = maxCommitId + 1;
                _nextBinlogSeq = _nextTxnSeq;
                _highestVisible = maxCommitId;
            }
        } else {
            _nextTxnSeq = Transaction::MIN_VALID_TXNID;
            _nextBinlogSeq = _nextTxnSeq;
            LOG(INFO) << "store:" << dbId() << ' ' << rk.getPrimaryKey()
                << " have no binlog, set nextSeq to " << _nextTxnSeq;
            _highestVisible = Transaction::TXNID_UNINITED;
            INVARIANT(_highestVisible < _nextTxnSeq);
        }
    } else if (expRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
        _nextTxnSeq = Transaction::MIN_VALID_TXNID;
        _nextBinlogSeq = _nextTxnSeq;
        LOG(INFO) << "store:" << dbId()
            << " all empty, set nextSeq to " << _nextTxnSeq;
        _highestVisible = Transaction::TXNID_UNINITED;
        INVARIANT(_highestVisible < _nextTxnSeq);
    } else {
        return expRcd.status();
    }
#endif

    _isRunning = true;
    return maxCommitId;
}

RocksKVStore::RocksKVStore(const std::string& id,
            const std::shared_ptr<ServerParams>& cfg,
            std::shared_ptr<rocksdb::Cache> blockCache,
            KVStore::StoreMode mode,
            TxnMode txnMode,
            uint64_t maxKeepLogs)
        :KVStore(id, cfg->dbPath),
         _isRunning(false),
         _isPaused(false),
         _hasBackup(false),
         _enableFilter(true),
         _mode(mode),
         _txnMode(txnMode),
         _optdb(nullptr),
         _pesdb(nullptr),
         _stats(rocksdb::CreateDBStatistics()),
         _blockCache(blockCache),
         _nextTxnSeq(0),
         _highestVisible(Transaction::TXNID_UNINITED),
         _logOb(nullptr),
         // NOTE(deyukong): we should keep at least 1 binlog to avoid cornercase
         _maxKeepLogs(std::max((uint64_t)1, maxKeepLogs)) {
    if (cfg->noexpire) {
        _enableFilter = false;
    }
    Expected<uint64_t> s = restart(false);
    if (!s.ok()) {
        LOG(FATAL) << "opendb:" << cfg->dbPath << "/" << id
                    << ", failed info:" << s.status().toString();
    }
}

Status RocksKVStore::releaseBackup() {
    try {
        if (!filesystem::exists(dftBackupDir())) {
            return {ErrorCodes::ERR_OK, ""};
        }
        filesystem::remove_all(dftBackupDir());
    } catch (const std::exception& ex) {
        LOG(FATAL) << "remove " << dftBackupDir() << " ex:" << ex.what();
    }
    {
        std::lock_guard<std::mutex> lk(_mutex);
        if (_hasBackup) {
            _hasBackup = false;
        }
        return {ErrorCodes::ERR_OK, ""};
    }
}

// this function guarantees that:
// If backup failed, there should be no remaining dirs left to clean,
// and the _hasBackup flag set to false
Expected<BackupInfo> RocksKVStore::backup(const std::string& dir,
    KVStore::BackupMode mode) {
    bool succ = false;
    auto guard = MakeGuard([this, &dir, &succ]() {
        if (succ) {
            return;
        }
        std::lock_guard<std::mutex> lk(_mutex);
        _hasBackup = false;
        try {
            if (!filesystem::exists(dir)) {
                return;
            }
            filesystem::remove_all(dir);
        } catch (const std::exception& ex) {
            LOG(FATAL) << "remove " << dir << " ex:" << ex.what();
        }
    });

    // NOTE(deyukong):
    // tendis uses BackupMode::BACKUP_COPY, we should keep compatible.
    // For tendisplus master/slave initial-sync, we use BackupMode::BACKUP_CKPT,
    // it's faster. here we assume BACKUP_CKPT works with default backupdir and
    // BACKUP_COPY works with arbitory dir except the default one.
    // But if someone feels it necessary to add one more param make it clearer,
    // go ahead.
    if (mode == KVStore::BackupMode::BACKUP_CKPT) {
        // BACKUP_CKPT works with the default backupdir and _hasBackup flag.
        if (dir != dftBackupDir()) {
            return {ErrorCodes::ERR_INTERNAL, "BACKUP_CKPT invalid dir"};
        }
        std::lock_guard<std::mutex> lk(_mutex);
        if (_hasBackup) {
            return {ErrorCodes::ERR_INTERNAL, "already have backup"};
        }
        _hasBackup = true;
    } else {
        if (dir == dftBackupDir()) {
            return {ErrorCodes::ERR_INTERNAL, "BACKUP_COPY invalid dir"};
        }
    }

    // NOTE(deyukong): we should get highVisible before making a ckpt
    BackupInfo result;
    uint64_t highVisible = getHighestBinlogId();
    if (highVisible == Transaction::TXNID_UNINITED) {
        LOG(WARNING) << "store:" << dbId() << " highVisible still zero";
    }
    result.setBinlogPos(highVisible);

    if (mode == KVStore::BackupMode::BACKUP_CKPT) {
        rocksdb::Checkpoint* checkpoint;
        auto s = rocksdb::Checkpoint::Create(getBaseDB(), &checkpoint);
        if (!s.ok()) {
            return {ErrorCodes::ERR_INTERNAL, s.ToString()};
        }
        s = checkpoint->CreateCheckpoint(dir);
        if (!s.ok()) {
            return {ErrorCodes::ERR_INTERNAL, s.ToString()};
        }
    } else {
        rocksdb::BackupEngine* bkEngine;
        auto s = rocksdb::BackupEngine::Open(rocksdb::Env::Default(),
            rocksdb::BackupableDBOptions(dir), &bkEngine);
        if (!s.ok()) {
            return {ErrorCodes::ERR_INTERNAL, s.ToString()};
        }
        std::unique_ptr<rocksdb::BackupEngine> pBkEngine(bkEngine);
        s = pBkEngine->CreateNewBackup(getBaseDB());
        if (!s.ok()) {
            return {ErrorCodes::ERR_INTERNAL, s.ToString()};
        }
    }
    std::map<std::string, uint64_t> flist;
    try {
        for (auto& p : filesystem::recursive_directory_iterator(dir)) {
            const filesystem::path& path = p.path();
            if (!filesystem::is_regular_file(p)) {
                LOG(INFO) << "backup ignore:" << p.path();
                continue;
            }
            size_t filesize = filesystem::file_size(path);
#ifndef _WIN32
            // assert path with bkupdir prefix
            // for win32, the dir should change to "\\"
            INVARIANT(path.string().find(dir) == 0);
#endif
            std::string relative = path.string().erase(0, dir.size());
            flist[relative] = filesize;
        }
    } catch (const std::exception& ex) {
        return {ErrorCodes::ERR_INTERNAL, ex.what()};
    }
    succ = true;
    result.setFileList(flist);
    return result;
}

Expected<std::unique_ptr<Transaction>> RocksKVStore::createTransaction() {
    std::lock_guard<std::mutex> lk(_mutex);
    if (!_isRunning) {
        return {ErrorCodes::ERR_INTERNAL, "db stopped!"};
    }
    uint64_t txnId = _nextTxnSeq++;
    bool replOnly = (_mode == KVStore::StoreMode::REPLICATE_ONLY);
    std::unique_ptr<Transaction> ret = nullptr;
    // TODO(vinchen): should new RocksTxn out of mutex?
    if (_txnMode == TxnMode::TXN_OPT) {
        ret.reset(new RocksOptTxn(this, txnId, replOnly, _logOb));
    } else {
        ret.reset(new RocksPesTxn(this, txnId, replOnly, _logOb));
    }
    addUnCommitedTxnInLock(txnId);
    return std::move(ret);
}

Status RocksKVStore::assignBinlogIdIfNeeded(Transaction* txn) {
    if (txn->getBinlogId() == Transaction::TXNID_UNINITED) {
        std::lock_guard<std::mutex> lk(_mutex);
        uint64_t binlogId = _nextBinlogSeq++;

        txn->setBinlogId(binlogId);
        INVARIANT(_aliveBinlogs.find(binlogId) == _aliveBinlogs.end());
        _aliveBinlogs.insert({ binlogId, { false, txn->getTxnId() } });

        auto it = _aliveTxns.find(txn->getTxnId());
        INVARIANT(it != _aliveTxns.end() && !it->second.first);

        it->second.second = binlogId;
    }

    return{ ErrorCodes::ERR_OK, "" };
}

void RocksKVStore::setNextBinlogSeq(uint64_t binlogId, Transaction* txn) {
    std::lock_guard<std::mutex> lk(_mutex);
    INVARIANT_D(txn->isReplOnly());

    _nextBinlogSeq = binlogId;

    txn->setBinlogId(binlogId);
    INVARIANT_D(_aliveBinlogs.find(binlogId) == _aliveBinlogs.end());
    _aliveBinlogs.insert({ binlogId, { false, txn->getTxnId() } });

    auto it = _aliveTxns.find(txn->getTxnId());
    INVARIANT_D(it != _aliveTxns.end() && !it->second.first);

    it->second.second = binlogId;
}

rocksdb::OptimisticTransactionDB* RocksKVStore::getUnderlayerOptDB() {
    return _optdb.get();
}

rocksdb::TransactionDB* RocksKVStore::getUnderlayerPesDB() {
    return _pesdb.get();
}

uint64_t RocksKVStore::getHighestBinlogId() const {
    std::lock_guard<std::mutex> lk(_mutex);
    return _highestVisible;
}

uint64_t RocksKVStore::getNextBinlogSeq() const {
    std::lock_guard<std::mutex> lk(_mutex);
    return _nextBinlogSeq;
}

rocksdb::DB* RocksKVStore::getBaseDB() const {
    return _optdb.get() ? _optdb->GetBaseDB() : _pesdb->GetBaseDB();
}

void RocksKVStore::addUnCommitedTxnInLock(uint64_t txnId) {
    // TODO(deyukong): need a better mutex mechnism to assert held
    if (_aliveTxns.find(txnId) != _aliveTxns.end()) {
        LOG(FATAL) << "BUG: txnid:" << txnId << " double add uncommitted";
    }
    _aliveTxns.insert({txnId, {false, Transaction::TXNID_UNINITED}});
}

void RocksKVStore::markCommitted(uint64_t txnId, uint64_t binlogTxnId) {
    std::lock_guard<std::mutex> lk(_mutex);
    markCommittedInLock(txnId, binlogTxnId);
}

std::set<uint64_t> RocksKVStore::getUncommittedTxns() const {
    std::lock_guard<std::mutex> lk(_mutex);
    std::set<uint64_t> result;
    for (auto& kv : _aliveTxns) {
        if (!kv.second.first) {
            result.insert(kv.first);
        }
    }
    return result;
}

void RocksKVStore::markCommittedInLock(uint64_t txnId, uint64_t binlogTxnId) {
    if (!_isRunning) {
        LOG(FATAL) << "BUG: _uncommittedTxns not empty after stopped";
    }

#ifdef BINLOG_V1
    // TODO(deyukong): need a better mutex mechnism to assert held
    auto it = _aliveTxns.find(txnId);
    if (it == _aliveTxns.end()) {
        LOG(FATAL) << "BUG: txnid:" << txnId << " not in uncommitted";
    }
    if (it->second.first) {
        LOG(FATAL) << "BUG: txnid:" << txnId << " already committed";
    }
    if (_mode == KVStore::StoreMode::READ_WRITE) {
        INVARIANT(binlogTxnId == txnId ||
                    binlogTxnId == Transaction::TXNID_UNINITED);
    } else {
        // currently, slaves do writes sequencially
        // NOTE(deyukong): the INVARIANT will not always hold
        // the physical-ckpt from master may have hole. slave
        // starts incr-sync from master's backupinfo's highVisible
        // id, which may have holes.
        // INVARIANT(binlogTxnId > _highestVisible ||
        //            binlogTxnId == Transaction::TXNID_UNINITED);
        if (binlogTxnId != Transaction::TXNID_UNINITED &&
            binlogTxnId <= _highestVisible) {
            LOG(WARNING) << "db:" << dbId()
                        << " markCommit with binlog:" << binlogTxnId
                        << " larger than _highestVisible:" << _highestVisible;
        }
    }
    it->second.first = true;
    it->second.second = binlogTxnId;

    // NOTE(deyukong): now, we can remove a visibility-hole
    // TODO(deyukong): afaik, slaves do writes sequencially, so in fact, there
    // is no write-holes.
    if (it == _aliveTxns.begin()) {
        while (it != _aliveTxns.end()) {
            if (!it->second.first) {
                break;
            }
            if (it->second.second != Transaction::TXNID_UNINITED) {
                _highestVisible = it->second.second;
            }
            it = _aliveTxns.erase(it);
        }
    }
#else
    auto it = _aliveTxns.find(txnId);
    INVARIANT(it != _aliveTxns.end());
    INVARIANT(!it->second.first);

    it->second.first = true;
    auto binlogId = it->second.second;
    _aliveTxns.erase(it);

    if (binlogId != Transaction::TXNID_UNINITED) {
        auto i = _aliveBinlogs.find(binlogId);
        INVARIANT(i != _aliveBinlogs.end());
        INVARIANT(i->second.second == txnId ||
            i->second.second == Transaction::TXNID_UNINITED);  // rollback

        i->second.first = true;
        i->second.second = binlogTxnId;
        if (i == _aliveBinlogs.begin()) {
            while (i != _aliveBinlogs.end()) {
                if (!i->second.first) {
                    break;
                }

                if (i->second.second != Transaction::TXNID_UNINITED) {
                    _highestVisible = i->first;
                    INVARIANT_D(_highestVisible <= _nextBinlogSeq);
                }
                i = _aliveBinlogs.erase(i);
            }
        }
    }
#endif
}

Expected<RecordValue> RocksKVStore::getKV(const RecordKey& key,
                                          Transaction *txn) {
    Expected<std::string> s = txn->getKV(key.encode());
    if (!s.ok()) {
        return s.status();
    }
    return RecordValue::decode(s.value());
}

Expected<RecordValue> RocksKVStore::getKV(const RecordKey& key,
    Transaction *txn, RecordType valueType) {
    auto eValue = getKV(key, txn);

    if (eValue.ok()) {
        if (eValue.value().getRecordType() != valueType) {
            return{ ErrorCodes::ERR_WRONG_TYPE, "" };
        }
    }

    return eValue;
}

Status RocksKVStore::setKV(const RecordKey& key,
                           const RecordValue& value,
                           Transaction *txn) {
    return txn->setKV(key.encode(), value.encode());
}

Status RocksKVStore::setKV(const Record& kv,
                           Transaction* txn) {
    // TODO(deyukong): statstics and inmemory-accumulative counter
    Record::KV pair = kv.encode();
    return txn->setKV(pair.first, pair.second);
}

Status RocksKVStore::setKV(const std::string& key,
                           const std::string& val,
                           Transaction *txn) {
    return txn->setKV(key, val);
}

Status RocksKVStore::delKV(const RecordKey& key,
                           Transaction *txn) {
    // TODO(deyukong): statstics and inmemory-accumulative counter
    return txn->delKV(key.encode());
}

void RocksKVStore::appendJSONStat(
            rapidjson::Writer<rapidjson::StringBuffer>& w) const {
    static const std::map<std::string, std::string> properties = {
        {"rocksdb.num-immutable-mem-table", "num_immutable_mem_table"},
        {"rocksdb.mem-table-flush-pending", "mem_table_flush_pending"},
        {"rocksdb.compaction-pending", "compaction_pending"},
        {"rocksdb.background-errors", "background_errors"},
        {"rocksdb.cur-size-active-mem-table", "cur_size_active_mem_table"},
        {"rocksdb.cur-size-all-mem-tables", "cur_size_all_mem_tables"},
        {"rocksdb.size-all-mem-tables", "size_all_mem_tables"},
        {"rocksdb.num-entries-active-mem-table",
                                "num_entries_active_mem_table"},
        {"rocksdb.num-entries-imm-mem-tables", "num_entries_imm_mem_tables"},
        {"rocksdb.num-deletes-active-mem-table",
                                "num_deletes_active_mem_table"},
        {"rocksdb.num-deletes-imm-mem-tables", "num_deletes_imm_mem_tables"},
        {"rocksdb.estimate-num-keys", "estimate_num_keys"},
        {"rocksdb.estimate-table-readers-mem", "estimate_table_readers_mem"},
        {"rocksdb.is-file-deletions-enabled", "is_file_deletions_enabled"},
        {"rocksdb.num-snapshots", "num_snapshots"},
        {"rocksdb.oldest-snapshot-time", "oldest_snapshot_time"},
        {"rocksdb.num-live-versions", "num_live_versions"},
        {"rocksdb.current-super-version-number",
                                "current_super_version_number"},
        {"rocksdb.estimate-live-data-size", "estimate_live_data_size"},
        {"rocksdb.min-log-number-to-keep", "min_log_number_to_keep"},
        {"rocksdb.total-sst-files-size", "total_sst_files_size"},
        {"rocksdb.live-sst-files-size", "live_sst_files_size"},
        {"rocksdb.base-level", "base_level"},
        {"rocksdb.estimate-pending-compaction-bytes",
                                "estimate_pending_compaction_bytes"},
        {"rocksdb.num-running-compactions", "num_running_compactions"},
        {"rocksdb.num-running-flushes", "num_running_flushses"},
        {"rocksdb.actual-delayed-write-rate", "actual_delayed_write_rate"},
        {"rocksdb.is-write-stopped", "is_write_stopped"},
    };
    w.Key("id");
    w.String(dbId().c_str());
    w.Key("is_running");
    w.Uint64(_isRunning);
    w.Key("is_paused");
    w.Uint64(_isPaused);
    w.Key("has_backup");
    w.Uint64(_hasBackup);
    w.Key("next_txn_seq");
    w.Uint64(_nextTxnSeq);
    w.Key("next_binlog_seq");
    w.Uint64(_nextBinlogSeq);
    {
        std::lock_guard<std::mutex> lk(_mutex);
        w.Key("alive_txns");
        w.Uint64(_aliveTxns.size());
#ifdef BINLOG_V1
        w.Key("min_alive_txn");
        w.Uint64(_aliveTxns.size() ? _aliveTxns.begin()->first : 0);
        w.Key("max_alive_txn");
        w.Uint64(_aliveTxns.size() ? _aliveTxns.rbegin()->first : 0);
#else
        w.Key("alive_binlogs");
        w.Uint64(_aliveBinlogs.size());
        w.Key("min_alive_binlog");
        w.Uint64(_aliveBinlogs.size() ? _aliveBinlogs.begin()->first : 0);
        w.Key("max_alive_binlog");
        w.Uint64(_aliveBinlogs.size() ? _aliveBinlogs.rbegin()->first : 0);
#endif
        w.Key("high_visible");
        w.Uint64(_highestVisible);
    }

    w.Key("compact_filter_count");
    w.Uint64(stat.compactFilterCount.load(std::memory_order_relaxed));
    w.Key("compact_kvexpired_count");
    w.Uint64(stat.compactKvExpiredCount.load(std::memory_order_relaxed));
    w.Key("paused_error_count");
    w.Uint64(stat.pausedErrorCount.load(std::memory_order_relaxed));
    w.Key("destroyed_error_count");
    w.Uint64(stat.destroyedErrorCount.load(std::memory_order_relaxed));

    w.Key("rocksdb");
    w.StartObject();
    if (_isRunning) {
        for (const auto& kv : properties) {
            uint64_t tmp;
            bool ok = getBaseDB()->GetIntProperty(kv.first, &tmp);
            if (!ok) {
                LOG(WARNING) << "db:" << dbId()
                    << " getProperity:" << kv.first << " failed";
                continue;
            }
            w.Key(kv.second.c_str());
            w.Uint64(tmp);
        }
    }
    w.EndObject();
}

}  // namespace tendisplus
