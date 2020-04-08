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
#include "rapidjson/prettywriter.h"
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/error/en.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/utilities/backupable_db.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/options.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"
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

#ifndef NO_VERSIONEP
#define RESET_PERFCONTEXT() do {\
    if (_session && _session->getCtx()->needResetPerLevel()) {\
        rocksdb::SetPerfLevel(rocksdb::PerfLevel(_session->getCtx()->getPerfLevel()));\
        rocksdb::get_perf_context()->Reset();\
        rocksdb::get_iostats_context()->Reset();\
    }\
  } while (0)
#else
#define RESET_PERFCONTEXT()
#endif

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
                   Session* sess,
                   uint64_t binlogId, uint32_t chunkId)
        :_txnId(txnId),
         _binlogId(binlogId),
         _chunkId(chunkId),
         _txn(nullptr),
         _store(store),
         _done(false),
         _replOnly(replOnly),
         _logOb(ob),
         _session(sess) {
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
    RESET_PERFCONTEXT();
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
        INVARIANT_D(!isReplOnly());

        if (_replLogValues.size() >= std::numeric_limits<uint16_t>::max()) {
            LOG(WARNING) << "too big binlog size:", std::to_string(_replLogValues.size());
        }

        _store->assignBinlogIdIfNeeded(this);
        INVARIANT_D(_binlogId != Transaction::TXNID_UNINITED);

        uint32_t chunkId = getChunkId();

        // NOTE(vinchen): Now, one transaction one repllog, so the flag is
        // (START | END)
        uint16_t oriFlag = static_cast<uint16_t>(ReplFlag::REPL_GROUP_START)
            | static_cast<uint16_t>(ReplFlag::REPL_GROUP_END);

        DLOG(INFO) << "RocksTxn::commit() storeid:" << _store->dbId() << " binlogid:" << _binlogId;

        ReplLogKeyV2 key(_binlogId);
        ReplLogValueV2 val(chunkId, static_cast<ReplFlag>(oriFlag), _txnId,
            _replLogValues.back().getTimestamp(),
#ifndef NO_VERSIONEP
            _session ? _session->getCtx()->getVersionEP() : SessionCtx::VERSIONEP_UNINITED,
            (_session && _session->getArgs().size() > 0) ? _session->getArgs()[0] : "",
#else
            SessionCtx::VERSIONEP_UNINITED,
            "",
#endif
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
    } else if (chunkId == Transaction::CHUNKID_FLUSH) {
        _chunkId = chunkId;
    } else if (_chunkId != chunkId) {
        INVARIANT_D(_chunkId != Transaction::CHUNKID_FLUSH);
        _chunkId = Transaction::CHUNKID_MULTI;
    }
}

Expected<std::string> RocksTxn::getKV(const std::string& key) {
    rocksdb::ReadOptions readOpts;
    std::string value;

    RESET_PERFCONTEXT();
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

    RESET_PERFCONTEXT();
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
    if (_store->enableRepllog()) {
        INVARIANT_D(_store->dbId() != CATALOG_NAME);
        setChunkId(RecordKey::decodeChunkId(key));
        if (_replLogValues.size() == std::numeric_limits<uint16_t>::max()) {
            // TODO(vinchen): if too large, it can flush to rocksdb first,
            // and get another binlogid using assignBinlogIdIfNeeded()
            auto eKey = RecordKey::decode(key);
            if (eKey.ok()) {
                LOG(WARNING) << "setKV too big binlog size, key:" + eKey.value().getPrimaryKey();
            } else {
                LOG(WARNING) << "setKV too big binlog size, invalid key:" + key;
            }
        }

        ReplLogValueEntryV2 logVal(ReplOp::REPL_OP_SET, ts ? ts : msSinceEpoch(),
            key, val);
        // TODO(vinchen): maybe OOM
        _replLogValues.emplace_back(std::move(logVal));
    }
#endif
    return {ErrorCodes::ERR_OK, ""};
}

Status RocksTxn::delKV(const std::string& key, const uint64_t ts) {
    if (_replOnly) {
        return {ErrorCodes::ERR_INTERNAL, "txn is replOnly"};
    }
    RESET_PERFCONTEXT();
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
    if (_store->enableRepllog()) {
        INVARIANT_D(_store->dbId() != CATALOG_NAME);
        setChunkId(RecordKey::decodeChunkId(key));
        if (_replLogValues.size() == std::numeric_limits<uint16_t>::max()) {
            // TODO(vinchen): if too large, it can flush to rocksdb first,
            // and get another binlogid using assignBinlogIdIfNeeded()
            auto eKey = RecordKey::decode(key);
            if (eKey.ok()) {
                LOG(WARNING) << "delKV too big binlog size, key:" + eKey.value().getPrimaryKey();
            } else {
                LOG(WARNING) << "delKV too big binlog size, invalid key:" + key;
            }
        }
        ReplLogValueEntryV2 logVal(ReplOp::REPL_OP_DEL, ts ? ts : msSinceEpoch(),
            key, "");
        // TODO(vinchen): maybe OOM
        _replLogValues.emplace_back(std::move(logVal));
    }
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
Status RocksTxn::flushall() {
    if (_replOnly) {
        return{ ErrorCodes::ERR_INTERNAL, "txn is replOnly" };
    }
    if (!_store->enableRepllog()) {
        return{ ErrorCodes::ERR_INTERNAL, "repllog is not enable" };
    }

    INVARIANT_D(_store->dbId() != CATALOG_NAME);
    setChunkId(Transaction::CHUNKID_FLUSH);
    INVARIANT_D(_replLogValues.size() == 0);

    std::string cmd = "flush";
#ifndef NO_VERSIONEP
    if (_session) {
        cmd = _session->getCmdStr();
    }
#endif

    ReplLogValueEntryV2 logVal(ReplOp::REPL_OP_STMT, _store->getCurrentTime(),
        cmd, "");
    _replLogValues.emplace_back(std::move(logVal));
    return{ ErrorCodes::ERR_OK, "" };
}

Status RocksTxn::applyBinlog(const ReplLogValueEntryV2& logEntry) {
    if (!_replOnly) {
        return{ ErrorCodes::ERR_INTERNAL, "txn is not replOnly" };
    }
    RESET_PERFCONTEXT();
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
    case ReplOp::REPL_OP_STMT: {
        INVARIANT_D(0);
    }
    default:
        INVARIANT_D(0);
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

    RESET_PERFCONTEXT();
    auto s = _txn->Put(logKey, logValue);
    if (!s.ok()) {
        return{ ErrorCodes::ERR_INTERNAL, s.ToString() };
    }

    return{ ErrorCodes::ERR_OK, "" };
}

Status RocksTxn::delBinlog(const ReplLogRawV2& log) {
    RESET_PERFCONTEXT();
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

#ifndef BINLOG_V1
    // NOTE(vinchen): make sure whether is there any command
    // forget to commit or rollback
    INVARIANT_D(_replLogValues.size() == 0);
#endif

    _txn.reset();
    _store->markCommitted(_txnId, Transaction::TXNID_UNINITED);
}

RocksOptTxn::RocksOptTxn(RocksKVStore* store, uint64_t txnId, bool replOnly,
            std::shared_ptr<tendisplus::BinlogObserver> ob, Session* sess)
    :RocksTxn(store, txnId, replOnly, ob, sess) {
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
    writeOpts.disableWAL = _store->getCfg()->rocksDisableWAL;
    writeOpts.sync = _store->getCfg()->rocksFlushLogAtTrxCommit;

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
            std::shared_ptr<BinlogObserver> ob, Session* sess)
    :RocksTxn(store, txnId, replOnly, ob, sess) {
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
    writeOpts.disableWAL = _store->getCfg()->rocksDisableWAL;
    writeOpts.sync = _store->getCfg()->rocksFlushLogAtTrxCommit;

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

Status rocksdbOptionsSet(rocksdb::Options& options, const std::string key, int64_t value) {
    // AdvancedColumnFamilyOptions
    if (key == "max_write_buffer_number") { options.max_write_buffer_number = (int)value; }
    else if (key == "min_write_buffer_number_to_merge") { options.min_write_buffer_number_to_merge = (int)value; }
    else if (key == "max_write_buffer_number_to_maintain") { options.max_write_buffer_number_to_maintain = (int)value; }
    else if (key == "inplace_update_support") { options.inplace_update_support = (bool)value; }
    else if (key == "inplace_update_num_locks") { options.inplace_update_num_locks = (size_t)value; }
    //else if (key == "memtable_prefix_bloom_size_ratio") { options.memtable_prefix_bloom_size_ratio = (double)value; }
#if ROCKSDB_MAJOR > 5 || (ROCKSDB_MAJOR == 5 && ROCKSDB_MINOR > 15)
    else if (key == "memtable_whole_key_filtering") { options.memtable_whole_key_filtering = (bool)value; }
#endif
    else if (key == "memtable_huge_page_size") { options.memtable_huge_page_size = (size_t)value; }
    else if (key == "bloom_locality") { options.bloom_locality = (uint32_t)value; }
    else if (key == "arena_block_size") { options.arena_block_size = (size_t)value; }
    else if (key == "num_levels") { options.num_levels = (int)value; }
    else if (key == "level0_slowdown_writes_trigger") { options.level0_slowdown_writes_trigger = (int)value; }
    else if (key == "level0_stop_writes_trigger") { options.level0_stop_writes_trigger = (int)value; }
    else if (key == "target_file_size_base") { options.target_file_size_base = (uint64_t)value; }
    else if (key == "target_file_size_multiplier") { options.target_file_size_multiplier = (int)value; }
    else if (key == "level_compaction_dynamic_level_bytes") { options.level_compaction_dynamic_level_bytes = (bool)value; }
    //else if (key == "max_bytes_for_level_multiplier") { options.max_bytes_for_level_multiplier = (double)value; }
    else if (key == "max_compaction_bytes") { options.max_compaction_bytes = (uint64_t)value; }
    else if (key == "soft_pending_compaction_bytes_limit") { options.soft_pending_compaction_bytes_limit = (uint64_t)value; }
    else if (key == "hard_pending_compaction_bytes_limit") { options.hard_pending_compaction_bytes_limit = (uint64_t)value; }
    else if (key == "max_sequential_skip_in_iterations") { options.max_sequential_skip_in_iterations = (uint64_t)value; }
    else if (key == "max_successive_merges") { options.max_successive_merges = (size_t)value; }
    else if (key == "optimize_filters_for_hits") { options.optimize_filters_for_hits = (bool)value; }
    else if (key == "paranoid_file_checks") { options.paranoid_file_checks = (bool)value; }
    else if (key == "force_consistency_checks") { options.force_consistency_checks = (bool)value; }
    else if (key == "report_bg_io_stats") { options.report_bg_io_stats = (bool)value; }
#if ROCKSDB_MAJOR > 5 || (ROCKSDB_MAJOR == 5 && ROCKSDB_MINOR > 15)
    else if (key == "ttl") { options.ttl = (uint64_t)value; }
#endif
    //else if (key == "soft_rate_limit") { options.soft_rate_limit = (double)value; }
    //else if (key == "hard_rate_limit") { options.hard_rate_limit = (double)value; }
    else if (key == "rate_limit_delay_max_milliseconds") { options.rate_limit_delay_max_milliseconds = (unsigned int)value; }
    else if (key == "purge_redundant_kvs_while_flush") { options.purge_redundant_kvs_while_flush = (bool)value; }
    // ColumnFamilyOptions
    else if (key == "write_buffer_size") { options.write_buffer_size = (size_t)value; }
    else if (key == "level0_file_num_compaction_trigger") { options.level0_file_num_compaction_trigger = (int)value; }
    else if (key == "max_bytes_for_level_base") { options.max_bytes_for_level_base = (uint64_t)value; }
    else if (key == "disable_auto_compactions") { options.disable_auto_compactions = (bool)value; }
    // DBOptions
    else if (key == "create_if_missing") { options.create_if_missing = (bool)value; }
    else if (key == "create_missing_column_families") { options.create_missing_column_families = (bool)value; }
    else if (key == "error_if_exists") { options.error_if_exists = (bool)value; }
    else if (key == "paranoid_checks") { options.paranoid_checks = (bool)value; }
    else if (key == "max_open_files") { options.max_open_files = (int)value; }
    else if (key == "max_file_opening_threads") { options.max_file_opening_threads = (int)value; }
    else if (key == "max_total_wal_size") { options.max_total_wal_size = (uint64_t)value; }
    else if (key == "use_fsync") { options.use_fsync = (bool)value; }
    else if (key == "delete_obsolete_files_period_micros") { options.delete_obsolete_files_period_micros = (uint64_t)value; }
    else if (key == "max_background_jobs") { options.max_background_jobs = (int)value; }
    else if (key == "base_background_compactions") { options.base_background_compactions = (int)value; }
    else if (key == "max_background_compactions") { options.max_background_compactions = (int)value; }
    else if (key == "max_subcompactions") { options.max_subcompactions = (uint32_t)value; }
    else if (key == "max_background_flushes") { options.max_background_flushes = (int)value; }
    else if (key == "max_log_file_size") { options.max_log_file_size = (size_t)value; }
    else if (key == "log_file_time_to_roll") { options.log_file_time_to_roll = (size_t)value; }
    else if (key == "keep_log_file_num") { options.keep_log_file_num = (size_t)value; }
    else if (key == "recycle_log_file_num") { options.recycle_log_file_num = (size_t)value; }
    else if (key == "max_manifest_file_size") { options.max_manifest_file_size = (uint64_t)value; }
    else if (key == "table_cache_numshardbits") { options.table_cache_numshardbits = (int)value; }
    else if (key == "wal_ttl_seconds") { options.WAL_ttl_seconds = (uint64_t)value; }
    else if (key == "wal_size_limit_mb") { options.WAL_size_limit_MB = (uint64_t)value; }
    else if (key == "manifest_preallocation_size") { options.manifest_preallocation_size = (size_t)value; }
    else if (key == "allow_mmap_reads") { options.allow_mmap_reads = (bool)value; }
    else if (key == "allow_mmap_writes") { options.allow_mmap_writes = (bool)value; }
    else if (key == "use_direct_reads") { options.use_direct_reads = (bool)value; }
    else if (key == "use_direct_io_for_flush_and_compaction") { options.use_direct_io_for_flush_and_compaction = (bool)value; }
    else if (key == "allow_fallocate") { options.allow_fallocate = (bool)value; }
    else if (key == "is_fd_close_on_exec") { options.is_fd_close_on_exec = (bool)value; }
    else if (key == "skip_log_error_on_recovery") { options.skip_log_error_on_recovery = (bool)value; }
    else if (key == "stats_dump_period_sec") { options.stats_dump_period_sec = (int)value; }
#if ROCKSDB_MAJOR > 5 || (ROCKSDB_MAJOR == 5 && ROCKSDB_MINOR > 15)
    else if (key == "stats_persist_period_sec") { options.stats_persist_period_sec = (int)value; }
    else if (key == "stats_history_buffer_size") { options.stats_history_buffer_size = (size_t)value; }
#endif
    else if (key == "advise_random_on_open") { options.advise_random_on_open = (bool)value; }
    else if (key == "db_write_buffer_size") { options.db_write_buffer_size = (size_t)value; }
    else if (key == "new_table_reader_for_compaction_inputs") { options.new_table_reader_for_compaction_inputs = (bool)value; }
    else if (key == "compaction_readahead_size") { options.compaction_readahead_size = (size_t)value; }
    else if (key == "random_access_max_buffer_size") { options.random_access_max_buffer_size = (size_t)value; }
    else if (key == "writable_file_max_buffer_size") { options.writable_file_max_buffer_size = (size_t)value; }
    else if (key == "use_adaptive_mutex") { options.use_adaptive_mutex = (bool)value; }
    else if (key == "bytes_per_sync") { options.bytes_per_sync = (uint64_t)value; }
    else if (key == "wal_bytes_per_sync") { options.wal_bytes_per_sync = (uint64_t)value; }
    else if (key == "enable_thread_tracking") { options.enable_thread_tracking = (bool)value; }
    else if (key == "delayed_write_rate") { options.delayed_write_rate = (uint64_t)value; }
    else if (key == "enable_pipelined_write") { options.enable_pipelined_write = (bool)value; }
    else if (key == "allow_concurrent_memtable_write") { options.allow_concurrent_memtable_write = (bool)value; }
    else if (key == "enable_write_thread_adaptive_yield") { options.enable_write_thread_adaptive_yield = (bool)value; }
    else if (key == "write_thread_max_yield_usec") { options.write_thread_max_yield_usec = (uint64_t)value; }
    else if (key == "write_thread_slow_yield_usec") { options.write_thread_slow_yield_usec = (uint64_t)value; }
    else if (key == "skip_stats_update_on_db_open") { options.skip_stats_update_on_db_open = (bool)value; }
    else if (key == "allow_2pc") { options.allow_2pc = (bool)value; }
    else if (key == "fail_if_options_file_error") { options.fail_if_options_file_error = (bool)value; }
    else if (key == "dump_malloc_stats") { options.dump_malloc_stats = (bool)value; }
    else if (key == "avoid_flush_during_recovery") { options.avoid_flush_during_recovery = (bool)value; }
    else if (key == "avoid_flush_during_shutdown") { options.avoid_flush_during_shutdown = (bool)value; }
    else if (key == "allow_ingest_behind") { options.allow_ingest_behind = (bool)value; }
    else if (key == "preserve_deletes") { options.preserve_deletes = (bool)value; }
    else if (key == "two_write_queues") { options.two_write_queues = (bool)value; }
    else if (key == "manual_wal_flush") { options.manual_wal_flush = (bool)value; }
#if ROCKSDB_MAJOR > 5 || (ROCKSDB_MAJOR == 5 && ROCKSDB_MINOR > 15)
    else if (key == "atomic_flush") { options.atomic_flush = (bool)value; }
#endif
    else {
        return { ErrorCodes::ERR_PARSEOPT, "invalid rocksdb option :" + key };
    }

    return { ErrorCodes::ERR_OK, "" };
}

Status rocksdbTableOptionsSet(rocksdb::BlockBasedTableOptions& options, const std::string key, int64_t value) {
    if (key == "cache_index_and_filter_blocks") { options.cache_index_and_filter_blocks = (bool)value; }
    else if (key == "cache_index_and_filter_blocks_with_high_priority") { options.cache_index_and_filter_blocks_with_high_priority = (bool)value; }
    else if (key == "pin_l0_filter_and_index_blocks_in_cache") { options.pin_l0_filter_and_index_blocks_in_cache = (bool)value; }
#if ROCKSDB_MAJOR > 5 || (ROCKSDB_MAJOR == 5 && ROCKSDB_MINOR > 15)
    else if (key == "pin_top_level_index_and_filter") { options.pin_top_level_index_and_filter = (bool)value; }
    //else if (key == "data_block_hash_table_util_ratio") { options.data_block_hash_table_util_ratio = (double)value; }
#endif
    else if (key == "hash_index_allow_collision") { options.hash_index_allow_collision = (bool)value; }
    else if (key == "no_block_cache") { options.no_block_cache = (bool)value; }
    else if (key == "block_size") { options.block_size = (size_t)value; }
    else if (key == "block_size_deviation") { options.block_size_deviation = (int)value; }
    else if (key == "block_restart_interval") { options.block_restart_interval = (int)value; }
    else if (key == "index_block_restart_interval") { options.index_block_restart_interval = (int)value; }
    else if (key == "metadata_block_size") { options.metadata_block_size = (uint64_t)value; }
    else if (key == "partition_filters") { options.partition_filters = (bool)value; }
    else if (key == "use_delta_encoding") { options.use_delta_encoding = (bool)value; }
    else if (key == "whole_key_filtering") { options.whole_key_filtering = (bool)value; }
    else if (key == "verify_compression") { options.verify_compression = (bool)value; }
    else if (key == "read_amp_bytes_per_bit") { options.read_amp_bytes_per_bit = (uint32_t)value; }
    else if (key == "format_version") { options.format_version = (uint32_t)value; }
    else if (key == "enable_index_compression") { options.enable_index_compression = (bool)value; }
#if ROCKSDB_MAJOR > 5 || (ROCKSDB_MAJOR == 5 && ROCKSDB_MINOR > 15)
    else if (key == "block_align") { options.block_align = (bool)value; }
#endif
    else {
        return { ErrorCodes::ERR_PARSEOPT, "invalid rocksdb  option :" + key };
    }

    return { ErrorCodes::ERR_OK, "" };
}

rocksdb::CompressionType rocksGetCompressType(const std::string& typeStr) {
    if (typeStr == "snappy") {
        return rocksdb::CompressionType::kSnappyCompression;
    } else if (typeStr == "lz4") {
        return rocksdb::CompressionType::kLZ4Compression;
    } else if (typeStr == "none") {
        return rocksdb::CompressionType::kNoCompression;
    } else {
        INVARIANT(0);
        return rocksdb::CompressionType::kNoCompression;
    }
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

    options.write_buffer_size = 64 * 1024 * 1024; // 64MB
    // level_0 max size: 8*64MB = 512MB
    options.level0_slowdown_writes_trigger = 8;
    options.max_write_buffer_number = 4;
    options.max_write_buffer_number_to_maintain = 1;
    options.max_background_compactions = 8;
    options.max_background_flushes = 2;
    options.target_file_size_base = 64 * 1024 * 1024; // 64MB
    options.level_compaction_dynamic_level_bytes = true;
    // level_1 max size: 512MB, in fact, things are more complex
    // since we set level_compaction_dynamic_level_bytes = true
    options.max_bytes_for_level_base = 512 * 1024 * 1024; // 512MB
    options.max_open_files = -1;
    // if we have no 'empty reads', we can disable bottom
    // level's bloomfilters
    options.optimize_filters_for_hits = false;
    options.enable_thread_tracking = true;
    options.compression_per_level.resize(ROCKSDB_NUM_LEVELS);
    for (int i = 0; i < ROCKSDB_NUM_LEVELS; ++i) {
        options.compression_per_level[i] = rocksGetCompressType(_cfg->rocksCompressType);
    }
    if (!_cfg->level0Compress) {
        options.compression_per_level[0] = rocksdb::kNoCompression;
    }
    if (!_cfg->level1Compress) {
        options.compression_per_level[1] = rocksdb::kNoCompression;
    }
    options.statistics = _stats;
    options.create_if_missing = true;

    options.max_total_wal_size = uint64_t(4294967296);  // 4GB

    if (_cfg->rocksWALDir != "") {
        options.wal_dir = _cfg->rocksWALDir + "/" + dbId() + "/";
    }

    for (const auto& iter : _cfg->getRocksdbOptions()) {
        auto status = rocksdbOptionsSet(options, iter.first, iter.second);
        if (!status.ok()) {
            status = rocksdbTableOptionsSet(table_options, iter.first, iter.second);
            if (!status.ok()) {
                LOG(ERROR) << status.toString();
            }
        }
    }

    options.table_factory.reset(
        rocksdb::NewBlockBasedTableFactory(table_options));

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

bool RocksKVStore::isEmpty(bool ignoreBinlog) const {
    //std::lock_guard<std::mutex> lk(_mutex);

    auto ptxn = const_cast<RocksKVStore*>(this)->createTransaction(nullptr);
    if (!ptxn.ok()) {
        return false;
    }
    std::unique_ptr<Transaction> txn = std::move(ptxn.value());

    auto baseCursor = txn->createCursor();
    //baseCursor->seekToLast();

    Expected<std::string> expKey = baseCursor->key();
    if (expKey.ok()) {
        if (!ignoreBinlog) {
            return false;
        }
        Expected<RecordKey> expRk = RecordKey::decode(expKey.value());
        if (!expRk.ok()) {
            LOG(ERROR) << "RecordKey::decode failed.";
            return false;
        }
        if (expRk.value().getChunkId() == ReplLogKeyV2::CHUNKID) {
            return true;
        }
        return false;
    } else if (expKey.status().code() == ErrorCodes::ERR_EXHAUST) {
        return true;
    } else {
        LOG(ERROR) << "baseCursor key failed:" << expKey.status().toString();
        return false;
    }
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
    uint64_t maxKeepLogs = std::max((uint64_t)1, _cfg->maxBinlogKeepNum);
    uint64_t gap = getHighestBinlogId() - start;
    if (gap < maxKeepLogs) {
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
    uint64_t cnt = std::min((uint64_t)10000, gap - maxKeepLogs);
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

Expected<bool> RocksKVStore::deleteBinlog(uint64_t start) {
    auto ptxn = const_cast<RocksKVStore*>(this)->createTransaction(nullptr);
    if (!ptxn.ok()) {
        LOG(ERROR) << "deleteBinlog create txn failed:"
                   << ptxn.status().toString();
        return false;
    }
    auto txn = std::move(ptxn.value());

    LOG(INFO) << "deleteBinlog begin, dbid:" << dbId()
        << " start:" << start;

    auto cursor = txn->createRepllogCursorV2(start);

    uint64_t count = 0;
    uint64_t end = 0;
    while (true) {
        auto explog = cursor->next();
        if (!explog.ok()) {
            if (explog.status().code() == ErrorCodes::ERR_EXHAUST) {
                break;
            }
            return explog.status();
        }
        count++;
        end = explog.value().getBinlogId();

        DLOG(INFO) <<"deleteBinlog dbid:"<< dbId() <<" delete:" << explog.value().getBinlogId();
        auto s = txn->delBinlog(explog.value());
        if (!s.ok()) {
            LOG(ERROR) << "delbinlog error:" << s.toString();
            return s;
        }
    }
    auto commitStat = txn->commit();
    if (!commitStat.ok()) {
        LOG(ERROR) << "deleteBinlog store:" << dbId()
                    << "commit failed:" << commitStat.status().toString();
        return false;
    }
    LOG(INFO) << "deleteBinlog success, dbid:" << dbId()
        << " start:" << start << " end:" << end << " count:" << count;
    return true;
}

Expected<TruncateBinlogResult> RocksKVStore::truncateBinlogV2(uint64_t start,
    uint64_t end, Transaction *txn, std::ofstream *fs) {
    DLOG(INFO) << "truncateBinlogV2 dbid:" << dbId()
        << " getHighestBinlogId:" << getHighestBinlogId()
        << " start:"<<start <<" end:"<< end;
    TruncateBinlogResult result;
    uint64_t ts = 0;
    uint64_t written = 0;
    uint64_t deleten = 0;
    // INVARIANT_D(RepllogCursorV2::getMinBinlogId(txn).value() == start);
    // INVARIANT_COMPARE_D(RepllogCursorV2::getMinBinlogId(txn).value(), >=, start);
#ifdef TENDIS_DEBUG
    Expected<uint64_t> minBinlogid = RepllogCursorV2::getMinBinlogId(txn);
    if (minBinlogid.status().code() != ErrorCodes::ERR_EXHAUST) {
        INVARIANT_COMPARE_D(minBinlogid.value(), >=, start);
    }
#endif
    auto cursor = txn->createRepllogCursorV2(start);

    uint64_t max_cnt = _cfg->truncateBinlogNum;
    uint64_t size = 0;
    uint64_t nextStart = start;
    uint64_t cur_ts = msSinceEpoch();
    while (true) {
        auto explog = cursor->next();
        if (!explog.ok()) {
            if (explog.status().code() == ErrorCodes::ERR_EXHAUST) {
                break;
            }
            return explog.status();
        }
        nextStart = explog.value().getBinlogId();

        // NOTE(deyukong): currently, we cant get the exact log count by
        // _highestVisible - startLogId, because "readonly" txns also
        // occupy txnIds, and in each txnId, there are more sub operations.
        // So, maxKeepLogs is not named precisely.
        // NOTE(deyukong): we should keep at least 1 binlog to avoid cornercase
        uint64_t maxKeepLogs = std::max((uint32_t)1, _cfg->maxBinlogKeepNum);
        if (nextStart > end ||
            size >= max_cnt ||
            getHighestBinlogId() - nextStart <= (maxKeepLogs - 1)) {
            break;
        }
        ts = explog.value().getTimestamp();
        uint64_t minKeepLogMs = _cfg->minBinlogKeepSec * 1000;
        if (minKeepLogMs != 0 && ts >= cur_ts - minKeepLogMs) {
            break;
        }

        size++;
        if (fs) {
            // save binlog
            written += saveBinlogV2(fs, explog.value());
        }

        // TODO(vinchen): compactrange or compactfilter should be better
        DLOG(INFO) <<"truncateBinlogV2 dbid:"<< dbId() <<" delete:" << explog.value().getBinlogId()
            << " time:" << (cur_ts - ts)/1000 << " sec ago.";
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

Expected<uint64_t> RocksKVStore::flush(Session* sess, uint64_t nextBinlogid) {
    auto s = stop();
    if (!s.ok()) {
        return s;
    }

    s = clear();
    if (!s.ok()) {
        return s;
    }

    auto ret = restart(false, nextBinlogid);
    if (!ret.ok()) {
        return ret.status();
    }
    INVARIANT_D(ret.value() == nextBinlogid - 1);

    // NOTE(vinchen): make sure the first binlog is flush db,
    // and write the flush binlog using nextBinlogid.
    // it will make everything simple.
    auto eptxn = createTransaction(sess);
    if (!eptxn.ok()) {
        return eptxn.status();
    }
    auto txn = std::move(eptxn.value());
    s = txn->flushall();
    if (!s.ok()) {
        return s;
    }

    return txn->commit();
}

Expected<uint64_t> RocksKVStore::restart(bool restore, uint64_t nextBinlogid, uint64_t maxBinlogid) {
  // when do backup will get _highestVisible first, and backup later. so the _highestVisible maybe smaller than backup.
  // so slaveof need the slave delete the binlogs after _highestVisible for safe,
  // and restorebackup need delete the binlogs after _highestVisible for safe too.
  bool needDeleteBinlog = false;

  uint64_t maxCommitId = 0;
  {
    std::lock_guard<std::mutex> lk(_mutex);
    if (_isRunning) {
        return {ErrorCodes::ERR_INTERNAL, "already running"};
    }
    LOG(INFO) << "RocksKVStore::restart id:"<< dbId() << " restore:" << restore
        << " nextBinlogid:" << nextBinlogid << " maxBinlogid:" << maxBinlogid;
    INVARIANT_D(nextBinlogid != Transaction::TXNID_UNINITED);

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
        LOG(INFO) << "rocksdb Open,id:"<< dbId() << " dbname:" << dbname;
        auto status = rocksdb::TransactionDB::Open(
            dbOpts, txnDbOptions, dbname, &tmpDb);
        if (!status.ok()) {
            LOG(INFO) << "rocksdb Open error,id:"<< dbId() << " dbname:" << dbname;
            return {ErrorCodes::ERR_INTERNAL, status.ToString()};
        }
        LOG(INFO) << "rocksdb Open sucess,id:"<< dbId() << " dbname:" << dbname;
        rocksdb::ReadOptions readOpts;
        iter.reset(tmpDb->GetBaseDB()->NewIterator(readOpts));
        _pesdb.reset(tmpDb);
    }
    // NOTE(deyukong): during starttime, mutex is held and
    // no need to consider visibility
    RocksKVCursor cursor(std::move(iter));

    cursor.seekToLast();
    Expected<Record> expRcd = cursor.next();

    maxCommitId = nextBinlogid - 1;
    INVARIANT_D(nextBinlogid > maxCommitId);
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
                needDeleteBinlog = true;
            }
        } else {
            _nextTxnSeq = nextBinlogid;
            _nextBinlogSeq = _nextTxnSeq;
            LOG(INFO) << "store:" << dbId() << ' ' << rk.getPrimaryKey()
                << " have no binlog, set nextSeq to " << _nextTxnSeq;
            _highestVisible = _nextBinlogSeq - 1;
            INVARIANT(_highestVisible < _nextBinlogSeq);
        }
    } else if (expRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
        _nextTxnSeq = nextBinlogid;
        _nextBinlogSeq = _nextTxnSeq;
        LOG(INFO) << "store:" << dbId()
            << " all empty, set nextSeq to " << _nextTxnSeq;
        _highestVisible = _nextBinlogSeq - 1;
        INVARIANT(_highestVisible < _nextBinlogSeq);
    } else {
        return expRcd.status();
    }
#endif

    _isRunning = true;
  }
  {
    if (needDeleteBinlog) {
        if (maxBinlogid != Transaction::TXNID_UNINITED)
        {
            Expected<bool> ret = deleteBinlog(maxBinlogid + 1);
            if (!ret.ok()) {
                return ret.status();
            }
            LOG(INFO) << "store:" << dbId()
                << " nextSeq change from:" << _nextTxnSeq
                << " to:" << maxBinlogid + 1;
            maxCommitId = maxBinlogid;

            std::lock_guard<std::mutex> lk(_mutex);
            _nextTxnSeq = maxCommitId + 1;
            _nextBinlogSeq = _nextTxnSeq;
            _highestVisible = maxCommitId;
        }
    }
  }
    return maxCommitId;
}

RocksKVStore::RocksKVStore(const std::string& id,
            const std::shared_ptr<ServerParams>& cfg,
            std::shared_ptr<rocksdb::Cache> blockCache,
            bool enableRepllog,
            KVStore::StoreMode mode,
            TxnMode txnMode)
        :KVStore(id, cfg->dbPath),
         _cfg(cfg),
         _isRunning(false),
         _isPaused(false),
         _hasBackup(false),
         _enableFilter(true),
         _enableRepllog(enableRepllog),
         _mode(mode),
         _txnMode(txnMode),
         _optdb(nullptr),
         _pesdb(nullptr),
         _stats(rocksdb::CreateDBStatistics()),
         _blockCache(blockCache),
         _nextTxnSeq(0),
         _highestVisible(Transaction::TXNID_UNINITED),
         _logOb(nullptr)
         {
    if (_cfg->noexpire) {
        _enableFilter = false;
    }
    Expected<uint64_t> s = restart(false);
    if (!s.ok()) {
        LOG(FATAL) << "opendb:" << _cfg->dbPath << "/" << id
                    << ", failed info:" << s.status().toString();
    }
    initRocksProperties();
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
    });

    // NOTE(deyukong):
    // tendis uses BackupMode::BACKUP_COPY, we should keep compatible.
    // For tendisplus master/slave initial-sync, we use BackupMode::BACKUP_CKPT,
    // it's faster. here we assume BACKUP_CKPT works with default backupdir and
    // BACKUP_COPY works with arbitory dir except the default one.
    // But if someone feels it necessary to add one more param make it clearer,
    // go ahead.
    if (mode == KVStore::BackupMode::BACKUP_CKPT_INTER) {
        // BACKUP_CKPT_INTER works with the default backupdir and _hasBackup flag.
        if (dir != dftBackupDir()) {
            return {ErrorCodes::ERR_INTERNAL, "BACKUP_CKPT_INTER invalid dir"};
        }
        std::lock_guard<std::mutex> lk(_mutex);
        if (_hasBackup) {
            return {ErrorCodes::ERR_INTERNAL, "already have backup"};
        }
        _hasBackup = true;
    } else {
        if (dir == dftBackupDir()) {
            return {ErrorCodes::ERR_INTERNAL, "BACKUP_CKPT|BACKUP_COPY cant equal dftBackupDir:" + dir};
        }
    }

    // NOTE(deyukong): we should get highVisible before making a ckpt
    BackupInfo result;
    uint64_t highVisible = getHighestBinlogId();
    if (highVisible == Transaction::TXNID_UNINITED) {
        LOG(WARNING) << "store:" << dbId() << " highVisible still zero";
    }
    result.setBinlogPos(highVisible);
    result.setStartTimeSec(sinceEpoch());
    if (mode == KVStore::BackupMode::BACKUP_CKPT ||
        mode == KVStore::BackupMode::BACKUP_CKPT_INTER) {
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
    result.setFileList(flist);
    result.setEndTimeSec(sinceEpoch());
    result.setBackupMode((uint32_t)mode);
    auto saveret = saveBackupMeta(dir, result);
    if (!saveret.ok()) {
        return saveret.status();
    }
    succ = true;
    return result;
}

Expected<std::string> RocksKVStore::saveBackupMeta(const std::string& dir, const BackupInfo& backup) {
    rapidjson::StringBuffer sb;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(sb);
    writer.StartObject();
    writer.Key("backupType");
    writer.Uint(backup.getBackupMode());
    writer.Key("binlogpos");
    writer.Uint64(backup.getBinlogPos());
    writer.Key("startTimeSec");
    writer.Uint64(backup.getStartTimeSec());
    writer.Key("endTimeSec");
    writer.Uint64(backup.getEndTimeSec());
    writer.Key("useTimeSec");
    writer.Uint64(backup.getEndTimeSec() - backup.getStartTimeSec());
    writer.EndObject();
    string data = sb.GetString();

    string filename = dir + "/backup_meta";
    std::ofstream metafile(filename);
    if (!metafile.is_open()) {
        return {ErrorCodes::ERR_INTERNAL, "open file failed:" + filename};
    }
    metafile << data;
    metafile.close();
    return std::string("ok");
}

Expected<rapidjson::Document> RocksKVStore::getBackupMeta(const std::string& dir) {
    string filename = dir + "/backup_meta";
    std::ifstream metafile(filename);
    if (!metafile.is_open()) {
        LOG(ERROR) << "backup_meta open failed:" << filename;
        return {ErrorCodes::ERR_INTERNAL, "open file failed:" + filename};
    }
    std::stringstream ss;
    ss << metafile.rdbuf();
    metafile.close();

    rapidjson::Document doc;
    doc.Parse(ss.str());
    if (doc.HasParseError()) {
        LOG(ERROR) << "backup_meta parse json failed:" << filename;
        return {ErrorCodes::ERR_NETWORK,
                rapidjson::GetParseError_En(doc.GetParseError())};
    }
    if (!doc.IsObject()) {
        LOG(ERROR) << "backup_meta json IsObject failed:" << filename;
        return {ErrorCodes::ERR_NOTFOUND, "json parse failed"};
    }
    return doc;
}

Expected<std::string> RocksKVStore::restoreBackup(const std::string& dir) {
    auto backup_meta = getBackupMeta(dir);
    if (!backup_meta.ok()) {
        return backup_meta.status();
    }

#ifdef _WIN32
#undef GetObject
#endif
    uint32_t mode = std::numeric_limits<uint32_t>::max();
    for (auto& o : backup_meta.value().GetObject()) {
        if (o.name == "backupType" && o.value.IsUint()) {
            mode = o.value.GetUint();
        }
    }

    if (mode == (uint32_t)KVStore::BackupMode::BACKUP_CKPT) {
        return copyCkpt(dir);
    } else if (mode == (uint32_t)KVStore::BackupMode::BACKUP_COPY) {
        return loadCopy(dir);
    }
    LOG(ERROR) << "restoreBackup mode failed:" << dir << " mode:" << mode;
    return {ErrorCodes::ERR_NOTFOUND, "mode error"};
}

Expected<std::string> RocksKVStore::loadCopy(const std::string& dir) {
    rocksdb::BackupEngineReadOnly* backup_engine;
    rocksdb::Status s = rocksdb::BackupEngineReadOnly::Open(
        rocksdb::Env::Default(), rocksdb::BackupableDBOptions(dir),
        &backup_engine);
    if (!s.ok()) {
        LOG(ERROR) << "BackupEngineReadOnly::Open failed."
            << s.ToString() << " dir:" << dir;
        return {ErrorCodes::ERR_INTERNAL, s.ToString()};
    }
    std::unique_ptr<rocksdb::BackupEngineReadOnly> pBkEngine(backup_engine);
    const std::string path = dbPath() + "/" + dbId();
    // restore from backup_dir to _dbPath
    s = pBkEngine->RestoreDBFromLatestBackup(path, path);
    if (!s.ok()) {
        LOG(ERROR) << "RestoreDBFromLatestBackup failed."
            << s.ToString() << " dir:" << dir;
        return {ErrorCodes::ERR_INTERNAL, s.ToString()};
    }
    LOG(INFO) << "loadCopy sucess. dbpath:" << path << " backup path:" << dir;
    return std::string("ok");
}

Expected<std::string> RocksKVStore::copyCkpt(const std::string& dir) {
    try {
        const std::string path = dbPath() + "/" + dbId();
        if (filesystem::exists(path)) {
            std::stringstream ss;
            ss << "path:" << path
               << " should not exist when restore";
            return {ErrorCodes::ERR_INTERNAL, ss.str()};
        }
        if (!filesystem::exists(dir)) {
            std::stringstream ss;
            ss << "recover path:" << dir
               << " not exist when restore";
            return {ErrorCodes::ERR_INTERNAL, ss.str()};
        }
        filesystem::copy(dir, path);
    } catch(std::exception& ex) {
        LOG(WARNING) << "dbId:" << dbId()
                     << "restore exception" << ex.what();
        return {ErrorCodes::ERR_INTERNAL, ex.what()};
    }
    return std::string("ok");
}

Expected<std::unique_ptr<Transaction>> RocksKVStore::createTransaction(Session* sess) {
    std::lock_guard<std::mutex> lk(_mutex);
    if (!_isRunning) {
        return {ErrorCodes::ERR_INTERNAL, "db stopped!"};
    }
    uint64_t txnId = _nextTxnSeq++;
    bool replOnly = (_mode == KVStore::StoreMode::REPLICATE_ONLY);
#ifndef NO_VERSIONEP
    if (sess) {
        // NOTE(vinchen): In some cases, it should do some writes in a
        // replonly KVStore, such as "flushalldisk"
        replOnly = sess->getCtx()->isReplOnly();
    }
#endif
    std::unique_ptr<Transaction> ret = nullptr;
    // TODO(vinchen): should new RocksTxn out of mutex?
    if (_txnMode == TxnMode::TXN_OPT) {
        ret.reset(new RocksOptTxn(this, txnId, replOnly, _logOb, sess));
    } else {
        ret.reset(new RocksPesTxn(this, txnId, replOnly, _logOb, sess));
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

    _nextBinlogSeq = binlogId + 1;

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
    INVARIANT_D(it != _aliveTxns.end());
    INVARIANT_D(!it->second.first);

    it->second.first = true;
    auto binlogId = it->second.second;
    _aliveTxns.erase(it);

    if (binlogId != Transaction::TXNID_UNINITED) {
        auto i = _aliveBinlogs.find(binlogId);
        INVARIANT_D(i != _aliveBinlogs.end());
        INVARIANT_D(i->second.second == txnId ||
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
                    DLOG(INFO) << "markCommittedInLock dbid:" << dbId()
                        << " _highestVisible:"<< _highestVisible;
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
    //DLOG(INFO) << "setKV storeid:"<< this->dbId() <<"key:" << key.getPrimaryKey() << " skey:" << key.getSecondaryKey() << " value:" << value.getValue();
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

void RocksKVStore::initRocksProperties()
{
    _rocksIntProperties = {
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
        {"rocksdb.num-immutable-mem-table-flushed", "num-immutable-mem-table-flushed"},
    };

    _rocksStringProperties = {
        {"rocksdb.stats", "stats"},
        {"rocksdb.sstables", "sstables"},
        {"rocksdb.cfstats", "cfstats"},
        {"rocksdb.cfstats-no-file-histogram", "cfstats-no-file-histogram"},
        {"rocksdb.cf-file-histogram", "cf-file-histogram"},
        {"rocksdb.dbstats", "dbstats"},
        {"rocksdb.levelstats", "levelstats"},
        {"rocksdb.aggregated-table-properties", "aggregated-table-properties"},
        {"rocksdb.num-files-at-level0", "num-files-at-level0"},
        //{"rocksdb.estimate-oldest-key-time", "estimate-oldest-key-time"},
    };
    for (int i = 0; i < ROCKSDB_NUM_LEVELS; ++i) {
        _rocksStringProperties["rocksdb.num-files-at-level" + std::to_string(i)] = "num_files_at_level" + std::to_string(i);
        _rocksStringProperties["rocksdb.compression-ratio-at-level" + std::to_string(i)] = "compression-ratio-at-level" + std::to_string(i);
        _rocksStringProperties["rocksdb.aggregated-table-properties-at-level" + std::to_string(i)] = "aggregated-table-properties-at-level" + std::to_string(i);
    }
}

bool RocksKVStore::getIntProperty(const std::string& property, uint64_t* value) const {
    bool ok = false;
    if (_isRunning) {
        ok = getBaseDB()->GetIntProperty(property, value);
        if (!ok) {
            LOG(WARNING) << "db:" << dbId()
                << " getProperty:" << property << " failed";
        }
    }
    return ok;
}

bool RocksKVStore::getProperty(const std::string& property, std::string* value) const {
    bool ok = false;
    if (_isRunning) {
        ok = getBaseDB()->GetProperty(property, value);
        if (!ok) {
            LOG(WARNING) << "db:" << dbId()
                << " getProperty:" << property << " failed";
        }
    }

    return ok;
}

std::string RocksKVStore::getAllProperty() const {
    std::stringstream ss;
    if (_isRunning) {
        std::string tmp;
        for (const auto& kv : _rocksIntProperties) {
            bool ok = getProperty(kv.first, &tmp);
            if (!ok) {
                continue;
            }
            ss << kv.first << ":" << tmp << "\r\n";
        }

        for (const auto& kv : _rocksStringProperties) {
            bool ok = getProperty(kv.first, &tmp);
            if (!ok) {
                continue;
            }
            ss << kv.first << ":" << tmp << "\r\n";
        }
    }

    return ss.str();
}

std::string RocksKVStore::getStatistics() const {
    if (_isRunning) {
        return _stats->ToString();
    } else {
        return "";
    }
}

void RocksKVStore::resetStatistics() {
    _stats->Reset();
}

void RocksKVStore::appendJSONStat(
            rapidjson::PrettyWriter<rapidjson::StringBuffer>& w) const {
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
        for (const auto& kv : _rocksIntProperties) {
            uint64_t tmp;
            bool ok = getIntProperty(kv.first, &tmp);
            if (!ok) {
                continue;
            }
            w.Key(kv.second.c_str());
            w.Uint64(tmp);
        }

        for (const auto& kv : _rocksStringProperties) {
            string tmp;
            bool ok = getProperty(kv.first, &tmp);
            if (!ok) {
                continue;
            }
            w.Key(kv.second.c_str());
            w.String(tmp);
        }

        sstMetaData level_summary[ROCKSDB_NUM_LEVELS];
        std::vector<rocksdb::LiveFileMetaData> metadata;
        getBaseDB()->GetLiveFilesMetaData(&metadata);
        for (size_t i = 0; i < metadata.size(); ++i) {
            int level = metadata[i].level;
            sstMetaData& meta = level_summary[level];

            meta.size += metadata[i].size;
            meta.num_entries += metadata[i].num_entries;
            meta.num_deletions += metadata[i].num_deletions;
        }

        w.Key("RocksDB Level stats");
        w.StartObject();
        for (size_t i = 0; i < ROCKSDB_NUM_LEVELS; ++i) {
            w.Key("level_" + std::to_string(i));
            w.StartObject();

            w.Key("size");
            w.Uint64(level_summary[i].size);
            w.Key("num_entries");
            w.Uint64(level_summary[i].num_entries);
            w.Key("num_deletions");
            w.Uint64(level_summary[i].num_deletions);

            w.EndObject();
        }
        w.EndObject();

    }
    w.EndObject();
}

}  // namespace tendisplus
