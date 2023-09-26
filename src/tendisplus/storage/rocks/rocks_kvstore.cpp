// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include "tendisplus/storage/rocks/rocks_kvstore.h"

#include <algorithm>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "rapidjson/document.h"
#include "rapidjson/error/en.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/slice.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/utilities/backup_engine.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/utilities/table_properties_collectors.h"

#include "tendisplus/server/server_entry.h"
#include "tendisplus/server/session.h"
#include "tendisplus/storage/rocks/rocks_kvttlcompactfilter.h"
#include "tendisplus/storage/varint.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/time.h"
#include "tendisplus/utils/time_record.h"

namespace tendisplus {

#ifndef NO_VERSIONEP
#define RESET_PERFCONTEXT()                                      \
  do {                                                           \
    if (_session && _session->getCtx()->needResetPerLevel()) {   \
      rocksdb::SetPerfLevel(                                     \
        rocksdb::PerfLevel(_session->getCtx()->getPerfLevel())); \
      rocksdb::get_perf_context()->Reset();                      \
      rocksdb::get_iostats_context()->Reset();                   \
    }                                                            \
  } while (0)
#else
#define RESET_PERFCONTEXT()
#endif

RocksKVCursor::RocksKVCursor(std::unique_ptr<rocksdb::Iterator> it)
  : Cursor(), _it(std::move(it)), _seeked(false) {}

void RocksKVCursor::seek(const std::string& prefix) {
  _it->Seek(rocksdb::Slice(prefix.c_str(), prefix.size()));
  _seeked = true;
}

void RocksKVCursor::seekToLast() {
  _it->SeekToLast();
  _seeked = true;
}

Expected<Record> RocksKVCursor::next() {
  INVARIANT(_seeked);
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
  INVARIANT(_seeked);
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
  INVARIANT(_seeked);
  if (!_it->status().ok()) {
    return {ErrorCodes::ERR_INTERNAL, _it->status().ToString()};
  }

  if (!_it->Valid()) {
    return {ErrorCodes::ERR_EXHAUST, "no more data"};
  }

  return _it->key().ToString();
}

RocksTxn::RocksTxn(RocksKVStore* store,
                   uint64_t txnId,
                   bool replOnly,
                   std::shared_ptr<tendisplus::BinlogObserver> ob,
                   Session* sess,
                   TxnMode txnMode,
                   uint64_t binlogId,
                   uint32_t chunkId)
  : _txnId(txnId),
    _binlogId(binlogId),
    _chunkId(chunkId),
    _txnMode(txnMode),
    _txn(nullptr),
    _store(store),
    _done(false),
    _replOnly(replOnly),
    _logOb(ob),
    _session(sess) {
  _writeOpts = _store->writeOptions();
}

RocksTxn::~RocksTxn() {
  if (_done) {
    return;
  }

  // NOTE(vinchen): make sure whether is there any command
  // forget to commit or rollback
  INVARIANT_D(_replLogValues.size() == 0);

  // _txn.get()->ClearSnapshot();
  _txn.reset();
  _store->markCommitted(_txnId, Transaction::TXNID_UNINITED);
}

std::unique_ptr<RepllogCursorV2> RocksTxn::createRepllogCursorV2(
  uint64_t begin, bool ignoreReadBarrier) {
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

std::unique_ptr<TTLIndexCursor> RocksTxn::createTTLIndexCursor(uint64_t until) {
  RecordKey upper(TTLIndex::CHUNKID + 1, 0, RecordType::RT_INVALID, "", "");
  string upperBound = upper.prefixChunkid();
  auto cursor =
    createCursor(ColumnFamilyNumber::ColumnFamily_Default, &upperBound);
  return std::make_unique<TTLIndexCursor>(std::move(cursor), until);
}

std::unique_ptr<SlotCursor> RocksTxn::createSlotCursor(uint32_t slot) {
  RecordKey chunkMax(slot + 1, 0, RecordType::RT_INVALID, "", "");
  string upperbound = chunkMax.prefixChunkid();
  auto cursor =
    createCursor(ColumnFamilyNumber::ColumnFamily_Default, &upperbound);
  return std::make_unique<SlotCursor>(std::move(cursor), slot);
}

unique_ptr<SlotsCursor> RocksTxn::createSlotsCursor(uint32_t start,
                                                    uint32_t end) {
  RecordKey chunkMax(end + 1, 0, RecordType::RT_INVALID, "", "");
  string upperbound = chunkMax.prefixChunkid();
  auto cursor =
    createCursor(ColumnFamilyNumber::ColumnFamily_Default, &upperbound);
  return std::make_unique<SlotsCursor>(std::move(cursor), start, end);
}

std::unique_ptr<VersionMetaCursor> RocksTxn::createVersionMetaCursor() {
  RecordKey chunkMax(
    VersionMeta::CHUNKID + 1, 0, RecordType::RT_INVALID, "", "");
  string upperbound = chunkMax.prefixChunkid();
  auto cursor =
    createCursor(ColumnFamilyNumber::ColumnFamily_Default, &upperbound);
  return std::make_unique<VersionMetaCursor>(std::move(cursor));
}

std::unique_ptr<BasicDataCursor> RocksTxn::createDataCursor() {
  auto cursor = createCursor(ColumnFamilyNumber::ColumnFamily_Default);
  return std::make_unique<BasicDataCursor>(std::move(cursor));
}

std::unique_ptr<AllDataCursor> RocksTxn::createAllDataCursor() {
  auto cursor = createCursor(ColumnFamilyNumber::ColumnFamily_Default);
  return std::make_unique<AllDataCursor>(std::move(cursor));
}

std::unique_ptr<BinlogCursor> RocksTxn::createBinlogCursor() {
  auto cursor = createCursor(ColumnFamilyNumber::ColumnFamily_Binlog);
  return std::make_unique<BinlogCursor>(std::move(cursor));
}

std::unique_ptr<Cursor> RocksTxn::createCursor(
  ColumnFamilyNumber column_family_num,
  const std::string* iterate_upper_bound) {
  rocksdb::ReadOptions readOpts;

  // NOTE: If force_recovery != 0, ignore verify checksums
  if (_store->recoveryMode()) {
    readOpts.verify_checksums = false;
  }

  RESET_PERFCONTEXT();
  if (iterate_upper_bound != NULL) {
    _strUpperBound = *iterate_upper_bound;
    _upperBound = rocksdb::Slice(_strUpperBound);
    readOpts.iterate_upper_bound = &_upperBound;
  }
  // create iterator corresponding to chosen column family
  rocksdb::Iterator* iter;
  rocksdb::ColumnFamilyHandle* handle =
    _store->getColumnFamilyHandle(column_family_num);

  readOpts.snapshot = getSnapshot();
  iter = getIterator(readOpts, handle);

  return std::unique_ptr<Cursor>(
    new RocksKVCursor(std::unique_ptr<rocksdb::Iterator>(iter)));
}

Expected<uint64_t> RocksTxn::commit() {
  INVARIANT_D(!_done);
  _done = true;

  uint64_t binlogTxnId = Transaction::TXNID_UNINITED;
  const auto guard = MakeGuard([this, &binlogTxnId] {
    _txn.reset();
    // for non-replonly mode, we should have binlogTxnId == _txnId
    if (!_replOnly) {
      INVARIANT_D(binlogTxnId == _txnId ||
                  binlogTxnId == Transaction::TXNID_UNINITED);
    }
    _store->markCommitted(_txnId, binlogTxnId);

#ifdef TENDIS_DEBUG
    // NOTE(takenliu) for test case psyncEnabled,
    //   we need update the slave binlogtime
    if (_store->getCfg()->psyncEnabled && _session &&
        _session->getCtx()->isMaster()) {
      auto storeId = tendisplus::stoul(_store->dbId());
      if (!storeId.ok()) {
        LOG(ERROR) << "error dbid:" << _store->dbId();
        return;
      }
      _session->getServerEntry()->getReplManager()->updateSyncTime(
        storeId.value());
    }
#endif
  });
  if (_txnMode != TxnMode::TXN_WB && _txn == nullptr) {
    return {ErrorCodes::ERR_OK, ""};
  }

  if (_replLogValues.size() != 0) {
    // NOTE(vinchen): for repl, binlog should be inserted by setBinlogKV()
    INVARIANT_D(!isReplOnly());

    if (_replLogValues.size() >= std::numeric_limits<uint16_t>::max()) {
      LOG(WARNING) << "too big binlog size:",
        std::to_string(_replLogValues.size());
    }

    _store->assignBinlogIdIfNeeded(this);
    INVARIANT_D(_binlogId != Transaction::TXNID_UNINITED);

    uint32_t chunkId = getChunkId();

    // NOTE(vinchen): Now, one transaction one repllog, so the flag is
    // (START | END)
    uint16_t oriFlag = static_cast<uint16_t>(ReplFlag::REPL_GROUP_START) |
      static_cast<uint16_t>(ReplFlag::REPL_GROUP_END);

    std::string sessionStr = "";
    if (_session && _session->getArgs().size() > 0) {
      sessionStr = _session->getServerEntry()->getParams()->aofEnabled
        ? _session->getSessionCmd()
        : _session->getArgs()[0];
    }
    ReplLogKeyV2 key(_binlogId);
    ReplLogValueV2 val(chunkId,
                       static_cast<ReplFlag>(oriFlag),
                       _txnId,
                       _replLogValues.back().getTimestamp(),
#ifndef NO_VERSIONEP
                       _session ? _session->getCtx()->getVersionEP()
                                : SessionCtx::VERSIONEP_UNINITED,
                       sessionStr,
#else
                       SessionCtx::VERSIONEP_UNINITED,
                       "",
#endif
                       nullptr,
                       0);

    binlogTxnId = _txnId;
    // put binlog into binlog_column_family
    rocksdb::ColumnFamilyHandle* handle = _store->getBinlogColumnFamilyHandle();
    auto s = put(handle, key.encode(), val.encode(_replLogValues));
    if (!s.ok()) {
      binlogTxnId = Transaction::TXNID_UNINITED;
      return _store->handleRocksdbError(s);
    }
  }
  if (isReplOnly() && _binlogId != Transaction::TXNID_UNINITED) {
    // NOTE(vinchen): for slave, binlog form master store directly
    binlogTxnId = _txnId;
  }

  TEST_SYNC_POINT("RocksTxn::commit()::1");
  TEST_SYNC_POINT("RocksTxn::commit()::2");
  auto s = txnCommit();
  if (s.ok()) {
    return _txnId;
  } else {
    binlogTxnId = Transaction::TXNID_UNINITED;
    if (s.IsBusy() || s.IsTryAgain()) {
      return {ErrorCodes::ERR_COMMIT_RETRY, s.ToString()};
    } else {
      return _store->handleRocksdbError(s);
    }
  }
}

Status RocksTxn::rollback() {
  INVARIANT_D(!_done);
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
    return _store->handleRocksdbError(s);
  }
}

uint64_t RocksTxn::getTxnId() const {
  return _txnId;
}

std::string RocksTxn::getKVStoreId() const {
  return _store->dbId();
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

  // NOTE: If force_recovery != 0, ignore verify checksums
  if (_store->recoveryMode()) {
    readOpts.verify_checksums = false;
  }
  // If read_options.snapshot is not set, the current version of the key will
  // be read.  Calling SetSnapshot() does not affect the version of the data
  // returned. See Transaction::Get() for more details.
  // If we use Snapshot Isolation isolation level, SetSnapshot() should be
  // called before Put/Get. Snapshot Isolation was used in these conditions:
  //  1) ChunkMigrateSender::initTxn()
  //  2) ReplManager::supplyFullPsyncRoutine()
  readOpts.snapshot = getSnapshot();

  RESET_PERFCONTEXT();
  rocksdb::Status s;
  rocksdb::ColumnFamilyHandle* handle =
    _store->getColumnFamilyHandleByRecordType(RecordKey::decodeType(key));
  s = get(readOpts, handle, key, &value);
  if (s.ok()) {
    return value;
  }
  if (s.IsNotFound()) {
    return {ErrorCodes::ERR_NOTFOUND, s.ToString()};
  }
  return _store->handleRocksdbError(s);
}

Status RocksTxn::setKV(const std::string& key,
                       const std::string& val,
                       const uint64_t ts) {
  if (_replOnly) {
    return {ErrorCodes::ERR_INTERNAL, "txn is replOnly"};
  }

  RESET_PERFCONTEXT();
  // put data into default column family
  auto s = put(key, val);
  if (!s.ok()) {
    return _store->handleRocksdbError(s);
  }

  if (_store->enableRepllog()) {
    INVARIANT_D(_store->dbId() != CATALOG_NAME);
    setChunkId(RecordKey::decodeChunkId(key));
    if (_replLogValues.size() == std::numeric_limits<uint16_t>::max()) {
      // TODO(vinchen): if too large, it can flush to rocksdb first,
      // and get another binlogid using assignBinlogIdIfNeeded()
      auto eKey = RecordKey::decode(key);
      if (eKey.ok()) {
        LOG(WARNING) << "setKV too big binlog size, key:" +
            eKey.value().getPrimaryKey();
      } else {
        LOG(WARNING) << "setKV too big binlog size, invalid key:" + key;
      }
    }

    ReplLogValueEntryV2 logVal(
      ReplOp::REPL_OP_SET, ts ? ts : msSinceEpoch(), key, val);
    // TODO(vinchen): maybe OOM
    _replLogValues.emplace_back(std::move(logVal));
  }
  return {ErrorCodes::ERR_OK, ""};
}

Status RocksTxn::setKVWithoutBinlog(const std::string& key,
                                    const std::string& val) {
  RESET_PERFCONTEXT();
  rocksdb::Status s;
  // put data into default column family
  s = put(key, val);
  if (!s.ok()) {
    return _store->handleRocksdbError(s);
  }
  return {ErrorCodes::ERR_OK, ""};
}

Status RocksTxn::delKV(const std::string& key, const uint64_t ts) {
  if (_replOnly) {
    return {ErrorCodes::ERR_INTERNAL, "txn is replOnly"};
  }
  RESET_PERFCONTEXT();
  rocksdb::Status s;
  rocksdb::ColumnFamilyHandle* handle =
    _store->getColumnFamilyHandleByRecordType(RecordKey::decodeType(key));
  s = del(handle, key);
  if (!s.ok()) {
    return _store->handleRocksdbError(s);
  }

  if (_store->enableRepllog()) {
    INVARIANT_D(_store->dbId() != CATALOG_NAME);
    setChunkId(RecordKey::decodeChunkId(key));
    if (_replLogValues.size() == std::numeric_limits<uint16_t>::max()) {
      // TODO(vinchen): if too large, it can flush to rocksdb first,
      // and get another binlogid using assignBinlogIdIfNeeded()
      auto eKey = RecordKey::decode(key);
      if (eKey.ok()) {
        LOG(WARNING) << "delKV too big binlog size, key:" +
            eKey.value().getPrimaryKey();
      } else {
        LOG(WARNING) << "delKV too big binlog size, invalid key:" + key;
      }
    }
    ReplLogValueEntryV2 logVal(
      ReplOp::REPL_OP_DEL, ts ? ts : msSinceEpoch(), key, "");
    // TODO(vinchen): maybe OOM
    _replLogValues.emplace_back(std::move(logVal));
  }
  return {ErrorCodes::ERR_OK, ""};
}

Status RocksTxn::addDeleteRangeBinlog(const std::string& begin,
                                      const std::string& end) {
  if (_replOnly) {
    return {ErrorCodes::ERR_INTERNAL, "txn is replOnly"};
  }

  if (_store->enableRepllog()) {
    INVARIANT_D(_store->dbId() != CATALOG_NAME);
    setChunkId(Transaction::CHUNKID_DEL_RANGE);
    if (_replLogValues.size() >= 1) {
      LOG(WARNING) << "deleteRange too big binlog size, begin:" << begin
                   << " end:" << end;
    }
    ReplLogValueEntryV2 logVal(
      ReplOp::REPL_OP_DEL_RANGE, msSinceEpoch(), begin, end);
    _replLogValues.emplace_back(std::move(logVal));
  }
  return {ErrorCodes::ERR_OK, ""};
}

Status RocksTxn::addDeleteFilesInRangeBinlog(const std::string& begin,
                                             const std::string& end,
                                             bool include_end) {
  if (_replOnly) {
    return {ErrorCodes::ERR_INTERNAL, "txn is replOnly"};
  }

  if (_store->enableRepllog()) {
    INVARIANT_D(_store->dbId() != CATALOG_NAME);
    setChunkId(Transaction::CHUNKID_DEL_RANGE);
    ReplLogValueEntryV2 logVal(include_end
                                 ? ReplOp::REPL_OP_DEL_FILES_INCLUDE_END
                                 : ReplOp::REPL_OP_DEL_FILES_EXCLUDE_END,
                               msSinceEpoch(),
                               begin,
                               end);
    _replLogValues.emplace_back(std::move(logVal));
  }
  return {ErrorCodes::ERR_OK, ""};
}

Status RocksTxn::flushall() {
  if (_replOnly) {
    return {ErrorCodes::ERR_INTERNAL, "txn is replOnly"};
  }
  if (!_store->enableRepllog()) {
    return {ErrorCodes::ERR_INTERNAL, "repllog is not enable"};
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

  ReplLogValueEntryV2 logVal(
    ReplOp::REPL_OP_STMT, _store->getCurrentTime(), cmd, "");
  _replLogValues.emplace_back(std::move(logVal));
  return {ErrorCodes::ERR_OK, ""};
}

Status RocksTxn::migrate(const std::string& logKey, const std::string& logVal) {
  if (_replOnly) {
    return {ErrorCodes::ERR_INTERNAL, "txn is replOnly"};
  }
  if (!_store->enableRepllog()) {
    return {ErrorCodes::ERR_INTERNAL, "repllog is not enable"};
  }

  INVARIANT_D(_store->dbId() != CATALOG_NAME);
  setChunkId(Transaction::CHUNKID_MIGRATE);
  INVARIANT_D(_replLogValues.size() == 0);

  // it's no use
  ReplLogValueEntryV2 logEntry(
    ReplOp::REPL_OP_SPEC, _store->getCurrentTime(), "", "");
  _replLogValues.emplace_back(std::move(logEntry));
  return {ErrorCodes::ERR_OK, ""};
}

Status RocksTxn::applyBinlog(const ReplLogValueEntryV2& logEntry) {
  if (!_replOnly) {
    return {ErrorCodes::ERR_INTERNAL, "txn is not replOnly or migrationOnly"};
  }
  rocksdb::Status s;
  rocksdb::ColumnFamilyHandle* handle = _store->getDataColumnFamilyHandle();
  RESET_PERFCONTEXT();
  switch (logEntry.getOp()) {
    case ReplOp::REPL_OP_SET: {
      s = put(handle, logEntry.getOpKey(), logEntry.getOpValue());
      if (!s.ok()) {
        return _store->handleRocksdbError(s);
      }
      break;
    }
    case ReplOp::REPL_OP_DEL: {
      s = del(handle, logEntry.getOpKey());
      if (!s.ok()) {
        return _store->handleRocksdbError(s);
      }
      break;
    }
    case ReplOp::REPL_OP_STMT: {
      INVARIANT_D(0);
    }
    case ReplOp::REPL_OP_SPEC: {
      INVARIANT_D(0);
    }
    case ReplOp::REPL_OP_DEL_RANGE: {
      auto s = _store->deleteRangeWithoutBinlog(
        handle, logEntry.getOpKey(), logEntry.getOpValue());
      RET_IF_ERR(s);
      break;
    }
    case ReplOp::REPL_OP_DEL_FILES_INCLUDE_END: {
      auto s = _store->deleteFilesInRangeWithoutBinlog(
        _store->getDataColumnFamilyHandle(),
        logEntry.getOpKey(),
        logEntry.getOpValue(),
        true);
      RET_IF_ERR(s);
      break;
    }
    case ReplOp::REPL_OP_DEL_FILES_EXCLUDE_END: {
      auto s = _store->deleteFilesInRangeWithoutBinlog(
        _store->getDataColumnFamilyHandle(),
        logEntry.getOpKey(),
        logEntry.getOpValue(),
        false);
      RET_IF_ERR(s);
      break;
    }
    default:
      INVARIANT_D(0);
      return {ErrorCodes::ERR_DECODE, "not a valid binlog"};
  }

  return {ErrorCodes::ERR_OK, ""};
}

Status RocksTxn::setBinlogKV(uint64_t binlogId,
                             const std::string& logKey,
                             const std::string& logValue) {
  if (!_replOnly) {
    return {ErrorCodes::ERR_INTERNAL, "txn is not replOnly"};
  }

  // NOTE(vinchen): Because the (logKey, logValue) from the master store in
  // slave's rocksdb directly, we should change the _nextBinlogSeq.
  // BTW, the txnid of logValue is different from _txnId. But it's ok.
  _store->setNextBinlogSeq(binlogId, this);
  INVARIANT_D(_binlogId != Transaction::TXNID_UNINITED);
  rocksdb::ColumnFamilyHandle* handle = _store->getBinlogColumnFamilyHandle();
  RESET_PERFCONTEXT();
  auto s = put(handle, logKey, logValue);
  if (!s.ok()) {
    return _store->handleRocksdbError(s);
  }

  return {ErrorCodes::ERR_OK, ""};
}

Status RocksTxn::setBinlogKV(const std::string& key, const std::string& value) {
  Expected<ReplLogKeyV2> logkey = ReplLogKeyV2::decode(key);
  if (!logkey.ok()) {
    cerr << "decode logkey failed." << endl;
    return {ErrorCodes::ERR_INTERGER, "ReplLogKeyV2::decode failed"};
  }

  // NOTE(takenliu) use self _binlogId to replace sender's binlogId.
  _store->assignBinlogIdIfNeeded(this);
  INVARIANT_D(_binlogId != Transaction::TXNID_UNINITED);
  logkey.value().setBinlogId(_binlogId);

  rocksdb::ColumnFamilyHandle* handle = _store->getBinlogColumnFamilyHandle();
  auto s = put(handle, logkey.value().encode(), value);
  if (!s.ok()) {
    return _store->handleRocksdbError(s);
  }
  return {ErrorCodes::ERR_OK, ""};
}

Status RocksTxn::delBinlog(const ReplLogRawV2& log) {
  RESET_PERFCONTEXT();
  rocksdb::ColumnFamilyHandle* handle = _store->getBinlogColumnFamilyHandle();
  auto s = del(handle, log.getReplLogKey());
  if (!s.ok()) {
    return _store->handleRocksdbError(s);
  }

  return {ErrorCodes::ERR_OK, ""};
}

uint64_t RocksTxn::getBinlogId() const {
  return _binlogId;
}

void RocksTxn::setBinlogId(uint64_t binlogId) {
  INVARIANT_D(_binlogId == Transaction::TXNID_UNINITED);
  _binlogId = binlogId;
}

rocksdb::Status RocksTxn::put(rocksdb::ColumnFamilyHandle* columnFamily,
                              const std::string& key,
                              const std::string& val) {
  TENDIS_ROCKSDB_LATENCY_RECORD(_txn->Put(columnFamily, key, val),
                                (key.size() + val.size()),
                                RocksdbLatencyType::RLT_PUT);
}

rocksdb::Status RocksTxn::put(const std::string& key, const std::string& val) {
  rocksdb::ColumnFamilyHandle* columnFamily =
    _store->getDataColumnFamilyHandle();
  return put(columnFamily, key, val);
}

rocksdb::Status RocksTxn::get(const rocksdb::ReadOptions& options,
                              rocksdb::ColumnFamilyHandle* columnFamily,
                              const std::string& key,
                              std::string* value) {
  TENDIS_ROCKSDB_LATENCY_RECORD(_txn->Get(options, columnFamily, key, value),
                                (key.size()),
                                RocksdbLatencyType::RLT_GET);
}

rocksdb::Status RocksTxn::del(rocksdb::ColumnFamilyHandle* columnFamily,
                              const std::string& key) {
  TENDIS_ROCKSDB_LATENCY_RECORD(_txn->Delete(columnFamily, key),
                                (key.size()),
                                RocksdbLatencyType::RLT_DELETE);
}

rocksdb::Status RocksTxn::txnCommit() {
  TENDIS_ROCKSDB_LATENCY_RECORD(
    _txn->Commit(), size_t(0), RocksdbLatencyType::RLT_COMMIT);
}

const rocksdb::Snapshot* RocksTxn::getSnapshot() {
  return _txn->GetSnapshot();
}

rocksdb::Iterator* RocksTxn::getIterator(
  rocksdb::ReadOptions readOpts, rocksdb::ColumnFamilyHandle* columnFamily) {
  return _txn->GetIterator(readOpts, columnFamily);
}

RocksOptTxn::RocksOptTxn(RocksKVStore* store,
                         uint64_t txnId,
                         bool replOnly,
                         std::shared_ptr<tendisplus::BinlogObserver> ob,
                         Session* sess)
  : RocksTxn(store, txnId, replOnly, ob, sess, TxnMode::TXN_OPT) {
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
  INVARIANT_D(!_done);
  if (_txn != nullptr) {
    return;
  }
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
  _txn.reset(db->BeginTransaction(_writeOpts, txnOpts));
  INVARIANT(_txn != nullptr);
}

void RocksOptTxn::SetSnapshot() {
  INVARIANT(_txn != nullptr);
  _txn->SetSnapshot();
}

RocksPesTxn::RocksPesTxn(RocksKVStore* store,
                         uint64_t txnId,
                         bool replOnly,
                         std::shared_ptr<BinlogObserver> ob,
                         Session* sess)
  : RocksTxn(store, txnId, replOnly, ob, sess, TxnMode::TXN_PES) {
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
  INVARIANT_D(!_done);
  if (_txn != nullptr) {
    return;
  }
  rocksdb::TransactionOptions txnOpts;

  // NOTE(deyukong): the txn won't set a snapshot automaticly.
  // if set_snapshot == false, the RC-level is guaranteed.
  // if set_snapshot == true, the SI-level is guaranteed.
  // due to server-layer's keylock, RC-level can satisfy our
  // requirements. so here set_snapshot = false
  txnOpts.set_snapshot = false;
#if ROCKSDB_MAJOR > 5 || (ROCKSDB_MAJOR == 5 && ROCKSDB_MINOR > 17)
  txnOpts.skip_concurrency_control = _store->getCfg()->skipConcurrencyControl;
#endif

  auto db = _store->getUnderlayerPesDB();
  if (!db) {
    LOG(FATAL) << "BUG: rocksKVStore underLayerDB nil";
  }
  _txn.reset(db->BeginTransaction(_writeOpts, txnOpts));
  INVARIANT(_txn != nullptr);
}

void RocksPesTxn::SetSnapshot() {
  INVARIANT(_txn != nullptr);
  _txn->SetSnapshot();
}

RocksWBTxn::RocksWBTxn(RocksKVStore* store,
                       uint64_t txnId,
                       bool replOnly,
                       std::shared_ptr<BinlogObserver> ob,
                       Session* sess)
  : RocksTxn(store, txnId, replOnly, ob, sess, TxnMode::TXN_WB),
    _snapshot(nullptr) {
  // NOTE(deyukong): the rocks-layer's snapshot should be opened in
  // RocksKVStore::createTransaction, with the guard of RocksKVStore::_mutex,
  // or, we are not able to guarantee the oplog order is the same as the
  // local commit,
  // In other words, to the same key, a txn with greater id can be committed
  // before a txn with smaller id, and they have no conflicts, it's wrong.
  // so ensureTxn() should be done in RocksOptTxn's constructor
  ensureTxn();
  _writeBatch =
    new rocksdb::WriteBatchWithIndex(rocksdb::BytewiseComparator(), 0, true);
}

RocksWBTxn::~RocksWBTxn() {
  delete _writeBatch;
  if (_snapshot) {
    _store->getBaseDB()->ReleaseSnapshot(_snapshot);
  }
}

void RocksWBTxn::ensureTxn() {
  INVARIANT(_txn == nullptr);
  setTxnType(TxnMode::TXN_WB);
}

void RocksWBTxn::SetSnapshot() {
  INVARIANT(_txn == nullptr);
  if (!_snapshot) {
    _snapshot = const_cast<rocksdb::Snapshot*>(_store->getSnapshot());
    INVARIANT_D(_snapshot != nullptr);
  } else {
    INVARIANT_D(0);
  }
}

Status RocksWBTxn::rollback() {
  INVARIANT_D(!_done);
  _done = true;

  const auto guard = MakeGuard(
    [this] { _store->markCommitted(_txnId, Transaction::TXNID_UNINITED); });
  INVARIANT_D(_txn == nullptr);
  getWriteBatch()->Clear();
  return {ErrorCodes::ERR_OK, ""};
}

rocksdb::Status RocksWBTxn::put(rocksdb::ColumnFamilyHandle* columnFamily,
                                const std::string& key,
                                const std::string& val) {
  TENDIS_ROCKSDB_LATENCY_RECORD(_writeBatch->Put(columnFamily, key, val),
                                (key.size() + val.size()),
                                RocksdbLatencyType::RLT_PUT);
}

rocksdb::Status RocksWBTxn::put(const std::string& key,
                                const std::string& val) {
  rocksdb::ColumnFamilyHandle* columnFamily =
    _store->getDataColumnFamilyHandle();
  return put(columnFamily, key, val);
}

rocksdb::Status RocksWBTxn::get(const rocksdb::ReadOptions& options,
                                rocksdb::ColumnFamilyHandle* columnFamily,
                                const std::string& key,
                                std::string* value) {
  // TODO(jingjunli): ReadUncommited or ReadCommited ?
  // s = _store->getBaseDB()->Get(readOpts, handle, key, &value);
  TENDIS_ROCKSDB_LATENCY_RECORD(
    _writeBatch->GetFromBatchAndDB(
      _store->getBaseDB(), options, columnFamily, key, value),
    (key.size()),
    RocksdbLatencyType::RLT_GET);
}

rocksdb::Status RocksWBTxn::del(rocksdb::ColumnFamilyHandle* columnFamily,
                                const std::string& key) {
  TENDIS_ROCKSDB_LATENCY_RECORD(_writeBatch->Delete(columnFamily, key),
                                (key.size()),
                                RocksdbLatencyType::RLT_DELETE);
}

rocksdb::Status RocksWBTxn::txnCommit() {
  TENDIS_ROCKSDB_LATENCY_RECORD(
    _store->getBaseDB()->Write(_writeOpts, _writeBatch->GetWriteBatch()),
    size_t(0),
    RocksdbLatencyType::RLT_COMMIT);
}

const rocksdb::Snapshot* RocksWBTxn::getSnapshot() {
  return _snapshot;
}

rocksdb::Iterator* RocksWBTxn::getIterator(
  rocksdb::ReadOptions readOpts, rocksdb::ColumnFamilyHandle* columnFamily) {
  // rocksdb::Iterator* dbIter = _store->newIterator(readOpts, columnFamily);
  // TODO(jingjunli): ReadUncommited or ReadCommited ?
  // return _writeBatch->NewIteratorWithBase(columnFamily, dbIter);
  return _store->newIterator(readOpts, columnFamily);
}

rocksdb::CompressionType rocksGetCompressType(const std::string& typeStr) {
  if (typeStr == "snappy") {
    return rocksdb::CompressionType::kSnappyCompression;
  } else if (typeStr == "lz4") {
    return rocksdb::CompressionType::kLZ4Compression;
  } else if (typeStr == "none") {
    return rocksdb::CompressionType::kNoCompression;
  } else {
    INVARIANT_D(0);
    return rocksdb::CompressionType::kNoCompression;
  }
}

Status rocksdbOptionsSet(rocksdb::Options& options,
                         const std::string& key,
                         const std::string& rawValue) {
  // TODO(takenliu): not int params need change two place, resolve it
  static std::set<std::string> notIntParams = {
    "blob_compression_type",
    "blob_garbage_collection_age_cutoff",
  };

  int64_t value = 0;
  if (!notIntParams.count(key)) {
    auto ed = tendisplus::stoll(rawValue);
    if (ed.ok()) {
      value = ed.value();
    } else {
      LOG(FATAL) << "param need to be int64_t, rocks." << key << ":"
                 << rawValue;
      return {ErrorCodes::ERR_PARSEOPT, "invalid rocksdb option :" + key};
    }
  }
  // AdvancedColumnFamilyOptions
  if (key == "max_write_buffer_number") {
    options.max_write_buffer_number = static_cast<int>(value);
  } else if (key == "min_write_buffer_number_to_merge") {
    options.min_write_buffer_number_to_merge = static_cast<int>(value);
  } else if (key == "max_write_buffer_number_to_maintain") {
    options.max_write_buffer_number_to_maintain = static_cast<int>(value);
  } else if (key == "inplace_update_support") {
    options.inplace_update_support = static_cast<bool>(value);
  } else if (key == "inplace_update_num_locks") {
    options.inplace_update_num_locks = static_cast<size_t>(value);
#if ROCKSDB_MAJOR > 5 || (ROCKSDB_MAJOR == 5 && ROCKSDB_MINOR > 15)
  } else if (key == "memtable_whole_key_filtering") {
    options.memtable_whole_key_filtering = static_cast<bool>(value);
#endif
  } else if (key == "memtable_huge_page_size") {
    options.memtable_huge_page_size = static_cast<size_t>(value);
  } else if (key == "bloom_locality") {
    options.bloom_locality = static_cast<uint32_t>(value);
  } else if (key == "arena_block_size") {
    options.arena_block_size = static_cast<size_t>(value);
  } else if (key == "num_levels") {
    options.num_levels = static_cast<int>(value);
  } else if (key == "level0_slowdown_writes_trigger") {
    options.level0_slowdown_writes_trigger = static_cast<int>(value);
  } else if (key == "level0_stop_writes_trigger") {
    options.level0_stop_writes_trigger = static_cast<int>(value);
  } else if (key == "target_file_size_base") {
    options.target_file_size_base = (uint64_t)value;
  } else if (key == "target_file_size_multiplier") {
    options.target_file_size_multiplier = static_cast<int>(value);
  } else if (key == "level_compaction_dynamic_level_bytes") {
    options.level_compaction_dynamic_level_bytes = static_cast<bool>(value);
  } else if (key == "max_compaction_bytes") {
    options.max_compaction_bytes = (uint64_t)value;
  } else if (key == "soft_pending_compaction_bytes_limit") {
    options.soft_pending_compaction_bytes_limit = (uint64_t)value;
  } else if (key == "hard_pending_compaction_bytes_limit") {
    options.hard_pending_compaction_bytes_limit = (uint64_t)value;
  } else if (key == "max_sequential_skip_in_iterations") {
    options.max_sequential_skip_in_iterations = (uint64_t)value;
  } else if (key == "max_successive_merges") {
    options.max_successive_merges = static_cast<size_t>(value);
  } else if (key == "optimize_filters_for_hits") {
    options.optimize_filters_for_hits = static_cast<bool>(value);
  } else if (key == "paranoid_file_checks") {
    options.paranoid_file_checks = static_cast<bool>(value);
  } else if (key == "force_consistency_checks") {
    options.force_consistency_checks = static_cast<bool>(value);
  } else if (key == "report_bg_io_stats") {
    options.report_bg_io_stats = static_cast<bool>(value);
#if ROCKSDB_MAJOR > 5 || (ROCKSDB_MAJOR == 5 && ROCKSDB_MINOR > 15)
  } else if (key == "ttl") {
    options.ttl = (uint64_t)value;
#endif
  } else if (key == "write_buffer_size") {
    options.write_buffer_size = static_cast<size_t>(value);
  } else if (key == "level0_file_num_compaction_trigger") {
    options.level0_file_num_compaction_trigger = static_cast<int>(value);
  } else if (key == "max_bytes_for_level_base") {
    options.max_bytes_for_level_base = (uint64_t)value;
  } else if (key == "disable_auto_compactions") {
    options.disable_auto_compactions = static_cast<bool>(value);
  } else if (key == "create_if_missing") {
    options.create_if_missing = static_cast<bool>(value);
  } else if (key == "create_missing_column_families") {
    options.create_missing_column_families = static_cast<bool>(value);
  } else if (key == "error_if_exists") {
    options.error_if_exists = static_cast<bool>(value);
  } else if (key == "paranoid_checks") {
    options.paranoid_checks = static_cast<bool>(value);
  } else if (key == "max_open_files") {
    options.max_open_files = static_cast<int>(value);
  } else if (key == "max_file_opening_threads") {
    options.max_file_opening_threads = static_cast<int>(value);
  } else if (key == "max_total_wal_size") {
    options.max_total_wal_size = (uint64_t)value;
  } else if (key == "use_fsync") {
    options.use_fsync = static_cast<bool>(value);
  } else if (key == "delete_obsolete_files_period_micros") {
    options.delete_obsolete_files_period_micros = (uint64_t)value;
  } else if (key == "max_background_jobs") {
    options.max_background_jobs = static_cast<int>(value);
  } else if (key == "max_subcompactions") {
    options.max_subcompactions = static_cast<uint32_t>(value);
  } else if (key == "max_log_file_size") {
    options.max_log_file_size = static_cast<size_t>(value);
  } else if (key == "log_file_time_to_roll") {
    options.log_file_time_to_roll = static_cast<size_t>(value);
  } else if (key == "keep_log_file_num") {
    options.keep_log_file_num = static_cast<size_t>(value);
  } else if (key == "recycle_log_file_num") {
    options.recycle_log_file_num = static_cast<size_t>(value);
  } else if (key == "max_manifest_file_size") {
    options.max_manifest_file_size = (uint64_t)value;
  } else if (key == "table_cache_numshardbits") {
    options.table_cache_numshardbits = static_cast<int>(value);
  } else if (key == "wal_ttl_seconds") {
    options.WAL_ttl_seconds = (uint64_t)value;
  } else if (key == "wal_size_limit_mb") {
    options.WAL_size_limit_MB = (uint64_t)value;
  } else if (key == "manifest_preallocation_size") {
    options.manifest_preallocation_size = static_cast<size_t>(value);
  } else if (key == "allow_mmap_reads") {
    options.allow_mmap_reads = static_cast<bool>(value);
  } else if (key == "allow_mmap_writes") {
    options.allow_mmap_writes = static_cast<bool>(value);
  } else if (key == "use_direct_reads") {
    options.use_direct_reads = static_cast<bool>(value);
  } else if (key == "use_direct_io_for_flush_and_compaction") {
    options.use_direct_io_for_flush_and_compaction = static_cast<bool>(value);
  } else if (key == "allow_fallocate") {
    options.allow_fallocate = static_cast<bool>(value);
  } else if (key == "is_fd_close_on_exec") {
    options.is_fd_close_on_exec = static_cast<bool>(value);
  } else if (key == "stats_dump_period_sec") {
    options.stats_dump_period_sec = static_cast<int>(value);
#if ROCKSDB_MAJOR > 5 || (ROCKSDB_MAJOR == 5 && ROCKSDB_MINOR > 15)
  } else if (key == "stats_persist_period_sec") {
    options.stats_persist_period_sec = static_cast<int>(value);
  } else if (key == "stats_history_buffer_size") {
    options.stats_history_buffer_size = static_cast<size_t>(value);
#endif
  } else if (key == "advise_random_on_open") {
    options.advise_random_on_open = static_cast<bool>(value);
  } else if (key == "db_write_buffer_size") {
    options.db_write_buffer_size = static_cast<size_t>(value);
  } else if (key == "compaction_readahead_size") {
    options.compaction_readahead_size = static_cast<size_t>(value);
  } else if (key == "random_access_max_buffer_size") {
    options.random_access_max_buffer_size = static_cast<size_t>(value);
  } else if (key == "writable_file_max_buffer_size") {
    options.writable_file_max_buffer_size = static_cast<size_t>(value);
  } else if (key == "use_adaptive_mutex") {
    options.use_adaptive_mutex = static_cast<bool>(value);
  } else if (key == "bytes_per_sync") {
    options.bytes_per_sync = (uint64_t)value;
  } else if (key == "wal_bytes_per_sync") {
    options.wal_bytes_per_sync = (uint64_t)value;
  } else if (key == "enable_thread_tracking") {
    options.enable_thread_tracking = static_cast<bool>(value);
  } else if (key == "delayed_write_rate") {
    options.delayed_write_rate = (uint64_t)value;
  } else if (key == "enable_pipelined_write") {
    options.enable_pipelined_write = static_cast<bool>(value);
  } else if (key == "allow_concurrent_memtable_write") {
    options.allow_concurrent_memtable_write = static_cast<bool>(value);
  } else if (key == "enable_write_thread_adaptive_yield") {
    options.enable_write_thread_adaptive_yield = static_cast<bool>(value);
  } else if (key == "write_thread_max_yield_usec") {
    options.write_thread_max_yield_usec = (uint64_t)value;
  } else if (key == "write_thread_slow_yield_usec") {
    options.write_thread_slow_yield_usec = (uint64_t)value;
  } else if (key == "skip_stats_update_on_db_open") {
    options.skip_stats_update_on_db_open = static_cast<bool>(value);
  } else if (key == "allow_2pc") {
    options.allow_2pc = static_cast<bool>(value);
  } else if (key == "fail_if_options_file_error") {
    options.fail_if_options_file_error = static_cast<bool>(value);
  } else if (key == "dump_malloc_stats") {
    options.dump_malloc_stats = static_cast<bool>(value);
  } else if (key == "avoid_flush_during_recovery") {
    options.avoid_flush_during_recovery = static_cast<bool>(value);
  } else if (key == "avoid_flush_during_shutdown") {
    options.avoid_flush_during_shutdown = static_cast<bool>(value);
  } else if (key == "allow_ingest_behind") {
    options.allow_ingest_behind = static_cast<bool>(value);
  } else if (key == "two_write_queues") {
    options.two_write_queues = static_cast<bool>(value);
  } else if (key == "manual_wal_flush") {
    options.manual_wal_flush = static_cast<bool>(value);
#if ROCKSDB_MAJOR > 5 || (ROCKSDB_MAJOR == 5 && ROCKSDB_MINOR > 15)
  } else if (key == "atomic_flush") {
    options.atomic_flush = static_cast<bool>(value);
#endif
#if ROCKSDB_MAJOR > 6 || (ROCKSDB_MAJOR == 6 && ROCKSDB_MINOR > 18)
  } else if (key == "enable_blob_files") {
    options.enable_blob_files = static_cast<bool>(value);
  } else if (key == "min_blob_size") {
    options.min_blob_size = static_cast<uint32_t>(value);
  } else if (key == "blob_file_size") {
    options.blob_file_size = static_cast<uint64_t>(value);
  } else if (key == "blob_compression_type") {
    options.blob_compression_type = rocksGetCompressType(rawValue);
  } else if (key == "enable_blob_garbage_collection") {
    options.enable_blob_garbage_collection = static_cast<bool>(value);
  } else if (key == "blob_garbage_collection_age_cutoff") {
    double data = 0.0;
    auto ed = tendisplus::stod(rawValue);
    if (ed.ok()) {
      data = ed.value();
      options.blob_garbage_collection_age_cutoff = static_cast<double>(data);
    } else {
      LOG(ERROR) << "error param, not double, " << key << ":" << rawValue;
    }
#endif
  } else {
    return {ErrorCodes::ERR_PARSEOPT, "invalid rocksdb option :" + key};
  }

  return {ErrorCodes::ERR_OK, ""};
}

Status rocksdbTableOptionsSet(rocksdb::BlockBasedTableOptions& options,
                              const std::string& key,
                              const std::string& rawValue) {
  // TODO(takenliu): not int params need change two place, resolve it
  static std::set<std::string> notIntParams = {};

  int64_t value = 0;
  if (notIntParams.find(key) == notIntParams.end()) {
    auto ed = tendisplus::stoll(rawValue);
    if (ed.ok()) {
      value = ed.value();
    } else {
      LOG(ERROR) << "param need to be int64_t, " << key << ":" << rawValue;
      return {ErrorCodes::ERR_PARSEOPT, "invalid rocksdb option :" + key};
    }
  }
  if (key == "cache_index_and_filter_blocks") {
    options.cache_index_and_filter_blocks = static_cast<bool>(value);
  } else if (key == "cache_index_and_filter_blocks_with_high_priority") {
    options.cache_index_and_filter_blocks_with_high_priority =
      static_cast<bool>(value);
  } else if (key == "pin_l0_filter_and_index_blocks_in_cache") {
    options.pin_l0_filter_and_index_blocks_in_cache = static_cast<bool>(value);
#if ROCKSDB_MAJOR > 5 || (ROCKSDB_MAJOR == 5 && ROCKSDB_MINOR > 15)
  } else if (key == "pin_top_level_index_and_filter") {
    options.pin_top_level_index_and_filter = static_cast<bool>(value);
#endif
  } else if (key == "no_block_cache") {
    options.no_block_cache = static_cast<bool>(value);
  } else if (key == "block_size") {
    options.block_size = static_cast<size_t>(value);
  } else if (key == "block_size_deviation") {
    options.block_size_deviation = static_cast<int>(value);
  } else if (key == "block_restart_interval") {
    options.block_restart_interval = static_cast<int>(value);
  } else if (key == "index_block_restart_interval") {
    options.index_block_restart_interval = static_cast<int>(value);
  } else if (key == "metadata_block_size") {
    options.metadata_block_size = (uint64_t)value;
  } else if (key == "partition_filters") {
    options.partition_filters = static_cast<bool>(value);
  } else if (key == "use_delta_encoding") {
    options.use_delta_encoding = static_cast<bool>(value);
  } else if (key == "whole_key_filtering") {
    options.whole_key_filtering = static_cast<bool>(value);
  } else if (key == "verify_compression") {
    options.verify_compression = static_cast<bool>(value);
  } else if (key == "read_amp_bytes_per_bit") {
    options.read_amp_bytes_per_bit = static_cast<uint32_t>(value);
  } else if (key == "format_version") {
    options.format_version = static_cast<uint32_t>(value);
  } else if (key == "enable_index_compression") {
    options.enable_index_compression = static_cast<bool>(value);
#if ROCKSDB_MAJOR > 5 || (ROCKSDB_MAJOR == 5 && ROCKSDB_MINOR > 15)
  } else if (key == "block_align") {
    options.block_align = static_cast<bool>(value);
#endif
  } else {
    return {ErrorCodes::ERR_PARSEOPT, "invalid rocksdb  option :" + key};
  }

  return {ErrorCodes::ERR_OK, ""};
}

RocksKVStore::RocksKVStore(
  const std::string& id,
  const std::shared_ptr<ServerParams>& cfg,
  std::shared_ptr<rocksdb::Cache> blockCache,
  std::shared_ptr<rocksdb::Cache> rowCache,
  std::shared_ptr<rocksdb::RateLimiter> rateLimiter,
  std::shared_ptr<rocksdb::SstFileManager> sstFileManager,
  bool enableRepllog,
  KVStore::StoreMode mode,
  TxnMode txnMode,
  uint32_t flag)
  : KVStore(id, cfg->dbPath),
    _cfg(cfg),
    _isRunning(false),
    _isPaused(false),
    _hasBackup(false),
    _enableRepllog(enableRepllog),
    _mode(mode),
    _txnMode(txnMode),
    _optdb(nullptr),
    _pesdb(nullptr),
    _stats(rocksdb::CreateDBStatistics()),
    _blockCache(blockCache),
    _rowCache(rowCache),
    _rateLimiter(rateLimiter),
    _sstFileManager(sstFileManager),
    _nextTxnSeq(0),
    _highestVisible(Transaction::TXNID_UNINITED),
    _logOb(nullptr),
    _env(std::make_shared<RocksdbEnv>()) {
  Expected<uint64_t> s =
    restart(false, Transaction::MIN_VALID_TXNID, UINT64_MAX, flag);
  if (!s.ok()) {
    LOG(FATAL) << "opendb:" << _cfg->dbPath << "/" << id
               << ", failed info:" << s.status().toString();
  }

  initRocksProperties();
}

rocksdb::Options RocksKVStore::options(const string cf) {
  rocksdb::Options options;
  rocksdb::BlockBasedTableOptions table_options;
  table_options.block_cache = _blockCache;
  table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
  table_options.block_size = 16 * 1024;  // 16KB
  table_options.format_version = 2;
  // let index and filters pining in mem forever
  table_options.cache_index_and_filter_blocks = false;

  // max LOG size: 128(MB) * 10(files) * 10(kvstore) = 12.8GB
  options.max_log_file_size = 128 * 1024 * 1024;
  options.keep_log_file_num = 10;
  options.row_cache = _rowCache;
  options.write_buffer_size = 64 * 1024 * 1024;  // 64MB
  // level_0 files don't have a fixed file size.
  options.level0_slowdown_writes_trigger = 20;
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 2;
  options.max_background_jobs = 8;
  options.target_file_size_base = 64 * 1024 * 1024;  // 64MB
  options.level_compaction_dynamic_level_bytes = true;
  // level_1 max size: 512MB, in fact, things are more complex
  // since we set level_compaction_dynamic_level_bytes = true
  options.max_bytes_for_level_base = 512 * 1024 * 1024;  // 512MB
  options.max_open_files = -1;
  // if we have no 'empty reads', we can disable bottom
  // level's bloomfilters
  options.optimize_filters_for_hits = true;
  options.enable_thread_tracking = true;
  options.compression_per_level.resize(ROCKSDB_NUM_LEVELS);
  for (int i = 0; i < ROCKSDB_NUM_LEVELS; ++i) {
    options.compression_per_level[i] =
      rocksGetCompressType(_cfg->rocksCompressType);
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

  if (_rateLimiter != nullptr) {
    options.rate_limiter = _rateLimiter;
  }

  if (_sstFileManager != nullptr) {
    options.sst_file_manager = _sstFileManager;
  }

  // TODO(takenliu): if rocksdbOptions config error,
  //    erase it from ServerParams::_rocksdbOptions,
  //    otherwise "config get" will return the error configure.
  //    RocksdbCFOptions is the same.
  for (const auto& iter : _cfg->getRocksdbOptions()) {
    auto status = rocksdbOptionsSet(options, iter.first, iter.second);
    if (!status.ok()) {
      status = rocksdbTableOptionsSet(table_options, iter.first, iter.second);
      if (!status.ok()) {
        LOG(ERROR) << status.toString();
      }
    }
  }

  // example: binlogcf
  if (cf != "") {
    auto cfOptions = _cfg->getRocksdbCFOptions(cf);
    if (cfOptions != nullptr) {
      for (const auto& iter : *cfOptions) {
        auto status = rocksdbOptionsSet(options, iter.first, iter.second);
        if (!status.ok()) {
          status =
            rocksdbTableOptionsSet(table_options, iter.first, iter.second);
          if (!status.ok()) {
            LOG(ERROR) << status.toString();
          }
        }
      }
    }
  }
  if (!_cfg->binlogUsingDefaultCF) {
    // TODO(takenliu): make memory usage of binlog cf correct
    options.write_buffer_size /= 2;
  }

  options.table_factory.reset(
    rocksdb::NewBlockBasedTableFactory(table_options));

  if (dbId() != CATALOG_NAME) {
    // setup the ttlcompactionfilter expect "catalog" db
    options.compaction_filter_factory.reset(
      new KVTtlCompactionFilterFactory(this, _cfg));
  }

  _env->clear();
  // background listener
  auto listener = std::make_shared<BackgroundErrorListener>(_env);
  options.listeners.push_back(listener);

  return options;
}

rocksdb::Options RocksKVStore::defaultColumnOptions() {
  return options();
}

// Binlog Column different from default
rocksdb::Options RocksKVStore::binlogColumnOptions() {
  auto columOpts = options("binlogcf");
  for (int i = 0; i < ROCKSDB_NUM_LEVELS; ++i) {
    columOpts.compression_per_level[i] =
      rocksGetCompressType(_cfg->rocksCompressType);
  }
  return columOpts;
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
  auto ptxn = const_cast<RocksKVStore*>(this)->createTransaction(nullptr);
  if (!ptxn.ok()) {
    return false;
  }
  std::unique_ptr<Transaction> txn = std::move(ptxn.value());

  auto baseCursor = txn->createAllDataCursor();
  Expected<std::string> expKey = baseCursor->key();

  if (expKey.ok()) {
    return false;
  } else if (expKey.status().code() == ErrorCodes::ERR_EXHAUST) {
    if (!ignoreBinlog) {
      auto binlogCursor = txn->createBinlogCursor();
      Expected<std::string> expBinlogKey = binlogCursor->key();
      if (expBinlogKey.ok()) {
        return false;
      } else if (expBinlogKey.status().code() == ErrorCodes::ERR_EXHAUST) {
        return true;
      } else {
        LOG(ERROR) << "binlogCursor key failed:"
                   << expBinlogKey.status().toString();
        return false;
      }
    } else {
      return true;
    }
  } else {
    LOG(ERROR) << "baseCursor key failed:" << expKey.status().toString();
    return false;
  }
}

Status RocksKVStore::pause() {
  std::lock_guard<std::mutex> lk(_mutex);
  if (_aliveTxns.size() != 0) {
    return {ErrorCodes::ERR_INTERNAL,
            "it's upperlayer's duty to guarantee no pinning txns alive"};
  }

  _isPaused = true;
  return {ErrorCodes::ERR_OK, ""};
}

Status RocksKVStore::resume() {
  std::lock_guard<std::mutex> lk(_mutex);
  if (_aliveTxns.size() != 0) {
    return {ErrorCodes::ERR_INTERNAL,
            "it's upperlayer's duty to guarantee no pinning txns alive"};
  }

  _isPaused = false;
  return {ErrorCodes::ERR_OK, ""};
}

Status RocksKVStore::stop() {
  std::lock_guard<std::mutex> lk(_mutex);
  if (_aliveTxns.size() != 0) {
    return {ErrorCodes::ERR_INTERNAL,
            "it's upperlayer's duty to guarantee no pinning txns alive"};
  }
  _isRunning = false;

  for (auto* h : _cfHandles) {
    delete h;
  }
  _cfHandles.clear();
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
      INVARIANT_D(_mode == KVStore::StoreMode::REPLICATE_ONLY);
      // in READ_WRITE mode, the binlog's key is identified by _nextTxnSeq,
      // in REPLICATE_ONLY mode, the binlog is same as the sync-source's
      // when changing from REPLICATE_ONLY to READ_WRITE mode, we shrink
      // _nextTxnSeq so that binlog's wont' be duplicated.
      if (_nextTxnSeq <= _highestVisible) {
        _nextTxnSeq = _highestVisible + 1;
      }
      break;

    case KVStore::StoreMode::REPLICATE_ONLY:
    case KVStore::StoreMode::STORE_NONE:
      INVARIANT_D(_mode == KVStore::StoreMode::READ_WRITE);
      break;
    default:
      INVARIANT_D(0);
  }

  LOG(INFO) << "store:" << dbId()
            << ",mode:" << static_cast<uint32_t>(_mode.load())
            << ",changes to:" << static_cast<uint32_t>(mode)
            << ",_nextTxnSeq:" << oldSeq << ",changes to:" << _nextTxnSeq;
  _mode = mode;
  return {ErrorCodes::ERR_OK, ""};
}

TxnMode RocksKVStore::getTxnMode() const {
  return _txnMode;
}

// keylen(4) + key + vallen(4) + value
int64_t RocksKVStore::dumpBinlogV2(std::ofstream* fs, const ReplLogRawV2& log) {
  uint64_t written = 0;
  INVARIANT_D(fs != nullptr);

  uint32_t keyLen = log.getReplLogKey().size();
  uint32_t keyLenTrans = int32Encode(keyLen);
  fs->write(reinterpret_cast<char*>(&keyLenTrans), sizeof(keyLenTrans));
  if (!fs->good()) {
    LOG(INFO) << "fs->write() failed.";
    return -1;
  }
  fs->write(log.getReplLogKey().c_str(), keyLen);
  if (!fs->good()) {
    LOG(INFO) << "fs->write() failed.";
    return -1;
  }
  uint32_t valLen = log.getReplLogValue().size();
  uint32_t valLenTrans = int32Encode(valLen);
  fs->write(reinterpret_cast<char*>(&valLenTrans), sizeof(valLenTrans));
  if (!fs->good()) {
    LOG(INFO) << "fs->write() failed.";
    return -1;
  }
  fs->write(log.getReplLogValue().c_str(), valLen);
  if (!fs->good()) {
    LOG(INFO) << "fs->write() failed.";
    return -1;
  }
  written += keyLen + valLen + sizeof(keyLen) + sizeof(valLen);

  INVARIANT_D(fs->good());
  return written;
}

Expected<bool> RocksKVStore::deleteBinlog(uint64_t start) {
  auto ptxn = createTransaction(nullptr);
  if (!ptxn.ok()) {
    LOG(ERROR) << "deleteBinlog create txn failed:" << ptxn.status().toString();
    return false;
  }
  auto txn = std::move(ptxn.value());

  LOG(INFO) << "deleteBinlog begin, dbid:" << dbId() << " start:" << start;

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

    DLOG(INFO) << "deleteBinlog dbid:" << dbId()
               << " delete:" << explog.value().getBinlogId();
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
  LOG(INFO) << "deleteBinlog success, dbid:" << dbId() << " start:" << start
            << " end:" << end << " count:" << count;
  return true;
}

// [start, end]
Expected<TruncateBinlogResult> RocksKVStore::truncateBinlogV2(
  uint64_t start,
  uint64_t end,
  uint64_t dump,
  std::ofstream* fs,
  int64_t maxWritelen,
  bool tailSlave) {
  // DLOG(INFO) << "truncateBinlogV2 dbid:" << dbId()
  //    << " getHighestBinlogId:" << getHighestBinlogId()
  //    << " start:"<<start <<" end:"<< end;
  auto ptxn = createTransaction(nullptr);
  RET_IF_ERR_EXPECTED(ptxn);
  auto txn = ptxn.value().get();
#ifdef TENDIS_DEBUG
  Expected<uint64_t> minBinlogid = RepllogCursorV2::getMinBinlogId(txn);
  // When binlogSaveLogs is enabled
  if (minBinlogid.status().code() != ErrorCodes::ERR_EXHAUST &&
      (fs != nullptr)) {
    INVARIANT_COMPARE_D(minBinlogid.value(), >=, start);
  }
#endif
  INVARIANT_COMPARE_D(start, <=, dump);
  if (start > dump) {
    LOG(WARNING) << "truncateBinlogV2 start:" << start << " > dump:" << dump
                 << ", set dump=start";
    dump = start;
  }

  TruncateBinlogResult result;
  uint64_t cur_ts = msSinceEpoch();
  uint64_t minKeepLogMs = static_cast<uint64_t>(_cfg->minBinlogKeepSec) * 1000;
  bool shouldCheckBinlogTime = minKeepLogMs != 0;
  Status s{ErrorCodes::ERR_OK, ""};
  uint64_t newStart = start;
  uint64_t newEnd = end;
  uint64_t newDump = dump;
  uint64_t written = 0;
  uint64_t ts = 0;
  int err = 0;

  if (fs != nullptr) {
    auto cursor = txn->createRepllogCursorV2(dump);
    while (true) {
      auto explog = cursor->next();
      if (!explog.ok() || explog.value().getBinlogId() > end ||
          (!tailSlave && shouldCheckBinlogTime &&
           explog.value().getTimestamp() >= cur_ts - minKeepLogMs) ||
          int64_t(written) >= maxWritelen) {
        break;
      }
      // dump binlog
      int len = dumpBinlogV2(fs, explog.value());
      if (len < 0) {
        LOG(ERROR) << "dumpBinlogV2 failed, break.";
        // NOTE(takenliu): maybe write part of explog, so the binlog file's last
        // binlog will be error. then we change a new binlog file.
        err = -1;
        break;
      }
      written += len;
      newDump = explog.value().getBinlogId() + 1;
      ts = explog.value().getTimestamp();
    }

    result.err = err;
    result.written = written;
    result.newDump = newDump;
    newEnd = newDump - 1;
  }

  auto nextTry = start;
  while (true) {
    nextTry += _cfg->truncateBinlogNum;
    if (nextTry - 1 > newEnd) {
      break;
    }
    auto explog = txn->createRepllogCursorV2(nextTry - 1)->next();
    if (!explog.ok()) {
      break;
    }
    // NOTE(takenliu) binlogid maybe has lag, so we need check again.
    if (explog.value().getBinlogId() > newEnd ||
        (shouldCheckBinlogTime &&
         explog.value().getTimestamp() >= cur_ts - minKeepLogMs)) {
      break;
    }
    newStart = nextTry;
    ts = explog.value().getTimestamp();
  }
  result.newStart = newStart;
  result.timestamp = ts;
  if (fs == nullptr) {
    result.newDump = result.newStart;
  }

  if (start == newStart) {
    return result;
  }

  INVARIANT_D(ts != 0);
  s = saveMinBinlogId(newStart, ts);
  RET_IF_ERR(s);
  s = deleteRangeBinlog(0, newStart);
  if (!s.ok()) {
    LOG(ERROR) << "deleteRangeBinlog error:" << s.toString();
  }
  return result;
}

Expected<uint64_t> RocksKVStore::getBinlogCnt(Transaction* txn) const {
  auto bcursor = txn->createRepllogCursorV2(Transaction::MIN_VALID_TXNID, true);
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
  auto bcursor = txn->createRepllogCursorV2(Transaction::MIN_VALID_TXNID, true);
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

Status RocksKVStore::compactRange(ColumnFamilyNumber cf,
                                  const std::string* begin,
                                  const std::string* end) {
  auto compactionOptions = rocksdb::CompactRangeOptions();
  auto db = getBaseDB();
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
  rocksdb::Status status;
  if (cf == ColumnFamilyNumber::ColumnFamily_Default) {
    status = db->CompactRange(compactionOptions, sbegin, send);
  } else if (cf == ColumnFamilyNumber::ColumnFamily_Binlog) {
    status = db->CompactRange(
      compactionOptions, getBinlogColumnFamilyHandle(), sbegin, send);
  }
  if (!status.ok()) {
    LOG(ERROR) << "compactRange failed:" << status.ToString();
    return handleRocksdbError(status);
  }
  return {ErrorCodes::ERR_OK, ""};
}

// [begin, end)
Expected<uint64_t> RocksKVStore::GetApproximateSizes(ColumnFamilyNumber cf,
                                                     const std::string* begin,
                                                     const std::string* end,
                                                     bool incl_mem) {
  auto db = getBaseDB();
  std::array<rocksdb::Range, 1> ranges;
  std::array<uint64_t, 1> sizes;
  rocksdb::SizeApproximationOptions options;

  options.include_memtables = incl_mem;
  options.files_size_error_margin = 0.1;

  ranges[0].start = rocksdb::Slice(*begin);
  ranges[0].limit = rocksdb::Slice(*end);

  rocksdb::Status s = db->GetApproximateSizes(
    options, getColumnFamilyHandle(cf), ranges.data(), 1, sizes.data());
  if (!s.ok()) {
    return {ErrorCodes::ERR_INTERNAL, s.ToString()};
  }
  return sizes[0];
}

Status RocksKVStore::fullCompact() {
  Status s;
  // compact data of default column family
  s = compactRange(ColumnFamilyNumber::ColumnFamily_Default, nullptr, nullptr);
  if (!s.ok())
    return s;
  // compact data of binlog column family
  s = compactRange(ColumnFamilyNumber::ColumnFamily_Binlog, nullptr, nullptr);
  if (!s.ok())
    return s;
  return s;
}

Status RocksKVStore::clear() {
  std::lock_guard<std::mutex> lk(_mutex);
  if (_isRunning) {
    return {ErrorCodes::ERR_INTERNAL, "should stop before clear"};
  }
  // We use rocksdb::DestroyDB to destroy the contents of the specified
  // database, because 'User' may config these path:DBOptions::db_paths,
  // DBOptions::db_log_dir, DBOptions::wal_dir, ColumnFamilyOptions::cf_paths.
  // Using rocksdb::DestroyDB is simple. But when 'data path' has other
  // files(not rocksdb generate), rocksdb::DestroyDB couldn't delete 'data path'
  // , so we remove 'data path' again
#if ROCKSDB_MAJOR > 5 || (ROCKSDB_MAJOR == 5 && ROCKSDB_MINOR > 15)
  auto s = rocksdb::DestroyDB(dbName(), options(), _cfDescs);
#else
  auto s = rocksdb::DestroyDB(dbName(), options());
#endif
  if (!s.ok()) {
    return handleRocksdbError(s);
  }

  try {
    const std::string path = dbPath() + "/" + dbId();
    if (!filesystem::exists(path)) {
      return {ErrorCodes::ERR_OK, ""};
    }
    auto n = filesystem::remove_all(dbPath() + "/" + dbId());
    LOG(INFO) << "dbId:" << dbId() << " cleared " << n << " files/dirs";
  } catch (const std::exception& ex) {
    LOG(WARNING) << "dbId:" << dbId() << " clear failed:" << ex.what();
    return {ErrorCodes::ERR_INTERNAL, ex.what()};
  }

  return {ErrorCodes::ERR_OK, ""};
}

Expected<uint64_t> RocksKVStore::flush(Session* sess, uint64_t nextBinlogid) {
  auto s = stop();
  RET_IF_ERR(s);

  s = clear();
  RET_IF_ERR(s);

  auto ret = restart(false, nextBinlogid);
  if (!ret.ok()) {
    return ret.status();
  }
  INVARIANT_D(ret.value() == nextBinlogid - 1);

  // When disabled binlog, do not write 'flush' binlog.
  if (!enableRepllog()) {
    return {ErrorCodes::ERR_OK, ""};
  }

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

Expected<uint64_t> RocksKVStore::restart(bool restore,
                                         uint64_t nextBinlogSeq,
                                         uint64_t highestVisible,
                                         uint32_t flags) {
  // when do backup will get _highestVisible first, and backup later.
  // so the _highestVisible maybe smaller than backup.
  // so slaveof need the slave delete the binlogs after _highestVisible for
  // safe, and restorebackup need delete the binlogs after _highestVisible for
  // safe too.
  bool needDeleteBinlog = false;

  uint64_t maxCommitId = 0;
  {
    std::lock_guard<std::mutex> lk(_mutex);
    if (_isRunning) {
      return {ErrorCodes::ERR_INTERNAL, "already running"};
    }
    LOG(INFO) << "RocksKVStore::restart id:" << dbId() << " restore:" << restore
              << " nextBinlogSeq:" << nextBinlogSeq
              << " highestVisible:" << highestVisible
              << " binlogEnabled:" << _enableRepllog;
    INVARIANT_D(nextBinlogSeq != Transaction::TXNID_UNINITED);

    // NOTE(vinchen): if stateMode is STORE_NONE, the store no need
    // to open in rocksdb layer.
    if (getMode() == KVStore::StoreMode::STORE_NONE) {
      return {ErrorCodes::ERR_OK, ""};
    }

    std::string dbname = dbPath() + "/" + dbId();
    if (restore) {
      try {
        const std::string path = dbPath() + "/" + dbId();
        if (filesystem::exists(path)) {
          std::stringstream ss;
          ss << "path:" << path << " should not exist when restore";
          return {ErrorCodes::ERR_INTERNAL, ss.str()};
        }
        if (!filesystem::exists(dftBackupDir())) {
          std::stringstream ss;
          ss << "recover path:" << dftBackupDir() << " not exist when restore";
          return {ErrorCodes::ERR_INTERNAL, ss.str()};
        }
        filesystem::rename(dftBackupDir(), path);
      } catch (const std::exception& ex) {
        LOG(WARNING) << "dbId:" << dbId() << "restore exception" << ex.what();
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

    rocksdb::Options defaultColumnFamilyOpts = defaultColumnOptions();
    // enable CompactOnDeletionCollectorFactory:
#if ROCKSDB_MAJOR > 6 || (ROCKSDB_MAJOR == 6 && ROCKSDB_MINOR > 11)
    defaultColumnFamilyOpts.table_properties_collector_factories.emplace_back(
      rocksdb::NewCompactOnDeletionCollectorFactory(
        _cfg->rocksCompactOnDeletionWindow,
        _cfg->rocksCompactOnDeletionTrigger,
        _cfg->rocksCompactOnDeletionRatio));
#else
    if (_cfg->rocksCompactOnDeletionWindow == 0) {
      // disable CompactOnDeletionCollectorFactory
    } else {
      defaultColumnFamilyOpts.table_properties_collector_factories.emplace_back(
        rocksdb::NewCompactOnDeletionCollectorFactory(
          _cfg->rocksCompactOnDeletionWindow,
          _cfg->rocksCompactOnDeletionTrigger));
    }
#endif
    std::unique_ptr<rocksdb::Iterator> iter = nullptr;
    std::unique_ptr<rocksdb::Iterator> binlog_iter = nullptr;
    // In clear() we use _cfDescs, so we don't put _cfDescs.clear() in
    // RocksKVStore::stop(). stop() is used in these conditions:
    // 1) flush/destroy: stop -> clear -> restart
    // 2) recoveryFromBgError: stop -> restart
    _cfDescs.clear();
    _cfDescs.push_back(rocksdb::ColumnFamilyDescriptor(
      rocksdb::kDefaultColumnFamilyName, defaultColumnFamilyOpts));
    if (!_cfg->binlogUsingDefaultCF) {
      rocksdb::Options binlogColumnFamilyOpts = binlogColumnOptions();
      _cfDescs.push_back(
        rocksdb::ColumnFamilyDescriptor("binlog_cf", binlogColumnFamilyOpts));
    }
    if (_txnMode == TxnMode::TXN_OPT) {
      rocksdb::OptimisticTransactionDB* tmpDb = nullptr;
      rocksdb::Options dbOpts = options();
      dbOpts.create_missing_column_families = true;
      auto status = rocksdb::OptimisticTransactionDB::Open(
        dbOpts,
        dbname,
        _cfDescs,
        &_cfHandles,
        &tmpDb);  // open two column_family in OptimisticTranDB
      if (!status.ok()) {
        if (tmpDb) {
          delete tmpDb;
        }
        return {ErrorCodes::ERR_INTERNAL, status.ToString()};
      }
      rocksdb::ReadOptions readOpts;
      if (_cfg->forceRecovery) {
        readOpts.verify_checksums = false;
      }
      iter.reset(
        tmpDb->GetBaseDB()->NewIterator(readOpts, getDataColumnFamilyHandle()));
      binlog_iter.reset(tmpDb->GetBaseDB()->NewIterator(
        readOpts, getBinlogColumnFamilyHandle()));
      _optdb.reset(tmpDb);
    } else {
      rocksdb::TransactionDB* tmpDb = nullptr;
      rocksdb::TransactionDBOptions txnDbOptions;
      // txnDbOptions.max_num_locks unlimit
      // txnDbOptions.transaction_lock_timeout 1sec
      // txnDbOptions.default_lock_timeout 1sec
      // txnDbOptions.write_policy WRITE_COMMITTED
      txnDbOptions.num_stripes = 40;
      rocksdb::Options dbOpts = options();
      dbOpts.create_missing_column_families = true;
      LOG(INFO) << "rocksdb Open,id:" << dbId() << " dbname:" << dbname;
      // open two colum_family in pessimisticTranDB
      auto status = rocksdb::TransactionDB::Open(
        dbOpts, txnDbOptions, dbname, _cfDescs, &_cfHandles, &tmpDb);
      if (!status.ok()) {
        LOG(INFO) << "rocksdb Open error,id:" << dbId() << " dbname:" << dbname;
        if (tmpDb) {
          delete tmpDb;
        }
        return {ErrorCodes::ERR_INTERNAL, status.ToString()};
      }
      LOG(INFO) << "rocksdb Open sucess,id:" << dbId() << " dbname:" << dbname;
      rocksdb::ReadOptions readOpts;
      if (_cfg->forceRecovery) {
        readOpts.verify_checksums = false;
      }
      iter.reset(
        tmpDb->GetBaseDB()->NewIterator(readOpts, getDataColumnFamilyHandle()));
      binlog_iter.reset(tmpDb->GetBaseDB()->NewIterator(
        readOpts, getBinlogColumnFamilyHandle()));
      _pesdb.reset(tmpDb);
    }
    // NOTE(deyukong): during starttime, mutex is held and
    // no need to consider visibility

    maxCommitId = nextBinlogSeq - 1;
    INVARIANT_D(nextBinlogSeq > maxCommitId);
    LOG(INFO) << "RocksKVStore::restart flags: " << flags;
    if (!(flags & ROCKS_FLAGS_BINLOGVERSION_CHANGED)) {
      // if we have binlog, we will inherit latest binlogId
      // if we have no binlog, we will reset binlogId
      RocksKVCursor binlog_cursor(std::move(binlog_iter));
      binlog_cursor.seekToLast();
      Expected<Record> binlog_expRcd = binlog_cursor.next();
      if (binlog_expRcd.ok()) {
        const RecordKey& rk = binlog_expRcd.value().getRecordKey();
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
        }
      } else if (binlog_expRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
        _nextTxnSeq = nextBinlogSeq;
        _nextBinlogSeq = _nextTxnSeq;
        LOG(INFO) << "store:" << dbId() << " have no binlog, set nextSeq to "
                  << _nextTxnSeq;
        _highestVisible = _nextBinlogSeq - 1;
        INVARIANT_D(_highestVisible < _nextBinlogSeq);
      } else {
        return binlog_expRcd.status();
      }
    } else {
      // we need to check binlogVersion, in case reforming db into two
      // column families
      // TODO(vinchen): we need to delete remaining binlog in
      // defaultCF, and pay attention to the number sequence of binlog
      // file flushed to disk.
      RocksKVCursor cursor(std::move(iter));
      cursor.seekToLast();
      Expected<Record> expRcd = cursor.next();
      if (expRcd.ok()) {
        const RecordKey& rk = expRcd.value().getRecordKey();
        if (rk.getRecordType() == RecordType::RT_BINLOG) {
          auto explk = ReplLogKeyV2::decode(rk);
          if (!explk.ok()) {
            return explk.status();
          } else {
            auto binlogId = explk.value().getBinlogId();
            LOG(INFO) << "store(upgrade binlogVersion from 1 to 2):" << dbId()
                      << " nextSeq change from:" << _nextTxnSeq
                      << " to:" << binlogId + 1;
            maxCommitId = binlogId;
            _nextTxnSeq = maxCommitId + 1;
            _nextBinlogSeq = _nextTxnSeq;
            _highestVisible = maxCommitId;
            needDeleteBinlog = true;
          }
        } else {
          LOG(INFO) << "store(upgrade binlogVersion from 1 to 2): no "
                       "binlog "
                       "in default CF";
        }
      } else if (expRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
        LOG(INFO) << "store(upgrade binlogVersion from 1 to 2): no data "
                     "in default CF";
      } else {
        return expRcd.status();
      }
    }

    _isRunning = true;
  }
  {
    if (highestVisible != UINT64_MAX) {
      if (needDeleteBinlog) {
        Expected<bool> ret = deleteBinlog(highestVisible + 1);
        if (!ret.ok()) {
          return ret.status();
        }
      }
      LOG(INFO) << "store:" << dbId() << " nextSeq change from:" << _nextTxnSeq
                << " to:" << highestVisible + 1
                << " needDeleteBinlog:" << needDeleteBinlog;
      maxCommitId = highestVisible;

      std::lock_guard<std::mutex> lk(_mutex);
      _nextTxnSeq = maxCommitId + 1;
      _nextBinlogSeq = _nextTxnSeq;
      _highestVisible = maxCommitId;
    }
  }
  return maxCommitId;
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
                                          KVStore::BackupMode mode,
                                          BinlogVersion binlogVersion) {
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
      return {ErrorCodes::ERR_INTERNAL,
              "BACKUP_CKPT|BACKUP_COPY cant equal dftBackupDir:" + dir};
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
    rocksdb::Checkpoint* checkpoint = nullptr;
    auto guard = MakeGuard([this, &checkpoint]() {
      if (checkpoint) {
        delete checkpoint;
      }
    });
    auto s = rocksdb::Checkpoint::Create(getBaseDB(), &checkpoint);
    if (!s.ok()) {
      return {ErrorCodes::ERR_INTERNAL, s.ToString()};
    }
    s = checkpoint->CreateCheckpoint(dir);
    if (!s.ok()) {
      return {ErrorCodes::ERR_INTERNAL, s.ToString()};
    }
  } else {
    rocksdb::BackupEngine* bkEngine = nullptr;
    auto s = rocksdb::BackupEngine::Open(
      rocksdb::Env::Default(), rocksdb::BackupEngineOptions(dir), &bkEngine);
    if (!s.ok()) {
      if (bkEngine)
        delete bkEngine;
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
  result.setBinlogVersion(binlogVersion);
  auto saveret = saveBackupMeta(dir, &result);
  if (!saveret.ok()) {
    return saveret.status();
  }
  succ = true;
  return result;
}

Expected<std::string> RocksKVStore::saveBackupMeta(const std::string& dir,
                                                   BackupInfo* backup) {
  rapidjson::StringBuffer sb;
  rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(sb);
  writer.StartObject();
  writer.Key("backupType");
  writer.Uint(backup->getBackupMode());
  writer.Key("binlogpos");
  writer.Uint64(backup->getBinlogPos());
  writer.Key("startTimeSec");
  writer.Uint64(backup->getStartTimeSec());
  writer.Key("endTimeSec");
  writer.Uint64(backup->getEndTimeSec());
  writer.Key("useTimeSec");
  writer.Uint64(backup->getEndTimeSec() - backup->getStartTimeSec());
  writer.Key("binlogVersion");
  writer.Uint64((uint64_t)backup->getBinlogVersion());
  writer.EndObject();
  string data = sb.GetString();

  string filename = dir + "/backup_meta";
  std::ofstream metafile(filename);
  if (!metafile.is_open()) {
    return {ErrorCodes::ERR_INTERNAL, "open file failed:" + filename};
  }
  metafile << data;
  metafile.close();

  // add metafile to filelist
  auto size = filesystem::file_size(filename);
  backup->addFile("backup_meta", size);

  return std::string("ok");
}

Expected<BackupInfo> RocksKVStore::getBackupMeta(const std::string& dir) {
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

  BackupInfo bkInfo;
#ifdef _WIN32
#undef GetObject
#endif
  for (auto& o : doc.GetObject()) {
    if (o.name == "backupType") {
      if (o.value.IsUint()) {
        bkInfo.setBackupMode(o.value.GetInt());
      } else {
        return {ErrorCodes::ERR_PARSEOPT, "Invalid backup meta"};
      }
    } else if (o.name == "binlogpos") {
      if (o.value.IsUint64()) {
        bkInfo.setBinlogPos(o.value.GetInt64());
      } else {
        return {ErrorCodes::ERR_PARSEOPT, "Invalid backup meta"};
      }
    } else if (o.name == "startTimeSec") {
      if (o.value.IsUint64()) {
        bkInfo.setStartTimeSec(o.value.GetInt64());
      } else {
        return {ErrorCodes::ERR_PARSEOPT, "Invalid backup meta"};
      }
    } else if (o.name == "endTimeSec") {
      if (o.value.IsUint64()) {
        bkInfo.setEndTimeSec(o.value.GetUint64());
      } else {
        return {ErrorCodes::ERR_PARSEOPT, "Invalid backup meta"};
      }
    } else if (o.name == "binlogVersion") {
      if (o.value.IsUint64()) {
        bkInfo.setBinlogVersion((BinlogVersion)o.value.GetUint64());
      } else {
        return {ErrorCodes::ERR_PARSEOPT, "Invalid backup meta"};
      }
    }
  }
  return bkInfo;
}

Expected<std::string> RocksKVStore::restoreBackup(const std::string& dir) {
  auto backup_meta = getBackupMeta(dir);
  if (!backup_meta.ok()) {
    return backup_meta.status();
  }

  uint32_t mode = backup_meta.value().getBackupMode();
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
    rocksdb::Env::Default(), rocksdb::BackupEngineOptions(dir), &backup_engine);
  if (!s.ok()) {
    LOG(ERROR) << "BackupEngineReadOnly::Open failed." << s.ToString()
               << " dir:" << dir;
    return {ErrorCodes::ERR_INTERNAL, s.ToString()};
  }
  std::unique_ptr<rocksdb::BackupEngineReadOnly> pBkEngine(backup_engine);
  const std::string path = dbPath() + "/" + dbId();
  // restore from backup_dir to _dbPath
  s = pBkEngine->RestoreDBFromLatestBackup(path, path);
  if (!s.ok()) {
    LOG(ERROR) << "RestoreDBFromLatestBackup failed." << s.ToString()
               << " dir:" << dir;
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
      ss << "path:" << path << " should not exist when restore";
      return {ErrorCodes::ERR_INTERNAL, ss.str()};
    }
    if (!filesystem::exists(dir)) {
      std::stringstream ss;
      ss << "recover path:" << dir << " not exist when restore";
      return {ErrorCodes::ERR_INTERNAL, ss.str()};
    }
    LOG(INFO) << (getCfg()->moveDirWhenRestoreCkpt ? "move" : "copy")
              << " ckpt, src:" << dir << ", dst:" << path;
    if (getCfg()->moveDirWhenRestoreCkpt) {
      filesystem::rename(dir, path);
    } else {
      filesystem::copy(dir, path);
    }
  } catch (const std::exception& ex) {
    LOG(WARNING) << "dbId:" << dbId() << "restore exception" << ex.what();
    return {ErrorCodes::ERR_INTERNAL, ex.what()};
  }
  return std::string("ok");
}

Expected<std::unique_ptr<Transaction>> RocksKVStore::createTransaction(
  Session* sess) {
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
  } else if (_txnMode == TxnMode::TXN_PES) {
    ret.reset(new RocksPesTxn(this, txnId, replOnly, _logOb, sess));
  } else if (_txnMode == TxnMode::TXN_WB) {
    ret.reset(new RocksWBTxn(this, txnId, replOnly, _logOb, sess));
  } else {
    INVARIANT_D(0);
  }
  addUnCommitedTxnInLock(txnId);
  return ret;
}

Status RocksKVStore::assignBinlogIdIfNeeded(Transaction* txn) {
  if (txn->getBinlogId() == Transaction::TXNID_UNINITED) {
    std::lock_guard<std::mutex> lk(_mutex);
    uint64_t binlogId = _nextBinlogSeq++;

    txn->setBinlogId(binlogId);
    INVARIANT_D(_aliveBinlogs.find(binlogId) == _aliveBinlogs.end());
    _aliveBinlogs.insert({binlogId, {false, txn->getTxnId()}});

    auto it = _aliveTxns.find(txn->getTxnId());
    INVARIANT_D(it != _aliveTxns.end() && !it->second.first);

    it->second.second = binlogId;
  }

  return {ErrorCodes::ERR_OK, ""};
}

void RocksKVStore::setNextBinlogSeq(uint64_t binlogId, Transaction* txn) {
  std::lock_guard<std::mutex> lk(_mutex);
  INVARIANT_D(txn->isReplOnly());

  _nextBinlogSeq = binlogId + 1;

  txn->setBinlogId(binlogId);
  INVARIANT_D(_aliveBinlogs.find(binlogId) == _aliveBinlogs.end());
  _aliveBinlogs.insert({binlogId, {false, txn->getTxnId()}});
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
          INVARIANT_D(_highestVisible <= _nextBinlogSeq);
        }
        i = _aliveBinlogs.erase(i);
      }
    }
  }
}

Expected<RecordValue> RocksKVStore::getKV(const RecordKey& key,
                                          Transaction* txn) {
  INVARIANT_D(txn->getKVStoreId() == dbId());
  Expected<std::string> s = txn->getKV(key.encode());
  if (!s.ok()) {
    return s.status();
  }
  return RecordValue::decode(s.value());
}

Status RocksKVStore::setKV(const RecordKey& key,
                           const RecordValue& value,
                           Transaction* txn) {
  INVARIANT_D(txn->getKVStoreId() == dbId());
  return txn->setKV(key.encode(), value.encode());
}

Status RocksKVStore::handleRocksdbError(rocksdb::Status s) const {
  if (_cfg->forceRecovery == 0) {
    if (s.IsCorruption()) {
      /* NOTE(vinchen): If got Corruption errors from rocksdb, instance should
       * coredump immediately, and trigger the failover of cluster */
      LOG(ERROR) << "Get corruption error from rocksdb:" << s.ToString();
      INVARIANT(0);
    }
  }

  LOG(ERROR) << "Get unexpected error from rocksdb:" << s.ToString();
  return {ErrorCodes::ERR_INTERNAL, s.ToString()};
}

Status RocksKVStore::delKV(const RecordKey& key, Transaction* txn) {
  // TODO(deyukong): statstics and inmemory-accumulative counter
  INVARIANT_D(txn->getKVStoreId() == dbId());
  return txn->delKV(key.encode());
}

Status RocksKVStore::deleteRange(const std::string& begin,
                                 const std::string& end,
                                 bool deleteFilesInRangeBeforeDeleteRange,
                                 bool compactRangeAfterDeleteRange) {
  // do deleteFilesInRange if required.
  if (deleteFilesInRangeBeforeDeleteRange) {
    // deleteFilesInRange and deleteRange are not atomic
    // since deleteRange affect [begin, end)
    // not set include_end for deleteFilesInRange
    auto s = deleteFilesInRange(begin, end, false);
    RET_IF_ERR(s);
  }

  // NOTE(takenliu) be care of db::DeleteRange and add binlog are not atomic
  auto s = deleteRangeWithoutBinlog(getDataColumnFamilyHandle(), begin, end);
  RET_IF_ERR(s);
  auto txn = createTransaction(nullptr);
  if (!txn.ok()) {
    LOG(ERROR) << "deleteRange not atomic,createTransaction failed!!!";
    return txn.status();
  }
  auto ret = txn.value()->addDeleteRangeBinlog(begin, end);
  if (!ret.ok()) {
    LOG(ERROR) << "deleteRange not atomic,add binlog failed!!!";
    return ret;
  }
  auto ret2 = txn.value()->commit();
  if (!ret2.ok()) {
    LOG(ERROR) << "deleteRange not atomic,add binlog commit failed!!!";
    return ret2.status();
  }

  // do compactRange if required.
  if (compactRangeAfterDeleteRange) {
    s = compactRange(ColumnFamilyNumber::ColumnFamily_Default, &begin, &end);
    RET_IF_ERR(s);
  }

  return {ErrorCodes::ERR_OK, ""};
}

// [begin, end)
Status RocksKVStore::deleteRangeWithoutBinlog(
  rocksdb::ColumnFamilyHandle* column_family,
  const std::string& begin,
  const std::string& end) {
  rocksdb::Slice sBegin(begin);
  rocksdb::Slice sEnd(end);
  rocksdb::DB* db = getBaseDB();
  auto status = db->DeleteRange(writeOptions(), column_family, sBegin, sEnd);
  if (!status.ok()) {
    LOG(ERROR) << "deleteRange failed:" << status.ToString();
    return handleRocksdbError(status);
  }
  return {ErrorCodes::ERR_OK, ""};
}

// [begin, end) if include_end = false
// [begin, end] if include_end = true
Status RocksKVStore::deleteFilesInRange(const std::string& begin,
                                        const std::string& end,
                                        bool include_end) {
  auto s = deleteFilesInRangeWithoutBinlog(
    getDataColumnFamilyHandle(), begin, end, include_end);
  RET_IF_ERR(s);
  auto ptxn = createTransaction(nullptr);
  if (!ptxn.ok()) {
    LOG(ERROR) << "deleteFilesInRange not atomic,createTransaction failed!!!";
    return ptxn.status();
  }
  auto txn = std::move(ptxn.value());
  s = txn->addDeleteFilesInRangeBinlog(begin, end, include_end);
  if (!s.ok()) {
    LOG(ERROR) << "deleteFilesInRange not atomic,add binlog failed!!!";
    return s;
  }
  auto ret = txn->commit();
  if (!ret.ok()) {
    LOG(ERROR) << "deleteFilesInRange not atomic, txn commit failed!!!";
    return ret.status();
  }
  return {ErrorCodes::ERR_OK, ""};
}

Status RocksKVStore::deleteFilesInRangeBinlog(uint64_t begin,
                                              uint64_t end,
                                              bool include_end) {
  auto beginKeyStr = ReplLogKeyV2(0).encode();  // begin always use 0
  auto endKeyStr = ReplLogKeyV2(end).encode();
  return deleteFilesInRangeWithoutBinlog(
    getBinlogColumnFamilyHandle(), beginKeyStr, endKeyStr, include_end);
}

Status RocksKVStore::deleteFilesInRangeWithoutBinlog(ColumnFamilyNumber cf,
                                                     const std::string& begin,
                                                     const std::string& end,
                                                     bool include_end) {
  if (cf == ColumnFamilyNumber::ColumnFamily_Default) {
    return deleteFilesInRangeWithoutBinlog(
      getDataColumnFamilyHandle(), begin, end, include_end);
  } else if (cf == ColumnFamilyNumber::ColumnFamily_Binlog) {
    return deleteFilesInRangeWithoutBinlog(
      getBinlogColumnFamilyHandle(), begin, end, include_end);
  }
  return {ErrorCodes::ERR_INTERNAL, "Unknown columnFamily"};
}

Status RocksKVStore::deleteFilesInRangeWithoutBinlog(
  rocksdb::ColumnFamilyHandle* column_family,
  const std::string& begin,
  const std::string& end,
  bool include_end) {
  rocksdb::Slice sBegin(begin);
  rocksdb::Slice sEnd(end);
  rocksdb::DB* db = getBaseDB();
  auto status =
    rocksdb::DeleteFilesInRange(db, column_family, &sBegin, &sEnd, include_end);
  if (!status.ok()) {
    LOG(ERROR) << "deleteFilesInRange failed:" << status.ToString();
    return handleRocksdbError(status);
  }
  return {ErrorCodes::ERR_OK, ""};
}

Status RocksKVStore::saveMinBinlogId(uint64_t id, uint64_t ts) {
  RecordKey key(REPLLOGKEYV2_META_CHUNKID,
                REPLLOGKEYV2_META_DBID,
                RecordType::RT_META,
                "",
                "");

  std::string val;
  val.resize(sizeof(uint64_t) + sizeof(uint64_t));
  // binlogId
  int64Encode(&val[0], id);
  // ts
  int64Encode(&val[0] + sizeof(uint64_t), ts);

  RecordValue value(std::move(val), RecordType::RT_META, -1);

  auto ptxn = createTransaction(nullptr);
  RET_IF_ERR_EXPECTED(ptxn);
  auto txn = std::move(ptxn.value());
  auto s = txn->setKVWithoutBinlog(key.encode(), value.encode());
  if (!s.ok()) {
    LOG(ERROR) << "setKV failed:" << s.toString();
    return s;
  }
  auto commitRes = txn->commit();
  RET_IF_ERR_EXPECTED(commitRes);
  return {ErrorCodes::ERR_OK, ""};
}

// [begin, end)
Status RocksKVStore::deleteRangeBinlog(uint64_t begin, uint64_t end) {
  auto s = deleteFilesInRangeBinlog(0, end, false);
  if (!s.ok()) {
    LOG(ERROR) << "call deleteFilesInRangeBinlog failed, end:" << end;
  }
  return s;
}

void RocksKVStore::initRocksProperties() {
  _rocksIntProperties = {
    {"rocksdb.num-immutable-mem-table", "num_immutable_mem_table"},
    {"rocksdb.mem-table-flush-pending", "mem_table_flush_pending"},
    {"rocksdb.compaction-pending", "compaction_pending"},
    {"rocksdb.background-errors", "background_errors"},
    {"rocksdb.cur-size-active-mem-table", "cur_size_active_mem_table"},
    {"rocksdb.cur-size-all-mem-tables", "cur_size_all_mem_tables"},
    {"rocksdb.size-all-mem-tables", "size_all_mem_tables"},
    {"rocksdb.num-entries-active-mem-table", "num_entries_active_mem_table"},
    {"rocksdb.num-entries-imm-mem-tables", "num_entries_imm_mem_tables"},
    {"rocksdb.num-deletes-active-mem-table", "num_deletes_active_mem_table"},
    {"rocksdb.num-deletes-imm-mem-tables", "num_deletes_imm_mem_tables"},
    {"rocksdb.estimate-num-keys", "estimate_num_keys"},
    {"rocksdb.estimate-table-readers-mem", "estimate_table_readers_mem"},
    {"rocksdb.is-file-deletions-enabled", "is_file_deletions_enabled"},
    {"rocksdb.num-snapshots", "num_snapshots"},
    {"rocksdb.oldest-snapshot-time", "oldest_snapshot_time"},
    {"rocksdb.num-live-versions", "num_live_versions"},
    {"rocksdb.current-super-version-number", "current_super_version_number"},
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
    {"rocksdb.num-immutable-mem-table-flushed",
     "num-immutable-mem-table-flushed"},
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
    // {"rocksdb.estimate-oldest-key-time", "estimate-oldest-key-time"},
  };
  for (int i = 0; i < ROCKSDB_NUM_LEVELS; ++i) {
    _rocksStringProperties["rocksdb.num-files-at-level" + std::to_string(i)] =
      "num_files_at_level" + std::to_string(i);
    _rocksStringProperties["rocksdb.compression-ratio-at-level" +
                           std::to_string(i)] =
      "compression-ratio-at-level" + std::to_string(i);
    _rocksStringProperties["rocksdb.aggregated-table-properties-at-level" +
                           std::to_string(i)] =
      "aggregated-table-properties-at-level" + std::to_string(i);
  }
}

bool RocksKVStore::getIntProperty(const std::string& property,
                                  uint64_t* value,
                                  ColumnFamilyNumber cf) const {
  bool ok = false;
  if (_isRunning) {
    if (cf == ColumnFamilyNumber::ColumnFamily_All &&
        _cfg->binlogUsingDefaultCF) {
      cf = ColumnFamilyNumber::ColumnFamily_Default;
    }
    if (cf == ColumnFamilyNumber::ColumnFamily_All) {
      *value = 0;
      uint64_t tmp = 0;
      ok = getBaseDB()->GetIntProperty(_cfHandles[0], property, &tmp);
      if (!ok) {
        LOG(WARNING) << "db:" << dbId() << " getProperty:" << property
                     << " failed";
      }
      *value += tmp;
      ok = getBaseDB()->GetIntProperty(_cfHandles[1], property, &tmp);
      if (!ok) {
        LOG(WARNING) << "db:" << dbId() << " getProperty:" << property
                     << " failed";
      }
      *value += tmp;
    } else {
      ok =
        getBaseDB()->GetIntProperty(getColumnFamilyHandle(cf), property, value);
      if (!ok) {
        LOG(WARNING) << "db:" << dbId() << " getProperty:" << property
                     << " failed";
      }
    }
  }
  return ok;
}

bool RocksKVStore::getProperty(const std::string& property,
                               std::string* value,
                               ColumnFamilyNumber cf) const {
  bool ok = false;
  if (_isRunning) {
    ok = getBaseDB()->GetProperty(getColumnFamilyHandle(cf), property, value);
    if (!ok) {
      LOG(WARNING) << "db:" << dbId() << " getProperty:" << property
                   << " failed";
    }
    replaceAll(*value, "\n", "\r\n");
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

uint64_t RocksKVStore::getStatCountById(uint32_t id) const {
  if (_isRunning) {
    return _stats->getTickerCount(id);
  }
  return 0;
}

uint64_t RocksKVStore::getStatCountByName(const std::string& name) const {
  static std::map<std::string, rocksdb::Tickers> tickersNameMap = {
    {"rocksdb.number.iter.skip", rocksdb::Tickers::NUMBER_ITER_SKIP},
  };
  if (tickersNameMap.find(name) != tickersNameMap.end()) {
    return getStatCountById(tickersNameMap[name]);
  }

  if (name == "rocksdb.compaction-filter-count") {
    return stat.compactFilterCount.load(memory_order_relaxed);
  } else if (name == "rocksdb.compaction-kv-expired-count") {
    return stat.compactKvExpiredCount.load(memory_order_relaxed);
  }

  INVARIANT_D(0);

  return 0;
}

std::string RocksKVStore::getBgError() const {
  return _env->getErrorString();
}

Status RocksKVStore::recoveryFromBgError() {
  if (getBgError() == "") {
    return {ErrorCodes::ERR_OK, ""};
  }
#if ROCKSDB_MAJOR > 5 || (ROCKSDB_MAJOR == 5 && ROCKSDB_MINOR > 15)
  {
    std::lock_guard<std::mutex> lk(_mutex);
    auto s = getBaseDB()->Resume();
    if (!s.ok()) {
      return handleRocksdbError(s);
    }
  }
  _env->resetError();
#else
  // NOTE(vinchen): in rocksdb-5.13.4 there is no DB::Resume().
  // We restart KVstore to recover rocksdb
  auto s = stop();
  if (!s.ok()) {
    return s;
  }

  auto nextBinlogid = getNextBinlogSeq();

  auto ret = restart(false, nextBinlogid, UINT64_MAX);
  if (!ret.ok()) {
    return ret.status();
  }

  INVARIANT_D(ret.value() == nextBinlogid - 1);
  _env->resetError();
#endif

  return {ErrorCodes::ERR_OK, ""};
}

Status RocksKVStore::setOptionDynamic(const std::string& option,
                                      const std::string& value) {
  std::unordered_map<std::string, std::string> map;
  if (option.substr(0, 6) != "rocks.") {
    return {ErrorCodes::ERR_INTERNAL, option + " is not rocksdb option"};
  }

  static std::set<std::string> rocksdb_dynamic_options = {
    "rocks.max_background_jobs",
    "rocks.max_open_files",
  };
  static std::set<std::string> rocksdb_cf_dynamic_options = {
    "rocks.enable_blob_files",
    "rocks.min_blob_size",
    "rocks.blob_file_size",
    "rocks.blob_garbage_collection_age_cutoff",
    "rocks.enable_blob_garbage_collection",
    "rocks.blob_compression_type"};
  // option, example: "rocks.binlogcf.enable_blob_files"
  // new_option, example: "rocks.enable_blob_files"
  // short_option, example: "enable_blob_files"
  string new_option = option;
  string short_option = "";
  auto argArray = tendisplus::stringSplit(option, ".");
  string specialCf = "";
  if (argArray.size() == 2) {
    // example: "rocks.enable_blob_files"
    short_option = argArray[1];
  } else if (argArray.size() == 3) {
    // example: "rocks.binlogcf.enable_blob_files"
    specialCf = argArray[1];
    short_option = argArray[2];
    new_option = argArray[0] + "." + argArray[2];
  }
  bool isDbOption = rocksdb_dynamic_options.count(new_option);
  bool isCfOption = rocksdb_cf_dynamic_options.count(new_option);

  if (!isDbOption && !isCfOption) {
    return {ErrorCodes::ERR_INTERNAL, option + " can't change dynamically"};
  }

  auto cf = ColumnFamilyNumber::ColumnFamily_All;
  if (isCfOption) {
    if (specialCf == "") {
      cf = ColumnFamilyNumber::ColumnFamily_Default;
    } else if (specialCf == "binlogcf") {
      cf = ColumnFamilyNumber::ColumnFamily_Binlog;
    } else {
      return {ErrorCodes::ERR_INTERNAL,
              "ColumnFamily " + specialCf + " not exist"};
    }
  }

  map[short_option] = value;

  if (isDbOption) {
    auto s = getBaseDB()->SetDBOptions(map);
    if (!s.ok()) {
      return {ErrorCodes::ERR_INTERNAL, s.ToString()};
    }
  } else {
    if (cf == ColumnFamilyNumber::ColumnFamily_All ||
        cf == ColumnFamilyNumber::ColumnFamily_Default) {
      auto s = getBaseDB()->SetOptions(
        getColumnFamilyHandle(ColumnFamilyNumber::ColumnFamily_Default), map);
      if (!s.ok()) {
        return {ErrorCodes::ERR_INTERNAL, s.ToString()};
      }
    }
    if (cf == ColumnFamilyNumber::ColumnFamily_All ||
        cf == ColumnFamilyNumber::ColumnFamily_Binlog) {
      auto s = getBaseDB()->SetOptions(
        getColumnFamilyHandle(ColumnFamilyNumber::ColumnFamily_Binlog), map);
      if (!s.ok()) {
        return {ErrorCodes::ERR_INTERNAL, s.ToString()};
      }
    }
  }
  return {ErrorCodes::ERR_OK, ""};
}

rocksdb::Iterator* RocksKVStore::newIterator(
  const rocksdb::ReadOptions& readOptions,
  rocksdb::ColumnFamilyHandle* columnFamily) {
  return getBaseDB()->NewIterator(readOptions, columnFamily);
}

const rocksdb::Snapshot* RocksKVStore::getSnapshot() {
  return getBaseDB()->GetSnapshot();
}

Status RocksKVStore::setCompactOnDeletionCollectorFactory(
  const std::string& option, const std::string& value) {
  if (option.substr(0, 25) != "rocks.compaction_deletes_") {
    return {ErrorCodes::ERR_INTERNAL, option + " is not rocksdb option"};
  }

#if ROCKSDB_MAJOR > 6 || (ROCKSDB_MAJOR == 6 && ROCKSDB_MINOR > 11)
  auto table_properties_collector_factories =
    getBaseDB()->GetOptions().table_properties_collector_factories;
  std::string errinfo;
  for (auto factory : table_properties_collector_factories) {
    if (std::string(factory->Name()) == "CompactOnDeletionCollector") {
      auto table_factory_option = option.substr(25, option.size() - 25);
      auto compactOnDel =
        static_cast<rocksdb::CompactOnDeletionCollectorFactory*>(factory.get());
      if (table_factory_option == "window") {
        auto ed = tendisplus::stoul(value);
        if (!ed.ok()) {
          errinfo = "invalid CompactOnDeletionCollector window value:" + value +
            " " + ed.status().toString();
          return {ErrorCodes::ERR_PARSEOPT, errinfo};
        }

        compactOnDel->SetWindowSize(ed.value());
        return {ErrorCodes::ERR_OK, ""};
      } else if (table_factory_option == "trigger") {
        auto ed = tendisplus::stoul(value);
        if (!ed.ok()) {
          errinfo =
            "invalid CompactOnDeletionCollector trigger value:" + value + " " +
            ed.status().toString();
          return {ErrorCodes::ERR_PARSEOPT, errinfo};
        }

        compactOnDel->SetDeletionTrigger(ed.value());
        return {ErrorCodes::ERR_OK, ""};
      } else if (table_factory_option == "ratio") {
        auto ed = tendisplus::stod(value);
        if (!ed.ok()) {
          errinfo = "invalid CompactOnDeletionCollector ratio value:" + value +
            " " + ed.status().toString();
          return {ErrorCodes::ERR_PARSEOPT, errinfo};
        }

        compactOnDel->SetDeletionRatio(ed.value());
        return {ErrorCodes::ERR_OK, ""};
      } else {
        return {ErrorCodes::ERR_INTERNAL,
                option +
                  " is not rocksdb-CompactOnDeletionTableFactory option"};
      }
    }
  }

  return {ErrorCodes::ERR_INTERNAL,
          "Options don't contain CompactOnDeletionTableFactory"};
#else
  return {ErrorCodes::ERR_INTERNAL,
          option + " can't be changed dynmaically in rocksdb(version < 6.11)"};
#endif
}

int64_t RocksKVStore::getOption(const std::string& option) {
  if (option == "rocks.max_background_jobs") {
    return getBaseDB()->GetDBOptions().max_background_jobs;
  } else if (option == "rocks.max_open_files") {
    return getBaseDB()->GetDBOptions().max_open_files;
  } else {
    return -2;
  }
}

rocksdb::WriteOptions RocksKVStore::writeOptions() {
  rocksdb::WriteOptions writeOpts;
  writeOpts.disableWAL = getCfg()->rocksDisableWAL;
  writeOpts.sync = getCfg()->rocksFlushLogAtTrxCommit;
  return writeOpts;
}

void RocksKVStore::resetStatistics() {
  _stats->Reset();
}

Expected<VersionMeta> RocksKVStore::getVersionMeta() {
  const std::string name("version");
  auto meta = getVersionMeta(name);
  if (!meta.ok()) {
    return meta.status();
  }
  return meta.value();
}

Expected<VersionMeta> RocksKVStore::getVersionMeta(const std::string& name) {
  VersionMeta defMeta(-1, -1, name);
  std::stringstream pkss;
  pkss << name << "_meta";
  RecordKey rk(VersionMeta::CHUNKID,
               VersionMeta::DBID,
               RecordType::RT_META,
               pkss.str(),
               "");
  auto ptxn = createTransaction(nullptr);
  if (!ptxn.ok()) {
    return ptxn.status();
  }
  auto expRv = getKV(rk, ptxn.value().get());
  if (!expRv.ok()) {
    if (expRv.status().code() == ErrorCodes::ERR_NOTFOUND) {
      return defMeta;
    }

    return expRv.status();
  }
  const auto& rv = expRv.value();

  auto meta = VersionMeta::decode(rk, rv);
  if (!meta.ok()) {
    return meta.status();
  }
  return meta.value();
}

/**
 * @brief get all version meta of this kv-store
 * @param txn transaction on this kv-store
 * @return vector contains all version meta of this kv-store
 */
Expected<std::vector<VersionMeta>> RocksKVStore::getAllVersionMeta(
  Transaction* txn) {
  auto cursor = txn->createVersionMetaCursor();
  std::vector<VersionMeta> versionMeta;
  RecordKey rk(
    VersionMeta::CHUNKID, VersionMeta::DBID, RecordType::RT_META, "", "");
  cursor->seek(rk.encode());
  while (true) {
    auto expRecord = cursor->next();

    // iterate all over this kvstore version meta data or no data
    if ((expRecord.status().code() == ErrorCodes::ERR_EXHAUST) ||
        (expRecord.status().code() == ErrorCodes::ERR_NOTFOUND)) {
      break;
    }
    RET_IF_ERR(expRecord.status());

    auto record = expRecord.value();
    versionMeta.emplace_back(record);
  }

  return versionMeta;
}

Status RocksKVStore::setVersionMeta(const std::string& name,
                                    uint64_t ts,
                                    uint64_t version) {
  std::stringstream pkss;
  pkss << name << "_meta";
  RecordKey rk(VersionMeta::CHUNKID,
               VersionMeta::DBID,
               RecordType::RT_META,
               pkss.str(),
               "");
  rapidjson::StringBuffer sb;
  rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
  writer.StartObject();
  writer.Key("timestamp");
  writer.Uint64(ts);
  writer.Key("version");
  writer.Uint64(version);
  writer.EndObject();

  RecordValue rv(sb.GetString(), RecordType::RT_META, -1);

  auto ptxn = createTransaction(nullptr);
  if (!ptxn.ok()) {
    return ptxn.status();
  }
  auto txn = std::move(ptxn.value());
  Status s = setKV(rk, rv, txn.get());
  if (!s.ok()) {
    return s;
  }
  s = txn->commit().status();

  return s;
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
    w.Key("alive_binlogs");
    w.Uint64(_aliveBinlogs.size());
    w.Key("min_alive_binlog");
    w.Uint64(_aliveBinlogs.size() ? _aliveBinlogs.begin()->first : 0);
    w.Key("max_alive_binlog");
    w.Uint64(_aliveBinlogs.size() ? _aliveBinlogs.rbegin()->first : 0);
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

RocksdbEnv::RocksdbEnv()
  : _errCnt(0), _reason(rocksdb::BackgroundErrorReason::kFlush), _bgError("") {}

void RocksdbEnv::setError(rocksdb::BackgroundErrorReason reason,
                          rocksdb::Status* error) {
  std::lock_guard<std::mutex> lk(_mutex);
  _reason = reason;
  _rocksbgError = *error;
  _bgError = error->ToString();
  _errCnt++;
}

void RocksdbEnv::clear() {
  std::lock_guard<std::mutex> lk(_mutex);
  _bgError = "";
#if ROCKSDB_MAJOR > 5 || (ROCKSDB_MAJOR == 5 && ROCKSDB_MINOR > 15)
  // do nothing
#else
  // TODO(vinchen): in rocksdb-5.13.4 there is no DB::Resume().
  // We can only reset the bg_error_ in rocksdb.
  _rocksbgError = rocksdb::Status::OK();
#endif
}

void RocksdbEnv::resetError() {
  std::lock_guard<std::mutex> lk(_mutex);
  _bgError = "";
#if ROCKSDB_MAJOR > 5 || (ROCKSDB_MAJOR == 5 && ROCKSDB_MINOR > 15)
  // do nothing
#else
  // TODO(vinchen): in rocksdb-5.13.4 there is no DB::Resume().
  // We reset the backgroundError in tendisplus.
  _rocksbgError = rocksdb::Status::OK();
#endif
}

std::string RocksdbEnv::getErrorString() const {
  std::lock_guard<std::mutex> lk(_mutex);
  if (_bgError == "") {
    return "";
  }

  std::stringstream ss;
  ss << "bgerror=" << _bgError;
  ss << ",reason=";

  switch (_reason) {
    case rocksdb::BackgroundErrorReason::kFlush:
      ss << "Flush";
      break;
    case rocksdb::BackgroundErrorReason::kCompaction:
      ss << "Compaction";
      break;
    case rocksdb::BackgroundErrorReason::kWriteCallback:
      ss << "WriteCallback";
      break;
    case rocksdb::BackgroundErrorReason::kMemTable:
      ss << "MemTable";
      break;
    default:
      INVARIANT_D(0);
      ss << "Unknown";
      break;
  }

  ss << ",count=" << std::to_string(_errCnt.load(memory_order_relaxed));

  return ss.str();
}

void BackgroundErrorListener::OnBackgroundError(
  rocksdb::BackgroundErrorReason reason, rocksdb::Status* bg_error) {
  if (bg_error) {
    _env->setError(reason, bg_error);
  }
}

}  // namespace tendisplus
