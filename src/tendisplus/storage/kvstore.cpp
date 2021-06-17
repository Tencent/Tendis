// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <fstream>
#include "glog/logging.h"
#include "tendisplus/storage/kvstore.h"
#include "tendisplus/utils/portable.h"
#include "tendisplus/utils/time.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/cluster/cluster_manager.h"
#include "tendisplus/include/endian.h"

namespace tendisplus {
RepllogCursorV2::RepllogCursorV2(Transaction* txn, uint64_t begin, uint64_t end)
  : _txn(txn), _baseCursor(nullptr), _start(begin), _cur(begin), _end(end) {}

Status RepllogCursorV2::seekToLast() {
  if (_cur == Transaction::TXNID_UNINITED) {
    return {ErrorCodes::ERR_INTERNAL,
            "RepllogCursorV2 error, detailed at the error log"};
  }
  if (!_baseCursor) {
    _baseCursor = std::move(_txn->createBinlogCursor());
  }

  // NOTE(vinchen): it works because binlog has a maximum prefix.
  // see RecordType::RT_BINLOG, plz note that it's tricky.
  _baseCursor->seekToLast();
  auto key = _baseCursor->key();
  if (key.ok()) {
    if (RecordKey::decodeType(key.value()) == RecordType::RT_BINLOG) {
      auto v = ReplLogKeyV2::decode(key.value());
      if (!v.ok()) {
        LOG(ERROR) << "RepllogCursorV2::seekToLast() failed, reason:"
                   << v.status().toString();
        return v.status();
      }

      _cur = v.value().getBinlogId();
      return {ErrorCodes::ERR_OK, ""};
    } else {
      return {ErrorCodes::ERR_EXHAUST, "no binlog"};
    }
  } else {
    LOG(ERROR) << "RepllogCursorV2::seekToLast() failed, reason:"
               << key.status().toString();
  }

  return key.status();
}

Expected<MinbinlogInfo> RepllogCursorV2::getMinBinlogMeta(Transaction* txn,
        bool checkTs = true) {
  MinbinlogInfo binlogInfo;

  RecordKey key(REPLLOGKEYV2_META_CHUNKID, REPLLOGKEYV2_META_DBID,
                RecordType::RT_META, "", "");
  auto eval = txn->getKV(key.encode());
  if (eval.ok()) {
    auto v = RecordValue::decode(eval.value());
    if (!v.ok()) {
      LOG(ERROR) << "binlog META decode error:" << v.status().toString();
      return {ErrorCodes::ERR_INTERGER, "binlog META decode error"};
    }
    // old version only has binlogid, don't has timestamp,
    //   we need use cursor.
    if (v.value().getRecordType() != RecordType::RT_META) {
      LOG(ERROR) << "get binlog META error, RecordType:"
                 << static_cast<int>(v.value().getRecordType());
      return {ErrorCodes::ERR_INTERGER, "get binlog META error"};
    }

    if (v.value().getValue().size() == sizeof(uint64_t)) {
      if (checkTs) {
        LOG(ERROR) << "get binlog META error, size:"
          << v.value().getValue().size();
        return {ErrorCodes::ERR_INTERGER, "get binlog META error"};
      } else {
        binlogInfo.id = int64Decode(v.value().getValue().c_str());
        binlogInfo.ts = 0;
        return binlogInfo;
      }
    } else if (v.value().getValue().size() == 2 * sizeof(uint64_t)) {
      binlogInfo.id = int64Decode(v.value().getValue().c_str());
      binlogInfo.ts = int64Decode(v.value().getValue().c_str()
                                  + sizeof(uint64_t));
      return binlogInfo;
    } else {
      LOG(ERROR) << "get binlog META error, size:"
        << v.value().getValue().size();
      return {ErrorCodes::ERR_INTERGER, "get binlog META error"};
    }
  } else if (!eval.ok() && eval.status().code() != ErrorCodes::ERR_NOTFOUND) {
    LOG(WARNING) << "get binlog META error:" << eval.status().toString();
    return eval.status();
  }

  return {ErrorCodes::ERR_NOTFOUND, "has no binlog META"};
}

Expected<MinbinlogInfo> RepllogCursorV2::getMinBinlogByCursor(
        Transaction *txn) {
  MinbinlogInfo binlogInfo;
  auto cursor = txn->createBinlogCursor();
  if (!cursor) {
    return {ErrorCodes::ERR_INTERNAL, "txn->createBinlogCursor() error"};
  }
  cursor->seek(RecordKey::prefixReplLogV2());
  // TODO(vinchen): should more fast
  Expected<Record> expRcd = cursor->next();
  if (!expRcd.ok()) {
    return expRcd.status();
  }

  /*if (expRcd.value().getRecordKey().getRecordType()
      != RecordType::RT_BINLOG) {
      return {ErrorCodes::ERR_EXHAUST, ""};
  }*/

  const RecordKey& rk = expRcd.value().getRecordKey();
  auto explk = ReplLogKeyV2::decode(rk);
  if (!explk.ok()) {
    return explk.status();
  }
  binlogInfo.id = explk.value().getBinlogId();
  const RecordValue& rv = expRcd.value().getRecordValue();
  auto explv = ReplLogValueV2::decode(rv.encode());
  if (!explv.ok()) {
    return explv.status();
  }
  binlogInfo.ts = explv.value().getTimestamp();
  return binlogInfo;
}

Expected<MinbinlogInfo> RepllogCursorV2::getMinBinlog(Transaction* txn) {
  if (gParams != nullptr && gParams->saveMinBinlogId) {
    auto binlogInfo = getMinBinlogMeta(txn);
    if (binlogInfo.ok()) {
      return binlogInfo;
    }
    DLOG(WARNING) << "binlog META is not exists, will use seek.";
  }

  auto binlogInfo = getMinBinlogByCursor(txn);
  if (!binlogInfo.ok()) {
    LOG(WARNING) << "getMinBinlogByCursor failed:"
      << binlogInfo.status().toString();
  }
  return binlogInfo;
}

Expected<uint64_t> RepllogCursorV2::getMinBinlogId(Transaction* txn) {
  auto ret = getMinBinlog(txn);
  if (!ret.ok()) {
    return ret.status();
  }
  return ret.value().id;
}

Expected<ReplLogRawV2> RepllogCursorV2::getMaxBinlog(Transaction* txn) {
  auto cursor = txn->createBinlogCursor();
  if (!cursor) {
    return {ErrorCodes::ERR_INTERNAL, "txn->createBinlogCursor() error"};
  }

  // NOTE(vinchen): it works because binlog has a maximum prefix.
  // see RecordType::RT_BINLOG, plz note that it's tricky.
  cursor->seekToLast();
  Expected<Record> expRcd = cursor->next();
  if (!expRcd.ok()) {
    return expRcd.status();
  }


  /*if (expRcd.value().getRecordKey().getRecordType()
      != RecordType::RT_BINLOG) {
      return{ ErrorCodes::ERR_EXHAUST, "" };
  }*/

  // TODO(vinchen): too more copy
  return ReplLogRawV2(expRcd.value());
}

Expected<uint64_t> RepllogCursorV2::getMaxBinlogId(Transaction* txn) {
  auto cursor = txn->createBinlogCursor();
  if (!cursor) {
    return {ErrorCodes::ERR_INTERNAL, "txn->createBinlogCursor() error"};
  }

  // NOTE(vinchen): it works because binlog has a maximum prefix.
  // see RecordType::RT_BINLOG, plz note that it's tricky.
  cursor->seekToLast();
  auto key = cursor->key();
  if (key.ok()) {
    if (RecordKey::decodeType(key.value()) == RecordType::RT_BINLOG) {
      auto v = ReplLogKeyV2::decode(key.value());
      if (!v.ok()) {
        LOG(ERROR) << "ReplLogKeyV2::getMaxBinlogId() failed, reason:"
                   << v.status().toString();
        return v.status();
      }

      return v.value().getBinlogId();
    } else {
      return {ErrorCodes::ERR_EXHAUST, "no binlog"};
    }
  }
  return key.status();
}

Expected<ReplLogRawV2> RepllogCursorV2::next() {
  if (_cur == Transaction::TXNID_UNINITED) {
    return {ErrorCodes::ERR_INTERNAL,
            "RepllogCursorV2 error, detailed at the error log"};
  }

  uint64_t num = 0;
  while (_cur <= _end) {
    ReplLogKeyV2 key(_cur);
    auto keyStr = key.encode();
    auto eval = _txn->getKV(keyStr);
    if (eval.status().code() == ErrorCodes::ERR_NOTFOUND) {
      DLOG(WARNING) << "binlogid " << _cur << " is not exists";
      _cur++;
      if (num++ % 1000 == 0) {
        LOG(WARNING) << "RepllogCursorV2::next ERR_NOTFOUND too much,"
                     << " num:" << num << " _cur:" << _cur << " _end:" << _end;
      }
      continue;
    } else if (!eval.ok()) {
      LOG(WARNING) << "get binlogid " << _cur
                   << " error:" << eval.status().toString();
      return eval.status();
    }

    // INVARIANT_D(ReplLogValueV2::decode(eval.value()).ok());
    _cur++;
    return ReplLogRawV2(keyStr, eval.value());
  }

  return {ErrorCodes::ERR_EXHAUST, ""};
}

Expected<ReplLogV2> RepllogCursorV2::nextV2() {
  if (_cur == Transaction::TXNID_UNINITED) {
    return {ErrorCodes::ERR_INTERNAL,
            "RepllogCursorV2 error, detailed at the error log"};
  }

  uint64_t num = 0;
  while (_cur <= _end) {
    ReplLogKeyV2 key(_cur);
    auto keyStr = key.encode();
    auto eval = _txn->getKV(keyStr);
    if (eval.status().code() == ErrorCodes::ERR_NOTFOUND) {
      _cur++;
      DLOG(WARNING) << "binlogid " << _cur << " is not exists";

      if (num++ % 1000 == 0) {
        LOG(WARNING) << "RepllogCursorV2::nextV2 ERR_NOTFOUND too much,"
                     << " num:" << num << " _cur:" << _cur << " _end:" << _end;
      }
      continue;
    } else if (!eval.ok()) {
      LOG(WARNING) << "get binlogid " << _cur
                   << " error:" << eval.status().toString();
      return eval.status();
    }

    auto v = ReplLogV2::decode(keyStr, eval.value());
    if (!v.ok()) {
      return v.status();
    }
    _cur++;

    return std::move(v.value());
  }

  return {ErrorCodes::ERR_EXHAUST, ""};
}

BasicDataCursor::BasicDataCursor(std::unique_ptr<Cursor> cursor)
  : _baseCursor(std::move(cursor)), _seeked(false) {}

void BasicDataCursor::seek(const std::string& prefix) {
  _baseCursor->seek(prefix);
  _seeked = true;
}

// can't be used currently
/*void BasicDataCursor::seekToLast() {
    _baseCursor->seekToLast();
}*/

Expected<Record> BasicDataCursor::next() {
  if (!_seeked) {
    _baseCursor->seek("");
    _seeked = true;
  }

  auto expRcd = _baseCursor->next();
  if (expRcd.ok()) {
    Record dataRecord(expRcd.value());
    if (dataRecord.getRecordKey().getChunkId() < CLUSTER_SLOTS) {
      return dataRecord;
    } else {
      return {ErrorCodes::ERR_EXHAUST, "no more basic data"};
    }
  } else {
    return expRcd.status();
  }
}

Status BasicDataCursor::prev() {
  if (!_seeked) {
    _baseCursor->seek("");
    _seeked = true;
  }
  return _baseCursor->prev();
}

Expected<std::string> BasicDataCursor::key() {
  if (!_seeked) {
    _baseCursor->seek("");
    _seeked = true;
  }
  auto expKey = _baseCursor->key();
  if (expKey.ok()) {
    std::string dataKey = expKey.value();
    if (RecordKey::decodeChunkId(dataKey) < CLUSTER_SLOTS) {
      return dataKey;
    } else {
      return {ErrorCodes::ERR_EXHAUST, "no more basic data"};
    }
  } else {
    return expKey.status();
  }
}

AllDataCursor::AllDataCursor(std::unique_ptr<Cursor> cursor)
  : _baseCursor(std::move(cursor)), _seeked(false) {}

void AllDataCursor::seek(const std::string& prefix) {
  _baseCursor->seek(prefix);
  _seeked = true;
}

// can't be used if binlogUsingDefaultCF is true
void AllDataCursor::seekToLast() {
  _baseCursor->seekToLast();
  _seeked = true;
}

Expected<Record> AllDataCursor::next() {
  if (!_seeked) {
    _baseCursor->seek("");
    _seeked = true;
  }
  auto expRcd = _baseCursor->next();
  if (expRcd.ok()) {
    Record dataRecord(expRcd.value());
    if (dataRecord.getRecordKey().getChunkId() < REPLLOGKEYV2_META_CHUNKID) {
      return dataRecord;
    } else {
      return {ErrorCodes::ERR_EXHAUST, "no more AllData"};
    }
  } else {
    return expRcd.status();
  }
}

Status AllDataCursor::prev() {
  if (!_seeked) {
    _baseCursor->seek("");
    _seeked = true;
  }
  return _baseCursor->prev();
}

Expected<std::string> AllDataCursor::key() {
  if (!_seeked) {
    _baseCursor->seek("");
    _seeked = true;
  }

  auto expKey = _baseCursor->key();
  if (expKey.ok()) {
    std::string dataKey = expKey.value();
    if (RecordKey::decodeChunkId(dataKey) < REPLLOGKEYV2_META_CHUNKID) {
      return dataKey;
    } else {
      return {ErrorCodes::ERR_EXHAUST, "no more AllData"};
    }
  } else {
    return expKey.status();
  }
}

BinlogCursor::BinlogCursor(std::unique_ptr<Cursor> cursor)
  : _baseCursor(std::move(cursor)), _seeked(false) {}

void BinlogCursor::seek(const std::string& prefix) {
  _baseCursor->seek(prefix);
  _seeked = true;
}

void BinlogCursor::seekToLast() {
  _baseCursor->seekToLast();
  _seeked = true;
}

Expected<Record> BinlogCursor::next() {
  if (!_seeked) {
    _baseCursor->seek(RecordKey::prefixReplLogV2());
    _seeked = true;
  }
  auto expRcd = _baseCursor->next();
  if (expRcd.ok()) {
    Record binlogRecord(expRcd.value());
    if (binlogRecord.getRecordKey().getRecordType() == RecordType::RT_BINLOG) {
      return binlogRecord;
    } else {
      return {ErrorCodes::ERR_EXHAUST, "no more binlog"};
    }
  } else {
    return expRcd.status();
  }
}

Status BinlogCursor::prev() {
  if (!_seeked) {
    _baseCursor->seek(RecordKey::prefixReplLogV2());
    _seeked = true;
  }
  return _baseCursor->prev();
}

Expected<std::string> BinlogCursor::key() {
  if (!_seeked) {
    _baseCursor->seek(RecordKey::prefixReplLogV2());
    _seeked = true;
  }
  auto expKey = _baseCursor->key();
  if (expKey.ok()) {
    std::string binlogKey = expKey.value();
    if (RecordKey::decodeType(binlogKey) == RecordType::RT_BINLOG) {
      return binlogKey;
    } else {
      return {ErrorCodes::ERR_EXHAUST, "no more binlog"};
    }
  } else {
    return expKey.status();
  }
}

TTLIndexCursor::TTLIndexCursor(std::unique_ptr<Cursor> cursor,
                               std::uint64_t until)
  : _until(until), _baseCursor(std::move(cursor)) {
  _baseCursor->seek(RecordKey::prefixTTLIndex());
}

void TTLIndexCursor::seek(const std::string& target) {
  _baseCursor->seek(target);
}

void TTLIndexCursor::prev() {
  _baseCursor->prev();
}

Expected<std::string> TTLIndexCursor::key() {
  return _baseCursor->key();
}

Expected<TTLIndex> TTLIndexCursor::next() {
  Expected<Record> expRcd = _baseCursor->next();
  if (expRcd.ok()) {
    const RecordKey& rk = expRcd.value().getRecordKey();
    if (rk.getRecordType() != RecordType::RT_TTL_INDEX) {
      return {ErrorCodes::ERR_EXHAUST, "no more ttl index"};
    }

    auto explk = TTLIndex::decode(rk);
    if (!explk.ok()) {
      return explk.status();
    }

    if (explk.value().getTTL() > _until) {
      return {ErrorCodes::ERR_NOT_EXPIRED, "read until ttl"};
    }

    return explk;
  } else {
    return expRcd.status();
  }
}

VersionMetaCursor::VersionMetaCursor(std::unique_ptr<Cursor> cursor)
  : _baseCursor(std::move(cursor)) {
  _baseCursor->seek(RecordKey::prefixVersionMeta());
}

void VersionMetaCursor::seek(const std::string& target) {
  _baseCursor->seek(target);
}

void VersionMetaCursor::prev() {
  _baseCursor->prev();
}

Expected<std::string> VersionMetaCursor::key() {
  return _baseCursor->key();
}

Expected<VersionMeta> VersionMetaCursor::next() {
  Expected<Record> expRcd = _baseCursor->next();
  if (expRcd.ok()) {
    const RecordKey& rk = expRcd.value().getRecordKey();
    const RecordValue& rv = expRcd.value().getRecordValue();
    if (rk.getRecordType() != RecordType::RT_META) {
      return {ErrorCodes::ERR_EXHAUST, "no more version meta"};
    }
    auto expmeta = VersionMeta::decode(rk, rv);
    if (!expmeta.ok()) {
      return expmeta.status();
    }
    return expmeta;
  } else {
    return expRcd.status();
  }
}

SlotCursor::SlotCursor(std::unique_ptr<Cursor> cursor, uint32_t slot)
  : _slot(slot), _baseCursor(std::move(cursor)) {
  RecordKey tmplRk(slot, 0, RecordType::RT_DATA_META, "", "");
  auto prefix = tmplRk.prefixSlotType();
  _baseCursor->seek(prefix);
}

Expected<Record> SlotCursor::next() {
  Expected<Record> expRcd = _baseCursor->next();
  if (expRcd.ok()) {
    const RecordKey& rk = expRcd.value().getRecordKey();
    if (rk.getRecordType() != RecordType::RT_DATA_META ||
        rk.getChunkId() != _slot) {
      return {ErrorCodes::ERR_EXHAUST, "no more primary key"};
    }
    return expRcd.value();
  } else {
    return expRcd.status();
  }
}

SlotsCursor::SlotsCursor(std::unique_ptr<Cursor> cursor,
                         uint32_t begin,
                         uint32_t end)
  : _startSlot(begin), _endSlot(end), _baseCursor(std::move(cursor)) {
  RecordKey tmplRk(begin, 0, RecordType::RT_DATA_META, "", "");
  auto prefix = tmplRk.prefixChunkid();
  _baseCursor->seek(prefix);
}

Expected<Record> SlotsCursor::next() {
  Expected<Record> expRcd = _baseCursor->next();
  if (expRcd.ok()) {
    const RecordKey& rk = expRcd.value().getRecordKey();
    if (rk.getChunkId() > _endSlot - 1) {
      return {ErrorCodes::ERR_EXHAUST, "no more primary key"};
    }
    return expRcd.value();
  } else {
    return expRcd.status();
  }
}


KVStore::KVStore(const std::string& id, const std::string& path)
  : _id(id), _dbPath(path), _backupDir(path + "/" + id + "_bak") {
  filesystem::path mypath = _dbPath;
#ifndef _WIN32
  if (filesystem::equivalent(mypath, "/")) {
    LOG(FATAL) << "dbpath set to root dir!";
  }
#endif
}

uint64_t KVStore::getBinlogTime() {
  return _binlogTimeSpov.load(std::memory_order_relaxed);
}

void KVStore::setBinlogTime(uint64_t timestamp) {
  _binlogTimeSpov.store(timestamp, std::memory_order_relaxed);
}

uint64_t KVStore::getCurrentTime() {
  uint64_t ts = 0;
  if (getMode() == KVStore::StoreMode::REPLICATE_ONLY) {
    // NOTE(vinchen): Here it may return zero, because the
    // slave never apply one binlog yet.
    ts = getBinlogTime();
  } else {
    ts = msSinceEpoch();
  }
  return ts;
}

std::ofstream* KVStore::createBinlogFile(const std::string& name,
                                         uint32_t storeId) {
  std::ofstream* fs = new std::ofstream(
    name.c_str(), std::ios::out | std::ios::app | std::ios::binary);
  if (!fs->is_open()) {
    LOG(ERROR) << "fs->is_open() failed:" << name;
    return nullptr;
  }

  // the header
  fs->write(BINLOG_HEADER_V2, strlen(BINLOG_HEADER_V2));
  if (!fs->good()) {
    LOG(ERROR) << "fs->write() failed:" << name;
    return nullptr;
  }
  uint32_t storeIdTrans = htobe32(storeId);
  fs->write(reinterpret_cast<char*>(&storeIdTrans), sizeof(storeIdTrans));
  if (!fs->good()) {
    LOG(ERROR) << "fs->write() failed:" << name;
    return nullptr;
  }
  return fs;
}

BackupInfo::BackupInfo()
  : _binlogPos(Transaction::TXNID_UNINITED),
    _backupMode(0),
    _startTimeSec(0),
    _endTimeSec(0),
    _binlogVersion(BinlogVersion::BINLOG_VERSION_1) {}

void BackupInfo::setFileList(const std::map<std::string, uint64_t>& fl) {
  _fileList = fl;
}

const std::map<std::string, uint64_t>& BackupInfo::getFileList() const {
  return _fileList;
}

void BackupInfo::addFile(const std::string& file, uint64_t size) {
  _fileList[file] = size;
}

void BackupInfo::setBinlogPos(uint64_t pos) {
  _binlogPos = pos;
}

void BackupInfo::setBackupMode(uint8_t mode) {
  _backupMode = mode;
}
void BackupInfo::setStartTimeSec(uint64_t time) {
  _startTimeSec = time;
}
void BackupInfo::setEndTimeSec(uint64_t time) {
  _endTimeSec = time;
}

void BackupInfo::setBinlogVersion(BinlogVersion binlogversion) {
  _binlogVersion = binlogversion;
}

BinlogVersion BackupInfo::getBinlogVersion() const {
  return _binlogVersion;
}

uint64_t BackupInfo::getBinlogPos() const {
  return _binlogPos;
}

uint8_t BackupInfo::getBackupMode() const {
  return _backupMode;
}

uint64_t BackupInfo::getStartTimeSec() const {
  return _startTimeSec;
}

uint64_t BackupInfo::getEndTimeSec() const {
  return _endTimeSec;
}
}  // namespace tendisplus
