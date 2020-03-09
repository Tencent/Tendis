#include <fstream>
#include "glog/logging.h"
#include "tendisplus/storage/kvstore.h"
#include "tendisplus/utils/portable.h"
#include "tendisplus/utils/time.h"
#include "tendisplus/utils/invariant.h"
#include "endian.h"

namespace tendisplus {
#ifdef BINLOG_V1
BinlogCursor::BinlogCursor(std::unique_ptr<Cursor> cursor, uint64_t begin,
            uint64_t end)
        :_baseCursor(std::move(cursor)),
         _beginPrefix(ReplLogKey::prefix(begin)),
         _end(end) {
    _baseCursor->seek(_beginPrefix);
}

void BinlogCursor::seekToLast() {
    // NOTE(deyukong): it works because binlog has a maximum prefix.
    // see RecordType::RT_BINLOG, plz note that it's tricky.
    _baseCursor->seekToLast();
}

Expected<ReplLog> BinlogCursor::next() {
    Expected<Record> expRcd = _baseCursor->next();
    if (expRcd.ok()) {
        const RecordKey& rk = expRcd.value().getRecordKey();
        if (rk.getRecordType() != RecordType::RT_BINLOG) {
            return {ErrorCodes::ERR_EXHAUST, ""};
        }
        auto explk = ReplLogKey::decode(rk);
        if (!explk.ok()) {
            return explk.status();
        }
        if (explk.value().getTxnId() > _end) {
            return {ErrorCodes::ERR_EXHAUST, ""};
        }
        Expected<ReplLogValue> val =
            ReplLogValue::decode(expRcd.value().getRecordValue());
        if (!val.ok()) {
            return val.status();
        }
        return ReplLog(std::move(explk.value()), std::move(val.value()));
    } else {
        return expRcd.status();
    }
}
#else

RepllogCursorV2::RepllogCursorV2(Transaction *txn, uint64_t begin, uint64_t end)
        : _txn(txn),
          _baseCursor(nullptr),
          _start(begin),
          _cur(begin),
          _end(end) {
}

Status RepllogCursorV2::seekToLast() {
    if (_cur == Transaction::TXNID_UNINITED) {
        return {ErrorCodes::ERR_INTERNAL,
                "RepllogCursorV2 error, detailed at the error log"};
    }
    if (!_baseCursor) {
        _baseCursor = _txn->createCursor();
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

Expected<ReplLogRawV2> RepllogCursorV2::getMinBinlog(Transaction *txn) {
    auto cursor = txn->createCursor();
    if (!cursor) {
        return {ErrorCodes::ERR_INTERNAL, "txn->createCursor() error"};
    }
    cursor->seek(RecordKey::prefixReplLogV2());
    // TODO(vinchen): should more fast
    Expected<Record> expRcd = cursor->next();
    if (!expRcd.ok()) {
        return expRcd.status();
    }

    if (expRcd.value().getRecordKey().getRecordType()
        != RecordType::RT_BINLOG) {
        return {ErrorCodes::ERR_EXHAUST, ""};
    }

    // TODO(vinchen): too more copy
    return ReplLogRawV2(expRcd.value());
}

Expected<uint64_t> RepllogCursorV2::getMinBinlogId(Transaction *txn) {
    auto cursor = txn->createCursor();
    if (!cursor) {
        return {ErrorCodes::ERR_INTERNAL, "txn->createCursor() error"};
    }
    cursor->seek(RecordKey::prefixReplLogV2());
    Expected<Record> expRcd = cursor->next();
    if (!expRcd.ok()) {
        return expRcd.status();
    }

    if (expRcd.value().getRecordKey().getRecordType()
        != RecordType::RT_BINLOG) {
        return {ErrorCodes::ERR_EXHAUST, ""};
    }

    const RecordKey &rk = expRcd.value().getRecordKey();
    auto explk = ReplLogKeyV2::decode(rk);
    if (!explk.ok()) {
        return explk.status();
    }
    return explk.value().getBinlogId();
}

Expected<ReplLogRawV2> RepllogCursorV2::getMaxBinlog(Transaction* txn) {
    auto cursor = txn->createCursor();
    if (!cursor) {
        return{ ErrorCodes::ERR_INTERNAL, "txn->createCursor() error" };
    }

    // NOTE(vinchen): it works because binlog has a maximum prefix.
    // see RecordType::RT_BINLOG, plz note that it's tricky.
    cursor->seekToLast();
    Expected<Record> expRcd = cursor->next();
    if (!expRcd.ok()) {
        return expRcd.status();
    }

    if (expRcd.value().getRecordKey().getRecordType()
        != RecordType::RT_BINLOG) {
        return{ ErrorCodes::ERR_EXHAUST, "" };
    }

    // TODO(vinchen): too more copy
    return ReplLogRawV2(expRcd.value());
}

Expected<uint64_t> RepllogCursorV2::getMaxBinlogId(Transaction* txn) {
    auto cursor = txn->createCursor();
    if (!cursor) {
        return {ErrorCodes::ERR_INTERNAL, "txn->createCursor() error"};
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

    while (_cur <= _end) {
        ReplLogKeyV2 key(_cur);
        auto keyStr = key.encode();
        auto eval = _txn->getKV(keyStr);
        if (eval.status().code() == ErrorCodes::ERR_NOTFOUND) {
            _cur++;
            DLOG(WARNING) << "binlogid " << _cur << " is not exists";

            continue;
        } else if (!eval.ok()) {
            LOG(WARNING) << "get binlogid " << _cur << " error:"
                         << eval.status().toString();
            return eval.status();
        }
#ifdef TENDIS_DEBUG
        INVARIANT_D(ReplLogValueV2::decode(eval.value()).ok());
#endif
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

    while (_cur <= _end) {
        ReplLogKeyV2 key(_cur);
        auto keyStr = key.encode();
        auto eval = _txn->getKV(keyStr);
        if (eval.status().code() == ErrorCodes::ERR_NOTFOUND) {
            _cur++;
            LOG(WARNING) << "binlogid " << _cur << " is not exists";

            continue;
        } else if (!eval.ok()) {
            LOG(WARNING) << "get binlogid " << _cur << " error:"
                         << eval.status().toString();
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

#endif

TTLIndexCursor::TTLIndexCursor(std::unique_ptr<Cursor> cursor,
                               std::uint64_t until)
        : _until(until), _baseCursor(std::move(cursor)) {
    _baseCursor->seek(RecordKey::prefixTTLIndex());
}

void TTLIndexCursor::seek(const std::string &target) {
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
        const RecordKey &rk = expRcd.value().getRecordKey();
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

SlotCursor::SlotCursor(std::unique_ptr<Cursor> cursor,
                       uint32_t slot)
        : _slot(slot), _baseCursor(std::move(cursor)) {
    RecordKey tmplRk(slot,
                     0,
                     RecordType::RT_DATA_META,
                     "", "");
    auto prefix = tmplRk.prefixSlotType();
    _baseCursor->seek(prefix);

}

Expected<Record> SlotCursor::next() {
    Expected<Record> expRcd = _baseCursor->next();
    if (expRcd.ok()) {
        const RecordKey &rk = expRcd.value().getRecordKey();
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
                         uint32_t begin, uint32_t end)
        : _startSlot(begin), _endSlot(end), _baseCursor(std::move(cursor)) {

    RecordKey tmplRk(begin,
                     0,
                     RecordType::RT_KV,
                     "", "");
    auto prefix = tmplRk.prefixSlotType();
    _baseCursor->seek(prefix);
}

Expected<Record> SlotsCursor::next() {
    Expected<Record> expRcd = _baseCursor->next();
    if (expRcd.ok()) {
        const RecordKey &rk = expRcd.value().getRecordKey();
        if (rk.getChunkId() > _endSlot - 1) {
            return {ErrorCodes::ERR_EXHAUST, "no more primary key"};
        }
        return expRcd.value();
    } else  {
        return expRcd.status();
    }
}




KVStore::KVStore(const std::string& id, const std::string& path)
     :_id(id),
      _dbPath(path),
      _backupDir(path+"/"+id+"_bak") {
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
    if ( getMode() == KVStore::StoreMode::REPLICATE_ONLY ) {
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
    std::ofstream* fs = new std::ofstream(name.c_str(),
        std::ios::out | std::ios::app | std::ios::binary);
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
    :_binlogPos(Transaction::TXNID_UNINITED) {
}

void BackupInfo::setFileList(const std::map<std::string, uint64_t>& fl) {
    _fileList = fl;
}

const std::map<std::string, uint64_t>& BackupInfo::getFileList() const {
    return _fileList;
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
