#include "glog/logging.h"
#include "tendisplus/storage/kvstore.h"
#include "tendisplus/utils/portable.h"
#include "tendisplus/utils/time.h"

namespace tendisplus {

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

uint32_t KVStore::getBinlogTime() {
    return _binlogTimeSpov.load(std::memory_order_relaxed);
}

void KVStore::setBinlogTime(uint32_t timestamp) {
    _binlogTimeSpov.store(timestamp, std::memory_order_relaxed);
}

uint64_t KVStore::getCurrentTime() {
    uint64_t ts = 0;
    if ( getMode() == KVStore::StoreMode::REPLICATE_ONLY ) {
        // NOTE(vinchen): Here it may return zero, because the
        // slave never apply one binlog yet.
        ts = ((uint64_t)getBinlogTime()) * 1000;
    } else {
        ts = msSinceEpoch();
    }
    return ts;
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

uint64_t BackupInfo::getBinlogPos() const {
    return _binlogPos;
}

}  // namespace tendisplus
