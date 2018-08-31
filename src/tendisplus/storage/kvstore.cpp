#include "glog/logging.h"
#include "tendisplus/storage/kvstore.h"
#include "tendisplus/utils/portable.h"

namespace tendisplus {

BinlogCursor::BinlogCursor(std::unique_ptr<Cursor> cursor, uint64_t begin,
            uint64_t end)
        :_baseCursor(std::move(cursor)),
         _beginPrefix(ReplLogKey::prefix(begin)),
         _end(end) {
    _baseCursor->seek(_beginPrefix);
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
        if (explk.value().getTxnId() >= _end) {
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

KVStore::KVStore(const std::string& id, const std::string& path)
     :_id(id),
      _dbPath(path),
      _backupDir(path+"/"+id+"_bak") {
    filesystem::path mypath = _dbPath;
    if (filesystem::equivalent(mypath, "/")) {
        LOG(FATAL) << "dbpath set to root dir!";
    }
}

void BackupInfo::setCommitId(uint64_t id) {
    _commitId = id;
}

void BackupInfo::setFileList(const std::map<std::string, uint64_t>& fl) {
    _fileList = fl;
}

uint64_t BackupInfo::getCommitId() const {
    return _commitId;
}

const std::map<std::string, uint64_t>& BackupInfo::getFileList() const {
    return _fileList;
}

}  // namespace tendisplus
