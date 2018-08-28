#include "glog/logging.h"
#include "tendisplus/storage/kvstore.h"
#include "tendisplus/utils/portable.h"

namespace tendisplus {

KVStore::KVStore(const std::string& id, const std::string& path)
     :_id(id),
      _dbPath(path),
      _backupDir(path+"/"+id+"_bak") {
    filesystem::path mypath = _dbPath;
    if (filesystem::equivalent(mypath, "/")) {
        LOG(FATAL) << "dbpath set to root dir!";
    }
}

void BackupInfo::setCommitId(const Transaction::CommitId& id) {
    _commitId = id;
}

void BackupInfo::setFileList(const std::map<std::string, uint64_t>& fl) {
    _fileList = fl;
}

Transaction::CommitId BackupInfo::getCommitId() const {
    return _commitId;
}

const std::map<std::string, uint64_t>& BackupInfo::getFileList() const {
    return _fileList;
}

}  // namespace tendisplus
