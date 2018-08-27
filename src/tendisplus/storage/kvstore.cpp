#include "glog/logging.h"
#include "tendisplus/storage/kvstore.h"
#include "tendisplus/utils/portable.h"

namespace tendisplus {

KVStore::KVStore(const std::string& id, const std::string& path)
     :_id(id),
      _dbPath(path) {
    filesystem::path mypath = _dbPath;
    if (filesystem::equivalent(mypath, "/")) {
        LOG(FATAL) << "dbpath set to root dir!";
    }
}

}  // namespace tendisplus
