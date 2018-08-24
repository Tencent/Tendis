#include "tendisplus/replication/repl_manager.h"

namespace tendisplus {

ReplManager::ReplManager(std::shared_ptr<ServerEntry> svr)
        :_svr(svr) {
}

Status ReplManager::startup() {
    Catalog *catalog = _svr->getCatalog();
    if (!catalog) {
        LOG(FATAL) << "ReplManager::startup catalog not inited!";
    }

    for (uint32_t i = 0; i < KVStore::INSTANCE_NUM; i++) {
        Expected<StoreMeta> meta = catalog->getStoreMeta(i);
        if (meta.ok()) {
            continue;
        }
        if (meta.status().code() == ErrorCodes::ERR_NOTFOUND) {
            StoreMeta tmp{i, ""};
            Status s = catalog->setStoreMeta(tmp);
            if (!s.ok()) {
                return s;
            }
            _meta.emplace_back(tmp);
        }
    }

    return Status{ErrorCodes::ERR_OK, ""};
}

}  // namespace tendisplus
