#ifndef SRC_TENDISPLUS_STORAGE_CATALOG_H_
#define SRC_TENDISPLUS_STORAGE_CATALOG_H_

#include "tendisplus/replication/repl_manager.h"

namespace tendisplus {

struct StoreMeta {
    uint32_t id;
    std::string syncFromHost;
    uint64_t binlogId;
    SyncStage stage;
};

class Catalog {
 public:
    Catalog(std::unique_ptr<KVStore> store);
    virtual ~Catalog() = default;
    Expected<StoreMeta> getStoreMeta(uint32_t idx);
    Status setStoreMeta(const StoreMeta& meta);

 private:
     std::unique_ptr<KVStore> _store;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_STORAGE_CATALOG_H_
