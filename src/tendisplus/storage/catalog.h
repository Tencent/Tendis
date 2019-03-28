#ifndef SRC_TENDISPLUS_STORAGE_CATALOG_H_
#define SRC_TENDISPLUS_STORAGE_CATALOG_H_

#include <memory>
#include <string>

#include "tendisplus/replication/repl_manager.h"

namespace tendisplus {

enum class ReplState: std::uint8_t;

// for MSVC, it must be class , but not struct
// repl meta
class StoreMeta {
 public:
    StoreMeta();
    StoreMeta(const StoreMeta&) = default;
    StoreMeta(StoreMeta&&) = delete;
    StoreMeta(uint32_t id_, const std::string& syncFromHost_,
            uint16_t syncFromPort_, int32_t syncFromId_,
            uint64_t binlogId_, ReplState replState_);
    std::unique_ptr<StoreMeta> copy() const;

    uint32_t id;
    std::string syncFromHost;
    uint16_t syncFromPort;
    uint32_t syncFromId;  // the storeid of master
    uint64_t binlogId;
    ReplState replState;
};

// store meta
class StoreMainMeta {
 public:
    StoreMainMeta()
         :StoreMainMeta(-1, KVStore::StoreMode::STORE_NONE) {}
    StoreMainMeta(const StoreMainMeta&) = default;
    StoreMainMeta(StoreMainMeta&&) = delete;
    StoreMainMeta(uint32_t id_, KVStore::StoreMode mode_)
        : id(id_), storeMode(mode_){}
    std::unique_ptr<StoreMainMeta> copy() const;

    uint32_t id;
    KVStore::StoreMode storeMode;
};

class Catalog {
 public:
    explicit Catalog(std::unique_ptr<KVStore> store);
    virtual ~Catalog() = default;
    Expected<std::unique_ptr<StoreMeta>> getStoreMeta(uint32_t idx);
    Status setStoreMeta(const StoreMeta& meta);
    Status stop();
    Expected<std::unique_ptr<StoreMainMeta>> getStoreMainMeta(uint32_t idx);
    Status setStoreMainMeta(const StoreMainMeta& meta);

 private:
    std::unique_ptr<KVStore> _store;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_STORAGE_CATALOG_H_
