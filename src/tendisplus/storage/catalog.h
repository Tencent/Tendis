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
    int32_t syncFromId;  // the storeid of master
    // the binlog has been applied, but it's unreliable in the
    // catalog. KVStore->getHighestBinlogId() should been more reliable.
    uint64_t binlogId;
    ReplState replState;
    string replErr;
};

// store meta
class StoreMainMeta {
 public:
    StoreMainMeta()
         :StoreMainMeta(-1, KVStore::StoreMode::STORE_NONE) {}
    StoreMainMeta(const StoreMainMeta&) = default;
    StoreMainMeta(StoreMainMeta&&) = delete;
    StoreMainMeta(uint32_t id_, KVStore::StoreMode mode_)
        : id(id_), storeMode(mode_) {}
    std::unique_ptr<StoreMainMeta> copy() const;

    uint32_t id;
    KVStore::StoreMode storeMode;
};

// store meta
class MainMeta {
 public:
    MainMeta() : MainMeta(0, 0) {}
    MainMeta(const MainMeta&) = default;
    MainMeta(MainMeta&&) = delete;
    MainMeta(uint32_t instCount, uint32_t hashSpace_)
        : kvStoreCount(instCount), chunkSize(hashSpace_) {}
    std::unique_ptr<MainMeta> copy() const;

    uint32_t kvStoreCount;
    uint32_t chunkSize;
};

class VersionMeta {
 public:
    VersionMeta() : VersionMeta(0, 0) {}
    VersionMeta(const VersionMeta&) = default;
    VersionMeta(VersionMeta&&) = delete;
    VersionMeta(uint64_t ts, uint64_t v)
        : timestamp(ts), version(v) {
    }
    std::unique_ptr<VersionMeta> copy() const;

    uint64_t timestamp;
    uint64_t version;
};

class Catalog {
 public:
    Catalog(std::unique_ptr<KVStore> store,
        uint32_t kvStoreCount, uint32_t chunkSize);
    virtual ~Catalog() = default;
    // repl meta for each store
    Expected<std::unique_ptr<StoreMeta>> getStoreMeta(uint32_t idx);
    Status setStoreMeta(const StoreMeta& meta);
    Status stop();
    // main meta for each store
    Expected<std::unique_ptr<StoreMainMeta>> getStoreMainMeta(uint32_t idx);
    Status setStoreMainMeta(const StoreMainMeta& meta);
    // main meta
    Expected<std::unique_ptr<MainMeta>> getMainMeta();

    Expected<std::unique_ptr<VersionMeta>> getVersionMeta();
    Expected<std::unique_ptr<VersionMeta>> getVersionMeta(PStore store, std::string name);
    Status setVersionMeta(const VersionMeta& meta);

    uint32_t getKVStoreCount() const { return _kvStoreCount; }
    uint32_t getChunkSize() const { return _chunkSize; }

 private:
    Status setMainMeta(const MainMeta& meta);
    std::unique_ptr<KVStore> _store;
    uint32_t _kvStoreCount;
    uint32_t _chunkSize;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_STORAGE_CATALOG_H_
