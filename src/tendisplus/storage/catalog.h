#ifndef SRC_TENDISPLUS_STORAGE_CATALOG_H_
#define SRC_TENDISPLUS_STORAGE_CATALOG_H_

#include <memory>

#include "tendisplus/replication/repl_manager.h"

namespace tendisplus {

enum class ReplState: std::uint8_t;

struct StoreMeta {
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
    uint64_t binlogId;
    ReplState replState;
};

class Catalog {
 public:
    explicit Catalog(std::unique_ptr<KVStore> store);
    virtual ~Catalog() = default;
    Expected<std::unique_ptr<StoreMeta>> getStoreMeta(uint32_t idx);
    Status setStoreMeta(const StoreMeta& meta);

 private:
     std::unique_ptr<KVStore> _store;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_STORAGE_CATALOG_H_
