#ifndef SRC_TENDISPLUS_REPLICATION_REPL_MANAGER_H_
#define SRC_TENDISPLUS_REPLICATION_REPL_MANAGER_H_

#include <memory>
#include <utility>
#include "tendisplus/server/server_entry.h"
#include "tendisplus/storage/catalog.h"

namespace tendisplus {

enum class SyncStage {
    SYNC_NONE = 0,
    SYNC_INIT = 1,
    SYNC_STEADY = 2,
};

class StoreMeta;
class ServerEntry;

class ReplManager {
 public:
    ReplManager(std::shared_ptr<ServerEntry> svr);
    Status startup();

 private:
    std::shared_ptr<ServerEntry> _svr;
    std::vector<StoreMeta> _meta;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_REPLICATION_REPL_MANAGER_H_
