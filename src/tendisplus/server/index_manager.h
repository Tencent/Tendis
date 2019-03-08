#ifndef SRC_TENDISPLUS_REPLICATION_INDEX_MANAGER_H_
#define SRC_TENDISPLUS_REPLICATION_INDEX_MANAGER_H_

#include <unordered_map>

#include "tendisplus/server/server_entry.h"
#include "tendisplus/network/worker_pool.h"

namespace tendisplus {

class IndexManager {
 public:
    explicit IndexManager(std::shared_ptr<ServerEntry> svr);
    Status startup();
    void stop();
    Status run();
    Status scanExpiredKeysJob(uint32_t storeId);
    bool tryDelExpiredKeysJob(uint32_t storeId);
    bool isRunning();

 private:
    std::unique_ptr<WorkerPool> _indexScanner;
    std::unique_ptr<WorkerPool> _keyDeleter;
    std::unordered_map<std::size_t, std::list<TTLIndex>> _expiredKeys;
    std::unordered_map<std::size_t, std::string> _scanPoints;

    std::atomic<bool>  _isRunning;
    std::shared_ptr<ServerEntry> _svr;
    std::thread _runner;
    std::mutex _mutex;

    std::shared_ptr<PoolMatrix> _scannerMatrix;
    std::shared_ptr<PoolMatrix> _deleterMatrix;

    std::uint64_t _totalDequeue;
    std::uint64_t _totalEnqueue;
};

}  // namespace tendisplus

#endif
