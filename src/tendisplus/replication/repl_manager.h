#ifndef SRC_TENDISPLUS_REPLICATION_REPL_MANAGER_H_
#define SRC_TENDISPLUS_REPLICATION_REPL_MANAGER_H_

#include <memory>
#include <vector>
#include <utility>
#include <list>
#include "tendisplus/server/server_entry.h"
#include "tendisplus/storage/catalog.h"
#include "tendisplus/network/blocking_tcp_client.h"

namespace tendisplus {

using SCLOCK = std::chrono::steady_clock;

// 1) a new slave store's state is default to REPL_NONE
// when it receives a slaveof command, its state steps to
// REPL_CONNECT, when the scheduler sees the new state, it
// tries to connect master, auth and send initsync command.
// "connect/auth/initsync" are done in one schedule unit.
// if successes, it steps to REPL_TRANSFER state,
// otherwise, it keeps REPL_CONNECT state and wait for next
// schedule.

// 2) when slave's state steps into REPL_TRANSFER. a transfer
// file list is stores in _fetchStatus[i]. master keeps sending
// physical files to slaves.

// 3) REPL_TRANSFER wont be persisted, if the procedure fails,
// no matter network error or process crashes, slave will turn
// to REPL_CONNECT state and retry from 1)

// 4) on master side, each store can have only one slave copying
// physical data. the backup will be released after failure or
// procedure done.
enum class ReplState: std::uint8_t {
    REPL_NONE = 0,
    REPL_CONNECT = 1,
    REPL_TRANSFER = 2,  // initialsync, transfer whole db
    REPL_CONNECTED = 3,  // steadysync, transfer binlog steady
};

class ServerEntry;
class StoreMeta;

struct FetchStatus {
    bool isRunning;
    SCLOCK::time_point nextSchedTime;
};

class ReplManager {
 public:
    explicit ReplManager(std::shared_ptr<ServerEntry> svr);
    Status startup();
    void stop();
    void supplyFullSync(asio::ip::tcp::socket, uint32_t storeId);
    Status changeReplSource(uint32_t storeId, std::string ip, uint32_t port,
            uint32_t sourceStoreId);
    static constexpr size_t POOL_SIZE = 12;

 protected:
    void supplyFullSyncRoutine(std::unique_ptr<BlockingTcpClient> client,
            uint32_t storeId);
    void controlRoutine();
    void fetchRoutine(uint32_t idx);
    BlockingTcpClient *ensureClient(uint32_t idx);
    void startFullSync(const StoreMeta& metaSnapshot);
    void changeReplState(const StoreMeta&, bool persist);
    Expected<uint64_t> fetchBinlog(const StoreMeta&);
    Status applySingleTxn(uint32_t storeId, uint64_t txnId,
        const std::list<ReplLog>& ops);
    bool isFullSupplierFull() const;

 private:
    void changeReplStateInLock(const StoreMeta&, bool persist);

    std::mutex _mutex;
    std::condition_variable _cv;
    std::atomic<bool> _isRunning;
    std::shared_ptr<ServerEntry> _svr;

    // protected by _mutex
    std::vector<std::unique_ptr<FetchStatus>> _fetchStatus;
    std::vector<std::unique_ptr<StoreMeta>> _fetchMeta;
    std::vector<std::unique_ptr<BlockingTcpClient>> _fetchClients;

    // slave side incr-sync threadpool
    std::unique_ptr<WorkerPool> _fetcher;
    // slave side full-sync threadpool
    std::unique_ptr<WorkerPool> _fullFetcher;
    // master side full-sync threadpool
    std::unique_ptr<WorkerPool> _fullSupplier;

    std::thread _controller;

    std::shared_ptr<PoolMatrix> _fetcherMatrix;
    std::shared_ptr<PoolMatrix> _fullFetcherMatrix;
    std::shared_ptr<PoolMatrix> _fullSupplierMatrix;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_REPLICATION_REPL_MANAGER_H_
