#ifndef SRC_TENDISPLUS_REPLICATION_REPL_MANAGER_H_
#define SRC_TENDISPLUS_REPLICATION_REPL_MANAGER_H_

#include <memory>
#include <vector>
#include <utility>
#include <list>
#include <map>
#include <string>

#include "tendisplus/server/server_entry.h"
#include "tendisplus/storage/catalog.h"
#include "tendisplus/network/blocking_tcp_client.h"

namespace tendisplus {

using SCLOCK = std::chrono::steady_clock;

// slave's pov, sync status
struct SPovStatus {
    bool isRunning;
    uint64_t sessionId;
    SCLOCK::time_point nextSchedTime;
    SCLOCK::time_point lastSyncTime;
};

struct MPovStatus {
    bool isRunning;
    uint32_t dstStoreId;
    // the greatest id that has been applied
    uint64_t binlogPos;
    SCLOCK::time_point nextSchedTime;
    std::unique_ptr<BlockingTcpClient> client;
    uint64_t clientId;
};

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

class ReplManager {
 public:
    explicit ReplManager(std::shared_ptr<ServerEntry> svr);
    Status startup();
    void stop();
    Status changeReplSource(uint32_t storeId, std::string ip, uint32_t port,
            uint32_t sourceStoreId);
    std::unique_ptr<BlockingTcpClient> createClient(const StoreMeta&);
    void slaveStartFullsync(const StoreMeta&);
    void slaveChkSyncStatus(const StoreMeta&);

    // binlogPos: the greatest id that has been applied
    Expected<uint64_t> masterSendBinlog(BlockingTcpClient*,
            uint32_t storeId, uint32_t dstStoreId, uint64_t binlogPos);
    void masterPushRoutine(uint32_t storeId, uint64_t clientId);
    void slaveSyncRoutine(uint32_t  storeId);
    void supplyFullSync(asio::ip::tcp::socket sock,
            const std::string& storeIdArg);
    void registerIncrSync(asio::ip::tcp::socket sock,
            const std::string& storeIdArg,
            const std::string& dstStoreIdArg,
            const std::string& binlogPosArg);
    void changeReplState(const StoreMeta& storeMeta, bool persist);
    Status applyBinlogs(uint32_t storeId, uint64_t sessionId,
            const std::map<uint64_t, std::list<ReplLog>>& binlogs);

    static constexpr size_t POOL_SIZE = 12;

 protected:
    void controlRoutine();
    void supplyFullSyncRoutine(std::unique_ptr<BlockingTcpClient> client,
            uint32_t storeId);
    Status applySingleTxn(uint32_t storeId, uint64_t txnId,
        const std::list<ReplLog>& ops);
    bool isFullSupplierFull() const;

 private:
    void changeReplStateInLock(const StoreMeta&, bool persist);

    std::mutex _mutex;
    std::condition_variable _cv;
    std::atomic<bool> _isRunning;
    std::shared_ptr<ServerEntry> _svr;

    // slave's pov, meta data.
    std::vector<std::unique_ptr<StoreMeta>> _syncMeta;

    // slave's pov, sync status
    std::vector<std::unique_ptr<SPovStatus>> _syncStatus;

    // master's pov, living slave clients
    std::vector<std::map<uint64_t, std::unique_ptr<MPovStatus>>> _pushStatus;

    std::unique_ptr<WorkerPool> _fullPusher;

    std::unique_ptr<WorkerPool> _incrPusher;

    std::unique_ptr<WorkerPool> _fullReceiver;

    // slave's pov, periodly check incr-sync status
    std::unique_ptr<WorkerPool> _incrChecker;

    // master's pov, smallest binlogId, moves on when truncated
    std::vector<uint64_t> _firstBinlogId;

    std::atomic<uint64_t> _clientIdGen;

    std::thread _controller;

    std::shared_ptr<PoolMatrix> _fullPushMatrix;
    std::shared_ptr<PoolMatrix> _incrPushMatrix;
    std::shared_ptr<PoolMatrix> _fullReceiveMatrix;
    std::shared_ptr<PoolMatrix> _incrCheckMatrix;
};

}  // namespace tendisplus


#endif  // SRC_TENDISPLUS_REPLICATION_REPL_MANAGER_H_
