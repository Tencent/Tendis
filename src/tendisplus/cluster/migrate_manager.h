#ifndef SRC_TENDISPLUS_CLUSTER_MIGRATE_MANAGER_H_
#define SRC_TENDISPLUS_CLUSTER_MIGRATE_MANAGER_H_

#include "tendisplus/server/server_entry.h"
#include "tendisplus/network/blocking_tcp_client.h"
#include "tendisplus/utils/rate_limiter.h"
#include "tendisplus/cluster/migrate_sender.h"
#include "tendisplus/cluster/migrate_receiver.h"
#include "tendisplus/storage/catalog.h"

namespace tendisplus {

class ServerEntry;
class StoreMeta;
class ChunkMeta;
class ChunkMigrateSender;
class ChunkMigrateReceiver;

// Sender POV
enum class MigrateSendState {
    NONE = 0,
    WAIT,
    START,
    CATCHUP,
    SUCC,
    CLEAR,
    ERR
};

class MigrateSendTask {
public:
    explicit MigrateSendTask(uint32_t chunk_id, std::shared_ptr<ServerEntry> svr,
        const std::shared_ptr<ServerParams> cfg) :
        chunkid(chunk_id),
        isRunning(false),
        state(MigrateSendState::WAIT),
        sender(chunk_id, svr, cfg){
    }
    void setClient(std::shared_ptr<BlockingTcpClient> client);

    uint32_t chunkid;
    uint32_t storeid;
    bool isRunning;
    SCLOCK::time_point nextSchedTime;
    MigrateSendState state;
    ChunkMigrateSender sender;
};

// receiver POV
enum class MigrateReceiveState {
    NONE = 0,
    RECEIVE_SNAPSHOT,
    RECEIVE_BINLOG,
    SUCC,
    ERR
};

class MigrateReceiveTask{
public:
    explicit MigrateReceiveTask(uint32_t chunk_id, uint32_t store_id, string& ip, uint16_t port,
        std::shared_ptr<ServerEntry> svr,
        const std::shared_ptr<ServerParams> cfg) :
        chunkid(chunk_id),
        storeid(store_id),
        srcIp(ip),
        srcPort(port),
        isRunning(false),
        state(MigrateReceiveState::RECEIVE_SNAPSHOT),
        receiver(chunk_id, store_id, svr, cfg){
    }
    uint32_t chunkid;
    uint32_t storeid;
    string srcIp;
    uint16_t srcPort;

    bool isRunning;
    SCLOCK::time_point nextSchedTime;
    SCLOCK::time_point lastSyncTime;
    MigrateReceiveState state;
    ChunkMigrateReceiver receiver;
    uint64_t sessionId;
};

class MigrateManager {
 public:
    explicit MigrateManager(std::shared_ptr<ServerEntry> svr,
          const std::shared_ptr<ServerParams> cfg);

    Status startup();
    Status stopChunk(uint32_t chunkid);
    void stop();

    // sender POV
    bool senderSchedule(const SCLOCK::time_point& now);
    Status migrating(uint32_t chunkid, string& ip, uint16_t port);
    void dstReadyMigrate(asio::ip::tcp::socket sock,
                         const std::string& chunkidArg,
                         const std::string& StoreidArg);

    // receiver POV
    bool receiverSchedule(const SCLOCK::time_point& now);
    Status importing(uint32_t chunkid, string& ip, uint16_t port);
    void fullReceive(uint32_t chunkid);
    void checkMigrateStatus(uint32_t chunkid);
    Status applyRepllog(Session* sess, uint32_t storeid, uint32_t chunkid,
                       const std::string& logKey, const std::string& logValue);
    Status supplyMigrateEnd(uint32_t chunkid);

 private:
    void controlRoutine();
    void sendChunk(uint32_t chunkid);
    void deleteChunk(uint32_t chunkid);

 private:
    const std::shared_ptr<ServerParams> _cfg;
    std::shared_ptr<ServerEntry> _svr;
    std::condition_variable _cv;
    std::atomic<bool> _isRunning;
    mutable std::mutex _mutex;
    std::unique_ptr<std::thread> _controller;

    // sender's pov
    std::map<uint32_t, std::unique_ptr<MigrateSendTask>> _migrateSendTask;
    std::unique_ptr<WorkerPool> _migrateSender;
    std::unique_ptr<WorkerPool> _migrateClear;
    std::shared_ptr<PoolMatrix> _migrateSenderMatrix;
    std::shared_ptr<PoolMatrix> _migrateClearMatrix;

    std::unique_ptr<RateLimiter> _rateLimiter;

    // receiver's pov
    std::map<uint32_t, std::unique_ptr<MigrateReceiveTask>> _migrateReceiveTask;
    std::unique_ptr<WorkerPool> _migrateReceiver;
    std::unique_ptr<WorkerPool> _migrateChecker;
    std::shared_ptr<PoolMatrix> _migrateReceiverMatrix;
    std::shared_ptr<PoolMatrix> _migrateCheckerMatrix;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_CLUSTER_MIGRATE_MANAGER_H_
