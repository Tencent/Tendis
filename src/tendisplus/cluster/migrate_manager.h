#ifndef SRC_TENDISPLUS_CLUSTER_MIGRATE_MANAGER_H_
#define SRC_TENDISPLUS_CLUSTER_MIGRATE_MANAGER_H_

#include <list>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include "tendisplus/cluster/cluster_manager.h"
#include "tendisplus/cluster/migrate_receiver.h"
#include "tendisplus/cluster/migrate_sender.h"
#include "tendisplus/network/blocking_tcp_client.h"
#include "tendisplus/server/server_entry.h"
#include "tendisplus/storage/catalog.h"
#include "tendisplus/utils/rate_limiter.h"

namespace tendisplus {

class ServerEntry;
class StoreMeta;
class ChunkMeta;
class ChunkMigrateSender;
class ClusterState;
enum class BinlogApplyMode;
using myMutex = std::recursive_mutex;

// Sender POV
enum class MigrateSendState {
    NONE = 0,
    WAIT,
    START,
    SUCC,
    CLEAR,
    HALF,
    ERR
};

enum MigrateBinlogType {
    RECEIVE_START,
    RECEIVE_END,
    SEND_START,
    SEND_END
};

using SlotsBitmap = std::bitset<CLUSTER_SLOTS>;

Expected<uint64_t> addMigrateBinlog(MigrateBinlogType type, string slots, uint32_t storeid,
        ServerEntry* svr, const string& nodeName);

class MigrateSendTask {
 public:
    explicit MigrateSendTask(uint32_t storeId, const SlotsBitmap& slots_,
            std::shared_ptr<ServerEntry> svr,
            const std::shared_ptr<ServerParams> cfg, bool is_fake = false) :
            storeid(storeId),
            slots(slots_),
            taskid(0),
            isRunning(false),
            state(MigrateSendState::WAIT),
            isFake(is_fake) {
                sender = std::make_unique<ChunkMigrateSender>(slots_, svr, cfg, is_fake);
    }

    uint32_t storeid;
    SlotsBitmap slots;
    std::atomic<uint64_t> taskid;
    bool isRunning;
    SCLOCK::time_point nextSchedTime;
    MigrateSendState state;
    bool isFake;
    std::unique_ptr<ChunkMigrateSender> sender;

    void setTaskId(const std::string& taskid);
};


// receiver POV
enum class MigrateReceiveState {
    NONE = 0,
    RECEIVE_SNAPSHOT,
    RECEIVE_BINLOG,
    SUCC,
    ERR
};


class ChunkMigrateReceiver;

class MigrateReceiveTask{
 public:
    explicit MigrateReceiveTask(const SlotsBitmap& slots_,
        uint32_t store_id, const string& taskid_, const string& ip, uint16_t port,
        std::shared_ptr<ServerEntry> svr,
        const std::shared_ptr<ServerParams> cfg) :
        slots(slots_),
        taskid(taskid_),
        storeid(store_id),
        srcIp(ip),
        srcPort(port),
        isRunning(false),
        state(MigrateReceiveState::RECEIVE_SNAPSHOT) {
            receiver = std::make_unique<ChunkMigrateReceiver>(slots_, store_id, taskid_, svr, cfg);
    }
    SlotsBitmap slots;
    std::string taskid;
    uint32_t storeid;
    string srcIp;
    uint16_t srcPort;

    bool isRunning;
    SCLOCK::time_point nextSchedTime;
    uint64_t lastSyncTime;
    MigrateReceiveState state;
    std::unique_ptr<ChunkMigrateReceiver> receiver;

};


class MigrateManager {
 public:
    explicit MigrateManager(std::shared_ptr<ServerEntry> svr,
          const std::shared_ptr<ServerParams> cfg);

    Status startup();
    Status stopStoreTask(uint32_t storid);
    void stop();

    // sender POV
    bool senderSchedule(const SCLOCK::time_point& now);

    Status migrating(SlotsBitmap slots, const string& ip, uint16_t port, uint32_t storeid);

    void dstReadyMigrate(asio::ip::tcp::socket sock,
                         const std::string& chunkidArg,
                         const std::string& StoreidArg,
                         const std::string& nodeidArg,
                         const std::string& taskidArg);

    void dstPrepareMigrate(asio::ip::tcp::socket sock,
                       const std::string& chunkidArg,
                       const std::string& nodeidArg,
                       uint32_t storeNum);

    // receiver POV
    bool receiverSchedule(const SCLOCK::time_point& now);

    Status importing(SlotsBitmap slots, const string& ip, uint16_t port, uint32_t storeid, const std::string& taskid);

    Status startTask(const SlotsBitmap& taskmap, const std::string& ip,
                        uint16_t port, uint32_t storeid,
                        bool import);

    void insertNodes(const std::vector<uint32_t>& slots, const std::string& nodeid, bool import);

    void fullReceive(MigrateReceiveTask* task);

    void checkMigrateStatus(MigrateReceiveTask* task);

    Status applyRepllog(Session* sess, uint32_t storeid, BinlogApplyMode mode,
                       const std::string& logKey, const std::string& logValue);
    Status supplyMigrateEnd(const std::string& taskid, bool binlogDone = true);
    uint64_t getProtectBinlogid(uint32_t storeid);

    bool slotInTask(uint32_t slot);
    bool slotsInTask(const SlotsBitmap& bitMap);
    Expected<std::string> getTaskInfo();
    Expected<std::string> getMigrateInfo();

    Expected<std::string> getMigrateInfoStr(const SlotsBitmap& bitMap);
    Expected<std::string> getMigrateInfoStrSimple(const SlotsBitmap& bitMap);
    SlotsBitmap getSteadySlots(const SlotsBitmap& bitMap);
    Expected<uint64_t> applyMigrateBinlog(ServerEntry* svr, PStore store,
            MigrateBinlogType type, string slots, const string& nodeName);
    Status restoreMigrateBinlog(MigrateBinlogType type, uint32_t storeid, string slots);
    Status onRestoreEnd(uint32_t storeId);
    Status deleteChunks(uint32_t storeid, const SlotsBitmap& slots);
    Status deleteChunkRange(uint32_t storeid, uint32_t beginChunk, uint32_t endChunk);

    void requestRateLimit(uint64_t bytes);

    void migrateSenderResize(size_t size);
    void migrateClearResize(size_t size);
    void migrateReceiverResize(size_t size);
    void migrateCheckerResize(size_t size);

    size_t migrateSenderSize();
    size_t migrateClearSize();
    size_t migrateReceiverSize();
    size_t migrateCheckerSize();

 private:
    std::unordered_map<uint32_t, std::unique_ptr<ChunkLock>> _lockMap;
    void controlRoutine();
    void sendSlots(MigrateSendTask* task);
    void deleteChunks(MigrateSendTask* task);
    bool containSlot(const SlotsBitmap& slots1, const SlotsBitmap& slots2);
    bool checkSlotOK(const SlotsBitmap& bitMap, const std::string& nodeid,
            std::vector<uint32_t>& taskSlots);
    std::string genTaskid();
 private:
    const std::shared_ptr<ServerParams> _cfg;
    std::shared_ptr<ServerEntry> _svr;
    std::shared_ptr<ClusterState> _cluster;

    std::condition_variable _cv;
    std::atomic<bool> _isRunning;
    std::atomic<uint64_t> _taskIdGen;
    mutable myMutex _mutex;
    std::unique_ptr<std::thread> _controller;

    // TODO(wayenchen) takenliu add, change all std::bitset<CLUSTER_SLOTS> to SlotsBitmap
    std::bitset<CLUSTER_SLOTS> _migrateSlots;
    std::bitset<CLUSTER_SLOTS> _importSlots;

    std::bitset<CLUSTER_SLOTS> _succMigrateSlots;
    std::bitset<CLUSTER_SLOTS> _failMigrateSlots;

    std::bitset<CLUSTER_SLOTS> _succImportSlots;
    std::bitset<CLUSTER_SLOTS> _failImportSlots;

    std::vector<std::string> _succSenderTask;
    std::vector<std::string> _failSenderTask;

    std::vector<std::string> _succReceTask;
    std::vector<std::string> _failReceTask;

    std::map<uint32_t, std::list<SlotsBitmap>> _restoreMigrateTask;

    // sender's pov
    std::list<std::unique_ptr<MigrateSendTask>> _migrateSendTask;

    std::map<std::string, std::unique_ptr<MigrateSendTask>> _MigrateSendTaskMap;

    std::unique_ptr<WorkerPool> _migrateSender;
    std::unique_ptr<WorkerPool> _migrateClear;
    std::shared_ptr<PoolMatrix> _migrateSenderMatrix;
    std::shared_ptr<PoolMatrix> _migrateClearMatrix;

    // receiver's pov
    std::map<std::string, std::unique_ptr<MigrateReceiveTask>> _migrateReceiveTaskMap;


    std::unique_ptr<WorkerPool> _migrateReceiver;
    std::shared_ptr<PoolMatrix> _migrateReceiverMatrix;

    uint16_t _workload;
    // mark dst node or source node
    std::unordered_map<uint32_t, std::string> _migrateNodes;
    std::unordered_map<uint32_t, std::string> _importNodes;

    // sender rate limiter
    std::unique_ptr<RateLimiter> _rateLimiter;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_CLUSTER_MIGRATE_MANAGER_H_
