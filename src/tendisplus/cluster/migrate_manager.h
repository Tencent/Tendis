#ifndef SRC_TENDISPLUS_CLUSTER_MIGRATE_MANAGER_H_
#define SRC_TENDISPLUS_CLUSTER_MIGRATE_MANAGER_H_

#include "tendisplus/server/server_entry.h"
#include "tendisplus/network/blocking_tcp_client.h"
#include "tendisplus/utils/rate_limiter.h"
#include "tendisplus/cluster/migrate_sender.h"
#include "tendisplus/cluster/migrate_receiver.h"
#include "tendisplus/storage/catalog.h"
#include <list>
#include "tendisplus/cluster/cluster_manager.h"


namespace tendisplus {

class ServerEntry;
class StoreMeta;
class ChunkMeta;
class ChunkMigrateSender;
enum class BinlogApplyMode;


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
    explicit MigrateSendTask(uint32_t storeId, const SlotsBitmap& slots_ , std::shared_ptr<ServerEntry> svr,
                             const std::shared_ptr<ServerParams> cfg) :
            storeid(storeId),
            slots(slots_),
            isRunning(false),
            state(MigrateSendState::WAIT){
                sender = std::make_unique<ChunkMigrateSender>(slots_, svr, cfg);
    }
    void setClient(std::shared_ptr<BlockingTcpClient> client);

    uint32_t storeid;
    SlotsBitmap slots;
    bool isRunning;
    SCLOCK::time_point nextSchedTime;
    MigrateSendState state;
    std::unique_ptr<ChunkMigrateSender> sender;
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
    explicit MigrateReceiveTask(const SlotsBitmap& slots_ , uint32_t store_id, string& ip, uint16_t port,
        std::shared_ptr<ServerEntry> svr,
        const std::shared_ptr<ServerParams> cfg) :
        slots(slots_),
        storeid(store_id),
        srcIp(ip),
        srcPort(port),
        isRunning(false),
        state(MigrateReceiveState::RECEIVE_SNAPSHOT){
            receiver = std::make_unique<ChunkMigrateReceiver>(slots_, store_id, svr, cfg);
    }
    SlotsBitmap slots;
    uint32_t storeid;
    string srcIp;
    uint16_t srcPort;

    bool isRunning;
    SCLOCK::time_point nextSchedTime;
    SCLOCK::time_point lastSyncTime;
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

    Status migrating(SlotsBitmap slots, string& ip, uint16_t port, uint32_t storeid );

    void dstReadyMigrate(asio::ip::tcp::socket sock,
                         const std::string& chunkidArg,
                         const std::string& StoreidArg,
                         const std::string& nodeidArg);

    void prepareSender(asio::ip::tcp::socket sock,
                       const std::string& chunkidArg,
                       const std::string& nodeidArg,
                       uint32_t storeNum);

    // receiver POV
    bool receiverSchedule(const SCLOCK::time_point& now);

    Status importing(SlotsBitmap slots, string& ip, uint16_t port, uint32_t storeid );

    Status startTask(const std::vector<uint32_t> slotsVec, std::string& ip,
                    uint16_t port, uint32_t storeid,
                    bool import, uint16_t taskSize);

    void insertNodes(std::vector<uint32_t >slots, std::string nodeid, bool import);

    void fullReceive(MigrateReceiveTask* task);

    void checkMigrateStatus(MigrateReceiveTask* task);

    Status applyRepllog(Session* sess, uint32_t storeid, BinlogApplyMode mode,
                       const std::string& logKey, const std::string& logValue);
    Status supplyMigrateEnd(const SlotsBitmap& slots);
    Status lockXChunk(uint32_t chunkid);
    Status unlockXChunk(uint32_t chunkid);

    Status lockChunks(const std::bitset<CLUSTER_SLOTS> &slots);
    Status unlockChunks(const std::bitset<CLUSTER_SLOTS> &slots);
    uint64_t getProtectBinlogid(uint32_t storeid);

    bool slotInTask(uint32_t slot);
    Expected<std::string> getTaskInfo();
    Expected<std::string> getMigrateInfo();

    Expected<std::string> getMigrateInfoStr(const SlotsBitmap& bitMap);
    SlotsBitmap getSteadySlots(const SlotsBitmap& bitMap);
    Expected<uint64_t> applyMigrateBinlog(ServerEntry* svr, PStore store, MigrateBinlogType type, string slots, string& nodeName);
    Status restoreMigrateBinlog(MigrateBinlogType type, uint32_t storeid, string slots);
    Status onRestoreEnd(uint32_t storeId);
    //Status deleteChunksWithLock(const SlotsBitmap& slots);
    Status asyncDeleteChunks(uint32_t storeid, const SlotsBitmap& slots);
    Status asyncDeleteChunksInLock(uint32_t storeid, const SlotsBitmap& slots);
    //Expected<uint64_t> deleteChunkInLock(uint32_t  chunkid);

 private:
    std::unordered_map<uint32_t, std::unique_ptr<ChunkLock>> _lockMap;
    void controlRoutine();
    void sendSlots(MigrateSendTask* task);
    void deleteChunks(MigrateSendTask* task);
    bool containSlot(const SlotsBitmap& slots1, const SlotsBitmap& slots2);
    bool checkSlotOK(const SlotsBitmap& bitMap, const std::string& nodeid, std::vector<uint32_t>& taskSlots);


 private:
    const std::shared_ptr<ServerParams> _cfg;
    std::shared_ptr<ServerEntry> _svr;
    std::shared_ptr<ClusterState> _cluster;

    std::condition_variable _cv;
    std::atomic<bool> _isRunning;
    mutable std::mutex _mutex;
    std::unique_ptr<std::thread> _controller;
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

    std::unique_ptr<WorkerPool> _migrateSender;
    std::unique_ptr<WorkerPool> _migrateClear;
    std::shared_ptr<PoolMatrix> _migrateSenderMatrix;
    std::shared_ptr<PoolMatrix> _migrateClearMatrix;


    // receiver's pov
    std::list<std::unique_ptr<MigrateReceiveTask>> _migrateReceiveTask;

    std::unique_ptr<WorkerPool> _migrateReceiver;
    std::unique_ptr<WorkerPool> _migrateChecker;
    std::shared_ptr<PoolMatrix> _migrateReceiverMatrix;
    std::shared_ptr<PoolMatrix> _migrateCheckerMatrix;

    uint16_t _workload;
    //mark dst node or source node
    std::unordered_map<uint32_t, std::string> _migrateNodes;
    std::unordered_map<uint32_t, std::string> _importNodes;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_CLUSTER_MIGRATE_MANAGER_H_
