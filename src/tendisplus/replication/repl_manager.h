#ifndef SRC_TENDISPLUS_REPLICATION_REPL_MANAGER_H_
#define SRC_TENDISPLUS_REPLICATION_REPL_MANAGER_H_

#include <memory>
#include <vector>
#include <utility>
#include <list>
#include <map>
#include <string>
#include <iostream>
#include <fstream>

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "tendisplus/server/server_entry.h"
#include "tendisplus/storage/catalog.h"
#include "tendisplus/network/blocking_tcp_client.h"
#include "tendisplus/utils/rate_limiter.h"

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
    SCLOCK::time_point lastSendBinlogTime;
    std::shared_ptr<BlockingTcpClient> client;
    uint64_t clientId;
};

struct RecycleBinlogStatus {
    bool isRunning;
    SCLOCK::time_point nextSchedTime;
    uint64_t firstBinlogId;
    uint64_t lastFlushBinlogId;
    uint32_t fileSeq;
	// the timestamp of last binlog in prev file or the first binlog in cur file
    uint64_t timestamp;
    SCLOCK::time_point fileCreateTime;
    uint64_t fileSize;
    std::unique_ptr<std::ofstream> fs;
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
    explicit ReplManager(std::shared_ptr<ServerEntry> svr, 
          const std::shared_ptr<ServerParams> cfg);
    Status startup();
    Status stopStore(uint32_t storeId);
    void stop();
    void togglePauseState(bool isPaused) { _incrPaused = isPaused; }
    Status changeReplSource(uint32_t storeId, std::string ip, uint32_t port,
            uint32_t sourceStoreId);
    void supplyFullSync(asio::ip::tcp::socket sock,
            const std::string& storeIdArg);
    void registerIncrSync(asio::ip::tcp::socket sock,
            const std::string& storeIdArg,
            const std::string& dstStoreIdArg,
            const std::string& binlogPosArg);
#ifdef BINLOG_V1
    Status applyBinlogs(uint32_t storeId, uint64_t sessionId,
            const std::map<uint64_t, std::list<ReplLog>>& binlogs);
    Status applySingleTxn(uint32_t storeId, uint64_t txnId,
        const std::list<ReplLog>& ops);
#else
    Status applyRepllogV2(Session* sess, uint32_t storeId,
            const std::string& logKey, const std::string& logValue);
    Expected<uint64_t> applySingleTxnV2(Session* sess, uint32_t storeId,
        const std::string& logKey, const std::string& logValue);
#endif
    void flushCurBinlogFs(uint32_t storeId);
    void appendJSONStat(rapidjson::Writer<rapidjson::StringBuffer>&) const;
    static constexpr size_t INCR_POOL_SIZE = 12;
    static constexpr size_t MAX_FULL_PARAL = 4;
    void onFlush(uint32_t storeId, uint64_t binlogid);

 protected:
    void controlRoutine();
    void supplyFullSyncRoutine(std::shared_ptr<BlockingTcpClient> client,
            uint32_t storeId);
    bool isFullSupplierFull() const;

    std::shared_ptr<BlockingTcpClient> createClient(const StoreMeta&);
    void slaveStartFullsync(const StoreMeta&);
    void slaveChkSyncStatus(const StoreMeta&);

#ifdef BINLOG_V1
    // binlogPos: the greatest id that has been applied
    Expected<uint64_t> masterSendBinlog(BlockingTcpClient*,
            uint32_t storeId, uint32_t dstStoreId, uint64_t binlogPos);
#else
    Expected<uint64_t> masterSendBinlogV2(BlockingTcpClient*,
        uint32_t storeId, uint32_t dstStoreId,
        uint64_t binlogPos, bool needHeartBeart);
    std::ofstream* getCurBinlogFs(uint32_t storeid);
    void updateCurBinlogFs(uint32_t storeId, uint64_t written,
        uint64_t ts, bool flushFile = false);
#endif

    void masterPushRoutine(uint32_t storeId, uint64_t clientId);
    void slaveSyncRoutine(uint32_t  storeId);

    // truncate binlog in [start, end]
    void recycleBinlog(uint32_t storeId, uint64_t start, uint64_t end, bool saveLogs);

 private:
    void changeReplState(const StoreMeta& storeMeta, bool persist);
    void changeReplStateInLock(const StoreMeta&, bool persist);

    Expected<uint32_t> maxDumpFileSeq(uint32_t storeId);
#ifdef BINLOG_V1
    Status saveBinlogs(uint32_t storeId, const std::list<ReplLog>& logs);
#endif

    mutable std::mutex _mutex;
    std::condition_variable _cv;
    std::atomic<bool> _isRunning;
    std::shared_ptr<ServerEntry> _svr;

    // slave's pov, meta data.
    std::vector<std::unique_ptr<StoreMeta>> _syncMeta;

    // slave's pov, sync status
    std::vector<std::unique_ptr<SPovStatus>> _syncStatus;

    // master's pov, living slave clients
    std::vector<std::map<uint64_t, std::unique_ptr<MPovStatus>>> _pushStatus;

    // master and slave's pov, smallest binlogId, moves on when truncated
    std::vector<std::unique_ptr<RecycleBinlogStatus>> _logRecycStatus;
    // TODO(takenliu):optimize the _mutex and _logRecycleMutex logic. it's not gracefull now.
    std::vector<std::unique_ptr<std::mutex>> _logRecycleMutex;

    // master's pov, workerpool of pushing full backup
    std::unique_ptr<WorkerPool> _fullPusher;

    // master's pov fullsync rate limiter
    std::unique_ptr<RateLimiter> _rateLimiter;

    // master's pov, workerpool of pushing incr backup
    std::unique_ptr<WorkerPool> _incrPusher;

    // master's pov, as its name
    bool _incrPaused;

    // slave's pov, workerpool of receiving full backup
    std::unique_ptr<WorkerPool> _fullReceiver;

    // slave's pov, periodly check incr-sync status
    std::unique_ptr<WorkerPool> _incrChecker;

    // master and slave's pov, log recycler
    std::unique_ptr<WorkerPool> _logRecycler;

    std::atomic<uint64_t> _clientIdGen;

    const std::string _dumpPath;

    std::unique_ptr<std::thread> _controller;

    std::shared_ptr<PoolMatrix> _fullPushMatrix;
    std::shared_ptr<PoolMatrix> _incrPushMatrix;
    std::shared_ptr<PoolMatrix> _fullReceiveMatrix;
    std::shared_ptr<PoolMatrix> _incrCheckMatrix;
    std::shared_ptr<PoolMatrix> _logRecycleMatrix;

    // TODO(takenliu): configable
    static constexpr size_t FILEBATCH = size_t(6ULL*1024*1024);
    static constexpr size_t BINLOGSIZE = 1024 * 1024 * 64;
    static constexpr size_t BINLOGSYNCSECS = 20 * 60;
    static constexpr size_t BINLOGHEARTBEATSECS = 60;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_REPLICATION_REPL_MANAGER_H_
