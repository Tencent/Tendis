#ifndef SRC_TENDISPLUS_CLUSTER_MIGRATE_SENDER_H_
#define SRC_TENDISPLUS_CLUSTER_MIGRATE_SENDER_H_

#include <bitset>
#include <list>
#include <memory>
#include <string>
#include "tendisplus/cluster/cluster_manager.h"
#include "tendisplus/cluster/migrate_manager.h"
#include "tendisplus/network/blocking_tcp_client.h"
#include "tendisplus/server/server_entry.h"
#include "tendisplus/server/server_params.h"
#include "tendisplus/utils/rate_limiter.h"
#include "tendisplus/utils/status.h"


namespace tendisplus {

#define CLUSTER_SLOTS 16384

class ClusterState;
class ClusterNode;

enum class MigrateSenderStatus {
    NONE = 0,
    SNAPSHOT_BEGIN,
    SNAPSHOT_DONE,
    BINLOG_DONE,
    DEL_DONE,
    METACHANGE_DONE
};


class ChunkMigrateSender{
 public:

    explicit ChunkMigrateSender(const std::bitset<CLUSTER_SLOTS>& slots,
        std::shared_ptr<ServerEntry> svr,
        std::shared_ptr<ServerParams> cfg,
        bool is_fake = false);

    Status sendChunk();

    const std::bitset<CLUSTER_SLOTS>& getSlots() {
        return _slots;
    }

    void setStoreid(uint32_t storeid) {
        _storeid = storeid;
    }
    void setClient(std::shared_ptr<BlockingTcpClient> client) {
        _client = client;
    }
    void setDstStoreid(uint32_t dstStoreid) {
        _dstStoreid = dstStoreid;
    }
    void setDstNode(const std::string nodeid);

    uint32_t  getStoreid() const { return  _storeid; }
    uint64_t  getSnapshotNum() const { return  _snapshotKeyNum; }
    uint64_t  getBinlogNum() const { return  _binlogNum; }
    uint64_t  getDelNum() const { return  _delNum; }
    bool getConsistentInfo() const { return _consistency; }
    MigrateSenderStatus getSenderState() { return _sendstate;}
    void setSenderStatus(MigrateSenderStatus s);

    Expected<uint64_t> deleteChunk(uint32_t chunkid);
    // TODO(wayenchen)  takenliu add, delete the slots param for all interface, use _slots
    bool deleteChunks(const std::bitset<CLUSTER_SLOTS>& slots);
    bool checkSlotsBlongDst(const std::bitset<CLUSTER_SLOTS>& slot);

    uint64_t getProtectBinlogid() {
        // TODO(wayenchen)  takenliu add, use atomic
        std::lock_guard<std::mutex> lk(_mutex);
        return _curBinlogid;
    }
    std::string getInfo();
    Status lockChunks();
    Status unlockChunks();

 private:
    Expected<std::unique_ptr<Transaction>> initTxn();
    Status sendBinlog(uint16_t time);
    Expected<uint64_t> sendRange(Transaction* txn, uint32_t begin, uint32_t end);
    Status sendSnapshot(const std::bitset<CLUSTER_SLOTS>& slots);

    bool pursueBinLog(uint16_t maxTime, uint64_t &startBinLog,
            uint64_t &binlogHigh, Transaction *txn);

    Expected<uint64_t> catchupBinlog(uint64_t start, uint64_t end,
            const std::bitset<CLUSTER_SLOTS>& slots);
    Status sendOver();


private:
    mutable std::mutex _mutex;

    std::bitset<CLUSTER_SLOTS> _slots;
    std::shared_ptr<ServerEntry> _svr;
    const std::shared_ptr<ServerParams> _cfg;
    bool _isFake;

    std::unique_ptr<DbWithLock> _dbWithLock;
    std::shared_ptr<BlockingTcpClient> _client;
    std::shared_ptr<ClusterState> _clusterState;
    MigrateSenderStatus _sendstate;
    uint32_t _storeid;
    uint64_t  _snapshotKeyNum;
    uint64_t  _binlogNum;
    uint64_t  _delNum;
    uint64_t  _delSlot;
    bool _consistency;
    std::string _nodeid;
    uint64_t _curBinlogid;
    string _dstIp;
    uint16_t _dstPort;
    uint32_t _dstStoreid;
    std::shared_ptr<ClusterNode>  _dstNode;
    uint64_t getMaxBinLog(Transaction * ptxn);
    std::list<std::unique_ptr<ChunkLock>> _slotsLockList;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_CLUSTER_MIGRATE_SENDER_H_
