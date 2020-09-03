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
using myMutex = std::recursive_mutex;

enum class MigrateSenderStatus {
    NONE = 0,
    SNAPSHOT_BEGIN,
    SNAPSHOT_DONE,
    BINLOG_DONE,
    LASTBINLOG_DONE,
    SENDOVER_DONE,
    METACHANGE_DONE,
    META_DONE,
    DEL_DONE,
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

    void setDstAddr(const std::string& ip, uint32_t port) {
        _dstIp = ip;
        _dstPort = port;
    }

    void freeDbLock() {
        _dbWithLock.reset();
    }

    void setTaskId(const std::string& taskid) {
        _taskid = taskid;
    }

    void setDstStoreid(uint32_t dstStoreid) {
        _dstStoreid = dstStoreid;
    }
    void setDstNode(const std::string nodeid);

    uint32_t  getStoreid() const { return  _storeid; }
    uint64_t  getSnapshotNum() const { return  _snapshotKeyNum; }
    uint64_t  getBinlogNum() const { return  _binlogNum; }
    bool getConsistentInfo() const { return _consistency; }
    MigrateSenderStatus getSenderState() { return _sendstate;}
    void setSenderStatus(MigrateSenderStatus s);

    bool checkSlotsBlongDst();

    uint64_t getProtectBinlogid() {
        // TODO(wayenchen)  takenliu add, use atomic
        std::lock_guard<myMutex> lk(_mutex);
        return _curBinlogid;
    }
    Status lockChunks();
    void unlockChunks();
    bool needToWaitMetaChanged() const;
    Status sendVersionMeta();
    bool needToSendFail() const;

 private:
    Expected<std::unique_ptr<Transaction>> initTxn();
    Status sendBinlog();
    Expected<uint64_t> sendRange(Transaction* txn, uint32_t begin, uint32_t end);
    Status sendSnapshot();

    Status sendLastBinlog();
    Status catchupBinlog(uint64_t end);

    Status resetClient();
    Status sendOver();

private:
    mutable myMutex _mutex;

    std::bitset<CLUSTER_SLOTS> _slots;
    std::shared_ptr<ServerEntry> _svr;
    const std::shared_ptr<ServerParams> _cfg;
    bool _isFake;

    std::unique_ptr<DbWithLock> _dbWithLock;
    std::shared_ptr<BlockingTcpClient> _client;
    std::string _taskid;
    std::shared_ptr<ClusterState> _clusterState;
    MigrateSenderStatus _sendstate;
    uint32_t _storeid;
    uint64_t  _snapshotKeyNum;
    uint64_t  _binlogNum;
    bool _consistency;
    std::string _nodeid;
    uint64_t _curBinlogid;
    string _dstIp;
    uint16_t _dstPort;
    uint32_t _dstStoreid;
    std::shared_ptr<ClusterNode>  _dstNode;
    uint64_t getMaxBinLog(Transaction * ptxn) const;
    std::list<std::unique_ptr<ChunkLock>> _slotsLockList;
    std::string _OKSTR = "+OK";
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_CLUSTER_MIGRATE_SENDER_H_
