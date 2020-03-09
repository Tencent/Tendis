#ifndef SRC_TENDISPLUS_CLUSTER_MIGRATE_SENDER_H_
#define SRC_TENDISPLUS_CLUSTER_MIGRATE_SENDER_H_

#include "tendisplus/server/server_entry.h"
#include "tendisplus/network/blocking_tcp_client.h"
#include "tendisplus/utils/rate_limiter.h"
#include "tendisplus/server/server_params.h"
#include "tendisplus/utils/status.h"
#include "tendisplus/cluster/migrate_manager.h"
#include <bitset>
#include "tendisplus/cluster/cluster_manager.h"

namespace tendisplus {

#define CLUSTER_SLOTS 16384

class ClusterState;
class ClusterNode;

class ChunkMigrateSender{
 public:

    explicit ChunkMigrateSender(const std::bitset<CLUSTER_SLOTS>& slots,
        std::shared_ptr<ServerEntry> svr,
        std::shared_ptr<ServerParams> cfg);

    Status sendChunk();

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

    uint32_t  getStoreid() { return  _storeid; }

    const std::string getNodeid() { return  _nodeid ; }

    Expected<bool> deleteChunk(uint32_t chunkid);
    bool deleteChunks(const std::bitset<CLUSTER_SLOTS>& slots);

    bool checkSlotsBlongDst(const std::bitset<CLUSTER_SLOTS>& slot);

    std::bitset<CLUSTER_SLOTS> _slots;

 private:
    Expected<Transaction*> initTxn();
    Status sendBinlog(uint16_t time);
    Expected<uint64_t> sendEndBinLog(uint64_t start , uint64_t end);
    Status sendRange(Transaction* txn, uint32_t begin, uint32_t end);
    Status sendSnapshot(uint32_t begin, uint32_t end);
    Status sendSnapshot(const std::bitset<CLUSTER_SLOTS>& slots);

    bool pursueBinLog(uint16_t maxTime , uint64_t  &startBinLog ,
                                                          uint64_t &binlogHigh, Transaction *txn);
    Status catchupBinlog(uint64_t start, uint64_t end, const std::bitset<CLUSTER_SLOTS>& slots);

    Status sendOver();

 private:
    uint32_t  _start;
    uint32_t  _end;
    std::shared_ptr<ServerEntry> _svr;
    const std::shared_ptr<ServerParams> _cfg;
    std::unique_ptr<DbWithLock> _dbWithLock;
    std::shared_ptr<BlockingTcpClient> _client;
    std::shared_ptr<ClusterState> _clusterState;
    uint32_t _storeid;

    std::string _nodeid;
    uint64_t _curBinlogid;
    uint64_t _endBinlogid;
    string _dstIp;
    uint16_t _dstPort;
    uint32_t _dstStoreid;
    std::shared_ptr<ClusterNode>  _dstNode;
    uint64_t getMaxBinLog(Transaction * ptxn);
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_CLUSTER_MIGRATE_SENDER_H_
