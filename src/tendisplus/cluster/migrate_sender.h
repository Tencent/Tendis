#ifndef SRC_TENDISPLUS_CLUSTER_MIGRATE_SENDER_H_
#define SRC_TENDISPLUS_CLUSTER_MIGRATE_SENDER_H_

#include "tendisplus/server/server_entry.h"
#include "tendisplus/network/blocking_tcp_client.h"
#include "tendisplus/utils/rate_limiter.h"
#include "tendisplus/server/server_params.h"
#include "tendisplus/utils/status.h"

namespace tendisplus {

class ChunkMigrateSender{
 public:
    explicit ChunkMigrateSender(uint32_t chunkid,
        std::shared_ptr<ServerEntry> svr,
        std::shared_ptr<ServerParams> cfg);
    Status sendChunk();
    Expected<bool> deleteChunk(uint32_t maxDeletenum = 1000);

    void setStoreid(uint32_t storeid) {
        _storeid = storeid;
    }
    void setClient(std::shared_ptr<BlockingTcpClient> client) {
        _client = client;
    }
    void setDstStoreid(uint32_t dstStoreid) {
        _dstStoreid = dstStoreid;
    }
 private:
    Status sendSnapshot();
    Status sendBinlog();
    Status sendOver();
    std::string encodePrefixPk(uint32_t chunkid);

 private:
    std::shared_ptr<ServerEntry> _svr;
    const std::shared_ptr<ServerParams> _cfg;
    std::unique_ptr<DbWithLock> _dbWithLock;
    std::shared_ptr<BlockingTcpClient> _client;
    uint32_t _chunkid;
    uint32_t _storeid;
    uint64_t _curBinlogid;
    uint64_t _endBinlogid;
    string _dstIp;
    uint16_t _dstPort;
    uint32_t _dstStoreid;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_CLUSTER_MIGRATE_SENDER_H_
