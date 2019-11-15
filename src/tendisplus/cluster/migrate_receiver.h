#ifndef SRC_TENDISPLUS_CLUSTER_MIGRATE_RECEIVER_H_
#define SRC_TENDISPLUS_CLUSTER_MIGRATE_RECEIVER_H_

#include "tendisplus/network/blocking_tcp_client.h"
#include "tendisplus/utils/rate_limiter.h"
#include "tendisplus/server/segment_manager.h"
#include "tendisplus/server/server_params.h"
#include "tendisplus/utils/status.h"

namespace tendisplus {
using namespace std;

class ChunkMigrateReceiver {
 public:
    explicit ChunkMigrateReceiver (uint32_t chunkid, uint32_t storeid,
        std::shared_ptr<ServerEntry> svr,
        std::shared_ptr<ServerParams> cfg);
    Status receiveSnapshot();

    void setDbWithLock(std::unique_ptr<DbWithLock> db) {
        _dbWithLock = std::move(db);
    }
    void setClient(std::shared_ptr<BlockingTcpClient> client) {
        _client = client;
    }

    Status applyBinlog( Session* sess, uint32_t storeid, uint32_t chunkid,
        const std::string& logKey, const std::string& logValue);

 private:
    Status supplySetKV(const string& key, const string& value);

 private:
    std::shared_ptr<ServerEntry> _svr;
    const std::shared_ptr<ServerParams> _cfg;
    std::unique_ptr<DbWithLock> _dbWithLock;
    std::shared_ptr<BlockingTcpClient> _client;
    uint32_t _chunkid;
    uint32_t _storeid;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_CLUSTER_MIGRATE_RECEIVER_H_
