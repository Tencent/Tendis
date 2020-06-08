#ifndef SRC_TENDISPLUS_REPLICATION_REPL_UTIL_H_
#define SRC_TENDISPLUS_REPLICATION_REPL_UTIL_H_

#include "tendisplus/server/server_entry.h"
#include "tendisplus/network/blocking_tcp_client.h"
#include "tendisplus/cluster/cluster_manager.h"

namespace tendisplus {
#define SyncReadData(data, len, timeout) \
    Expected<std::string> (data) = \
        _client->read((len), std::chrono::seconds((timeout))); \
    if (!data.ok()) { \
        LOG(ERROR) << "SyncReadData failed:" \
                    << data.status().toString(); \
        return { ErrorCodes::ERR_INTERNAL, "read failed" }; \
    }

#define SyncWriteData(data) \
    if (!_client->writeData((data)).ok()) { \
        LOG(ERROR) << "write data failed:" << s.toString(); \
        return { ErrorCodes::ERR_INTERNAL, "write failed" }; \
    } \

std::shared_ptr<BlockingTcpClient> createClient(const string& ip, uint16_t port,
    std::shared_ptr<ServerEntry> svr);

std::shared_ptr<BlockingTcpClient> createClient(const string& ip, uint16_t port,
    ServerEntry* svr);



Expected<uint64_t> masterSendBinlogV2(BlockingTcpClient*,
    uint32_t storeId, uint32_t dstStoreId,
    uint64_t binlogPos, bool needHeartBeart,
    std::shared_ptr<ServerEntry> svr,
    const std::shared_ptr<ServerParams> cfg);


Expected<uint64_t> applySingleTxnV2(Session* sess, uint32_t storeId,
    const std::string& logKey, const std::string& logValue,
    BinlogApplyMode mode);



Status sendWriter(BinlogWriter* writer,
                    BlockingTcpClient*,
                    uint32_t dstStoreId, bool needHeartBeat,
                    uint32_t secs);


Expected<uint64_t> SendSlotsBinlog(BlockingTcpClient*,
                       uint32_t storeId, uint32_t dstStoreId,
                       uint64_t binlogPos, uint64_t  binglogEnd,
                       bool needHeartBeart,
                       const std::bitset<CLUSTER_SLOTS>& slots,
                       std::shared_ptr<ServerEntry> svr,
                       const std::shared_ptr<ServerParams> cfg);

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_REPLICATION_REPL_UTIL_H_
