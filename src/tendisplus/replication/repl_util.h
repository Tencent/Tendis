#ifndef SRC_TENDISPLUS_REPLICATION_REPL_UTIL_H_
#define SRC_TENDISPLUS_REPLICATION_REPL_UTIL_H_

#include "tendisplus/server/server_entry.h"
#include "tendisplus/network/blocking_tcp_client.h"

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
        return s; \
    } \

std::shared_ptr<BlockingTcpClient> createClient(const string& ip, uint16_t port,
    std::shared_ptr<ServerEntry> svr);

Expected<uint64_t> masterSendBinlogV2(BlockingTcpClient*,
    uint32_t storeId, uint32_t dstStoreId,
    uint64_t binlogPos, bool needHeartBeart,
    std::shared_ptr<ServerEntry> svr,
    const std::shared_ptr<ServerParams> cfg,
    uint32_t chunkid = Transaction::CHUNKID_UNINITED);

Expected<uint64_t> applySingleTxnV2(Session* sess, uint32_t storeId,
    const std::string& logKey, const std::string& logValue,
    uint32_t chunkid = Transaction::CHUNKID_UNINITED);

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_REPLICATION_REPL_UTIL_H_
