// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_REPLICATION_REPL_UTIL_H_
#define SRC_TENDISPLUS_REPLICATION_REPL_UTIL_H_

#include <memory>
#include <string>
#include "tendisplus/cluster/cluster_manager.h"
#include "tendisplus/network/blocking_tcp_client.h"
#include "tendisplus/server/server_entry.h"

namespace tendisplus {

#define CLUSTER_SLOTS 16384

#define SyncReadData(data, len, timeout)                              \
  Expected<std::string>(data) =                                       \
    _client->read((len), std::chrono::seconds((timeout)));            \
  if (!data.ok()) {                                                   \
    LOG(ERROR) << "SyncReadData failed:" << data.status().toString(); \
    return {ErrorCodes::ERR_INTERNAL, "read failed"};                 \
  }

#define SyncWriteData(data)                             \
  if (!_client->writeData((data)).ok()) {               \
    LOG(ERROR) << "write data failed:" << s.toString(); \
    return {ErrorCodes::ERR_INTERNAL, "write failed"};  \
  }

std::shared_ptr<BlockingTcpClient> createClient(
  const string& ip, uint16_t port, std::shared_ptr<ServerEntry> svr);

std::shared_ptr<BlockingTcpClient> createClient(const string& ip,
                                                uint16_t port,
                                                ServerEntry* svr);


struct BinlogResult {
  uint64_t binlogId = 0;
  uint64_t binlogTs = 0;
};

Expected<BinlogResult> masterSendBinlogV2(
  BlockingTcpClient*,
  uint32_t storeId,
  uint32_t dstStoreId,
  uint64_t binlogPos,
  bool needHeartBeart,
  std::shared_ptr<ServerEntry> svr,
  const std::shared_ptr<ServerParams> cfg);

Expected<BinlogResult> masterSendAof(BlockingTcpClient*,
                                     uint32_t storeId,
                                     uint32_t dstStoreId,
                                     uint64_t binlogPos,
                                     bool needHeartBeart,
                                     std::shared_ptr<ServerEntry> svr,
                                     const std::shared_ptr<ServerParams> cfg);

Expected<BinlogResult> applySingleTxnV2(Session* sess,
                                        uint32_t storeId,
                                        const std::string& logKey,
                                        const std::string& logValue,
                                        BinlogApplyMode mode);

Status sendWriter(BinlogWriter* writer,
                  BlockingTcpClient*,
                  uint32_t dstStoreId,
                  const std::string& taskid,
                  bool needHeartBeat,
                  bool* needRetry,
                  uint32_t secs);


Status SendSlotsBinlog(BlockingTcpClient*,
                       uint32_t storeId,
                       uint32_t dstStoreId,
                       uint64_t binlogStart,
                       uint64_t binglogEnd,
                       bool needHeartBeart,
                       const std::bitset<CLUSTER_SLOTS>& slots,
                       const std::string& taskid,
                       std::shared_ptr<ServerEntry> svr,
                       uint64_t* sendBinlogNum,
                       uint64_t* newBinlogId,
                       bool* needRetry,
                       uint64_t* binlogTimeStamp);

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_REPLICATION_REPL_UTIL_H_
