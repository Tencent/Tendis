// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_CLUSTER_MIGRATE_BATCH_H_
#define SRC_TENDISPLUS_CLUSTER_MIGRATE_BATCH_H_

#include <list>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include "tendisplus/cluster/cluster_manager.h"
#include "tendisplus/cluster/migrate_receiver.h"
#include "tendisplus/cluster/migrate_sender.h"
#include "tendisplus/network/blocking_tcp_client.h"
#include "tendisplus/server/server_entry.h"
#include "tendisplus/storage/catalog.h"
#include "tendisplus/utils/rate_limiter.h"
#include "tendisplus/utils/string.h"

namespace tendisplus {
class MigrateBatch {
 public:
  explicit MigrateBatch(uint32_t maxBytes,
                        std::shared_ptr<BlockingTcpClient> client,
                        std::shared_ptr<ServerEntry> svr)
    : _counter(0),
      _sendCounter(0),
      _maxBytes(maxBytes),
      _addBytes(0),
      _sentBytes(0),
      _client(client),
      _svr(svr) {
    _buffer.reserve(maxBytes);
  }

  ~MigrateBatch();

  bool isFull() const;
  bool isEmpty() const;
  Status add(const std::string& key, const std::string& value);
  Status send();
  // The total number of bytes sent to dest
  uint32_t sendBytes() const;
  uint32_t sendKVEntries() const;

 private:
  uint32_t _counter;      // Number of entries added
  uint32_t _sendCounter;  // Number of batch send to dest

  uint32_t _maxBytes;   // Max bytes of bach
  size_t _addBytes;     // Number of bytes add
  uint32_t _sentBytes;  // Number of bytes sent to dest

  std::vector<byte> _buffer;

  std::shared_ptr<BlockingTcpClient> _client;
  std::shared_ptr<ServerEntry> _svr;
};
}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_CLUSTER_MIGRATE_BATCH_H_
