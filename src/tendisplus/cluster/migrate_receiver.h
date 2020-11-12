#ifndef SRC_TENDISPLUS_CLUSTER_MIGRATE_RECEIVER_H_
#define SRC_TENDISPLUS_CLUSTER_MIGRATE_RECEIVER_H_

#include <memory>
#include <string>
#include <utility>
#include "tendisplus/cluster/cluster_manager.h"
#include "tendisplus/cluster/migrate_manager.h"
#include "tendisplus/network/blocking_tcp_client.h"
#include "tendisplus/server/segment_manager.h"
#include "tendisplus/server/server_params.h"
#include "tendisplus/utils/rate_limiter.h"
#include "tendisplus/utils/status.h"

namespace tendisplus {

#define CLUSTER_SLOTS 16384

class ChunkMigrateReceiver {
 public:
  explicit ChunkMigrateReceiver(const std::bitset<CLUSTER_SLOTS>& slots,
                                uint32_t storeid,
                                std::string taskid,
                                std::shared_ptr<ServerEntry> svr,
                                std::shared_ptr<ServerParams> cfg);

  Status receiveSnapshot();

  void setDbWithLock(std::unique_ptr<DbWithLock> db) {
    _dbWithLock = std::move(db);
  }
  void setClient(std::shared_ptr<BlockingTcpClient> client) {
    _client = client;
  }

  void setTaskId(const std::string& taskid) {
    _taskid = taskid;
  }

  void freeDbLock() {
    _dbWithLock.reset();
  }

  uint32_t getsStoreid() const {
    return _storeid;
  }
  std::bitset<CLUSTER_SLOTS> getSlots() {
    return _slots;
  }
  std::string getTaskid() {
    return _taskid;
  }

  uint64_t getSnapshotNum() const {
    return _snapshotKeyNum.load(std::memory_order_relaxed);
  }

  void stop();
  void start();
  bool isRunning();

 private:
  Status supplySetKV(const string& key, const string& value);

  std::shared_ptr<ServerEntry> _svr;
  const std::shared_ptr<ServerParams> _cfg;
  std::atomic<bool> _isRunning;
  std::unique_ptr<DbWithLock> _dbWithLock;
  std::shared_ptr<BlockingTcpClient> _client;
  uint32_t _storeid;
  std::string _taskid;
  std::bitset<CLUSTER_SLOTS> _slots;

  std::atomic<uint64_t> _snapshotKeyNum;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_CLUSTER_MIGRATE_RECEIVER_H_
