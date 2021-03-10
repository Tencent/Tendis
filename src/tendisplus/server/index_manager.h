// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

// Copyright [2017] <eliotwang, deyukong>
#ifndef SRC_TENDISPLUS_SERVER_INDEX_MANAGER_H_
#define SRC_TENDISPLUS_SERVER_INDEX_MANAGER_H_

#include <unordered_map>
#include <list>
#include <string>
#include <memory>
#include <vector>
#include "tendisplus/server/server_entry.h"
#include "tendisplus/network/worker_pool.h"

namespace tendisplus {

using JobStatus = std::unordered_map<std::size_t, std::atomic<bool>>;
using JobCnt = std::unordered_map<std::size_t, std::atomic<uint32_t>>;

class IndexManager {
 public:
  IndexManager(std::shared_ptr<ServerEntry> svr,
               const std::shared_ptr<ServerParams>& cfg);
  Status startup();
  void stop();
  Status run();
  Status scanExpiredKeysJob(uint32_t storeId);
  int tryDelExpiredKeysJob(uint32_t storeId);
  bool isRunning();
  Status stopStore(uint32_t storeId);
  void indexScannerResize(size_t size);
  void keyDeleterResize(size_t size);
  size_t indexScannerSize();
  size_t keyDeleterSize();
  size_t scanExpiredCount() const {
    return _totalEnqueue;
  }
  size_t delExpiredCount() const {
    return _totalDequeue;
  }
  std::string getInfoString();

 private:
  std::unique_ptr<WorkerPool> _indexScanner;
  std::unique_ptr<WorkerPool> _keyDeleter;
  std::unordered_map<std::size_t, std::list<TTLIndex>> _expiredKeys;
  std::unordered_map<std::size_t, std::string> _scanPoints;
  std::vector<uint64_t> _scanPonitsTtl;
  JobStatus _scanJobStatus;
  JobStatus _delJobStatus;
  // when destroystore, _disableStatus[storeId] = true
  JobStatus _disableStatus;
  JobCnt _scanJobCnt;
  JobCnt _delJobCnt;

  std::atomic<bool> _isRunning;
  std::shared_ptr<ServerEntry> _svr;
  std::shared_ptr<ServerParams> _cfg;
  std::thread _runner;
  std::mutex _mutex;

  std::shared_ptr<PoolMatrix> _scannerMatrix;
  std::shared_ptr<PoolMatrix> _deleterMatrix;

  uint64_t _totalDequeue;
  uint64_t _totalEnqueue;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_SERVER_INDEX_MANAGER_H_
