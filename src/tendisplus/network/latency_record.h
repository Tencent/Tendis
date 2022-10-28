// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_NETWORK_LATENCY_RECORD_H_
#define SRC_TENDISPLUS_NETWORK_LATENCY_RECORD_H_

#include <array>
#include <cstdint>
#include <string>
#include <utility>

namespace tendisplus {

enum LockLatencyType : uint8_t {
  LLT_STORE,
  LLT_CHUNK,
  LLT_KEY,

  MAX_LLT,
};
const std::array<std::string, LockLatencyType::MAX_LLT> LLTToString{
  "StoreLock:", "ChunkLock:", "KeyLock:"};

enum RocksdbLatencyType : uint8_t {
  RLT_PUT,
  RLT_GET,
  RLT_DELETE,
  RLT_COMMIT,

  MAX_RLT,
};
const std::array<std::string, RocksdbLatencyType::MAX_RLT> RLTToString{
  "RocksdbPut:", "RocksdbGet:", "RocksdbDelete:", "RocksdbCommit:"};

struct LockLatencyRecord {
  uint64_t _totalTimeAcquireLock;  // us
  uint64_t _countAcquireLock;
  uint64_t _maxTimeAcquireLock;  // us
  std::string _idOfMaxTimeAcquireLock;

  LockLatencyRecord();
  LockLatencyRecord(const LockLatencyRecord&) = delete;
  LockLatencyRecord& operator=(const LockLatencyRecord&) = delete;
  void addRecord(uint64_t timeCost, std::string id);
  std::string toString() const;
  void reset();
};

struct RocksdbLatencyRecord {
  uint64_t _totalTimeRocksdb;  // us
  uint64_t _maxTimeRocksdb;    // us
  uint64_t _totalSizeRocksdb;  // byte
  uint64_t _countRocksdb;
  uint64_t _succRocksdb;
  uint64_t _failRocksdb;

  RocksdbLatencyRecord();
  RocksdbLatencyRecord(const RocksdbLatencyRecord&) = delete;
  RocksdbLatencyRecord& operator=(const RocksdbLatencyRecord&) = delete;
  void addRecord(uint64_t timeCost, bool succ, uint64_t rwSize);
  std::string toString() const;
  void reset();
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_NETWORK_LATENCY_RECORD_H_
