// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include "tendisplus/network/latency_record.h"

namespace tendisplus {

LockLatencyRecord::LockLatencyRecord()
  : _totalTimeAcquireLock(0),
    _countAcquireLock(0),
    _maxTimeAcquireLock(0),
    _idOfMaxTimeAcquireLock("") {}

void LockLatencyRecord::addRecord(uint64_t timeCost, std::string id) {
  _totalTimeAcquireLock += timeCost;
  _countAcquireLock++;
  if (_maxTimeAcquireLock < timeCost) {
    _maxTimeAcquireLock = timeCost;
    _idOfMaxTimeAcquireLock = std::move(id);
  }
}

std::string LockLatencyRecord::toString() const {
  char temp[256];
  auto r = snprintf(temp,
                    256,
                    " TotalTimeUsedAcquireLock(us):%lu AcquireLockCount:%lu "
                    "LongestTimeAcquireLock(us):%lu Associated id:%s",
                    _totalTimeAcquireLock,
                    _countAcquireLock,
                    _maxTimeAcquireLock,
                    _idOfMaxTimeAcquireLock.data());
  return std::string(temp, r);
}

void LockLatencyRecord::reset() {
  _totalTimeAcquireLock = 0;
  _countAcquireLock = 0;
  _maxTimeAcquireLock = 0;
  _idOfMaxTimeAcquireLock.clear();
}

RocksdbLatencyRecord::RocksdbLatencyRecord()
  : _totalTimeRocksdb(0),
    _maxTimeRocksdb(0),
    _totalSizeRocksdb(0),
    _countRocksdb(0),
    _succRocksdb(0),
    _failRocksdb(0) {}

void RocksdbLatencyRecord::addRecord(uint64_t timeCost,
                                     bool succ,
                                     uint64_t rwSize) {
  _totalTimeRocksdb += timeCost;
  if (_maxTimeRocksdb < timeCost) {
    _maxTimeRocksdb = timeCost;
  }
  _totalSizeRocksdb += rwSize;
  _countRocksdb++;
  if (succ) {
    _succRocksdb++;
  } else {
    _failRocksdb++;
  }
}

std::string RocksdbLatencyRecord::toString() const {
  char temp[256];
  auto r =
    snprintf(temp,
             256,
             " TotalTimeUsed(us):%lu LongestTime(us):%lu TotalSize(byte):%lu "
             "Count:%lu Succ:%lu Fail:%lu",
             _totalTimeRocksdb,
             _maxTimeRocksdb,
             _totalSizeRocksdb,
             _countRocksdb,
             _succRocksdb,
             _failRocksdb);
  return std::string(temp, r);
}

void RocksdbLatencyRecord::reset() {
  _totalTimeRocksdb = 0;
  _maxTimeRocksdb = 0;
  _totalSizeRocksdb = 0;
  _countRocksdb = 0;
  _succRocksdb = 0;
  _failRocksdb = 0;
}

}  // namespace tendisplus
