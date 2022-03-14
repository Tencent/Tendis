// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include "glog/logging.h"
#include "tendisplus/cluster/migrate_receiver.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

ChunkMigrateReceiver::ChunkMigrateReceiver(
  const std::bitset<CLUSTER_SLOTS>& slots,
  uint32_t storeid,
  std::string taskid,
  std::shared_ptr<ServerEntry> svr,
  std::shared_ptr<ServerParams> cfg)
  : _svr(svr),
    _cfg(cfg),
    _isRunning(false),
    _storeid(storeid),
    _taskid(taskid),
    _slots(slots),
    _snapshotKeyNum(0),
    _snapshotStartTime(0),
    _snapshotEndTime(0),
    _binlogEndTime(0),
    _taskStartTime(0) {}

Status ChunkMigrateReceiver::receiveSingleBatch() {
  uint32_t timeoutSec = _cfg->migrateNetworkTimeout;
  uint32_t receiveNum = 0;
  SyncReadData(batchPosData, 4, timeoutSec);
  uint32_t batchPos =
    *reinterpret_cast<const uint32_t*>(batchPosData.value().c_str());

  SyncReadData(writeBatchData, batchPos, timeoutSec);
  if (!writeBatchData.ok()) {
    return {ErrorCodes::ERR_TIMEOUT, "receive writeBatch data fail"};
  }

  auto s = PutSingleBatch(writeBatchData.value(), &receiveNum);
  _snapshotKeyNum.fetch_add(receiveNum, std::memory_order_relaxed);
  TEST_SYNC_POINT_CALLBACK(
    "ChunkMigrateReceiver::receiveSingleBatch::receiveKeyNum", &receiveNum);
  RET_IF_ERR(s);

  return {ErrorCodes::ERR_OK, ""};
}

Status ChunkMigrateReceiver::receiveSnapshot() {
  if (!isRunning()) {
    LOG(ERROR) << "stop receiver task on taskid:" << _taskid;
    return {ErrorCodes::ERR_INTERNAL, "stop running"};
  }
  std::stringstream ss;
  const std::string nodename =
    _svr->getClusterMgr()->getClusterState()->getMyselfName();
  std::string bitmapStr = _slots.to_string();
  ss << "readymigrate " << bitmapStr << " " << _storeid << " " << nodename
     << " " << _taskid;
  Status s = _client->writeLine(ss.str());
  if (!s.ok()) {
    LOG(ERROR) << "readymigrate srcDb failed:" << s.toString();
    return s;
  }

  auto expRsp = _client->readLine(std::chrono::seconds(5));
  if (!expRsp.ok()) {
    LOG(ERROR) << "readymigrate req srcDb error:" << expRsp.status().toString();
    return expRsp.status();
  }
  std::string value = expRsp.value();
  if (expRsp.value() != "+OK") {
    LOG(WARNING) << "readymigrate req srcDb failed:" << expRsp.value()
                 << "the task should not retry";
    return {ErrorCodes::ERR_READY_MIGRATE, "readymigrate req srcDb failed"};
  }

  uint64_t startTime = msSinceEpoch();
  setSnapShotStartTime(startTime);
  setTaskStartTime(startTime);
  uint32_t timeoutSec = _cfg->migrateNetworkTimeout;
  while (true) {
    if (!isRunning()) {
      LOG(ERROR) << "stop receiver task on taskid:" << _taskid;
      return {ErrorCodes::ERR_INTERNAL, "stop running"};
    }

    SyncReadData(exptData, 1, timeoutSec);
    if (!exptData.ok()) {
      return {ErrorCodes::ERR_TIMEOUT, "receive data fail"};
    }
#ifdef TENDIS_DEBUG
    // TODO(takenliu): delete this log
    LOG(INFO) << "receive protocal type sucess:" << exptData.value()[0];
#endif
    // DEPRECATED previous single migrate
    if (exptData.value()[0] == '0') {
      SyncReadData(keylenData, 4, timeoutSec);
      uint32_t keylen =
        *reinterpret_cast<const uint32_t*>(keylenData.value().c_str());

      SyncReadData(keyData, keylen, timeoutSec);
      if (!keyData.ok()) {
        return {ErrorCodes::ERR_TIMEOUT, "receive key data fail"};
      }

      SyncReadData(valuelenData, 4, timeoutSec);

      uint32_t valuelen =
        *reinterpret_cast<const uint32_t*>(valuelenData.value().c_str());

      SyncReadData(valueData, valuelen, timeoutSec);
      if (!valueData.ok()) {
        return {ErrorCodes::ERR_TIMEOUT, "receive value data fail"};
      }

      auto s = supplySetKV(keyData.value(), valueData.value());
      if (!s.ok()) {
        LOG(ERROR) << "supply set key: " << keyData.value() << "fail";
        return s;
      }
      _snapshotKeyNum.fetch_add(1, std::memory_order_relaxed);
    } else if (exptData.value()[0] == '1') {
      SyncWriteData("+OK")
    } else if (exptData.value()[0] == '2') {
      SyncWriteData("+OK")
    } else if (exptData.value()[0] == '3') {
      SyncWriteData("+OK") break;
    } else if (exptData.value()[0] == '5') {
      auto s = receiveSingleBatch();
      RET_IF_ERR(s);
    }
  }
  LOG(INFO) << "migrate snapshot transfer done, readnum:" << getSnapshotNum()
            << "taskid:" << _taskid;

  setSnapShotEndTime(msSinceEpoch());
  return {ErrorCodes::ERR_OK, ""};
}

Status ChunkMigrateReceiver::supplySetKV(const string& key,
                                         const string& value) {
  Expected<RecordKey> expRk = RecordKey::decode(key);
  if (!expRk.ok()) {
    return expRk.status();
  }
  Expected<RecordValue> expRv = RecordValue::decode(value);
  if (!expRv.ok()) {
    return expRv.status();
  }

  uint32_t slotid = expRk.value().getChunkId();
  if (!_slots.test(slotid)) {
    LOG(ERROR) << "slotid:" << expRk.value().getPrimaryKey()
               << "is not a member in bitmap";
    return {ErrorCodes::ERR_INTERNAL, "slotid not match"};
  }

  PStore kvstore = _dbWithLock->store;
  auto eTxn = kvstore->createTransaction(nullptr);
  if (!eTxn.ok()) {
    LOG(ERROR) << "createTransaction failed:" << eTxn.status().toString();
    return eTxn.status();
  }
  std::unique_ptr<Transaction> txn = std::move(eTxn.value());

  Status s = kvstore->setKV(expRk.value(), expRv.value(), txn.get());

  if (!s.ok()) {
    LOG(ERROR) << "setKV failed:" << s.toString();
    return s;
  }
  // NOTE(takenliu) TTLIndex's chunkid is different from key's chunkid,
  // so need to recover TTLIndex.
  // only RT_*_META need recover, it's saved as RT_DATA_META in RecordKey
  // if RecordValue's type is RT_KV need ignore recovering.
  if (expRk.value().getRecordType() == RecordType::RT_DATA_META) {
    if (expRv.value().getTtl() > 0 &&
        expRv.value().getRecordType() != RecordType::RT_KV) {
      // add new index entry
      TTLIndex n_ictx(expRk.value().getPrimaryKey(),
                      expRv.value().getRecordType(),
                      expRk.value().getDbId(),
                      expRv.value().getTtl());
      s = txn->setKV(n_ictx.encode(),
                     RecordValue(RecordType::RT_TTL_INDEX).encode());
      if (!s.ok()) {
        return s;
      }
    }
  }

  auto commitStatus = txn->commit();
  if (!commitStatus.ok()) {
    return commitStatus.status();
  }

  return {ErrorCodes::ERR_OK, ""};
}

Status ChunkMigrateReceiver::PutSingleBatch(const string& migrateBatch,
                                            uint32_t* totalNum) {
  PStore kvstore = _dbWithLock->store;
  auto eTxn = kvstore->createTransaction(nullptr);
  if (!eTxn.ok()) {
    LOG(ERROR) << "createTransaction failed:" << eTxn.status().toString();
    return eTxn.status();
  }
  std::unique_ptr<Transaction> txn = std::move(eTxn.value());

  size_t len = migrateBatch.length();
  size_t batchPos = 0;
  size_t batchItem = 0;
  while (batchPos < len) {
    uint32_t keyLen = 0;
    uint32_t valueLen = 0;

    if (easyCopy(&keyLen, migrateBatch, &batchPos) != sizeof(uint32_t)) {
      INVARIANT_D(0);
      // bad protocol
      return {ErrorCodes::ERR_INTERNAL,
              "ChunkMigrateReceiver parse keylen bad protocol"};
    }

    std::string key(migrateBatch.begin() + batchPos,
                    migrateBatch.begin() + batchPos + keyLen);
    batchPos += keyLen;
    Expected<RecordKey> expRk = RecordKey::decode(key);
    RET_IF_ERR_EXPECTED(expRk);

    if (easyCopy(&valueLen, migrateBatch, &batchPos) != sizeof(uint32_t)) {
      INVARIANT_D(0);
      return {ErrorCodes::ERR_INTERNAL,
              "ChunkMigrateReceiver parse valueLen bad protocol"};
    }

    std::string value(migrateBatch.begin() + batchPos,
                      migrateBatch.begin() + batchPos + valueLen);
    batchPos += valueLen;
    Expected<RecordValue> expRv = RecordValue::decode(value);
    RET_IF_ERR_EXPECTED(expRv);

    uint32_t slotid = expRk.value().getChunkId();
    if (!_slots.test(slotid)) {
      LOG(ERROR) << "slotid:" << expRk.value().getPrimaryKey()
                 << "is not a member in bitmap";
      return {ErrorCodes::ERR_INTERNAL, "slotid not match"};
    }

    Status s = kvstore->setKV(expRk.value(), expRv.value(), txn.get());
    RET_IF_ERR(s);
    // NOTE(takenliu) TTLIndex's chunkid is different from key's chunkid,
    // so need to recover TTLIndex.
    // only RT_*_META need recover, it's saved as RT_DATA_META in RecordKey
    // if RecordValue's type is RT_KV need ignore recovering.
    if (expRk.value().getRecordType() == RecordType::RT_DATA_META) {
      if (expRv.value().getTtl() > 0 &&
          expRv.value().getRecordType() != RecordType::RT_KV) {
        // add new index entry
        TTLIndex n_ictx(expRk.value().getPrimaryKey(),
                        expRv.value().getRecordType(),
                        expRk.value().getDbId(),
                        expRv.value().getTtl());
        s = txn->setKV(n_ictx.encode(),
                       RecordValue(RecordType::RT_TTL_INDEX).encode());
        RET_IF_ERR(s);
      }
    }
    batchItem++;
  }
  DLOG(INFO) << "supplyKVBatch "
             << "size:" << migrateBatch.size() << "content end";
  auto commitStatus = txn->commit();
  if (!commitStatus.ok()) {
    return commitStatus.status();
  }

  *totalNum += batchItem;
  return {ErrorCodes::ERR_OK, ""};
}

void ChunkMigrateReceiver::stop() {
  _isRunning.store(false, std::memory_order_relaxed);
}

void ChunkMigrateReceiver::start() {
  _isRunning.store(true, std::memory_order_relaxed);
}

bool ChunkMigrateReceiver::isRunning() {
  return _isRunning.load(std::memory_order_relaxed);
}


void ChunkMigrateReceiver::setTaskStartTime(uint64_t t) {
  _taskStartTime.store(t, std::memory_order_relaxed);
}

void ChunkMigrateReceiver::setBinlogEndTime(uint64_t t) {
  _binlogEndTime.store(t, std::memory_order_relaxed);
}

void ChunkMigrateReceiver::setSnapShotStartTime(uint64_t t) {
  _snapshotStartTime.store(t, std::memory_order_relaxed);
}

void ChunkMigrateReceiver::setSnapShotEndTime(uint64_t t) {
  _snapshotEndTime.store(t, std::memory_order_relaxed);
}

}  // namespace tendisplus
