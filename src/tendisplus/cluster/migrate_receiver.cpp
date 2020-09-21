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
    _storeid(storeid),
    _taskid(taskid),
    _slots(slots),
    _snapshotKeyNum(0),
    _binlogNum(0) {}

Status ChunkMigrateReceiver::receiveSnapshot() {
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
    LOG(WARNING) << "readymigrate req srcDb failed:" << expRsp.value();
    return {ErrorCodes::ERR_INTERNAL, "readymigrate req srcDb failed"};
  }

  DLOG(INFO) << "receiveSnapshot, get response of readymigrate ok";

  uint32_t timeoutSec = 200;
  uint32_t readNum = 0;
  while (true) {
    SyncReadData(exptData, 1, timeoutSec);
    if (exptData.value()[0] == '0') {
      SyncReadData(keylenData, 4, timeoutSec) uint32_t keylen =
        *reinterpret_cast<const uint32_t*>(keylenData.value().c_str());
      SyncReadData(keyData, keylen, timeoutSec)

        SyncReadData(valuelenData, 4, timeoutSec) uint32_t valuelen =
          *reinterpret_cast<const uint32_t*>(valuelenData.value().c_str());
      SyncReadData(valueData, valuelen, timeoutSec)

        supplySetKV(keyData.value(), valueData.value());
      readNum++;
    } else if (exptData.value()[0] == '1') {
      SyncWriteData("+OK")
    } else if (exptData.value()[0] == '2') {
      SyncWriteData("+OK")
    } else if (exptData.value()[0] == '3') {
      SyncWriteData("+OK") break;
    }
  }
  LOG(INFO) << "migrate snapshot transfer done, readnum:" << readNum;
  _snapshotKeyNum = readNum;
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
  // NOTE(takenliu) TTLIndex's chunkid is diffrent from key's chunkid,
  // so need to recover TTLIndex.
  // only RT_*_META need recover, it's saved as RT_DATA_META in RecordKey
  // if RecordValue's type is RT_KV need ignore recovering.
  if (expRk.value().getRecordType() == RecordType::RT_DATA_META) {
    if (!Command::noExpire() && expRv.value().getTtl() > 0 &&
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

}  // namespace tendisplus
