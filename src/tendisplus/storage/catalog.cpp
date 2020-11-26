// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <sstream>
#include <utility>
#include <memory>
#include <string>
#include <vector>
#include "glog/logging.h"
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/error/en.h"
#include "tendisplus/storage/catalog.h"
#include "tendisplus/utils/invariant.h"

namespace tendisplus {

ConnectState int2ConnectState(const uint8_t t) {
  if (t == 1) {
    return ConnectState::DISCONNECTED;
  } else {
    return ConnectState::CONNECTED;
  }
}

StoreMeta::StoreMeta() : StoreMeta(0, "", 0, -1, 0, ReplState::REPL_NONE) {}

StoreMeta::StoreMeta(uint32_t id_,
                     const std::string& syncFromHost_,
                     uint16_t syncFromPort_,
                     int32_t syncFromId_,
                     uint64_t binlogId_,
                     ReplState replState_)
  : id(id_),
    syncFromHost(syncFromHost_),
    syncFromPort(syncFromPort_),
    syncFromId(syncFromId_),
    binlogId(binlogId_),
    replState(replState_) {}

std::unique_ptr<StoreMeta> StoreMeta::copy() const {
  return std::move(std::unique_ptr<StoreMeta>(new StoreMeta(*this)));
}


ChunkMeta::ChunkMeta()
  : ChunkMeta(0, "", 0, -1, 0, MigrateReceiveState::NONE) {}

ChunkMeta::ChunkMeta(uint32_t id_,
                     const std::string& syncFromHost_,
                     uint16_t syncFromPort_,
                     int32_t syncFromId_,
                     uint64_t binlogId_,
                     MigrateReceiveState migrateState_)
  : id(id_),
    syncFromHost(syncFromHost_),
    syncFromPort(syncFromPort_),
    syncFromId(syncFromId_),
    binlogId(binlogId_),
    migrateState(migrateState_) {}

std::unique_ptr<ChunkMeta> ChunkMeta::copy() const {
  return std::move(std::unique_ptr<ChunkMeta>(new ChunkMeta(*this)));
}


std::unique_ptr<StoreMainMeta> StoreMainMeta::copy() const {
  return std::move(std::unique_ptr<StoreMainMeta>(new StoreMainMeta(*this)));
}

std::unique_ptr<MainMeta> MainMeta::copy() const {
  return std::move(std::unique_ptr<MainMeta>(new MainMeta(*this)));
}

// server first start in cluster mode
// c1780cb48b3398452e3fd8b162b60246213e3379 127.0.0.1 0 myself,master - 0 0 0
// connected
ClusterMeta::ClusterMeta()
  : ClusterMeta(getUUid(20),
                "",
                0,
                0,
                CLUSTER_NODE_MYSELF | CLUSTER_NODE_MASTER,
                "-",
                0,
                0,
                0,
                {}) {
  // get clustermeta , if not exit ,create uuid
}

ClusterMeta::ClusterMeta(const std::string& name)
  : nodeName(name),
    ip(""),
    port(0),
    cport(0),
    nodeFlag(CLUSTER_NODE_MYSELF | CLUSTER_NODE_MASTER),
    masterName("-"),
    pingTime(0),
    pongTime(0),
    configEpoch(0) {}

/*
ClusterMeta::ClusterMeta()
    :ClusterMeta("57731f1f4f95c376b22f59bb3728a413216573c01e3329d7a2a4357e0e5baaf81a89e476073fe2a6","",0,"myself,master","-",0,0,0,ConnectState::CONNECTED,{}){
    //get clustermeta , if not exit ,create uuid
}
*/
ClusterMeta::ClusterMeta(const std::string& nodeName_,
                         const std::string& ip_,
                         uint64_t port_,
                         uint64_t cport_,
                         uint16_t nodeFlag_,
                         const std::string& masterName_,
                         uint64_t pingTime_,
                         uint64_t pongTime_,
                         uint64_t configEpoch_,
                         const std::vector<uint16_t>& slots_)
  : nodeName(nodeName_),
    ip(ip_),
    port(port_),
    cport(cport_),
    nodeFlag(nodeFlag_),
    masterName(masterName_),
    pingTime(pingTime_),
    pongTime(pongTime_),
    configEpoch(configEpoch_),
    slots(slots_) {}


std::unique_ptr<ClusterMeta> ClusterMeta::copy() const {
  return std::move(std::unique_ptr<ClusterMeta>(new ClusterMeta(*this)));
}

// get prefix with "store_cluster_"
std::string& ClusterMeta::getClusterPrefix() {
  static std::string realPrefix = []() {
    RecordKey rk(
      0, 0, RecordType::RT_META, std::string(ClusterMeta::CLUSTER_PREFIX), "");
    // CHUNKID(4) + TYPE(1) + DBID(4) + ClusterMeta::CLUSTER_PREFIX
    auto prefixPk = rk.prefixPk();
    std::string prefix =
      prefixPk.substr(0, 9 + strlen(ClusterMeta::CLUSTER_PREFIX));
    return prefix;
  }();
  return realPrefix;
}


Catalog::Catalog(std::unique_ptr<KVStore> store,
                 uint32_t kvStoreCount,
                 uint32_t chunkSize,
                 bool binlogUsingDefaultCF)
  : _store(std::move(store)),
    _kvStoreCount(kvStoreCount),
    _chunkSize(chunkSize) {
  auto mainMeta = getMainMeta();
  if (mainMeta.ok()) {
    if (_kvStoreCount != mainMeta.value()->kvStoreCount ||
        _chunkSize != mainMeta.value()->chunkSize) {
      LOG(FATAL) << "kvStoreCount(" << _kvStoreCount << ","
                 << mainMeta.value()->kvStoreCount << ") or chunkSize("
                 << _chunkSize << "," << mainMeta.value()->chunkSize
                 << ") not equal";
      INVARIANT(0);
    }
  } else if (mainMeta.status().code() == ErrorCodes::ERR_NOTFOUND) {
    auto binlogVersion = binlogUsingDefaultCF ? BinlogVersion::BINLOG_VERSION_1
                                              : BinlogVersion::BINLOG_VERSION_2;
    auto pMeta = std::unique_ptr<MainMeta>(
      new MainMeta(kvStoreCount, chunkSize, binlogVersion));
    Status s = setMainMeta(*pMeta);
    if (!s.ok()) {
      LOG(FATAL) << "catalog setMainMeta error:" << s.toString();
      INVARIANT(0);
    }
  } else {
    LOG(FATAL) << "catalog getMainMeta error:" << mainMeta.status().toString();
    INVARIANT(0);
  }
}

Status Catalog::setStoreMeta(const StoreMeta& meta) {
  std::stringstream ss;
  ss << "store_" << meta.id;
  RecordKey rk(0, 0, RecordType::RT_META, ss.str(), "");
  rapidjson::StringBuffer sb;
  rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
  writer.StartObject();

  writer.Key("Version");
  writer.String("1");

  writer.Key("syncFromHost");
  writer.String(meta.syncFromHost);

  writer.Key("syncFromPort");
  writer.Uint64(meta.syncFromPort);

  writer.Key("syncFromId");
  writer.Uint64(meta.syncFromId);

  writer.Key("binlogId");
  writer.Uint64(meta.binlogId);

  writer.Key("replState");
  writer.Uint64(static_cast<uint8_t>(meta.replState));

  writer.Key("id");
  writer.Uint64(meta.id);

  writer.EndObject();

  RecordValue rv(sb.GetString(), RecordType::RT_META, -1);

  auto exptxn = _store->createTransaction(nullptr);
  if (!exptxn.ok()) {
    return exptxn.status();
  }

  Transaction* txn = exptxn.value().get();

  Record rd(std::move(rk), std::move(rv));
  Status s = _store->setKV(rd, txn);
  if (!s.ok()) {
    return s;
  }
  return txn->commit().status();
}

Expected<std::unique_ptr<StoreMeta>> Catalog::getStoreMeta(uint32_t idx) {
  auto result = std::make_unique<StoreMeta>();
  std::stringstream ss;
  ss << "store_" << idx;
  RecordKey rk(0, 0, RecordType::RT_META, ss.str(), "");

  auto exptxn = _store->createTransaction(nullptr);
  if (!exptxn.ok()) {
    return exptxn.status();
  }

  Transaction* txn = exptxn.value().get();
  Expected<RecordValue> exprv = _store->getKV(rk, txn);
  if (!exprv.ok()) {
    return exprv.status();
  }
  RecordValue rv = exprv.value();
  const std::string& json = rv.getValue();

  rapidjson::Document doc;
  doc.Parse(json);
  if (doc.HasParseError()) {
    LOG(FATAL) << "parse meta failed"
               << rapidjson::GetParseError_En(doc.GetParseError());
  }

  INVARIANT(doc.IsObject());

  INVARIANT(doc.HasMember("syncFromHost"));
  INVARIANT(doc["syncFromHost"].IsString());
  result->syncFromHost = doc["syncFromHost"].GetString();

  INVARIANT(doc.HasMember("syncFromPort"));
  INVARIANT(doc["syncFromPort"].IsUint64());
  result->syncFromPort = static_cast<uint16_t>(doc["syncFromPort"].GetUint64());

  INVARIANT(doc.HasMember("id"));
  INVARIANT(doc["id"].IsUint64());
  result->id = (uint32_t)doc["id"].GetUint64();

  INVARIANT(doc.HasMember("syncFromId"));
  INVARIANT(doc["syncFromId"].IsUint64());
  result->syncFromId = static_cast<uint32_t>(doc["syncFromId"].GetUint64());

  INVARIANT(doc.HasMember("binlogId"));
  INVARIANT(doc["binlogId"].IsUint64());
  result->binlogId = static_cast<uint64_t>(doc["binlogId"].GetUint64());

  INVARIANT(doc.HasMember("replState"));
  INVARIANT(doc["replState"].IsUint64());
  result->replState = static_cast<ReplState>(doc["replState"].GetUint64());

  return result;
}

Status Catalog::stop() {
  return _store->stop();
}

Status Catalog::setStoreMainMeta(const StoreMainMeta& meta) {
  std::stringstream ss;
  ss << "store_main_" << meta.id;
  RecordKey rk(0, 0, RecordType::RT_META, ss.str(), "");
  rapidjson::StringBuffer sb;
  rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
  writer.StartObject();

  writer.Key("Version");
  writer.String("1");

  writer.Key("storeMode");
  writer.Uint64(static_cast<uint8_t>(meta.storeMode));

  writer.Key("id");
  writer.Uint64(meta.id);

  writer.EndObject();

  RecordValue rv(sb.GetString(), RecordType::RT_META, -1);

  auto exptxn = _store->createTransaction(nullptr);
  if (!exptxn.ok()) {
    return exptxn.status();
  }

  Transaction* txn = exptxn.value().get();

  Record rd(std::move(rk), std::move(rv));
  Status s = _store->setKV(rd, txn);
  if (!s.ok()) {
    return s;
  }
  return txn->commit().status();
}

Expected<std::unique_ptr<StoreMainMeta>> Catalog::getStoreMainMeta(
  uint32_t idx) {
  auto result = std::make_unique<StoreMainMeta>();
  std::stringstream ss;
  ss << "store_main_" << idx;
  RecordKey rk(0, 0, RecordType::RT_META, ss.str(), "");

  auto exptxn = _store->createTransaction(nullptr);
  if (!exptxn.ok()) {
    return exptxn.status();
  }

  Transaction* txn = exptxn.value().get();
  Expected<RecordValue> exprv = _store->getKV(rk, txn);
  if (!exprv.ok()) {
    return exprv.status();
  }
  RecordValue rv = exprv.value();
  const std::string& json = rv.getValue();

  rapidjson::Document doc;
  doc.Parse(json);
  if (doc.HasParseError()) {
    LOG(FATAL) << "parse meta failed"
               << rapidjson::GetParseError_En(doc.GetParseError());
  }

  INVARIANT(doc.IsObject());

  INVARIANT(doc.HasMember("id"));
  INVARIANT(doc["id"].IsUint64());
  result->id = (uint32_t)doc["id"].GetUint64();

  INVARIANT(doc.HasMember("storeMode"));
  INVARIANT(doc["storeMode"].IsUint64());
  result->storeMode =
    static_cast<KVStore::StoreMode>(doc["storeMode"].GetUint64());

  return result;
}

Status Catalog::setMainMeta(const MainMeta& meta) {
  std::stringstream ss;
  ss << "main_meta";
  RecordKey rk(0, 0, RecordType::RT_META, ss.str(), "");
  rapidjson::StringBuffer sb;
  rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
  writer.StartObject();

  writer.Key("Version");
  writer.String("1");

  writer.Key("kvStoreCount");
  writer.Uint64(static_cast<uint32_t>(meta.kvStoreCount));

  writer.Key("chunkSize");
  writer.Uint64(meta.chunkSize);

  writer.Key("binlogVersion");
  writer.Uint64((uint64_t)meta.binlogVersion);

  _binlogVersion = meta.binlogVersion;

  writer.EndObject();

  RecordValue rv(sb.GetString(), RecordType::RT_META, -1);

  auto exptxn = _store->createTransaction(nullptr);
  if (!exptxn.ok()) {
    LOG(ERROR) << "Catalog::setMainMeta failed:" << exptxn.status().toString()
               << " " << sb.GetString();
    return exptxn.status();
  }

  Transaction* txn = exptxn.value().get();

  Record rd(std::move(rk), std::move(rv));
  Status s = _store->setKV(rd, txn);
  if (!s.ok()) {
    LOG(ERROR) << "Catalog::setMainMeta failed:" << s.toString() << " "
               << sb.GetString();
    return s;
  }
  LOG(INFO) << "Catalog::setMainMeta sucess:" << sb.GetString();
  return txn->commit().status();
}

Expected<std::unique_ptr<MainMeta>> Catalog::getMainMeta() {
  auto result = std::make_unique<MainMeta>();
  std::stringstream ss;
  ss << "main_meta";
  RecordKey rk(0, 0, RecordType::RT_META, ss.str(), "");

  auto exptxn = _store->createTransaction(nullptr);
  if (!exptxn.ok()) {
    return exptxn.status();
  }

  Transaction* txn = exptxn.value().get();
  Expected<RecordValue> exprv = _store->getKV(rk, txn);
  if (!exprv.ok()) {
    return exprv.status();
  }
  RecordValue rv = exprv.value();
  const std::string& json = rv.getValue();

  LOG(INFO) << "Catalog::getMainMeta succ," << json;

  rapidjson::Document doc;
  doc.Parse(json);
  if (doc.HasParseError()) {
    LOG(FATAL) << "parse meta failed"
               << rapidjson::GetParseError_En(doc.GetParseError());
  }

  INVARIANT(doc.IsObject());

  INVARIANT(doc.HasMember("kvStoreCount"));
  INVARIANT(doc["kvStoreCount"].IsUint64());
  result->kvStoreCount = (uint32_t)doc["kvStoreCount"].GetUint64();

  INVARIANT(doc.HasMember("chunkSize"));
  INVARIANT(doc["chunkSize"].IsUint64());
  result->chunkSize = (uint32_t)doc["chunkSize"].GetUint64();

  // old version may not have binlog version.
  // if one version have binlog version, it's bigger than 1.
  if (doc.HasMember("binlogVersion")) {
    INVARIANT(doc["binlogVersion"].IsUint64());
    result->binlogVersion = (BinlogVersion)doc["binlogVersion"].GetUint64();
    _binlogVersion = result->binlogVersion;
  } else {
    result->binlogVersion = BinlogVersion::BINLOG_VERSION_1;
    _binlogVersion = result->binlogVersion;
  }

  return result;
}

Status Catalog::setClusterMeta(const ClusterMeta& meta) {
  std::stringstream ss;
  ss << ClusterMeta::CLUSTER_PREFIX << meta.nodeName;

  RecordKey rk(0, 0, RecordType::RT_META, ss.str(), "");

  rapidjson::StringBuffer sb;
  rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
  writer.StartObject();
  writer.Key("Version");
  writer.String("1");

  writer.Key("nodeName");
  writer.String(meta.nodeName);

  writer.Key("ip");
  writer.String(meta.ip);

  writer.Key("port");
  writer.Uint64(meta.port);

  writer.Key("cport");
  writer.Uint64(meta.cport);

  writer.Key("nodeFlag");
  writer.Uint64(meta.nodeFlag);

  writer.Key("masterName");
  writer.String(meta.masterName);

  writer.Key("pingTime");
  writer.Uint64(meta.pingTime);

  writer.Key("pongTime");
  writer.Uint64(meta.pongTime);

  writer.Key("configEpoch");
  writer.Uint64(meta.configEpoch);

  // TODO(wayenchen): how to store the slots?
  writer.Key("slots");
  writer.StartArray();
  for (auto& v : meta.slots)
    writer.Uint64(v);
  writer.EndArray();

  writer.EndObject();
  RecordValue rv(sb.GetString(), RecordType::RT_META, -1);

  auto exptxn = _store->createTransaction(nullptr);
  if (!exptxn.ok()) {
    LOG(ERROR) << "Catalog::ClusterMeta failed:" << exptxn.status().toString()
               << " " << sb.GetString();
    return exptxn.status();
  }

  Transaction* txn = exptxn.value().get();

  Record rd(std::move(rk), std::move(rv));
  Status s = _store->setKV(rd, txn);
  if (!s.ok()) {
    LOG(ERROR) << "Catalog::ClusterMeta set failed:" << s.toString() << " "
               << sb.GetString();
    return s;
  }
  return txn->commit().status();
}

// get all cluster data , use cursor
Expected<std::vector<std::unique_ptr<ClusterMeta>>>
Catalog::getAllClusterMeta() {
  std::vector<std::unique_ptr<ClusterMeta>> resultList;
  const std::string prefix = ClusterMeta::getClusterPrefix();
  auto exptxn = _store->createTransaction(nullptr);

  if (!exptxn.ok()) {
    return exptxn.status();
  }

  auto keyprefix = std::string(ClusterMeta::CLUSTER_PREFIX);
  Transaction* txn = exptxn.value().get();
  auto bcursor = txn->createDataCursor();
  bcursor->seek(prefix);
  while (true) {
    auto result = std::make_unique<ClusterMeta>();

    auto v = bcursor->next();
    if (!v.ok()) {
      if (v.status().code() == ErrorCodes::ERR_EXHAUST) {
        return resultList;
      }
      LOG(ERROR) << "get ClusterMeta error: " << v.status().toString();
      return v.status();
    }
    const auto& nodeRecord = v.value();
    const auto key = nodeRecord.getRecordKey();
    auto primaryKey = key.getPrimaryKey();

    // judge if prefix begin with "store_cluster"
    if (primaryKey.compare(0, keyprefix.length(), keyprefix) != 0) {
      break;
    }

    const auto& rv = nodeRecord.getRecordValue();
    const std::string& json = rv.getValue();
    rapidjson::Document doc;
    doc.Parse(json);
    if (doc.HasParseError()) {
      LOG(FATAL) << "parse meta failed"
                 << rapidjson::GetParseError_En(doc.GetParseError());
    }

    INVARIANT(doc.IsObject());

    INVARIANT(doc.HasMember("nodeName"));
    INVARIANT(doc["nodeName"].IsString());
    result->nodeName = doc["nodeName"].GetString();

    INVARIANT(doc.HasMember("ip"));
    INVARIANT(doc["ip"].IsString());
    result->ip = doc["ip"].GetString();

    INVARIANT(doc.HasMember("port"));
    INVARIANT(doc["port"].IsUint64());
    result->port = static_cast<uint64_t>(doc["port"].GetUint64());

    INVARIANT(doc.HasMember("cport"));
    INVARIANT(doc["cport"].IsUint64());
    result->cport = static_cast<uint64_t>(doc["cport"].GetUint64());

    INVARIANT(doc.HasMember("nodeFlag"));
    INVARIANT(doc["nodeFlag"].IsUint64());
    result->nodeFlag = static_cast<uint16_t>(doc["nodeFlag"].GetUint64());

    INVARIANT(doc.HasMember("masterName"));
    INVARIANT(doc["masterName"].IsString());
    result->masterName = doc["masterName"].GetString();

    INVARIANT(doc.HasMember("pingTime"));
    INVARIANT(doc["pingTime"].IsUint64());
    result->pingTime = static_cast<uint64_t>(doc["pingTime"].GetUint64());

    INVARIANT(doc.HasMember("pongTime"));
    INVARIANT(doc["pongTime"].IsUint64());
    result->pongTime = static_cast<uint64_t>(doc["pongTime"].GetUint64());

    INVARIANT(doc.HasMember("configEpoch"));
    INVARIANT(doc["configEpoch"].IsUint64());
    result->configEpoch = static_cast<uint64_t>(doc["configEpoch"].GetUint64());

    INVARIANT(doc.HasMember("slots"));
    INVARIANT(doc["slots"].IsArray());

    rapidjson::Value& slotArray = doc["slots"];
    result->slots.clear();
    for (rapidjson::SizeType i = 0; i < slotArray.Size(); i++) {
      const rapidjson::Value& object = slotArray[i];
      auto element = static_cast<uint16_t>(object.GetUint64());
      result->slots.push_back(element);
    }

    LOG(INFO) << "Get ClusterMeta Node name is" << result->nodeName
              << "ip address is " << result->ip << "node Flag is"
              << result->nodeFlag;

    resultList.emplace_back(std::move(result));
  }

  return resultList;
}


bool Catalog::clusterMetaExist(Transaction* txn, const std::string& nodeName) {
  auto result = std::make_unique<ClusterMeta>();
  std::stringstream ss;

  ss << "store_cluster_" << nodeName;
  RecordKey rk(0, 0, RecordType::RT_META, ss.str(), "");

  Expected<RecordValue> exprv = _store->getKV(rk, txn);
  if (!exprv.ok()) {
    return false;
  }
  return true;
}

// delete cluster node data
Status Catalog::delClusterMeta(const std::string& nodeName) {
  std::stringstream ss;
  ss << "store_cluster_" << nodeName;
  RecordKey rk(0, 0, RecordType::RT_META, ss.str(), "");

  auto exptxn = _store->createTransaction(nullptr);
  if (!exptxn.ok()) {
    LOG(ERROR) << "ERROR Get createTransaction status"
               << exptxn.status().toString();
    return exptxn.status();
  }
  std::unique_ptr<Transaction> txn = std::move(exptxn.value());
#ifdef TENDIS_DEBUG
  bool existKey = clusterMetaExist(txn.get(), nodeName);
  if (!existKey) {
    INVARIANT_D(0);
    LOG(ERROR) << "delteMeta data:" << nodeName << "not found";
  }
#endif

  Status s = _store->delKV(rk, txn.get());
  if (!s.ok()) {
    return s;
  }

  Expected<uint64_t> commitStatus = txn->commit();
  if (!s.ok()) {
    return commitStatus.status();
  }
  return s;
}

// get one cluster node data
Expected<std::unique_ptr<ClusterMeta>> Catalog::getClusterMeta(
  const std::string& nodeName) {
  auto result = std::make_unique<ClusterMeta>();
  std::stringstream ss;

  ss << "store_cluster_" << nodeName;
  RecordKey rk(0, 0, RecordType::RT_META, ss.str(), "");

  auto exptxn = _store->createTransaction(nullptr);
  if (!exptxn.ok()) {
    LOG(ERROR) << "ERROR Get createTransaction status"
               << exptxn.status().toString();
    return exptxn.status();
  }

  Transaction* txn = exptxn.value().get();
  Expected<RecordValue> exprv = _store->getKV(rk, txn);
  if (!exprv.ok()) {
    return exprv.status();
    LOG(INFO) << "ERROR Get getKV status" << exprv.status().toString();
  }

  RecordValue rv = exprv.value();
  const std::string& json = rv.getValue();
  rapidjson::Document doc;
  doc.Parse(json);
  if (doc.HasParseError()) {
    LOG(FATAL) << "parse meta failed"
               << rapidjson::GetParseError_En(doc.GetParseError());
  }
  INVARIANT(doc.IsObject());

  INVARIANT(doc.HasMember("nodeName"));
  INVARIANT(doc["nodeName"].IsString());

  INVARIANT(doc.HasMember("ip"));
  INVARIANT(doc["ip"].IsString());
  result->ip = doc["ip"].GetString();

  INVARIANT(doc.HasMember("port"));
  INVARIANT(doc["port"].IsUint64());
  result->port = static_cast<uint64_t>(doc["port"].GetUint64());

  INVARIANT(doc.HasMember("cport"));
  INVARIANT(doc["cport"].IsUint64());
  result->cport = static_cast<uint64_t>(doc["cport"].GetUint64());

  INVARIANT(doc.HasMember("nodeFlag"));
  INVARIANT(doc["nodeFlag"].IsUint64());
  result->nodeFlag = static_cast<uint16_t>(doc["port"].GetUint64());

  INVARIANT(doc.HasMember("masterName"));
  INVARIANT(doc["masterName"].IsString());
  result->masterName = doc["masterName"].GetString();

  INVARIANT(doc.HasMember("pingTime"));
  INVARIANT(doc["pingTime"].IsUint64());
  result->pingTime = static_cast<uint64_t>(doc["pingTime"].GetUint64());

  INVARIANT(doc.HasMember("pongTime"));
  INVARIANT(doc["pongTime"].IsUint64());
  result->pongTime = static_cast<uint64_t>(doc["pongTime"].GetUint64());

  INVARIANT(doc.HasMember("configEpoch"));
  INVARIANT(doc["configEpoch"].IsUint64());
  result->configEpoch = static_cast<uint64_t>(doc["configEpoch"].GetUint64());

  INVARIANT(doc.HasMember("slots"));
  INVARIANT(doc["slots"].IsArray());

  rapidjson::Value& slotArray = doc["slots"];
  result->slots.clear();
  for (rapidjson::SizeType i = 0; i < slotArray.Size(); i++) {
    const rapidjson::Value& object = slotArray[i];
    auto element = static_cast<uint16_t>(object.GetUint64());
    result->slots.push_back(element);
  }

  return result;
}

Status Catalog::setEpochMeta(const EpochMeta& meta) {
  std::stringstream ss;
  ss << "epoch_meta";
  RecordKey rk(0, 0, RecordType::RT_META, ss.str(), "");
  rapidjson::StringBuffer sb;
  rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
  writer.StartObject();

  writer.Key("Version");
  writer.String("1");

  writer.Key("currentEpoch");
  writer.Uint64(meta.currentEpoch);

  writer.Key("lastVoteEpoch");
  writer.Uint64(meta.lastVoteEpoch);

  writer.EndObject();

  RecordValue rv(sb.GetString(), RecordType::RT_META, -1);

  auto exptxn = _store->createTransaction(nullptr);
  if (!exptxn.ok()) {
    LOG(ERROR) << "Catalog::setEpochMeta failed:" << exptxn.status().toString()
               << " " << sb.GetString();
    return exptxn.status();
  }

  Transaction* txn = exptxn.value().get();

  Record rd(std::move(rk), std::move(rv));
  Status s = _store->setKV(rd, txn);
  if (!s.ok()) {
    LOG(ERROR) << "Catalog::setEpochMeta failed:" << s.toString() << " "
               << sb.GetString();
    return s;
  }
  DLOG(INFO) << "Catalog::setEpochMeta sucess:" << sb.GetString();
  return txn->commit().status();
}

Expected<std::unique_ptr<EpochMeta>> Catalog::getEpochMeta() {
  auto result = std::make_unique<EpochMeta>();
  std::stringstream ss;
  ss << "epoch_meta";
  RecordKey rk(0, 0, RecordType::RT_META, ss.str(), "");

  auto exptxn = _store->createTransaction(nullptr);
  if (!exptxn.ok()) {
    return exptxn.status();
  }

  Transaction* txn = exptxn.value().get();
  Expected<RecordValue> exprv = _store->getKV(rk, txn);
  if (!exprv.ok()) {
    return exprv.status();
  }
  RecordValue rv = exprv.value();
  const std::string& json = rv.getValue();

  LOG(INFO) << "Catalog::getEpochMeta succ," << json;

  rapidjson::Document doc;
  doc.Parse(json);
  if (doc.HasParseError()) {
    LOG(FATAL) << "parse epoch meta failed"
               << rapidjson::GetParseError_En(doc.GetParseError());
  }

  INVARIANT(doc.IsObject());

  INVARIANT(doc.HasMember("currentEpoch"));
  INVARIANT(doc["currentEpoch"].IsUint64());
  result->currentEpoch = static_cast<uint64_t>(doc["currentEpoch"].GetUint64());

  INVARIANT(doc.HasMember("lastVoteEpoch"));
  INVARIANT(doc["lastVoteEpoch"].IsUint64());
  result->lastVoteEpoch =
    static_cast<uint64_t>(doc["lastVoteEpoch"].GetUint64());

  return result;
}

}  // namespace tendisplus
