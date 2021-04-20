// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <list>
#include <chrono>  // NOLINT
#include <fstream>
#include <string>
#include <memory>
#include "tendisplus/commands/command.h"

#include "glog/logging.h"
#include "rapidjson/document.h"
#include "rapidjson/writer.h"

#include "tendisplus/replication/repl_manager.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/test_util.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/commands/dump.h"

namespace tendisplus {
bool ReplManager::supplyFullSync(asio::ip::tcp::socket sock,
                                 const std::string& storeIdArg,
                                 const std::string& slaveIpArg,
                                 const std::string& slavePortArg) {
  std::shared_ptr<BlockingTcpClient> client =
    std::move(_svr->getNetwork()->createBlockingClient(std::move(sock),
                                                       64 * 1024 * 1024));
  client->setFlags(CLIENT_SLAVE);

  // NOTE(deyukong): this judge is not precise
  // even it's not full at this time, it can be full during schedule.
  if (isFullSupplierFull()) {
    LOG(WARNING) << "ReplManager::supplyFullSync fullPusher isFull.";
    client->writeLine("-ERR workerpool full");
    return false;
  }

  auto expStoreId = tendisplus::stoul(storeIdArg);
  if (!expStoreId.ok()) {
    LOG(ERROR) << "ReplManager::supplyFullSync storeIdArg error:" << storeIdArg;
    client->writeLine("-ERR invalid storeId");
    return false;
  }
  uint32_t storeId = static_cast<uint32_t>(expStoreId.value());

  auto expSlavePort = tendisplus::stoul(slavePortArg);
  if (!expSlavePort.ok()) {
    LOG(ERROR) << "ReplManager::supplyFullSync expSlavePort error:"
               << slavePortArg;
    client->writeLine("-ERR invalid expSlavePort");
    return false;
  }
  LOG(INFO) << "ReplManager::supplyFullSync storeId:" << storeIdArg << " "
            << slaveIpArg << ":" << slavePortArg;
  uint16_t slavePort = static_cast<uint16_t>(expSlavePort.value());
  _fullPusher->schedule([this,
                         storeId,
                         client(std::move(client)),
                         slaveIpArg,
                         slavePort]() mutable {
    supplyFullSyncRoutine(std::move(client), storeId, slaveIpArg, slavePort);
  });

  return true;
}

bool ReplManager::isFullSupplierFull() const {
  return _fullPusher->isFull();
}

void ReplManager::masterPushRoutine(uint32_t storeId, uint64_t clientId) {
  SCLOCK::time_point nextSched = SCLOCK::now();
  SCLOCK::time_point lastSend = SCLOCK::time_point::min();
  auto guard = MakeGuard([this, &nextSched, &lastSend, storeId, clientId] {
    std::lock_guard<std::mutex> lk(_mutex);
    auto& mpov = _pushStatus[storeId];
    if (mpov.find(clientId) == mpov.end()) {
      return;
    }
    INVARIANT_D(mpov[clientId]->isRunning);
    mpov[clientId]->isRunning = false;
    if (nextSched > mpov[clientId]->nextSchedTime) {
      mpov[clientId]->nextSchedTime = nextSched;
    }
    if (lastSend > mpov[clientId]->lastSendBinlogTime) {
      mpov[clientId]->lastSendBinlogTime = lastSend;
    }
    // currently nothing waits for master's push process
    // _cv.notify_all();
  });

  uint64_t binlogPos = 0;
  BlockingTcpClient* client = nullptr;
  uint32_t dstStoreId = 0;
  bool needHeartbeat = false;
  MPovClientType clientType = MPovClientType::repllogClient;
  {
    std::lock_guard<std::mutex> lk(_mutex);
    if (_incrPaused ||
        _pushStatus[storeId].find(clientId) == _pushStatus[storeId].end()) {
      nextSched = nextSched + std::chrono::seconds(1);
      lastSend = _pushStatus[storeId][clientId]->lastSendBinlogTime;
      return;
    }
    binlogPos = _pushStatus[storeId][clientId]->binlogPos;
    client = _pushStatus[storeId][clientId]->client.get();
    dstStoreId = _pushStatus[storeId][clientId]->dstStoreId;
    lastSend = _pushStatus[storeId][clientId]->lastSendBinlogTime;
    clientType = _pushStatus[storeId][clientId]->clientType;
  }
  if (lastSend + std::chrono::seconds(gBinlogHeartbeatSecs) < SCLOCK::now()) {
    needHeartbeat = true;
  }

  BinlogResult br;
  Expected<BinlogResult> ret = br;

  if (_cfg->aofEnabled &&
      (clientType == MPovClientType::respClient || _cfg->psyncEnabled)) {
    ret = masterSendAof(
      client, storeId, dstStoreId, binlogPos, needHeartbeat, _svr, _cfg);
  } else {
    ret = masterSendBinlogV2(
      client, storeId, dstStoreId, binlogPos, needHeartbeat, _svr, _cfg);
  }
  if (!ret.ok()) {
    LOG(WARNING) << "masterSendBinlog to client:" << client->getRemoteRepr()
                 << " failed:" << ret.status().toString();
    std::lock_guard<std::mutex> lk(_mutex);
#if defined(WIN32) && _MSC_VER > 1900
    if (_pushStatus[storeId][clientId] != nullptr) {
      delete _pushStatus[storeId][clientId];
    }
#endif
    // it is safe to remove a non-exist key
    _pushStatus[storeId].erase(clientId);
    return;
  } else {
    if (ret.value().binlogId > binlogPos) {
      nextSched = SCLOCK::now();
      lastSend = nextSched;
    } else {
      nextSched = SCLOCK::now() + std::chrono::seconds(1);
      if (needHeartbeat) {
        lastSend = SCLOCK::now();
      }
    }
    std::lock_guard<std::mutex> lk(_mutex);
    _pushStatus[storeId][clientId]->binlogPos = ret.value().binlogId;
    _pushStatus[storeId][clientId]->binlogTs = ret.value().binlogTs;
  }
}

//  1) s->m INCRSYNC (m side: session2Client)
//  2) m->s +OK
//  3) s->m +PONG (s side: client2Session)
//  4) m->s periodly send binlogs
//  the 3) step is necessary, if ignored, the +OK in step 2) and binlogs
//  in step 4) may sticky together. and redis-resp protocal is not fixed-size
//  That makes client2Session complicated.

// NOTE(deyukong): we define binlogPos the greatest id that has been applied.
// "NOT" the smallest id that has not been applied. keep the same with
// BackupInfo's setCommitId
bool ReplManager::registerIncrSync(asio::ip::tcp::socket sock,
                                   const std::string& storeIdArg,
                                   const std::string& dstStoreIdArg,
                                   const std::string& binlogPosArg,
                                   const std::string& listenIpArg,
                                   const std::string& listenPortArg) {
  LOG(INFO) << "registerIncrSync storeIdArg:" << storeIdArg
    << " dstStoreIdArg:" << dstStoreIdArg
    << " binlogPosArg:" << binlogPosArg
    << " listenIpArg:" << listenIpArg
    << " listenPortArg:" << listenPortArg;
  std::shared_ptr<BlockingTcpClient> client =
    std::move(_svr->getNetwork()->createBlockingClient(std::move(sock),
                                                       64 * 1024 * 1024));
  client->setFlags(CLIENT_SLAVE);

  uint32_t storeId;
  uint32_t dstStoreId;
  uint64_t binlogPos;
  uint16_t listen_port;
  try {
    storeId = std::stoul(storeIdArg);
    dstStoreId = std::stoul(dstStoreIdArg);
    binlogPos = std::stoull(binlogPosArg);
    listen_port = std::stoull(listenPortArg);
  } catch (const std::exception& ex) {
    std::stringstream ss;
    ss << "-ERR parse opts failed:" << ex.what();
    client->writeLine(ss.str());
    return false;
  }

  if (storeId >= _svr->getKVStoreCount() ||
      dstStoreId >= _svr->getKVStoreCount()) {
    client->writeLine("-ERR invalid storeId");
    return false;
  }

  // NOTE(vinchen): In the cluster view, storeID of source and dest must be
  // same.
  if (storeId != dstStoreId) {
    client->writeLine("-ERR source storeId is different from dstStoreId ");
    return false;
  }

  LocalSessionGuard sg(_svr.get());
  auto expdb = _svr->getSegmentMgr()->getDb(
    sg.getSession(), storeId, mgl::LockMode::LOCK_IS);
  if (!expdb.ok()) {
    std::stringstream ss;
    ss << "-ERR store " << storeId << " error: " << expdb.status().toString();
    client->writeLine(ss.str());
    return false;
  }

  uint64_t firstPos = 0;
  uint64_t lastFlushBinlogId = 0;
  {
    std::lock_guard<std::mutex> lk(_mutex);
    firstPos = _logRecycStatus[storeId]->firstBinlogId;
    lastFlushBinlogId = _logRecycStatus[storeId]->lastFlushBinlogId;
  }

  // NOTE(deyukong): this check is not precise
  // (not in the same critical area with the modification to _pushStatus),
  // but it does not harm correctness.
  // A strict check may be too complicated to read.
  // NOTE(takenliu): 1.recycleBinlog use firstPos, and incrSync use
  // binlogPos+1
  //     2. slave do command slaveof master, master do flushall and
  //     truncateBinlogV2,
  //        slave send binlogpos will smaller than master.
  if (firstPos > (binlogPos + 1) && firstPos != lastFlushBinlogId) {
    std::stringstream ss;
    ss << "-ERR invalid binlogPos,storeId:" << storeId
       << ",master firstPos:" << firstPos << ",slave binlogPos:" << binlogPos
       << ",lastFlushBinlogId:" << lastFlushBinlogId;
    client->writeLine(ss.str());
    LOG(ERROR) << ss.str();
    return false;
  }
  client->writeLine("+OK");
  Expected<std::string> exptPong = client->readLine(std::chrono::seconds(1));
  if (!exptPong.ok()) {
    LOG(WARNING) << "slave incrsync handshake failed:"
                 << exptPong.status().toString();
    return false;
  } else if (exptPong.value() != "+PONG") {
    LOG(WARNING) << "slave incrsync handshake not +PONG:" << exptPong.value();
    return false;
  }

  std::string remoteHost = client->getRemoteRepr();
  bool registPosOk = [this,
                      storeId,
                      dstStoreId,
                      binlogPos,
                      client = std::move(client),
                      listenIpArg,
                      listen_port]() mutable {
    std::lock_guard<std::mutex> lk(_mutex);
    // takenliu: recycleBinlog use firstPos, and incrSync use binlogPos+1
    if (_logRecycStatus[storeId]->firstBinlogId > (binlogPos + 1) &&
        _logRecycStatus[storeId]->firstBinlogId !=
          _logRecycStatus[storeId]->lastFlushBinlogId) {
      std::stringstream ss;
      ss << "-ERR invalid binlogPos,storeId:" << storeId
         << ",master firstPos:" << _logRecycStatus[storeId]->firstBinlogId
         << ",slave binlogPos:" << binlogPos << ",lastFlushBinlogId:"
         << _logRecycStatus[storeId]->lastFlushBinlogId;
      LOG(ERROR) << ss.str();
      return false;
    }

    string slaveNode = listenIpArg + ":" + to_string(listen_port);
    auto iter = _fullPushStatus[storeId].find(slaveNode);
    if (iter != _fullPushStatus[storeId].end()) {
      LOG(INFO) << "registerIncrSync erase _fullPushStatus, "
                << iter->second->toString();
      _fullPushStatus[storeId].erase(iter);
    }

    uint64_t clientId = _clientIdGen.fetch_add(1);
#if defined(_WIN32) && _MSC_VER > 1900
    _pushStatus[storeId][clientId] =
      new MPovStatus{false,
                     static_cast<uint32_t>(dstStoreId),
                     binlogPos,
                     0,
                     SCLOCK::now(),
                     SCLOCK::time_point::min(),
                     std::move(client),
                     clientId,
                     listenIpArg,
                     listen_port};
#else
    _pushStatus[storeId][clientId] = std::move(std::unique_ptr<MPovStatus>(
      new MPovStatus{false,
                     static_cast<uint32_t>(dstStoreId),
                     binlogPos,
                     0,
                     SCLOCK::now(),
                     SCLOCK::time_point::min(),
                     std::move(client),
                     clientId,
                     listenIpArg,
                     listen_port}));
#endif
    return true;
  }();
  LOG(INFO) << "slave:" << remoteHost << " registerIncrSync "
            << (registPosOk ? "ok" : "failed");

  return registPosOk;
}

// mpov's network communicate procedure
// send binlogpos low watermark
// send filelist={filename->filesize}
// foreach file
//     send filename
//     send content
//     read +OK
// read +OK
void ReplManager::supplyFullSyncRoutine(
  std::shared_ptr<BlockingTcpClient> client,
  uint32_t storeId,
  const string& slave_listen_ip,
  uint16_t slave_listen_port) {
  LocalSessionGuard sg(_svr.get());
  sg.getSession()->setArgs(
    {"masterfullsync", client->getRemoteRepr(), std::to_string(storeId)});
  LOG(INFO) << "client:" << client->getRemoteRepr() << ",storeId:" << storeId
            << ",begins fullsync";
  auto expdb = _svr->getSegmentMgr()->getDb(
    sg.getSession(), storeId, mgl::LockMode::LOCK_IS);
  if (!expdb.ok()) {
    std::stringstream ss;
    ss << "-ERR store " << storeId << " error: " << expdb.status().toString();
    client->writeLine(ss.str());
    LOG(ERROR) << "getDb failed:" << expdb.status().toString();
    return;
  }
  auto store = std::move(expdb.value().store);
  INVARIANT(store != nullptr);

  if (!store->isRunning()) {
    client->writeLine("-ERR store is not running");
    LOG(ERROR) << "store is not running.";
    return;
  }

  {
    std::lock_guard<std::mutex> lk(_mutex);
    uint64_t highestBinlogid = store->getHighestBinlogId();
    string slaveNode = slave_listen_ip + ":" + to_string(slave_listen_port);
    auto iter = _fullPushStatus[storeId].find(slaveNode);
    if (iter != _fullPushStatus[storeId].end()) {
      LOG(INFO) << "supplyFullSyncRoutine already have _fullPushStatus, "
                << iter->second->toString();
      if (iter->second->state == FullPushState::ERR) {
        _fullPushStatus[storeId].erase(iter);
      } else {
        client->writeLine("-ERR already have _fullPushStatus, " +
                          iter->second->toString());
        return;
      }
    }

    uint64_t clientId = _clientIdGen.fetch_add(1);
#if defined(_WIN32) && _MSC_VER > 1900
    _fullPushStatus[storeId][slaveNode] =
      new MPovFullPushStatus{storeId,
                             FullPushState::PUSHING,
                             highestBinlogid,
                             SCLOCK::now(),
                             SCLOCK::time_point::min(),
                             client,
                             clientId,
                             slave_listen_ip,
                             slave_listen_port};
#else
    _fullPushStatus[storeId][slaveNode] =
      std::move(std::unique_ptr<MPovFullPushStatus>(
        new MPovFullPushStatus{storeId,
                               FullPushState::PUSHING,
                               highestBinlogid,
                               SCLOCK::now(),
                               SCLOCK::time_point::min(),
                               client,
                               clientId,
                               slave_listen_ip,
                               slave_listen_port}));
#endif
  }
  bool hasError = true;
  auto guard_0 = MakeGuard(
    [this, store, storeId, &hasError, slave_listen_ip, slave_listen_port]() {
      std::lock_guard<std::mutex> lk(_mutex);
      string slaveNode = slave_listen_ip + ":" + to_string(slave_listen_port);
      auto iter = _fullPushStatus[storeId].find(slaveNode);
      if (iter != _fullPushStatus[storeId].end()) {
        if (hasError) {
          LOG(INFO) << "supplyFullSyncRoutine hasError, _fullPushStatus erase, "
                    << iter->second->toString();
          _fullPushStatus[storeId].erase(iter);
        } else {
          iter->second->endTime = SCLOCK::now();
          iter->second->state = FullPushState::SUCESS;
        }
      } else {
        LOG(ERROR) << "supplyFullSyncRoutine, _fullPushStatus find node "
                      "failed, storeid:"
                   << storeId << " slave node:" << slaveNode;
      }
    });

  uint64_t currTime = nsSinceEpoch();
  Expected<BackupInfo> bkInfo =
    store->backup(store->dftBackupDir(),
                  KVStore::BackupMode::BACKUP_CKPT_INTER,
                  _svr->getCatalog()->getBinlogVersion());
  if (!bkInfo.ok()) {
    std::stringstream ss;
    ss << "-ERR backup failed:" << bkInfo.status().toString();
    client->writeLine(ss.str());
    LOG(ERROR) << "backup failed:" << bkInfo.status().toString();
    return;
  } else {
    LOG(INFO) << "storeId:" << storeId
              << ",backup cost:" << (nsSinceEpoch() - currTime) << "ns"
              << ",pos:" << bkInfo.value().getBinlogPos();
  }

  auto guard = MakeGuard([this, store, storeId]() {
    Status s = store->releaseBackup();
    if (!s.ok()) {
      LOG(ERROR) << "supplyFullSync end clean store:" << storeId
                 << " error:" << s.toString();
    }
  });

  // send binlogPos
  Status s = client->writeLine(std::to_string(bkInfo.value().getBinlogPos()));
  if (!s.ok()) {
    LOG(ERROR) << "store:" << storeId
               << " fullsync send binlogpos failed:" << s.toString();
    return;
  }
  LOG(INFO) << "fullsync " << storeId
            << " send binlogPos success:" << bkInfo.value().getBinlogPos();

  // send fileList
  rapidjson::StringBuffer sb;
  rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
  writer.StartObject();
  for (const auto& kv : bkInfo.value().getFileList()) {
    writer.Key(kv.first.c_str());
    writer.Uint64(kv.second);
  }
  writer.EndObject();
  uint32_t secs = 10;
  s = client->writeLine(sb.GetString());
  if (!s.ok()) {
    LOG(ERROR) << "store:" << storeId
               << " fullsync send filelist failed:" << s.toString();
    return;
  }
  LOG(INFO) << "fullsync " << storeId
            << " send fileList success:" << sb.GetString();

  std::string readBuf;
  size_t fileBatch = (_cfg->binlogRateLimitMB * 1024 * 1024) / 10;
  readBuf.reserve(fileBatch);
  for (auto& fileInfo : bkInfo.value().getFileList()) {
    s = client->writeLine(fileInfo.first);
    if (!s.ok()) {
      LOG(ERROR) << "write fname:" << fileInfo.first
                 << " to client failed:" << s.toString();
      return;
    }
    LOG(INFO) << "fulsync send filename success:" << fileInfo.first;
    std::string fname = store->dftBackupDir() + "/" + fileInfo.first;
    auto myfile = std::ifstream(fname, std::ios::binary);
    if (!myfile.is_open()) {
      LOG(ERROR) << "open file:" << fname << " for read failed";
      return;
    }
    size_t remain = fileInfo.second;
    while (remain) {
      size_t batchSize = std::min(remain, fileBatch);
      _rateLimiter->SetBytesPerSecond((uint64_t)_cfg->binlogRateLimitMB * 1024 *
                                      1024);
      _rateLimiter->Request(batchSize);
      readBuf.resize(batchSize);
      remain -= batchSize;
      myfile.read(&readBuf[0], batchSize);
      if (!myfile) {
        LOG(ERROR) << "read file:" << fname
                   << " failed with err:" << strerror(errno);
        return;
      }
      s = client->writeData(readBuf);
      if (!s.ok()) {
        LOG(ERROR) << "write bulk to client failed:" << s.toString();
        return;
      }
      secs = _cfg->timeoutSecBinlogWaitRsp;  // 10
      auto rpl = client->readLine(std::chrono::seconds(secs));
      if (!rpl.ok() || rpl.value() != "+OK") {
        LOG(ERROR) << "send client:" << client->getRemoteRepr()
                   << "file:" << fileInfo.first << ",size:" << fileInfo.second
                   << " failed:"
                   << (rpl.ok() ? rpl.value()
                                : rpl.status().toString());  // NOLINT
        return;
      }
    }
    LOG(INFO) << "fulsync send file success:" << fname;
  }
  secs = _cfg->timeoutSecBinlogWaitRsp;  // 10
  Expected<std::string> reply = client->readLine(std::chrono::seconds(secs));
  if (!reply.ok()) {
    LOG(ERROR) << "fullsync done read " << client->getRemoteRepr()
               << " reply failed:" << reply.status().toString();
  } else {
    LOG(INFO) << "fullsync storeid:" << storeId << " done, read "
              << client->getRemoteRepr() << "port" << slave_listen_port
              << " reply:" << reply.value();
    hasError = false;
  }
}

void ReplManager::AddFakeFullPushStatus(
        uint64_t slaveOffset, const std::string& ip, uint64_t port) {
  std::lock_guard<std::mutex> lk(_mutex);
  string slaveNode = ip + ":" + to_string(port);

  for (uint32_t storeId = 0; storeId < _svr->getKVStoreCount(); storeId++) {
    if (_fullPushStatus[storeId].find(slaveNode)
      == _fullPushStatus[storeId].end()) {
      auto expdb = _svr->getSegmentMgr()->getDb(
              nullptr, storeId, mgl::LockMode::LOCK_NONE);
      if (!expdb.ok()) {
        LOG(ERROR) << "slave offset get db error:" << expdb.status().toString();
        continue;
      }

      auto kvstore = std::move(expdb.value().store);
      uint64_t maxBinlog = kvstore->getHighestBinlogId();
      LOG(INFO) << "AddFakeFullPushStatus add fake task, storeId:" << storeId
                << " slaveNode:" << slaveNode << " slaveOffset:" << slaveOffset
                << " maxBinlog:" << maxBinlog;
      _fullPushStatus[storeId][slaveNode] =
        std::move(std::unique_ptr<MPovFullPushStatus>(
          new MPovFullPushStatus{storeId,
                                 FullPushState::SUCESS,
                                 maxBinlog,
                                 SCLOCK::now(),
                                 SCLOCK::now(),
                                 nullptr,
                                 0,
                                 ip,
                                 static_cast<uint16_t>(port)}));
    } else {
      LOG(INFO) << "AddFakeFullPushStatus already has task, storeId:"
                << storeId << " slaveNode:" << slaveNode;
    }
  }
}

void ReplManager::DelFakeFullPushStatus(
        const std::string& ip, uint64_t port) {
  std::lock_guard<std::mutex> lk(_mutex);
  string slaveNode = ip + ":" + to_string(port);

  for (uint32_t storeId = 0; storeId < _svr->getKVStoreCount(); storeId++) {
    if (_fullPushStatus[storeId].find(slaveNode)
        != _fullPushStatus[storeId].end()) {
      LOG(INFO) << "DelFakeFullPushStatus del fake task, storeId:" << storeId
                << " slaveNode:" << slaveNode;
      _fullPushStatus[storeId].erase(slaveNode);
    } else {
      LOG(INFO) << "DelFakeFullPushStatus dont has task, storeId:"
                << storeId << " slaveNode:" << slaveNode;
    }
  }
}

bool ReplManager::supplyFullPsync(asio::ip::tcp::socket sock,
                                  const std::string& storeIdArg) {
  std::shared_ptr<BlockingTcpClient> client =
    std::move(_svr->getNetwork()->
              createBlockingClient(std::move(sock),
                                   64 * 1024 * 1024,
                                   0,
                                   3600));  // set timeout 1 hour

  client->setFlags(CLIENT_SLAVE);
  // NOTE(deyukong): this judge is not precise
  // even it's not full at this time, it can be full during schedule.
  if (isFullSupplierFull()) {
    LOG(WARNING) << "ReplManager::supplyFullPsync fullPusher isFull.";
    client->writeLine("-ERR workerpool full");
    return false;
  }

  auto expStoreId = tendisplus::stoul(storeIdArg);
  if (!expStoreId.ok()) {
    LOG(ERROR) << "ReplManager::supplyFullPsync storeIdArg error:"
               << storeIdArg;
    client->writeLine("-ERR invalid storeId");
    return false;
  }

  uint32_t storeId = static_cast<uint32_t>(expStoreId.value());
  if (storeId >= _svr->getKVStoreCount()) {
    client->writeLine("-ERR invalid storeId");
    return false;
  }

  _fullPusher->schedule([this, storeId, client(std::move(client))]() mutable {
    supplyFullPsyncRoutine(std::move(client), storeId);
  });

  return true;
}

void ReplManager::supplyFullPsyncRoutine(
  std::shared_ptr<BlockingTcpClient> client, uint32_t storeId) {
  char runId[CONFIG_RUN_ID_SIZE + 1];
  redis_port::getRandomHexChars(runId, CONFIG_RUN_ID_SIZE);
  client->writeData("+FULLRESYNC " + string(runId) + " 0\r\n");

  // send no keys rdb to client
  unsigned char rdbData[] = {
    0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x30, 0x39, 0xfa, 0x09, 0x72,
    0x65, 0x64, 0x69, 0x73, 0x2d, 0x76, 0x65, 0x72, 0x05, 0x36, 0x2e, 0x30,
    0x2e, 0x39, 0xfa, 0x0a, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x62, 0x69,
    0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74, 0x69, 0x6d, 0x65, 0xc2,
    0xdf, 0x81, 0xe8, 0x5f, 0xfa, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d,
    0x65, 0x6d, 0xc2, 0x50, 0x8a, 0x0f, 0x00, 0xfa, 0x0c, 0x61, 0x6f, 0x66,
    0x2d, 0x70, 0x72, 0x65, 0x61, 0x6d, 0x62, 0x6c, 0x65, 0xc0, 0x00, 0xff,
    0x3a, 0xd1, 0x2d, 0xca, 0x85, 0x7c, 0xdd, 0x26};

  auto length = sizeof(rdbData) / sizeof(rdbData[0]);
  client->writeData("$" + to_string(length) + "\r\n");

  auto s = client->writeData(
    std::string(reinterpret_cast<const char*>(rdbData), length));
  if (!s.ok()) {
    LOG(ERROR) << "write rdb data to client failed:" << s.toString();
    return;
  }

  // create snapshot
  std::unique_ptr<BasicDataCursor> cursor;
  std::shared_ptr<KVStore> store;
  std::unique_ptr<Transaction> txn;
  uint64_t binlogPos;

  LocalSessionGuard sg(_svr.get());
  sg.getSession()->setArgs({
    "masterpsync",
    std::to_string(storeId),
    client->getRemoteRepr(),
  });

  {
    auto expdb = _svr->getSegmentMgr()->getDb(
      sg.getSession(), storeId, mgl::LockMode::LOCK_S);
    if (!expdb.ok()) {
      return;
    }

    store = expdb.value().store;
    // Because LOCK_S here, BLWM is same as BHWM
    binlogPos = store->getHighestBinlogId();

    auto ptxn = store->createTransaction(sg.getSession());
    if (!ptxn.ok()) {
      LOG(ERROR) << "createTransaction failed:" << ptxn.status().toString();
      client->writeLine("-ERR createTransaction failed");
      return;
    }

    txn = std::move(ptxn.value());
    txn->SetSnapshot();
    cursor = txn->createDataCursor();
  }

  // Free LOCK_S, Lock IS again
  auto expdb = _svr->getSegmentMgr()->getDb(
    sg.getSession(), storeId, mgl::LockMode::LOCK_IS);
  if (!expdb.ok()) {
    client->writeLine("-ERR getdb error");
    return;
  }

  // save full status
  uint64_t clientId = _clientIdGen.fetch_add(1);
  string remoteIP;  // always empty
  auto remotePort = client->getRemotePort();
  {
    std::lock_guard<std::mutex> lk(_mutex);
    string slaveNode = remoteIP + ":" + to_string(remotePort);
    auto iter = _fullPushStatus[storeId].find(slaveNode);
    if (iter != _fullPushStatus[storeId].end()) {
      LOG(INFO) << "supplyFullPsyncRoutine already have _fullPushStatus, "
                << iter->second->toString();
      if (iter->second->state == FullPushState::ERR) {
        _fullPushStatus[storeId].erase(iter);
      } else {
        client->writeLine("-ERR already have _fullPushStatus, " +
                          iter->second->toString());
        return;
      }
    }

#if defined(_WIN32) && _MSC_VER > 1900
    _fullPushStatus[storeId][slaveNode] =
      new MPovFullPushStatus{storeId,
                             FullPushState::PUSHING,
                             binlogPos,
                             SCLOCK::now(),
                             SCLOCK::time_point::min(),
                             client,
                             clientId,
                             remoteIP,
                             remotePort};
#else
    _fullPushStatus[storeId][slaveNode] =
      std::move(std::unique_ptr<MPovFullPushStatus>(
        new MPovFullPushStatus{storeId,
                               FullPushState::PUSHING,
                               binlogPos,
                               SCLOCK::now(),
                               SCLOCK::time_point::min(),
                               client,
                               clientId,
                               remoteIP,
                               remotePort}));
#endif
  }


  auto guard_0 = MakeGuard([this, storeId, remoteIP, remotePort]() {
    std::lock_guard<std::mutex> lk(_mutex);
    string slaveNode = remoteIP + ":" + to_string(remotePort);
    auto iter = _fullPushStatus[storeId].find(slaveNode);
    if (iter != _fullPushStatus[storeId].end()) {
      LOG(INFO) << "supplyFullPsyncRoutine, _fullPushStatus erase, "
                << iter->second->toString();
      _fullPushStatus[storeId].erase(iter);
    } else {
      LOG(ERROR) << "supplyFullPsyncRoutine, _fullPushStatus find node "
                    "failed, storeid:"
                 << storeId << " slave node:" << slaveNode;
    }
  });


  // send data from snapshot
  uint64_t currentTs = msSinceEpoch();
  std::list<Record> result;
  std::string prevPrimaryKey;
  uint64_t prevTTL = 0;
  uint32_t prevChunkId = CLUSTER_SLOTS;
  uint64_t kvCount = 0;
  std::string sendBuf;
  while (true) {
    Expected<Record> exptRcd = cursor->next();
    if (exptRcd.status().code() == ErrorCodes::ERR_EXHAUST ||
        exptRcd.value().getRecordKey().getChunkId() >= CLUSTER_SLOTS) {
      if (!result.empty()) {
        auto aofStr = recordList2Aof(result);
        if (!aofStr.ok()) {
          LOG(WARNING) << aofStr.status().toString();
          client->writeLine("-ERR invalid data");
          return;
        }
        sendBuf.append(aofStr.value());
        result.clear();

        if (prevTTL != 0 && currentTs <= prevTTL) {
          std::stringstream ss1;
          Command::fmtMultiBulkLen(ss1, 3);
          Command::fmtBulk(ss1, "PEXPIREAT");
          Command::fmtBulk(ss1, prevPrimaryKey);
          Command::fmtBulk(ss1, std::to_string(prevTTL));
          sendBuf.append(ss1.str());
        }
      }

      if (!sendBuf.empty()) {
        Status s = client->writeData(sendBuf);
        if (!s.ok()) {
          LOG(ERROR) << "full psync write data error." << s.toString();
        }
        sendBuf.clear();
      }

      break;
    }

    if (!exptRcd.ok()) {
      client->writeLine("-ERR invalid data");
      return;
    }

    auto keyType = exptRcd.value().getRecordKey().getRecordType();
    auto chunkId = exptRcd.value().getRecordKey().getChunkId();
    auto dbid = exptRcd.value().getRecordKey().getDbId();
    auto valueType = exptRcd.value().getRecordValue().getRecordType();
    if (!isRealEleType(keyType, valueType)) {
      continue;
    }

    kvCount++;
    if (chunkId != prevChunkId) {
      LOG(INFO) << "full psync. begin chunkId: " << chunkId
                << " key count: " << kvCount;
      prevChunkId = chunkId;
    }

    uint64_t targetTTL = 0;
    auto curPrimarykey = exptRcd.value().getRecordKey().getPrimaryKey();
    if (valueType != RecordType::RT_KV) {
      if (kvCount > 1 && curPrimarykey == prevPrimaryKey) {
        targetTTL = prevTTL;
      } else {
        RecordKey mk(
          chunkId, dbid, RecordType::RT_DATA_META, curPrimarykey, "");
        Expected<RecordValue> eValue = store->getKV(mk, txn.get());
        if (eValue.ok()) {
          targetTTL = eValue.value().getTtl();
        } else {
          LOG(WARNING) << "Get target ttl for key " << curPrimarykey
                       << " of type: " << rt2Str(keyType) << " in db:" << dbid
                       << " from chunk: " << chunkId
                       << " failed, error: " << eValue.status().toString();
          client->writeLine("-ERR get ttl error");
          return;
        }
      }
    } else {
      targetTTL = exptRcd.value().getRecordValue().getTtl();
    }

    if (0 != targetTTL && currentTs > targetTTL) {
      if (kvCount > 1 && currentTs <= prevTTL &&
          prevPrimaryKey != curPrimarykey) {
        if (!result.empty()) {
          auto aofStr = recordList2Aof(result);
          if (!aofStr.ok()) {
            LOG(WARNING) << aofStr.status().toString();
            client->writeLine("-ERR invalid data");
            return;
          }
          sendBuf.append(aofStr.value());
          result.clear();
        }

        std::stringstream ss1;
        Command::fmtMultiBulkLen(ss1, 3);
        Command::fmtBulk(ss1, "PEXPIREAT");
        Command::fmtBulk(ss1, prevPrimaryKey);
        Command::fmtBulk(ss1, std::to_string(prevTTL));
        sendBuf.append(ss1.str());
      }

      prevPrimaryKey = curPrimarykey;
      prevTTL = targetTTL;
      continue;
    }

    if (kvCount == 1) {
      prevPrimaryKey = curPrimarykey;
      prevTTL = targetTTL;
      result.emplace_back(std::move(exptRcd.value()));

    } else if (curPrimarykey == prevPrimaryKey) {
      result.emplace_back(std::move(exptRcd.value()));
      if (result.size() >= 1000) {
        auto aofStr = recordList2Aof(result);
        if (!aofStr.ok()) {
          LOG(WARNING) << aofStr.status().toString();
          client->writeLine("-ERR invalid data");
          return;
        }
        sendBuf.append(aofStr.value());
        result.clear();
      }
    } else {
      auto aofStr = recordList2Aof(result);
      if (!aofStr.ok()) {
        LOG(WARNING) << aofStr.status().toString();
        client->writeLine("-ERR invalid data");
        return;
      }
      sendBuf.append(aofStr.value());

      if (prevTTL != 0 && currentTs <= prevTTL) {
        std::stringstream ss1;
        Command::fmtMultiBulkLen(ss1, 3);
        Command::fmtBulk(ss1, "PEXPIREAT");
        Command::fmtBulk(ss1, prevPrimaryKey);
        Command::fmtBulk(ss1, std::to_string(prevTTL));
        sendBuf.append(ss1.str());
      }

      prevPrimaryKey = curPrimarykey;
      prevTTL = targetTTL;

      result.clear();
      result.emplace_back(std::move(exptRcd.value()));
    }

    if (sendBuf.size() > 10000) {
      Status s = client->writeData(sendBuf);
      if (!s.ok()) {
        LOG(ERROR) << "full psync write data error." << s.toString();
        break;
      }
      sendBuf.clear();
    }
  }

  LOG(INFO) << "full psync done."
            << "remote addr:" << client->getRemoteRepr()
            << "sotreId:" << storeId;


#if defined(_WIN32) && _MSC_VER > 1900
  _pushStatus[storeId][clientId] =
    new MPovStatus{false,
                   static_cast<uint32_t>(storeId),
                   binlogPos,
                   0,
                   SCLOCK::now(),
                   SCLOCK::time_point::min(),
                   std::move(client),
                   clientId,
                   remoteIP,
                   remotePort,
                   MPovClientType::respClient};
#else
  _pushStatus[storeId][clientId] = std::move(
    std::unique_ptr<MPovStatus>(new MPovStatus{false,
                                               static_cast<uint32_t>(storeId),
                                               binlogPos,
                                               0,
                                               SCLOCK::now(),
                                               SCLOCK::time_point::min(),
                                               std::move(client),
                                               clientId,
                                               remoteIP,
                                               remotePort,
                                               MPovClientType::respClient}));
#endif

  return;
}
}  // namespace tendisplus
