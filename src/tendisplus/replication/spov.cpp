// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <algorithm>
#include <chrono>  // NOLINT
#include <fstream>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "glog/logging.h"
#include "rapidjson/document.h"
#include "rapidjson/error/en.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

#include "tendisplus/commands/command.h"
#include "tendisplus/lock/lock.h"
#include "tendisplus/replication/repl_manager.h"
#include "tendisplus/storage/record.h"
#include "tendisplus/storage/rocks/rocks_kvstore.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/string.h"

namespace tendisplus {

Expected<BackupInfo> getBackupInfo(BlockingTcpClient* client,
                                   const StoreMeta& metaSnapshot,
                                   const string& ip,
                                   uint16_t port) {
  std::stringstream ss;
  ss << "FULLSYNC " << metaSnapshot.syncFromId << " " << ip << " " << port;
  Status s = client->writeLine(ss.str());
  if (!s.ok()) {
    LOG(WARNING) << "fullSync master failed:" << s.toString();
    return s;
  }

  BackupInfo bkInfo;
  auto expPos = client->readLine(std::chrono::seconds(1000));
  if (!expPos.ok()) {
    LOG(WARNING) << "fullSync req master error:" << expPos.status().toString();
    return expPos.status();
  }
  if (expPos.value().size() == 0 || expPos.value()[0] == '-') {
    LOG(WARNING) << "fullSync req master failed:" << expPos.value();
    return {ErrorCodes::ERR_INTERNAL, "fullSync master not ok"};
  }
  Expected<uint64_t> pos = ::tendisplus::stoul(expPos.value());
  if (!pos.ok()) {
    LOG(WARNING) << "fullSync binlogpos parse fail:" << pos.status().toString();
    return pos.status();
  }
  bkInfo.setBinlogPos(pos.value());

  auto expFlist = client->readLine(std::chrono::seconds(100));
  if (!expFlist.ok()) {
    LOG(WARNING) << "fullSync req flist error:" << expFlist.status().toString();
    return expFlist.status();
  }
  rapidjson::Document doc;
  doc.Parse(expFlist.value());
  if (doc.HasParseError()) {
    return {ErrorCodes::ERR_NETWORK,
            rapidjson::GetParseError_En(doc.GetParseError())};
  }
  if (!doc.IsObject()) {
    return {ErrorCodes::ERR_NOTFOUND, "flist not json obj"};
  }
  std::map<std::string, uint64_t> result;
#ifdef _WIN32
#undef GetObject
#endif
  for (auto& o : doc.GetObject()) {
    if (!o.value.IsUint64()) {
      return {ErrorCodes::ERR_NOTFOUND, "json value not uint64"};
    }
    result[o.name.GetString()] = o.value.GetUint64();
  }
  bkInfo.setFileList(result);
  return bkInfo;
}

// spov's network communicate procedure
// read binlogpos low watermark
// read filelist={filename->filesize}
// foreach file
//     read filename
//     read content
//     send +OK
// send +OK
void ReplManager::slaveStartFullsync(const StoreMeta& metaSnapshot) {
  LOG(INFO) << "store:" << metaSnapshot.id << " fullsync start";
  // NOTE(wayenchen):lastsyncTime should be inited if fullsync to new master
  {
    std::lock_guard<std::mutex> lk(_mutex);
    _syncStatus[metaSnapshot.id]->lastSyncTime = getGmtUtcTime();
  }
  LocalSessionGuard sg(_svr.get());
  // NOTE(deyukong): there is no need to setup a guard to clean the temp ctx
  // since it's on stack
  sg.getSession()->setArgs({"slavefullsync", std::to_string(metaSnapshot.id)});

  // 1) stop store and clean it's directory
  auto expdb = _svr->getSegmentMgr()->getDb(
    sg.getSession(), metaSnapshot.id, mgl::LockMode::LOCK_X);
  if (!expdb.ok()) {
    LOG(ERROR) << "get store:" << metaSnapshot.id
               << " failed: " << expdb.status().toString();
    return;
  }
  auto store = std::move(expdb.value().store);
  INVARIANT(store != nullptr);

  Status stopStatus = store->stop();
  if (!stopStatus.ok()) {
    // there may be uncanceled transactions binding with the store
    LOG(WARNING) << "stop store:" << metaSnapshot.id
                 << " failed:" << stopStatus.toString();
    return;
  }
  INVARIANT_D(!store->isRunning());
  Status clearStatus = store->clear();
  if (!clearStatus.ok()) {
    LOG(FATAL) << "Unexpected store:" << metaSnapshot.id << " clear"
               << " failed:" << clearStatus.toString();
  }

  std::shared_ptr<BlockingTcpClient> client;
  // 2) necessary pre-conditions all ok, startup a guard to rollback
  // state if failed
  bool rollback = true;
  auto guard = MakeGuard([this, &rollback, &metaSnapshot, &store, &client] {
    std::lock_guard<std::mutex> lk(_mutex);
    if (rollback) {
      auto newMeta = metaSnapshot.copy();
      newMeta->replState = ReplState::REPL_CONNECT;
      newMeta->binlogId = Transaction::TXNID_UNINITED;
      changeReplStateInLock(*newMeta, false);

      LOG(INFO) << "slaveStartFullsync rollback, rm dir:"
                << store->dftBackupDir();
      std::error_code ec;
      filesystem::remove_all(filesystem::path(store->dftBackupDir()), ec);
      if (ec) {
        LOG(ERROR) << "slaveStartFullsync rollback, rm dir:"
                   << store->dftBackupDir() << " failed:" << ec.message();
      }

      auto s = store->restart();
      if (!s.ok()) {
        LOG(ERROR) << "restart store fail in guard" << s.status().toString();
      }
    }
  });

  // 3) require a blocking-client
  client = std::move(
    createClient(metaSnapshot, _connectMasterTimeoutMs, CLIENT_MASTER));
  if (client == nullptr) {
    LOG(WARNING) << "startFullSync storeid:" << metaSnapshot.id
                 << " with: " << metaSnapshot.syncFromHost << ":"
                 << metaSnapshot.syncFromPort << " failed, no valid client";
    return;
  }

  auto newMeta = metaSnapshot.copy();
  newMeta->replState = ReplState::REPL_TRANSFER;
  newMeta->binlogId = Transaction::TXNID_UNINITED;
  changeReplState(*newMeta, false);

  // 4) read backupinfo from master
  // get binlogPos and filelist, other messages get from "backup_meta" file
  auto ebkInfo = getBackupInfo(client.get(),
                               metaSnapshot,
                               _svr->getParams()->bindIp,
                               _svr->getParams()->port);
  if (!ebkInfo.ok()) {
    LOG(WARNING) << "storeId:" << metaSnapshot.id
                 << ",syncMaster:" << metaSnapshot.syncFromHost << ":"
                 << metaSnapshot.syncFromPort << ":" << metaSnapshot.syncFromId
                 << " failed:" << ebkInfo.status().toString();
    return;
  }

  // TODO(deyukong): split the transfering-physical-task into many
  // small schedule-unit, each processes one file, or a fixed-size block.
  auto backupExists = [store]() -> Expected<bool> {
    std::error_code ec;
    bool exists =
      filesystem::exists(filesystem::path(store->dftBackupDir()), ec);
    if (ec) {
      return {ErrorCodes::ERR_INTERNAL, ec.message()};
    }
    return exists;
  }();
  if (!backupExists.ok() || backupExists.value()) {
    LOG(ERROR) << "store:" << metaSnapshot.id
               << " backupDir exists:" << store->dftBackupDir();
    return;
  }

  auto flist = ebkInfo.value().getFileList();

  std::set<std::string> finishedFiles;
  while (true) {
    if (finishedFiles.size() == flist.size()) {
      break;
    }
    Expected<std::string> s = client->readLine(std::chrono::seconds(10));
    if (!s.ok()) {
      return;
    }
    if (finishedFiles.find(s.value()) != finishedFiles.end()) {
      LOG(FATAL) << "BUG: fullsync " << s.value() << " retransfer";
    }
    if (flist.find(s.value()) == flist.end()) {
      LOG(FATAL) << "BUG: fullsync " << s.value() << " invalid file";
    }
    std::string fullFileName = store->dftBackupDir() + "/" + s.value();
    LOG(INFO) << "fullsync file:" << fullFileName << " transfer begin";

    filesystem::path fileDir = filesystem::path(fullFileName).remove_filename();
    if (!filesystem::exists(fileDir)) {
      LOG(INFO) << "slaveStartFullsync create_directories:" << fileDir;
      filesystem::create_directories(fileDir);
    }
    auto myfile = std::fstream(fullFileName, std::ios::out | std::ios::binary);
    if (!myfile.is_open()) {
      LOG(ERROR) << "open file:" << fullFileName << " for write failed";
      return;
    }
    size_t remain = flist.at(s.value());
    size_t fileBatch = (_cfg->binlogRateLimitMB * 1024 * 1024) / 10;
    while (remain) {
      size_t batchSize = std::min(remain, fileBatch);
      remain -= batchSize;
      Expected<std::string> exptData =
        client->read(batchSize, std::chrono::seconds(100));
      if (!exptData.ok()) {
        LOG(ERROR) << "fullsync read bulk data failed:"
                   << exptData.status().toString();
        return;
      }
      myfile.write(exptData.value().c_str(), exptData.value().size());
      if (myfile.bad()) {
        LOG(ERROR) << "write file:" << fullFileName
                   << " failed:" << strerror(errno);
        return;
      }
      Status s = client->writeLine("+OK");
      if (!s.ok()) {
        LOG(ERROR) << "write file:" << fullFileName
                   << " reply failed:" << s.toString();
        return;
      }
    }
    LOG(INFO) << "fullsync file:" << fullFileName << " transfer done";
    finishedFiles.insert(s.value());
  }

  BackupInfo bkInfo = std::move(ebkInfo.value());
  auto metaFile = store->dftBackupDir() + "/" + "backup_meta";
  if (filesystem::exists(metaFile)) {
    // if backup_meta exists, get the backupinfo from backup_meata
    auto ebinfo = store->getBackupMeta(store->dftBackupDir());
    if (!ebinfo.ok()) {
      INVARIANT_D(0);
      LOG(ERROR) << "invalid backup_meta" << ebinfo.status().toString();
      return;
    }

    bkInfo = std::move(ebinfo.value());
    INVARIANT(bkInfo.getBinlogPos() == ebkInfo.value().getBinlogPos());
    bkInfo.setFileList(ebkInfo.value().getFileList());
  }

  uint32_t flags = 0;
  auto binlogVersion = bkInfo.getBinlogVersion();
  BinlogVersion mybversion = _svr->getCatalog()->getBinlogVersion();
  LOG(INFO) << "store: " << store->dbId()
            << " binlogVersion:" << static_cast<int>(binlogVersion)
            << " mybversion:" << static_cast<int>(mybversion);
  if (binlogVersion == BinlogVersion::BINLOG_VERSION_1) {
    if (mybversion == BinlogVersion::BINLOG_VERSION_2) {
      flags |= ROCKS_FLAGS_BINLOGVERSION_CHANGED;
    }
  } else if (binlogVersion == BinlogVersion::BINLOG_VERSION_2) {
    if (mybversion == BinlogVersion::BINLOG_VERSION_1) {
      LOG(ERROR) << "invalid binlog version";
      return;
    }
  } else {
    INVARIANT_D(0);
  }

  client->writeLine("+OK");

  // 5) restart store, change to stready-syncing mode
  Expected<uint64_t> restartStatus = store->restart(
    true, Transaction::MIN_VALID_TXNID, bkInfo.getBinlogPos(), flags);
  if (!restartStatus.ok()) {
    LOG(FATAL) << "fullSync restart store:" << metaSnapshot.id
               << ",failed:" << restartStatus.status().toString();
  }

  newMeta = metaSnapshot.copy();
  newMeta->replState = ReplState::REPL_CONNECTED;
  newMeta->binlogId = bkInfo.getBinlogPos();
  {
    std::lock_guard<std::mutex> lk(_mutex);
    _syncStatus[metaSnapshot.id]->fullsyncSuccTimes++;
  }

  changeReplState(*newMeta, true);
  // NOTE(takenliu):should reset firstBinlogId to the MinBinlog,
  // otherwise truncateBinlogV2 will use cursor to add from 1, it will cost a
  // long time
  resetRecycleState(metaSnapshot.id);
  rollback = false;

  LOG(INFO) << "store:" << metaSnapshot.id
            << ",fullsync Done, files:" << finishedFiles.size()
            << ",binlogId:" << newMeta->binlogId
            << ",restart binlogId:" << restartStatus.value();
}

void ReplManager::slaveChkSyncStatus(const StoreMeta& metaSnapshot) {
  bool reconn = [this, &metaSnapshot] {
    std::lock_guard<std::mutex> lk(_mutex);
    auto sessionId = _syncStatus[metaSnapshot.id]->sessionId;
    auto lastSyncTime = _syncStatus[metaSnapshot.id]->lastSyncTime;
    if (sessionId == std::numeric_limits<uint64_t>::max()) {
      return true;
    }
    if (lastSyncTime + std::chrono::seconds(gBinlogHeartbeatTimeout) <=
        SCLOCK::now()) {
      return true;
    }
    return false;
  }();

  if (!reconn) {
    return;
  }
  LOG(INFO) << "store:" << metaSnapshot.id
            << " reconn with:" << metaSnapshot.syncFromHost << ","
            << metaSnapshot.syncFromPort << "," << metaSnapshot.syncFromId;

  bool has_error = true;
  std::string errStr = "";
  std::string errPrefix = "store:" + std::to_string(metaSnapshot.id) + " ";
  auto guard = MakeGuard([this, &metaSnapshot, &has_error, &errStr] {
    if (has_error) {
      auto newMeta = metaSnapshot.copy();
      newMeta->replState = ReplState::REPL_ERR;
      newMeta->replErr = errStr;
      LOG(WARNING) << errStr;
      changeReplState(*newMeta, false);
    }
  });


  std::shared_ptr<BlockingTcpClient> client = std::move(
    createClient(metaSnapshot, _connectMasterTimeoutMs, CLIENT_MASTER));
  if (client == nullptr) {
    errStr = errPrefix + "reconn master failed";
    return;
  }

  std::stringstream ss;
  ss << "INCRSYNC " << metaSnapshot.syncFromId << ' ' << metaSnapshot.id << ' '
     << metaSnapshot.binlogId << ' ' << _cfg->bindIp << ' ' << _cfg->port;
  auto status = client->writeLine(ss.str());
  if (!status.ok()) {
    errStr =
      errPrefix + "psync master write failed with error:" + status.toString();
    return;
  }
  Expected<std::string> s = client->readLine(std::chrono::seconds(10));
  if (!s.ok()) {
    errStr =
      errPrefix + "psync master failed with error:" + s.status().toString();
    return;
  }
  if (s.value().size() == 0 || s.value()[0] != '+') {
    errStr = errPrefix + "incrsync master bad return:" + s.value();
    return;
  }

  Status pongStatus = client->writeLine("+PONG");
  if (!pongStatus.ok()) {
    errStr = errPrefix + "write pong failed:" + pongStatus.toString();
    return;
  }

  NetworkAsio* network = _svr->getNetwork();
  INVARIANT_D(network != nullptr);

  // why dare we transfer a client to a session ?
  // 1) the logic gets here, so there wont be any
  // async handlers in the event queue.
  // 2) every handler is triggered by calling client's
  // some read/write/connect functions.
  // 3) master side will read +PONG before sending
  // new data, so there wont be any sticky packets.
  Expected<uint64_t> expSessionId = network->client2Session(std::move(client));
  if (!expSessionId.ok()) {
    errStr =
      errPrefix + "client2Session failed:" + expSessionId.status().toString();
    return;
  }
  uint64_t sessionId = expSessionId.value();
  uint64_t currSessId = std::numeric_limits<uint64_t>::max();
  {
    std::lock_guard<std::mutex> lk(_mutex);
    currSessId = _syncStatus[metaSnapshot.id]->sessionId;
    _syncStatus[metaSnapshot.id]->sessionId = sessionId;
  }

  if (currSessId != std::numeric_limits<uint64_t>::max()) {
    Status s = _svr->cancelSession(currSessId);
    LOG(INFO) << "sess:" << currSessId
              << ",discard status:" << (s.ok() ? "ok" : s.toString());
  }

  if (metaSnapshot.replState != ReplState::REPL_CONNECTED) {
    auto newMeta = metaSnapshot.copy();
    newMeta->replState = ReplState::REPL_CONNECTED;
    newMeta->replErr = "";
    changeReplState(*newMeta, false);
  }
  has_error = false;

  LOG(INFO) << "store:" << metaSnapshot.id
            << ",binlogId:" << metaSnapshot.binlogId << " psync master succ."
            << "session id: " << sessionId << ";";
}

void ReplManager::slaveSyncRoutine(uint32_t storeId) {
  SCLOCK::time_point nextSched = SCLOCK::now();
  auto guard = MakeGuard([this, &nextSched, storeId] {
    std::lock_guard<std::mutex> lk(_mutex);
    INVARIANT_D(_syncStatus[storeId]->isRunning);
    _syncStatus[storeId]->isRunning = false;
    if (nextSched > _syncStatus[storeId]->nextSchedTime) {
      _syncStatus[storeId]->nextSchedTime = nextSched;
    }
    _cv.notify_all();
  });

  std::unique_ptr<StoreMeta> metaSnapshot = [this, storeId]() {
    std::lock_guard<std::mutex> lk(_mutex);
    return std::move(_syncMeta[storeId]->copy());
  }();

  if (metaSnapshot->syncFromHost == "") {
    // if master is nil, try sched after 1 second
    LOG(WARNING) << "metaSnapshot->syncFromHost is nil, sleep 10 seconds";
    nextSched = nextSched + std::chrono::seconds(10);
    return;
  }

  if (metaSnapshot->replState == ReplState::REPL_CONNECT) {
    slaveStartFullsync(*metaSnapshot);
    nextSched = nextSched + std::chrono::seconds(3);
    return;
  } else if (metaSnapshot->replState == ReplState::REPL_CONNECTED ||
             metaSnapshot->replState == ReplState::REPL_ERR) {
    slaveChkSyncStatus(*metaSnapshot);
    nextSched = nextSched + std::chrono::seconds(10);
    return;
  } else {
    INVARIANT(false);
  }
}

// if logKey == "", it means binlog_heartbeat
Status ReplManager::applyRepllogV2(Session* sess,
                                   uint32_t storeId,
                                   const std::string& logKey,
                                   const std::string& logValue) {
  [this, storeId]() {
    std::unique_lock<std::mutex> lk(_mutex);
    _cv.wait(lk, [this, storeId] { return !_syncStatus[storeId]->isRunning; });
    _syncStatus[storeId]->isRunning = true;
  }();

  uint64_t sessionId = sess->id();
  uint64_t binlogTs = 0;
  bool idMatch = [this, storeId, sessionId]() {
    std::unique_lock<std::mutex> lk(_mutex);
    return (sessionId == _syncStatus[storeId]->sessionId);
  }();
  auto guard = MakeGuard([this, storeId, &binlogTs, &idMatch] {
    std::unique_lock<std::mutex> lk(_mutex);
    INVARIANT_D(_syncStatus[storeId]->isRunning);
    _syncStatus[storeId]->isRunning = false;
    if (idMatch) {
      _syncStatus[storeId]->lastSyncTime = SCLOCK::now();
      if (binlogTs > _syncStatus[storeId]->lastBinlogTs) {
        _syncStatus[storeId]->lastBinlogTs = binlogTs;
      }
    }
  });

  if (!idMatch) {
    return {ErrorCodes::ERR_NOTFOUND, "sessionId not match"};
  }

  if (logKey == "") {
    // binlog_heartbeat
    auto ets = tendisplus::stoull(logValue);
    INVARIANT_D(ets.ok());
    binlogTs = ets.value();
    if (binlogTs == 0) {
      /* If binlogTs == 0, it means the binlog_heartbeat generated by old
       * tendisplus version before 2.0.6 */
      binlogTs = msSinceEpoch();
    }
  } else {
    auto binlog = applySingleTxnV2(
      sess, storeId, logKey, logValue, BinlogApplyMode::KEEP_BINLOG_ID);
    if (!binlog.ok()) {
      return binlog.status();
    } else {
      std::lock_guard<std::mutex> lk(_mutex);
      // NOTE(vinchen): store the binlogId without changeReplState()
      // If it's shutdown, we can get the largest binlogId from rocksdb.
      _syncMeta[storeId]->binlogId = binlog.value().binlogId;
      binlogTs = binlog.value().binlogTs;
    }
  }
  return {ErrorCodes::ERR_OK, ""};
}

std::ofstream* ReplManager::getCurBinlogFs(uint32_t storeId) {
  std::ofstream* fs = nullptr;
  uint32_t currentId = 0;
  uint64_t ts = 0;
  {
    std::unique_lock<std::mutex> lk(_mutex);
    fs = _logRecycStatus[storeId]->fs.get();
    currentId = _logRecycStatus[storeId]->fileSeq;
    ts = _logRecycStatus[storeId]->timestamp;
  }
  if (fs == nullptr) {
    if (ts == 0) {
      ts = _svr->getStartupTimeNs() / 1000000;
    }
    char fname[256], tbuf[256];
    memset(fname, 0, 128);
    memset(tbuf, 0, 128);

    // ms to second
    time_t time = (time_t)(uint32_t)(ts / 1000);
    struct tm lt;
    (void)localtime_r(&time, &lt);
    strftime(tbuf, sizeof(tbuf), "%Y%m%d%H%M%S", &lt);

    snprintf(fname,
             sizeof(fname),
             "%s/%d/binlog-%d-%07d-%s.log",
             _dumpPath.c_str(),
             storeId,
             storeId,
             currentId + 1,
             tbuf);

    fs = KVStore::createBinlogFile(fname, storeId);
    if (!fs) {
      return fs;
    }

    std::unique_lock<std::mutex> lk(_mutex);
    auto& v = _logRecycStatus[storeId];
    v->fs.reset(fs);
    v->fileSeq = currentId + 1;
    v->fileCreateTime = SCLOCK::now();
    v->fileSize = BINLOG_HEADER_V2_LEN;
    v->needNewFile = false;
  }
  return fs;
}

bool ReplManager::newBinlogFs(uint32_t storeId) {
  {
    std::unique_lock<std::mutex> lk(_mutex);
    auto& v = _logRecycStatus[storeId];
    v->needNewFile = true;
  }
  int wait_times = 0;
  int sleepInter = 100;
  int maxWaitTimes =
    _svr->getParams()->truncateBinlogIntervalMs * 3 / sleepInter;
  while (wait_times++ <= maxWaitTimes) {
    {
      std::unique_lock<std::mutex> lk(_mutex);
      auto& v = _logRecycStatus[storeId];
      if (!v->needNewFile) {
        return true;
      }
    }
    std::this_thread::sleep_for(100ms);
  }
  LOG(WARNING) << "newBinlogFs failed, storeId:" << storeId;
  return false;
}

void ReplManager::updateCurBinlogFs(uint32_t storeId,
                                    uint64_t written,
                                    uint64_t ts,
                                    bool changeNewFile) {
  std::unique_lock<std::mutex> lk(_mutex);
  auto& v = _logRecycStatus[storeId];
  v->fileSize += written;
  if (ts) {
    v->timestamp = ts;
  }
  if (v->fileSize >= (uint64_t)_cfg->binlogFileSizeMB * 1024 * 1024 ||
      v->fileCreateTime + std::chrono::seconds(_cfg->binlogFileSecs) <=
        SCLOCK::now() ||
      changeNewFile || v->needNewFile) {
    if (v->fs) {
      v->fs->close();
      v->fs.reset();
    }
    v->needNewFile = false;
  }
}

}  // namespace tendisplus
