// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "glog/logging.h"
#include "tendisplus/cluster/migrate_manager.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {
Expected<uint64_t> addMigrateBinlog(MigrateBinlogType type,
                                    string slots,
                                    uint32_t storeid,
                                    ServerEntry* svr,
                                    const string& nodeName = "none") {
  // Temporarily disabled
  INVARIANT_D(0);
  auto expdb =
    svr->getSegmentMgr()->getDb(NULL, storeid, mgl::LockMode::LOCK_IX);
  if (!expdb.ok()) {
    return expdb.status();
  }
  // NOTE(takenliu) save all info to cmd
  string saveInfo =
    to_string(type) + "_" + to_string(storeid) + "_" + slots + "_" + nodeName;

  LocalSessionGuard sg(svr);
  sg.getSession()->setArgs({saveInfo});

  sg.getSession()->getCtx()->setReplOnly(false);
  auto eptxn = expdb.value().store->createTransaction(sg.getSession());
  if (!eptxn.ok()) {
    return eptxn.status();
  }
  auto txn = std::move(eptxn.value());

  // write the binlog using nextBinlogid.
  auto s = txn->migrate("", "");
  if (!s.ok()) {
    return s;
  }

  auto eBinlogid = txn->commit();
  if (!eBinlogid.ok()) {
    LOG(ERROR) << "addMigrateBinlog commit failed:"
               << eBinlogid.status().toString();
  }
  LOG(INFO) << "addMigrateBinlog storeid:" << storeid << " type:" << type
            << " nodeName:" << nodeName << " slots:" << slots
            << " binlogid:" << eBinlogid.value();
  return eBinlogid;
}

const std::string receTaskTypeString(MigrateReceiveState c) {
  switch (c) {
    case MigrateReceiveState::ERR:
      return "FAIL";
    case MigrateReceiveState::SUCC:
      return "SUCC";
    case MigrateReceiveState::RECEIVE_BINLOG:
    case MigrateReceiveState::RECEIVE_SNAPSHOT:
      return "RUNNING";
    case MigrateReceiveState::NONE:
      return "NONE";
  }
  return "unknown";
}

const std::string sendTaskTypeString(MigrateSendState c) {
  switch (c) {
    case MigrateSendState::ERR:
      return "FAIL";
    case MigrateSendState::START:
    case MigrateSendState::CLEAR:
    case MigrateSendState::HALF:
      return "RUNNING";
    case MigrateSendState::SUCC:
      return "SUCC";
    case MigrateSendState::WAIT:
      return "WAITING";
  }
  return "unknown";
}

MigrateManager::MigrateManager(std::shared_ptr<ServerEntry> svr,
                               const std::shared_ptr<ServerParams> cfg)
  : _cfg(cfg),
    _svr(svr),
    _isRunning(false),
    _taskIdGen(0),
    _pTaskIdGen(0),
    _migrateSenderMatrix(std::make_shared<PoolMatrix>()),
    _migrateReceiverMatrix(std::make_shared<PoolMatrix>()),
    _workload(0),
    _rateLimiter(
      std::make_unique<RateLimiter>(_cfg->migrateRateLimitMB * 1024 * 1024)) {
  _cluster = _svr->getClusterMgr()->getClusterState();

  _cfg->serverParamsVar("migrateSenderThreadnum")->setUpdate([this]() {
    migrateSenderResize(_cfg->migrateSenderThreadnum);
  });

  _cfg->serverParamsVar("migrateReceiveThreadnum")->setUpdate([this]() {
    migrateReceiverResize(_cfg->migrateReceiveThreadnum);
  });
}

Status MigrateManager::startup() {
  std::lock_guard<myMutex> lk(_mutex);

  // sender's pov
  _migrateSender =
    std::make_unique<WorkerPool>("tx-mgrt-snd", _migrateSenderMatrix);
  Status s = _migrateSender->startup(_cfg->migrateSenderThreadnum);
  if (!s.ok()) {
    return s;
  }

  // receiver's pov
  _migrateReceiver =
    std::make_unique<WorkerPool>("tx-mgrt-rcv", _migrateReceiverMatrix);
  s = _migrateReceiver->startup(_cfg->migrateReceiveThreadnum);
  if (!s.ok()) {
    return s;
  }

  for (uint32_t storeid = 0; storeid < _svr->getKVStoreCount(); storeid++) {
    _restoreMigrateTask[storeid] = std::list<SlotsBitmap>();
  }

  _isRunning.store(true, std::memory_order_relaxed);

  _controller =
    std::make_unique<std::thread>(std::move([this]() { controlRoutine(); }));
  return {ErrorCodes::ERR_OK, ""};
}

void MigrateManager::stop() {
  LOG(INFO) << "MigrateManager begins stops...";
  _isRunning.store(false, std::memory_order_relaxed);
  _controller->join();
  /*NOTE(wayenchen) all task should be stopped first*/
  stopAllTasks(false);
  // make sure all workpool has been stopped; otherwise calling
  // the destructor of a std::thread that is running will crash
  _migrateSender->stop();
  _migrateReceiver->stop();

  LOG(INFO) << "MigrateManager stops succ";
}

Status MigrateManager::stopStoreTask(uint32_t storeid) {
  std::lock_guard<myMutex> lk(_mutex);
  for (auto& iter : _migrateSendTaskMap) {
    if (iter.second->_storeid == storeid) {
      iter.second->stopTask();
    }
  }
  for (auto& iter : _migrateReceiveTaskMap) {
    if (iter.second->_storeid == storeid) {
      iter.second->stopTask();
    }
  }
  return {ErrorCodes::ERR_OK, ""};
}

Status MigrateManager::stopSrcNode(const std::string& taskid,
                                   const std::string& srcIp,
                                   uint64_t port) {
  // auto node =_cluster->clusterLookupNode(nodeid);
  LOG(INFO) << "send stop command to srcNode:" << srcIp << ":" << port
            << "on taskid:" << taskid;
  std::string srcHost = srcIp;
  auto srcPort = port;
  std::shared_ptr<BlockingTcpClient> client =
    std::move(createClient(srcHost, srcPort, _svr));

  if (client == nullptr) {
    LOG(ERROR) << "stop command send to: " << srcHost << ":" << srcPort
               << " failed, no valid client";
    return {ErrorCodes::ERR_NETWORK, "no valid client"};
  }
  std::stringstream ss;
  std::vector<std::string> vec = {"cluster", "setslot", "stop", taskid};
  Command::fmtMultiBulkLen(ss, vec.size());
  for (auto& arg : vec) {
    Command::fmtBulk(ss, arg);
  }

  Status s = client->writeLine(ss.str());
  if (!s.ok()) {
    LOG(ERROR) << "stop sender task failed:" << s.toString()
               << "pTask:" << taskid;
    return s;
  }

  auto expRsp = client->readLine(std::chrono::seconds(5));
  if (!expRsp.ok()) {
    LOG(ERROR) << " stop taskid SrcNode req error:"
               << expRsp.status().toString() << "pTask:" << taskid;
    return expRsp.status();
  }
  std::string value = expRsp.value();
  if (expRsp.value() != "+OK") {
    LOG(WARNING) << " stop taskid failed:" << expRsp.value()
                 << "pTask:" << taskid;
    return {ErrorCodes::ERR_INTERNAL, "readymigrate req srcDb failed"};
  }

  return {ErrorCodes::ERR_OK, ""};
}
/* stop the tasks which have the father taskid */
Status MigrateManager::stopTasks(const std::string& taskid) {
  bool find = false;
  // std::string srcNodeid;
  std::string srcHost;
  uint64_t port = 0;
  {
    std::lock_guard<myMutex> lk(_mutex);
    for (auto& iter : _migrateReceiveTaskMap) {
      if (iter.first.find(taskid) != std::string::npos) {
        iter.second->stopTask();
        srcHost = iter.second->_srcIp;
        port = iter.second->_srcPort;
        find = true;
      }
    }
    for (auto& iter : _migrateSendTaskMap) {
      if (iter.first.find(taskid) != std::string::npos) {
        iter.second->stopTask();
      }
    }
  }

  if (find) {
    // send stop command to srcNode
    auto s = stopSrcNode(taskid, srcHost, port);
    if (!s.ok()) {
      LOG(ERROR) << "stop srcNode taskid:" << taskid << "fail" << s.toString();
      return s;
    }
  }

  return {ErrorCodes::ERR_OK, ""};
}

/* stop all tasks which is doing now */
Status MigrateManager::stopAllTasks(bool saveSlots) {
  std::map<std::string, std::string> pTaskMap;
  {
    std::lock_guard<myMutex> lk(_mutex);

    for (auto& iter : _migrateReceiveTaskMap) {
      if (saveSlots) {
        _stopImportSlots |= iter.second->_slots;
      }
      auto pTask = iter.second->_pTask;
      if (pTaskMap.find(pTask->getTaskid()) == pTaskMap.end()) {
        pTaskMap.insert(std::make_pair(pTask->getTaskid(), pTask->_nodeid));
      }
      iter.second->stopTask();
    }

    if (saveSlots) {
      uint32_t idx = 0;
      while (idx < CLUSTER_SLOTS) {
        if (_stopImportSlots.test(idx)) {
          std::string nodeId = _importNodes[idx];
          auto iter = _stopImportMap.find(nodeId);
          if (iter != _stopImportMap.end()) {
            _stopImportMap[nodeId].set(idx);
          } else {
            std::bitset<CLUSTER_SLOTS> taskmap;
            taskmap.set(idx);
            _stopImportMap.insert(std::make_pair(nodeId, taskmap));
          }
        }
        idx++;
      }
    }
  }
  for (auto iter = pTaskMap.begin(); iter != pTaskMap.end(); iter++) {
    auto node = _cluster->clusterLookupNode(iter->second);
    // send stop command to srcNode
    if (!node) {
      LOG(ERROR) << "can not find srcNode:" << iter->second;
      return {ErrorCodes::ERR_CLUSTER, "can not find srcNode"};
    }

    auto s = stopSrcNode(iter->first, node->getNodeIp(), node->getPort());
    if (!s.ok()) {
      LOG(ERROR) << "stop srcNode task fail:" << s.toString();
      return s;
    }
  }

  LOG(INFO) << "stop all import slots:" << bitsetStrEncode(_stopImportSlots);
  return {ErrorCodes::ERR_OK, ""};
}

void MigrateManager::removeRestartSlots(const std::string& nodeid,
                                        const SlotsBitmap& slots) {
  std::lock_guard<myMutex> lk(_mutex);
  _stopImportSlots ^= slots;
  _stopImportMap.erase(nodeid);
}

void MigrateManager::controlRoutine() {
  while (_isRunning.load(std::memory_order_relaxed)) {
    bool doSth = false;
    auto now = SCLOCK::now();
    {
      std::lock_guard<myMutex> lk(_mutex);

      doSth = senderSchedule(now) || doSth;
      doSth = receiverSchedule(now) || doSth;
    }
    if (doSth) {
      std::this_thread::yield();
    } else {
      std::this_thread::sleep_for(10ms);
    }
  }
  LOG(INFO) << "migration manager controller exits";
}

////////////////////////////////////
// Sender POV
///////////////////////////////////
bool MigrateManager::senderSchedule(const SCLOCK::time_point& now) {
  bool doSth = false;

  for (auto it = _migrateSendTaskMap.begin();
       it != _migrateSendTaskMap.end();) {
    auto taskPtr = (*it).second.get();
    if (taskPtr->_isRunning || now < (taskPtr)->_nextSchedTime) {
      ++it;
      continue;
    }
    doSth = true;
    if (taskPtr->_state == MigrateSendState::WAIT) {
      SCLOCK::time_point nextSched = SCLOCK::now();
      taskPtr->_nextSchedTime = nextSched + std::chrono::milliseconds(100);
      /* NOTE(wayenchen) if dst node fail, stop the sender tasks to it*/
      if (_cluster->clusterNodeFailed(taskPtr->_pTask->_nodeid)) {
        taskPtr->_state = MigrateSendState::ERR;
        LOG(ERROR) << "receiver node failed, give up the task,"
                   << " taskid:" << taskPtr->_taskid;
      }
      ++it;
    } else if (taskPtr->_state == MigrateSendState::START) {
      /* NOTE(wayenchen) send slots task should not be in queue in case of
       * timeout */
      if (!_migrateSender->isFull()) {
        taskPtr->_isRunning = true;
        _migrateSender->schedule(
          [this, iter = taskPtr]() { iter->sendSlots(); });
      } else {
        LOG(ERROR) << "sender task threadpool is full on slots:"
                   << bitsetStrEncode(taskPtr->_slots)
                   << "taskid:" << taskPtr->_taskid;
        taskPtr->_sender->stop();
        taskPtr->_nextSchedTime =
          SCLOCK::now() + std::chrono::milliseconds(100);
        taskPtr->_state = MigrateSendState::WAIT;
      }
      ++it;
    } else if (taskPtr->_state == MigrateSendState::CLEAR) {
      taskPtr->_isRunning = true;
      taskPtr->deleteSenderChunks();
      ++it;
    } else if (taskPtr->_state == MigrateSendState::SUCC ||
               taskPtr->_state == MigrateSendState::ERR) {
      std::string slot = bitsetStrEncode(taskPtr->_slots);
      for (size_t i = 0; i < CLUSTER_SLOTS; i++) {
        if (taskPtr->_slots.test(i)) {
          if (taskPtr->_state == MigrateSendState::SUCC) {
            _succMigrateSlots.set(i);
            if (_failMigrateSlots.test(i)) {
              _failMigrateSlots.reset(i);
            }
          } else {
            _failMigrateSlots.set(i);
          }
          _migrateNodes.erase(i);
        }
      }
      if (taskPtr->_state == MigrateSendState::SUCC) {
        _succSenderTask.push_back(slot);
        taskPtr->_pTask->_succStateMap[taskPtr->_taskid] = taskPtr->toString();
      } else {
        _failSenderTask.push_back(slot);
        taskPtr->_pTask->_failStateMap[taskPtr->_taskid] = taskPtr->toString();
      }
      LOG(INFO) << "erase sender task state:"
                << sendTaskTypeString(taskPtr->_state)
                << " slots:" << bitsetStrEncode(taskPtr->_slots)
                << " taskid:" << taskPtr->_taskid;
      _migrateSlots ^= taskPtr->_slots;
      /* NOTE(wayenchen) before task erase ,insert the info into PtaskstateMap,
       * get it when called selslot taskinfo*/
      it = _migrateSendTaskMap.erase(it);
      // TODO(wayenchen) make it safe if others change the count of shared_ptr
      /* erase father Task if all small task was finished or erased */
      if (_migratePtaskMap.size() > 0) {
        for (auto it = _migratePtaskMap.begin();
             it != _migratePtaskMap.end();) {
          if (it->second.use_count() == 1) {
            it = _migratePtaskMap.erase(it);
          } else {
            ++it;
          }
        }
      }


      continue;
    } else if (taskPtr->_state == MigrateSendState::HALF) {
      // middle state
      // check if metadata change
      LOG(INFO) << "task:" << taskPtr->_taskid
                << "middle state, check if metadata change";
      if (taskPtr->_sender->checkSlotsBlongDst()) {
        taskPtr->_sender->setSenderStatus(MigrateSenderStatus::METACHANGE_DONE);
        taskPtr->_sender->unlockChunks();
        taskPtr->_state = MigrateSendState::CLEAR;
      } else {
        // if not change after node timeout, mark fail
        taskPtr->_sender->unlockChunks();
        taskPtr->_state = MigrateSendState::ERR;
      }
      ++it;
    }
  }
  return doSth;
}

bool MigrateManager::slotInTask(uint32_t slot) {
  std::lock_guard<myMutex> lk(_mutex);
  if (_migrateSlots.test(slot) || _importSlots.test(slot)) {
    return true;
  }
  return false;
}

bool MigrateManager::slotsInTask(const SlotsBitmap& bitMap) {
  std::lock_guard<myMutex> lk(_mutex);
  size_t idx = 0;
  while (idx < bitMap.size()) {
    if (bitMap.test(idx)) {
      if (_migrateSlots.test(idx) || _importSlots.test(idx)) {
        return true;
      }
    }
    ++idx;
  }
  return false;
}

std::string MigrateSendTask::toString() {
  std::lock_guard<std::mutex> lk(_mutex);
  std::stringstream ss;
  int64_t snapshotTime = -1;
  int64_t binlogTime = -1;
  auto snapStart = _sender->getSnapShotStartTime();

  if (sendTaskTypeString(_state) == "RUNNING") {
    auto senderState = _sender->getSenderState();
    if (senderState == MigrateSenderStatus::SNAPSHOT_BEGIN) {
      snapshotTime = snapStart > 0 ? msSinceEpoch() - snapStart : -1;
      ss << "SNAPSHOT SENDING";
    } else if (senderState == MigrateSenderStatus::SNAPSHOT_DONE) {
      ss << "BINLOG SENDING";
    } else if (senderState == MigrateSenderStatus::BINLOG_DONE) {
      ss << "BINLOGDONE";
    } else if (senderState == MigrateSenderStatus::LASTBINLOG_DONE) {
      ss << "LASTBINGLOG DONE";
    } else if (senderState == MigrateSenderStatus::METACHANGE_DONE) {
      ss << "GOSSIP DATA CHANGED";
    }
  } else {
    ss << "NONE";
  }

  auto snapshotEnd = _sender->getSnapShotEndTime();
  if (snapshotTime == -1 && snapshotEnd > 0) {
    snapshotTime = snapshotEnd - snapStart;
  }
  auto binlogEnd = _sender->getBinlogEndTime();

  if (snapStart > 0 && snapshotEnd > 0) {
    binlogTime =
      binlogEnd > 0 ? binlogEnd - snapshotEnd : msSinceEpoch() - snapshotEnd;
  }

  std::string runningState = ss.str();
  std::string slotMap = bitsetStrEncode(_slots);
  std::stringstream ss2;

  uint64_t startLockTime = _sender->getLockStartTime();
  uint64_t endLockTime = _sender->getLockEndTime();
  int64_t lockTime;
  if (endLockTime > 0) {
    lockTime = endLockTime - startLockTime;
  } else {
    lockTime = startLockTime > 0 ? msSinceEpoch() - startLockTime : -1;
  }

  uint64_t startTime = _sender->getTaskStartTime();
  int64_t taskTime = startTime > 0 ? msSinceEpoch() - startTime : -1;
  auto beginTime = startTime > 0 ? _sender->getStartTime() : "-1";
  if (startTime > 0) {
    beginTime.erase(beginTime.end() - 1);
  }
  ss2 << "taskid:" + _taskid << "\n"
      << "beginTime: " << beginTime << "\n"
      << "runTime: " << taskTime << "ms \n"
      << "snapShotTime: " << snapshotTime << "ms \n"
      << "snapshotKeys: " << _sender->getSnapshotNum() << "\n"
      << "binlogTime: " << binlogTime << "ms \n"
      << "binlogNum:" << _sender->getBinlogNum() << "\n"
      << "lockTime: " << lockTime << "ms \n"
      << "binlogDelay: " << _sender->getBinlogDelay() << "ms \n"
      << "State:" << sendTaskTypeString(_state) << "\n"
      << "RunningState: " << runningState << "\n"
      << "slots: " << bitsetStrEncode(_slots);
  return ss2.str();
}

std::string MigrateReceiveTask::toString() {
  std::lock_guard<std::mutex> lk(_mutex);
  std::stringstream ss1;
  int64_t snapshotTime = -1;
  int64_t binlogTime = -1;
  uint64_t snapStart = _receiver->getSnapShotStartTime();
  uint64_t startTime = _receiver->getTaskStartTime();
  auto taskTime = startTime > 0 ? msSinceEpoch() - startTime : 0;
  int64_t snapshotEnd = _receiver->getSnapShotEndTime();

  if (_state == MigrateReceiveState::RECEIVE_SNAPSHOT) {
    snapshotTime =
      startTime > 0 ? msSinceEpoch() - _receiver->getSnapShotStartTime() : -1;
    ss1 << "SNAPSHOT RECEIVING:" << _receiver->getSnapshotNum()
        << " runtime:" << snapshotTime << "ms";
  } else if (_state == MigrateReceiveState::RECEIVE_BINLOG) {
    binlogTime = msSinceEpoch() - snapshotEnd;
    ss1 << "BINLOG RECEVING runtime:" << binlogTime << "ms";
  } else {
    ss1 << "NONE";
  }

  if (snapshotEnd > 0) {
    snapshotTime = snapshotEnd - snapStart;
  }
  uint64_t binlogEnd = _receiver->getBinlogEndTime();
  if (binlogEnd > 0) {
    binlogTime = binlogEnd - snapshotEnd;
  }

  std::string runningState = ss1.str();
  std::string taskState = receTaskTypeString(_state);
  auto beginTime = snapshotTime > 0 ? _receiver->getStartTime() : "-1";
  if (startTime > 0) {
    beginTime.erase(beginTime.end() - 1);
  }
  std::stringstream ss2;

  ss2 << "taskid:" + _taskid << "\n"
      << "beginTime: " + beginTime << "\n"
      << "runTime: " << taskTime << "ms \n"
      << "snapShotTime: " << snapshotTime << "ms \n"
      << "snapshotKeys: " << _receiver->getSnapshotNum() << "\n"
      << "binlogTime: " << binlogTime << "ms \n"
      << "State: " << taskState << "\n"
      << "RunningState: " << runningState << "\n"
      << "slots: " << bitsetStrEncode(_slots);

  return ss2.str();
}

void MigrateSendTask::sendSlots() {
  auto s = _sender->sendChunk();
  _sender->setClient(nullptr);
  _sender->freeDbLock();
  SCLOCK::time_point nextSched;

  std::lock_guard<std::mutex> lk(_mutex);
  if (!s.ok()) {
    if (_sender->needToWaitMetaChanged()) {
      /* middle state, wait for  half node timeout to change */
      // TODO(wayenchen) check it for 1s one time
      _state = MigrateSendState::HALF;
      auto delayTime = _svr->getParams()->clusterNodeTimeout / 2 + 1000;
      nextSched = SCLOCK::now() + std::chrono::milliseconds(delayTime);
    } else if (_sender->getSnapshotNum() == 0 && _sender->isRunning()) {
      LOG(ERROR) << "send snap shot num zero, need retry"
                 << bitsetStrEncode(_sender->getSlots()) << "taskid:" << _taskid
                 << "error str:" << s.toString();
      _state = MigrateSendState::WAIT;
      _sender->stop();
      nextSched = SCLOCK::now() + std::chrono::milliseconds(100);
    } else {
      _state = MigrateSendState::ERR;
      nextSched = SCLOCK::now();
      LOG(ERROR) << "Send slots failed, bitmap is:"
                 << bitsetStrEncode(_sender->getSlots()) << _taskid << _taskid
                 << "error str:" << s.toString();
    }
  } else {
    nextSched = SCLOCK::now();
    _state = MigrateSendState::CLEAR;
  }
  _nextSchedTime = nextSched;
  _isRunning = false;
}

void MigrateSendTask::deleteSenderChunks() {
  /* NOTE(wayenchen) check if chunk not belong to me，
  make sure MOVE work well before delete */
  if (!_sender->checkSlotsBlongDst()) {
    LOG(ERROR) << "slots not belongs to dstNodes on task:"
               << bitsetStrEncode(_slots);
    _state = MigrateSendState::ERR;
  } else {
    auto s = _svr->getGcMgr()->deleteSlotsData(_slots, _storeid);
    std::lock_guard<std::mutex> lk(_mutex);
    if (!s.ok()) {
      LOG(ERROR) << "sender task delete chunk fail on store:" << _storeid
                 << "slots:" << bitsetStrEncode(_slots) << s.toString();
      /* NOTE(wayenchen) if delete fail, no need retry,
       * gcMgr will delete at last*/
      _state = MigrateSendState::ERR;
    } else {
      _sender->setSenderStatus(MigrateSenderStatus::DEL_DONE);
      _state = MigrateSendState::SUCC;
    }
  }
  _nextSchedTime = SCLOCK::now();
  _isRunning = false;
}

Status MigrateManager::migrating(const SlotsBitmap& slots,
                                 const string& ip,
                                 uint16_t port,
                                 uint32_t storeid,
                                 const std::string& taskid,
                                 const std::shared_ptr<pTask> ptaskPtr) {
  std::lock_guard<myMutex> lk(_mutex);
  size_t idx = 0;
  while (idx < slots.size()) {
    if (slots.test(idx)) {
      if (_migrateSlots.test(idx)) {
        LOG(ERROR) << "slot:" << idx
                   << "already be migrating, slots:" << _migrateSlots;
        return {ErrorCodes::ERR_INTERNAL, "already be migrating"};
      } else {
        _migrateSlots.set(idx);
      }
    }
    idx++;
  }

  auto sendTask = std::make_unique<MigrateSendTask>(
    storeid, taskid, slots, _svr, _cfg, ptaskPtr);
  sendTask->_nextSchedTime = SCLOCK::now();
  /* askid should be unique*/
  if (_migrateSendTaskMap.find(taskid) != _migrateSendTaskMap.end()) {
    LOG(ERROR) << "sender taskid:" << taskid << "already exists";
    return {ErrorCodes::ERR_INTERNAL, "taskid already exists"};
  }
  sendTask->_sender->setStoreid(storeid);
  sendTask->_sender->setDstAddr(ip, port);
  _migrateSendTaskMap.insert(std::make_pair(taskid, std::move(sendTask)));

  LOG(INFO) << "sender task start on slots: " << bitsetStrEncode(slots)
            << " task id is: " << taskid;
  return {ErrorCodes::ERR_OK, ""};
}

// judge if largeMap contain all slots in smallMap
bool MigrateManager::containSlot(const SlotsBitmap& smallMap,
                                 const SlotsBitmap& largeMap) {
  if (smallMap.size() != largeMap.size()) {
    return false;
  }
  size_t idx = 0;
  while (idx < largeMap.size()) {
    if (smallMap.test(idx)) {
      bool s = largeMap.test(idx);
      if (!s) {
        return false;
      }
    }
    idx++;
  }
  return true;
}

void MigrateManager::requestRateLimit(uint64_t bytes) {
  /* *
   * Set migration rate limit periodically
   */
  _rateLimiter->SetBytesPerSecond((uint64_t)_cfg->migrateRateLimitMB * 1024 *
                                  1024);
  _rateLimiter->Request(bytes);
}

bool MigrateManager::checkSlotOK(const SlotsBitmap& bitMap,
                                 const std::string& nodeid,
                                 std::vector<uint32_t>* taskSlots) {
  CNodePtr dstNode = _cluster->clusterLookupNode(nodeid);
  CNodePtr myself = _cluster->getMyselfNode();
  if (dstNode == nullptr) {
    LOG(ERROR) << "can not find dstNode:" << nodeid;
    return false;
  }
  size_t idx = 0;

  while (idx < bitMap.size()) {
    if (bitMap.test(idx)) {
      auto thisNode = _cluster->getNodeBySlot(idx);
      if (thisNode == dstNode) {
        LOG(ERROR) << "slot:" << idx << " has already been migrated to "
                   << "node:" << nodeid;
        return false;
      }
      if (thisNode != myself) {
        LOG(ERROR) << "slot:" << idx << " doesn't belong to myself";
        return false;
      }
      if (slotInTask(idx)) {
        LOG(ERROR) << "migrating task exists in slot:" << idx;
        return false;
      }
      (*taskSlots).push_back(idx);
    }
    idx++;
  }
  return true;
}

/* son taskid  */
std::string MigrateManager::genTaskid() {
  uint64_t taskNum = _taskIdGen.fetch_add(1);
  return std::to_string(taskNum);
}

/* father taskid contain importer Nodeid and automic number */
std::string MigrateManager::genPTaskid() {
  uint64_t pTaskNum = _pTaskIdGen.fetch_add(1);
  return _cluster->getMyselfName() + "-" + std::to_string(pTaskNum);
}

uint64_t MigrateManager::getAllTaskNum() {
  std::lock_guard<myMutex> lk(_mutex);
  return _migrateReceiveTaskMap.size() + _migrateSendTaskMap.size();
}

/* get migrate task job num of the same head Taskid */
uint64_t MigrateManager::getTaskNum(const std::string& pTaskid,
                                    bool noWait,
                                    bool noRunning) {
  std::lock_guard<myMutex> lk(_mutex);
  uint64_t taskNum = 0;
  /* can not both forbidden wait and running */
  if (!noWait && !noRunning) {
    return 0;
  }
  for (auto it = _migrateReceiveTaskMap.begin();
       it != _migrateReceiveTaskMap.end();
       it++) {
    if (it->first.find(pTaskid) != string::npos) {
      if (it->second->_receiver->getTaskStartTime() == 0) {
        // get waiting task num
        if (!noWait && noRunning) {
          taskNum++;
          continue;
        } else if (noWait && !noRunning) {
          continue;
        }
      }
      // get running task num
      if (!noRunning) {
        taskNum++;
      }
    }
  }

  for (auto it = _migrateSendTaskMap.begin(); it != _migrateSendTaskMap.end();
       it++) {
    if (it->first.find(pTaskid) != string::npos) {
      if (it->second->_state == MigrateSendState::WAIT) {
        if (!noWait && noRunning) {
          taskNum++;
          continue;
        } else if (noWait && !noRunning) {
          continue;
        }
      }
      if (!noRunning) {
        taskNum++;
      }
    }
  }
  return taskNum;
}

uint64_t MigrateManager::getWaitTaskNum(const std::string& pTaskid) {
  std::lock_guard<myMutex> lk(_mutex);
  uint64_t taskNum = 0;
  for (auto it = _migrateReceiveTaskMap.begin();
       it != _migrateReceiveTaskMap.end();) {
    if (it->first.find(pTaskid) != string::npos) {
      if (it->second->_receiver->getTaskStartTime() == 0) {
        taskNum++;
        LOG(INFO) << "receiver wait tasknum is:" << taskNum;
        it++;
        continue;
      }
    }
    it++;
  }
  for (auto it = _migrateSendTaskMap.begin();
       it != _migrateSendTaskMap.end();) {
    if (it->first.find(pTaskid) != string::npos) {
      if (it->second->_state == MigrateSendState::WAIT) {
        taskNum++;
        LOG(INFO) << "sender wait tasknum is:" << taskNum;
        it++;
        continue;
      }
    }
    it++;
  }
  LOG(INFO) << "wait task num is " << taskNum;
  return taskNum;
}

std::map<std::string, SlotsBitmap> MigrateManager::getStopMap() {
  std::lock_guard<myMutex> lk(_mutex);
  return _stopImportMap;
}

bool MigrateManager::existMigrateTask() {
  std::lock_guard<myMutex> lk(_mutex);
  if (_migrateSendTaskMap.size() == 0 && _migrateReceiveTaskMap.size() == 0) {
    return false;
  }
  return true;
}

bool MigrateManager::existPtask(const std::string& taskid) {
  std::lock_guard<myMutex> lk(_mutex);
  if (_importPtaskMap.find(taskid) != _importPtaskMap.end() ||
      _migratePtaskMap.find(taskid) != _migratePtaskMap.end()) {
    return true;
  }
  return false;
}

SlotsBitmap convertMap(const std::vector<uint32_t>& vec) {
  SlotsBitmap map;
  for (const auto& vs : vec) {
    map.set(vs);
  }
  return map;
}

void MigrateManager::dstPrepareMigrate(asio::ip::tcp::socket sock,
                                       const std::string& bitmap,
                                       const std::string& nodeid,
                                       const std::string& pTaskid,
                                       uint32_t storeNum) {
  std::lock_guard<myMutex> lk(_mutex);
  std::shared_ptr<BlockingTcpClient> client =
    std::move(_svr->getNetwork()->createBlockingClient(std::move(sock),
                                                       64 * 1024 * 1024));

  INVARIANT_D(client != nullptr);

  SlotsBitmap taskMap;
  // send json message to receiver
  rapidjson::StringBuffer sb;
  rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
  writer.StartObject();
  // if no error, value is "+OK"

  writer.Key("errMsg");
  try {
    taskMap = std::bitset<CLUSTER_SLOTS>(bitmap);
  } catch (const std::exception& ex) {
    std::stringstream ss;
    ss << "-ERR parse bitmap failed:" << ex.what();
    LOG(ERROR) << ss.str();
    writer.String("-ERR parse");
    writer.EndObject();
    client->writeLine(sb.GetString());
    return;
  }

  auto dstNode = _cluster->clusterLookupNode(nodeid);
  if (!dstNode) {
    LOG(ERROR) << "import node" << nodeid << "not find";
    writer.String("-ERR node not found");
    writer.EndObject();
    client->writeLine(sb.GetString());
    return;
  }
  auto ip = dstNode->getNodeIp();
  auto port = dstNode->getPort();

  // check slots
  std::vector<uint32_t> taskSlots;
  if (!checkSlotOK(taskMap, nodeid, &taskSlots) || !_cluster->clusterIsOK()) {
    std::stringstream ss;
    for (auto& vs : taskSlots) {
      ss << vs << " ";
    }
    LOG(ERROR) << "sender prepare fail when check slots :" << ss.str();
    writer.String("-ERR check");
    writer.EndObject();
    client->writeLine(sb.GetString());
    return;
  }
  // check kvstore count
  uint32_t mystoreNum = _svr->getKVStoreCount();
  if (mystoreNum != storeNum) {
    LOG(ERROR) << "my storenum:" << mystoreNum
               << "is not equal to:" << storeNum;
    writer.String("-ERR store count");
    writer.EndObject();
    client->writeLine(sb.GetString());
    return;
  }
  writer.String("+OK");
  // split slots and start task

  std::unordered_map<uint32_t, std::vector<uint32_t>> slotSet;
  for (const auto& slot : taskSlots) {
    uint32_t storeid = _svr->getSegmentMgr()->getStoreid(slot);
    if ((slotSet.find(storeid)) != slotSet.end()) {
      slotSet[storeid].push_back(slot);
    } else {
      std::vector<uint32_t> temp;
      temp.push_back(slot);
      slotSet.insert(std::make_pair(storeid, temp));
    }
  }

  bool startMigate = true;
  uint32_t taskSize = _svr->getParams()->migrateTaskSlotsLimit;
  writer.Key("taskinfo");
  writer.StartArray();

  auto ptaskPtr = std::make_shared<pTask>(pTaskid, nodeid);
  for (const auto& v : slotSet) {
    uint32_t storeid = v.first;
    auto slotsVec = v.second;

    uint32_t slotsSize = slotsVec.size();
    for (size_t i = 0; i < slotsSize; i += taskSize) {
      std::vector<uint32_t> vecTask;
      auto last = std::min(slotsVec.size(), i + taskSize);
      vecTask.insert(
        vecTask.end(), slotsVec.begin() + i, slotsVec.begin() + last);

      writer.StartObject();
      writer.Key("storeid");
      writer.Uint64(storeid);

      std::string myid = genTaskid();
      writer.Key("taskid");
      writer.String(myid);

      writer.Key("migrateSlots");
      writer.StartArray();
      for (auto& v : vecTask)
        writer.Uint64(v);
      writer.EndArray();

      writer.EndObject();
      // migrate task should start before send task information to
      // receiver
      auto s = startTask(
        convertMap(vecTask), ip, port, pTaskid + "-" + myid, storeid, ptaskPtr);
      if (!s.ok()) {
        LOG(ERROR) << "start task on store:" << storeid << "fail";
        startMigate = false;
        break;
      }
      for (const auto& vs : vecTask) {
        _migrateNodes[vs] = nodeid;
      }
    }
  }
  writer.EndArray();

  // if start all task, value is "+OK"
  writer.Key("finishMsg");
  if (!startMigate) {
    writer.String("-ERR");
  } else {
    writer.String("+OK");
  }

  writer.EndObject();
  // send slots and storeid back
  Status s = client->writeLine(sb.GetString());
  if (!s.ok()) {
    LOG(ERROR) << "preparemigrate send json failed:" << s.toString();
    return;
  }
  addMigratePTask(ptaskPtr);
}

void MigrateManager::dstReadyMigrate(asio::ip::tcp::socket sock,
                                     const std::string& slotsArg,
                                     const std::string& StoreidArg,
                                     const std::string& nodeidArg,
                                     const std::string& taskidArg) {
  std::shared_ptr<BlockingTcpClient> client =
    std::move(_svr->getNetwork()->createBlockingClient(std::move(sock),
                                                       64 * 1024 * 1024));
  if (client == nullptr) {
    LOG(ERROR) << "sender ready with:" << nodeidArg
               << " failed, no valid client";
    return;
  }
  SlotsBitmap receiveMap;
  uint32_t dstStoreid;
  try {
    receiveMap = std::bitset<CLUSTER_SLOTS>(slotsArg);
    dstStoreid = std::stoul(StoreidArg);
  } catch (const std::exception& ex) {
    std::stringstream ss;
    ss << "-ERR parse opts failed:" << ex.what();
    LOG(ERROR) << ss.str();
    client->writeLine(ss.str());
    return;
  }
  auto dstNode = _cluster->clusterLookupNode(nodeidArg);
  if (!dstNode) {
    LOG(ERROR) << "import node" << nodeidArg << "not find";
    std::stringstream ss;
    ss << "-ERR can not find dst node: invalid nodeid";
    client->writeLine(ss.str());
    return;
  }
  std::lock_guard<myMutex> lk(_mutex);
  /* find the right task by taskid, if not found, return error */
  if (_migrateSendTaskMap.find(taskidArg) != _migrateSendTaskMap.end()) {
    // send response to srcNode
    std::stringstream ss;
    ss << "+OK";
    client->writeLine(ss.str());
    _migrateSendTaskMap[taskidArg]->_sender->setClient(client);
    _migrateSendTaskMap[taskidArg]->_sender->setDstNode(nodeidArg);
    _migrateSendTaskMap[taskidArg]->_sender->setDstStoreid(dstStoreid);
    _migrateSendTaskMap[taskidArg]->_sender->start();
    _migrateSendTaskMap[taskidArg]->_state = MigrateSendState::START;
    LOG(INFO) << "sender task marked start on taskid:" << taskidArg;
  } else {
    LOG(ERROR) << "findJob failed, taskid:" << taskidArg
               << " receiveMap:" << bitsetStrEncode(receiveMap);

    std::stringstream ss;
    ss << "-ERR not be migrating: invalid taskid";
    client->writeLine(ss.str());
  }

  return;
}

////////////////////////////////////
// receiver POV
///////////////////////////////////
bool MigrateManager::receiverSchedule(const SCLOCK::time_point& now) {
  bool doSth = false;
  for (auto it = _migrateReceiveTaskMap.begin();
       it != _migrateReceiveTaskMap.end();) {
    auto taskPtr = (*it).second.get();
    if ((taskPtr)->_isRunning || now < (taskPtr)->_nextSchedTime) {
      ++it;
      continue;
    }

    doSth = true;
    if (taskPtr->_state == MigrateReceiveState::RECEIVE_SNAPSHOT) {
      taskPtr->_isRunning = true;
      taskPtr->_receiver->start();
      _migrateReceiver->schedule(
        [this, iter = taskPtr]() { iter->fullReceive(); });
      ++it;
    } else if (taskPtr->_state == MigrateReceiveState::RECEIVE_BINLOG) {
      taskPtr->_isRunning = true;
      taskPtr->_receiver->start();
      taskPtr->checkMigrateStatus();
      ++it;
    } else if (taskPtr->_state == MigrateReceiveState::SUCC ||
               taskPtr->_state == MigrateReceiveState::ERR) {
      std::string slot = bitsetStrEncode(taskPtr->_slots);
      for (size_t i = 0; i < CLUSTER_SLOTS; i++) {
        if (taskPtr->_slots.test(i)) {
          if (taskPtr->_state == MigrateReceiveState::SUCC) {
            _succImportSlots.set(i);
            if (_failImportSlots.test(i)) {
              _failImportSlots.reset(i);
            }
          } else {
            LOG(INFO) << "_migrateReceiveTask ERR, erase it, slots" << i;
            _failImportSlots.set(i);
          }
          _importNodes.erase(i);
        }
      }
      if (taskPtr->_state == MigrateReceiveState::SUCC) {
        _succReceTask.push_back(slot);
        taskPtr->_pTask->_succStateMap[taskPtr->_taskid] = taskPtr->toString();
      } else {
        /*NOTE(wayenchen) delete Receiver dirty data in gc*/
        auto s =
          _svr->getGcMgr()->deleteSlotsData(taskPtr->_slots, taskPtr->_storeid);
        if (!s.ok()) {
          // no need retry, gc will do it again
          LOG(ERROR) << "receiver task delete chunk fail" << s.toString();
        }
        _failReceTask.push_back(slot);
        taskPtr->_pTask->_failStateMap[taskPtr->_taskid] = taskPtr->toString();
      }
      // taskPtr->_pTask->addDoneTaskNum();
      LOG(INFO) << "erase receiver task stat:"
                << receTaskTypeString(taskPtr->_state)
                << " slots:" << bitsetStrEncode(taskPtr->_slots)
                << " taskid:" << taskPtr->_taskid;
      _importSlots ^= (taskPtr->_slots);
      it = _migrateReceiveTaskMap.erase(it);
      /* erase father Task if all small task was finished or erased */
      if (_importPtaskMap.size() > 0) {
        for (auto it = _importPtaskMap.begin(); it != _importPtaskMap.end();) {
          if ((*it).second.use_count() == 1) {
            it = _importPtaskMap.erase(it);
          } else {
            ++it;
          }
        }
      }

      continue;
    }
  }
  return doSth;
}

Status MigrateManager::importing(const SlotsBitmap& slots,
                                 const std::string& ip,
                                 uint16_t port,
                                 uint32_t storeid,
                                 const std::string& taskid,
                                 const std::shared_ptr<pTask> ptaskPtr) {
  std::lock_guard<myMutex> lk(_mutex);
  std::size_t idx = 0;
  // set slot flag
  while (idx < slots.size()) {
    if (slots.test(idx)) {
      if (_importSlots.test(idx)) {
        LOG(ERROR) << "slot:" << idx << "already be importing" << _importSlots;
        return {ErrorCodes::ERR_INTERNAL, "already be importing"};
      } else {
        _importSlots.set(idx);
      }
    }
    idx++;
  }
  auto receiveTask = std::make_unique<MigrateReceiveTask>(
    slots, storeid, taskid, ip, port, _svr, _cfg, ptaskPtr);
  receiveTask->_nextSchedTime = SCLOCK::now();
  /* taskid should be unique*/
  if (_migrateReceiveTaskMap.find(taskid) != _migrateReceiveTaskMap.end()) {
    LOG(ERROR) << "receiver taskid:" << taskid << "already exists";
    return {ErrorCodes::ERR_INTERNAL, "taskid already exists"};
  }
  _migrateReceiveTaskMap.insert(std::make_pair(taskid, std::move(receiveTask)));

  LOG(INFO) << "receiver task start on slots:" << bitsetStrEncode(slots)
            << "task id is:" << taskid;

  return {ErrorCodes::ERR_OK, ""};
}

Status MigrateManager::startTask(const SlotsBitmap& taskmap,
                                 const std::string& ip,
                                 uint16_t port,
                                 const std::string& taskid,
                                 uint32_t storeid,
                                 const std::shared_ptr<pTask> ptaskPtr,
                                 bool importFlag) {
  Status s;
  if (importFlag) {
    s = importing(taskmap, ip, port, storeid, taskid, ptaskPtr);
    if (!s.ok()) {
      LOG(ERROR) << "start task fail on store:" << storeid;
      return s;
    }
  } else {
    s = migrating(taskmap, ip, port, storeid, taskid, ptaskPtr);
    if (!s.ok()) {
      LOG(ERROR) << "start task fail on store:" << storeid;
      return s;
    }
  }
  ptaskPtr->addTaskNum();
  return {ErrorCodes::ERR_OK, ""};
}

void MigrateManager::insertNodes(const SlotsBitmap& slots,
                                 const std::string& nodeid,
                                 bool importFlag) {
  std::lock_guard<myMutex> lk(_mutex);
  size_t idx = 0;
  while (idx < CLUSTER_SLOTS) {
    if (slots.test(idx)) {
      if (importFlag) {
        _importNodes[idx] = nodeid;
      } else {
        _migrateNodes[idx] = nodeid;
      }
    }
    idx++;
  }
}

std::string MigrateManager::getNodeIdBySlot(uint32_t slot, bool importFlag) {
  std::lock_guard<myMutex> lk(_mutex);
  if (importFlag) {
    return _importNodes[slot];
  } else {
    return _migrateNodes[slot];
  }
}

void MigrateManager::addImportPTask(std::shared_ptr<pTask> task) {
  std::lock_guard<myMutex> lk(_mutex);
  std::string taskid = task->_taskid;
  _importPtaskMap.insert(std::make_pair(taskid, task));
  auto timeStr = timePointRepr(SCLOCK::now());
  timeStr.erase(timeStr.end() - 1);
  task->_startTime = timeStr;
  task->_migrateTime = sinceEpoch();
}

void MigrateManager::addMigratePTask(std::shared_ptr<pTask> task) {
  std::lock_guard<myMutex> lk(_mutex);
  std::string taskid = task->_taskid;
  _migratePtaskMap.insert(std::make_pair(taskid, task));
  auto timeStr = timePointRepr(SCLOCK::now());
  timeStr.erase(timeStr.end() - 1);
  task->_startTime = timeStr;
  task->_migrateTime = sinceEpoch();
}

void MigrateReceiveTask::checkMigrateStatus() {
  // TODO(takenliu) : if connect break when receive binlog, reconnect and
  // continue receive.
  std::lock_guard<std::mutex> lk(_mutex);
  SCLOCK::time_point nextSched = SCLOCK::now() + std::chrono::seconds(1);
  _nextSchedTime = nextSched;
  _isRunning = false;
  auto delay = sinceEpoch() - _lastSyncTime;
  /* NOTE(wayenchen):sendbinlog beatheat interval is set to 6s,
      so mark 20s as no heartbeat for more than three times*/
  if (delay > 20 * MigrateManager::SEND_RETRY_CNT || !_receiver->isRunning()) {
    LOG(ERROR) << "receiver task receive binlog timeout"
               << " on slots:" << bitsetStrEncode(_slots);
    _receiver->freeDbLock();
    _state = MigrateReceiveState::ERR;
  }
  return;
}

void MigrateReceiveTask::fullReceive() {
  // 2) require a blocking-clients
  LOG(INFO) << "full receive remote_addr(" << _srcIp << ":" << _srcPort
            << ") on slots:" << bitsetStrEncode(_slots);
  std::shared_ptr<BlockingTcpClient> client =
    std::move(createClient(_srcIp, _srcPort, _svr));

  if (client == nullptr) {
    LOG(ERROR) << "fullReceive with: " << _srcIp << ":" << _srcPort
               << " failed, no valid client";
    _nextSchedTime = SCLOCK::now();
    _isRunning = false;
    _state = MigrateReceiveState::ERR;
    return;
  }
  _receiver->setClient(client);

  uint32_t storeid = _receiver->getsStoreid();
  /*
   * NOTE: It can't getDb using LocalSession here. Because the IX LOCK
   * would be held all the whole receive process. But LocalSession()
   * should be destructed soon.
   */
  auto expdb =
    _svr->getSegmentMgr()->getDb(nullptr, storeid, mgl::LockMode::LOCK_IX);
  if (!expdb.ok()) {
    LOG(ERROR) << "get store:" << storeid
               << " failed: " << expdb.status().toString();
    _nextSchedTime = SCLOCK::now();
    _isRunning = false;
    _state = MigrateReceiveState::ERR;
    return;
  }
  _receiver->setDbWithLock(
    std::make_unique<DbWithLock>(std::move(expdb.value())));

  Status s = _receiver->receiveSnapshot();
  std::lock_guard<std::mutex> lk(_mutex);
  if (!s.ok()) {
    /*NOTE(wayenchen) if srcNode not Fail
    and my receiver snapshot key num is zero,
    retry for three times*/
    bool srcNodeFail =
      _svr->getClusterMgr()->getClusterState()->clusterNodeFailed(
        _pTask->_nodeid);
    auto retryCnt = _svr->getParams()->snapShotRetryCnt;
    if (_receiver->getSnapshotNum() == 0 && !srcNodeFail &&
        getRetryCount() <= retryCnt && _receiver->isRunning() &&
        s.code() != ErrorCodes::ERR_MIGRATE) {
      auto delayTime = 1000 + redis_port::random() % 5000;
      _nextSchedTime = SCLOCK::now() + std::chrono::milliseconds(delayTime);
      _state = MigrateReceiveState::RECEIVE_SNAPSHOT;
      _retryTime.fetch_add(1, memory_order_relaxed);
      LOG(ERROR) << "receiveSnapshot need retry" << bitsetStrEncode(_slots)
                 << "taskid:" << _taskid << "error str:" << s.toString();
    } else {
      LOG(ERROR) << "receiveSnapshot failed:" << bitsetStrEncode(_slots)
                 << "taskid:" << _taskid << "error str:" << s.toString();
      // TODO(takenliu) : clear task, and delete the kv of the chunk.
      _nextSchedTime = SCLOCK::now();
      _state = MigrateReceiveState::ERR;
    }
    _isRunning = false;
    _receiver->freeDbLock();
    _receiver->stop();
    return;
  }

  {
    _receiver->setClient(nullptr);
    SCLOCK::time_point nextSched = SCLOCK::now();
    _state = MigrateReceiveState::RECEIVE_BINLOG;
    _nextSchedTime = nextSched;
    _lastSyncTime = sinceEpoch();
    _isRunning = false;
    _receiver->stop();
  }
  // add client to commands schedule
  NetworkAsio* network = _svr->getNetwork();
  INVARIANT(network != nullptr);
  bool migrateOnly = true;
  Expected<uint64_t> expSessionId =
    network->client2Session(std::move(client), migrateOnly);
  if (!expSessionId.ok()) {
    LOG(ERROR) << "client2Session failed:" << expSessionId.status().toString();
    return;
  }
}

Status MigrateManager::applyRepllog(Session* sess,
                                    uint32_t storeid,
                                    BinlogApplyMode mode,
                                    const std::string& logKey,
                                    const std::string& logValue) {
  if (logKey == "") {
    if (logValue != "") {
      /* NOTE(wayenchen) find the binlog task by taskid in migrate
       * heartbeat,
       * set the lastSyncTime of receiver in order to judge if heartbeat
       * timeout*/
      std::lock_guard<myMutex> lk(_mutex);
      auto iter = _migrateReceiveTaskMap.find(logValue);
      if (iter != _migrateReceiveTaskMap.end()) {
        iter->second->_lastSyncTime = sinceEpoch();
      } else {
        LOG(ERROR) << "migrate heartbeat taskid:" << logValue
                   << "not find, may be erase";
      }
    }
  } else {
    auto value = ReplLogValueV2::decode(logValue);
    if (!value.ok()) {
      return value.status();
    }
    // NOTE(takenliu): use the keys chunkid to check
    if (!slotInTask(value.value().getChunkId())) {
      LOG(ERROR) << "applyBinlog chunkid err:" << value.value().getChunkId()
                 << "value:" << value.value().getData();
      return {ErrorCodes::ERR_INTERNAL, "chunk not be migrating"};
    }

    auto binlog = applySingleTxnV2(sess, storeid, logKey, logValue, mode);
    if (!binlog.ok()) {
      LOG(ERROR) << "applySingleTxnV2 failed:" << binlog.status().toString();
      return binlog.status();
    }
  }
  return {ErrorCodes::ERR_OK, ""};
}

Status MigrateManager::supplyMigrateEnd(const std::string& taskid,
                                        bool binlogDone) {
  SlotsBitmap slots;
  {
    std::lock_guard<myMutex> lk(_mutex);
    std::string taskResult = binlogDone ? "success" : "failed";

    auto iter = _migrateReceiveTaskMap.find(taskid);
    if (iter != _migrateReceiveTaskMap.end()) {
      slots = iter->second->_receiver->getSlots();
      SCLOCK::time_point nextSched = SCLOCK::now();
      iter->second->_nextSchedTime = nextSched;
      iter->second->_isRunning = false;
      iter->second->_receiver->stop();
      if (!binlogDone) {
        iter->second->_state = MigrateReceiveState::ERR;
        return {ErrorCodes::ERR_OK, ""};
      }
      iter->second->_receiver->setBinlogEndTime(msSinceEpoch());
      iter->second->_state = MigrateReceiveState::SUCC;
      LOG(INFO) << "supplyMigrateEnd finished on slots:"
                << bitsetStrEncode(slots) << " taskid: " << taskid
                << " task result is:" << taskResult;
    } else {
      LOG(ERROR) << "supplyMigrateEnd find task failed,"
                 << " slots:" << bitsetStrEncode(slots) << " taskid:" << taskid
                 << " task result is:" << taskResult;

      return {ErrorCodes::ERR_INTERNAL, "migrating task not find"};
    }
  }

  /* update gossip message and save*/
  auto clusterState = _svr->getClusterMgr()->getClusterState();
  auto s = clusterState->setSlots(clusterState->getMyselfNode(), slots);
  if (!s.ok()) {
    LOG(ERROR) << "setSlots failed, slots:" << bitsetStrEncode(slots)
               << " err:" << s.toString();
    return {ErrorCodes::ERR_CLUSTER, "set slot myself fail"};
  }

  clusterState->setTodoFlag(CLUSTER_TODO_FLAG_SAVE);
  clusterState->clusterUpdateState();
  return {ErrorCodes::ERR_OK, ""};
}

uint64_t MigrateManager::getProtectBinlogid(uint32_t storeid) {
  std::lock_guard<myMutex> lk(_mutex);
  uint64_t minbinlogid = UINT64_MAX;
  for (auto it = _migrateSendTaskMap.begin(); it != _migrateSendTaskMap.end();
       ++it) {
    uint64_t binlogid = (*it).second->_sender->getProtectBinlogid();
    if ((*it).second->_storeid == storeid && binlogid < minbinlogid) {
      minbinlogid = binlogid;
    }
  }
  return minbinlogid;
}

Expected<std::string> MigrateManager::getMigrateInfoStr(
  const SlotsBitmap& slots) {
  std::stringstream stream1;
  std::stringstream stream2;
  std::lock_guard<myMutex> lk(_mutex);
  for (auto iter = _migrateNodes.begin(); iter != _migrateNodes.end(); ++iter) {
    if (slots.test(iter->first)) {
      stream1 << "[" << iter->first << "->-" << iter->second << "]"
              << " ";
    }
  }
  for (auto iter = _importNodes.begin(); iter != _importNodes.end(); ++iter) {
    stream2 << "[" << iter->first << "-<-" << iter->second << "]"
            << " ";
  }
  if (stream1.str().size() == 0 && stream2.str().size() == 0) {
    return {ErrorCodes::ERR_WRONG_TYPE, "no migrate or import slots"};
  }

  std::string importInfo = stream1.str();
  std::string migrateInfo = stream2.str();
  if (importInfo.empty() || migrateInfo.empty()) {
    return importInfo + migrateInfo;
  }
  return importInfo + " " + migrateInfo;
}

// pass cluster node slots to check
Expected<std::string> MigrateManager::getMigrateInfoStrSimple(
  const SlotsBitmap& slots) {
  std::stringstream stream1;
  std::stringstream stream2;
  std::unordered_map<std::string, std::bitset<CLUSTER_SLOTS>> importSlots;
  std::unordered_map<std::string, std::bitset<CLUSTER_SLOTS>> migrateSlots;

  {
    std::lock_guard<myMutex> lk(_mutex);
    std::size_t idx = 0;
    while (idx < CLUSTER_SLOTS) {
      std::string nodeid;
      auto iter = _migrateNodes.find(idx);
      if (slots.test(idx) && iter != _migrateNodes.end()) {
        nodeid = _migrateNodes[idx];
        migrateSlots[nodeid].set(idx);
      }
      auto iter2 = _importNodes.find(idx);
      if (iter2 != _importNodes.end()) {
        nodeid = _importNodes[idx];
        importSlots[nodeid].set(idx);
      }
      idx++;
    }
  }

  if (importSlots.empty() && migrateSlots.empty()) {
    return {ErrorCodes::ERR_WRONG_TYPE, "no migrate or import slots"};
  }
  if (importSlots.size()) {
    for (const auto& x : importSlots) {
      stream1 << "[" << bitsetStrEncode(x.second) << "-<-" << x.first << "]"
              << " ";
    }
  }
  if (migrateSlots.size()) {
    for (const auto& x : migrateSlots) {
      stream2 << "[" << bitsetStrEncode(x.second) << "->-" << x.first << "]"
              << " ";
    }
  }
  return stream1.str() + " " + stream2.str();
}

SlotsBitmap MigrateManager::getSteadySlots(const SlotsBitmap& slots) {
  std::lock_guard<myMutex> lk(_mutex);
  std::size_t idx = 0;
  SlotsBitmap tempSlots(slots);
  while (idx < CLUSTER_SLOTS) {
    if (tempSlots.test(idx) &&
        (_importSlots.test(idx) || _migrateSlots.test(idx))) {
      tempSlots.reset(idx);
    }
    idx++;
  }
  return tempSlots;
}

Expected<std::string> MigrateManager::getMigrateInfo() {
  std::lock_guard<myMutex> lk(_mutex);
  std::stringstream ss;
  auto commandLen = 0;
  uint16_t isMigrating = (_migrateSlots.count() || _succMigrateSlots.count() ||
                          _failMigrateSlots.count())
    ? 1
    : 0;
  uint16_t isImporting = (_importSlots.count() || _succImportSlots.count() ||
                          _failImportSlots.count())
    ? 1
    : 0;
  commandLen += (isMigrating * 7 + isImporting * 7);

  Command::fmtMultiBulkLen(ss, commandLen);

  if (isMigrating) {
    std::string migrateTaskStr = "migrating taskid:";
    for (auto& iter : _migratePtaskMap) {
      auto lastTime = sinceEpoch() - iter.second->_migrateTime;
      migrateTaskStr +=
        (iter.second->getTaskid() + " [" + iter.second->_startTime) +
        " migrateTime:" + std::to_string(lastTime) + "s] ";
    }
    Command::fmtBulk(ss, migrateTaskStr);
    std::string migrateSlots =
      "migrating slots:" + bitsetStrEncode(_migrateSlots);

    std::string succMSlots =
      "success migrate slots:" + bitsetStrEncode(_succMigrateSlots);

    std::string failMSlots =
      "fail migrate slots:" + bitsetStrEncode(_failMigrateSlots);

    std::string taskSizeInfo =
      "running sender task num:" + std::to_string(_migrateSendTaskMap.size());
    std::string succcInfo =
      "success sender task num:" + std::to_string(_succSenderTask.size());
    std::string failInfo =
      "fail sender task num:" + std::to_string(_failSenderTask.size());

    Command::fmtBulk(ss, migrateSlots);
    Command::fmtBulk(ss, succMSlots);
    Command::fmtBulk(ss, failMSlots);
    Command::fmtBulk(ss, taskSizeInfo);
    Command::fmtBulk(ss, succcInfo);
    Command::fmtBulk(ss, failInfo);
  }

  if (isImporting) {
    std::string migrateTaskStr = "importing taskid:";
    for (auto& iter : _importPtaskMap) {
      auto lastTime = sinceEpoch() - iter.second->_migrateTime;
      migrateTaskStr +=
        (iter.second->getTaskid() + " [" + iter.second->_startTime) +
        " migrateTime:" + std::to_string(lastTime) + "s] ";
    }
    Command::fmtBulk(ss, migrateTaskStr);
    std::string importSlots =
      "importing slots:" + bitsetStrEncode(_importSlots);

    std::string succISlots =
      "success import slots:" + bitsetStrEncode(_succImportSlots);

    std::string failISlots =
      "fail import slots:" + bitsetStrEncode(_failImportSlots);

    std::string taskSizeInfo2 = "running receiver task num:" +
      std::to_string(_migrateReceiveTaskMap.size());
    std::string succcInfo2 =
      "success receiver task num:" + std::to_string(_succReceTask.size());
    std::string failInfo2 =
      "fail receiver task num:" + std::to_string(_failReceTask.size());

    Command::fmtBulk(ss, importSlots);
    Command::fmtBulk(ss, succISlots);
    Command::fmtBulk(ss, failISlots);
    Command::fmtBulk(ss, taskSizeInfo2);
    Command::fmtBulk(ss, succcInfo2);
    Command::fmtBulk(ss, failInfo2);
  }

  if (ss.str().size() == 0) {
    return {ErrorCodes::ERR_WRONG_TYPE, "no task info"};
  }
  return ss.str();
}

/* get taskinfo , if noWait, ignore waiting tasking, if noRunning, ignore
 * running task*/
Expected<std::string> MigrateManager::getTaskInfo(bool noWait, bool noRunning) {
  std::stringstream ss;
  std::lock_guard<myMutex> lk(_mutex);
  Command::fmtMultiBulkLen(ss,
                           _importPtaskMap.size() + _migratePtaskMap.size());
  size_t taskNum = 0;
  if (!noWait && !noRunning) {
    return {ErrorCodes::ERR_WRONG_TYPE, "Task should be at last one type"};
  }
  for (auto& task : _migratePtaskMap) {
    std::string pTaskid = task.second->_taskid;
    Command::fmtMultiBulkLen(ss, getTaskNum(pTaskid, noWait, noRunning) + 1);
    Command::fmtBulk(ss, "PTASK ID:" + pTaskid);
    for (auto& iter : _migrateSendTaskMap) {
      if (noWait && iter.second->_state == MigrateSendState::WAIT) {
        continue;
      }
      if (noRunning && !noWait &&
          iter.second->_state != MigrateSendState::WAIT) {
        continue;
      }
      if (iter.first.find(pTaskid) != std::string::npos) {
        taskNum++;
        auto taskinfo = stringSplit(iter.second->toString(), "\n");
        Command::fmtMultiBulkLen(ss, taskinfo.size());
        for (auto& vs : taskinfo) {
          Command::fmtBulk(ss, vs);
        }
      }
    }
  }

  for (auto& task : _importPtaskMap) {
    std::string pTaskid = task.second->_taskid;
    Command::fmtMultiBulkLen(ss, getTaskNum(pTaskid, noWait, noRunning) + 1);
    Command::fmtBulk(ss, "PTASK ID:" + pTaskid);

    for (auto& iter : _migrateReceiveTaskMap) {
      if (noWait && iter.second->_receiver->getTaskStartTime() == 0) {
        continue;
      }
      if (noRunning && iter.second->_receiver->getTaskStartTime() != 0) {
        continue;
      }
      if (iter.first.find(pTaskid) != std::string::npos) {
        taskNum++;
        auto taskinfo = stringSplit(iter.second->toString(), "\n");
        Command::fmtMultiBulkLen(ss, taskinfo.size());
        for (auto& vs : taskinfo) {
          Command::fmtBulk(ss, vs);
        }
      }
    }
  }
  if (taskNum == 0) {
    return {ErrorCodes::ERR_WRONG_TYPE, "no task info"};
  }

  return ss.str();
}

/* get task info if state is succ or fail */
Expected<std::string> MigrateManager::getSuccFailInfo(bool succ) {
  std::stringstream ss;
  std::lock_guard<myMutex> lk(_mutex);
  uint64_t taskNum = 0;
  Command::fmtMultiBulkLen(ss,
                           _importPtaskMap.size() + _migratePtaskMap.size());

  std::map<std::string, std::string> tempMap;
  if (_migratePtaskMap.size() > 0) {
    for (auto& task : _migratePtaskMap) {
      if (succ) {
        tempMap = task.second->_succStateMap;
      } else {
        tempMap = task.second->_failStateMap;
      }
      Command::fmtMultiBulkLen(ss, tempMap.size() + 1);
      Command::fmtBulk(ss, "PTASK ID:" + task.second->_taskid);
      for (auto& iter : tempMap) {
        taskNum++;
        auto taskinfo = stringSplit(iter.second, "\n");
        Command::fmtMultiBulkLen(ss, taskinfo.size());
        for (auto& vs : taskinfo) {
          Command::fmtBulk(ss, vs);
        }
      }
    }
  }

  if (_importPtaskMap.size() > 0) {
    for (auto& task : _importPtaskMap) {
      if (succ) {
        tempMap = task.second->_succStateMap;
      } else {
        tempMap = task.second->_failStateMap;
      }
      Command::fmtMultiBulkLen(ss, tempMap.size() + 1);
      Command::fmtBulk(ss, "PTASK ID:" + task.second->_taskid);
      for (auto& iter : tempMap) {
        taskNum++;
        auto taskinfo = stringSplit(iter.second, "\n");
        Command::fmtMultiBulkLen(ss, taskinfo.size());
        for (auto& vs : taskinfo) {
          Command::fmtBulk(ss, vs);
        }
      }
    }
  }

  if (taskNum == 0) {
    return {ErrorCodes::ERR_NOTFOUND, "no task info"};
  }
  return ss.str();
}

/* get taskinfo of pTaskid, if noWait, ignore waiting tasking, if noRunning,
 * ignore running task*/
Expected<std::string> MigrateManager::getTaskInfo(const std::string& pTaskid,
                                                  bool noWait,
                                                  bool noRunning) {
  std::stringstream ss;
  std::lock_guard<myMutex> lk(_mutex);
  if (!noWait && !noRunning) {
    return {ErrorCodes::ERR_WRONG_TYPE, "Task should be at last one type"};
  }
  if (existPtask(pTaskid)) {
    Command::fmtMultiBulkLen(ss, getTaskNum(pTaskid, noWait, noRunning));

    for (auto& iter : _migrateSendTaskMap) {
      if (iter.first.find(pTaskid) != std::string::npos) {
        if (noWait && iter.second->_state == MigrateSendState::WAIT) {
          continue;
        }
        if (noRunning && iter.second->_state != MigrateSendState::WAIT) {
          continue;
        }
        auto taskinfo = stringSplit(iter.second->toString(), "\n");
        Command::fmtMultiBulkLen(ss, taskinfo.size());
        for (auto& vs : taskinfo) {
          Command::fmtBulk(ss, vs);
        }
      }
    }
    for (auto& iter : _migrateReceiveTaskMap) {
      if (iter.first.find(pTaskid) != std::string::npos) {
        if (noWait && iter.second->_receiver->getTaskStartTime() == 0) {
          continue;
        }
        if (noRunning && iter.second->_receiver->getTaskStartTime() != 0) {
          continue;
        }
        auto taskinfo = stringSplit(iter.second->toString(), "\n");
        Command::fmtMultiBulkLen(ss, taskinfo.size());
        for (auto& vs : taskinfo) {
          Command::fmtBulk(ss, vs);
        }
      }
    }
  } else {
    return {ErrorCodes::ERR_NOTFOUND, "can not find the ptaskid" + pTaskid};
  }
  return ss.str();
}

/* get task info of pTask, if state is succ or fail */
Expected<std::string> MigrateManager::getSuccFailInfo(
  const std::string& pTaskid, bool succ) {
  std::stringstream ss;
  std::lock_guard<myMutex> lk(_mutex);
  if (existPtask(pTaskid)) {
    auto task = _migratePtaskMap[pTaskid];
    std::map<std::string, std::string> tempMap;
    if (succ) {
      tempMap = task->_succStateMap;
    } else {
      tempMap = task->_failStateMap;
    }
    Command::fmtMultiBulkLen(ss, tempMap.size());
    for (auto& iter : tempMap) {
      auto taskinfo = stringSplit(iter.second, "\n");
      Command::fmtMultiBulkLen(ss, taskinfo.size());
      for (auto& vs : taskinfo) {
        Command::fmtBulk(ss, vs);
      }
    }
  } else {
    return {ErrorCodes::ERR_NOTFOUND, "can not find the ptaskid" + pTaskid};
  }
  return ss.str();
}

Expected<uint64_t> MigrateManager::applyMigrateBinlog(ServerEntry* svr,
                                                      PStore store,
                                                      MigrateBinlogType type,
                                                      string slots,
                                                      const string& nodeName) {
  SlotsBitmap slotsMap;
  try {
    slotsMap = std::bitset<CLUSTER_SLOTS>(slots);
  } catch (const std::exception& ex) {
    LOG(ERROR) << "parse slots err, slots:" << slots << " err:" << ex.what();
    return {ErrorCodes::ERR_INTERGER, "slots wrong"};
  }
  LOG(INFO) << "applyMigrateBinlog type:" << type
            << " slots:" << bitsetStrEncode(slotsMap);

  if (type == MigrateBinlogType::RECEIVE_START) {
    // do nothing
  } else if (type == MigrateBinlogType::RECEIVE_END) {
    // NOTE(wayenchen) need do nothing, receiver have broadcast message
  } else if (type == MigrateBinlogType::SEND_START) {
    // do nothing
  } else if (type == MigrateBinlogType::SEND_END) {
    // NOTE(wayenchen) need do nothing, receiver have broadcast message
  }
  return {ErrorCodes::ERR_OK, ""};
}

Status MigrateManager::restoreMigrateBinlog(MigrateBinlogType type,
                                            uint32_t storeid,
                                            string slots) {
  LOG(INFO) << "restoreMigrateBinlog type:" << type << " storeid:" << storeid
            << " slots:" << slots;
  SlotsBitmap slotsMap;
  try {
    slotsMap = std::bitset<CLUSTER_SLOTS>(slots);
  } catch (const std::exception& ex) {
    LOG(ERROR) << "parse slots err, slots:" << slots << " err:" << ex.what();
    return {ErrorCodes::ERR_INTERGER, "slots wrong"};
  }

  if (type == MigrateBinlogType::RECEIVE_START) {
    std::lock_guard<myMutex> lk(_mutex);
    for (auto iter = _restoreMigrateTask[storeid].begin();
         iter != _restoreMigrateTask[storeid].end();
         iter++) {
      if (*iter == slotsMap) {
        LOG(ERROR) << "_restoreMigrateTask RECEIVE_START already has task:"
                   << slots;
        return {ErrorCodes::ERR_INTERGER, "has task"};
      }
    }
    _restoreMigrateTask[storeid].push_back(slotsMap);
    LOG(INFO) << "_restoreMigrateTask RECEIVE_START push_back task:"
              << bitsetStrEncode(slotsMap);
  } else if (type == MigrateBinlogType::RECEIVE_END) {
    // clear temp save slots
    {
      std::lock_guard<myMutex> lk(_mutex);
      bool find = false;
      for (auto iter = _restoreMigrateTask[storeid].begin();
           iter != _restoreMigrateTask[storeid].end();
           iter++) {
        if (*iter == slotsMap) {
          _restoreMigrateTask[storeid].erase(iter);
          find = true;
          LOG(INFO) << "_restoreMigrateTask RECEIVE_END erase task:" << slots;
          break;
        }
      }
      if (!find) {
        // maybe backup is after receive_start and before receive_end.
        LOG(INFO) << "_restoreMigrateTask RECEIVE_END dont has task:" << slots;
      }
    }
    auto myself = _cluster->getMyselfNode();
    for (size_t slot = 0; slot < CLUSTER_SLOTS; slot++) {
      if (slotsMap.test(slot)) {
        auto node = _cluster->getNodeBySlot(slot);
        if (node != myself) {
          _cluster->clusterDelSlot(slot);
          _cluster->clusterAddSlot(myself, slot);
          // NOTE(takenliu): the clusterState change to CLUSTER_OK
          // will need a long time.
        } else {
          // TODO(takenliu): if dba set slots first restore second.and
          // set slot to dst_restore node,
          //   it will be node==myself
          LOG(ERROR) << "restoreMigrateBinlog error, slot:" << slot
                     << " myself:" << myself->getNodeName()
                     << " node:" << node->getNodeName() << " slots:" << slots;
        }
      }
    }
  } else if (type == MigrateBinlogType::SEND_START) {
    // do nothing
  } else if (type == MigrateBinlogType::SEND_END) {
    // NOTE(takenliu) set slots first
    auto myself = _cluster->getMyselfNode();
    for (size_t slot = 0; slot < CLUSTER_SLOTS; slot++) {
      if (slotsMap.test(slot)) {
        auto node = _cluster->getNodeBySlot(slot);
        if (node == myself) {
          _cluster->clusterDelSlot(slot);
          // NOTE(takenliu): when restore cant get the dst node,
          //     so slot cant be set, need gossip to notify.
        } else if (node != nullptr) {
          // TODO(takenliu): do what ?
          LOG(ERROR) << "restoreMigrateBinlog error, slot:" << slot
                     << " myself:" << myself->getNodeName()
                     << " node:" << node->getNodeName() << " slots:" << slots;
        } else {
          LOG(INFO) << "restoreMigrateBinlog slot has no node, slot:" << slot
                    << " slots:" << slots;
        }
      }
    }

    auto s = _svr->getGcMgr()->deleteSlotsData(slotsMap, storeid);
    if (!s.ok()) {
      LOG(ERROR) << "restoreMigrateBinlog deletechunk fail:" << s.toString();
      return s;
    }
  }
  return {ErrorCodes::ERR_OK, ""};
}

Status MigrateManager::onRestoreEnd(uint32_t storeId) {
  {
    std::lock_guard<myMutex> lk(_mutex);
    for (auto iter = _restoreMigrateTask[storeId].begin();
         iter != _restoreMigrateTask[storeId].end();
         iter++) {
      LOG(INFO) << "migrate task has receive_start and has no receive_end,"
                << " so delete keys for slots:" << (*iter).to_string();
      auto s = _svr->getGcMgr()->deleteSlotsData(*iter, storeId);
      if (!s.ok()) {
        LOG(ERROR) << "onRestoreEnd deletechunk fail:" << s.toString();
        return s;
      }
    }
  }

  // NOTE(takenliu) for the below case, need clear dont contain slots keys,
  //     for simple we clear anymore.
  // 1. for source node, backup maybe done when do deleteChunk(),
  //     and restorebinlog will dont has SEND_START and SEND_END
  //     so, we need clear the keys which deleteChunk() not finished.
  // 2. for dst node, backup maybe done between RECEIVE_START and RECEIVE_END,
  //     and restorebinlog end timestamp is before RECEIVE_END
  //     so, we need clear the received keys.
  SlotsBitmap selfSlots = _cluster->getMyselfNode()->getSlots();
  SlotsBitmap dontContainSlots;
  for (size_t chunkid = 0; chunkid < CLUSTER_SLOTS; ++chunkid) {
    if (_svr->getSegmentMgr()->getStoreid(chunkid) == storeId &&
        !selfSlots.test(chunkid)) {
      dontContainSlots.set(chunkid);
    }
  }
  LOG(INFO) << "onRestoreEnd deletechunks:" << dontContainSlots.to_string();
  // TODO(takenliu) check the logical of locking the chunks
  auto s = _svr->getGcMgr()->deleteSlotsData(dontContainSlots, storeId);
  if (!s.ok()) {
    LOG(ERROR) << "onRestoreEnd deletechunk fail:" << s.toString();
    return s;
  }
  return {ErrorCodes::ERR_OK, ""};
}

void MigrateManager::migrateSenderResize(size_t size) {
  std::lock_guard<myMutex> lk(_mutex);
  _migrateSender->resize(size);
}

void MigrateManager::migrateReceiverResize(size_t size) {
  std::lock_guard<myMutex> lk(_mutex);
  _migrateReceiver->resize(size);
}

size_t MigrateManager::migrateSenderSize() {
  std::lock_guard<myMutex> lk(_mutex);
  return _migrateSender->size();
}

size_t MigrateManager::migrateReceiverSize() {
  std::lock_guard<myMutex> lk(_mutex);
  return _migrateReceiver->size();
}
/* NOTE(wayenchen) if task is sending slots,
 * just stop the snapshot or binlog process
 * else if the Task state is WAIT, just stop it
 * else if the Task is deleting or succ
 * do nothing and wait it finish beause the task is nearly finished*/
void MigrateSendTask::stopTask() {
  std::lock_guard<std::mutex> lk(_mutex);
  if (_state == MigrateSendState::START) {
    _sender->stop();
  } else if (_state == MigrateSendState::WAIT) {
    _state = MigrateSendState::ERR;
    _nextSchedTime = SCLOCK::now();
    _isRunning = false;
  }
  LOG(INFO) << "finish stop sender task on slots:" << bitsetStrEncode(_slots)
            << "taskid is:" << _taskid;
}
/* NOTE(wayenchen) if task is on receiving snapshot or binlog state,
 * just stop the snapshot or binlog process,
 * other state do nothing, wait it finish beause the task is nearly finished*/
void MigrateReceiveTask::stopTask() {
  std::lock_guard<std::mutex> lk(_mutex);
  if (_state == MigrateReceiveState::RECEIVE_SNAPSHOT ||
      _state == MigrateReceiveState::RECEIVE_BINLOG) {
    _receiver->stop();
  }
  LOG(INFO) << "finish stop receive task on slots:" << bitsetStrEncode(_slots)
            << "taskid is:" << _taskid;
}

}  // namespace tendisplus
