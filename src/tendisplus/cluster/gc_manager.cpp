// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <utility>
#include "glog/logging.h"
#include "tendisplus/cluster/gc_manager.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

GCManager::GCManager(std::shared_ptr<ServerEntry> svr)
  : _svr(svr),
    _cstate(svr->getClusterMgr()->getClusterState()),
    _isRunning(false),
    _gcDeleterMatrix(std::make_shared<PoolMatrix>()) {
  _svr->getParams()
    ->serverParamsVar("garbageDeleteThreadnum")
    ->setUpdate([this]() {
      garbageDeleterResize(_svr->getParams()->garbageDeleteThreadnum);
    });
}

Status GCManager::startup() {
  std::lock_guard<myMutex> lk(_mutex);
  _gcDeleter = std::make_unique<WorkerPool>("tx-gc-manager", _gcDeleterMatrix);

  auto s = _gcDeleter->startup(_svr->getParams()->garbageDeleteThreadnum);
  if (!s.ok()) {
    return s;
  }
  _isRunning.store(true, std::memory_order_relaxed);

  _controller =
    std::make_unique<std::thread>(std::move([this]() { controlRoutine(); }));

  return {ErrorCodes::ERR_OK, ""};
}

void GCManager::stop() {
  LOG(INFO) << "GCManager begins stops...";
  _isRunning.store(false, std::memory_order_relaxed);
  _controller->join();
  _gcDeleter->stop();
  LOG(INFO) << "GCManager stops succ";
}

Status GCManager::stopStoreTask(uint32_t storeid) {
  std::lock_guard<myMutex> lk(_mutex);
  for (auto& iter : _deleteChunkTask) {
    if (iter->_storeid == storeid) {
      iter->_nextSchedTime = SCLOCK::time_point::max();
    }
  }
  return {ErrorCodes::ERR_OK, ""};
}

void GCManager::controlRoutine() {
  while (_isRunning.load(std::memory_order_relaxed)) {
    bool doSth = false;
    auto now = SCLOCK::now();
    doSth = gcSchedule(now) || doSth;
    if (doSth) {
      std::this_thread::yield();
    } else {
      std::this_thread::sleep_for(100ms);
    }
  }
  DLOG(INFO) << "gc controller exits";
}

/* get cron check slots list, which contain dirty data*/
SlotsBitmap GCManager::getCheckList() {
  SlotsBitmap checklist;
  checklist.set();
  size_t idx = 0;
  SlotsBitmap myslots = _cstate->getMyselfNode()->getSlots();
  while (idx < CLUSTER_SLOTS) {
    /*NOTE(wayenchen) garbage list should not be migrating,
     * should not belong to me and is not empty*/
    if (myslots.test(idx) || slotIsDeleting(idx) ||
        _svr->getMigrateManager()->slotInTask(idx) ||
        _svr->getClusterMgr()->emptySlot(idx)) {
      checklist.reset(idx);
    }
    idx++;
  }
  LOG(INFO) << "check list is:" << bitsetStrEncode(checklist);
  return checklist;
}

/* check the chunks which not belong to me but contain keys
 * @note wayenchen checkGarbage is a cron task, may be two ways to finish it:
 *  delete slots one by one or delete at once after check finish
 */
Status GCManager::delGarbage() {
  /* only master node should be check */
  if (!_cstate->isMyselfMaster()) {
    return {ErrorCodes::ERR_CLUSTER, "not master"};
  }
  if (!_cstate->clusterIsOK()) {
    return {ErrorCodes::ERR_CLUSTER, "cluster error state"};
  }
  auto garbageList = getCheckList();
  if (garbageList.count() == 0) {
    LOG(WARNING) << "no dirty data find in " << _cstate->getMyselfName();
    return {ErrorCodes::ERR_NOTFOUND, "no dirty data to delete"};
  }
  std::lock_guard<myMutex> lk(_mutex);
  auto s = deleteBitMap(garbageList);
  if (!s.ok()) {
    LOG(ERROR) << "delete garbage fail:" << s.toString();
  }

  LOG(INFO) << "finish del garbage, deleting list is :"
            << bitsetStrEncode(garbageList)
            << "delete num is:" << garbageList.count();

  return {ErrorCodes::ERR_OK, ""};
}

Status GCManager::startDeleteTask(uint32_t storeid,
                                  uint32_t slotStart,
                                  uint32_t slotEnd,
                                  mstime_t delay) {
  std::lock_guard<myMutex> lk(_mutex);
  for (uint32_t i = slotStart; i <= slotEnd; i++) {
    if (_svr->getSegmentMgr()->getStoreid(i) == storeid) {
      if (_deletingSlots.test(i)) {
        LOG(WARNING) << "slot:" << i << "is already deleting";
        return {ErrorCodes::ERR_INTERNAL, "already deleting"};
      }
      _deletingSlots.set(i);
    }
  }
  auto deleteTask =
    std::make_unique<DeleteRangeTask>(storeid, slotStart, slotEnd, _svr);
  /* NOTE(wayenchen) it is better to delay deleting after migrting  */
  deleteTask->_nextSchedTime = SCLOCK::now() + chrono::seconds(delay);
  _deleteChunkTask.push_back(std::move(deleteTask));
  LOG(INFO) << "add deleting task from:" << slotStart << "to:" << slotEnd;
  return {ErrorCodes::ERR_OK, ""};
}

// garbage dat delete schedule job
bool GCManager::gcSchedule(const SCLOCK::time_point& now) {
  bool doSth = false;
  std::list<std::shared_ptr<DeleteRangeTask>> startDeletingTask;
  {
    std::lock_guard<myMutex> lk(_mutex);
    for (auto it = _deleteChunkTask.begin(); it != _deleteChunkTask.end();) {
      if ((*it)->_isRunning || now < (*it)->_nextSchedTime) {
        ++it;
        continue;
      }
      doSth = true;
      auto startPos = (*it)->_slotStart;
      auto endPos = (*it)->_slotEnd;
      auto storeid = (*it)->_storeid;
      if ((*it)->_state == DeleteRangeState::START) {
        LOG(INFO) << "deletetask start,"
                  << " from:" << startPos << " to:" << endPos;
        (*it)->_isRunning = true;
        startDeletingTask.push_back(*it);
        ++it;
      } else if ((*it)->_state == DeleteRangeState::SUCC) {
        LOG(INFO) << "garbage delete success,"
                  << " from:" << startPos << " to:" << endPos;
        for (uint32_t i = startPos; i <= endPos; i++) {
          if (_svr->getSegmentMgr()->getStoreid(i) == storeid)
            _deletingSlots.reset(i);
        }
        it = _deleteChunkTask.erase(it);
        continue;
      } else if ((*it)->_state == DeleteRangeState::ERR) {
        LOG(ERROR) << "garbage delete failed,"
                   << " from:" << startPos << " to:" << endPos;
        for (uint32_t i = startPos; i <= endPos; i++) {
          if (_svr->getSegmentMgr()->getStoreid(i) == storeid) {
            _deletingSlots.reset(i);
          }
        }
        /*NOTE (wayenchen) no need rerty again,
        beacause croncheck job will do the fail job */
        it = _deleteChunkTask.erase(it);
        continue;
      }
    }
  }
  /* NOTE(wayenchen) delete task is running in Db lock, so it can not be
   * running in mutex */
  for (auto it = startDeletingTask.begin(); it != startDeletingTask.end();) {
    /*NOTE(wayenchen) if it is migrating, delay ten minutes to delete*/

    if (_svr->getMigrateManager()->existMigrateTask() &&
        !_svr->getParams()->enableGcInMigate) {
      (*it)->_nextSchedTime = SCLOCK::now() + chrono::seconds(600);
      (*it)->_isRunning = false;
    } else {
      _gcDeleter->schedule(
        [this, iter = (*it).get()]() { garbageDelete(iter); });
    }
    it = startDeletingTask.erase(it);
  }
  return doSth;
}

bool GCManager::slotIsDeleting(uint32_t slot) {
  std::lock_guard<myMutex> lk(_mutex);
  return _deletingSlots.test(slot) ? true : false;
}
/*
 *  if any cluster or migrate task pass a [storeid start, end] deleteRange to
 this API, the the task will start after delay seconds (delay deleting is more
 stable when migrating)
 * @note (wayenchen) it is better to delay for sometime to delete in migrating
 situation
*/
Status GCManager::deleteChunks(uint32_t storeid,
                               uint32_t slotStart,
                               uint32_t slotEnd,
                               mstime_t delay) {
  if (_svr->getSegmentMgr()->getStoreid(slotStart) != storeid ||
      _svr->getSegmentMgr()->getStoreid(slotEnd) != storeid) {
    LOG(ERROR) << "delete slot range from:" << slotStart << "to:" << slotEnd
               << "not match storeid:" << storeid;
    return {ErrorCodes::ERR_INTERNAL,
            "storeid of SlotStart or slotEnd is not matched"};
  }
  if (slotStart > slotEnd) {
    LOG(ERROR) << "delete slot range begin pos:" << slotStart
               << "bigger than end pos:" << slotEnd;
    return {ErrorCodes::ERR_INTERNAL, "startSlot should be less than endSlot"};
  }

  if (slotEnd >= CLUSTER_SLOTS || slotStart >= CLUSTER_SLOTS) {
    LOG(ERROR) << "delete slot range from:" << slotStart << "to:" << slotEnd
               << "error, slot is more than 16383";
    return {ErrorCodes::ERR_INTERNAL, "slot should be less than 16383"};
  }

  auto s = startDeleteTask(storeid, slotStart, slotEnd, delay);
  if (!s.ok()) {
    LOG(ERROR) << "gen delete task fail:" << s.toString();
    return s;
  }
  return {ErrorCodes::ERR_OK, ""};
}

/* delete slots numbers should be control, if more than 300, split into small
 * list*/
Status GCManager::deleteLargeChunks(uint32_t storeid,
                                    uint32_t slotStart,
                                    uint32_t slotEnd,
                                    mstime_t delay) {
  auto maxSize = _svr->getParams()->garbageDeleteSize;
  auto kvcount = _svr->getParams()->kvStoreCount;
  const uint32_t maxDiff = maxSize * kvcount;
  if (slotEnd - slotStart < maxDiff) {
    auto s = deleteChunks(storeid, slotStart, slotEnd, delay);
    if (!s.ok()) {
      LOG(ERROR) << "delete range fail:" << s.toString();
    }
    return s;
  }
  for (size_t i = slotStart; i <= slotEnd; i += maxDiff) {
    uint32_t endPos = slotEnd < i + maxDiff ? slotEnd : i + maxDiff;
    auto s = deleteChunks(storeid, i, endPos, delay);
    if (!s.ok()) {
      LOG(ERROR) << "delete range fail:" << s.toString();
      return s;
    }
    i += kvcount;
  }
  return {ErrorCodes::ERR_OK, ""};
}

/*  note(wayenchen): given a slotlist and delete them of all storeids
 * before deleting all nodes will be sorted and made into contious groups
 * this slots may belong to different storeid*/
Status GCManager::deleteBitMap(const SlotsBitmap& slots, mstime_t delay) {
  Status s;
  for (uint64_t i = 0; i < _svr->getKVStoreCount(); i++) {
    s = deleteSlotsData(slots, i, delay);
    if (!s.ok()) {
      LOG(ERROR) << "delete fail on storeid:" << i << s.toString();
      return s;
    }
  }
  return {ErrorCodes::ERR_OK, ""};
}

Status GCManager::deleteSlotsData(const SlotsBitmap& slots,
                                  uint32_t storeid,
                                  uint64_t delay) {
  size_t idx = 0;
  uint32_t startChunkid = UINT32_MAX;
  uint32_t endChunkid = UINT32_MAX;
  while (idx < slots.size()) {
    if (_svr->getSegmentMgr()->getStoreid(idx) == storeid) {
      if (slots.test(idx)) {
        if (startChunkid == UINT32_MAX) {
          startChunkid = idx;
        }
        endChunkid = idx;
      } else {
        if (startChunkid != UINT32_MAX) {
          /* throw it into gc job, no waiting */
          auto s = deleteLargeChunks(storeid, startChunkid, endChunkid, delay);
          if (!s.ok()) {
            LOG(ERROR) << "deleteChunk fail, startChunkid:" << startChunkid
                       << " endChunkid:" << endChunkid
                       << " err:" << s.toString();
            return s;
          }
          LOG(INFO) << "deleteChunk ok, startChunkid:" << startChunkid
                    << " endChunkid:" << endChunkid;
          startChunkid = UINT32_MAX;
          endChunkid = UINT32_MAX;
        }
      }
    }
    idx++;
  }
  if (startChunkid != UINT32_MAX) {
    auto s = deleteLargeChunks(storeid, startChunkid, endChunkid, delay);
    if (!s.ok()) {
      LOG(ERROR) << "deleteChunk fail, startChunkid:" << startChunkid
                 << " endChunkid:" << endChunkid << " err:" << s.toString();
      return s;
    }
    LOG(INFO) << "deleteChunk ok, startChunkid:" << startChunkid
              << " endChunkid:" << endChunkid;
  }
  return {ErrorCodes::ERR_OK, ""};
}

void GCManager::garbageDelete(DeleteRangeTask* task) {
  uint64_t start = msSinceEpoch();
  auto s = task->deleteSlotRange();
  std::lock_guard<myMutex> lk(_mutex);
  if (!s.ok()) {
    LOG(ERROR) << "fail delete slots range" << s.toString();
    task->_state = DeleteRangeState::ERR;
  } else {
    auto end = msSinceEpoch();
    task->_state = DeleteRangeState::SUCC;
    serverLog(LL_NOTICE,
              "GCManager::garbageDelete success"
              "from: [%u] to [%u] [total used time:%lu]",
              task->_slotStart,
              task->_slotEnd,
              end - start);
  }
  task->_nextSchedTime = SCLOCK::now();
  task->_isRunning = false;
}

Status DeleteRangeTask::deleteSlotRange() {
  auto expdb =
    _svr->getSegmentMgr()->getDb(NULL, _storeid, mgl::LockMode::LOCK_IS);
  if (!expdb.ok()) {
    LOG(ERROR) << "getDb failed:" << _storeid;
    return expdb.status();
  }
  PStore kvstore = expdb.value().store;
  RecordKey rkStart(_slotStart, 0, RecordType::RT_INVALID, "", "");
  RecordKey rkEnd(_slotEnd + 1, 0, RecordType::RT_INVALID, "", "");
  string start = rkStart.prefixChunkid();
  string end = rkEnd.prefixChunkid();
  auto s = kvstore->deleteRange(start, end);

  if (!s.ok()) {
    serverLog(LL_NOTICE,
              "DeleteRangeTask::deleteChunk commit failed,"
              "from [startSlot:%u] to [endSlot:%u] [bad response:%s]",
              _slotStart,
              _slotEnd,
              s.toString().c_str());
    return s;
  }

  if (_svr->getParams()->compactRangeAfterDeleteRange) {
    // NOTE(takenliu) after deleteRange, cursor seek will scan all the keys in
    // delete range,
    //   so we call compactRange to real delete the keys.
    s = kvstore->compactRange(
      ColumnFamilyNumber::ColumnFamily_Default, &start, &end);

    if (!s.ok()) {
      serverLog(LL_NOTICE,
              "DeleteRangeTask::kvstore->compactRange failed,"
              "from [startSlot:%u] to [endSlot:%u] [bad response:%s]",
              _slotStart,
              _slotEnd,
              s.toString().c_str());
      return s;
    }
  }
  serverLog(LL_VERBOSE,
            "DeleteRangeTask::deleteRange finished,"
            "from [startSlot: %u] to [endSlot: %u]",
            _slotStart,
            _slotEnd);

  return {ErrorCodes::ERR_OK, ""};
}

void GCManager::garbageDeleterResize(size_t size) {
  _gcDeleter->resize(size);
}

size_t GCManager::garbageDeleterSize() {
  return _gcDeleter->size();
}

}  // namespace tendisplus
