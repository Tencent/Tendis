// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include "glog/logging.h"
#include "tendisplus/cluster/gc_manager.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

GCManager::GCManager(std::shared_ptr<ServerEntry> svr)
  : _svr(svr),
    _isRunning(false),
    _waitTimeAfterMigrate(0) {}

Status GCManager::startup() {
  std::lock_guard<std::mutex> lk(_mutex);
  _isRunning.store(true, std::memory_order_relaxed);
  _controller =
    std::make_unique<std::thread>(std::move([this]() { controlRoutine(); }));
  return {ErrorCodes::ERR_OK, ""};
}

void GCManager::stop() {
  LOG(INFO) << "GCManager begins stops...";
  _isRunning.store(false, std::memory_order_relaxed);
  _controller->join();
  LOG(INFO) << "GCManager stops succ";
}

void GCManager::controlRoutine() {
  // 1. check _isRunning is true.
  // 2. check if there is migrate task.
  // 3. check if we should wait for sometime.
  // 4. do actual deleteRange job.
  while (_isRunning.load(std::memory_order_relaxed)) {
    if (_svr->getMigrateManager()->existMigrateTask()) {
      _waitTimeAfterMigrate = _svr->getParams()->waitTimeIfExistsMigrateTask;
    } else if (_waitTimeAfterMigrate > 0) {
      _waitTimeAfterMigrate--;
    } else {
      gcSchedule();
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  DLOG(INFO) << "gc controller exits";
}

// garbage data delete schedule job
void GCManager::gcSchedule() {
  SlotsBitmap tmpDeletingBitmap;
  {
    std::lock_guard<std::mutex> lk(_mutex);
    tmpDeletingBitmap = _deletingSlots;
  }

  // if no slot to delete, return.
  if (tmpDeletingBitmap.none()) {
    return;
  }

  // check _deletingSlots and generate deleteRangetask.
  for (const auto& it : generateDeleleRangeTask(_svr, tmpDeletingBitmap)) {
    auto s = deleteSlots(it);
    if (!s.ok()) {
      LOG(ERROR) << "deleteSlots failed. begin: " << it._slotStart
                 << " end: " << it._slotEnd << " on storeid: " << it._storeid
                 << " detail: " << s.getErrmsg();
      INVARIANT_D(0);
    }
  }

  {
    std::lock_guard<std::mutex> lk(_mutex);
    // check if there are slots delete failed
    tmpDeletingBitmap &= _deletingSlots;
  }

  if (tmpDeletingBitmap.any()) {
    LOG(ERROR) << "after deleteSlots, there are still slots not be deleted.";
    INVARIANT_D(0);
  }
}

std::vector<DeleteRangeTask> GCManager::generateDeleleRangeTask(
  const std::shared_ptr<tendisplus::ServerEntry>& svr,
  const SlotsBitmap& deletingSlots) {
  std::vector<DeleteRangeTask> deleteRangeTaskList;
  for (uint32_t i = 0; i < svr->getKVStoreCount(); ++i) {
    int slotStart = -1;
    int slotEnd = -1;
    for (uint32_t j = 0; j < CLUSTER_SLOTS; ++j) {
      if (svr->getSegmentMgr()->getStoreid(j) != i) {
        continue;
      }
      if (deletingSlots.test(j)) {
        if (slotStart == -1) {
          slotStart = j;
        }
        slotEnd = j;
      } else {
        if (slotStart != -1) {
          deleteRangeTaskList.emplace_back(i, slotStart, slotEnd);
          slotStart = -1;
          slotEnd = -1;
        }
      }
    }
    if (slotStart != -1) {
      deleteRangeTaskList.emplace_back(i, slotStart, slotEnd);
    }
  }

  return deleteRangeTaskList;
}

Status GCManager::deleteSlots(const DeleteRangeTask& task) {
  LocalSessionGuard g(_svr.get());
  auto expdb = _svr->getSegmentMgr()->getDb(
    g.getSession(), task._storeid, mgl::LockMode::LOCK_IX);
  RET_IF_ERR_EXPECTED(expdb);

  // try to lock all slots in case of something wrong.
  std::vector<std::unique_ptr<ChunkLock>> lockList;
  for (uint32_t i = task._slotStart; i <= task._slotEnd; ++i) {
    if (_svr->getSegmentMgr()->getStoreid(i) == task._storeid) {
      auto lock = ChunkLock::AquireChunkLock(
        task._storeid, i, mgl::LockMode::LOCK_X,
        nullptr, _svr->getMGLockMgr());
      RET_IF_ERR_EXPECTED(lock);
      lockList.push_back(std::move(lock.value()));
    }
  }

  // slots to be deleted mustn't belong to current node
  for (uint32_t i = task._slotStart; i <= task._slotEnd; ++i) {
    if (_svr->getSegmentMgr()->getStoreid(i) == task._storeid &&
        _svr->getClusterMgr()->getClusterState()->isSlotBelongToMe(i)) {
      return {ErrorCodes::ERR_CLUSTER,
              "Deleting slot " + std::to_string(i) +
              " belong to current node."};
    }
  }

  const auto& begin = RecordKey(
    task._slotStart, 0, RecordType::RT_INVALID, "", "").prefixChunkid();
  const auto& end = RecordKey(
    task._slotEnd + 1, 0, RecordType::RT_INVALID, "", "").prefixChunkid();
  LOG(INFO) << "deleteslots [" << task._slotStart << ", " << task._slotEnd + 1
            << ") on storeid: " << task._storeid<< ", begin...";
  auto s = expdb.value().store->deleteRange(
    begin, end, _svr->getParams()->deleteFilesInRangeForMigrateGc,
    _svr->getParams()->compactRangeAfterDeleteRange);
  RET_IF_ERR(s);
  LOG(INFO) << "deleteslots [" << task._slotStart << ", " << task._slotEnd + 1
            << ") on storeid: " << task._storeid<< ", done.";

  {
    std::lock_guard<std::mutex> lk(_mutex);
    for (uint32_t i = task._slotStart; i <= task._slotEnd; ++i) {
      if (_svr->getSegmentMgr()->getStoreid(i) == task._storeid) {
        _deletingSlots.reset(i);
      }
    }
  }
  return {ErrorCodes::ERR_OK, ""};
}

bool GCManager::isDeletingSlot() const {
  std::lock_guard<std::mutex> lk(_mutex);
  return _deletingSlots.any();
}

bool GCManager::isDeletingSlot(uint32_t slot) const {
  std::lock_guard<std::mutex> lk(_mutex);
  std::size_t s = static_cast<std::size_t>(slot);
  return _deletingSlots.test(s);
}

/*
 * note(raffertyyu):
 *   check if any of slots is already in deleting in case of
 *   user input wrong slots;
 */
Status GCManager::deleteBitMap(const SlotsBitmap& slots, bool dumpIfError) {
  SlotsBitmap tmp;
  {
    std::lock_guard<std::mutex> lk(_mutex);
    tmp = _deletingSlots;
  }

  // check if any slot already be deleting
  tmp &= slots;
  if (tmp.any()) {
    std::stringstream ss("slots: ");
    for (uint32_t i = 0; i < tmp.size(); i++) {
      if (tmp.test(i)) {
        ss << std::to_string(i) << " ";
      }
    }
    ss << "are already in deleting, check if slots is right.";
    LOG(ERROR) << ss.str();
    if (dumpIfError) {
      INVARIANT_D(0);
    }
    return {ErrorCodes::ERR_INTERNAL, ss.str()};
  }

  {
    std::lock_guard<std::mutex> lk(_mutex);
    _deletingSlots |= slots;
  }

  return {ErrorCodes::ERR_OK, ""};
}

Status GCManager::deleteBitMap(
  const SlotsBitmap& slots, uint32_t storeid, bool dumpIfError) {
  // check slots not belong to storeid
  for (uint32_t i = 0; i < slots.size(); i++) {
    if (slots.test(i) && _svr->getSegmentMgr()->getStoreid(i) != storeid) {
      LOG(ERROR) << "invalid slot " << i << " in deleteSlots. "
                 << "slots must all belong to storeid " << storeid;
      if (dumpIfError) {
        INVARIANT_D(0);
      }
      return {ErrorCodes::ERR_INTERNAL,
              "slot " + std::to_string(i) + " not belong to storeid " +
              std::to_string(storeid)};
    }
  }

  return deleteBitMap(slots);
}

/* get cron check slots list, which contain dirty data*/
SlotsBitmap GCManager::getCheckList() const {
  SlotsBitmap checklist;
  checklist.set();
  const auto& myslots =
    _svr->getClusterMgr()->getClusterState()->getMyselfNode()->getSlots();
  /* NOTE(wayenchen) garbage list should not be migrating,
   * should not belong to me and is not empty. */
  for (std::size_t idx = 0; idx < CLUSTER_SLOTS; idx++) {
    if (myslots.test(idx) || isDeletingSlot(idx) ||
        _svr->getMigrateManager()->slotInTask(idx) ||
        _svr->getClusterMgr()->emptySlot(idx)) {
      checklist.reset(idx);
    }
  }
  LOG(INFO) << "check list is: " << bitsetStrEncode(checklist);
  return checklist;
}

/* check the chunks which not belong to me but contain keys
 * @note wayenchen checkGarbage is a cron task, may be two ways to finish it:
 *  delete slots one by one or delete at once after check finish
 */
Status GCManager::delGarbage() {
  /* only master node should be check */
  auto cstate = _svr->getClusterMgr()->getClusterState();
  if (!cstate->isMyselfMaster()) {
    return {ErrorCodes::ERR_CLUSTER, "not master"};
  }
  if (!cstate->clusterIsOK()) {
    return {ErrorCodes::ERR_CLUSTER, "cluster error state"};
  }
  const auto& garbageList = getCheckList();
  if (garbageList.none()) {
    LOG(WARNING) << "no dirty data find in " << cstate->getMyselfName();
    return {ErrorCodes::ERR_NOTFOUND, "no dirty data to delete"};
  }
  std::stringstream ss;
  ss << "Deleting garbage Slots List: ";
  for (uint32_t i = 0; i < garbageList.size(); i++) {
    if (garbageList.test(i)) {
      ss << std::to_string(i) << " ";
    }
  }
  LOG(INFO) << ss.str() << "...";
  auto s = deleteBitMap(garbageList, false);
  if (!s.ok()) {
    LOG(ERROR) << "delete garbage fail:" << s.toString();
  }

  LOG(INFO) << "finish del garbage, deleting list is :"
            << bitsetStrEncode(garbageList)
            << "delete num is:" << garbageList.count();

  return {ErrorCodes::ERR_OK, ""};
}

}  // namespace tendisplus
