// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_CLUSTER_GC_MANAGER_H_
#define SRC_TENDISPLUS_CLUSTER_GC_MANAGER_H_

#include <list>
#include <memory>
#include <string>
#include <utility>

#include "tendisplus/cluster/cluster_manager.h"
#include "tendisplus/server/server_entry.h"

namespace tendisplus {

class ServerEntry;
class ClusterState;

using mstime_t = uint64_t;
using SlotsBitmap = std::bitset<CLUSTER_SLOTS>;
using myMutex = std::recursive_mutex;

enum class DeleteRangeState { NONE = 0, START, SUCC, ERR };

class DeleteRangeTask {
 public:
  explicit DeleteRangeTask(uint32_t storeid,
                           uint32_t slotIdStart,
                           uint64_t slotIdEnd,
                           std::shared_ptr<ServerEntry> svr)
    : _storeid(storeid),
      _slotStart(slotIdStart),
      _slotEnd(slotIdEnd),
      _svr(svr),
      _isRunning(false),
      _state(DeleteRangeState::START) {}

  uint32_t _storeid;
  uint32_t _slotStart;
  uint32_t _slotEnd;
  std::shared_ptr<ServerEntry> _svr;
  bool _isRunning;
  SCLOCK::time_point _nextSchedTime;
  mutable myMutex _mutex;
  DeleteRangeState _state;
  Status deleteSlotRange();
};

using SlotsBitmap = std::bitset<CLUSTER_SLOTS>;
class GCManager {
 public:
  explicit GCManager(std::shared_ptr<ServerEntry> svr);

  Status startup();
  Status stopStoreTask(uint32_t storid);
  void stop();

  Status deleteChunks(uint32_t storeid,
                      uint32_t slotStart,
                      uint32_t slotEnd,
                      mstime_t delay = 0);

  Status deleteBitMap(const SlotsBitmap& slots, uint64_t delay = 0);
  Status deleteSlotsData(const SlotsBitmap& slots,
                         uint32_t storeid,
                         uint64_t delay = 0);
  bool slotIsDeleting(uint32_t slot);

  void garbageDeleterResize(size_t size);
  size_t garbageDeleterSize();
  Status delGarbage();

 private:
  void controlRoutine();
  bool gcSchedule(const SCLOCK::time_point& now);
  SlotsBitmap getCheckList();
  Status startDeleteTask(uint32_t storeid,
                         uint32_t slotStart,
                         uint32_t slotEnd,
                         mstime_t delay = 0);
  void garbageDelete(DeleteRangeTask* task);
  Status deleteLargeChunks(uint32_t storeid,
                           uint32_t slotStart,
                           uint32_t slotEnd,
                           mstime_t delay = 0);

 private:
  std::shared_ptr<ServerEntry> _svr;
  std::shared_ptr<ClusterState> _cstate;
  std::atomic<bool> _isRunning;
  mutable myMutex _mutex;
  std::unique_ptr<std::thread> _controller;

  std::list<std::shared_ptr<DeleteRangeTask>> _deleteChunkTask;

  std::unique_ptr<WorkerPool> _gcDeleter;
  std::shared_ptr<PoolMatrix> _gcDeleterMatrix;

  // slots in deleting task
  std::bitset<CLUSTER_SLOTS> _deletingSlots;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_CLUSTER_GC_MANAGER_H_
