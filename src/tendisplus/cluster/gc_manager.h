// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_CLUSTER_GC_MANAGER_H_
#define SRC_TENDISPLUS_CLUSTER_GC_MANAGER_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "tendisplus/cluster/cluster_manager.h"
#include "tendisplus/server/server_entry.h"

namespace tendisplus {

struct DeleteRangeTask {
  explicit DeleteRangeTask(uint32_t storeid,
                           uint32_t slotIdStart,
                           uint32_t slotIdEnd)
    : _storeid(storeid), _slotStart(slotIdStart), _slotEnd(slotIdEnd) {}

  uint32_t _storeid;
  uint32_t _slotStart;
  uint32_t _slotEnd;
};

class ServerEntry;
class ClusterState;

using SlotsBitmap = std::bitset<CLUSTER_SLOTS>;

class GCManager {
 public:
  explicit GCManager(std::shared_ptr<ServerEntry> svr);

  Status startup();
  void stop();

  static std::vector<DeleteRangeTask> generateDeleleRangeTask(
    const std::shared_ptr<tendisplus::ServerEntry>& svr,
    const SlotsBitmap& deletingSlots);

  bool isDeletingSlot() const;
  bool isDeletingSlot(uint32_t slot) const;

  Status deleteBitMap(const SlotsBitmap& slots, bool dumpIfError = true);
  Status deleteBitMap(const SlotsBitmap& slots,
                      uint32_t storeid,
                      bool dumpIfError = true);

  Status delGarbage();

 private:
  void controlRoutine();
  void gcSchedule();
  SlotsBitmap getCheckList() const;
  Status deleteSlots(const DeleteRangeTask& task);

 private:
  std::shared_ptr<ServerEntry> _svr;
  std::atomic<bool> _isRunning;
  mutable std::mutex _mutex;
  std::unique_ptr<std::thread> _controller;
  SlotsBitmap _deletingSlots;
  uint32_t _waitTimeAfterMigrate;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_CLUSTER_GC_MANAGER_H_
