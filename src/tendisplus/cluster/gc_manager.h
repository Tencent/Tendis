#ifndef SRC_TENDISPLUS_CLUSTER_GC_MANAGER_H_
#define SRC_TENDISPLUS_CLUSTER_GC_MANAGER_H_

#include <list>
#include <string>
#include <utility>
#include "tendisplus/cluster/cluster_manager.h"
#include "tendisplus/server/server_entry.h"
#include "tendisplus/cluster/cluster_manager.h"

namespace tendisplus {

#define CLUSTER_SLOTS 16384

class ServerEntry;
class ClusterState;

using mstime_t = uint64_t;
using SlotsBitmap = std::bitset<CLUSTER_SLOTS>;
using myMutex = std::recursive_mutex;

enum class DeleteRangeState {
    NONE = 0,
    START,
    SUCC,
    ERR
};

class DeleteRangeTask {
public:
    explicit DeleteRangeTask(uint32_t storeid,
                             uint32_t slotIdStart, uint64_t slotIdEnd,
                             std::shared_ptr<ServerEntry> svr) :
            _storeid(storeid),
            _slotStart(slotIdStart),
            _slotEnd(slotIdEnd),
            _svr(svr),
            _isRunning(false),
            _state(DeleteRangeState::START) {
    }

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

    Status deleteChunks(uint32_t storeid, uint32_t slotStart, uint32_t slotEnd, mstime_t delay = 0);
    Status deleteSlotsList(std::vector<uint32_t> slotsList, mstime_t delay = 0);
    bool slotIsDeleting(uint32_t slot);

    void garbageDeleterResize(size_t size);
    size_t garbageDeleterSize() ;

private:
    void controlRoutine();
    void cronCheck();
    void checkGarbage();
    void retryDelete();
    bool gcSchedule(const SCLOCK::time_point& now);
    SlotsBitmap  getCheckList();
    void startDeleteTask(uint32_t  storeid, uint32_t slotStart, uint32_t slotEnd, mstime_t delay = 0);
    void garbageDelete(DeleteRangeTask* task);

 private:
    std::shared_ptr<ServerEntry> _svr;
    std::shared_ptr<ClusterState> _cstate;
    std::atomic<bool> _isRunning;
    mutable myMutex _mutex;
    mutable std::mutex _checkerMutex;
    std::condition_variable _cv;
    std::unique_ptr<std::thread> _controller;

    std::list<std::shared_ptr<DeleteRangeTask>> _deleteChunkTask;

    std::unique_ptr<WorkerPool> _gcDeleter;
    std::shared_ptr<PoolMatrix> _gcDeleterMatrix;
    // checker is single thead
    std::unique_ptr<std::thread> _gcChecker;

    // slots in deleting task
    std::bitset<CLUSTER_SLOTS> _deletingSlots;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_CLUSTER_MIGRATE_MANAGER_H_
