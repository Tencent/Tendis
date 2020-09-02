#include <utility>
#include "glog/logging.h"
#include "tendisplus/commands/command.h"
#include "gc_manager.h"

namespace tendisplus {

GCManager::GCManager(std::shared_ptr<ServerEntry> svr)
        : _svr(svr),
          _cstate(svr->getClusterMgr()->getClusterState()),
          _isRunning(false),
          _gcDeleterMatrix(std::make_shared<PoolMatrix>()) {
}

Status GCManager::startup() {
    std::lock_guard<myMutex> lk(_mutex);
    _gcDeleter = std::make_unique<WorkerPool>("gc-clear", _gcDeleterMatrix);

    auto s = _gcDeleter->startup(_svr->getParams()->garbageDeleteThreadnum);
    if (!s.ok()) {
        return  s;
    }
    _isRunning.store(true, std::memory_order_relaxed);

    _controller = std::make_unique<std::thread>(std::move([this]() {
        controlRoutine();
    }));

    _gcChecker= std::make_unique<std::thread>(std::move([this]() {
        cronCheck();
    }));

    return {ErrorCodes::ERR_OK, ""};
}

void GCManager::stop() {
    LOG(INFO) << "GCManager begins stops...";
    _isRunning.store(false, std::memory_order_relaxed);
    _controller->join();
    _cv.notify_one();
    _gcChecker->join();
    _gcDeleter->stop();
    LOG(INFO) << "GCManager stops succ";
}

Status GCManager::stopStoreTask(uint32_t storeid) {
    std::lock_guard<myMutex> lk(_mutex);
    for (auto &iter : _deleteChunkTask) {
        if (iter->_storeid == storeid) {
            iter->_nextSchedTime = SCLOCK::time_point::max();
        }
    }
    return { ErrorCodes::ERR_OK, "" };
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

void GCManager::cronCheck() {
    // NOTE(wayenchen) use cv wait_for to make a crontask instead of longtime sleep(thread.join is not work when sleep)
     std::unique_lock<std::mutex> lk(_checkerMutex);
     while(!_cv.wait_for(lk, std::chrono::minutes(30),
             [this]() { return !_isRunning; })) {
        checkGarbage();
        DLOG(INFO) << "check thread is running";
     }
}

/* get cron check slots list, which contain dirty data*/
SlotsBitmap GCManager::getCheckList() {
    std::lock_guard<myMutex> lk(_mutex);
    SlotsBitmap checklist;
    checklist.set();
    size_t idx = 0;
    SlotsBitmap myslots = _cstate->getMyselfNode()->getSlots();
    while (idx < checklist.size()) {
        /*NOTE(wayenchen) garbage list should not be migrating,
         * should not belong to me and is not empty*/
        if (_svr->getMigrateManager()->slotInTask(idx)
                || myslots.test(idx)
                || _svr->getClusterMgr()->emptySlot(idx)
                || _deletingSlots.test(idx)) {
            checklist.reset(idx);
        }
        idx ++;
    }
    return checklist;
}

/* check the chunks which not belong to me but contain keys
 * @note wayenchen checkGarbage is a cron task, may be two ways to finish it:
 *  delete slots one by one or delete at once after check finish
 */
void GCManager::checkGarbage()  {
    std::lock_guard<myMutex> lk(_mutex);
    /* only master node should be check */
    if (!_cstate->isMyselfMaster()) {
        return;
    }
    auto garbageList = getCheckList();
    if (garbageList.count() == 0) {
        return;
    }
    LOG(INFO) << "garbage list is :" << bitsetStrEncode(garbageList);
    size_t idx = 0;
    while (idx < garbageList.size()) {
        // start detele task of a single slot
        if (garbageList.test(idx)) {
            auto storeid = _svr->getSegmentMgr()->getStoreid(idx);
            startDeleteTask(storeid, idx, idx);
        }
        idx ++;
    }
}

void GCManager::startDeleteTask(uint32_t storeid, uint32_t slotStart, uint32_t slotEnd, mstime_t delay) {
    std::lock_guard<myMutex> lk(_mutex);
    for (uint32_t  i = slotStart; i <= slotEnd; i ++) {
        if (_svr->getSegmentMgr()->getStoreid(i) == storeid) {
            _deletingSlots.set(i);
        }
    }
    auto deleteTask = std::make_unique<DeleteRangeTask>(storeid,
                                        slotStart, slotEnd, _svr);
    /* NOTE(wayenchen) it is better to delay deleting after migrting  */
    deleteTask->_nextSchedTime = SCLOCK::now() + chrono::seconds (delay);
    _deleteChunkTask.push_back(std::move(deleteTask));
    LOG(INFO) << "add deleting task from:" << slotStart << "to:" << slotEnd;
}

// garbage dat delete schedule job
bool GCManager::gcSchedule(const SCLOCK::time_point &now) {
    bool doSth = false;
    std::list<std::shared_ptr<DeleteRangeTask>> startDeletingTask;
    {
        std::lock_guard<myMutex> lk(_mutex);
        for (auto it = _deleteChunkTask.begin(); it != _deleteChunkTask.end();) {
            if ((*it)->_isRunning) {
                ++it;
                continue;
            }
            doSth = true;
            auto startPos = (*it)->_slotStart;
            auto endPos = (*it)->_slotEnd;
            auto storeid = (*it)->_storeid;
            if ((*it)->_state == DeleteRangeState::START) {
                LOG(INFO) << "deletetask start,"
                          << " from:" << startPos
                          << " to:" << endPos;
                (*it)->_isRunning = true;
                startDeletingTask.push_back(*it);
                ++it;
            } else if ((*it)->_state == DeleteRangeState::SUCC) {
                LOG(INFO) << "garbage delete success,"
                          << " from:" << startPos
                          << " to:" << endPos;
                for (uint32_t i = startPos; i <= endPos; i ++) {
                    if (_svr->getSegmentMgr()->getStoreid(i) == storeid)
                        _deletingSlots.reset(i);
                }
                it = _deleteChunkTask.erase(it);
                continue;
            } else if ((*it)->_state == DeleteRangeState::ERR) {
                LOG(ERROR) << "garbage delete failed,"
                           << " from:" << startPos
                           << " to:" << endPos;
                for (uint32_t i = startPos; i <= endPos; i ++) {
                    if (_svr->getSegmentMgr()->getStoreid(i) == storeid)
                        _deletingSlots.reset(i);;
                }
                /*NOTE (wayenchen) no need rerty again, 
                beacause croncheck job will do the fail job */
                it = _deleteChunkTask.erase(it);
                continue;
            }
        }
    }
    /* NOTE(wayenchen) delete task is running in Db lock, so it can not be running in mutex */
    for (auto it = startDeletingTask.begin(); it != startDeletingTask.end();) {
        _gcDeleter->schedule([this, iter = (*it).get()]() {
            garbageDelete(iter);
        });
        ++it;
    }
    return doSth;
}

bool GCManager::slotIsDeleting(uint32_t slot) {
    std::lock_guard<myMutex> lk(_mutex);
    return _deletingSlots.test(slot) ? true : false;
}
/*
 *  if any cluster or migrate task pass a [storeid start, end] deleteRange to this API, the
    the task will start after delay seconds (delay deleting is more stable when migrating)
 * @note (wayenchen) it is better to delay for sometime to delete in migrating situation
*/
Status GCManager::deleteChunks(uint32_t storeid, uint32_t slotStart, uint32_t slotEnd, mstime_t delay) {
    if ( _svr->getSegmentMgr()->getStoreid(slotStart) != storeid
                || _svr->getSegmentMgr()->getStoreid(slotEnd) != storeid) {
        LOG(ERROR) << "delete slot range from:" << slotStart 
                   << "to:" << slotEnd
                   << "not match storeid:" << storeid;
        return {ErrorCodes::ERR_INTERNAL,
                "storeid of SlotStart or slotEnd is not matched"};
    }
    if (slotStart > slotEnd) {
        LOG(ERROR) << "delete slot range begin pos:" << slotStart
                   << "bigger than end pos:" << slotEnd;
        return {ErrorCodes::ERR_INTERNAL,
                "startSlot should be less than endSlot"};
    }

    if (slotEnd >= CLUSTER_SLOTS || slotStart >= CLUSTER_SLOTS) {
        LOG(ERROR) << "delete slot range from:" << slotStart
                   << "to:" << slotEnd
                   << "error, slot is more than 16383";
        return {ErrorCodes::ERR_INTERNAL,
                "slot should be less than 16383"};
    }

    startDeleteTask(storeid, slotStart, slotEnd, delay);
    return  {ErrorCodes::ERR_OK, ""};
}

/* given a slotlist and delete them of all storeids
 * note(wayenchen): this slots belong to different storeid*/
Status GCManager::deleteSlotsList(std::vector<uint32_t> slotsList,
                                mstime_t delay) {
    std::unordered_map<uint32_t, std::vector<uint32_t>> slotMap;
    for (const auto &slot : slotsList) {
        uint32_t storeid = _svr->getSegmentMgr()->getStoreid(slot);
        if ((slotMap.find(storeid)) != slotMap.end()) {
            slotMap[storeid].push_back(slot);
        } else {
            std::vector<uint32_t> temp;
            temp.push_back(slot);
            slotMap.insert(std::make_pair(storeid, temp));
        }
    }
    for (const auto& n : slotMap) {
        auto jobList = n.second;
        if (!jobList.empty()) {
            auto startPos = n.second[0];
            auto endPos = n.second.back();
            uint32_t storeid = _svr->getSegmentMgr()->getStoreid(startPos);
            auto s = deleteChunks(storeid, startPos, endPos , delay);
            if (!s.ok()) {
                LOG(ERROR) << s.toString();
                return  s;
            }
        }
    }
    return  {ErrorCodes::ERR_OK, ""};
}

void GCManager::garbageDelete(DeleteRangeTask *task) {
    uint64_t start = msSinceEpoch();
    auto s = task->deleteSlotRange();
    std::lock_guard<myMutex> lk(_mutex);
    if (!s.ok()) {
        LOG(ERROR) << "fail delete slots range" << s.toString();
        task->_state = DeleteRangeState::ERR;
    } else {
        auto end = msSinceEpoch();
        task->_state = DeleteRangeState::SUCC;
        serverLog(LL_NOTICE, "GCManager::garbageDelete success"
            "from: [%u] to [%u] [total used time:%lu]",
            task->_slotStart,
            task->_slotEnd,
            end - start);
    }
    task->_nextSchedTime = SCLOCK::now();
    task->_isRunning = false;
}

Status DeleteRangeTask::deleteSlotRange() {
    auto expdb = _svr->getSegmentMgr()->getDb(NULL, _storeid,
                                              mgl::LockMode::LOCK_IS);
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
        serverLog(LL_NOTICE, "DeleteRangeTask::deleteChunk commit failed,"
                        "from [startSlot:%u] to [endSlot:%u] [bad response:%s]",
            _slotStart,
            _slotEnd,
            s.toString().c_str());
        return s;
    }
    // NOTE(takenliu) after deleteRange, cursor seek will scan all the keys in delete range,
    //     so we call compactRange to real delete the keys.
    s = kvstore->compactRange(ColumnFamilyNumber::ColumnFamily_Default, &start, &end);

    if (!s.ok()) {
        serverLog(LL_NOTICE, "DeleteRangeTask::kvstore->compactRange failed,"
                        "from [startSlot:%u] to [endSlot:%u] [bad response:%s]",
            _slotStart,
            _slotEnd,
            s.toString().c_str());
        return s;
    }

    serverLog(LL_VERBOSE, "DeleteRangeTask::deleteRange finished,"
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

}