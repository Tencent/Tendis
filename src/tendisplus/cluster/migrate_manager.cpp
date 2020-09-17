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
Expected<uint64_t> addMigrateBinlog(MigrateBinlogType type, string slots, uint32_t storeid,
        ServerEntry* svr, const string& nodeName = "none") {
    // Temporarily disabled 
    INVARIANT_D(0);
    auto expdb = svr->getSegmentMgr()->getDb(NULL, storeid,
        mgl::LockMode::LOCK_IX);
    if (!expdb.ok()) {
        return expdb.status();
    }
    // NOTE(takenliu) save all info to cmd
    string saveInfo = to_string(type) + "_" + to_string(storeid) + "_" + slots + "_" + nodeName;

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
        LOG(ERROR) << "addMigrateBinlog commit failed:" << eBinlogid.status().toString();
    }
    LOG(INFO) << "addMigrateBinlog storeid:" << storeid
        << " type:" << type << " nodeName:" << nodeName
        << " slots:" << slots << " binlogid:" << eBinlogid.value();
    return eBinlogid;
}

const std::string receTaskTypeString(MigrateReceiveState c) {
    switch (c) {
        case MigrateReceiveState::ERR:
            return "fail";
        case MigrateReceiveState::SUCC:
            return "success";
        case MigrateReceiveState::RECEIVE_BINLOG:
            return "receiving binlog";
        case MigrateReceiveState::RECEIVE_SNAPSHOT:
            return "receiving snaphot";
        case MigrateReceiveState::NONE:
            return  "none";
    }
    return  "unknown";
}

const std::string sendTaskTypeString(MigrateSendState c) {
    switch (c) {
        case MigrateSendState::ERR:
            return "fail";
        case MigrateSendState::START:
            return "sending data";
        case MigrateSendState::CLEAR:
            return "deleting";
        case MigrateSendState::SUCC:
            return "success";
        case MigrateSendState::HALF:
            return "waiting meta change";
        case MigrateSendState::WAIT:
            return "wait to schedule";
        case MigrateSendState::NONE:
            return  "none";
    }
    return  "unknown";
}

MigrateManager::MigrateManager(std::shared_ptr<ServerEntry> svr,
                          const std::shared_ptr<ServerParams> cfg)
    :_cfg(cfg),
     _svr(svr),
     _isRunning(false),
     _taskIdGen(0),
     _migrateSenderMatrix(std::make_shared<PoolMatrix>()),
     _migrateReceiverMatrix(std::make_shared<PoolMatrix>()),
     _rateLimiter(std::make_unique<RateLimiter>(_cfg->binlogRateLimitMB * 1024 * 1024)) {
     _cluster = _svr->getClusterMgr()->getClusterState();
     _cfg->serverParamsVar("migrateSenderThreadnum")->setUpdate([this]() {migrateSenderResize(_cfg->migrateSenderThreadnum);});
     _cfg->serverParamsVar("migrateReceiveThreadnum")->setUpdate([this]() {migrateReceiverResize(_cfg->migrateReceiveThreadnum);});
}

Status MigrateManager::startup() {
    std::lock_guard<myMutex> lk(_mutex);

    // sender's pov
    _migrateSender = std::make_unique<WorkerPool>(
            "migrate-sender", _migrateSenderMatrix);
    Status s = _migrateSender->startup(_cfg->migrateSenderThreadnum);
    if (!s.ok()) {
        return s;
    }

    // receiver's pov
    _migrateReceiver = std::make_unique<WorkerPool>(
            "migrate-receiver", _migrateReceiverMatrix);
    s = _migrateReceiver->startup(_cfg->migrateReceiveThreadnum);
    if (!s.ok()) {
        return s;
    }

    for (uint32_t storeid = 0; storeid < _svr->getKVStoreCount(); storeid++) {
        _restoreMigrateTask[storeid] = std::list<SlotsBitmap>();
    }

    _isRunning.store(true, std::memory_order_relaxed);

    _controller = std::make_unique<std::thread>(std::move([this]() {
        controlRoutine();
    }));
    return { ErrorCodes::ERR_OK, ""};
}

void MigrateManager::stop() {
    LOG(INFO) << "MigrateManager begins stops...";
    _isRunning.store(false, std::memory_order_relaxed);
    _controller->join();

    // make sure all workpool has been stopped; otherwise calling
    // the destructor of a std::thread that is running will crash
    _migrateSender->stop();
    _migrateReceiver->stop();

    LOG(INFO) << "MigrateManager stops succ";
}

Status MigrateManager::stopStoreTask(uint32_t storeid) {
    std::lock_guard<myMutex> lk(_mutex);
    for (auto &iter : _migrateSendTask) {
        if (iter->storeid == storeid) {
            iter->nextSchedTime = SCLOCK::time_point::max();
        }
    }
    for (auto &iter : _migrateReceiveTaskMap) {
        if (iter.second->storeid == storeid) {
            iter.second->nextSchedTime = SCLOCK::time_point::max();
        }
    }

    return { ErrorCodes::ERR_OK, "" };
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
    for (auto it = _migrateSendTask.begin(); it != _migrateSendTask.end(); ) {
        if ((*it)->isRunning || now < (*it)->nextSchedTime) {
            ++it;
            continue;
        }
        doSth = true;
        if ((*it)->state == MigrateSendState::WAIT) {
            SCLOCK::time_point nextSched = SCLOCK::now();
            (*it)->nextSchedTime = nextSched + std::chrono::milliseconds(100);
            ++it;
        } else if ((*it)->state == MigrateSendState::START) {
            (*it)->isRunning = true;
            LOG(INFO) << "MigrateSender schedule on slots ";
            _migrateSender->schedule([this, iter = (*it).get()](){
                sendSlots(iter);
                });
            ++it;
        } else if ((*it)->state == MigrateSendState::CLEAR) {
            (*it)->isRunning = true;
            deleteSenderChunks((*it).get());
            ++it;
        } else if ((*it)->state == MigrateSendState::SUCC
            || (*it)->state == MigrateSendState::ERR) {
            std::string slot = bitsetStrEncode((*it)->slots);
            for (size_t i = 0; i < CLUSTER_SLOTS; i++) {
                if ((*it)->slots.test(i)) {
                    if ((*it)->state == MigrateSendState::SUCC) {
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
            if ((*it)->state == MigrateSendState::SUCC) {
                _succSenderTask.push_back(slot);
            } else {
                _failSenderTask.push_back(slot);
            }
            LOG(INFO) << "erase sender task state:" << sendTaskTypeString((*it)->state)
                << " slots:" << bitsetStrEncode((*it)->slots);
            _migrateSlots ^= ((*it)->slots);
            it = _migrateSendTask.erase(it);
            continue;
        } else if ((*it)->state == MigrateSendState::HALF) {
            // middle state
            // check if metadata change
            if ((*it)->sender->checkSlotsBlongDst()) {
                (*it)->sender->setSenderStatus(MigrateSenderStatus::METACHANGE_DONE);
                (*it)->sender->unlockChunks();
                (*it)->state = MigrateSendState::CLEAR;
            } else {
                // if not change after node timeout, mark fail
               (*it)->sender->unlockChunks();
               (*it)->state = MigrateSendState::ERR;
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

bool MigrateManager::slotsInTask(const SlotsBitmap & bitMap) {
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

void MigrateManager::sendSlots(MigrateSendTask* task) {
    auto s = task->sender->sendChunk();
    SCLOCK::time_point nextSched;
    if (!s.ok()) {
        if (task->sender->needToWaitMetaChanged()) {
            // middle state, wait for 10s( half node timeout) to change
            task->state = MigrateSendState::HALF;
            nextSched = SCLOCK::now() + std::chrono::seconds(10);
        } else  {
            task->state = MigrateSendState::ERR;
            nextSched = SCLOCK::now();
            LOG(ERROR) << "Send slots failed, bitmap is:"
                       << bitsetStrEncode(task->sender->getSlots())
                       << s.toString();
        }
    } else {
        nextSched = SCLOCK::now();
        task->state = MigrateSendState::CLEAR;
    }

    std::lock_guard<myMutex> lk(_mutex);
    task->sender->setClient(nullptr);
    // NOTE(wayenchen) free DB lock
    task->sender->freeDbLock();
    task->nextSchedTime = nextSched;
    task->isRunning = false;
}

void MigrateManager::deleteSenderChunks(MigrateSendTask* task) {
    /* NOTE(wayenchen) check if chunk not belong to me，
    make sure MOVE work well before delete */
    if (!task->sender->checkSlotsBlongDst()) {
        LOG(ERROR) << "slots not belongs to dstNodes on task:"
                   << bitsetStrEncode(task->slots);
        task->state = MigrateSendState::ERR;
    } else {
        auto s = _svr->getGcMgr()->deleteSlotsData(task->slots, task->storeid);
        std::lock_guard<myMutex> lk(_mutex);
        if (!s.ok()) {
            LOG(ERROR) << "sender task delete chunk fail on store:" << task->storeid
                       <<  "slots:" << bitsetStrEncode(task->slots)
                       <<  s.toString();
            /* NOTE(wayenchen) if delete fail, no need retry,
             * gcMgr will delete at last*/
            task->state = MigrateSendState::ERR;
        } else {
            task->sender->setSenderStatus(MigrateSenderStatus::DEL_DONE);
            task->state = MigrateSendState::SUCC;
        }
    }
    task->nextSchedTime = SCLOCK::now();
    task->isRunning = false;
}

Status MigrateManager::migrating(SlotsBitmap slots,
        const string& ip, uint16_t port, uint32_t storeid) {
    std::lock_guard<myMutex> lk(_mutex);
    size_t idx = 0;
    LOG(INFO) << "migrating slot:" << bitsetStrEncode(slots);
    while (idx < slots.size()) {
        if (slots.test(idx)) {
            if (_migrateSlots.test(idx)) {
                LOG(ERROR) << "slot:" << idx << "already be migrating, slots:" << _migrateSlots;
                return { ErrorCodes::ERR_INTERNAL, "already be migrating" };
            } else {
                _migrateSlots.set(idx);
            }
        }
        idx++;
    }

    auto sendTask = std::make_unique<MigrateSendTask>(storeid, slots, _svr, _cfg);
    sendTask->nextSchedTime = SCLOCK::now();
    sendTask->sender->setStoreid(storeid);
    sendTask->sender->setDstAddr(ip, port);
    _migrateSendTask.push_back(std::move(sendTask));
    return { ErrorCodes::ERR_OK, "" };
}

// judge if largeMap contain all slots in smallMap
bool MigrateManager::containSlot(const SlotsBitmap& smallMap, const SlotsBitmap& largeMap) {
    if(smallMap.size() != largeMap.size()) {
        return  false;
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
    _rateLimiter->SetBytesPerSecond((uint64_t)_cfg->migrateRateLimitMB * 1024 * 1024);
    _rateLimiter->Request(bytes);
}

bool MigrateManager::checkSlotOK(const SlotsBitmap& bitMap,
                const std::string& nodeid, std::vector<uint32_t>& taskSlots) {
    CNodePtr dstNode = _cluster->clusterLookupNode(nodeid);
    CNodePtr myself = _cluster->getMyselfNode();
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
            taskSlots.push_back(idx);
        }
        idx++;
    }
    return true;
}

std::string MigrateManager::genTaskid() {
    /* taskid contain importer Nodeid and automic number */
    uint64_t taskNum = _taskIdGen.fetch_add(1);
    return  _cluster->getMyselfName() + std::to_string(taskNum);
}

SlotsBitmap convertMap(const std::vector<uint32_t>& vec) {
    SlotsBitmap map;
    for (const auto& vs : vec) {
        map.set(vs);
    }
    return  map;
}

void MigrateManager::dstPrepareMigrate(asio::ip::tcp::socket sock,
                                const std::string& bitmap,
                                const std::string& nodeid,
                                uint32_t storeNum) {
    std::lock_guard<myMutex> lk(_mutex);
    std::shared_ptr<BlockingTcpClient> client =
            std::move(_svr->getNetwork()->createBlockingClient(
                    std::move(sock), 64*1024*1024));

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
    if (!checkSlotOK(taskMap, nodeid, taskSlots)) {
        std::stringstream ss;
        for (auto &vs : taskSlots) {
            ss << vs << " ";
        }
        LOG(ERROR) << "sender prepare fail when check slots :" << ss.str();
        writer.String("-ERR check");
        writer.EndObject();
        client->writeLine(sb.GetString());
        return;
    }
    //check kvstore count
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

    for (const auto &slot : taskSlots) {
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
    uint16_t taskSize = _svr->getParams()->migrateTaskSlotsLimit;
    writer.Key("taskinfo");
    writer.StartArray();
    for (const auto &v : slotSet) {
        uint32_t storeid = v.first;
        auto slotsVec = v.second;

        uint16_t slotsSize = slotsVec.size();
        for (size_t i = 0; i < slotsSize; i += taskSize) {
            std::vector<uint32_t> vecTask;
            auto last = std::min(slotsVec.size(), i + taskSize);
            vecTask.insert(vecTask.end(), slotsVec.begin() + i, slotsVec.begin() + last);

            writer.StartObject();
            writer.Key("storeid");
            writer.Uint64(storeid);

            writer.Key("migrateSlots");
            writer.StartArray();
            for (auto &v : vecTask)
                writer.Uint64(v);
            writer.EndArray();
            writer.EndObject();

            // migrate task should start before send task information to receiver
            auto s = startTask(convertMap(vecTask), ip, port, storeid, false);
            if (!s.ok()) {
                LOG(ERROR) << "start task on store:" << storeid << "fail";
                startMigate = false;
                break;
            }
            for (const auto &vs :vecTask) {
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
}

void MigrateManager::dstReadyMigrate(asio::ip::tcp::socket sock,
                                       const std::string& slotsArg,
                                       const std::string& StoreidArg,
                                       const std::string& nodeidArg,
                                       const std::string& taskidArg) {
    std::shared_ptr<BlockingTcpClient> client =
            std::move(_svr->getNetwork()->createBlockingClient(
                    std::move(sock), 64*1024*1024));
    if (client == nullptr) {
        LOG(ERROR) << "sender ready with:"
                   <<  nodeidArg
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
    std::lock_guard<myMutex> lk(_mutex);

    //  bitmap from receiver must be in _migrateSlots
    if (!containSlot(receiveMap, _migrateSlots)) {
        std::stringstream ss;
        ss << "-ERR not be migrating:";
        LOG(ERROR) << ss.str();
        client->writeLine(ss.str());
        return;
    }

    // find the sender job and set start, just do once
    bool findJob = false;
    for (auto it = _migrateSendTask.begin(); it != _migrateSendTask.end(); ++it) {
        if ((*it)->sender->getSlots() == receiveMap) {
            findJob = true;
            // send response to srcNode
            std::stringstream ss;
            ss << "+OK";
            client->writeLine(ss.str());
        
            (*it)->sender->setClient(client);
            (*it)->sender->setDstNode(nodeidArg);
            (*it)->sender->setDstStoreid(dstStoreid);
            // set taskid
            (*it)->sender->setTaskId(taskidArg);
            // start migrate task
            (*it)->state = MigrateSendState::START;
            LOG(INFO) << "sender start on taskid:" << taskidArg;
            break;
        }
    }
    if (!findJob) {
        LOG(ERROR) << "findJob failed, store:" << dstStoreid
            << " receiveMap:" << bitsetStrEncode(receiveMap);

        std::stringstream ss;
        ss << "-ERR not be migrating: invalid task bitmap";
        LOG(ERROR) << ss.str();
        client->writeLine(ss.str());
    }
    return;
}

////////////////////////////////////
// receiver POV
///////////////////////////////////
bool MigrateManager::receiverSchedule(const SCLOCK::time_point& now) {
    bool doSth = false;
    for (auto it = _migrateReceiveTaskMap.begin(); it != _migrateReceiveTaskMap.end(); ) {
        auto taskPtr = (*it).second.get();
        if ((taskPtr)->isRunning || now < (taskPtr)->nextSchedTime) {
            ++it;
            continue;
        }

        doSth = true;
        if ((taskPtr)->state == MigrateReceiveState::RECEIVE_SNAPSHOT) {
            (taskPtr)->isRunning = true;
            LOG(INFO) << "receive_snapshot begin";
            _migrateReceiver->schedule([this, iter = taskPtr]() {
                fullReceive(iter);
            });
            ++it;
        } else if (taskPtr->state == MigrateReceiveState::RECEIVE_BINLOG) {
            taskPtr->isRunning = true;
            checkMigrateStatus(taskPtr);
            ++it;
        } else if (taskPtr->state == MigrateReceiveState::SUCC
                    || taskPtr->state == MigrateReceiveState::ERR) {
            std::string slot = bitsetStrEncode(taskPtr->slots);
            for (size_t i = 0; i < CLUSTER_SLOTS; i++) {
                if (taskPtr->slots.test(i)) {
                    if (taskPtr->state == MigrateReceiveState::SUCC) {
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
            if (taskPtr->state == MigrateReceiveState::SUCC) {
                _succReceTask.push_back(slot);
            } else {
                /*NOTE(wayenchen) delete Receiver dirty data in gc*/
                auto s = _svr->getGcMgr()->deleteSlotsData(taskPtr->slots, taskPtr->storeid);
                if (!s.ok()) {
                    //no need retry, gc will do it again
                    LOG(ERROR) << "receiver task delete chunk fail"
                               << s.toString();
                }
                _failReceTask.push_back(slot);
            }
            LOG(INFO) << "erase receiver task stat:"
                      << receTaskTypeString(taskPtr->state)
                      << " slots:" << bitsetStrEncode(taskPtr->slots);
            _importSlots ^= (taskPtr->slots);
            it = _migrateReceiveTaskMap.erase(it);
            continue;
        }
    }
    return doSth;
}

Status MigrateManager::importing(SlotsBitmap slots,
            const std::string& ip, uint16_t port,
            uint32_t storeid, const std::string& taskid) {
    std::lock_guard<myMutex> lk(_mutex);
    std::size_t idx = 0;
    // set slot flag
    LOG(INFO) << "importing slot:" << bitsetStrEncode(slots)
              << " task id is:" << taskid;
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
    auto receiveTask = std::make_unique<MigrateReceiveTask>(slots,
                        storeid, taskid, ip, port, _svr, _cfg);
    receiveTask->nextSchedTime = SCLOCK::now();
    /* taskid should be unique*/
    auto iter = _migrateReceiveTaskMap.find(taskid);
    if (iter != _migrateReceiveTaskMap.end()) {
        LOG(ERROR) << "receiver taskid:" << taskid << "already exists";
        return  {ErrorCodes::ERR_INTERNAL, "taskid already exists"};
    } else {
        _migrateReceiveTaskMap.insert(
                std::make_pair(taskid, std::move(receiveTask)));
    }
    return {ErrorCodes::ERR_OK, ""};
}

Status MigrateManager::startTask(const SlotsBitmap& taskmap,
                        const std::string & ip, uint16_t port,
                        uint32_t storeid, bool importFlag) {

    Status s;
    if (importFlag) {
        /* if it is called by importer, generate a taskid to mark the task*/
        s = importing(taskmap, ip, port, storeid, genTaskid());
        if (!s.ok()) {
            LOG(ERROR) << "start task fail on store:" << storeid;
            return  s;
        }
    } else {
        s = migrating(taskmap, ip, port, storeid);
        if (!s.ok()) {
            LOG(ERROR) << "start task fail on store:" << storeid;
            return s;
        }
    }
    return {ErrorCodes::ERR_OK, ""};
}

void MigrateManager::insertNodes(const std::vector<uint32_t>& slots,
                            const std::string& nodeid, bool importFlag) {
    std::lock_guard<myMutex> lk(_mutex);
    for (auto &vs : slots) {
        if (importFlag) {
            _importNodes[vs] = nodeid;
        } else {
            _migrateNodes[vs] = nodeid;
        }
    }
}

void MigrateManager::checkMigrateStatus(MigrateReceiveTask* task) {
    // TODO(takenliu) : if connect break when receive binlog, reconnect and continue receive.
    std::lock_guard<myMutex> lk(_mutex);
    SCLOCK::time_point nextSched = SCLOCK::now() + std::chrono::seconds(1);
    task->nextSchedTime = nextSched;
    task->isRunning = false;
    auto delay = sinceEpoch() - task->lastSyncTime;
    /* NOTE(wayenchen):sendbinlog beatheat interval is set to 6s,
        so mark 20s as no heartbeat for more than three times*/
    if (delay > 20 * MigrateManager::RETRY_CNT) {
        LOG(ERROR) << "receiver task receive binlog timeout"
                   << " on slots:" << bitsetStrEncode(task->slots);
        task->state = MigrateReceiveState::ERR;
    }
    return;
}

void MigrateManager::fullReceive(MigrateReceiveTask* task) {
    // 2) require a blocking-clients
    std::shared_ptr<BlockingTcpClient> client =
            std::move(createClient(task->srcIp, task->srcPort, _svr));

    LOG(INFO) << "full receive remote_addr("
              << task->srcIp << ":" << task->srcPort
              << ") on slots:" << bitsetStrEncode(task->slots);
    if (client == nullptr) {
        LOG(ERROR) << "fullReceive with: "
                   << task->srcIp << ":"
                   << task->srcPort
                   << " failed, no valid client";
        return;
    }

    task->receiver->setClient(client);

    uint32_t  storeid = task->receiver->getsStoreid();
    /*
     * NOTE: It can't getDb using LocalSession here. Because the IX LOCK
     * would be held all the whole receive process. But LocalSession() 
     * should be destructed soon.
     */
    auto expdb = _svr->getSegmentMgr()->getDb(nullptr, storeid,
        mgl::LockMode::LOCK_IX);
    if (!expdb.ok()) {
        LOG(ERROR) << "get store:" << storeid
            << " failed: " << expdb.status().toString();
        return;
    }
    task->receiver->setDbWithLock(std::make_unique<DbWithLock>(std::move(expdb.value())));

    Status s = task->receiver->receiveSnapshot();
    if (!s.ok()) {
        LOG(ERROR) << "receiveSnapshot failed:" << task->receiver->getSlots().to_string();
        // TODO(takenliu) : clear task, and delete the kv of the chunk.
        return;
    }

    {
        std::lock_guard<myMutex> lk(_mutex);
        task->receiver->setClient(nullptr);
        SCLOCK::time_point nextSched = SCLOCK::now();
        task->state = MigrateReceiveState::RECEIVE_BINLOG;
        task->nextSchedTime = nextSched;
        task->lastSyncTime = sinceEpoch();
        task->isRunning = false;
    }
    // add client to commands schedule
    NetworkAsio *network = _svr->getNetwork();
    INVARIANT(network != nullptr);
    bool migrateOnly = true;
    Expected<uint64_t> expSessionId =
            network->client2Session(std::move(client), migrateOnly);
    if (!expSessionId.ok()) {
        LOG(ERROR) << "client2Session failed:"
                     << expSessionId.status().toString();
        return;
    }

}

Status MigrateManager::applyRepllog(Session* sess, uint32_t storeid, BinlogApplyMode mode,
    const std::string& logKey, const std::string& logValue) {
    std::lock_guard<myMutex> lk(_mutex);
    if (logKey == "") {
        if (logValue != "") {
            /* NOTE(wayenchen) find the binlog task by taskid in migrate heartbeat,
             * set the lastSyncTime of receiver in order to judge if heartbeat timeout*/
            LOG(INFO) << "receive migrate heartbeat on taskid:" << logValue;
            auto iter = _migrateReceiveTaskMap.find(logValue);
            if (iter != _migrateReceiveTaskMap.end()) {
                iter->second->lastSyncTime = sinceEpoch();
            } else {
                LOG(ERROR) << "migrate heartbeat taskid:"
                    << logValue << "not find, may be erase";
            }
        }
    } else {
        auto value = ReplLogValueV2::decode(logValue);
        if (!value.ok()) {
            return value.status();
        }
        // NOTE(takenliu): use the keys chunkid to check
        if (!_importSlots.test(value.value().getChunkId())) {
            LOG(ERROR) << "applyBinlog chunkid err:" << value.value().getChunkId()
                       << "value:" << value.value().getData();
            return {ErrorCodes::ERR_INTERNAL, "chunk not be migrating"};;
        }
        auto binlog = applySingleTxnV2(sess, storeid,
                                       logKey, logValue, mode);
        if (!binlog.ok()) {
            LOG(ERROR) << "applySingleTxnV2 failed:" << binlog.status().toString();
            return binlog.status();
        }
    }
    return{ ErrorCodes::ERR_OK, "" };
}

Status MigrateManager::supplyMigrateEnd(const std::string& taskid, bool binlogDone) {
    SlotsBitmap  slots;
    {
        std::lock_guard<myMutex> lk(_mutex);
        std::string taskResult = binlogDone ? "success" : "failed";

        auto iter = _migrateReceiveTaskMap.find(taskid);
        if (iter != _migrateReceiveTaskMap.end()) {
            slots = iter->second->receiver->getSlots();
            SCLOCK::time_point nextSched = SCLOCK::now();
            iter->second->nextSchedTime = nextSched;
            iter->second->isRunning = false;
            if (!binlogDone) {
                iter->second->state = MigrateReceiveState::ERR;
                return{ ErrorCodes::ERR_OK, "" };
            }
            iter->second->state = MigrateReceiveState::SUCC;
            LOG(INFO) << "supplyMigrateEnd finished on slots:"
                      << bitsetStrEncode(slots)
                      << " taskid: " << taskid
                      << " task result is:" << taskResult;
        } else {
            LOG(ERROR) << "supplyMigrateEnd find task failed,"
                       << " slots:" << bitsetStrEncode(slots)
                       << " taskid:" << taskid
                       << " task result is:" << taskResult;

            return { ErrorCodes::ERR_INTERNAL, "migrating task not find" };
        }
    }

    /* update gossip message and save*/
    auto clusterState = _svr->getClusterMgr()->getClusterState();
    auto s = clusterState->setSlots(clusterState->getMyselfNode(), slots);
    if (!s.ok()) {
        LOG(ERROR) << "setSlots failed, slots:" << bitsetStrEncode(slots)
            << " err:" << s.toString();
        return  {ErrorCodes::ERR_CLUSTER, "set slot myself fail"};
    }

    clusterState->clusterSaveNodes();
    clusterState->clusterUpdateState();
    return{ ErrorCodes::ERR_OK, "" };
}

uint64_t MigrateManager::getProtectBinlogid(uint32_t storeid) {
    std::lock_guard<myMutex> lk(_mutex);
    uint64_t minbinlogid = UINT64_MAX;
    for (auto it = _migrateSendTask.begin(); it != _migrateSendTask.end(); ++it) {
        uint64_t binlogid = (*it)->sender->getProtectBinlogid();
        if ((*it)->storeid == storeid && binlogid < minbinlogid) {
            minbinlogid = binlogid;
        }
    }
    return minbinlogid;
}

Expected<std::string> MigrateManager::getMigrateInfoStr(const SlotsBitmap& slots) {
    std::stringstream stream1;
    std::stringstream stream2;
    std::lock_guard<myMutex> lk(_mutex);
    for (auto iter = _migrateNodes.begin(); iter != _migrateNodes.end(); ++iter) {
        if (slots.test(iter->first)) {
            stream1 << "[" << iter->first
                    << "-<-" << iter->second << "]" << " ";
        }
    }
    for (auto iter = _importNodes.begin(); iter != _importNodes.end(); ++iter) {
        stream2 << "[" << iter->first
                << "->-" << iter->second << "]" << " ";
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

//pass cluster node slots to check
Expected<std::string> MigrateManager::getMigrateInfoStrSimple(const SlotsBitmap& slots) {
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
            if (slots.test(idx)
                && iter != _migrateNodes.end()) {
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
        for (const auto &x : importSlots) {
            stream1 << "[" << bitsetStrEncode(x.second)
                    << "-<-" << x.first << "]" << " ";
        }
    }
    if (migrateSlots.size()) {
        for (const auto &x : migrateSlots) {
            stream2 << "[" << bitsetStrEncode(x.second)
                    << "->-" << x.first << "]" << " ";
        }
    }
    return stream1.str() + " " + stream2.str();
}

SlotsBitmap MigrateManager::getSteadySlots(const SlotsBitmap& slots) {
    std::lock_guard<myMutex> lk(_mutex);
    std::size_t idx = 0;
    SlotsBitmap  tempSlots(slots);
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
    uint16_t isMigrating = (_migrateSlots.count() || _succMigrateSlots.count()
                             || _failMigrateSlots.count()) ? 1 : 0;
    uint16_t isImporting = (_importSlots.count() || _succImportSlots.count()
                            || _failImportSlots.count()) ? 1 : 0;
    commandLen += (isMigrating*6 + isImporting*6);

    Command::fmtMultiBulkLen(ss, commandLen);
    if (isMigrating) {
        std::string migrateSlots = "migrating slots:"
                                   + bitsetStrEncode(_migrateSlots);

        std::string succMSlots = "success migrate slots:"
                                 + bitsetStrEncode(_succMigrateSlots);

        std::string failMSlots = "fail migrate slots:"
                                 + bitsetStrEncode(_failMigrateSlots);

        std::string taskSizeInfo = "running sender task num:" +
                                   std::to_string(_migrateSendTask.size());
        std::string succcInfo = "success sender task num:" +
                                std::to_string(_succSenderTask.size());
        std::string failInfo = "fail sender task num:" +
                               std::to_string(_failSenderTask.size());

        Command::fmtBulk(ss,  migrateSlots);
        Command::fmtBulk(ss, succMSlots);
        Command::fmtBulk(ss, failMSlots);
        Command::fmtBulk(ss, taskSizeInfo);
        Command::fmtBulk(ss, succcInfo);
        Command::fmtBulk(ss, failInfo);
    }

    if (isImporting) {
        std::string importSlots = "importing slots:"
                                  + bitsetStrEncode(_importSlots);

        std::string succISlots = "success import slots:"
                                 + bitsetStrEncode(_succImportSlots);

        std::string failISlots = "fail import slots:"
                                 + bitsetStrEncode(_failImportSlots);

        std::string taskSizeInfo2 = "running receiver task num:" +
                                    std::to_string(_migrateReceiveTaskMap.size());
        std::string succcInfo2 = "success receiver task num:" +
                                  std::to_string(_succReceTask.size());
        std::string failInfo2 = "fail receiver task num:" +
                                  std::to_string(_failReceTask.size());

        Command::fmtBulk(ss, importSlots);
        Command::fmtBulk(ss, succISlots);
        Command::fmtBulk(ss, failISlots);
        Command::fmtBulk(ss, taskSizeInfo2);
        Command::fmtBulk(ss, succcInfo2);
        Command::fmtBulk(ss, failInfo2);
    }

    if (ss.str().size() == 0) {
        return  {ErrorCodes::ERR_WRONG_TYPE, "no task info"};
    }
    return ss.str();
}

Expected<std::string> MigrateManager::getTaskInfo() {
    std::stringstream ss;
    std::lock_guard<myMutex> lk(_mutex);
    uint32_t  totalSize = _migrateSendTask.size()+ _migrateReceiveTaskMap.size()
                    +_failSenderTask.size() +_succSenderTask.size()
                    +_failReceTask.size() +_succReceTask.size();
    Command::fmtMultiBulkLen(ss, totalSize);

    for (auto &vs : _succSenderTask) {
        Command::fmtMultiBulkLen(ss, 2);
        Command::fmtBulk(ss, "sender task slots:" + vs);
        Command::fmtBulk(ss, "taskState:finished");
    }

    for (auto &vs : _failSenderTask) {
        Command::fmtMultiBulkLen(ss, 2);
        Command::fmtBulk(ss, "sender task slots:" + vs);
        Command::fmtBulk(ss, "taskState:failed");
    }
    for (auto &vs : _succReceTask) {
        Command::fmtMultiBulkLen(ss, 2);
        Command::fmtBulk(ss, "receiver task slots:" + vs);
        Command::fmtBulk(ss, "taskState:finished");
    }

    for (auto &vs : _failReceTask) {
        Command::fmtMultiBulkLen(ss, 2);
        Command::fmtBulk(ss, "receiver task slots:" + vs);
        Command::fmtBulk(ss, "taskState:failed");
    }
    //TODO(wayenchen): store the string in MigrateManager
    for (auto it = _migrateSendTask.begin(); it != _migrateSendTask.end(); ) {
        Command::fmtMultiBulkLen(ss, 7);

        std::string slotsInfo = bitsetStrEncode((*it)->slots);

        Command::fmtBulk(ss, "senderTaskSlots:" + slotsInfo);
        
        std::string taskState = "senderTaskState:"
                                    + sendTaskTypeString((*it)->state);
        Command::fmtBulk(ss, taskState);

        std::string snapDone = (*it)->sender->getSenderState() ==
                                MigrateSenderStatus::SNAPSHOT_DONE ? "(finished)" : "(running)";
        std::string binlogDone = (*it)->sender->getSenderState() ==
                                MigrateSenderStatus::BINLOG_DONE ? "(finished)" : "(running)";
        std::string delDone = (*it)->sender->getSenderState() ==
                                MigrateSenderStatus::DEL_DONE ? "(finished)" : "(running)";

        std::string snapshotInfo = "send snapshot keys num: " +
                                    std::to_string((*it)->sender->getSnapshotNum()) + snapDone;
        Command::fmtBulk(ss, snapshotInfo);

        std::string binlogInfo = "send binlog num: "+
                        std::to_string((*it)->sender->getBinlogNum()) + binlogDone;

        Command::fmtBulk(ss, binlogInfo);
        std::string metaInfo = (*it)->sender->getSenderState() ==
                                MigrateSenderStatus::METACHANGE_DONE ? "changed":"unchanged";
        Command::fmtBulk(ss, "metadata:"+ metaInfo);

        std::string delInfo = "delete keys :" + delDone;
        Command::fmtBulk(ss, delInfo);

        std::string consistentInfo = (*it)->sender->getConsistentInfo() ? "OK":"ERROR";
        Command::fmtBulk(ss, "consistent enable:"+ consistentInfo);

        ++it;
    }

    for (auto it = _migrateReceiveTaskMap.begin(); it != _migrateReceiveTaskMap.end(); ) {
        Command::fmtMultiBulkLen(ss, 4);
        std::string taskState = "receiveTaskState:" +
                            receTaskTypeString((it->second)->state);
        std::string slotsInfo = bitsetStrEncode((it->second)->slots);
        slotsInfo.erase(slotsInfo.end()-1);

        Command::fmtBulk(ss, "receiveTaskSlots:" + slotsInfo);
        Command::fmtBulk(ss, taskState);

        std::string snapshotInfo = "receive snapshot keys num:"
                            + std::to_string(it->second->receiver->getSnapshotNum());
        Command::fmtBulk(ss, snapshotInfo);

        std::string binlogInfo = "receive binlog num:"
                            + std::to_string(it->second->receiver->getBinlogNum());

        Command::fmtBulk(ss, binlogInfo);
        ++it;
    }

    if (ss.str().size() == 0) {
        return  {ErrorCodes::ERR_WRONG_TYPE, "no task info"};
    }
    return ss.str();
}

Expected<uint64_t> MigrateManager::applyMigrateBinlog(ServerEntry* svr, PStore store,
        MigrateBinlogType type, string slots, const string& nodeName) {
    SlotsBitmap slotsMap;
    try {
        slotsMap = std::bitset<CLUSTER_SLOTS>(slots);
    } catch (const std::exception& ex) {
        LOG(ERROR) << "parse slots err, slots:" << slots << " err:" << ex.what();
        return {ErrorCodes::ERR_INTERGER, "slots wrong"};
    }
    LOG(INFO) << "applyMigrateBinlog type:" << type << " slots:" << bitsetStrEncode(slotsMap);

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
        uint32_t storeid, string slots) {
    LOG(INFO) << "restoreMigrateBinlog type:" << type
        << " storeid:" << storeid << " slots:" << slots;
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
            iter != _restoreMigrateTask[storeid].end(); iter++) {
            if (*iter == slotsMap) {
                LOG(ERROR) << "_restoreMigrateTask RECEIVE_START already has task:" << slots;
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
                iter != _restoreMigrateTask[storeid].end(); iter++) {
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
                    // NOTE(takenliu): the clusterState change to CLUSTER_OK will need a long time.
                } else {
                    // TODO(takenliu): if dba set slots first restore second.and set slot to dst_restore node,
                    //   it will be node==myself
                    LOG(ERROR) << "restoreMigrateBinlog error, slot:" << slot
                               << " myself:" << myself->getNodeName()
                               << " node:" << node->getNodeName()
                               << " slots:" << slots;
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
                               << " node:" << node->getNodeName()
                               << " slots:" << slots;
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
            iter != _restoreMigrateTask[storeId].end(); iter++) {
            LOG(INFO) << "migrate task has receive_start and has no receive_end,"
                << " so delete keys for slots:"
                << (*iter).to_string();
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
        if (_svr->getSegmentMgr()->getStoreid(chunkid) == storeId
            && !selfSlots.test(chunkid)) {
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
    _migrateSender->resize(size);
}

void MigrateManager::migrateReceiverResize(size_t size) {
    _migrateReceiver->resize(size);
}

size_t MigrateManager::migrateSenderSize() {
    return _migrateSender->size();
}

size_t MigrateManager::migrateReceiverSize() {
    return _migrateReceiver->size();
}

}  // namespace tendisplus
