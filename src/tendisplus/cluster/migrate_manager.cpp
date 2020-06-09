#include "glog/logging.h"
#include "tendisplus/cluster/migrate_manager.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/cluster/cluster_manager.h"
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "tendisplus/commands/command.h"
#include <sstream>
#include <vector>

namespace tendisplus {
Expected<uint64_t> addMigrateBinlog(MigrateBinlogType type, string slots, uint32_t storeid,
        ServerEntry* svr, const string& nodeName = "none") {
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


const std::string receTaskTypeString(MigrateReceiveState c)
{
    switch (c) {
        case MigrateReceiveState ::ERR:
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

const std::string sendTaskTypeString(MigrateSendState c)
{
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
     _migrateSenderMatrix(std::make_shared<PoolMatrix>()),
     _migrateClearMatrix(std::make_shared<PoolMatrix>()),
     _migrateReceiverMatrix(std::make_shared<PoolMatrix>()),
     _migrateCheckerMatrix(std::make_shared<PoolMatrix>()) {
     _cluster = _svr->getClusterMgr()->getClusterState();
}

Status MigrateManager::startup() {
    std::lock_guard<std::mutex> lk(_mutex);

    // sender's pov
    _migrateSender = std::make_unique<WorkerPool>(
            "migrate-sender", _migrateSenderMatrix);
    Status s = _migrateSender->startup(_cfg->migrateSenderThreadnum);
    if (!s.ok()) {
        return s;
    }

    _migrateClear = std::make_unique<WorkerPool>(
            "migrate-clear", _migrateClearMatrix);
    s = _migrateClear->startup(_cfg->migrateClearThreadnum);
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

    _migrateChecker = std::make_unique<WorkerPool>(
            "migrate-checker", _migrateCheckerMatrix);
    s = _migrateChecker->startup(_cfg->migrateCheckThreadnum);
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
    _migrateClear->stop();
    _migrateReceiver->stop();
    _migrateChecker->stop();

    LOG(INFO) << "MigrateManager stops succ";
}


Status MigrateManager::stopStoreTask(uint32_t storeid) {
    std::lock_guard<std::mutex> lk(_mutex);
    for (auto &iter: _migrateSendTask) {
        if (iter->storeid == storeid) {
            iter->nextSchedTime = SCLOCK::time_point::max();
        }
    }
    for (auto &iter: _migrateReceiveTask) {
        if (iter->storeid == storeid) {
            iter->nextSchedTime = SCLOCK::time_point::max();
        }
    }

    return { ErrorCodes::ERR_OK, "" };
}

void MigrateManager::controlRoutine() {
    while (_isRunning.load(std::memory_order_relaxed)) {
        bool doSth = false;
        auto now = SCLOCK::now();
        {
            std::lock_guard<std::mutex> lk(_mutex);

            doSth = senderSchedule(now) || doSth;
            doSth = receiverSchedule(now) || doSth;
        }
        if (doSth) {
            std::this_thread::yield();
        } else {
            std::this_thread::sleep_for(10ms);
        }
    }
    LOG(INFO) << "repl controller exits";
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
            LOG(INFO) << "MigrateSender scheule on slots";
            _migrateSender->schedule([this, iter = (*it).get()](){
                    sendSlots(iter);
                });
            ++it;
        } else if ((*it)->state == MigrateSendState::CLEAR) {
            (*it)->isRunning = true;
            _migrateClear->schedule([this, iter = (*it).get()](){
                    deleteChunks(iter);
                });
            ++it;
        } else if ((*it)->state == MigrateSendState::SUCC
            || (*it)->state == MigrateSendState::ERR) {
            // ERR, do what?
            // if err, retry, to do wayen
            std::string slot = bitsetStrEncode((*it)->slots);
            for (size_t i = 0; i < CLUSTER_SLOTS; i++) {
                if ((*it)->slots.test(i)) {
                    if((*it)->state == MigrateSendState::SUCC) {
                        LOG(INFO) << "_migrateSendTask SUCC, erase it, slots" << i;
                        _succMigrateSlots.set(i);
                        if (_failMigrateSlots.test(i)) {
                            _failMigrateSlots.reset(i);
                        }
                    } else {
                        LOG(INFO) << "_migrateSendTask ERROR, erase it, slots" << i;
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
            LOG(INFO) << "erase sender task on slots:" << bitsetStrEncode((*it)->slots);
            _migrateSlots ^= ((*it)->slots);
            _migrateSendTask.erase(it++);
            continue;
        } else if ((*it)->state == MigrateSendState::HALF) {
            // middle state
            // check if metadata change
            if ((*it)->sender->checkSlotsBlongDst((*it)->sender->_slots)) {
                (*it)->sender->setSenderStatus(MigrateSenderStatus::METACHANGE_DONE);
                auto s = _svr->getMigrateManager()->unlockChunks((*it)->slots);
                if (!s.ok()) {
                    LOG(ERROR) << "unlock fail on slots:";
                } else {
                    (*it)->state = MigrateSendState::CLEAR;
                }
            } else {
                // if not change after node timeout, mark fail
                (*it)->state = MigrateSendState::ERR;
            }
            ++it;
        }
    }
    return doSth;
}

Status MigrateManager::lockXChunk(uint32_t chunkid) {
    uint32_t storeId = _svr->getSegmentMgr()->getStoreid(chunkid);
    if (_lockMap.count(chunkid)) {
        LOG(ERROR) << "chunk" + chunkid << "lock already find in map";
        return {ErrorCodes::ERR_CLUSTER, "lock already find"};
    }

    auto lock = std::make_unique<ChunkLock>
            (storeId, chunkid, mgl::LockMode::LOCK_X, nullptr, _svr->getMGLockMgr());

    _lockMap[chunkid] = std::move(lock);
    LOG(INFO) << "lock chunk sucess, chunkid:"<< chunkid << " storeid:" << storeId;
    return  {ErrorCodes::ERR_OK, "finish chunk:"+ dtos(chunkid)+"lock"};
}

Status MigrateManager::lockChunks(const std::bitset<CLUSTER_SLOTS>& slots) {
    std::lock_guard<std::mutex> lk(_mutex);
    size_t idx = 0;
    Status s;
    while (idx < slots.size()) {
        if (slots.test(idx)) {
            s = lockXChunk(idx);
            if (!s.ok()) {
                return  s;
            }
        }
        idx++;
    }
    return  {ErrorCodes::ERR_OK, "finish bitmap lock"};
}

Status MigrateManager::unlockChunks(const std::bitset<CLUSTER_SLOTS>& slots) {
    std::lock_guard<std::mutex> lk(_mutex);
    size_t idx = 0;
    Status s;
    while (idx < slots.size()) {
        if (slots.test(idx)) {
            s = unlockXChunk(idx);
            if (!s.ok()) {
                return  s;
            }
        }
        idx++;
    }
    return  {ErrorCodes::ERR_OK, "finish bitmap unlock"};
}


Status MigrateManager::unlockXChunk(uint32_t chunkid) {
    auto it = _lockMap.find(chunkid);
    if (it != _lockMap.end()) {
        _lockMap.erase(it);
    } else {
        LOG(ERROR) << "chunk" + chunkid << "lock not find in map";
        return {ErrorCodes::ERR_CLUSTER, "lock already find"};
    }
    LOG(INFO) << "unlock chunk sucess, chunkid:"<< chunkid;
    return  {ErrorCodes::ERR_OK, "finish chunk:"+  dtos(chunkid) +"unlock"};
}

bool MigrateManager::slotInTask(uint32_t slot) {
    std::lock_guard<std::mutex> lk(_mutex);
    if (_migrateSlots.test(slot) || _importSlots.test(slot)) {
        return true;
    }
    return false;
}


void MigrateManager::sendSlots(MigrateSendTask* task) {
    auto s = task->sender->sendChunk();
    SCLOCK::time_point nextSched;
    if (!s.ok()) {
        if (s.code() == ErrorCodes::ERR_CLUSTER) {
            //  middle state, wait for 10s( half node timeout) to change
            task->state = MigrateSendState::HALF;
            nextSched = SCLOCK::now() + std::chrono::seconds(10);
        } else  {
            task->state = MigrateSendState::ERR;
            nextSched = SCLOCK::now();
            LOG(ERROR) << "Send slots failed, bitmap is:" << task->sender->_slots.to_string();
        }
    } else {
        task->sender->setSenderStatus(MigrateSenderStatus::METACHANGE_DONE);
        nextSched = SCLOCK::now();
        task->state = MigrateSendState::CLEAR;
    }

    std::lock_guard<std::mutex> lk(_mutex);
    task->nextSchedTime = nextSched;
    task->isRunning = false;
}

void MigrateManager::deleteChunks(MigrateSendTask* task) {
    bool isover = task->sender->deleteChunks(task->slots);

    if (!isover)
        LOG(ERROR) << "delele chunk fail on store:"<< task->sender->getStoreid();
    std::lock_guard<std::mutex> lk(_mutex);
    SCLOCK::time_point nextSched = SCLOCK::now();
    // TODO(takenliu): not over, limit the delete rate?

    task->nextSchedTime = nextSched;
    task->isRunning = false;
    if (isover) {
        task->sender->setSenderStatus(MigrateSenderStatus::DEL_DONE);
        task->state = MigrateSendState::SUCC;
    }

}

Status MigrateManager::migrating(SlotsBitmap slots, string& ip, uint16_t port, uint32_t storeid) {
    std::lock_guard<std::mutex> lk(_mutex);
    size_t idx = 0;
    LOG(INFO) << "migrating slot:" << bitsetStrEncode(slots);
    while (idx < slots.size()) {
         if (slots.test(idx)) {
             // LOG(INFO) << "migrating slot:" << idx;
             if (_migrateSlots.test(idx)) {
                 LOG(ERROR) << "slot:" << idx << "already be migrating" << "bitmap is:" <<_migrateSlots;
                 return {ErrorCodes::ERR_INTERNAL, "already be migrating"};
             } else {
                 _migrateSlots.set(idx);
             }
         }
         idx++;
    }

    auto sendTask = std::make_unique<MigrateSendTask>(storeid, slots, _svr, _cfg);
    sendTask->nextSchedTime = SCLOCK::now();
    sendTask->sender->setStoreid(storeid);
    _migrateSendTask.push_back(std::move(sendTask));

    auto ret = addMigrateBinlog(MigrateBinlogType::SEND_START, slots.to_string(), storeid, _svr.get());
    if (!ret.ok()) {
        LOG(ERROR) << "addMigrateBinlog failed:" << ret.status().toString()
            << " slots:" << bitsetStrEncode(slots);
        return ret.status();
    }
    return {ErrorCodes::ERR_OK, ""};
}

// judge if largeMap contain all slots in smallMap
bool MigrateManager::containSlot(const SlotsBitmap& smallMap, const SlotsBitmap& largeMap) {
    SlotsBitmap temp(smallMap);
    temp |= largeMap;

    return temp == largeMap ? true : false;
}

bool MigrateManager::checkSlotOK(const SlotsBitmap& bitMap,
                const std::string& nodeid, std::vector<uint32_t> &taskSlots) {
    CNodePtr  setNode = _cluster->clusterLookupNode(nodeid);
    CNodePtr  myself = _cluster->getMyselfNode();
    size_t idx = 0;
    while (idx < bitMap.size()) {
        if (bitMap.test(idx)) {
            if (_cluster->getNodeBySlot(idx) == setNode) {
                LOG(ERROR) << "slot:" << idx << "has already migrated to:"
                           << "node:" << nodeid;
                return  false;
            }
            if (slotInTask(idx)) {
                LOG(ERROR) << "migrating task exists in slot:" << idx;
                return false;
            }
            taskSlots.push_back(idx);
        }
        idx ++;
    }
    return true;
}


SlotsBitmap  convertMap(const std::vector<uint32_t>& vec) {
    SlotsBitmap  map;
    for (const auto& vs: vec) {
        map.set(vs);
    }
    return  map;
}

void MigrateManager::prepareSender(asio::ip::tcp::socket sock,
                                const std::string &bitmap,
                                const std::string &nodeidArg,
                                uint32_t storeNum) {
    std::shared_ptr<BlockingTcpClient> client =
            std::move(_svr->getNetwork()->createBlockingClient(
                    std::move(sock), 64*1024*1024));

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

    auto dstNode = _cluster->clusterLookupNode(nodeidArg);
    if (!dstNode) {
        LOG(ERROR) << "import node" << nodeidArg << "not find";
        writer.String("-ERR node not found");
        writer.EndObject();
        client->writeLine(sb.GetString());
        return;
    }
    auto ip = dstNode->getNodeIp();
    auto port = dstNode->getPort();

    // check slots
    std::vector<uint32_t> taskSlots;
    if (!checkSlotOK(taskMap, nodeidArg, taskSlots)) {
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
    uint32_t  mystoreNum = _svr->getKVStoreCount();
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
    for (const auto &slot: taskSlots) {
        uint32_t storeid = _svr->getSegmentMgr()->getStoreid(slot);
        std::unordered_map<uint32_t, std::vector<uint32_t>>::iterator it;
        if ((it= slotSet.find(storeid)) != slotSet.end()) {
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
    for (const auto &v: slotSet) {
        uint32_t storeid = v.first;
        auto slotVec = v.second;

        writer.StartObject();
        writer.Key("storeid");
        writer.Uint64(storeid);

        writer.Key("migrateSlots");
        writer.StartArray();
        for (auto &v : slotVec)
            writer.Uint64(v);
        writer.EndArray();
        writer.EndObject();
        // migrate task should start before send task information to receiver
        auto s = startTask(slotVec, ip, port, storeid, false, taskSize);
        if (!s.ok()) {
            LOG(ERROR) << "start task on store:" << storeid << "fail";
            startMigate = false;
            break;
        }
        for (const auto&vs: slotVec) {
            _migrateNodes[vs] = nodeidArg;
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
                                       const std::string& nodeidArg) {
    std::shared_ptr<BlockingTcpClient> client =
            std::move(_svr->getNetwork()->createBlockingClient(
                    std::move(sock), 256*1024*1024));

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
    std::lock_guard<std::mutex> lk(_mutex);

    //  bitmap from receiver must be in _migrateSlots
    if (!containSlot(receiveMap, _migrateSlots)) {
        std::stringstream ss;
        ss << "-ERR not be migrating:" ;
        LOG(ERROR) << ss.str();
        client->writeLine(ss.str());
        return;
    }
    std::stringstream ss;
    ss << "+OK";
    client->writeLine(ss.str());

    // find the sender job and set start, just do once
    bool findJob = false;
    for (auto it = _migrateSendTask.begin(); it != _migrateSendTask.end(); ++it) {
        if ((*it)->sender->_slots == receiveMap) {
            findJob = true;
            (*it)->state = MigrateSendState::START;
            (*it)->sender->setClient(client);
            (*it)->sender->setDstNode(nodeidArg);
            (*it)->sender->setDstStoreid(dstStoreid);
            break;
        }
    }
    if (!findJob) {
        LOG(ERROR) << "findJob failed, store:" << dstStoreid
                   << " receiveMap:" << bitsetStrEncode(receiveMap);
        for (auto it = _migrateSendTask.begin(); it != _migrateSendTask.end(); ++it) {
            LOG(ERROR) << "findJob failed, _migrateSendTask:"
                << bitsetStrEncode((*it)->sender->_slots);
        }
    }
    return;
}

////////////////////////////////////
// receiver POV
///////////////////////////////////
bool MigrateManager::receiverSchedule(const SCLOCK::time_point& now) {
    bool doSth = false;
    for (auto it = _migrateReceiveTask.begin(); it != _migrateReceiveTask.end(); ) {
        if ((*it)->isRunning || now < (*it)->nextSchedTime) {
            ++it;
            continue;
        }

        doSth = true;
        if ((*it)->state == MigrateReceiveState::RECEIVE_SNAPSHOT) {
            (*it)->isRunning = true;
            LOG(INFO) << "receive_snapshot begin";
            _migrateReceiver->schedule([this, iter = (*it).get()]() {
                fullReceive(iter);
            });
            ++it;
        } else if ((*it)->state == MigrateReceiveState::RECEIVE_BINLOG) {
            _migrateChecker->schedule([this, iter = (*it).get()]() {
                checkMigrateStatus(iter);
            });
            ++it;
        } else if ((*it)->state == MigrateReceiveState::SUCC
                    || (*it)->state == MigrateReceiveState::ERR) {
            std::string slot = bitsetStrEncode((*it)->slots);
            for (size_t i = 0; i < CLUSTER_SLOTS; i++) {
                if ((*it)->slots.test(i)) {
                    if ((*it)->state == MigrateReceiveState::SUCC) {
                        LOG(INFO) << "_migrateReceiveTask SUCC, erase it, slots" << i;
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
            if ((*it)->state == MigrateReceiveState::SUCC) {
                _succReceTask.push_back(slot);
            } else {
                _failReceTask.push_back(slot);
            }
            LOG(INFO) << "erase receiver task on slots:" << bitsetStrEncode((*it)->slots);
            _importSlots ^= ((*it)->slots);
            _migrateReceiveTask.erase(it++);
            continue;
        }
    }
    return doSth;
}

Status MigrateManager::importing(SlotsBitmap slots, std::string& ip, uint16_t port, uint32_t storeid) {
    std::lock_guard<std::mutex> lk(_mutex);
    std::size_t idx = 0;
    // set slot flag
    LOG(INFO) << "importing slot:" << bitsetStrEncode(slots);
    while (idx < slots.size()) {
        if (slots.test(idx)) {
            // LOG(INFO) << "importing slot:" << idx;
            if (_importSlots.test(idx)) {
                LOG(ERROR) << "slot:" << idx << "already be importing" << _importSlots;
                return {ErrorCodes::ERR_INTERNAL, "already be importing"};
            } else {
                _importSlots.set(idx);
            }
        }
        idx++;
    }

    auto receiveTask = std::make_unique<MigrateReceiveTask>(slots, storeid, ip, port,  _svr, _cfg);
    receiveTask->nextSchedTime = SCLOCK::now();
    _migrateReceiveTask.push_back(std::move(receiveTask));

    auto ret = addMigrateBinlog(MigrateBinlogType::RECEIVE_START, slots.to_string(), storeid, _svr.get());
    if (!ret.ok()) {
        LOG(ERROR) << "addMigrateBinlog failed:" << ret.status().toString()
                   << " slots:" << bitsetStrEncode(slots);
        return ret.status();
    }
    return {ErrorCodes::ERR_OK, ""};
}


Status MigrateManager::startTask(const std::vector<uint32_t> slotsVec,
                                 std::string & ip, uint16_t port,
                                 uint32_t storeid, bool importFlag,
                                 uint16_t taskSize) {
    Status s;
    uint16_t slotsSize = slotsVec.size();
    for (size_t i = 0; i < slotsSize; i += taskSize) {
        std::vector<uint32_t > vecSmall;
        auto last = std::min(slotsVec.size(), i + taskSize);
        vecSmall.insert(vecSmall.end(), slotsVec.begin() + i, slotsVec.begin() + last);
        SlotsBitmap  taskmap = convertMap(vecSmall);
        if (importFlag) {
            s = importing(taskmap, ip, port, storeid);
            if (!s.ok()) {
                LOG(ERROR) << "start task fail on store:" << storeid;
                return  s;
            }
        } else {
            s = migrating(taskmap, ip, port, storeid);
            if (!s.ok()) {
                LOG(ERROR) << "start task fail on store:" << storeid;
                return  s;
            }
        }
    }
    return {ErrorCodes::ERR_OK, ""};
}

void MigrateManager::insertNodes(std::vector<uint32_t>slots, std::string nodeid, bool importFlag) {
    std::lock_guard<std::mutex> lk(_mutex);
    for (auto &vs: slots) {
        if (importFlag) {
            _importNodes[vs] = nodeid;
        } else {
            _migrateNodes[vs] = nodeid;
        }
    }
}

void MigrateManager::checkMigrateStatus(MigrateReceiveTask* task) {
    // TODO(takenliu) : if connect break when receive binlog, reconnect and continue receive.
    std::lock_guard<std::mutex> lk(_mutex);
    SCLOCK::time_point nextSched = SCLOCK::now() + std::chrono::seconds(1);
    task->nextSchedTime = nextSched;
    return;
}


void MigrateManager::fullReceive(MigrateReceiveTask* task) {
    // 2) require a blocking-clients
    std::shared_ptr<BlockingTcpClient> client =
            std::move(createClient(task->srcIp, task->srcPort, _svr));

    LOG(INFO) << " full receive remote_addr " << client->getRemoteRepr() << ":"<< client->getRemotePort()
              << "on slots:" << bitsetStrEncode(task->slots);
    if (client == nullptr) {
        LOG(ERROR) << "fullReceive with: "
                   << task->srcIp << ":"
                   << task->srcPort
                   << " failed, no valid client";
        return;
    }
    task->receiver->setClient(client);

    LocalSessionGuard sg(_svr.get());
    uint32_t  storeid = task->receiver->getsStoreid();
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
        //TODO(takenliu) : clear task, and delete the kv of the chunk.
        return;
    }

    {
        std::lock_guard<std::mutex> lk(_mutex);
        task->receiver->setClient(nullptr);
        SCLOCK::time_point nextSched = SCLOCK::now();
        task->state = MigrateReceiveState::RECEIVE_BINLOG;
        task->nextSchedTime = nextSched;
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
    if (logKey == "") {
        // binlog_heartbeat, do nothing
    } else {
        auto value = ReplLogValueV2::decode(logValue);
        if (!value.ok()) {
            return value.status();
        }
        // NOTE(takenliu): use the keys chunkid to check
        if (!_importSlots.test(value.value().getChunkId())) {
            LOG(ERROR) << "applyBinlog chunkid err:" << value.value().getChunkId();
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


Status MigrateManager::supplyMigrateEnd(const SlotsBitmap& slots) {
    LOG(INFO) << "supplyMigrateEnd slots:" << bitsetStrEncode(slots);
    uint32_t storeid = 0;
    {
        std::lock_guard<std::mutex> lk(_mutex);
        if (!containSlot(slots, _importSlots)) {
                LOG(ERROR) << "supplyMigrateEnd bitmap err";
                return {ErrorCodes::ERR_INTERNAL, "slots not be migrating"};
        }

        bool find = false;
        for (auto it = _migrateReceiveTask.begin(); it != _migrateReceiveTask.end(); it++) {
            if ((*it)->receiver->getSlots() == slots) {
                find = true;

                (*it)->state =  MigrateReceiveState::SUCC;
                storeid = (*it)->storeid;
                break;
            }
        }
        if (!find) {
            LOG(ERROR) << "supplyMigrateEnd find slots failed:" << bitsetStrEncode(slots);
            return {ErrorCodes::ERR_INTERNAL, "migrating task not find"};
        }
    }

    /* update gossip message and save*/
    auto clusterState = _svr->getClusterMgr()->getClusterState();
    auto s = clusterState->setSlots(clusterState->getMyselfNode(), slots);
    LOG(INFO) << "set slot metadata begin:" << bitsetStrEncode(slots);
    if (!s.ok()) {
        LOG(ERROR) << "setSlots failed, slots:" << bitsetStrEncode(slots) << " err:" << s.toString();
        return  {ErrorCodes ::ERR_CLUSTER, "set slot myself fail"};
    }

    clusterState->clusterSaveNodes();
    clusterState->clusterUpdateState();
    // TODO(takenliu) add src node name
    auto ret = addMigrateBinlog(MigrateBinlogType::RECEIVE_END, slots.to_string(), storeid, _svr.get());
    if (!ret.ok()) {
        LOG(ERROR) << "addMigrateBinlog failed:" << ret.status().toString()
                   << " slots:" << bitsetStrEncode(slots);
        // return ret.status();  // TODO(takenliu) what should to do?
    }
    return{ ErrorCodes::ERR_OK, "" };
}

uint64_t MigrateManager::getProtectBinlogid(uint32_t storeid) {
    std::lock_guard<std::mutex> lk(_mutex);
    uint64_t minbinlogid = UINT64_MAX;
    for (auto it = _migrateSendTask.begin(); it != _migrateSendTask.end(); ++it) {
        uint64_t binlogid = (*it)->sender->getProtectBinlogid();
        if ((*it)->storeid == storeid && binlogid < minbinlogid) {
            minbinlogid = binlogid;
        }
    }
    return minbinlogid;
}

//pass cluster node slots to check
Expected<std::string> MigrateManager::getMigrateInfoStr(const SlotsBitmap& slots) {
    std::stringstream stream1;
    std::stringstream stream2;
    std::unordered_map<std::string, std::bitset<CLUSTER_SLOTS>> importSlots;
    std::unordered_map<std::string, std::bitset<CLUSTER_SLOTS>> migrateSlots;

    {
        std::lock_guard<std::mutex> lk(_mutex);
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
        for (const auto &x: importSlots) {
            stream1 << "[" << bitsetStrEncode(x.second)
                    << "-<-" << x.first << "]" << " ";
        }
    }
    if (migrateSlots.size()) {
        for (const auto &x: migrateSlots) {
            stream2 << "[" << bitsetStrEncode(x.second)
                    << "->-" << x.first << "]" << " ";
        }
    }
    return stream1.str() + " " + stream2.str();
}


SlotsBitmap MigrateManager::getSteadySlots(const SlotsBitmap& slots) {
    std::lock_guard<std::mutex> lk(_mutex);
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
    std::lock_guard<std::mutex> lk(_mutex);
    std::string migrateSlots = "migrating slots:"
                                    + bitsetStrEncode(_migrateSlots);
    std::string importSlots = "importing slots:"
                                    + bitsetStrEncode(_importSlots);

    std::string succMSlots = "success migrate slots:"
                                    + bitsetStrEncode(_succMigrateSlots);

    std::string failMSlots = "fail migrate slots:"
                                    + bitsetStrEncode(_failMigrateSlots);

    std::string succISlots = "success import slots:"
                                    + bitsetStrEncode(_succImportSlots);

    std::string failISlots = "fail import slots:"
                                    + bitsetStrEncode(_failImportSlots);


    std::stringstream ss;

    std::string taskSizeInfo = "running sender task num:" +
                                std::to_string(_migrateSendTask.size());
    std::string succcInfo = "success sender task num:" +
                                std::to_string(_succSenderTask.size());
    std::string failInfo = "fail sender task num:" +
                                std::to_string(_failSenderTask.size());

    std::string taskSizeInfo2 = "running receiver task num:" +
                               std::to_string(_migrateReceiveTask.size());
    std::string succcInfo2 = "success receiver task num:" +
                            std::to_string(_succReceTask.size());
    std::string failInfo2 = "fail receiver task num:" +
                           std::to_string(_failReceTask.size());

    Command::fmtMultiBulkLen(ss, 12);
    Command::fmtBulk(ss,  migrateSlots);
    Command::fmtBulk(ss,  importSlots);
    Command::fmtBulk(ss, succMSlots);
    Command::fmtBulk(ss, failMSlots);
    Command::fmtBulk(ss, succISlots);
    Command::fmtBulk(ss, failISlots);
    Command::fmtBulk(ss, taskSizeInfo);
    Command::fmtBulk(ss, succcInfo);
    Command::fmtBulk(ss, failInfo);
    Command::fmtBulk(ss, taskSizeInfo2);
    Command::fmtBulk(ss, succcInfo2);
    Command::fmtBulk(ss, failInfo2);

    if (ss.str().size() == 0) {
        return  {ErrorCodes::ERR_WRONG_TYPE, "no task info"};
    }
    return ss.str();
}

Expected<std::string> MigrateManager::getTaskInfo() {
    std::stringstream ss;
    std::lock_guard<std::mutex> lk(_mutex);
    uint32_t  totalSize = _migrateSendTask.size()+ _migrateReceiveTask.size()
                    +_failSenderTask.size() +_succSenderTask.size()
                    +_failReceTask.size() +_succReceTask.size();
    Command::fmtMultiBulkLen(ss, totalSize);

    for (auto &vs: _succSenderTask) {
        Command::fmtMultiBulkLen(ss, 2);
        Command::fmtBulk(ss, "sender task slots:" + vs);
        Command::fmtBulk(ss, "taskState:finished");
    }

    for (auto &vs: _failSenderTask) {
        Command::fmtMultiBulkLen(ss, 2);
        Command::fmtBulk(ss, "sender task slots:" + vs);
        Command::fmtBulk(ss, "taskState:failed");
    }
    for (auto &vs: _succReceTask) {
        Command::fmtMultiBulkLen(ss, 2);
        Command::fmtBulk(ss, "receiver task slots:" + vs);
        Command::fmtBulk(ss, "taskState:finished");
    }

    for (auto &vs: _failReceTask) {
        Command::fmtMultiBulkLen(ss, 2);
        Command::fmtBulk(ss, "receiver task slots:" + vs);
        Command::fmtBulk(ss, "taskState:failed");
    }
    // to do: store the string in MigrateManager (wayen)
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

        std::string delInfo = "delete keys num:" +
                            std::to_string((*it)->sender->getDelNum())+ delDone;
        Command::fmtBulk(ss, delInfo);

        std::string consistentInfo = (*it)->sender->getConsistentInfo() ? "OK":"ERROR";
        Command::fmtBulk(ss, "consistent enable:"+ consistentInfo);

        ++it;
    }

    for (auto it = _migrateReceiveTask.begin(); it != _migrateReceiveTask.end(); ) {
        Command::fmtMultiBulkLen(ss, 4);

        std::string taskState = "receiveTaskState:" + 
                            receTaskTypeString((*it)->state);
        std::string slotsInfo = bitsetStrEncode((*it)->slots);
        slotsInfo.erase(slotsInfo.end()-1);

        Command::fmtBulk(ss, "receiveTaskSlots:" + slotsInfo);
        Command::fmtBulk(ss, taskState);

        std::string snapshotInfo = "receive snapshot keys num:"
                            + std::to_string((*it)->receiver->getSnapshotNum());
        Command::fmtBulk(ss, snapshotInfo);

        std::string binlogInfo = "receive binlog num:"
                            + std::to_string((*it)->receiver->getBinlogNum());

        Command::fmtBulk(ss, binlogInfo);
        ++it;
    }

    if (ss.str().size() == 0) {
        return  {ErrorCodes::ERR_WRONG_TYPE, "no task info"};
    }
    return ss.str();
}

Expected<uint64_t> MigrateManager::applyMigrateBinlog(ServerEntry* svr, PStore store,
        MigrateBinlogType type, string slots, string& nodeName) {
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
        if (_cluster->getMyselfNode()->nodeIsMaster()) {
            LOG(ERROR) << "applyMigrateBinlog error, self is master, slots:" << bitsetStrEncode(slotsMap);
            return {ErrorCodes::ERR_INTERGER, "self is master"};
        }
        // TODO(takenliu) now nodeName hasnot set right ,it's "none"
        /*auto srcnode = _cluster->clusterLookupNode(nodeName);
        if (srcnode == nullptr) {
            LOG(ERROR) << "applyMigrateBinlog error, find node failed:" << nodeName
                       << " slots:" << bitsetStrEncode(slotsMap);
            return {ErrorCodes::ERR_INTERGER, "srcnode find failed"};
        }
        // maybe gossip is earlier come before binlog
        for (size_t i = 0; i < CLUSTER_SLOTS; i++) {
            if (slotsMap.test(i) && _cluster->getNodeBySlot(i) != srcnode) {
                LOG(WARNING) << "slot info not match,slot:" << i
                    << " owner:" << _cluster->getNodeBySlot(i)->getNodeName()
                    << " srcnode:" << nodeName
                    << " slots:" << bitsetStrEncode(slotsMap);
            }
        }*/
        auto s = _cluster->setSlots(_cluster->getMyselfNode()->getMaster(), slotsMap);
        if (!s.ok()) {
            LOG(ERROR) << "setSlots error:" << s.toString();
        }
    } else if (type == MigrateBinlogType::SEND_START) {
        // do nothing
    } else if (type == MigrateBinlogType::SEND_END) {
        if (_cluster->getMyselfNode()->nodeIsMaster()) {
            LOG(ERROR) << "applyMigrateBinlog error, self is master, slots:" << bitsetStrEncode(slotsMap);
            return {ErrorCodes::ERR_INTERGER, "self is master"};
        }
        auto dstnode = _cluster->clusterLookupNode(nodeName);
        if (dstnode == nullptr) {
            LOG(ERROR) << "applyMigrateBinlog error, find node failed:" << nodeName
                       << " slots:" << bitsetStrEncode(slotsMap);
            return {ErrorCodes::ERR_INTERGER, "dstnode find failed"};
        }
        // maybe gossip is earlier come before binlog
        auto master = _cluster->getMyselfNode()->getMaster();
        for (size_t i = 0; i < CLUSTER_SLOTS; i++) {
            if (slotsMap.test(i)) {
                auto node = _cluster->getNodeBySlot(i);
                if (_cluster->getNodeBySlot(i) != master) {
                    LOG(WARNING) << "applyMigrateBinlog, maybe gossip is earlier come before binlog, slot:" << i
                        << " master:" << master->getNodeName() << " owner:" << node->getNodeName()
                        << " slots:" << bitsetStrEncode(slotsMap);
                }
                if (_cluster->getNodeBySlot(i) != master && _cluster->getNodeBySlot(i) != dstnode) {
                    LOG(ERROR) << "applyMigrateBinlog, slot info not match, slot:" << i
                               << " master:" << master->getNodeName() << " owner:" << node->getNodeName()
                               << " slots:" << bitsetStrEncode(slotsMap);
                    // should do what?
                    // return {ErrorCodes::ERR_INTERGER, "slot info not match"};
                }
            }
        }

        auto s = _cluster->setSlots(dstnode, slotsMap);
        if (!s.ok()) {
            LOG(ERROR) << "setSlots error:" << s.toString();
        }
    }
    return {ErrorCodes::ERR_OK, ""};
}

Status MigrateManager::restoreMigrateBinlog(MigrateBinlogType type, uint32_t storeid, string slots) {
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
        std::lock_guard<std::mutex> lk(_mutex);
        for (auto iter = _restoreMigrateTask[storeid].begin(); iter != _restoreMigrateTask[storeid].end(); iter++) {
            if (*iter == slotsMap) {
                LOG(ERROR) << "_restoreMigrateTask RECEIVE_START already has task:" << slots;
                return {ErrorCodes::ERR_INTERGER, "has task"};
            }
        }
        _restoreMigrateTask[storeid].push_back(slotsMap);
        LOG(INFO) << "_restoreMigrateTask RECEIVE_START push_back task:" << bitsetStrEncode(slotsMap);
    } else if (type == MigrateBinlogType::RECEIVE_END) {
        // clear temp save slots
        {
            std::lock_guard<std::mutex> lk(_mutex);
            bool find = false;
            for (auto iter = _restoreMigrateTask[storeid].begin(); iter != _restoreMigrateTask[storeid].end(); iter++) {
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
                    // TODO(takenliu): do what ?
                    LOG(ERROR) << "restoreMigrateBinlog error, slot:" << slot
                               << " myself:" << myself->getNodeName() << " node:" << node->getNodeName()
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
                    // NOTE(takenliu): when restore cant get the dst node, so slot cant be set, need gossip to notify.
                } else if (node != nullptr) {
                    // TODO(takenliu): do what ?
                    LOG(ERROR) << "restoreMigrateBinlog error, slot:" << slot
                               << " myself:" << myself->getNodeName() << " node:" << node->getNodeName()
                               << " slots:" << slots;
                } else {
                    LOG(INFO) << "restoreMigrateBinlog slot has no node, slot:" << slot
                        << " slots:" << slots;
                }
            }
        }
        // lockchunk
        asyncDeleteChunks(storeid, slotsMap);
        // unlockchunk
    }
    return {ErrorCodes::ERR_OK, ""};
}

Status MigrateManager::onRestoreEnd(uint32_t storeId) {
    {
        std::lock_guard<std::mutex> lk(_mutex);
        for (auto iter = _restoreMigrateTask[storeId].begin(); iter != _restoreMigrateTask[storeId].end(); iter++) {
            LOG(INFO) << "migrate task has receive_start and has no receive_end, so delete keys for slots:"
                << (*iter).to_string();
            asyncDeleteChunksInLock(storeId, *iter);
        }
    }

    // NOTE(takenliu) for the below case, need clear dont contain slots keys, for simple we clear anymore.
    // 1. for source node, backup maybe done when do deleteChunk(), and restorebinlog will dont has SEND_START and SEND_END
    //     so, we need clear the keys which deleteChunk() not finished.
    // 2. for dst node, backup maybe done between RECEIVE_START and RECEIVE_END, and restorebinlog end timestamp is before RECEIVE_END
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
    asyncDeleteChunks(storeId, dontContainSlots);
    return {ErrorCodes::ERR_OK, ""};
}

Status MigrateManager::asyncDeleteChunks(uint32_t storeid, const SlotsBitmap& slots) {
    std::lock_guard<std::mutex> lk(_mutex);
    return asyncDeleteChunksInLock(storeid, slots);
}

Status MigrateManager::asyncDeleteChunksInLock(uint32_t storeid, const SlotsBitmap& slots) {
    // TODO(takenliu) should lock first, then thread process, unlock when callback.

    // TODO(takenliu) 1.add slots to _migrateSlots
    //     2._importSlots nedd add too, avoid import request
    //     3.ChunkMigrateReceiver need deleteChunk when migrate failed.
    auto sendTask = std::make_unique<MigrateSendTask>(storeid, slots, _svr, _cfg);
    sendTask->nextSchedTime = SCLOCK::now();
    sendTask->sender->setStoreid(storeid);
    sendTask->state = MigrateSendState::CLEAR;
    _migrateSendTask.push_back(std::move(sendTask));
    LOG(INFO) << "asyncDeleteChunksWithLock _migrateSendTask add fake task, store:" << storeid
              << " slots:" << bitsetStrEncode(slots);
    return {ErrorCodes::ERR_OK, ""};
}

/*
Status MigrateManager::deleteChunksWithLock(const SlotsBitmap& slots) {
    // NOTE(takenliu) need lock all chunk first.
    std::list<std::unique_ptr<ChunkLock>> locklist;
    for (size_t chunkid = 0; chunkid < CLUSTER_SLOTS; ++chunkid) {
        uint32_t storeId = _svr->getSegmentMgr()->getStoreid(chunkid);
        auto lock = std::make_unique<ChunkLock>
                (storeId, chunkid, mgl::LockMode::LOCK_X, nullptr, _svr->getMGLockMgr());
        locklist.push_back(std::move(lock));
    }
    uint64_t deleteNum = 0;
    for (size_t chunkid = 0; chunkid < CLUSTER_SLOTS; ++chunkid) {
        if (slots.test(chunkid)) {
            auto v = deleteChunkInLock(chunkid);
            if (!v.ok()) {
                LOG(ERROR) << "delete slot:" << chunkid << "fail";
                return {ErrorCodes::ERR_INTERGER, "deleteChunkInLock failed"};
            }
            deleteNum += v.value();
        }
    }
    LOG(INFO) << "deleteChunksWithLock deleteNum:" << deleteNum << " slots:" << bitsetStrEncode(slots);
    return {ErrorCodes::ERR_OK, ""};
}

Expected<uint64_t> MigrateManager::deleteChunkInLock(uint32_t  chunkid) {
    uint32_t storeId = _svr->getSegmentMgr()->getStoreid(chunkid);
    LOG(INFO) << "deleteChunk begin on chunkid:" << chunkid << " storeId:" << storeId;

    auto expdb = _svr->getSegmentMgr()->getDb(NULL, storeId,
  mgl::LockMode::LOCK_IX);
    if (!expdb.ok()) {
        LOG(ERROR) << "deleteChunkInLock getDb failed, chunkid:" << chunkid << " storeId:" << storeId;
        return expdb.status();
    }
    PStore kvstore = expdb.value().store;
    auto ptxn = kvstore->createTransaction(NULL);
    if (!ptxn.ok()) {
        return ptxn.status();
    }
    auto cursor = std::move(ptxn.value()->createSlotsCursor(chunkid, chunkid+1));
    bool over = false;
    uint32_t deleteNum = 0;

    while (true) {
        Expected<Record> expRcd = cursor->next();
        if (expRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
            over = true;
            break;
        }

        if (!expRcd.ok()) {
            LOG(ERROR) << "delete cursor error on chunkid:" << chunkid;
            return false;
        }

        Record &rcd = expRcd.value();
        const RecordKey &rcdKey = rcd.getRecordKey();

        auto s = ptxn.value()->delKV(rcdKey.encode());
        if (!s.ok()) {
            LOG(ERROR) << "delete key fail";
            continue;
        }
        deleteNum++;
    }

    auto s = ptxn.value()->commit();
    // todo: retry_times
    if (!s.ok()) {
        return s.status();
    }
    LOG(INFO) << "deleteChunk chunkid:" << chunkid
              << " num:" << deleteNum << " is_over:" << over;
    return deleteNum;
}
*/
}  // namespace tendisplus
