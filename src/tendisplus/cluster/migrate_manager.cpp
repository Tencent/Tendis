#include "glog/logging.h"
#include "tendisplus/cluster/migrate_manager.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/cluster/cluster_manager.h"


namespace tendisplus {

MigrateManager::MigrateManager(std::shared_ptr<ServerEntry> svr,
                          const std::shared_ptr<ServerParams> cfg)
    :_cfg(cfg),
     _svr(svr),
     _isRunning(false),
     _migrateSenderMatrix(std::make_shared<PoolMatrix>()),
     _migrateClearMatrix(std::make_shared<PoolMatrix>()),
     _migrateReceiverMatrix(std::make_shared<PoolMatrix>()),
     _migrateCheckerMatrix(std::make_shared<PoolMatrix>()) {
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
        //uint32_t chunkid = iter->first;
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
            if ((*it)->state == MigrateSendState::SUCC) {
                LOG(INFO) << "_migrateSendTask SUCC, erase it";
                for (size_t i = 0; i < CLUSTER_SLOTS; i++) {
                    if ((*it)->slots.test(i)) {
                        LOG(INFO) << "_migrateSendTask SUCC, erase it, slots" << i;
                    }
                }
            }
            _importSlots ^= ((*it)->slots);
            _migrateSendTask.erase(it++);
            continue;
        } else if ((*it)->state == MigrateSendState::HALF) {
            // middle state
            // check if metadata change
            if ((*it)->sender->checkSlotsBlongDst((*it)->sender->_slots)) {
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
    LOG(INFO) << "finish lock chunk on :"<< chunkid << "storeid:" << storeId;
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
    LOG(INFO) << "finish unlock chunk on :"<< chunkid;
    return  {ErrorCodes::ERR_OK, "finish chunk:"+  dtos(chunkid) +"unlock"};
}


void MigrateManager::sendSlots(MigrateSendTask* task) {
    auto s = task->sender->sendChunk();
    SCLOCK::time_point nextSched;
    if (!s.ok()) {
        if (s.code() == ErrorCodes::ERR_CLUSTER) {
            //  middle state, wait for 8s(node timeout) to change
            task->state = MigrateSendState::HALF;
            nextSched = SCLOCK::now() + std::chrono::seconds(8);
        } else  {
            task->state = MigrateSendState::ERR;
            nextSched = SCLOCK::now();
            LOG(ERROR) << "Send slots failed, bitmap is:" << task->sender->_slots.to_string();
        }
    } else {
        nextSched = SCLOCK::now();
        task->state = MigrateSendState::CLEAR;
    }

    std::lock_guard<std::mutex> lk(_mutex);
    task->nextSchedTime = nextSched;
    task->isRunning = false;
}



void MigrateManager::deleteChunks(MigrateSendTask* task) {
    bool isover = task->sender->deleteChunks(task->slots);

    std::lock_guard<std::mutex> lk(_mutex);
    SCLOCK::time_point nextSched = SCLOCK::now();
    // TODO(takenliu): not over, limit the delete rate?
    task->nextSchedTime = nextSched;
    task->isRunning = false;
    if (isover) {
        task->state = MigrateSendState::SUCC;
    }
}


Status MigrateManager::migrating(SlotsBitmap slots, string& ip, uint16_t port, uint32_t storeid) {
    std::lock_guard<std::mutex> lk(_mutex);

    std::size_t idx = 0;
    while (idx < slots.size()) {
         if (slots.test(idx)) {
             LOG(INFO) << "migrating idx=:" << idx;
             if (_migrateSlots.test(idx)) {
                 return {ErrorCodes::ERR_INTERNAL, "already be migrating"};
             } else {
                 _migrateSlots.set(idx);
                 LOG(INFO) << "_migrateSlots.test" << idx << _migrateSlots.test(idx);
             }
         }
         idx++;
    }

    auto sendTask = std::make_unique<MigrateSendTask>(slots, _svr, _cfg);
    sendTask->nextSchedTime = SCLOCK::now();
    sendTask->sender->setStoreid(storeid);
    _migrateSendTask.push_back(std::move(sendTask));

    return {ErrorCodes::ERR_OK, ""};
}

// judge if largeMap contain all slots in smallMap
bool MigrateManager::containSlot(const SlotsBitmap& smallMap, const SlotsBitmap& largeMap) {
    SlotsBitmap temp(smallMap);
    temp |= largeMap;

    return temp == largeMap ? true : false;
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
    if (!findJob)
        LOG(ERROR) << "fail to start the job";
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
            _migrateReceiver->schedule([this, iter = (*it).get()](){
                fullReceive(iter);
                });
            ++it;
        } else if ((*it)->state == MigrateReceiveState::RECEIVE_BINLOG) {
            _migrateChecker->schedule([this, iter = (*it).get()](){
                checkMigrateStatus(iter);
            });
            ++it;
        } else if ((*it)->state == MigrateReceiveState::SUCC) {
            for (size_t i=0; i < CLUSTER_SLOTS; i++) {
                if ((*it)->slots.test(i)) {
                    LOG(INFO) << "_migrateReceiveTask SUCC, erase it, slots"<< i;
                }
            }
            _importSlots ^= ((*it)->slots);
            _migrateReceiveTask.erase(it++);
            continue;
        } else if ((*it)->state == MigrateReceiveState::ERR) {
            _importSlots ^= ((*it)->slots);
            _migrateReceiveTask.erase(it++);
        }
    }
    return doSth;
}


Status MigrateManager::importing(SlotsBitmap slots, std::string& ip, uint16_t port, uint32_t storeid) {
    std::lock_guard<std::mutex> lk(_mutex);

    std::size_t idx = 0;
    // set slot flag
    while (idx < slots.size()) {
        if (slots.test(idx)) {
            LOG(INFO) << "import idx:=" << idx;
            if (_importSlots.test(idx)) {
                return {ErrorCodes::ERR_INTERNAL, "already be importing"};
            } else {
                _importSlots.set(idx);
                LOG(INFO) << "_importSlots.test" << idx << _importSlots.test(idx);
            }
        }
        idx++;
    }

    auto receiveTask = std::make_unique<MigrateReceiveTask>(slots, storeid, ip, port,  _svr, _cfg);
    receiveTask->nextSchedTime = SCLOCK::now();
    _migrateReceiveTask.push_back(std::move(receiveTask));
    LOG(INFO) << "consumer queue tasks:" << _migrateReceiveTask.size();

    return {ErrorCodes::ERR_OK, ""};
}



void MigrateManager::checkMigrateStatus(MigrateReceiveTask* task) {
    // TODO(takenliu) : if connect break when receive binlog, reconnect and continue receive.
    std::lock_guard<std::mutex> lk(_mutex);
    SCLOCK::time_point nextSched = SCLOCK::now() + std::chrono::seconds(1);
    task->nextSchedTime = nextSched;
    return;
}


void MigrateManager::fullReceive(MigrateReceiveTask* task) {
    LOG(INFO) << "MigrateManager fullReceive begin:";
    // 2) require a blocking-client

    std::shared_ptr<BlockingTcpClient> client =
        std::move(createClient(task->srcIp, task->srcPort, _svr));

    LOG(INFO) << " full receive remote_addr " << client->getRemoteRepr() << ":"<< client->getRemotePort();
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

    task->receiver->setClient(nullptr);

    std::lock_guard<std::mutex> lk(_mutex);
    SCLOCK::time_point nextSched = SCLOCK::now();
    task->state = MigrateReceiveState::RECEIVE_BINLOG;
    task->nextSchedTime = nextSched;
    task->isRunning = false;
}


Status MigrateManager::applyRepllog(Session* sess, uint32_t storeid, uint32_t chunkid,
    const std::string& logKey, const std::string& logValue) {

    if (!_importSlots.test(chunkid)) {
        LOG(ERROR) << "applyBinlog chunkid err:" << chunkid;
        return {ErrorCodes::ERR_INTERNAL, "chunk not be migrating"};;
    }

    if (logKey == "") {
        // binlog_heartbeat, do nothing
    } else {
        auto binlog = applySingleTxnV2(sess, storeid,
                                       logKey, logValue, chunkid);
        if (!binlog.ok()) {
            return binlog.status();
        }
    }
    return{ ErrorCodes::ERR_OK, "" };
}



Status MigrateManager::supplyMigrateEnd(const SlotsBitmap& slots) {
    std::lock_guard<std::mutex> lk(_mutex);
    if (!containSlot(slots, _importSlots)) {
            LOG(ERROR) << "supplyMigrateEnd bitmap err";
            return {ErrorCodes::ERR_INTERNAL, "slots not be migrating"};
    }

    // todo: when migrating, only source db can write, clients cant write.

    /* update gossip message and save*/
    auto clusterState = _svr->getClusterMgr()->getClusterState();
    auto s = clusterState->setSlots(clusterState->getMyselfNode(), slots);
    if (!s.ok()) {
        return  {ErrorCodes ::ERR_CLUSTER, "set slot myself fail"};
    }
    clusterState->clusterSaveNodes();
    clusterState->clusterUpdateState();


    for (auto it = _migrateReceiveTask.begin(); it != _migrateReceiveTask.end(); it++) {
        if ((*it)->receiver->getSlots() == slots) {
            (*it)->isRunning = false;
            (*it)->state =  MigrateReceiveState::SUCC;
            break;
        }
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

}  // namespace tendisplus
