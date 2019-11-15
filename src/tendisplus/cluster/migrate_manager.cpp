#include "glog/logging.h"
#include "tendisplus/cluster/migrate_manager.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/scopeguard.h"

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

void MigrateManager::stop(){
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

Status MigrateManager::stopChunk(uint32_t chunkid) {
    std::lock_guard<std::mutex> lk(_mutex);

    // TODO:only dont schedule?
    auto iter = _migrateSendTask.find(chunkid);
    if (iter != _migrateSendTask.end()) {
        iter->second->nextSchedTime = SCLOCK::time_point::max();
    }

    auto iter2 = _migrateReceiveTask.find(chunkid);
    if (iter2 != _migrateReceiveTask.end()) {
        iter2->second->nextSchedTime = SCLOCK::time_point::max();
    }

    return { ErrorCodes::ERR_OK, "" };
}

void MigrateManager::controlRoutine(){
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
bool MigrateManager::senderSchedule(const SCLOCK::time_point& now){
    bool doSth = false;
    auto iter = _migrateSendTask.begin();
    while (iter != _migrateSendTask.end()) {
        if (iter->second->isRunning
            || now < iter->second->nextSchedTime) {
            iter++;
            continue;
        }
        doSth = true;
        uint32_t chunkid = iter->first;
        if (iter->second->state == MigrateSendState::WAIT) {
            SCLOCK::time_point nextSched = SCLOCK::now();
            nextSched = nextSched + std::chrono::milliseconds(100);
            iter->second->nextSchedTime = nextSched;
        }
        else if (iter->second->state == MigrateSendState::START)
        {
            iter->second->isRunning = true;
            _migrateSender->schedule([this, chunkid](){
                    sendChunk(chunkid);
                });
        } else if (iter->second->state == MigrateSendState::CLEAR) {
            iter->second->isRunning = true;
            _migrateClear->schedule([this, chunkid](){
                    deleteChunk(chunkid);
                });
        } else if (iter->second->state == MigrateSendState::SUCC
            || iter->second->state == MigrateSendState::ERR){
            // ERR, do what?
            LOG(INFO) << "_migrateSendTask SUCC, erase it, chunkid:" << iter->first;
            iter = _migrateSendTask.erase(iter);
            continue;
        }
        iter++;
    }
    return doSth;
}

void MigrateManager::sendChunk(uint32_t chunkid){
    uint32_t storeid = _svr->getStoreid(chunkid);
    LOG(INFO) << "MigrateManager::sendChunk begin, chunkid:" << chunkid << " storeid:" << storeid;
    _migrateSendTask[chunkid]->sender.setStoreid(storeid);

    auto s = _migrateSendTask[chunkid]->sender.sendChunk();
    if (!s.ok()) {
        LOG(ERROR) << "SendChunk failed, chunkid:" << chunkid << "," << s.toString();
    }

    std::lock_guard<std::mutex> lk(_mutex);
    SCLOCK::time_point nextSched = SCLOCK::now();
    _migrateSendTask[chunkid]->state = s.ok() ? MigrateSendState::CLEAR : MigrateSendState::ERR;
    _migrateSendTask[chunkid]->nextSchedTime = nextSched;
    _migrateSendTask[chunkid]->isRunning = false;
}

void MigrateManager::deleteChunk(uint32_t chunkid){
    auto isOver = _migrateSendTask[chunkid]->sender.deleteChunk(chunkid);

    std::lock_guard<std::mutex> lk(_mutex);
    SCLOCK::time_point nextSched = SCLOCK::now();
    // TODO(takenliu): not over, limit the delete rate?
    _migrateSendTask[chunkid]->nextSchedTime = nextSched;
    _migrateSendTask[chunkid]->isRunning = false;
    if (isOver.value()) {
        _migrateSendTask[chunkid]->state = MigrateSendState::SUCC;
        LOG(INFO) << "deleteChunk over, chunkid:" << chunkid;
    }
}

Status MigrateManager::migrating(uint32_t chunkid, string& ip, uint16_t port) {
    std::lock_guard<std::mutex> lk(_mutex);
    if (_migrateSendTask.find(chunkid) != _migrateSendTask.end()) {
        LOG(ERROR) << "already be migrating:" << chunkid;
        return {ErrorCodes::ERR_INTERNAL, "already be migrating"};
    }
    if (!_svr->isContainChunk(chunkid)) {
        LOG(ERROR) << "chunkid is not mine:" << chunkid;
        return {ErrorCodes::ERR_INTERNAL, "chunk is not mine"};
    }
    _migrateSendTask[chunkid] = std::move(std::unique_ptr<MigrateSendTask>(
            new MigrateSendTask(chunkid, _svr, _cfg)
            ));
    _migrateSendTask[chunkid]->nextSchedTime = SCLOCK::now();
    return {ErrorCodes::ERR_OK, ""};
}

void MigrateManager::dstReadyMigrate(asio::ip::tcp::socket sock,
                                       const std::string& chunkidArg,
                                       const std::string& StoreidArg){
    std::shared_ptr<BlockingTcpClient> client =
            std::move(_svr->getNetwork()->createBlockingClient(
                    std::move(sock), 64*1024*1024));

    uint32_t chunkid;
    uint32_t dstStoreid;
    try {
        chunkid = std::stoul(chunkidArg);
        dstStoreid = std::stoul(StoreidArg);
    } catch (const std::exception& ex) {
        std::stringstream ss;
        ss << "-ERR parse opts failed:" << ex.what();
        LOG(ERROR) << ss.str();
        client->writeLine(ss.str());
        return;
    }

    std::lock_guard<std::mutex> lk(_mutex);
    if (_migrateSendTask.find(chunkid) == _migrateSendTask.end()) {
        std::stringstream ss;
        ss << "-ERR not be migrating:" << chunkid;
        LOG(ERROR) << ss.str();
        client->writeLine(ss.str());
        return;
    }
    std::stringstream ss;
    ss << "+OK";
    client->writeLine(ss.str());

    LOG(INFO) << "MigrateManager::dstReadyMigrate chunkid:" << chunkid;
    _migrateSendTask[chunkid]->state = MigrateSendState::START;
    _migrateSendTask[chunkid]->sender.setClient(client);
    _migrateSendTask[chunkid]->sender.setDstStoreid(dstStoreid);
    return;
}

////////////////////////////////////
// receiver POV
///////////////////////////////////
bool MigrateManager::receiverSchedule(const SCLOCK::time_point& now){
    bool doSth = false;
    auto iter = _migrateReceiveTask.begin();
    while (iter != _migrateReceiveTask.end()) {
        if (iter->second->isRunning
            || now < iter->second->nextSchedTime) {
            iter++;
            continue;
        }

        uint32_t chunkid = iter->first;
        doSth = true;
        if (iter->second->state == MigrateReceiveState::RECEIVE_SNAPSHOT)
        {
            iter->second->isRunning = true;
            _migrateReceiver->schedule([this, chunkid](){
                fullReceive(chunkid);
                });
        } else if (iter->second->state == MigrateReceiveState::RECEIVE_BINLOG) {
            _migrateChecker->schedule([this, chunkid](){
                checkMigrateStatus(chunkid);
            });
        } else if (iter->second->state == MigrateReceiveState::SUCC) {
            LOG(INFO) << "_migrateReceiveTask SUCC, erase it, chunkid:" << iter->first;
            iter = _migrateReceiveTask.erase(iter);
            continue;
        } else if (iter->second->state == MigrateReceiveState::ERR) {
            // TODO
        }
        iter++;
    }
    return doSth;
}

Status MigrateManager::importing(uint32_t chunkid, string& ip, uint16_t port){
    std::lock_guard<std::mutex> lk(_mutex);
    if (_migrateReceiveTask.find(chunkid) != _migrateReceiveTask.end()) {
        LOG(ERROR) << "importing error, chunkid is imported:" << chunkid;
        return {ErrorCodes::ERR_INTERNAL, "chunkid already imported"};
    }
    uint32_t storeid = _svr->getStoreid(chunkid);
    //if (_svr->isContainChunk(chunkid)) { // takenliutest, TODO
    if (false) {
        LOG(ERROR) << "importing error, chunkid is mine:" << chunkid;
        return {ErrorCodes::ERR_INTERNAL, "chunk is mine"};
    }
    if (!_svr->lockChunk(chunkid, "X").ok()) {
        LOG(ERROR) << "importing error, getChunkLock failed:" << chunkid;
        return {ErrorCodes::ERR_INTERNAL, "getChunkLock failed"};
    }
    if (!_svr->deleteChunk(chunkid).ok()) {
        LOG(ERROR) << "importing error, deleteChunk failed:" << chunkid;
        return {ErrorCodes::ERR_INTERNAL, "deleteChunk failed"};
    }
    _migrateReceiveTask[chunkid] = std::move(std::unique_ptr<MigrateReceiveTask>(
            new MigrateReceiveTask(chunkid, storeid, ip, port, _svr, _cfg)));
    _migrateReceiveTask[chunkid]->nextSchedTime = SCLOCK::now();
    return { ErrorCodes::ERR_OK, "" };
}

void MigrateManager::fullReceive(uint32_t chunkid){
    LOG(INFO) << "MigrateManager fullReceive begin:" << chunkid;
    // 2) require a blocking-client
    std::shared_ptr<BlockingTcpClient> client =
        std::move(createClient(_migrateReceiveTask[chunkid]->srcIp,
                _migrateReceiveTask[chunkid]->srcPort, _svr));
    if (client == nullptr) {
        LOG(ERROR) << "fullReceive with: "
                    << _migrateReceiveTask[chunkid]->srcIp << ":"
                    << _migrateReceiveTask[chunkid]->srcPort
                    << " failed, no valid client";
        return;
    }
    _migrateReceiveTask[chunkid]->receiver.setClient(client);

    LocalSessionGuard sg(_svr);
    uint32_t storeid = _svr->getStoreid(chunkid);
    auto expdb = _svr->getSegmentMgr()->getDb(nullptr, storeid,
        mgl::LockMode::LOCK_IX);
    if (!expdb.ok()) {
        LOG(ERROR) << "get store:" << storeid
            << " failed: " << expdb.status().toString();
        return;
    }
    //auto store = std::move(expdb.value().store);
    //_migrateReceiveTask[chunkid].receiver.setDbWithLock(store);
    _migrateReceiveTask[chunkid]->receiver.setDbWithLock(std::make_unique<DbWithLock>(std::move(expdb.value())));

    Status s = _migrateReceiveTask[chunkid]->receiver.receiveSnapshot();
    if (!s.ok()) {
        LOG(ERROR) << "receiveSnapshot failed:" << chunkid;
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
    _migrateReceiveTask[chunkid]->sessionId = expSessionId.value();
    _migrateReceiveTask[chunkid]->receiver.setClient(nullptr);

    std::lock_guard<std::mutex> lk(_mutex);
    SCLOCK::time_point nextSched = SCLOCK::now();
    _migrateReceiveTask[chunkid]->state = MigrateReceiveState::RECEIVE_BINLOG;
    _migrateReceiveTask[chunkid]->nextSchedTime = nextSched;
    _migrateReceiveTask[chunkid]->isRunning = false;
}

void MigrateManager::checkMigrateStatus(uint32_t chunkid){
    // TODO(takenliu) : if connect break when receive binlog, reconnect and continue receive.

    std::lock_guard<std::mutex> lk(_mutex);
    SCLOCK::time_point nextSched = SCLOCK::now() + std::chrono::seconds(1);
    _migrateReceiveTask[chunkid]->nextSchedTime = nextSched;
    return;
}

Status MigrateManager::applyRepllog(Session* sess, uint32_t storeid, uint32_t chunkid,
    const std::string& logKey, const std::string& logValue) {
    uint64_t sessionId;
    {
        std::unique_lock<std::mutex> lk(_mutex);
        if (_migrateReceiveTask.find(chunkid) == _migrateReceiveTask.end()) {
            LOG(ERROR) << "applyBinlog chunkid err:" << chunkid;
            return {ErrorCodes::ERR_INTERNAL, "chunk not be migrating"};;
        }
        _cv.wait(lk,
            [this, chunkid] {
                return !_migrateReceiveTask[chunkid]->isRunning;
            });
        _migrateReceiveTask[chunkid]->isRunning = true;
        sessionId = _migrateReceiveTask[chunkid]->sessionId;
    }
    bool idMatch = sessionId == sess->id();
    auto guard = MakeGuard([this, chunkid, &idMatch] {
        std::unique_lock<std::mutex> lk(_mutex);
        INVARIANT(_migrateReceiveTask[chunkid]->isRunning);
        _migrateReceiveTask[chunkid]->isRunning = false;
        if (idMatch) {
            _migrateReceiveTask[chunkid]->lastSyncTime = SCLOCK::now();
        }
    });
    if (!idMatch) {
        return{ ErrorCodes::ERR_NOTFOUND, "sessionId not match" };
    }

    if (logKey == "") {
        // binlog_heartbeat, do nothing
    } else {
        //LOG(INFO) << "takenliutest: applyBinlog " << chunkid;
        auto s =
            _migrateReceiveTask[chunkid]->receiver.applyBinlog(sess, storeid, chunkid, logKey, logValue);
        if (!s.ok()) {
            return s;
        }
    }
    return{ ErrorCodes::ERR_OK, "" };
}

Status MigrateManager::supplyMigrateEnd(uint32_t chunkid){
    LOG(INFO) << "supplyMigrateEnd begin:" << chunkid;
    {
        std::unique_lock<std::mutex> lk(_mutex);
        if (_migrateReceiveTask.find(chunkid) == _migrateReceiveTask.end()) {
            LOG(ERROR) << "supplyMigrateEnd chunk err:" << chunkid;
            return {ErrorCodes::ERR_INTERNAL, "chunk not be migrating"};
        }
        _cv.wait(lk,
            [this, chunkid] {
                return !_migrateReceiveTask[chunkid]->isRunning;
            });
        _migrateReceiveTask[chunkid]->isRunning = true;
    }

    _svr->updateChunkInfo(chunkid, "is_mine");
    _svr->gossipBroadcast(chunkid, _svr->getNetwork()->getIp(), _svr->getNetwork()->getPort());
    // todo: when migrating, only source db can write, clients cant write.
    _svr->unlockChunk(chunkid, "X");

    std::lock_guard<std::mutex> lk(_mutex);
    _migrateReceiveTask[chunkid]->isRunning = false;
    _migrateReceiveTask[chunkid]->state = MigrateReceiveState::SUCC;
    LOG(INFO) << "supplyMigrateEnd end:" << chunkid;
    return{ ErrorCodes::ERR_OK, "" };
}

}  // namespace tendisplus
