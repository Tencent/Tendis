#include <list>
#include <chrono>
#include <algorithm>
#include <fstream>
#include <string>
#include <set>
#include <map>
#include <limits>

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/error/en.h"
#include "glog/logging.h"
#include "tendisplus/replication/repl_manager.h"
#include "tendisplus/storage/record.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/utils/invariant.h"

namespace tendisplus {

ReplManager::ReplManager(std::shared_ptr<ServerEntry> svr)
    :_isRunning(false),
     _svr(svr),
     _firstBinlogId(0),
     _clientIdGen(0),
     _fullPushMatrix(std::make_shared<PoolMatrix>()),
     _incrPushMatrix(std::make_shared<PoolMatrix>()),
     _fullReceiveMatrix(std::make_shared<PoolMatrix>()),
     _incrCheckMatrix(std::make_shared<PoolMatrix>()) {
}

Status ReplManager::startup() {
    std::lock_guard<std::mutex> lk(_mutex);
    Catalog *catalog = _svr->getCatalog();
    INVARIANT(catalog != nullptr);

    for (uint32_t i = 0; i < KVStore::INSTANCE_NUM; i++) {
        Expected<std::unique_ptr<StoreMeta>> meta = catalog->getStoreMeta(i);
        if (meta.ok()) {
            _syncMeta.emplace_back(std::move(meta.value()));
        } else if (meta.status().code() == ErrorCodes::ERR_NOTFOUND) {
            auto pMeta = std::unique_ptr<StoreMeta>(
                new StoreMeta(i, "", 0, -1,
                    Transaction::MAX_VALID_TXNID+1, ReplState::REPL_NONE));
            Status s = catalog->setStoreMeta(*pMeta);
            if (!s.ok()) {
                return s;
            }
            _syncMeta.emplace_back(std::move(pMeta));
        } else {
            return meta.status();
        }
    }

    INVARIANT(_syncMeta.size() == KVStore::INSTANCE_NUM);

    for (size_t i = 0; i < _syncMeta.size(); ++i) {
        if (i != _syncMeta[i]->id) {
            std::stringstream ss;
            ss << "meta:" << i << " has id:" << _syncMeta[i]->id;
            return {ErrorCodes::ERR_INTERNAL, ss.str()};
        }
    }

    for (uint32_t i = 0; i < KVStore::INSTANCE_NUM; i++) {
        _syncStatus.emplace_back(
            std::unique_ptr<SPovStatus>(
                new SPovStatus {
                    isRunning: false,
                    sessionId: std::numeric_limits<uint64_t>::max(),
                    nextSchedTime: SCLOCK::now(),
                    lastSyncTime: SCLOCK::now(),
                }));
    }

    // TODO(deyukong): configure
    size_t cpuNum = std::thread::hardware_concurrency();
    if (cpuNum == 0) {
        return {ErrorCodes::ERR_INTERNAL, "cpu num cannot be detected"};
    }

    _incrPusher = std::make_unique<WorkerPool>(_incrPushMatrix);
    Status s = _incrPusher->startup(std::max(POOL_SIZE, cpuNum/2));
    if (!s.ok()) {
        return s;
    }

    _fullPusher = std::make_unique<WorkerPool>(_fullPushMatrix);
    s = _fullPusher->startup(std::max(POOL_SIZE/2, cpuNum/4));
    if (!s.ok()) {
        return s;
    }

    _fullReceiver = std::make_unique<WorkerPool>(_fullReceiveMatrix);
    s = _fullReceiver->startup(std::max(POOL_SIZE/2, cpuNum/4));
    if (!s.ok()) {
        return s;
    }

    _incrChecker = std::make_unique<WorkerPool>(_incrCheckMatrix);
    s = _incrChecker->startup(2);
    if (!s.ok()) {
        return s;
    }

    // init master's pov, incrpush status
    for (uint32_t i = 0; i < KVStore::INSTANCE_NUM; i++) {
        _pushStatus.emplace_back(
            std::map<uint64_t, std::unique_ptr<MPovStatus>>());
    }

    // init first binlogpos, empty binlogs makes cornercase complicated.
    // so we put an no-op to binlogs everytime startup.
    for (uint32_t i = 0; i < KVStore::INSTANCE_NUM; i++) {
        PStore store = _svr->getSegmentMgr()->getInstanceById(i);
        INVARIANT(store != nullptr);

        auto ptxn = store->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }

        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        RecordKey rk(0, RecordType::RT_META, "NOOP", "");
        RecordValue rv("");
        Status putStatus = store->setKV(rk, rv, txn.get());
        if (!putStatus.ok()) {
            return putStatus;
        }
        Expected<uint64_t> commitStatus = txn->commit();
        if (!commitStatus.ok()) {
            return commitStatus.status();
        }

        ptxn = store->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }

        txn = std::move(ptxn.value());
        std::unique_ptr<BinlogCursor> cursor =
            txn->createBinlogCursor(Transaction::MIN_VALID_TXNID);
        Expected<ReplLog> explog = cursor->next();
        if (explog.ok()) {
            const auto& rlk = explog.value().getReplLogKey();
            _firstBinlogId.emplace_back(rlk.getTxnId());
        } else {
            INVARIANT(explog.status().code() != ErrorCodes::ERR_EXHAUST);
            return explog.status();
        }
    }

    INVARIANT(_firstBinlogId.size() == KVStore::INSTANCE_NUM);

    _isRunning.store(true, std::memory_order_relaxed);
    _controller = std::thread([this]() {
        controlRoutine();
    });

    return {ErrorCodes::ERR_OK, ""};
}

void ReplManager::changeReplStateInLock(const StoreMeta& storeMeta,
                                        bool persist) {
    // TODO(deyukong): mechanism to INVARIANT mutex held
    if (persist) {
        Catalog *catalog = _svr->getCatalog();
        Status s = catalog->setStoreMeta(storeMeta);
        if (!s.ok()) {
            LOG(FATAL) << "setStoreMeta failed:" << s.toString();
        }
    }
    _syncMeta[storeMeta.id] = std::move(storeMeta.copy());
}

void ReplManager::changeReplState(const StoreMeta& storeMeta,
                                        bool persist) {
    std::lock_guard<std::mutex> lk(_mutex);
    changeReplStateInLock(storeMeta, persist);
}

std::shared_ptr<BlockingTcpClient> ReplManager::createClient(
                    const StoreMeta& metaSnapshot) {
    std::shared_ptr<BlockingTcpClient> client =
        std::move(_svr->getNetwork()->createBlockingClient(64*1024*1024));
    Status s = client->connect(
        metaSnapshot.syncFromHost,
        metaSnapshot.syncFromPort,
        std::chrono::seconds(3));
    if (!s.ok()) {
        LOG(WARNING) << "connect " << metaSnapshot.syncFromHost
            << ":" << metaSnapshot.syncFromPort << " failed:"
            << s.toString();
        return nullptr;
    }

    std::shared_ptr<std::string> masterauth = _svr->masterauth();
    if (*masterauth != "") {
        std::stringstream ss;
        ss << "AUTH " << *masterauth;
        client->writeLine(ss.str(), std::chrono::seconds(1));
        Expected<std::string> s = client->readLine(std::chrono::seconds(1));
        if (!s.ok()) {
            LOG(WARNING) << "fullSync auth error:" << s.status().toString();
            return nullptr;
        }
        if (s.value().size() == 0 || s.value()[0] == '-') {
            LOG(INFO) << "fullSync auth failed:" << s.value();
            return nullptr;
        }
    }
    return std::move(client);
}

void ReplManager::slaveStartFullsync(const StoreMeta& metaSnapshot) {
    LOG(INFO) << "store:" << metaSnapshot.id << " fullsync start";

    // 1) stop store and clean it's directory
    PStore store = _svr->getSegmentMgr()->getInstanceById(metaSnapshot.id);
    INVARIANT(store != nullptr);

    Status stopStatus = store->stop();
    if (!stopStatus.ok()) {
        // there may be uncanceled transactions binding with the store
        LOG(WARNING) << "stop store:" << metaSnapshot.id
                    << " failed:" << stopStatus.toString();
        return;
    }
    INVARIANT(!store->isRunning());
    Status clearStatus =  store->clear();
    if (!clearStatus.ok()) {
        LOG(FATAL) << "Unexpected store:" << metaSnapshot.id << " clear"
            << " failed:" << clearStatus.toString();
    }

    // 2) require a sync-client
    std::shared_ptr<BlockingTcpClient> client =
        std::move(createClient(metaSnapshot));
    if (client == nullptr) {
        LOG(WARNING) << "startFullSync with: "
                    << metaSnapshot.syncFromHost << ":"
                    << metaSnapshot.syncFromPort
                    << " failed, no valid client";
        return;
    }

    // 3) necessary pre-conditions all ok, startup a guard to rollback
    // state if failed
    bool rollback = true;
    auto guard = MakeGuard([this, &rollback, &metaSnapshot]{
        std::lock_guard<std::mutex> lk(_mutex);
        if (rollback) {
            auto newMeta = metaSnapshot.copy();
            newMeta->replState = ReplState::REPL_CONNECT;
            newMeta->binlogId = Transaction::MAX_VALID_TXNID+1;
            changeReplStateInLock(*newMeta, false);
        }
    });

    // 4) read backupinfo from master
    std::stringstream ss;
    ss << "FULLSYNC " << metaSnapshot.syncFromId;
    client->writeLine(ss.str(), std::chrono::seconds(1));
    Expected<std::string> s = client->readLine(std::chrono::seconds(3));
    if (!s.ok()) {
        LOG(WARNING) << "fullSync req master error:" << s.status().toString();
        return;
    }

    if (s.value().size() == 0 || s.value()[0] == '-') {
        LOG(INFO) << "fullSync req master failed:" << s.value();
        return;
    }

    auto newMeta = metaSnapshot.copy();
    newMeta->replState = ReplState::REPL_TRANSFER;
    newMeta->binlogId = Transaction::MAX_VALID_TXNID+1;
    changeReplState(*newMeta, false);

    auto expFlist = [&s]() -> Expected<std::map<std::string, size_t>> {
        rapidjson::Document doc;
        doc.Parse(s.value());
        if (doc.HasParseError()) {
            return {ErrorCodes::ERR_NETWORK,
                        rapidjson::GetParseError_En(doc.GetParseError())};
        }
        if (!doc.IsObject()) {
            return {ErrorCodes::ERR_NOTFOUND, "flist not json obj"};
        }
        std::map<std::string, size_t> result;
        for (auto& o : doc.GetObject()) {
            if (!o.value.IsUint64()) {
                return {ErrorCodes::ERR_NOTFOUND, "json value not uint64"};
            }
            result[o.name.GetString()] = o.value.GetUint64();
        }
        return result;
    }();

    // TODO(deyukong): split the transfering-physical-task into many
    // small schedule-unit, each processes one file, or a fixed-size block.
    if (!expFlist.ok()) {
        return;
    }

    auto backupExists = [store]() -> Expected<bool> {
        std::error_code ec;
        bool exists = filesystem::exists(
                        filesystem::path(store->backupDir()), ec);
        if (ec) {
            return {ErrorCodes::ERR_INTERNAL, ec.message()};
        }
        return exists;
    }();
    if (!backupExists.ok() || backupExists.value()) {
        LOG(FATAL) << "store:" << metaSnapshot.id << " backupDir exists";
    }

    const std::map<std::string, size_t>& flist = expFlist.value();

    std::set<std::string> finishedFiles;
    while (true) {
        if (finishedFiles.size() == flist.size()) {
            break;
        }
        Expected<std::string> s = client->readLine(std::chrono::seconds(1));
        if (!s.ok()) {
            return;
        }
        if (finishedFiles.find(s.value()) != finishedFiles.end()) {
            LOG(FATAL) << "BUG: fullsync " << s.value() << " retransfer";
        }
        if (flist.find(s.value()) == flist.end()) {
            LOG(FATAL) << "BUG: fullsync " << s.value() << " invalid file";
        }
        std::string fullFileName = store->backupDir() + "/" + s.value();
        filesystem::path fileDir =
                filesystem::path(fullFileName).remove_filename();
        if (!filesystem::exists(fileDir)) {
            filesystem::create_directories(fileDir);
        }
        auto myfile = std::fstream(fullFileName,
                    std::ios::out|std::ios::binary);
        if (!myfile.is_open()) {
            LOG(ERROR) << "open file:" << fullFileName << " for write failed";
            return;
        }
        size_t remain = flist.at(s.value());
        while (remain) {
            size_t batchSize = std::min(remain, size_t(20ULL*1024*1024));
            remain -= batchSize;
            Expected<std::string> exptData =
                client->read(batchSize, std::chrono::seconds(1));
            if (!exptData.ok()) {
                LOG(ERROR) << "fullsync read bulk data failed:"
                            << exptData.status().toString();
                return;
            }
            myfile.write(exptData.value().c_str(), exptData.value().size());
            if (myfile.bad()) {
                LOG(ERROR) << "write file:" << fullFileName
                            << " failed:" << strerror(errno);
                return;
            }
        }
        LOG(INFO) << "fullsync file:" << fullFileName << " transfer done";
        finishedFiles.insert(s.value());
    }

    client->writeLine("+OK", std::chrono::seconds(1));

    // 5) restart store, change to stready-syncing mode
    Expected<uint64_t> restartStatus = store->restart(true);
    if (!restartStatus.ok()) {
        LOG(FATAL) << "fullSync restart store:" << metaSnapshot.id
            << " failed:" << restartStatus.status().toString();
    }
    
    newMeta = metaSnapshot.copy();
    newMeta->replState = ReplState::REPL_CONNECTED;
    newMeta->binlogId = restartStatus.value();

    // in ReplManager.startup(), a dummy binlog is written. here we should not
    // get an empty binlog set.
    INVARIANT(newMeta->binlogId != Transaction::TXNID_UNINITED);
    changeReplState(*newMeta, true);

    rollback = false;

    LOG(INFO) << "store:" << metaSnapshot.id
                << " fullsync Done, files:" << finishedFiles.size()
                << ", binlogId:" << newMeta->binlogId;
}

void ReplManager::slaveChkSyncStatus(const StoreMeta& metaSnapshot) {
    bool reconn = [this, &metaSnapshot] {
        std::lock_guard<std::mutex> lk(_mutex);
        auto sessionId = _syncStatus[metaSnapshot.id]->sessionId;
        auto lastSyncTime = _syncStatus[metaSnapshot.id]->lastSyncTime;
        if (sessionId == std::numeric_limits<uint64_t>::max()) {
            return true;
        }
        if (lastSyncTime + std::chrono::seconds(10) <= SCLOCK::now()) {
            return true;
        }
        return false;
    }();

    if (!reconn) {
        return;
    }
    LOG(INFO) << "store:" << metaSnapshot.id
                << " reconn with:" << metaSnapshot.syncFromHost
                << "," << metaSnapshot.syncFromPort
                << "," << metaSnapshot.syncFromId;

    std::shared_ptr<BlockingTcpClient> client =
        std::move(createClient(metaSnapshot));
    if (client == nullptr) {
        LOG(WARNING) << "store:" << metaSnapshot.id << " reconn master failed";
        return;
    }

    std::stringstream ss;
    ss << "INCRSYNC " << metaSnapshot.syncFromId
        << ' ' << metaSnapshot.id
        << ' ' << metaSnapshot.binlogId;
    client->writeLine(ss.str(), std::chrono::seconds(1));
    Expected<std::string> s = client->readLine(std::chrono::seconds(3));
    if (!s.ok()) {
        LOG(WARNING) << "store:" << metaSnapshot.id
                << " psync master failed with error:" << s.status().toString();
        return;
    }
    if (s.value().size() == 0 || s.value()[0] != '+') {
        LOG(WARNING) << "store:" << metaSnapshot.id
                << " incrsync master bad return:" << s.value();
        return;
    }

    Status pongStatus  = client->writeLine("+PONG", std::chrono::seconds(1));
    if (!pongStatus.ok()) {
        LOG(WARNING) << "store:" << metaSnapshot.id
                << " write pong failed:" << pongStatus.toString();
        return;
    }

    NetworkAsio *network = _svr->getNetwork();
    INVARIANT(network != nullptr);

    // why dare we transfer a client to a session ?
    // 1) the logic gets here, so there wont be any
    // async handlers in the event queue.
    // 2) every handler is trigger by calling client's
    // some read/write/connect functions.
    // 3) master side will read +PONG before sending
    // new data, so there wont be any sticky packets.
    Expected<uint64_t> expSessionId =
            network->client2Session(std::move(client));
    if (!expSessionId.ok()) {
        LOG(WARNING) << "client2Session failed:"
                    << expSessionId.status().toString();
        return;
    }
    uint64_t sessionId = expSessionId.value();

    {
        std::lock_guard<std::mutex> lk(_mutex);
        _syncStatus[metaSnapshot.id]->sessionId = sessionId;
        _syncStatus[metaSnapshot.id]->lastSyncTime = SCLOCK::now();
    }
    LOG(INFO) << "store:" << metaSnapshot.id << " psync master succ";
}

Expected<uint64_t> ReplManager::masterSendBinlog(BlockingTcpClient* client,
                uint32_t storeId, uint32_t dstStoreId, uint64_t binlogPos) {
    constexpr uint32_t suggestBatch = 64;
    constexpr size_t suggestBytes = 16*1024*1024;
    PStore store = _svr->getSegmentMgr()->getInstanceById(storeId);
    INVARIANT(store != nullptr);

    auto ptxn = store->createTransaction();
    if (!ptxn.ok()) {
        return ptxn.status();
    }

    std::unique_ptr<Transaction> txn = std::move(ptxn.value());
    std::unique_ptr<BinlogCursor> cursor =
        txn->createBinlogCursor(binlogPos+1);

    std::vector<ReplLog> binlogs;
    uint32_t cnt = 0;
    uint64_t nowId = 0;
    size_t estimateSize = 0;

    // TODO(deyukong): limit by datasize
    while (true) {
        Expected<ReplLog> explog = cursor->next();
        if (explog.ok()) {
            cnt += 1;
            const ReplLogKey& rlk = explog.value().getReplLogKey();
            const ReplLogValue& rlv = explog.value().getReplLogValue();
            estimateSize += rlv.getOpValue().size();
            if (nowId == 0 || nowId != rlk.getTxnId()) {
                nowId = rlk.getTxnId();
                if (cnt >= suggestBatch || estimateSize >= suggestBytes) {
                    break;
                } else {
                    binlogs.emplace_back(std::move(explog.value()));
                }
            } else {
                binlogs.emplace_back(std::move(explog.value()));
            }
        } else if (explog.status().code() == ErrorCodes::ERR_EXHAUST) {
            break;
        } else {
            LOG(ERROR) << "iter binlog failed:"
                        << explog.status().toString();
            return explog.status();
        }
    }

    std::stringstream ss;
    Command::fmtMultiBulkLen(ss, binlogs.size()*2 + 2);
    Command::fmtBulk(ss, "applybinlogs");
    Command::fmtBulk(ss, std::to_string(dstStoreId));
    for (auto& v : binlogs) {
        ReplLog::KV kv = v.encode();
        Command::fmtBulk(ss, kv.first);
        Command::fmtBulk(ss, kv.second);
    }
    std::string stingtoWrite = ss.str();
    uint32_t secs = 1;
    if (stingtoWrite.size() > 1024*1024) {
        secs = 2;
    } else if (stingtoWrite.size() > 1024*1024*10) {
        secs = 4;
    }
    Status s = client->writeData(ss.str(), std::chrono::seconds(secs));
    if (!s.ok()) {
        return s;
    }
    if (binlogs.size() == 0) {
        return binlogPos;
    } else {
        return binlogs[binlogs.size()-1].getReplLogKey().getTxnId();
    }
}

void ReplManager::masterPushRoutine(uint32_t storeId, uint64_t clientId) {
    SCLOCK::time_point nextSched = SCLOCK::now();
    auto guard = MakeGuard([this, &nextSched, storeId, clientId] {
        std::lock_guard<std::mutex> lk(_mutex);
        auto& mpov = _pushStatus[storeId];
        if (mpov.find(clientId) == mpov.end()) {
            return;
        }
        INVARIANT(mpov[clientId]->isRunning);
        mpov[clientId]->isRunning = false;
        mpov[clientId]->nextSchedTime = nextSched;
        // currently nothing waits for master's push process
        // _cv.notify_all();
    });

    uint64_t binlogPos = 0;
    BlockingTcpClient *client = nullptr;
    uint32_t dstStoreId = 0;
    {
        std::lock_guard<std::mutex> lk(_mutex);
        if (_pushStatus[storeId].find(clientId) == _pushStatus[storeId].end()) {
            nextSched = nextSched + std::chrono::seconds(1);
            return;
        }
        binlogPos = _pushStatus[storeId][clientId]->binlogPos;
        client = _pushStatus[storeId][clientId]->client.get();
        dstStoreId = _pushStatus[storeId][clientId]->dstStoreId;
    }

    Expected<uint64_t> newPos = masterSendBinlog(client, storeId, dstStoreId, binlogPos);
    if (!newPos.ok()) {
        LOG(WARNING) << "masterSendBinlog to client:"
                << client->getRemoteRepr() << " failed:"
                << newPos.status().toString();
        std::lock_guard<std::mutex> lk(_mutex);
        // it is safe to remove a non-exist key
        _pushStatus[storeId].erase(clientId);
        return;
    } else {
        std::lock_guard<std::mutex> lk(_mutex);
        _pushStatus[storeId][clientId]->binlogPos = newPos.value();
        if (newPos.value() > binlogPos) {
            nextSched = SCLOCK::now();
        } else {
            nextSched = SCLOCK::now() + std::chrono::seconds(1);
        }
    }
}

void ReplManager::slaveSyncRoutine(uint32_t storeId) {
    SCLOCK::time_point nextSched = SCLOCK::now();
    auto guard = MakeGuard([this, &nextSched, storeId] {
        std::lock_guard<std::mutex> lk(_mutex);
        INVARIANT(_syncStatus[storeId]->isRunning);
        _syncStatus[storeId]->isRunning = false;
        _syncStatus[storeId]->nextSchedTime = nextSched;
        _cv.notify_all();
    });

    std::unique_ptr<StoreMeta> metaSnapshot = [this, storeId]() {
        std::lock_guard<std::mutex> lk(_mutex);
        return std::move(_syncMeta[storeId]->copy());
    }();

    if (metaSnapshot->syncFromHost == "") {
        // if master is nil, try sched after 1 second
        nextSched = nextSched + std::chrono::seconds(1);
        return;
    }

    INVARIANT(metaSnapshot->replState == ReplState::REPL_CONNECT ||
        metaSnapshot->replState == ReplState::REPL_CONNECTED);

    if (metaSnapshot->replState == ReplState::REPL_CONNECT) {
        slaveStartFullsync(*metaSnapshot);
        nextSched = nextSched + std::chrono::seconds(3);
        return;
    } else if (metaSnapshot->replState == ReplState::REPL_CONNECTED) {
        slaveChkSyncStatus(*metaSnapshot);
        nextSched = nextSched + std::chrono::seconds(10);
        return;
    } else {
        INVARIANT(false);
    }
}

void ReplManager::controlRoutine() {
    using namespace std::chrono_literals;  // (NOLINT)
    auto schedSlaveInLock = [this](const SCLOCK::time_point& now) {
        // slave's POV
        bool doSth = false;
        for (size_t i = 0; i < _syncStatus.size(); i++) {
            if (_syncStatus[i]->isRunning
                    || now < _syncStatus[i]->nextSchedTime) {
                continue;
            }
            doSth = true;
            // NOTE(deyukong): we dispatch fullsync/incrsync jobs into
            // different pools.
            if (_syncMeta[i]->replState == ReplState::REPL_CONNECT) {
                _syncStatus[i]->isRunning = true;
                _fullReceiver->schedule([this, i]() {
                    slaveSyncRoutine(i);
                });
            } else if (_syncMeta[i]->replState == ReplState::REPL_CONNECTED) {
                _syncStatus[i]->isRunning = true;
                _incrChecker->schedule([this, i]() {
                    slaveSyncRoutine(i);
                });
            } else if (_syncMeta[i]->replState == ReplState::REPL_TRANSFER) {
                LOG(FATAL) << "sync store:" << i
                    << " REPL_TRANSFER should not be visitable";
            } else {  // REPL_NONE
                // nothing to do with REPL_NONE
            }
        }
        return doSth;
    };
    auto schedMasterInLock = [this](const SCLOCK::time_point& now) {
        // master's POV
        bool doSth = false;
        for (size_t i = 0; i < _pushStatus.size(); i++) {
            for (auto& mpov : _pushStatus[i]) {
                if (mpov.second->isRunning
                        || now < mpov.second->nextSchedTime) {
                    continue;
                }

                doSth = true;
                mpov.second->isRunning = true;
                uint64_t clientId = mpov.first;
                _incrPusher->schedule([this, i, clientId]() {
                    masterPushRoutine(i, clientId);
                });
            }
        }
        return doSth;
    };
    while (_isRunning.load(std::memory_order_relaxed)) {
        bool doSth = false;
        {
            std::lock_guard<std::mutex> lk(_mutex);
            auto now = SCLOCK::now();
            doSth = schedSlaveInLock(now);
            doSth = schedMasterInLock(now) || doSth;
        }
        if (doSth) {
            // schedyield
            std::this_thread::yield();
        } else {
            std::this_thread::sleep_for(10ms);
        }
    }
    LOG(INFO) << "repl controller exits";
}

bool ReplManager::isFullSupplierFull() const {
    return _fullPusher->isFull();
}

void ReplManager::supplyFullSync(asio::ip::tcp::socket sock,
                        const std::string& storeIdArg) {
    std::shared_ptr<BlockingTcpClient> client =
        std::move(_svr->getNetwork()->createBlockingClient(
            std::move(sock), 64*1024*1024));

    // NOTE(deyukong): this judge is not precise
    // even it's not full at this time, it can be full during schedule.
    if (isFullSupplierFull()) {
        client->writeLine("-ERR workerpool full", std::chrono::seconds(1));
        return;
    }

    Expected<int64_t> expStoreId = stoul(storeIdArg);
    if (!expStoreId.ok() || expStoreId.value() < 0) {
        client->writeLine("-ERR invalid storeId", std::chrono::seconds(1));
        return;
    }
    uint32_t storeId = static_cast<uint32_t>(expStoreId.value());
    _fullPusher->schedule([this, storeId, client(std::move(client))]() mutable {
        supplyFullSyncRoutine(std::move(client), storeId);
    });
}

//  1) s->m INCRSYNC (m side: session2Client)
//  2) m->s +OK
//  3) s->m +PONG (s side: client2Session)
//  4) m->s periodly send binlogs
//  the 3) step is necessary, if ignored, the +OK in step 2) and binlogs
//  in step 4) may sticky together. and redis-resp protocal is not fixed-size
//  That makes client2Session complicated.

// NOTE(deyukong): we define binlogPos the greatest id that has been applied.
// "NOT" the smallest id that has not been applied. keep the same with BackupInfo's
// setCommitId
void ReplManager::registerIncrSync(asio::ip::tcp::socket sock,
            const std::string& storeIdArg,
            const std::string& dstStoreIdArg,
            const std::string& binlogPosArg) {
    std::shared_ptr<BlockingTcpClient> client =
        std::move(_svr->getNetwork()->createBlockingClient(
            std::move(sock), 64*1024*1024));

    uint64_t storeId;
    uint64_t  dstStoreId;
    uint64_t binlogPos;
    try {
        storeId = stoul(storeIdArg);
        dstStoreId = stoul(dstStoreIdArg);
        binlogPos = stoul(binlogPosArg);
    } catch (const std::exception& ex) {
        std::stringstream ss;
        ss << "-ERR parse opts failed:" << ex.what();
        client->writeLine(ss.str(), std::chrono::seconds(1));
        return;
    }

    if (storeId >= KVStore::INSTANCE_NUM ||
            dstStoreId >= KVStore::INSTANCE_NUM) {
        client->writeLine("-ERR invalid storeId", std::chrono::seconds(1));
        return;
    }

    uint64_t firstPos = [this, storeId]() {
        std::lock_guard<std::mutex> lk(_mutex);
        return _firstBinlogId[storeId];
    }();

    // NOTE(deyukong): this check is not precise
    // (not in the same critical area with the modification to _pushStatus),
    // but it does not harm correctness.
    // A strict check may be too complicated to read.
    if (firstPos > binlogPos) {
        client->writeLine("-ERR invalid binlogPos", std::chrono::seconds(1));
        return;
    }
    client->writeLine("+OK", std::chrono::seconds(1));
    Expected<std::string> exptPong = client->readLine(std::chrono::seconds(1));
    if (!exptPong.ok()) {
        LOG(WARNING) << "slave incrsync handshake failed:"
                << exptPong.status().toString();
        return;
    } else if (exptPong.value() != "+PONG") {
        LOG(WARNING) << "slave incrsync handshake not +PONG:"
                << exptPong.value();
        return;
    }

    std::string remoteHost = client->getRemoteRepr();
    bool registPosOk =
            [this,
             storeId,
             dstStoreId,
             binlogPos,
             client = std::move(client)]() mutable {
        std::lock_guard<std::mutex> lk(_mutex);
        if (_firstBinlogId[storeId] > binlogPos) {
            return false;
        }
        uint64_t clientId = _clientIdGen.fetch_add(1);
        _pushStatus[storeId][clientId] =
            std::move(std::unique_ptr<MPovStatus>(
                new MPovStatus {
                    isRunning: false,
                    dstStoreId: static_cast<uint32_t>(dstStoreId),
                    binlogPos: binlogPos,
                    nextSchedTime: SCLOCK::now(),
                    client: std::move(client),
                    clientId: clientId}));
        return true;
    }();
    LOG(INFO) << "slave:" << remoteHost
            << " registerIncrSync " << (registPosOk ? "ok" : "failed");
}

Status ReplManager::applySingleTxn(uint32_t storeId, uint64_t txnId,
                                   const std::list<ReplLog>& ops) {
    PStore store = _svr->getSegmentMgr()->getInstanceById(storeId);
    INVARIANT(store != nullptr); 
    auto ptxn = store->createTransaction();
    if (!ptxn.ok()) {
        return ptxn.status();
    }

    std::unique_ptr<Transaction> txn = std::move(ptxn.value());
    for (const auto& log : ops) {
        const ReplLogValue& logVal = log.getReplLogValue();

        Expected<RecordKey> expRk = RecordKey::decode(logVal.getOpKey());
        if (!expRk.ok()) {
            return expRk.status();
        }

        auto strPair = log.encode();
        // write binlog
        auto s = store->setKV(strPair.first, strPair.second,
                              txn.get(), false /*withlog*/);
        if (!s.ok()) {
            return s;
        }
        switch (logVal.getOp()) {
            case (ReplOp::REPL_OP_SET): {
                Expected<RecordValue> expRv =
                    RecordValue::decode(logVal.getOpValue());
                if (!expRv.ok()) {
                    return expRv.status();
                }
                s = store->setKV(expRk.value(), expRv.value(),
                                      txn.get(), false /*withlog*/);
                if (!s.ok()) {
                    return s;
                } else {
                    break;
                }
            }
            case (ReplOp::REPL_OP_DEL): {
                s = store->delKV(expRk.value(), txn.get(), false /*withlog*/);
                if (!s.ok()) {
                    return s;
                } else {
                    break;
                }
            }
            default: {
                LOG(FATAL) << "invalid binlogOp:"
                            << static_cast<uint8_t>(logVal.getOp());
            }
        }
    }
    Expected<uint64_t> expCmit = txn->commit();
    if (!expCmit.ok()) {
        return expCmit.status();
    }
    return {ErrorCodes::ERR_OK, ""};
}

Status ReplManager::applyBinlogs(uint32_t storeId, uint64_t sessionId,
            const std::map<uint64_t, std::list<ReplLog>>& binlogs) {
    [this, storeId]() {
        std::unique_lock<std::mutex> lk(_mutex);
        _cv.wait(lk,
                [this, storeId]
                { return !_syncStatus[storeId]->isRunning;});
        _syncStatus[storeId]->isRunning = true;
    }();

    auto guard = MakeGuard([this, storeId] {
        std::unique_lock<std::mutex> lk(_mutex);
        INVARIANT(_syncStatus[storeId]->isRunning);
        _syncStatus[storeId]->isRunning = false;
    });

    bool idMatch = [this, storeId, sessionId]() {
        std::unique_lock<std::mutex> lk(_mutex);
        return (sessionId == _syncStatus[storeId]->sessionId);
    }();
    if (!idMatch) {
        return {ErrorCodes::ERR_NOTFOUND, "sessionId not match"};
    }

    for (const auto& logList : binlogs) {
        Status s = applySingleTxn(storeId, logList.first, logList.second);
        if (!s.ok()) {
            return s;
        }
    }

    // TODO(deyukong): perf and maybe periodly save binlogpos
    if (binlogs.size() > 0) {
        std::lock_guard<std::mutex> lk(_mutex);
        auto newMeta = _syncMeta[storeId]->copy();
        newMeta->binlogId = binlogs.rbegin()->first;
        INVARIANT(newMeta->replState == ReplState::REPL_CONNECTED);
        changeReplStateInLock(*newMeta, true);
    }
    return {ErrorCodes::ERR_OK, ""};
}

void ReplManager::supplyFullSyncRoutine(
            std::shared_ptr<BlockingTcpClient> client, uint32_t storeId) {
    PStore store = _svr->getSegmentMgr()->getInstanceById(storeId);
    INVARIANT(store != nullptr);
    if (!store->isRunning()) {
        client->writeLine("-ERR store is not running", std::chrono::seconds(1));
        return;
    }

    Expected<BackupInfo> bkInfo = store->backup();
    if (!bkInfo.ok()) {
        std::stringstream ss;
        ss << "-ERR backup failed:" << bkInfo.status().toString();
        client->writeLine(ss.str(), std::chrono::seconds(1));
        return;
    }

    auto guard = MakeGuard([this, store, storeId]() {
        Status s = store->releaseBackup();
        if (!s.ok()) {
            LOG(ERROR) << "supplyFullSync end clean store:"
                    << storeId << " error:" << s.toString();
        }
    });

    rapidjson::StringBuffer sb;
    rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
    writer.StartObject();
    for (const auto& kv : bkInfo.value().getFileList()) {
        writer.Key(kv.first.c_str());
        writer.Uint64(kv.second);
    }
    writer.EndObject();
    Status s = client->writeLine(sb.GetString(), std::chrono::seconds(1));
    if (!s.ok()) {
        LOG(ERROR) << "store:" << storeId << " writeLine failed"
                    << s.toString();
        return;
    }

    std::string readBuf;
    readBuf.reserve(size_t(20ULL*1024*1024));  // 20MB
    for (auto& fileInfo : bkInfo.value().getFileList()) {
        s = client->writeLine(fileInfo.first, std::chrono::seconds(1));
        if (!s.ok()) {
            LOG(ERROR) << "write fname:" << fileInfo.first
                        << " to client failed:" << s.toString();
            return;
        }
        std::string fname = store->backupDir() + "/" + fileInfo.first;
        auto myfile = std::ifstream(fname, std::ios::binary);
        if (!myfile.is_open()) {
            LOG(ERROR) << "open file:" << fname << " for read failed";
            return;
        }
        size_t remain = fileInfo.second;
        while (remain) {
            size_t batchSize = std::min(remain, readBuf.capacity());
            readBuf.resize(batchSize);
            remain -= batchSize;
            myfile.read(&readBuf[0], batchSize);
            if (!myfile) {
                LOG(ERROR) << "read file:" << fname
                            << " failed with err:" << strerror(errno);
                return;
            }
            s = client->writeData(readBuf, std::chrono::seconds(1));
            if (!s.ok()) {
                LOG(ERROR) << "write bulk to client failed:" << s.toString();
                return;
            }
        }
    }
    Expected<std::string> reply = client->readLine(std::chrono::seconds(1));
    if (!reply.ok()) {
        LOG(ERROR) << "fullsync done read "
                   << client->getRemoteRepr() << " reply failed:"
                   << reply.status().toString();
    } else {
        LOG(INFO) << "fullsync done read "
                  << client->getRemoteRepr() << " reply:" << reply.value();
    }
}

Status ReplManager::changeReplSource(uint32_t storeId, std::string ip,
            uint32_t port, uint32_t sourceStoreId) {
    std::unique_lock<std::mutex> lk(_mutex);
    LOG(INFO) << "wait for store:" << storeId << " to yield work";
    // NOTE(deyukong): we must wait for the target to stop before change meta,
    // or the meta may be rewrited
    if (!_cv.wait_for(lk, std::chrono::seconds(1),
            [this, storeId] { return !_syncStatus[storeId]->isRunning; })) {
        return {ErrorCodes::ERR_TIMEOUT, "wait for yeild failed"};
    }
    LOG(INFO) << "wait for store:" << storeId << " to yield work succ";
    INVARIANT(!_syncStatus[storeId]->isRunning);

    if (storeId >= _syncMeta.size()) {
        return {ErrorCodes::ERR_INTERNAL, "invalid storeId"};
    }

    auto newMeta = _syncMeta[storeId]->copy();
    if (ip != "") {
        if (_syncMeta[storeId]->syncFromHost != "") {
            return {ErrorCodes::ERR_BUSY,
                    "explicit set sync source empty before change it"};
        }

        newMeta->syncFromHost = ip;
        newMeta->syncFromPort = port;
        newMeta->syncFromId = sourceStoreId;
        newMeta->replState = ReplState::REPL_CONNECT;
        newMeta->binlogId = Transaction::MAX_VALID_TXNID+1;
        LOG(INFO) << "change store:" << storeId
                    << " syncSrc from no one to " << newMeta->syncFromHost
                    << ":" << newMeta->syncFromPort
                    << ":" << newMeta->syncFromId;
        changeReplStateInLock(*newMeta, true);
        return {ErrorCodes::ERR_OK, ""};
    } else {  // ip == ""
        if (newMeta->syncFromHost == "") {
            return {ErrorCodes::ERR_OK, ""};
        }
        LOG(INFO) << "change store:" << storeId
                    << " syncSrc:" << newMeta->syncFromHost
                    << " to no one";
        Status closeStatus =
            _svr->cancelSession(_syncStatus[storeId]->sessionId);
        if (!closeStatus.ok()) {
            // this error does not affect much, just log and continue
            LOG(WARNING) << "cancel store:" << storeId << " session failed:"
                        << closeStatus.toString();
        }

        newMeta->syncFromHost = ip;
        INVARIANT(port == 0 && sourceStoreId == 0);
        newMeta->syncFromPort = port;
        newMeta->syncFromId = sourceStoreId;
        newMeta->replState = ReplState::REPL_NONE;
        newMeta->binlogId = Transaction::MAX_VALID_TXNID+1;
        changeReplStateInLock(*newMeta, true);
        return {ErrorCodes::ERR_OK, ""};
    }
}

void ReplManager::appendJSONStat(rapidjson::Writer<rapidjson::StringBuffer>& w) const {
    w.Key("slaves");
    {
        w.StartObject();
        std::lock_guard<std::mutex> lk(_mutex);
        for (size_t i = 0; i < _pushStatus.size(); i++) {
            for (auto& mpov : _pushStatus[i]) {
                std::stringstream ss;
                ss << "client_" << mpov.second->clientId;
                w.Key(ss.str().c_str());
                w.StartObject();
                w.Key("isRunning");
                w.Uint64(mpov.second->isRunning);
                w.Key("dstStoreId");
                w.Uint64(mpov.second->dstStoreId);
                w.Key("binlogPos");
                w.Uint64(mpov.second->binlogPos);
                w.Key("remoteHost");
                if (mpov.second->client != nullptr) {
                    w.String(mpov.second->client->getRemoteRepr());
                } else {
                    w.String("???");
                }
                w.EndObject();
            }
        }
        w.EndObject();
    }
}

void ReplManager::stop() {
    LOG(WARNING) << "repl manager begins stops...";
    _isRunning.store(false, std::memory_order_relaxed);
    _controller.join();
    _fullPusher->stop();
    _incrPusher->stop();
    _fullReceiver->stop();
    _incrChecker->stop();
    LOG(WARNING) << "repl manager stops succ";
}

}  // namespace tendisplus
