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
#include "tendisplus/lock/lock.h"

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
                    Transaction::TXNID_UNINITED, ReplState::REPL_NONE));
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

    _incrPusher = std::make_unique<WorkerPool>(
            "repl-minc", _incrPushMatrix);
    Status s = _incrPusher->startup(INCR_POOL_SIZE);
    if (!s.ok()) {
        return s;
    }

    _fullPusher = std::make_unique<WorkerPool>(
            "repl-mfull", _fullPushMatrix);
    s = _fullPusher->startup(MAX_FULL_PARAL);
    if (!s.ok()) {
        return s;
    }

    _fullReceiver = std::make_unique<WorkerPool>(
            "repl-sfull", _fullReceiveMatrix);
    s = _fullReceiver->startup(MAX_FULL_PARAL);
    if (!s.ok()) {
        return s;
    }

    _incrChecker = std::make_unique<WorkerPool>(
            "repl-scheck", _incrCheckMatrix);
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
    // in this way, the full-backup will always have a committed txnId.
    for (uint32_t i = 0; i < KVStore::INSTANCE_NUM; i++) {
        // here we are starting up, dont acquire a storelock.
        PStore store = _svr->getSegmentMgr()->getInstanceById(i);
        INVARIANT(store != nullptr);

        auto ptxn = store->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }

        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        RecordKey rk(0, RecordType::RT_META, "NOOP", "");
        RecordValue rv("NOOP");
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

void ReplManager::controlRoutine() {
    using namespace std::chrono_literals;  // (NOLINT)
    auto schedSlaveInLock = [this](const SCLOCK::time_point& now) {
        // slave's POV
        bool doSth = false;
        for (size_t i = 0; i < _syncStatus.size(); i++) {
            if (_syncStatus[i]->isRunning
                    || now < _syncStatus[i]->nextSchedTime
                    || _syncMeta[i]->replState == ReplState::REPL_NONE) {
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
            std::this_thread::yield();
        } else {
            std::this_thread::sleep_for(10ms);
        }
    }
    LOG(INFO) << "repl controller exits";
}

Status ReplManager::changeReplSource(uint32_t storeId, std::string ip,
            uint32_t port, uint32_t sourceStoreId) {
    LOG(INFO) << "wait for store:" << storeId << " to yield work";
    std::unique_lock<std::mutex> lk(_mutex);
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
        newMeta->binlogId = Transaction::TXNID_UNINITED;
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
        _syncStatus[storeId]->sessionId = std::numeric_limits<uint64_t>::max();

        newMeta->syncFromHost = ip;
        INVARIANT(port == 0 && sourceStoreId == 0);
        newMeta->syncFromPort = port;
        newMeta->syncFromId = sourceStoreId;
        newMeta->replState = ReplState::REPL_NONE;
        newMeta->binlogId = Transaction::TXNID_UNINITED;
        changeReplStateInLock(*newMeta, true);
        return {ErrorCodes::ERR_OK, ""};
    }
}

void ReplManager::appendJSONStat(
        rapidjson::Writer<rapidjson::StringBuffer>& w) const {
    std::lock_guard<std::mutex> lk(_mutex);
    INVARIANT(_pushStatus.size() == KVStore::INSTANCE_NUM);
    INVARIANT(_syncStatus.size() == KVStore::INSTANCE_NUM);
    for (size_t i = 0; i < KVStore::INSTANCE_NUM; ++i) {
        std::stringstream ss;
        ss << "store_" << i;
        w.Key(ss.str().c_str());
        w.StartObject();

        w.Key("sync_dest");
        w.StartObject();
        // sync to
        for (auto& mpov : _pushStatus[i]) {
            std::stringstream ss;
            ss << "client_" << mpov.second->clientId;
            w.Key(ss.str().c_str());
            w.StartObject();
            w.Key("is_running");
            w.Uint64(mpov.second->isRunning);
            w.Key("dest_store_id");
            w.Uint64(mpov.second->dstStoreId);
            w.Key("binlog_pos");
            w.Uint64(mpov.second->binlogPos);
            w.Key("remote_host");
            if (mpov.second->client != nullptr) {
                w.String(mpov.second->client->getRemoteRepr());
            } else {
                w.String("???");
            }
            w.EndObject();
        }
        w.EndObject();

        // sync from
        w.Key("sync_source");
        ss.str("");
        ss << _syncMeta[i]->syncFromHost << ":"
           << _syncMeta[i]->syncFromPort << ":"
           << _syncMeta[i]->syncFromId;
        w.String(ss.str().c_str());
        w.Key("binlog_id");
        w.Uint64(_syncMeta[i]->binlogId);
        w.Key("repl_state");
        w.Uint64(static_cast<uint64_t>(_syncMeta[i]->replState));
        w.Key("last_sync_time");
        w.String(timePointRepr(_syncStatus[i]->lastSyncTime));

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
