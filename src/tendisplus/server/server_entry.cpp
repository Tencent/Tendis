#include <utility>
#include <memory>
#include <algorithm>
#include <chrono>
#include <string>
#include "glog/logging.h"
#include "tendisplus/server/server_entry.h"
#include "tendisplus/server/server_params.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/storage/rocks/rocks_kvstore.h"

namespace tendisplus {

ServerEntry::ServerEntry()
        :_isRunning(false),
         _isStopped(true),
         _network(nullptr),
         _executor(nullptr),
         _segmentMgr(nullptr),
         _replMgr(nullptr),
         _catalog(nullptr),
         _netMatrix(std::make_shared<NetworkMatrix>()),
         _poolMatrix(std::make_shared<PoolMatrix>()),
         _ftmcThd(nullptr),
         _requirepass(nullptr),
         _masterauth(nullptr) {
}

void ServerEntry::installStoresInLock(const std::vector<PStore>& o) {
    // TODO(deyukong): assert mutex held
    _kvstores = o;
}

void ServerEntry::installSegMgrInLock(std::unique_ptr<SegmentMgr> o) {
    // TODO(deyukong): assert mutex held
    _segmentMgr = std::move(o);
}

void ServerEntry::installCatalog(std::unique_ptr<Catalog> o) {
    _catalog = std::move(o);
}

Catalog* ServerEntry::getCatalog() {
    return _catalog.get();
}

Status ServerEntry::startup(const std::shared_ptr<ServerParams>& cfg) {
    std::lock_guard<std::mutex> lk(_mutex);

    _requirepass = std::make_shared<std::string>(cfg->requirepass);
    _masterauth = std::make_shared<std::string>(cfg->masterauth);

    // catalog init
    auto catalog = std::make_unique<Catalog>(
        std::move(std::unique_ptr<KVStore>(
            new RocksKVStore("catalog", cfg, nullptr))));
    installCatalog(std::move(catalog));

    // kvstore init
    auto blockCache =
        rocksdb::NewLRUCache(cfg->rocksBlockcacheMB * 1024 * 1024LL, 6);
    std::vector<PStore> tmpStores;
    for (size_t i = 0; i < KVStore::INSTANCE_NUM; ++i) {
        std::stringstream ss;
        ss << i;
        std::string dbId = ss.str();
        tmpStores.emplace_back(std::unique_ptr<KVStore>(
            new RocksKVStore(dbId, cfg, blockCache)));
    }
    installStoresInLock(tmpStores);

    // segment mgr
    auto tmpSegMgr = std::unique_ptr<SegmentMgr>(
        new SegmentMgrFnvHash64(_kvstores));
    installSegMgrInLock(std::move(tmpSegMgr));

    // network listener
    _network = std::make_unique<NetworkAsio>(shared_from_this(), _netMatrix);
    Status s = _network->prepare(cfg->bindIp, cfg->port);
    if (!s.ok()) {
        return s;
    }
    s = _network->run();
    if (!s.ok()) {
        return s;
    }

    // network executePool
    _executor = std::make_unique<WorkerPool>(_poolMatrix);
    size_t cpuNum = std::thread::hardware_concurrency();
    if (cpuNum == 0) {
        return {ErrorCodes::ERR_INTERNAL, "cpu num cannot be detected"};
    }
    s = _executor->startup(std::max(size_t(4), cpuNum/2));
    if (!s.ok()) {
        return s;
    }

    // replication
    _replMgr = std::make_unique<ReplManager>(shared_from_this());
    s = _replMgr->startup();
    if (!s.ok()) {
        return s;
    }

    _isRunning.store(true, std::memory_order_relaxed);
    _isStopped.store(false, std::memory_order_relaxed);

    // server stats monitor
    _ftmcThd = std::make_unique<std::thread>([this] {
        ftmc();
    });
    return {ErrorCodes::ERR_OK, ""};
}

NetworkAsio* ServerEntry::getNetwork() {
    return _network.get();
}

ReplManager* ServerEntry::getReplManager() {
    return _replMgr.get();
}

const SegmentMgr* ServerEntry::getSegmentMgr() const {
    return _segmentMgr.get();
}

const std::shared_ptr<std::string> ServerEntry::requirepass() const {
    std::lock_guard<std::mutex> lk(_mutex);
    return _requirepass;
}

const std::shared_ptr<std::string> ServerEntry::masterauth() const {
    std::lock_guard<std::mutex> lk(_mutex);
    return _masterauth;
}

void ServerEntry::addSession(std::unique_ptr<NetSession> sess) {
    std::lock_guard<std::mutex> lk(_mutex);
    if (!_isRunning.load(std::memory_order_relaxed)) {
        LOG(WARNING) << "session:" << sess->getRemoteRepr()
            << "comes when stopping, ignore it";
        return;
    }
    // TODO(deyukong): max conns


    // NOTE(deyukong): first driving force
    sess->start();
    uint64_t id = sess->getConnId();
    if (_sessions.find(id) != _sessions.end()) {
        LOG(FATAL) << "add conn:" << id << ",id already exists";
    }
    _sessions[id] = std::move(sess);
}

Status ServerEntry::cancelSession(uint64_t connId) {
    std::lock_guard<std::mutex> lk(_mutex);
    if (!_isRunning.load(std::memory_order_relaxed)) {
        return {ErrorCodes::ERR_BUSY, "server is shutting down"};
    }
    auto it = _sessions.find(connId);
    if (it == _sessions.end()) {
        return {ErrorCodes::ERR_NOTFOUND, "session not found"};
    }
    return it->second->cancel();
}

void ServerEntry::endSession(uint64_t connId) {
    std::lock_guard<std::mutex> lk(_mutex);
    if (!_isRunning.load(std::memory_order_relaxed)) {
        return;
    }
    auto it = _sessions.find(connId);
    if (it == _sessions.end()) {
        LOG(FATAL) << "destroy conn:" << connId << ",not exists";
    }
    _sessions.erase(it);
}

bool ServerEntry::processRequest(uint64_t connId) {
    NetSession *sess = nullptr;
    {
        std::lock_guard<std::mutex> lk(_mutex);
        if (!_isRunning.load(std::memory_order_relaxed)) {
            return false;
        }
        auto it = _sessions.find(connId);
        if (it == _sessions.end()) {
            LOG(FATAL) << "conn:" << connId << ",invalid state";
        }
        sess = it->second.get();
        if (sess == nullptr) {
            LOG(FATAL) << "conn:" << connId << ",null in servermap";
        }
    }
    auto expCmdName = Command::precheck(sess);
    if (!expCmdName.ok()) {
        sess->setResponse(redis_port::errorReply(expCmdName.status().toString()));
        return true;
    }
    if (expCmdName.value() == "fullsync") {
        LOG(WARNING) << "connId:" << connId << " socket borrowed";
        std::vector<std::string> args = sess->getArgs();
        // we have called precheck, it should have 2 args
        INVARIANT(args.size() == 2);
        _replMgr->supplyFullSync(sess->borrowConn(), args[1]);
        return false;
    } else if (expCmdName.value() == "incrsync") {
        LOG(WARNING) << "connId:" << connId << " socket borrowed";
        std::vector<std::string> args = sess->getArgs();
        // we have called precheck, it should have 2 args
        INVARIANT(args.size() == 4);
        _replMgr->registerIncrSync(sess->borrowConn(), args[1], args[2], args[3]);
        return false;
    }

    auto expect = Command::runSessionCmd(sess);
    if (!expect.ok()) {
        sess->setResponse(Command::fmtErr(expect.status().toString()));
        return true;
    }
    sess->setResponse(expect.value());
    return true;
}

// full-time matrix collect
void ServerEntry::ftmc() {
    using namespace std::chrono_literals;  // NOLINT(build/namespaces)
    LOG(INFO) << "server ftmc thread starts";
    auto oldNetMatrix = *_netMatrix;
    auto oldPoolMatrix = *_poolMatrix;
    while (_isRunning.load(std::memory_order_relaxed)) {
        auto tmpNetMatrix = *_netMatrix - oldNetMatrix;
        auto tmpPoolMatrix = *_poolMatrix - oldPoolMatrix;
        oldNetMatrix = *_netMatrix;
        oldPoolMatrix = *_poolMatrix;
        LOG(INFO) << "network matrix status:\n" << tmpNetMatrix.toString();
        LOG(INFO) << "pool matrix status:\n" << tmpPoolMatrix.toString();

        std::unique_lock<std::mutex> lk(_mutex);
        bool ok = _eventCV.wait_for(lk, 1000ms, [this] {
            return _isRunning.load(std::memory_order_relaxed) == false;
        });
        if (ok) {
            LOG(INFO) << "server ftmc thread exits";
            return;
        }
    }
}

void ServerEntry::waitStopComplete() {
    std::unique_lock<std::mutex> lk(_mutex);
    _eventCV.wait(lk, [this] {
        return _isRunning.load(std::memory_order_relaxed) == false
            && _isStopped.load(std::memory_order_relaxed) == true;
    });
}

void ServerEntry::stop() {
    if (_isRunning.load(std::memory_order_relaxed) == false) {
        LOG(INFO) << "server is stopping, plz donot kill again";
        return;
    }
    LOG(INFO) << "server begins to stop...";
    _isRunning.store(false, std::memory_order_relaxed);
    _eventCV.notify_all();
    _network->stop();
    _executor->stop();
    _replMgr->stop();
    _sessions.clear();
    _ftmcThd->join();
    LOG(INFO) << "server stops complete...";
    _isStopped.store(true, std::memory_order_relaxed);
    _eventCV.notify_all();
}

}  // namespace tendisplus
