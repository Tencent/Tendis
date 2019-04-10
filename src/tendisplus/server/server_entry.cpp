#include <utility>
#include <memory>
#include <algorithm>
#include <chrono>
#include <string>
#include <list>
#include "glog/logging.h"
#include "tendisplus/server/server_entry.h"
#include "tendisplus/server/server_params.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/time.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/storage/rocks/rocks_kvstore.h"

namespace tendisplus {

ServerEntry::ServerEntry()
        :_ftmcEnabled(false),
         _isRunning(false),
         _isStopped(true),
         _startupTime(nsSinceEpoch()),
         _network(nullptr),
         _executor(nullptr),
         _segmentMgr(nullptr),
         _replMgr(nullptr),
         _indexMgr(nullptr),
         _pessimisticMgr(nullptr),
         _catalog(nullptr),
         _netMatrix(std::make_shared<NetworkMatrix>()),
         _poolMatrix(std::make_shared<PoolMatrix>()),
         _reqMatrix(std::make_shared<RequestMatrix>()),
         _ftmcThd(nullptr),
         _requirepass(nullptr),
         _masterauth(nullptr),
         _versionIncrease(true) {
}

void ServerEntry::installPessimisticMgrInLock(
        std::unique_ptr<PessimisticMgr> o) {
    _pessimisticMgr = std::move(o);
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

void ServerEntry::logGeneral(Session *sess) {
    if (!_generalLog) {
        return;
    }
    const std::vector<std::string>& args = sess->getArgs();

    std::stringstream ss;
    ss << "Command: ";
    for (auto arg : args) {
        ss << arg << " ";
    }

    LOG(INFO) << ss.str();
}

Status ServerEntry::startup(const std::shared_ptr<ServerParams>& cfg) {
    std::lock_guard<std::mutex> lk(_mutex);

    _requirepass = std::make_shared<std::string>(cfg->requirepass);
    _masterauth = std::make_shared<std::string>(cfg->masterauth);
    _versionIncrease = cfg->versionIncrease;
    _generalLog = cfg->generalLog;

    // catalog init
    auto catalog = std::make_unique<Catalog>(
        std::move(std::unique_ptr<KVStore>(
            new RocksKVStore(CATALOG_NAME, cfg, nullptr))));
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

    // pessimisticMgr
    auto tmpPessimisticMgr = std::make_unique<PessimisticMgr>(
        KVStore::INSTANCE_NUM);
    installPessimisticMgrInLock(std::move(tmpPessimisticMgr));

    // request executePool
    _executor = std::make_unique<WorkerPool>("req-exec", _poolMatrix);
    size_t cpuNum = std::thread::hardware_concurrency();
    if (cpuNum == 0) {
        return {ErrorCodes::ERR_INTERNAL, "cpu num cannot be detected"};
    }
    Status s = _executor->startup(std::max(size_t(4), cpuNum/2));
    if (!s.ok()) {
        return s;
    }

    // network
    _network = std::make_unique<NetworkAsio>(shared_from_this(),
                                             _netMatrix,
                                             _reqMatrix);
    s = _network->prepare(cfg->bindIp, cfg->port);
    if (!s.ok()) {
        return s;
    }

    // replication
    // replication relys on blocking-client
    // must startup after network prepares ok
    _replMgr = std::make_unique<ReplManager>(shared_from_this(), cfg);
    s = _replMgr->startup();
    if (!s.ok()) {
        LOG(WARNING) << "start up repl manager failed!";
        return s;
    }

    _indexMgr = std::make_unique<IndexManager>(shared_from_this(), cfg);
    s = _indexMgr->startup();
    if (!s.ok()) {
      return s;
    }

    // listener should be the lastone to run.
    s = _network->run();
    if (!s.ok()) {
        return s;
    } else {
        LOG(WARNING) << "ready to accept connections at "
            << cfg->bindIp << ":" << cfg->port;
    }

    _isRunning.store(true, std::memory_order_relaxed);
    _isStopped.store(false, std::memory_order_relaxed);

    // server stats monitor
    _ftmcThd = std::make_unique<std::thread>([this] {
        ftmc();
    });
    return {ErrorCodes::ERR_OK, ""};
}

uint64_t ServerEntry::getStartupTimeNs() const {
    return _startupTime;
}

NetworkAsio* ServerEntry::getNetwork() {
    return _network.get();
}

ReplManager* ServerEntry::getReplManager() {
    return _replMgr.get();
}

SegmentMgr* ServerEntry::getSegmentMgr() const {
    return _segmentMgr.get();
}

PessimisticMgr* ServerEntry::getPessimisticMgr() {
    return _pessimisticMgr.get();
}

IndexManager* ServerEntry::getIndexMgr() {
    return _indexMgr.get();
}

const std::shared_ptr<std::string> ServerEntry::requirepass() const {
    std::lock_guard<std::mutex> lk(_mutex);
    return _requirepass;
}

const std::shared_ptr<std::string> ServerEntry::masterauth() const {
    std::lock_guard<std::mutex> lk(_mutex);
    return _masterauth;
}

bool ServerEntry::versionIncrease() const {
    return _versionIncrease;
}

void ServerEntry::addSession(std::shared_ptr<Session> sess) {
    std::lock_guard<std::mutex> lk(_mutex);
    if (!_isRunning.load(std::memory_order_relaxed)) {
        LOG(WARNING) << "session:" << sess->id()
            << " comes when stopping, ignore it";
        return;
    }
    // TODO(deyukong): max conns


    // NOTE(deyukong): first driving force
    sess->start();
    uint64_t id = sess->id();
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

std::list<std::shared_ptr<Session>> ServerEntry::getAllSessions() const {
    std::lock_guard<std::mutex> lk(_mutex);
    uint64_t start = nsSinceEpoch();
    std::list<std::shared_ptr<Session>> sesses;
    for (const auto& kv : _sessions) {
        sesses.push_back(kv.second);
    }
    uint64_t delta = (nsSinceEpoch() - start)/1000000;
    if (delta >= 5) {
        LOG(WARNING) << "get sessions cost:" << delta << "ms"
                     << "length:" << sesses.size();
    }
    return sesses;
}

bool ServerEntry::processRequest(uint64_t connId) {
    Session *sess = nullptr;
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
    // general log if nessarry
    sess->getServerEntry()->logGeneral(sess);

    auto expCmdName = Command::precheck(sess);
    if (!expCmdName.ok()) {
        sess->setResponse(
            redis_port::errorReply(expCmdName.status().toString()));
        return true;
    }
    if (expCmdName.value() == "fullsync") {
        LOG(WARNING) << "connId:" << connId << " socket borrowed";
        NetSession *ns = dynamic_cast<NetSession*>(sess);
        INVARIANT(ns != nullptr);
        std::vector<std::string> args = ns->getArgs();
        // we have called precheck, it should have 2 args
        INVARIANT(args.size() == 2);
        _replMgr->supplyFullSync(ns->borrowConn(), args[1]);
        return false;
    } else if (expCmdName.value() == "incrsync") {
        LOG(WARNING) << "connId:" << connId << " socket borrowed";
        NetSession *ns = dynamic_cast<NetSession*>(sess);
        INVARIANT(ns != nullptr);
        std::vector<std::string> args = ns->getArgs();
        // we have called precheck, it should have 2 args
        INVARIANT(args.size() == 4);
        _replMgr->registerIncrSync(ns->borrowConn(), args[1], args[2], args[3]);
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

void ServerEntry::appendJSONStat(rapidjson::Writer<rapidjson::StringBuffer>& w,
                                 const std::set<std::string>& sections) const {
    if (sections.find("network") != sections.end()) {
        w.Key("network");
        w.StartObject();
        w.Key("sticky_packets");
        w.Uint64(_netMatrix->stickyPackets.get());
        w.Key("conn_created");
        w.Uint64(_netMatrix->connCreated.get());
        w.Key("conn_released");
        w.Uint64(_netMatrix->connReleased.get());
        w.Key("invalid_packets");
        w.Uint64(_netMatrix->invalidPackets.get());
        w.EndObject();
    }
    if (sections.find("request") != sections.end()) {
        w.Key("request");
        w.StartObject();
        w.Key("processed");
        w.Uint64(_reqMatrix->processed.get());
        w.Key("process_cost");
        w.Uint64(_reqMatrix->processCost.get());
        w.Key("send_packet_cost");
        w.Uint64(_reqMatrix->sendPacketCost.get());
        w.EndObject();
    }
    if (sections.find("req_pool") != sections.end()) {
        w.Key("req_pool");
        w.StartObject();
        w.Key("in_queue");
        w.Uint64(_poolMatrix->inQueue.get());
        w.Key("executed");
        w.Uint64(_poolMatrix->executed.get());
        w.Key("queue_time");
        w.Uint64(_poolMatrix->queueTime.get());
        w.Key("execute_time");
        w.Uint64(_poolMatrix->executeTime.get());
        w.EndObject();
    }
}

// full-time matrix collect
void ServerEntry::ftmc() {
    using namespace std::chrono_literals;  // NOLINT(build/namespaces)
    LOG(INFO) << "server ftmc thread starts";
    auto oldNetMatrix = *_netMatrix;
    auto oldPoolMatrix = *_poolMatrix;
    auto oldReqMatrix = *_reqMatrix;
    while (_isRunning.load(std::memory_order_relaxed)) {
        std::unique_lock<std::mutex> lk(_mutex);
        bool ok = _eventCV.wait_for(lk, 1000ms, [this] {
            return _isRunning.load(std::memory_order_relaxed) == false;
        });
        if (ok) {
            LOG(INFO) << "server ftmc thread exits";
            return;
        }
        if (!_ftmcEnabled.load(std::memory_order_relaxed)) {
            continue;
        }
        auto tmpNetMatrix = *_netMatrix - oldNetMatrix;
        auto tmpPoolMatrix = *_poolMatrix - oldPoolMatrix;
        auto tmpReqMatrix = *_reqMatrix - oldReqMatrix;
        oldNetMatrix = *_netMatrix;
        oldPoolMatrix = *_poolMatrix;
        oldReqMatrix = *_reqMatrix;
        LOG(INFO) << "network matrix status:\n" << tmpNetMatrix.toString();
        LOG(INFO) << "pool matrix status:\n" << tmpPoolMatrix.toString();
        LOG(INFO) << "req matrix status:\n" << tmpReqMatrix.toString();
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
    _indexMgr->stop();
    _sessions.clear();

    _network.reset();
    _replMgr.reset();
    _indexMgr.reset();
    _pessimisticMgr.reset();
    _segmentMgr.reset();

    _ftmcThd->join();
    LOG(INFO) << "server stops complete...";
    _isStopped.store(true, std::memory_order_relaxed);
    _eventCV.notify_all();
}

void ServerEntry::toggleFtmc(bool enable) {
    _ftmcEnabled.store(enable, std::memory_order_relaxed);
}

}  // namespace tendisplus
