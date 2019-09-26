#include <utility>
#include <memory>
#include <algorithm>
#include <chrono>
#include <string>
#include <list>
#include <mutex>
#include "glog/logging.h"
#include "tendisplus/server/server_entry.h"
#include "tendisplus/server/server_params.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/time.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/storage/rocks/rocks_kvstore.h"
#include "tendisplus/utils/string.h"

namespace tendisplus {

ServerEntry::ServerEntry()
        :_ftmcEnabled(false),
         _isRunning(false),
         _isStopped(true),
         _isShutdowned(false),
         _startupTime(nsSinceEpoch()),
         _network(nullptr),
         _segmentMgr(nullptr),
         _replMgr(nullptr),
         _indexMgr(nullptr),
         _pessimisticMgr(nullptr),
         _mgLockMgr(nullptr),
         _catalog(nullptr),
         _netMatrix(std::make_shared<NetworkMatrix>()),
         _poolMatrix(std::make_shared<PoolMatrix>()),
         _reqMatrix(std::make_shared<RequestMatrix>()),
         _ftmcThd(nullptr),
         _requirepass(nullptr),
         _masterauth(nullptr),
         _versionIncrease(true),
         _generalLog(false),
         _checkKeyTypeForSet(false),
         _protoMaxBulkLen(CONFIG_DEFAULT_PROTO_MAX_BULK_LEN),
         _dbNum(CONFIG_DEFAULT_DBNUM),
         _maxClients(CONFIG_DEFAULT_MAX_CLIENTS),
         _slowlogId(0),
         _scheduleNum(0) {
}

ServerEntry::ServerEntry(const std::shared_ptr<ServerParams>& cfg)
    : ServerEntry() {
    _requirepass = std::make_shared<std::string>(cfg->requirepass);
    _masterauth = std::make_shared<std::string>(cfg->masterauth);
    _versionIncrease = cfg->versionIncrease;
    _generalLog = cfg->generalLog;
    _checkKeyTypeForSet = cfg->checkKeyTypeForSet;
    _protoMaxBulkLen = cfg->protoMaxBulkLen;
    _dbNum = cfg->dbNum;
    _maxClients = cfg->maxClients;
    _slowlogLogSlowerThan = cfg->slowlogLogSlowerThan;
    _slowlogFlushInterval = cfg->slowlogFlushInterval;
}

void ServerEntry::installPessimisticMgrInLock(
        std::unique_ptr<PessimisticMgr> o) {
    _pessimisticMgr = std::move(o);
}

void ServerEntry::installMGLockMgrInLock(
    std::unique_ptr<mgl::MGLockMgr> o) {
    _mgLockMgr = std::move(o);
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

    LOG(INFO) << sess->getCmdStr();
}

void ServerEntry::logWarning(const std::string& str, Session* sess) {
    std::stringstream ss;
    if (sess) {
        ss << sess->id() << "cmd:" << sess->getCmdStr();
    }

    ss << ", warning:" << str;

    LOG(WARNING) << ss.str();
}

void ServerEntry::logError(const std::string& str, Session* sess) {
    std::stringstream ss;
    if (sess) {
        ss << sess->id() << "cmd:" << sess->getCmdStr();
    }

    ss << ", error:" << str;

    LOG(ERROR) << ss.str();
}

uint32_t ServerEntry::getKVStoreCount() const {
    INVARIANT_D(_kvstores.size() == _catalog->getKVStoreCount());
    return _catalog->getKVStoreCount();
}

Status ServerEntry::startup(const std::shared_ptr<ServerParams>& cfg) {
    std::lock_guard<std::mutex> lk(_mutex);

    LOG(INFO) << "ServerEntry::startup,,,";

    uint32_t kvStoreCount = cfg->kvStoreCount;
    uint32_t chunkSize = cfg->chunkSize;

    // set command config
    Command::setNoExpire(cfg->noexpire);

    // catalog init
    auto catalog = std::make_unique<Catalog>(
        std::move(std::unique_ptr<KVStore>(
            new RocksKVStore(CATALOG_NAME, cfg, nullptr, false,
                KVStore::StoreMode::READ_WRITE, RocksKVStore::TxnMode::TXN_PES,
                cfg->maxBinlogKeepNum))),
          kvStoreCount, chunkSize);
    installCatalog(std::move(catalog));

    // kvstore init
    auto blockCache =
        rocksdb::NewLRUCache(cfg->rocksBlockcacheMB * 1024 * 1024LL, 6, cfg->strictCapacityLimit);
    std::vector<PStore> tmpStores;
    tmpStores.reserve(kvStoreCount);
    for (size_t i = 0; i < kvStoreCount; ++i) {
        auto meta = _catalog->getStoreMainMeta(i);
        KVStore::StoreMode mode = KVStore::StoreMode::READ_WRITE;

        if (meta.ok()) {
            mode = meta.value()->storeMode;
        } else if (meta.status().code() == ErrorCodes::ERR_NOTFOUND) {
            auto pMeta = std::unique_ptr<StoreMainMeta>(
                    new StoreMainMeta(i, KVStore::StoreMode::READ_WRITE));
            Status s = _catalog->setStoreMainMeta(*pMeta);
            if (!s.ok()) {
                LOG(FATAL) << "catalog setStoreMainMeta error:"
                    << s.toString();
                return s;
            }
        } else {
            LOG(FATAL) << "catalog getStoreMainMeta error:"
                << meta.status().toString();
            return meta.status();
        }

        tmpStores.emplace_back(std::unique_ptr<KVStore>(
            new RocksKVStore(std::to_string(i), cfg, blockCache, true, mode,
                RocksKVStore::TxnMode::TXN_PES, cfg->maxBinlogKeepNum)));
    }

    /*auto vm = _catalog-> getVersionMeta();
    if (vm.ok()) {
        _cfrmTs = vm.value()->timestamp;
        _cfrmVersion = vm.value()->version;
    } else if (vm.status().code() == ErrorCodes::ERR_NOTFOUND) {
        auto pVm = std::make_unique<VersionMeta>();
        Status s = _catalog->setVersionMeta(*pVm);
        if (!s.ok()) {
            LOG(FATAL) << "catalog setVersionMeta error:"
                << s.toString();
            return s;
        }
    } else {
        LOG(FATAL) << "catalog getVersionMeta error:"
            << vm.status().toString();
        return vm.status();
    }*/

    installStoresInLock(tmpStores);
    INVARIANT(getKVStoreCount() == kvStoreCount);

    // segment mgr
    auto tmpSegMgr = std::unique_ptr<SegmentMgr>(
        new SegmentMgrFnvHash64(_kvstores, chunkSize));
    installSegMgrInLock(std::move(tmpSegMgr));

    // pessimisticMgr
    auto tmpPessimisticMgr = std::make_unique<PessimisticMgr>(
        kvStoreCount);
    installPessimisticMgrInLock(std::move(tmpPessimisticMgr));

    auto tmpMGLockMgr = std::make_unique <mgl::MGLockMgr>();
    installMGLockMgrInLock(std::move(tmpMGLockMgr));

    // request executePool
    size_t cpuNum = std::thread::hardware_concurrency();
    if (cpuNum == 0) {
        return {ErrorCodes::ERR_INTERNAL, "cpu num cannot be detected"};
    }
    uint32_t threadnum = std::max(size_t(4), cpuNum/2);
    if (cfg->executorThreadNum != 0) {
        threadnum = cfg->executorThreadNum;
    }
    LOG(INFO) << "ServerEntry::startup executor thread num:" << threadnum
        << " executorThreadNum:" << cfg->executorThreadNum;
    for (uint32_t i = 0; i < threadnum; ++i) {
        auto executor = std::make_unique<WorkerPool>("req-exec", _poolMatrix);
        Status s = executor->startup(1);
        if (!s.ok()) {
            return s;
        }
        _executorList.push_back(std::move(executor));
    }

    // network
    _network = std::make_unique<NetworkAsio>(shared_from_this(),
                                             _netMatrix,
                                             _reqMatrix);
    Status s = _network->prepare(cfg->bindIp, cfg->port, cfg->netIoThreadNum);
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

    if (!cfg->noexpire) {
        _indexMgr = std::make_unique<IndexManager>(shared_from_this(), cfg);
        s = _indexMgr->startup();
        if (!s.ok()) {
            return s;
        }
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

    // init slowlog
    initSlowlog(cfg->slowlogPath);

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

mgl::MGLockMgr* ServerEntry::getMGLockMgr() {
    return _mgLockMgr.get();
}

IndexManager* ServerEntry::getIndexMgr() {
    return _indexMgr.get();
}

const std::shared_ptr<std::string> ServerEntry::requirepass() const {
    return _requirepass;
}

const std::shared_ptr<std::string> ServerEntry::masterauth() const {
    return _masterauth;
}

bool ServerEntry::versionIncrease() const {
    return _versionIncrease;
}

bool ServerEntry::addSession(std::shared_ptr<Session> sess) {
    std::lock_guard<std::mutex> lk(_mutex);
    if (!_isRunning.load(std::memory_order_relaxed)) {
        LOG(WARNING) << "session:" << sess->id()
            << " comes when stopping, ignore it";
        return false;
    }

    // TODO(deyukong): max conns
    // NOTE(deyukong): first driving force
    sess->start();
    uint64_t id = sess->id();
    if (_sessions.find(id) != _sessions.end()) {
        LOG(FATAL) << "add session:" << id << ",session id already exists";
    }
    DLOG(INFO) << "ServerEntry addSession id:" << id << " addr:" << sess->getRemote();
    _sessions[id] = std::move(sess);
    return true;
}

size_t ServerEntry::getSessionCount() {
    std::lock_guard<std::mutex> lk(_mutex);
    return _sessions.size();
}

Status ServerEntry::cancelSession(uint64_t connId) {
    std::lock_guard<std::mutex> lk(_mutex);
    if (!_isRunning.load(std::memory_order_relaxed)) {
        return {ErrorCodes::ERR_BUSY, "server is shutting down"};
    }
    auto it = _sessions.find(connId);
    if (it == _sessions.end()) {
        return {ErrorCodes::ERR_NOTFOUND, "session not found:" + std::to_string(connId)};
    }
    LOG(INFO) << "ServerEntry cancelSession id:" << connId << " addr:" << it->second->getRemote();
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
    SessionCtx* pCtx = it->second->getCtx();
    INVARIANT(pCtx != nullptr);
    if (pCtx->getIsMonitor()) {
        DelMonitorNoLock(connId);
    }
    DLOG(INFO) << "ServerEntry endSession id:" << connId << " addr:" << it->second->getRemote();
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

void ServerEntry::AddMonitor(Session* sess) {
    std::lock_guard<std::mutex> lk(_mutex);
    for (const auto& monSess : _monitors) {
        if (monSess->id() == sess->id()) {
            return;
        }
    }
    _monitors.push_back(std::shared_ptr<Session>(sess));
}

void ServerEntry::DelMonitorNoLock(uint64_t connId) {
    for (auto it = _monitors.begin(); it != _monitors.end(); ++it) {
        if (it->get()->id() == connId) {
            _monitors.erase(it);
            break;
        }
    }
}

void ServerEntry::replyMonitors(Session* sess) {
    if (_monitors.size() <= 0) {
        return;
    }

    std::string info = "+";

    auto timeNow = std::chrono::duration_cast<std::chrono::microseconds>
        (std::chrono::system_clock::now().time_since_epoch());
    uint64_t timestamp = timeNow.count();

    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    uint32_t dbId = pCtx->getDbId();

    info += std::to_string(timestamp/1000000) + "." + std::to_string(timestamp%1000000);
    info += " [" + std::to_string(dbId) + " " + sess->getRemote() + "] ";
    const auto& args = sess->getArgs();
    for (uint i = 0; i < args.size(); ++i) {
        info += "\"" + args[i] + "\"";
        if (i != (args.size() -1)) {
            info += " ";
        }
    }
    info += "\r\n";

    std::lock_guard<std::mutex> lk(_mutex);

    for (auto& it : _monitors) {
        it->setResponse(info);
    }
}

bool ServerEntry::processRequest(Session *sess) {
    if (!_isRunning.load(std::memory_order_relaxed)) {
        return false;
    }
    // general log if nessarry
    sess->getServerEntry()->logGeneral(sess);
    // NOTE(vinchen): process the ExtraProtocol of timestamp and version
    /*auto s = sess->processExtendProtocol();
    if (!s.ok()) {
        sess->setResponse(
            redis_port::errorReply(s.toString()));
        return true;
    }*/

    auto expCmdName = Command::precheck(sess);
    if (!expCmdName.ok()) {
        sess->setResponse(
            redis_port::errorReply(expCmdName.status().toString()));
        return true;
    }

    replyMonitors(sess);

    if (expCmdName.value() == "fullsync") {
        LOG(WARNING) << "[master] session id:" << sess->id() << " socket borrowed";
        NetSession *ns = dynamic_cast<NetSession*>(sess);
        INVARIANT(ns != nullptr);
        std::vector<std::string> args = ns->getArgs();
        // we have called precheck, it should have 2 args
        INVARIANT(args.size() == 2);
        _replMgr->supplyFullSync(ns->borrowConn(), args[1]);
        return false;
    } else if (expCmdName.value() == "incrsync") {
        LOG(WARNING) << "[master] session id:" << sess->id() << " socket borrowed";
        NetSession *ns = dynamic_cast<NetSession*>(sess);
        INVARIANT(ns != nullptr);
        std::vector<std::string> args = ns->getArgs();
        // we have called precheck, it should have 2 args
        INVARIANT(args.size() == 4);
        _replMgr->registerIncrSync(ns->borrowConn(), args[1], args[2], args[3]);
        return false;
    } else if (expCmdName.value() == "quit") {
        LOG(INFO) << "quit command";
        NetSession *ns = dynamic_cast<NetSession*>(sess);
        INVARIANT(ns != nullptr);
        ns->setCloseAfterRsp();
        ns->setResponse(Command::fmtOK());
        return true;
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

Status ServerEntry::destroyStore(Session *sess,
            uint32_t storeId, bool isForce) {
    auto expdb = getSegmentMgr()->getDb(sess, storeId,
        mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
        return expdb.status();
    }

    auto store = expdb.value().store;
    if (!isForce) {
        if (!store->isEmpty()) {
            return{ ErrorCodes::ERR_INTERNAL,
                "try to close an unempty store" };
        }
    }

    if (!store->isPaused()) {
        return{ ErrorCodes::ERR_INTERNAL,
            "please pausestore first before destroystore" };
    }

    if (store->getMode() == KVStore::StoreMode::READ_WRITE) {
        // TODO(vinchen)
        // NOTE(vinchen): maybe it should create a binlog here to
        // destroy the store of slaves.
        // But it maybe hard to confirm whether all the slaves apply
        // this binlog before the master destroy. (check MPOVStatus?)
    }

    auto meta = getCatalog()->getStoreMainMeta(storeId);
    if (!meta.ok()) {
        LOG(WARNING) << "get store main meta:" << storeId
            << " failed:" << meta.status().toString();
        return meta.status();
    }
    meta.value()->storeMode = KVStore::StoreMode::STORE_NONE;
    Status status = getCatalog()->setStoreMainMeta(*meta.value());
    if (!status.ok()) {
        LOG(WARNING) << "set store main meta:" << storeId
            << " failed:" << status.toString();
        return status;
    }

    status = store->destroy();
    if (!status.ok()) {
        LOG(ERROR) << "destroy store :" << storeId
            << " failed:" << status.toString();
        return status;
    }
    INVARIANT(store->getMode() == KVStore::StoreMode::STORE_NONE);

    status = _replMgr->stopStore(storeId);
    if (!status.ok()) {
        LOG(ERROR) << "replMgr stopStore :" << storeId
            << " failed:" << status.toString();
        return status;
    }

    status = _indexMgr->stopStore(storeId);
    if (!status.ok()) {
        LOG(ERROR) << "indexMgr stopStore :" << storeId
            << " failed:" << status.toString();
        return status;
    }

    return status;
}

Status ServerEntry::setStoreMode(PStore store,
    KVStore::StoreMode mode) {

    // assert held the X lock of store
    if (store->getMode() == mode) {
        return{ ErrorCodes::ERR_OK, "" };
    }

    auto catalog = getCatalog();
    Status status = store->setMode(mode);
    if (!status.ok()) {
        LOG(FATAL) << "ServerEntry::setStoreMode error, "
                << status.toString();
        return status;
    }
    auto storeId = tendisplus::stoul(store->dbId());
    if (!storeId.ok()) {
        return storeId.status();
    }
    auto meta = catalog->getStoreMainMeta(storeId.value());
    meta.value()->storeMode = mode;

    return catalog->setStoreMainMeta(*meta.value());
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
        // TODO(vinchen): we should create a view here
        LOG(INFO) << "network matrix status:\n" << tmpNetMatrix.toString();
        LOG(INFO) << "pool matrix status:\n" << tmpPoolMatrix.toString();
        LOG(INFO) << "req matrix status:\n" << tmpReqMatrix.toString();
    }
}

void ServerEntry::waitStopComplete() {
    using namespace std::chrono_literals;  // NOLINT(build/namespaces)
    bool shutdowned = false;
    while (_isRunning.load(std::memory_order_relaxed)) {
        std::unique_lock<std::mutex> lk(_mutex);
        bool ok = _eventCV.wait_for(lk, 1000ms, [this] {
            return _isRunning.load(std::memory_order_relaxed) == false
                && _isStopped.load(std::memory_order_relaxed) == true;
        });
        if (ok) {
            return;
        }

        if (_isShutdowned.load(std::memory_order_relaxed)) {
            LOG(INFO) << "shutdown command";
            shutdowned = true;
            break;
        }
    }

    // NOTE(vinchen): it can't hold the _mutex before stop()
    if (shutdowned) {
        stop();
    }
}

void ServerEntry::handleShutdownCmd() {
    _isShutdowned.store(true, std::memory_order_relaxed);
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
    for (executor : _executorList) {
        executor->stop();
    }
    _replMgr->stop();
    _indexMgr->stop();
    _sessions.clear();

    if (!_isShutdowned.load(std::memory_order_relaxed)) {
        // NOTE(vinchen): if it's not the shutdown command, it should reset the
        // workerpool to decr the referent count of share_ptr<server>
        _network.reset();
        for (executor : _executorList) {
            executor.reset();
        }
        _replMgr.reset();
        _indexMgr.reset();
        _pessimisticMgr.reset();
        _mgLockMgr.reset();
        _segmentMgr.reset();
    }

    // stop the rocksdb
    std::stringstream ss;
    Status status = _catalog->stop();
    if (!status.ok()) {
        ss << "stop kvstore catalog failed: "
                        << status.toString();
        LOG(ERROR) << ss.str();
    }

    for (auto& store : _kvstores) {
        Status status = store->stop();
        if (!status.ok()) {
            ss.clear();
            ss << "stop kvstore " << store->dbId() << "failed: "
                        << status.toString();
            LOG(ERROR) << ss.str();
        }
    }

    _ftmcThd->join();
    _slowLog.close();
    LOG(INFO) << "server stops complete...";
    _isStopped.store(true, std::memory_order_relaxed);
    _eventCV.notify_all();
}

void ServerEntry::toggleFtmc(bool enable) {
    _ftmcEnabled.store(enable, std::memory_order_relaxed);
}

uint64_t ServerEntry::getTsEp() const {
    return _tsFromExtendedProtocol.load(std::memory_order_relaxed);
}

void ServerEntry::setTsEp(uint64_t timestamp) {
    _tsFromExtendedProtocol.store(timestamp, std::memory_order_relaxed);
}

Status ServerEntry::setTsVersion(const std::string& name, uint64_t ts, uint64_t version) {
    if (confirmTs(name) == 0 && confirmVer(name) == 0) {
        std::lock_guard<std::shared_timed_mutex> lock(_rwlock);
        _cfrmTs[name] = ts;
        _cfrmVersion[name] = version;
    } else {
        std::shared_lock<std::shared_timed_mutex> lock(_rwlock);
        _cfrmTs[name] = ts;
        _cfrmVersion[name] = version;
    }

    return {ErrorCodes::ERR_OK, ""};
}

void ServerEntry::setMaxCli(uint32_t max) {
    _maxClients = max;
}

uint32_t ServerEntry::getMaxCli() {
    return _maxClients;
}

Status ServerEntry::initSlowlog(std::string logPath) {
    _slowLog.open(logPath, std::ofstream::app);
    if (!_slowLog.is_open()) {
        std::stringstream ss;
        ss << "open:" << logPath << " failed";
        return {ErrorCodes::ERR_INTERNAL, ss.str()};
    }

    return {ErrorCodes::ERR_OK, ""};
}

void ServerEntry::setSlowlogLogSlowerThan(uint64_t time) {
    _slowlogLogSlowerThan = time;
}

uint64_t ServerEntry::getSlowlogLogSlowerThan() {
    return _slowlogLogSlowerThan;
}
    
void ServerEntry::slowlogPushEntryIfNeeded(uint64_t time, uint64_t duration, 
            const std::vector<std::string>& args) {
    if(duration > _slowlogLogSlowerThan) {
        std::unique_lock<std::mutex> lk(_mutex);
        _slowLog << "#Id: " << _slowlogId.load(std::memory_order_relaxed) << "\n";
        _slowLog << "#Time: " << time << "\n";
        _slowLog << "#Query_time: " << duration << "\n";
        for(size_t i = 0; i < args.size(); ++i) {
            _slowLog << args[i] << " ";
        }
        _slowLog << "\n";
        _slowLog << "#argc: " << args.size() << "\n\n";
        if ((_slowlogId.load(std::memory_order_relaxed)%_slowlogFlushInterval) == 0) {
            _slowLog.flush();
        }
        
        _slowlogId.fetch_add(1, std::memory_order_relaxed);
    }
}

}  // namespace tendisplus
