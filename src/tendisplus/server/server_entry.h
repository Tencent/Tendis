#ifndef SRC_TENDISPLUS_SERVER_SERVER_ENTRY_H_
#define SRC_TENDISPLUS_SERVER_SERVER_ENTRY_H_

#include <vector>
#include <utility>
#include <memory>
#include <map>
#include <string>
#include <list>
#include <set>
#include <shared_mutex>

#include "glog/logging.h"
#include "tendisplus/network/network.h"
#include "tendisplus/network/worker_pool.h"
#include "tendisplus/server/server_params.h"
#include "tendisplus/server/segment_manager.h"
#include "tendisplus/storage/pessimistic.h"
#include "tendisplus/replication/repl_manager.h"
#include "tendisplus/server/index_manager.h"
#include "tendisplus/storage/kvstore.h"
#include "tendisplus/storage/catalog.h"
#include "tendisplus/lock/mgl/mgl_mgr.h"

namespace tendisplus {
class Session;
class NetworkAsio;
class NetworkMatrix;
class PoolMatrix;
class RequestMatrix;
class Catalog;
class ReplManager;
class IndexManager;

#define TENDISPLUS_VERSION "4.0.10-Tendisx-v0.0.1"
#define REDIS_GIT_SHA1 "00000000"
#define REDIS_GIT_DIRTY "0"
#define REDIS_BUILD_ID "TENCENT64site-1562728800"

class ServerEntry;
std::shared_ptr<ServerEntry> getGlobalServer();

class ServerEntry: public std::enable_shared_from_this<ServerEntry> {
 public:
    ServerEntry(const std::shared_ptr<ServerParams>& cfg);
    ServerEntry(const ServerEntry&) = delete;
    ServerEntry(ServerEntry&&) = delete;
    Catalog* getCatalog();
    Status startup(const std::shared_ptr<ServerParams>& cfg);
    uint64_t getStartupTimeNs() const;
    template <typename fn>
    void schedule(fn&& task) {
        _scheduleNum.fetch_add(1, std::memory_order_relaxed);
        int32_t index = _scheduleNum.load(std::memory_order_relaxed) % _executorList.size();
        _executorList[index]->schedule(std::forward<fn>(task));
    }
    std::shared_ptr<ServerParams>& getParams(){
        return _cfg;
    }
    bool addSession(std::shared_ptr<Session> sess);

    // NOTE(deyukong): be careful, currently, the callpath of
    // serverEntry::endSession is
    // NetSession.endSession -> ServerEntry.endSession
    // -> ServerEntry.eraseSession -> NetSession.~NetSession
    // this function is initially triggered by NetSession.
    // If you want to close a NetSession from serverside, do not
    // call ServerEntry.endSession, or the underlay's socket
    // may never have a chance to be destroyed.
    // Instead, you should call NetSession.cancel to close NetSession's
    // underlying socket and let itself trigger the whole path.
    void endSession(uint64_t connId);
    size_t getSessionCount();

    Status cancelSession(uint64_t connId);

    std::list<std::shared_ptr<Session>> getAllSessions() const;

    // returns true if NetSession should continue schedule
    bool processRequest(Session *sess);

    void installStoresInLock(const std::vector<PStore>&);
    void installSegMgrInLock(std::unique_ptr<SegmentMgr>);
    void installCatalog(std::unique_ptr<Catalog>);
    void installPessimisticMgrInLock(std::unique_ptr<PessimisticMgr>);
    void installMGLockMgrInLock(std::unique_ptr<mgl::MGLockMgr> o);

    void stop();
    void waitStopComplete();
    SegmentMgr* getSegmentMgr() const;
    ReplManager* getReplManager();;
    NetworkAsio* getNetwork();
    PessimisticMgr* getPessimisticMgr();
    mgl::MGLockMgr* getMGLockMgr();
    IndexManager* getIndexMgr();

    // TODO(takenliu) : args exist at two places, has better way?
    std::string requirepass() const;
    std::string masterauth() const;
    void setRequirepass(const std::string& v);
    void setMasterauth(const std::string& v);

    bool versionIncrease() const;
    bool checkKeyTypeForSet() const { return _checkKeyTypeForSet; }
    uint32_t protoMaxBulkLen() const { return _protoMaxBulkLen; }
    uint32_t dbNum() const { return _dbNum; }

    const std::vector<PStore>& getStores() const { return _kvstores; }

    void toggleFtmc(bool enable);
    void appendJSONStat(rapidjson::PrettyWriter<rapidjson::StringBuffer>&,
                        const std::set<std::string>& sections) const;
    void getStatInfo(std::stringstream& ss) const;
    void logGeneral(Session *sess);
    void handleShutdownCmd();
    Status setStoreMode(PStore store, KVStore::StoreMode mode);
    Status destroyStore(Session* sess, uint32_t storeId, bool isForce);
    uint32_t getKVStoreCount() const;
    void setTsEp(uint64_t timestamp);
    uint64_t getTsEp() const;
    void AddMonitor(Session* sess);
    void setMaxCli(uint32_t max);
    uint32_t getMaxCli();
    static void logWarning(const std::string& str, Session* sess = nullptr);
    static void logError(const std::string& str, Session* sess = nullptr);
    inline uint64_t confirmTs(const std::string& name) const {
        std::shared_lock<std::shared_timed_mutex> lock(_rwlock);
        auto it = _cfrmTs.find(name);
        return it == _cfrmTs.end() ? 0 : it->second;
    }
    inline uint64_t confirmVer(const std::string& name) const {
        std::shared_lock<std::shared_timed_mutex> lock(_rwlock);
        auto it = _cfrmVersion.find(name);
        return it == _cfrmVersion.end() ? 0 : it->second;
    }
    Status setTsVersion(const std::string& name, uint64_t ts, uint64_t version);
    void slowlogPushEntryIfNeeded(uint64_t time, uint64_t duration, const std::vector<std::string>& args);
    Status initSlowlog(std::string logPath);
    void resetSlowlogNum() {
        _slowlogId = 0;
    }
    uint64_t getSlowlogNum() {
        return _slowlogId.load(std::memory_order_relaxed);
    }
    void onBackupEnd() {
        std::lock_guard<std::mutex> lk(_mutex);
        _lastBackupTime = sinceEpoch();
        _backupTimes.fetch_add(1, std::memory_order_relaxed);
    }
    uint64_t getLastBackupTime() {
        return _lastBackupTime;
    }
    uint64_t getBackupTimes() {
        return _backupTimes.load(std::memory_order_relaxed);
    }

 private:
    ServerEntry();
    void ftmc();
    void replyMonitors(Session* sess);
    void DelMonitorNoLock(uint64_t connId);

    // NOTE(deyukong): _isRunning = true -> running
    // _isRunning = false && _isStopped = false -> stopping in progress
    // _isRunning = false && _isStopped = true -> stop complete
    std::atomic<bool> _ftmcEnabled;
    std::atomic<bool> _isRunning;
    std::atomic<bool> _isStopped;
    // whether shutdown command is excuted
    std::atomic<bool> _isShutdowned;
    uint64_t _startupTime;
    mutable std::mutex _mutex;
    std::condition_variable _eventCV;
    std::unique_ptr<NetworkAsio> _network;
    std::map<uint64_t, std::shared_ptr<Session>> _sessions;
    std::vector<std::unique_ptr<WorkerPool>> _executorList;
    std::unique_ptr<SegmentMgr> _segmentMgr;
    std::unique_ptr<ReplManager> _replMgr;
    std::unique_ptr<IndexManager> _indexMgr;
    std::unique_ptr<PessimisticMgr> _pessimisticMgr;
    std::unique_ptr<mgl::MGLockMgr> _mgLockMgr;

    std::vector<PStore> _kvstores;
    std::unique_ptr<Catalog> _catalog;

    std::shared_ptr<NetworkMatrix> _netMatrix;
    std::shared_ptr<PoolMatrix> _poolMatrix;
    std::shared_ptr<RequestMatrix> _reqMatrix;
    std::unique_ptr<std::thread> _ftmcThd;

    // NOTE(deyukong):
    // return string's reference have race conditions if changed during
    // runtime. return by value is quite costive.
    std::string _requirepass;
    std::string _masterauth;
    bool _versionIncrease;
    bool _generalLog;
    bool _checkKeyTypeForSet;
    uint32_t _protoMaxBulkLen;
    uint32_t _dbNum;
    std::ofstream _slowLog;
    std::atomic<uint64_t> _slowlogId;
    std::atomic<uint64_t> _tsFromExtendedProtocol;
    mutable std::shared_timed_mutex _rwlock;
    std::map<std::string, uint64_t> _cfrmTs;
    std::map<std::string, uint64_t> _cfrmVersion;
    std::list<std::shared_ptr<Session>> _monitors;
    std::atomic<uint64_t> _scheduleNum;
    std::shared_ptr<ServerParams> _cfg;
    std::atomic<uint64_t> _lastBackupTime;
    std::atomic<uint64_t> _backupTimes;
};
}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_SERVER_SERVER_ENTRY_H_
