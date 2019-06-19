#ifndef SRC_TENDISPLUS_SERVER_SERVER_ENTRY_H_
#define SRC_TENDISPLUS_SERVER_SERVER_ENTRY_H_

#include <vector>
#include <utility>
#include <memory>
#include <map>
#include <string>
#include <list>
#include <set>

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
        _executor->schedule(std::forward<fn>(task));
    }
    void addSession(std::shared_ptr<Session> sess);

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

    Status cancelSession(uint64_t connId);

    std::list<std::shared_ptr<Session>> getAllSessions() const;

    // returns true if NetSession should continue schedule
    bool processRequest(uint64_t connId);

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

    const std::shared_ptr<std::string> requirepass() const;
    const std::shared_ptr<std::string> masterauth() const;
    bool versionIncrease() const;
    bool checkKeyTypeForSet() const { return _checkKeyTypeForSet; }
    uint32_t protoMaxBulkLen() const { return _protoMaxBulkLen; }
    uint32_t dbNum() const { return _dbNum; }

    std::vector<PStore> getStores() const;

    void toggleFtmc(bool enable);
    void appendJSONStat(rapidjson::Writer<rapidjson::StringBuffer>&,
                        const std::set<std::string>& sections) const;
    void logGeneral(Session *sess);
    void handleShutdownCmd();
    Status setStoreMode(PStore store, KVStore::StoreMode mode);
    Status destroyStore(Session* sess, uint32_t storeId, bool isForce);
    uint32_t getKVStoreCount() const;
    void setTsEp(uint64_t timestamp);
    uint64_t getTsEp() const;
    static void logWarning(const std::string& str, Session* sess = nullptr);
    static void logError(const std::string& str, Session* sess = nullptr);

 private:
    ServerEntry();
    void ftmc();
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
    std::unique_ptr<WorkerPool> _executor;
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
    std::shared_ptr<std::string> _requirepass;
    std::shared_ptr<std::string> _masterauth;
    bool _versionIncrease;
    bool _generalLog;
    bool _checkKeyTypeForSet;
    uint32_t _protoMaxBulkLen;
    uint32_t _dbNum;
    std::atomic<uint64_t> _tsFromExtendedProtocol;
};
}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_SERVER_SERVER_ENTRY_H_
