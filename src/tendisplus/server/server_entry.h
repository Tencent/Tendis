#ifndef SRC_TENDISPLUS_SERVER_SERVER_ENTRY_H_
#define SRC_TENDISPLUS_SERVER_SERVER_ENTRY_H_

#include <vector>
#include <utility>
#include <memory>
#include <map>
#include <string>

#include "tendisplus/network/network.h"
#include "tendisplus/network/worker_pool.h"
#include "tendisplus/server/server_params.h"
#include "tendisplus/server/segment_manager.h"
#include "tendisplus/storage/pessimistic.h"
#include "tendisplus/replication/repl_manager.h"
#include "tendisplus/storage/kvstore.h"
#include "tendisplus/storage/catalog.h"

namespace tendisplus {
class NetSession;
class NetworkAsio;
class NetworkMatrix;
class PoolMatrix;
class Catalog;
class ReplManager;

class ServerEntry: public std::enable_shared_from_this<ServerEntry> {
 public:
    ServerEntry();
    ServerEntry(const ServerEntry&) = delete;
    ServerEntry(ServerEntry&&) = delete;
    Catalog* getCatalog();
    Status startup(const std::shared_ptr<ServerParams>& cfg);
    template <typename fn>
    void schedule(fn&& task) {
        _executor->schedule(std::forward<fn>(task));
    }
    void addSession(std::shared_ptr<NetSession> sess);

    // NOTE(deyukong): be careful, currently, the callpath of
    // serverEntry::endSession is
    // NetSession.endSession -> ServerEntry.endSession
    // -> ServerEntry.eraseSession -> NetSession.~NetSession
    // this function is initially triggered by NetSession.
    // If you want to close a NetSession from serverside, do not 
    // call ServerEntry.endSession, or asio's calllback may
    // meet a nil pointer.
    // Instead, you should call NetSession.cancel to close NetSession's
    // underlying socket and let itself trigger the whole path.
    void endSession(uint64_t connId);

    Status cancelSession(uint64_t connId);

    // returns true if NetSession should continue schedule
    bool processRequest(uint64_t connId);

    void installStoresInLock(const std::vector<PStore>&);
    void installSegMgrInLock(std::unique_ptr<SegmentMgr>);
    void installCatalog(std::unique_ptr<Catalog>);
    void installPessimisticMgrInLock(std::unique_ptr<PessimisticMgr>);

    void stop();
    void waitStopComplete();
    const SegmentMgr* getSegmentMgr() const;
    ReplManager* getReplManager();;
    NetworkAsio* getNetwork();
    PessimisticMgr* getPessimisticMgr();

    const std::shared_ptr<std::string> requirepass() const;
    const std::shared_ptr<std::string> masterauth() const;

 private:
    void ftmc();
    // NOTE(deyukong): _isRunning = true -> running
    // _isRunning = false && _isStopped = false -> stopping in progress
    // _isRunning = false && _isStopped = true -> stop complete
    std::atomic<bool> _isRunning;
    std::atomic<bool> _isStopped;
    mutable std::mutex _mutex;
    std::condition_variable _eventCV;
    std::unique_ptr<NetworkAsio> _network;
    std::map<uint64_t, std::shared_ptr<NetSession>> _sessions;
    std::unique_ptr<WorkerPool> _executor;
    std::unique_ptr<SegmentMgr> _segmentMgr;
    std::unique_ptr<ReplManager> _replMgr;
    std::unique_ptr<PessimisticMgr> _pessimisticMgr;

    std::vector<PStore> _kvstores;
    std::unique_ptr<Catalog> _catalog;

    std::shared_ptr<NetworkMatrix> _netMatrix;
    std::shared_ptr<PoolMatrix> _poolMatrix;
    std::unique_ptr<std::thread> _ftmcThd;

    // NOTE(deyukong):
    // return string's reference have race conditions if changed during
    // runtime. return by value is quite costive.
    std::shared_ptr<std::string> _requirepass;
    std::shared_ptr<std::string> _masterauth;
};
}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_SERVER_SERVER_ENTRY_H_
