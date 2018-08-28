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
    void addSession(std::unique_ptr<NetSession> sess);
    void endSession(uint64_t connId);

    // continue schedule if returns true
    bool processRequest(uint64_t connId);

    void installStoresInLock(const std::vector<PStore>&);
    void installSegMgrInLock(std::unique_ptr<SegmentMgr>);
    void installCatalog(std::unique_ptr<Catalog>);
    void stop();
    void waitStopComplete();
    const SegmentMgr* getSegmentMgr() const;
    const ReplManager* getReplManager() const;

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
    std::map<uint64_t, std::unique_ptr<NetSession>> _sessions;
    std::unique_ptr<WorkerPool> _executor;
    std::unique_ptr<SegmentMgr> _segmentMgr;
    std::unique_ptr<ReplManager> _replMgr;
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
