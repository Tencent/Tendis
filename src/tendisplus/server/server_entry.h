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
#include "tendisplus/storage/kvstore.h"
#include "tendisplus/storage/rocks/rocks_kvstore.h"

namespace tendisplus {
class NetSession;
class NetworkAsio;
class NetworkMatrix;
class PoolMatrix;

class ServerEntry: public std::enable_shared_from_this<ServerEntry> {
 public:
    ServerEntry();
    ServerEntry(const ServerEntry&) = delete;
    ServerEntry(ServerEntry&&) = delete;
    Status startup(const std::shared_ptr<ServerParams>& cfg);
    template <typename fn>
    void schedule(fn&& task) {
        _executor->schedule(std::forward<fn>(task));
    }
    void addSession(std::unique_ptr<NetSession> sess);
    void endSession(uint64_t connId);
    void processRequest(uint64_t connId);
    void installStoresInLock(const std::vector<PStore>&);
    void installSegMgrInLock(std::unique_ptr<SegmentMgr>);
    void stop();
    void waitStopComplete();
    const SegmentMgr* getSegmentMgr() const;
    const std::string& requirepass() const;

 private:
    void ftmc();
    // NOTE(deyukong): _isRunning = true -> running
    // _isRunning = false && _isStopped = false -> stopping in progress
    // _isRunning = false && _isStopped = true -> stop complete
    std::atomic<bool> _isRunning;
    std::atomic<bool> _isStopped;
    std::mutex _mutex;
    std::condition_variable _eventCV;
    std::unique_ptr<NetworkAsio> _network;
    std::map<uint64_t, std::unique_ptr<NetSession>> _sessions;
    std::unique_ptr<WorkerPool> _executor;
    std::unique_ptr<SegmentMgr> _segmentMgr;
    std::vector<PStore> _kvstores;

    std::shared_ptr<NetworkMatrix> _netMatrix;
    std::shared_ptr<PoolMatrix> _poolMatrix;
    std::unique_ptr<std::thread> _ftmcThd;

    std::string _requirepass;
};
}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_SERVER_SERVER_ENTRY_H_
