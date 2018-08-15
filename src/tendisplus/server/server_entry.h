#ifndef SRC_TENDISPLUS_SERVER_SERVER_ENTRY_H_
#define SRC_TENDISPLUS_SERVER_SERVER_ENTRY_H_

#include <vector>
#include <utility>
#include <memory>
#include <map>

#include "tendisplus/network/network.h"
#include "tendisplus/network/worker_pool.h"

namespace tendisplus {
class NetSession;
class NetworkAsio;
class ServerEntry: public std::enable_shared_from_this<ServerEntry> {
 public:
    ServerEntry() = default;
    ServerEntry(const ServerEntry&) = delete;
    ServerEntry(ServerEntry&&) = delete;
    template <typename fn>
    void schedule(fn&& task) {
        _executor->schedule(std::forward<fn>(task));
    }
    void addSession(std::unique_ptr<NetSession> sess);
    void processReq(uint64_t connId);
 private:
    std::mutex _mutex;
    std::unique_ptr<NetworkAsio> _network;
    std::map<uint64_t, std::unique_ptr<NetSession>> _sessions;
    std::unique_ptr<WorkerPool> _executor;
};
}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_SERVER_SERVER_ENTRY_H_
