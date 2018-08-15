#include <utility>
#include <memory>
#include "glog/logging.h"
#include "tendisplus/server/server_entry.h"

namespace tendisplus {
void ServerEntry::addSession(std::unique_ptr<NetSession> sess) {
    std::lock_guard<std::mutex> lk(_mutex);
    // TODO(deyukong): max conns


    // NOTE(deyukong): god's first driving force
    sess->start();
    uint64_t id = sess->getConnId();
    if (_sessions.find(id) != _sessions.end()) {
        LOG(FATAL) << "conn:" << id << ",invalid state";
    }
    _sessions[id] = std::move(sess);
}

void ServerEntry::processReq(uint64_t connId) {
}

}  // namespace tendisplus
