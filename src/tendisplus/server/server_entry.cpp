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
        LOG(FATAL) << "add conn:" << id << ",id already exists";
    }
    _sessions[id] = std::move(sess);
}

void ServerEntry::endSession(uint64_t connId) {
    std::lock_guard<std::mutex> lk(_mutex);
    auto it = _sessions.find(connId);
    if (it == _sessions.end()) {
        LOG(FATAL) << "destroy conn:" << connId << ",not exists";
    }
    _sessions.erase(it);
}

void ServerEntry::processReq(uint64_t connId) {
    NetSession *sess = nullptr;
    {
        std::lock_guard<std::mutex> lk(_mutex);
        auto it = _sessions.find(connId);
        if (it == _sessions.end()) {
            LOG(FATAL) << "conn:" << connId << ",invalid state";
        }
        sess = it->second.get();
        if (sess == nullptr) {
            LOG(FATAL) << "conn:" << connId << ",null in servermap";
        }
    }
    sess->setOkRsp();
}

}  // namespace tendisplus
