#include <utility>
#include <memory>
#include "tendisplus/server/server_entry.h"

namespace tendisplus {
void ServerEntry::addSession(std::unique_ptr<NetSession> sess) {
    std::lock_guard<std::mutex> lk(_mutex);
    // TODO(deyukong): max conns


    // NOTE(deyukong): god's first driving force
    sess->start();

    _sessions.emplace_back(std::move(sess));
}

}  // namespace tendisplus
