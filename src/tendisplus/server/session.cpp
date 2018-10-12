#include <algorithm>
#include "tendisplus/server/session.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/network/session_ctx.h"

namespace tendisplus {
std::atomic<uint64_t> Session::_idGen(0);
std::atomic<uint64_t> Session::_aliveCnt(0);

Session::Session(std::shared_ptr<ServerEntry> svr)
        :_args(std::vector<std::string>()),
         _respBuf(std::vector<char>()),
         _server(svr),
         _ctx(std::make_unique<SessionCtx>()),
         _sessId(_idGen.fetch_add(1, std::memory_order_relaxed)) {
    _aliveCnt.fetch_add(1, std::memory_order_relaxed);
}

Session::~Session() {
    _aliveCnt.fetch_sub(1, std::memory_order_relaxed);
}

uint64_t Session::id() const {
    return _sessId;
}

void Session::setResponse(const std::string& s) {
    INVARIANT(_respBuf.size() == 0);
    std::copy(s.begin(), s.end(), std::back_inserter(_respBuf));
}

const std::vector<std::string>& Session::getArgs() const {
    return _args;
}

std::shared_ptr<ServerEntry> Session::getServerEntry() const {
    return _server;
}

SessionCtx *Session::getCtx() const {
    return _ctx.get();
}

}  // namespace tendisplus
