#include <algorithm>
#include <thread>
#include "tendisplus/server/session.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/network/session_ctx.h"
#include "tendisplus/server/server_entry.h"
#include "tendisplus/utils/string.h"

namespace tendisplus {
std::atomic<uint64_t> Session::_idGen(0);
std::atomic<uint64_t> Session::_aliveCnt(0);

thread_local Session* curSession = nullptr;

Session::Session(std::shared_ptr<ServerEntry> svr)
        :_args(std::vector<std::string>()),
         _server(svr),
         _ctx(std::make_unique<SessionCtx>(this)),
         _sessId(_idGen.fetch_add(1, std::memory_order_relaxed)) {
    _aliveCnt.fetch_add(1, std::memory_order_relaxed);
}

Session::~Session() {
    _aliveCnt.fetch_sub(1, std::memory_order_relaxed);
}

void Session::setCurSess(Session* sess) {
    curSession = sess;
}

Session* Session::getCurSess() {
    return curSession;
}

std::string Session::getCmdStr() const {
    std::stringstream ss;
    size_t i = 0;
    if (_args[0] == "applybinlogsv2") {
        for (auto arg : _args) {
            if (i++ == 2) {
                ss << "[" << arg.size() << "]";
            } else {
                ss << (arg.size() > 0 ? arg : "\"\"");
            }
            if (i <= _args.size() - 1) {
                ss << " ";
            }
        }
    } else {
        for (auto arg : _args) {
            ss << (arg.size() > 0 ? arg : "\"\"");

            if (i++ < _args.size() - 1) {
                ss << " ";
            }

        }
    }
    return ss.str();
}

std::string Session::getName() const {
    std::lock_guard<std::mutex> lk(_baseMutex);
    return _name;
}

void Session::setName(const std::string& name) {
    std::lock_guard<std::mutex> lk(_baseMutex);
    _name = name;
}

uint64_t Session::id() const {
    return _sessId;
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

LocalSession::LocalSession(std::shared_ptr<ServerEntry> svr)
        :Session(svr) {
}

void LocalSession::start() {
    _ctx->setProcessPacketStart(nsSinceEpoch());
}

void LocalSession::setArgs(const std::vector<std::string>& args) {
    _args = args;
    _ctx->setArgsBrief(_args);
}

void LocalSession::setArgs(const std::string& cmd) {
    _args = stringSplit(cmd, " ");
    _ctx->setArgsBrief(_args);
}

Status LocalSession::cancel() {
    return {ErrorCodes::ERR_INTERNAL,
        "LocalSession::cancel should not be called"};
}

int LocalSession::getFd() {
    return -1;
}

std::string LocalSession::getRemote() const {
    return "";
}

void LocalSession::setResponse(const std::string& s) {
    INVARIANT(_respBuf.size() == 0);
    std::copy(s.begin(), s.end(), std::back_inserter(_respBuf));
}

LocalSessionGuard::LocalSessionGuard(std::shared_ptr<ServerEntry> svr) {
    _sess = std::make_shared<LocalSession>(svr);
    if (svr.get()) {
        svr->addSession(_sess);
    }
}

LocalSessionGuard::~LocalSessionGuard() {
    auto svr = _sess->getServerEntry();
    // DLOG(INFO) << "local session, id:" << _sess->id() << " destroyed";
    if (svr.get()) {
        svr->endSession(_sess->id());
    }
}

LocalSession* LocalSessionGuard::getSession() {
    return _sess.get();
}

Status Session::processExtendProtocol() {
    if (!this->getCtx()->isEp()) {
        return{ ErrorCodes::ERR_OK, "" };
    }

    // cmd key timestamp version tendisex
    if (_args.size() < 4) {
        return{ ErrorCodes::ERR_EXTENDED_PROTOCOL, "" };
    }

    uint32_t i = _args.size() - 1;
    if (toLower(_args[i]) == "v1") {
        auto v = tendisplus::stoull(_args[--i]);
        if (!v.ok()) {
            return v.status();
        }
        uint64_t version = v.value();
        v = tendisplus::stoull(_args[--i]);
        if (!v.ok()) {
            return v.status();
        }
        uint64_t timestamp = v.value();
        _ctx->setExtendProtocolValue(timestamp, version);

        // remove the extra args
        _args.pop_back();
        _args.pop_back();
        _args.pop_back();

        return{ ErrorCodes::ERR_OK, "" };
    }

    return{ ErrorCodes::ERR_EXTENDED_PROTOCOL, "" };
}

}  // namespace tendisplus
