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

Session::Session(std::shared_ptr<ServerEntry> svr, Type type)
  : Session(svr.get(), type) {}

Session::Session(ServerEntry* svr, Type type)
  : _args(std::vector<std::string>()),
    _server(svr),
    _ctx(std::make_unique<SessionCtx>(this)),
    _type(type),
    _timestamp(msSinceEpoch()),
    _sessId(_idGen.fetch_add(1, std::memory_order_relaxed)) {
  _aliveCnt.fetch_add(1, std::memory_order_relaxed);
}

Session::~Session() {
  _aliveCnt.fetch_sub(1, std::memory_order_relaxed);
  // LOG(INFO) << id() << " " <<  _server->getParams()->port <<  " aaaaaaaaa
  // session("
  //    << getTypeStr() << ")"<< " is destroyed.";
#ifdef TENDIS_DEBUG
  if (_type == Type::CLUSTER) {
    DLOG(INFO) << "cluster session " << id() << " is destroyed.";
  }
#endif
}

uint64_t Session::getCtime() const {
  return _timestamp;
}

void Session::setCurSess(Session* sess) {
  curSession = sess;
}

Session* Session::getCurSess() {
  return curSession;
}

std::string Session::getTypeStr() const {
  static std::string ts[] = {"Net", "Local", "Cluster"};

  INVARIANT_D((int32_t)_type <= 2);

  return ts[(int32_t)_type];
}

std::string Session::getCmdStr() const {
  std::stringstream ss;
  size_t i = 0;
  if (_args[0] == "applybinlogsv2" || _args[0] == "migratebinlogs") {
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

ServerEntry* Session::getServerEntry() const {
  return _server;
}

SessionCtx* Session::getCtx() const {
  return _ctx.get();
}

LocalSession::LocalSession(std::shared_ptr<ServerEntry> svr)
  : Session(svr, Type::LOCAL) {}

LocalSession::LocalSession(ServerEntry* svr) : Session(svr, Type::LOCAL) {}

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

Status LocalSession::setResponse(const std::string& s) {
  INVARIANT(_respBuf.size() == 0);
  std::copy(s.begin(), s.end(), std::back_inserter(_respBuf));
  return {ErrorCodes::ERR_OK, ""};
}

LocalSessionGuard::LocalSessionGuard(ServerEntry* svr, Session* sess) {
  _sess = std::make_shared<LocalSession>(svr);
  if (sess && sess->getCtx()->authed()) {
    _sess->getCtx()->setAuthed();
  }
  if (svr) {
    svr->addSession(_sess);
  }
}

LocalSessionGuard::~LocalSessionGuard() {
  auto svr = _sess->getServerEntry();
  if (svr) {
    svr->endSession(_sess->id());
  }
}

LocalSession* LocalSessionGuard::getSession() {
  return _sess.get();
}

Status Session::processExtendProtocol() {
  if (!this->getCtx()->isEp()) {
    return {ErrorCodes::ERR_OK, ""};
  }

  // cmd key timestamp version tendisex
  if (_args.size() < 4) {
    return {ErrorCodes::ERR_EXTENDED_PROTOCOL, ""};
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

    return {ErrorCodes::ERR_OK, ""};
  }

  return {ErrorCodes::ERR_EXTENDED_PROTOCOL, ""};
}

}  // namespace tendisplus
