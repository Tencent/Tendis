// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_SERVER_SESSION_H_
#define SRC_TENDISPLUS_SERVER_SESSION_H_

#include <utility>
#include <atomic>
#include <memory>
#include <string>
#include <vector>
#include "asio.hpp"
#include "tendisplus/utils/status.h"

namespace tendisplus {

class ServerEntry;
class SessionCtx;

class Session : public std::enable_shared_from_this<Session> {
 public:
  enum class Type {
    NET = 0,
    LOCAL = 1,
    CLUSTER = 2,
  };

  Session(std::shared_ptr<ServerEntry> svr, Type type);
  Session(ServerEntry* svr, Type type);
  virtual ~Session();
  uint64_t id() const;
  virtual Status setResponse(const std::string& s) = 0;
  // only for unittest
  virtual std::vector<std::string> getResponse() {
    return std::vector<std::string>();
  }
  const std::vector<std::string>& getArgs() const;
  Status processExtendProtocol();
  SessionCtx* getCtx() const;
  ServerEntry* getServerEntry() const;
  std::string getCmdStr() const;
  std::string getSessionCmd();
  uint64_t getCtime() const;

  virtual void start() = 0;
  virtual Status cancel() = 0;
  virtual int getFd() = 0;
  virtual std::string getRemote() const = 0;


  virtual Expected<std::string> getRemoteIp() const {
    return {ErrorCodes::ERR_NETWORK, ""};
  }
  virtual Expected<uint32_t> getRemotePort() const {
    return {ErrorCodes::ERR_NETWORK, ""};
  }

  virtual Expected<std::string> getLocalIp() const {
    return {ErrorCodes::ERR_NETWORK, ""};
  }
  virtual Expected<uint32_t> getLocalPort() const {
    return {ErrorCodes::ERR_NETWORK, ""};
  }

  std::string getName() const;
  void setName(const std::string&);
  Type getType() const {
    return _type;
  }
  std::string getTypeStr() const;
  static Session* getCurSess();
  static void setCurSess(Session* sess);
  int64_t changeExpireTime(const std::string& cmdStr, int64_t millsecs);
  void setInLua(bool val) {
    _inLua = val;
  }
  bool isInLua() {
    return _inLua;
  }

 protected:
  std::vector<std::string> _args;
  ServerEntry* _server;
  std::unique_ptr<SessionCtx> _ctx;
  Type _type;
  uint64_t _timestamp;

 private:
  mutable std::mutex _baseMutex;
  std::string _name;
  const uint64_t _sessId;
  static std::atomic<uint64_t> _idGen;
  static std::atomic<uint64_t> _aliveCnt;
  bool _inLua;
};

class LocalSession : public Session {
 public:
  explicit LocalSession(std::shared_ptr<ServerEntry> svr);
  explicit LocalSession(ServerEntry* svr);
  void start() final;
  Status cancel() final;
  int getFd() final;
  std::string getRemote() const final;
  Status setResponse(const std::string& s) final;
  void setArgs(const std::vector<std::string>& args);
  void setArgs(const std::string& cmd);

 private:
  std::vector<char> _respBuf;
};

class LocalSessionGuard {
 public:
  explicit LocalSessionGuard(ServerEntry* svr, Session* sess = nullptr);
  LocalSession* getSession();
  ~LocalSessionGuard();

 private:
  std::shared_ptr<LocalSession> _sess;
};

}  // namespace tendisplus
#endif  // SRC_TENDISPLUS_SERVER_SESSION_H_
