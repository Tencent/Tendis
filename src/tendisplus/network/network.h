// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_NETWORK_NETWORK_H_
#define SRC_TENDISPLUS_NETWORK_NETWORK_H_

#include <unistd.h>

#include <utility>
#include <atomic>
#include <memory>
#include <string>
#include <vector>
#include <list>

#include "asio.hpp"
#include "gtest/gtest.h"

#include "tendisplus/network/session_ctx.h"
#include "tendisplus/network/blocking_tcp_client.h"
#include "tendisplus/server/session.h"
#include "tendisplus/server/server_params.h"
#include "tendisplus/utils/status.h"
#include "tendisplus/utils/atomic_utility.h"

namespace tendisplus {

void printShellResult(std::string cmd);
void printPortRunningInfo(uint32_t port);

class ServerEntry;

enum class RedisReqMode : std::uint8_t {
  REDIS_REQ_UNKNOWN = 0,
  REDIS_REQ_INLINE = 1,
  REDIS_REQ_MULTIBULK = 2,
};

class NetworkMatrix {
 public:
  Atom<uint64_t> stickyPackets{0};
  Atom<uint64_t> connCreated{0};
  Atom<uint64_t> connReleased{0};
  Atom<uint64_t> invalidPackets{0};
  NetworkMatrix operator-(const NetworkMatrix& right);
  std::string toString() const;
  void reset();
};

class RequestMatrix {
 public:
  Atom<uint64_t> processed{0};       // number of commands
  Atom<uint64_t> processCost{0};     // time cost for commands (ns)
  Atom<uint64_t> sendPacketCost{0};  //
  RequestMatrix operator-(const RequestMatrix& right);
  std::string toString() const;
  void reset();
};

class ClusterSession;

class NetworkAsio {
 public:
  NetworkAsio(std::shared_ptr<ServerEntry> server,
              std::shared_ptr<NetworkMatrix> netMatrix,
              std::shared_ptr<RequestMatrix> reqMatrix,
              std::shared_ptr<ServerParams> cfg,
              const std::string& name = "tx-io");
  NetworkAsio(const NetworkAsio&) = delete;
  NetworkAsio(NetworkAsio&&) = delete;

  // blocking client related apis
  std::unique_ptr<BlockingTcpClient> createBlockingClient(
    size_t readBuf, uint64_t rateLimit = 0);
  std::unique_ptr<BlockingTcpClient> createBlockingClient(
    asio::ip::tcp::socket,
    size_t readBuf,
    uint64_t rateLimit = 0,
    uint32_t netBatchTimeoutSec = 0);
  Expected<uint64_t> client2Session(std::shared_ptr<BlockingTcpClient>,
                                    bool migrateOnly = false);
  Expected<std::shared_ptr<ClusterSession>> client2ClusterSession(
    std::shared_ptr<BlockingTcpClient> c);

  Status prepare(const std::string& ip,
                 const uint16_t port,
                 uint32_t netIoThreadNum);

  Status run(bool forGossip = false);
  void stop();
  std::string getIp() {
    return _ip;
  }
  uint16_t getPort() {
    return _port;
  }
#ifdef _WIN32
  void releaseForWin();
#endif

 private:
  Status startThread();
  // we envolve a single-thread accept, mutex is not needed.
  template <typename T>
  void doAccept();
  std::shared_ptr<asio::io_context> getRwCtx();
  std::shared_ptr<asio::io_context> getRwCtx(asio::ip::tcp::socket& socket);

  std::atomic<uint64_t> _connCreated;
  std::shared_ptr<ServerEntry> _server;
  std::unique_ptr<asio::io_context> _acceptCtx;
  std::vector<std::shared_ptr<asio::io_context>> _rwCtxList;
  std::unique_ptr<asio::ip::tcp::acceptor> _acceptor;
  std::unique_ptr<std::thread> _acceptThd;
  std::vector<std::thread> _rwThreads;
  std::atomic<bool> _isRunning;
  std::shared_ptr<NetworkMatrix> _netMatrix;
  std::shared_ptr<RequestMatrix> _reqMatrix;
  std::string _ip;
  uint16_t _port;
  uint32_t _netIoThreadNum;
  std::shared_ptr<ServerParams> _cfg;
  std::string _name;
};

struct SendBuffer {
  std::vector<char> buffer;
  bool closeAfterThis;
};

// represent a ingress tcp-connection
class NetSession : public Session {
 public:
  NetSession(std::shared_ptr<ServerEntry> server,
             asio::ip::tcp::socket sock,
             uint64_t connid,
             bool initSock,
             std::shared_ptr<NetworkMatrix> netMatrix,
             std::shared_ptr<RequestMatrix> reqMatrix,
             Session::Type type = Session::Type::NET);
  NetSession(const NetSession&) = delete;
  NetSession(NetSession&&) = delete;
  virtual ~NetSession() = default;
  virtual std::string getRemoteRepr() const;
  virtual std::string getLocalRepr() const;
  asio::ip::tcp::socket borrowConn();
  virtual Status setResponse(const std::string& s);
  void setCloseAfterRsp();
  virtual void start();
  virtual Status cancel();
  virtual std::string getRemote() const;
  virtual int getFd();

  virtual Expected<std::string> getRemoteIp() const;
  virtual Expected<uint32_t> getRemotePort() const;

  virtual Expected<std::string> getLocalIp() const;
  virtual Expected<uint32_t> getLocalPort() const;

  // close session, and the socket(by raii)
  virtual void endSession();

  const std::vector<std::string>& getArgs() const;
  void setArgs(const std::vector<std::string>&);
  void setIoCtxId(uint32_t id) {
    _ioCtxId = id;
  }
  enum class State {
    Created,
    DrainReqNet,
    DrainReqBuf,
    Process,
  };

 protected:
  // schedule related functions
  virtual void schedule();
  virtual void stepState();
  virtual void setState(State s);

  // read data from socket
  virtual void drainReqNet();
  virtual void drainReqBuf();
  virtual void drainReqCallback(const std::error_code& ec, size_t actualLen);

  // send data to tcpbuff
  virtual void drainRsp(std::shared_ptr<SendBuffer>);
  virtual void drainRspCallback(const std::error_code& ec,
                                size_t actualLen,
                                std::shared_ptr<SendBuffer> buf);

  // handle msg parsed from drainReqCallback
  virtual void processReq();
  // cleanup state for next request
  virtual void resetMultiBulkCtx();

 private:
  FRIEND_TEST(NetSession, drainReqInvalid);
  FRIEND_TEST(NetSession, Completed);
  FRIEND_TEST(Command, common);
  friend class NoSchedNetSession;

  void processMultibulkBuffer();
  void processInlineBuffer();

  // network is ok, but client's msg is not ok, reply and close
  void setRspAndClose(const std::string&);

  // utils to shift parsed partial params from _queryBuf
  void shiftQueryBuf(ssize_t start, ssize_t end);

 protected:
  uint64_t _connId;
  bool _closeAfterRsp;
  std::atomic<State> _state;
  asio::ip::tcp::socket _sock;
  std::vector<char> _queryBuf;
  ssize_t _queryBufPos;

  // contexts for RedisReqMode::REDIS_REQ_MULTIBULK
  RedisReqMode _reqType;
  int64_t _multibulklen;
  int64_t _bulkLen;

  // _mutex protects _isSendRunning, _isEnded, _sendBuffer
  // other variables will never be visited in send-threads.
  std::mutex _mutex;
  bool _isSendRunning;
  bool _isEnded;
  bool _first;
  std::list<std::shared_ptr<SendBuffer>> _sendBuffer;

  std::shared_ptr<NetworkMatrix> _netMatrix;
  std::shared_ptr<RequestMatrix> _reqMatrix;
  uint32_t _ioCtxId = UINT32_MAX;
};

}  // namespace tendisplus
#endif  // SRC_TENDISPLUS_NETWORK_NETWORK_H_
