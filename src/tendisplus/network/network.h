#ifndef SRC_TENDISPLUS_NETWORK_NETWORK_H_
#define SRC_TENDISPLUS_NETWORK_NETWORK_H_

#include <utility>
#include <atomic>
#include <memory>
#include <string>
#include <vector>
#include "asio.hpp"
#include "tendisplus/server/server_entry.h"
#include "tendisplus/utils/status.h"
#include "gtest/gtest.h"

namespace tendisplus {
class ServerEntry;
class NetworkAsio: public std::enable_shared_from_this<NetworkAsio> {
 public:
    NetworkAsio();
    NetworkAsio(const NetworkAsio&) = delete;
    NetworkAsio(NetworkAsio&&) = delete;
    Status prepare(const std::string& ip, const uint16_t port);
    Status run();
    void stop();
 private:
    // we envolve a single-thread accept, mutex is not needed.
    void doAccept();
    uint64_t _connCreated;
    std::shared_ptr<ServerEntry> _server;
    std::unique_ptr<asio::io_context> _acceptCtx;
    std::unique_ptr<asio::ip::tcp::acceptor> _acceptor;
    std::unique_ptr<std::thread> _acceptThd;
    std::atomic<bool> _isRunning;
};

// represent a ingress tcp-connection
class NetSession {
 public:
    NetSession(std::shared_ptr<ServerEntry> server, asio::ip::tcp::socket sock,
        uint64_t connid, bool initSock);
    NetSession(const NetSession&) = delete;
    NetSession(NetSession&&) = delete;
    std::string getRemoteRepr() const;
    std::string getLocalRepr() const;
    uint64_t getConnId() const;
    void start();
    const std::vector<std::string>& getArgs() const;
    void setOkRsp();

    // normal clients
    // Created -> [DrainReq]+ -> Process -> DrainRsp -> [DrainReq]+
    // clients with bad input
    // Created -> [DrainReq]+ -> DrainRsp(with _closeAfterRsp set) -> End
    enum class State {
        Created,
        DrainReq,
        Process,
        DrainRsp,
        End,
    };

 private:
    FRIEND_TEST(NetSession, drainReqInvalid);
    FRIEND_TEST(NetSession, Completed);

    // read data from socket
    void drainReq();
    void drainReqCallback(const std::error_code& ec, size_t actualLen);

    // send data to tcpbuff
    void drainRsp();
    void drainRspCallback(const std::error_code& ec, size_t actualLen);

    // close session, and the socket(by raii)
    void endSession();

    // handle msg parsed from drainReqCallback
    void processReq();

    // schedule related functions
    void stepState();
    void setState(State s);
    void schedule();

    // network is ok, but client's msg is not ok, reply and close
    void setRspAndClose(const std::string&);

    // utils to shift parsed partial params from _queryBuf
    void shiftQueryBuf(ssize_t start, ssize_t end);

    uint64_t _connId;
    bool _closeAfterRsp;
    std::shared_ptr<ServerEntry> _server;
    std::atomic<State> _state;
    asio::ip::tcp::socket _sock;
    std::vector<char> _queryBuf;
    size_t _queryBufPos;
    int64_t _multibulklen;
    int64_t _bulkLen;
    std::vector<std::string> _args;
    std::vector<char> _respBuf;
};

}  // namespace tendisplus
#endif  // SRC_TENDISPLUS_NETWORK_NETWORK_H_
