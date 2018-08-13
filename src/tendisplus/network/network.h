#ifndef SRC_TENDISPLUS_NETWORK_NETWORK_H_
#define SRC_TENDISPLUS_NETWORK_NETWORK_H_

#include <utility>
#include <atomic>
#include <memory>
#include <string>
#include <vector>
#include "asio.hpp"
#include "tendisplus/utils/status.h"
#include "gtest/gtest_prod.h"

namespace tendisplus {
class NetworkAsio: public std::enable_shared_from_this<NetworkAsio> {
 public:
    NetworkAsio();
    NetworkAsio(const NetworkAsio&) = delete;
    NetworkAsio(NetworkAsio&&) = delete;
    Status prepare(const std::string& ip, const uint16_t port);
    Status run();
    void stop();
 private:
    void doAccept();
    std::unique_ptr<asio::io_context> _acceptCtx;
    std::unique_ptr<asio::io_context> _workCtx;
    std::unique_ptr<asio::ip::tcp::acceptor> _acceptor;
    std::unique_ptr<std::thread> _acceptThd;
    std::atomic<bool> _isRunning;
};

// represent a ingress tcp-connection
class NetSession {
 public:
    NetSession(std::shared_ptr<NetworkAsio> net, asio::ip::tcp::socket sock,
        bool initSock);
    NetSession(const NetSession&) = delete;
    NetSession(NetSession&&) = delete;
    std::string getRemoteRepr() const;
    std::string getLocalRepr() const;
    void start();
    void drainReq();
    void forwardState();
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
    virtual void setState(State s);
    virtual void replyAndClose(const std::string&);
    void shiftQueryBuf(ssize_t start, ssize_t end);
    void drainReqCallback(const std::error_code& ec, size_t actualLen);
    virtual void schedule();
    bool _close_after_rsp;
    std::shared_ptr<NetworkAsio> _netIface;
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
