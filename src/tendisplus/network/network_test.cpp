#include <iostream>
#include <string>
#include <algorithm>
#include "gtest/gtest.h"
#include "glog/logging.h"
#include "tendisplus/network/network.h"
#include "tendisplus/network/blocking_tcp_client.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/scopeguard.h"

namespace tendisplus {

class NoSchedNetSession: public NetSession {
 public:
    NoSchedNetSession(std::shared_ptr<ServerEntry> server,
        asio::ip::tcp::socket sock, uint64_t connid, bool initSock,
        std::shared_ptr<NetworkMatrix> netMatrix,
        std::shared_ptr<RequestMatrix> reqMatrix)
            :NetSession(server, std::move(sock), connid,
                        initSock, netMatrix, reqMatrix) {
    }

 protected:
    virtual void schedule() {
    }
};

TEST(NetSession, drainReqInvalid) {
    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext);
    auto sess = std::make_shared<NoSchedNetSession>(
           nullptr,
           std::move(socket),
           1,
           false,
           std::make_shared<NetworkMatrix>(),
           std::make_shared<RequestMatrix>());

    const auto guard = MakeGuard([] { 
        SyncPoint::GetInstance()->ClearAllCallBacks(); 
    });  
    SyncPoint::GetInstance()->EnableProcessing(); 
    bool hasCalled = false;
    SyncPoint::GetInstance()->SetCallBack( 
        "NetSession::drainRsp", [&](void* arg) { 
            hasCalled = true;
            SendBuffer* v = static_cast<SendBuffer*>(arg);
            EXPECT_EQ(std::string(v->buffer.data(), v->buffer.size()),
                "-ERR Protocol error: unbalanced quotes in request\r\n");
        });
    sess->setState(NetSession::State::DrainReqNet);
    const std::string s = "\r\n :1\r\n :2\r\n :3\r\n";
    std::copy(s.begin(), s.end(), std::back_inserter(sess->_queryBuf));
    sess->drainReqCallback(std::error_code(), s.size());
    EXPECT_EQ(sess->_closeAfterRsp, true);
    EXPECT_TRUE(hasCalled);
}

TEST(NetSession, Completed) {
    std::string s = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r";
    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext);
    auto sess = std::make_shared<NoSchedNetSession>(
           nullptr,
           std::move(socket),
           1,
           false,
           std::make_shared<NetworkMatrix>(),
           std::make_shared<RequestMatrix>());

    sess->setState(NetSession::State::DrainReqNet);
    sess->_queryBuf.resize(128, 0);
    for (auto& c : s) {
        sess->_queryBuf[sess->_queryBufPos] = c;
        sess->drainReqCallback(std::error_code(), 1);
        EXPECT_EQ(sess->_state.load(), NetSession::State::DrainReqNet);
        EXPECT_EQ(sess->_closeAfterRsp, false);
    }
    sess->_queryBuf[sess->_queryBufPos] = '\n';
    sess->drainReqCallback(std::error_code(), 1);
    EXPECT_EQ(sess->_state.load(), NetSession::State::Process);
    EXPECT_EQ(sess->_closeAfterRsp, false);
    EXPECT_EQ(sess->_args.size(), size_t(2));
    EXPECT_EQ(sess->_args[0], "foo");
    EXPECT_EQ(sess->_args[1], "bar");

    sess->resetMultiBulkCtx();
    s = "FULLSYNC 1\r";
    sess->setState(NetSession::State::DrainReqNet);
    sess->_queryBuf.resize(128, 0);
    for (auto& c : s) {
        sess->_queryBuf[sess->_queryBufPos] = c;
        sess->drainReqCallback(std::error_code(), 1);
        EXPECT_EQ(sess->_state.load(), NetSession::State::DrainReqNet);
        EXPECT_EQ(sess->_closeAfterRsp, false);
    }
    sess->_queryBuf[sess->_queryBufPos] = '\n';
    sess->drainReqCallback(std::error_code(), 1);
    EXPECT_EQ(sess->_state.load(), NetSession::State::Process);
    EXPECT_EQ(sess->_closeAfterRsp, false);
    EXPECT_EQ(sess->_args.size(), size_t(2));
    EXPECT_EQ(sess->_args[0], "FULLSYNC");
    EXPECT_EQ(sess->_args[1], "1");
}


class session : public std::enable_shared_from_this<session> {
 public:
    explicit session(asio::ip::tcp::socket socket)
        :_socket(std::move(socket)) {
    }

    void start() {
        do_read();
    }

 private:
    void do_read() {
        auto self(shared_from_this());
        using namespace std::chrono_literals;  // NOLINT
        _socket.async_read_some(asio::buffer(_data, max_length),
            [this, self](std::error_code ec, size_t length) {
                  if (!ec) {
                    std::this_thread::sleep_for(2s);
                    do_write(length);
                  }
            });
    }

    void do_write(size_t length) {
        auto self(shared_from_this());
        asio::async_write(_socket, asio::buffer(_data, length),
            [this, self](std::error_code ec, size_t /*length*/) {
                if (!ec) {
                    do_read();
                }
            });
    }

    asio::ip::tcp::socket _socket;
    enum {
        max_length = 1024
    };
    char _data[max_length];
};


class server {
 public:
    server(asio::io_context& io_context, uint16_t port)  // NOLINT
            :_acceptor(
                io_context,
                asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port)) {
        _acceptor.set_option(asio::ip::tcp::acceptor::reuse_address(true));
        do_accept();
    }

 private:
    void do_accept() {
        _acceptor.async_accept(
            [this](std::error_code ec, asio::ip::tcp::socket socket) {
                if (!ec) {
                    std::make_shared<session>(std::move(socket))->start();
                }
                do_accept();
            });
    }
    asio::ip::tcp::acceptor _acceptor;
};

TEST(BlockingTcpClient, Common) {
    auto ioCtx  = std::make_shared<asio::io_context>();
    auto ioCtx1 = std::make_shared<asio::io_context>();
    /*
    BlockingTcpClient cli(ioCtx, 128);
    Status s = cli.connect("127.0.0.1", 54321, std::chrono::seconds(1));
    EXPECT_FALSE(s.ok());
    s = cli.connect("127.0.0.1", 54321, std::chrono::seconds(1));
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.toString(), "already inited sock");
    */
    server svr(*ioCtx, 54321);

    std::thread thd([&ioCtx] {
        asio::io_context::work work(*ioCtx);
        ioCtx->run();
    });
    std::thread thd1([&ioCtx1] {
        asio::io_context::work work(*ioCtx1);
        ioCtx1->run();
    });

    auto cli1 = std::make_shared<BlockingTcpClient>(ioCtx1, 128);
    Status s = cli1->connect("127.0.0.1", 54321, std::chrono::seconds(1));
    EXPECT_TRUE(s.ok());

    s = cli1->connect("127.0.0.1", 54321, std::chrono::seconds(1));
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.toString(), "ERR:1,msg:already inited sock");
    s = cli1->writeLine("hello world\r\n hello world1\r\n trailing",
        std::chrono::seconds(1));
    EXPECT_TRUE(s.ok());
    Expected<std::string> exps = cli1->readLine(std::chrono::seconds(3));
    EXPECT_TRUE(exps.ok());
    EXPECT_EQ(exps.value(), "hello world");
    exps = cli1->readLine(std::chrono::seconds(3));
    EXPECT_TRUE(exps.ok());
    EXPECT_EQ(exps.value(), " hello world1");

    EXPECT_EQ(cli1->getReadBufSize(), std::string(" trailing\r\n").size());
    exps = cli1->read(1, std::chrono::seconds(1));
    EXPECT_TRUE(exps.ok()) << exps.status().toString();
    EXPECT_EQ(exps.value()[0], ' ');
    EXPECT_EQ(cli1->getReadBufSize(), std::string("trailing\r\n").size());

    exps = cli1->read(10, std::chrono::seconds(1));
    EXPECT_TRUE(exps.ok()) << exps.status().toString();
    EXPECT_EQ(exps.value(), "trailing\r\n");
    EXPECT_EQ(cli1->getReadBufSize(), size_t(0));

    s = cli1->writeLine("hello world", std::chrono::seconds(1));
    // timeout

    exps = cli1->readLine(std::chrono::seconds(1));
    EXPECT_FALSE(exps.ok()) << exps.value();

    // more than max buf size
    auto cli2 = std::make_shared<BlockingTcpClient>(ioCtx1, 4);
    s = cli2->connect("127.0.0.1", 54321, std::chrono::seconds(1));
    EXPECT_TRUE(s.ok());
    s = cli2->writeLine("hello world", std::chrono::seconds(1));
    exps = cli2->readLine(std::chrono::seconds(3));
    EXPECT_FALSE(exps.ok());
    ioCtx->stop();
    ioCtx1->stop();
    thd.join();
    thd1.join();
}

}  // namespace tendisplus
