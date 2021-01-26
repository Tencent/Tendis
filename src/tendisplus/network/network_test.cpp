// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <stdio.h>
#include <iostream>
#include <string>
#include <algorithm>
#include "gtest/gtest.h"
#include "glog/logging.h"
#include "tendisplus/network/network.h"
#include "tendisplus/network/blocking_tcp_client.h"
#include "tendisplus/utils/test_util.h"
#include "tendisplus/utils/time.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/sync_point.h"

namespace tendisplus {

TEST(NetSession, drainReqInvalid) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext);
  auto sess =
    std::make_shared<NoSchedNetSession>(nullptr,
                                        std::move(socket),
                                        1,
                                        false,
                                        std::make_shared<NetworkMatrix>(),
                                        std::make_shared<RequestMatrix>());

  const auto guard =
    MakeGuard([] { SyncPoint::GetInstance()->ClearAllCallBacks(); });
  SyncPoint::GetInstance()->EnableProcessing();
  bool hasCalled = false;
  SyncPoint::GetInstance()->SetCallBack(
    "NetSession::setRspAndClose", [&](void* arg) {
      hasCalled = true;
      std::string* v = static_cast<std::string*>(arg);
      EXPECT_TRUE(v->find("Protocol error") != std::string::npos);
    });
  // const std::string s = "\r\n :1\r\n :2\r\n :3\r\n";
  // std::vector<uint32_t> lens = { 2, 4, 4, 4 };
  std::vector<std::pair<std::string, NetSession::State>> arr = {
    {"\r", NetSession::State::DrainReqNet},
    {"\r\n", NetSession::State::Process},
    {":1\r\n", NetSession::State::Process},
    {"ping\r\n", NetSession::State::Process},
    {"\"\r\n", NetSession::State::Created},  // error
    {"*10\r", NetSession::State::DrainReqNet},
    {"*2\r\n$1\r\n", NetSession::State::DrainReqNet},
    {"*2\r\n$a\r\n", NetSession::State::Created},    // error
    {"*100000000\r\n", NetSession::State::Created},  // error
    {"*-10\r\n", NetSession::State::Process},
    {"*2\r\n:\r\n", NetSession::State::Created},  // error, should be $
  };
  sess->_queryBuf.reserve(128);
  int i = 0;
  for (auto s : arr) {
    hasCalled = false;
    sess->_queryBuf.clear();
    sess->_queryBufPos = 0;
    sess->resetMultiBulkCtx();
    std::copy(
      s.first.begin(), s.first.end(), std::back_inserter(sess->_queryBuf));
#ifdef _WIN32
    sess->_queryBuf.resize(s.first.size() + 1);
#endif
    sess->setState(NetSession::State::DrainReqNet);
    sess->drainReqCallback(std::error_code(), s.first.size());
    if (s.second == NetSession::State::Created) {
      EXPECT_TRUE(sess->_closeAfterRsp && hasCalled);
    } else {
      EXPECT_EQ(sess->_state, s.second);
    }
    i++;
  }
}

TEST(NetSession, Completed) {
  std::string s = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r";
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext);
  auto sess =
    std::make_shared<NoSchedNetSession>(nullptr,
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
  explicit session(asio::ip::tcp::socket socket) : _socket(std::move(socket)) {}

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
    asio::async_write(_socket,
                      asio::buffer(_data, length),
                      [this, self](std::error_code ec, size_t /*length*/) {
                        if (!ec) {
                          do_read();
                        }
                      });
  }

  asio::ip::tcp::socket _socket;
  enum { max_length = 1024 };
  char _data[max_length];
};

class server {
 public:
  server(asio::io_context& io_context, uint16_t port) {
    try {
      _acceptor = new asio::ip::tcp::acceptor(
        io_context, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port));
    } catch (exception e) {
#if defined(TENDIS_DEBUG) && !defined(_WIN32)
      printPortRunningInfo(port);
#endif
      LOG(FATAL) << "_acceptor async_accept catch error:" << e.what();
    }
    _acceptor->set_option(asio::ip::tcp::acceptor::reuse_address(true));
    do_accept();
  }
  ~server() {
    if (_acceptor) {
      delete _acceptor;
    }
  }

 private:
  void do_accept() {
    _acceptor->async_accept(
      [this](std::error_code ec, asio::ip::tcp::socket socket) {
        if (!ec) {
          std::make_shared<session>(std::move(socket))->start();
        }
        do_accept();
      });
  }
  asio::ip::tcp::acceptor* _acceptor;
};

TEST(BlockingTcpClient, Common) {
  auto ioCtx = std::make_shared<asio::io_context>();
  auto ioCtx1 = std::make_shared<asio::io_context>();
  uint32_t port = 54001;
  /*
  BlockingTcpClient cli(ioCtx, 128);
  Status s = cli.connect("127.0.0.1", port, std::chrono::seconds(1));
  EXPECT_FALSE(s.ok());
  s = cli.connect("127.0.0.1", port, std::chrono::seconds(1));
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(s.toString(), "already inited sock");
  */

  server svr(*ioCtx, port);

  std::thread thd([&ioCtx] {
    asio::io_context::work work(*ioCtx);
    ioCtx->run();
  });
  std::thread thd1([&ioCtx1] {
    asio::io_context::work work(*ioCtx1);
    ioCtx1->run();
  });

  auto cli1 = std::make_shared<BlockingTcpClient>(ioCtx1, 128, 1024 * 1024, 10);
  Status s = cli1->connect("127.0.0.1", port, std::chrono::seconds(1));
  EXPECT_TRUE(s.ok());

  s = cli1->connect("127.0.0.1", port, std::chrono::seconds(1));
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(s.toString(), "-ERR:1,msg:already inited sock\r\n");
  s = cli1->writeLine("hello world\r\n hello world1\r\n trailing");
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

  s = cli1->writeLine("hello world");
  // timeout

  exps = cli1->readLine(std::chrono::seconds(1));
  EXPECT_FALSE(exps.ok()) << exps.value();

  // more than max buf size
  auto cli2 = std::make_shared<BlockingTcpClient>(ioCtx1, 4, 1024 * 1024, 10);
  s = cli2->connect("127.0.0.1", port, std::chrono::seconds(1));
  EXPECT_TRUE(s.ok());
  s = cli2->writeLine("hello world");
  exps = cli2->readLine(std::chrono::seconds(3));
  EXPECT_FALSE(exps.ok());
  ioCtx->stop();
  ioCtx1->stop();
  thd.join();
  thd1.join();
}

class session2 : public std::enable_shared_from_this<session2> {
 public:
  explicit session2(asio::ip::tcp::socket socket)
    : _socket(std::move(socket)) {}

  void start() {
    do_read();
  }

 private:
  void do_read() {
    auto self(shared_from_this());
    using namespace std::chrono_literals;  // NOLINT
    _socket.async_read_some(asio::buffer(_data, max_length),
                            [this, self](std::error_code ec, size_t length) {
                              if (ec) {
                                // LOG(ERROR) << ec.message();
                              }
                              do_read();
                            });
  }

  asio::ip::tcp::socket _socket;
  enum { max_length = 1024 };
  char _data[max_length];
};


class server2 {
 public:
  server2(asio::io_context& io_context, uint16_t port) {
    try {
      _acceptor = new asio::ip::tcp::acceptor(
        io_context, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port));
    } catch (exception e) {
#if defined(TENDIS_DEBUG) && !defined(_WIN32)
      printPortRunningInfo(port);
#endif
      LOG(FATAL) << "_acceptor async_accept catch error:" << e.what();
    }

    _acceptor->set_option(asio::ip::tcp::acceptor::reuse_address(true));
    do_accept();
  }

  ~server2() {
    if (_acceptor) {
      delete _acceptor;
    }
  }

 private:
  void do_accept() {
    _acceptor->async_accept(
      [this](std::error_code ec, asio::ip::tcp::socket socket) {
        if (!ec) {
          std::make_shared<session2>(std::move(socket))->start();
        }
        do_accept();
      });
  }
  asio::ip::tcp::acceptor* _acceptor;
};

void rateLimit(uint64_t ratelimit,
               std::shared_ptr<asio::io_context> ioCtx,
               uint32_t port) {
  uint32_t total = ratelimit * 10;
  auto cli1 =
    std::make_shared<BlockingTcpClient>(ioCtx, 128, 1024 * 1024, 10, ratelimit);
  Status s = cli1->connect("127.0.0.1", port, std::chrono::seconds(1));
  EXPECT_TRUE(s.ok());

  uint32_t count = 0;
  auto now = msSinceEpoch();
  size_t buffer_size = 1024 * 16;
  if (buffer_size > total) {
    buffer_size = ratelimit;
  }

  std::string str;
  str.assign(buffer_size, 'a');
  while (count < total) {
    s = cli1->writeLine(str);
    EXPECT_TRUE(s.ok());
    count += str.size();
  }

  uint32_t use_time = (msSinceEpoch() - now) / 1000;
  LOG(INFO) << "rate:" << ratelimit << " use " << use_time << " seconds";
  EXPECT_TRUE(use_time >= total / ratelimit &&
              use_time < total * 1.3 / ratelimit);
}

TEST(BlockingTcpClient, RateLimit) {
  auto ioCtx = std::make_shared<asio::io_context>();
  auto ioCtx1 = std::make_shared<asio::io_context>();
  uint32_t port = 54011;

  server2 svr(*ioCtx, port);

  std::thread thd([&ioCtx] {
    asio::io_context::work work(*ioCtx);
    ioCtx->run();
  });
  std::thread thd1([&ioCtx1] {
    asio::io_context::work work(*ioCtx1);
    ioCtx1->run();
  });

  rateLimit(1024, ioCtx1, port);
  rateLimit(102400, ioCtx1, port);
  rateLimit(1024000, ioCtx1, port);
  rateLimit(10240000, ioCtx1, port);
  // rateLimit(102400000, ioCtx1, port);

  ioCtx->stop();
  ioCtx1->stop();
  thd.join();
  thd1.join();
}

}  // namespace tendisplus
