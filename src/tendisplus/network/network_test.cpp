#include <iostream>
#include <string>
#include <algorithm>
#include "gtest/gtest.h"
#include "glog/logging.h"
#include "tendisplus/network/network.h"

namespace tendisplus {

TEST(NetSession, drainReqInvalid) {
    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext);
    NetSession sess(nullptr, std::move(socket),
        1, false, std::make_shared<NetworkMatrix>());
    sess.setState(NetSession::State::DrainReq);
    const std::string s = "\r\n :1\r\n :2\r\n :3\r\n";
    std::copy(s.begin(), s.end(), std::back_inserter(sess._queryBuf));
    sess.drainReqCallback(std::error_code(), s.size());
    EXPECT_EQ(sess._state.load(), NetSession::State::DrainRsp);
    EXPECT_EQ(sess._closeAfterRsp, true);
    EXPECT_EQ(std::string(sess._respBuf.data(), sess._respBuf.size()),
        "-ERR Protocol error: only support multilen proto\r\n");
}

TEST(NetSession, Completed) {
    std::string s = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r";
    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext);
    NetSession sess(nullptr, std::move(socket),
        1, false, std::make_shared<NetworkMatrix>());
    sess.setState(NetSession::State::DrainReq);
    sess._queryBuf.resize(128, 0);
    for (auto& c : s) {
        sess._queryBuf[sess._queryBufPos] = c;
        sess.drainReqCallback(std::error_code(), 1);
        EXPECT_EQ(sess._state.load(), NetSession::State::DrainReq);
        EXPECT_EQ(sess._closeAfterRsp, false);
    }
    sess._queryBuf.emplace_back('\n');
    sess.drainReqCallback(std::error_code(), 1);
    EXPECT_EQ(sess._state.load(), NetSession::State::Process);
    EXPECT_EQ(sess._closeAfterRsp, false);
    EXPECT_EQ(sess._args.size(), size_t(2));
    EXPECT_EQ(sess._args[0], "foo");
    EXPECT_EQ(sess._args[1], "bar");
}

}  // namespace tendisplus
