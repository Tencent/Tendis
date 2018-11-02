#include <fstream>
#include <utility>
#include <memory>
#include <vector>
#include <limits>
#include <algorithm>
#include "gtest/gtest.h"
#include "tendisplus/utils/status.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/utils/portable.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/storage/rocks/rocks_kvstore.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/server/server_params.h"
#include "tendisplus/server/server_entry.h"

namespace tendisplus {

static std::shared_ptr<ServerParams> genParams() {
    const auto guard = MakeGuard([] {
        remove("a.cfg");
    });
    std::ofstream myfile;
    myfile.open("a.cfg");
    myfile << "bind 127.0.0.1\n";
    myfile << "port 8903\n";
    myfile << "loglevel debug\n";
    myfile << "logdir ./log\n";
    myfile << "storage rocks\n";
    myfile << "dir ./db\n";
    myfile << "rocks.blockcachemb 4096\n";
    myfile.close();
    auto cfg = std::make_shared<ServerParams>();
    auto s = cfg->parseFile("a.cfg");
    EXPECT_EQ(s.ok(), true) << s.toString();
    return cfg;
}

void testList(std::shared_ptr<ServerEntry> svr) {
    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
    NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

    sess.setArgs({"lindex", "a", std::to_string(0)});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtNull());

    for (uint32_t i = 0; i < 10000; i++) {
        sess.setArgs({"lpush", "a", std::to_string(2*i)});
        auto expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtLongLong(i+1));

        sess.setArgs({"lindex", "a", std::to_string(i)});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtBulk(std::to_string(0)));

        sess.setArgs({"llen", "a"});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtLongLong(i+1));
    }
}

void testHash2(std::shared_ptr<ServerEntry> svr) {
    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
    NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

    int sum = 0;
    for (int i = 0; i < 1000; ++i) {
        int cur = rand()%100-50;  // NOLINT(runtime/threadsafe_fn)
        sum += cur;
        sess.setArgs({"hincrby", "testkey", "testsubkey", std::to_string(cur)});
        auto expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtLongLong(sum));

        sess.setArgs({"hget", "testkey", "testsubkey"});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtBulk(std::to_string(sum)));

        sess.setArgs({"hlen", "testkey"});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtLongLong(1));
    }

    int64_t delta = 0;
    if (sum > 0) {
        delta = std::numeric_limits<int64_t>::max();
    } else {
        delta = std::numeric_limits<int64_t>::min();
    }
    sess.setArgs({"hincrby", "testkey", "testsubkey", std::to_string(delta)});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_FALSE(expect.ok());
    EXPECT_EQ(expect.status().code(), ErrorCodes::ERR_OVERFLOW);

    const long double pi = 3.14159265358979323846L;
    long double floatSum = sum + pi;
    sess.setArgs({"hincrbyfloat", "testkey", "testsubkey",
                  redis_port::ldtos(pi)});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    std::string result = redis_port::ldtos(floatSum);
    EXPECT_EQ(Command::fmtBulk(result), expect.value());
}

void testHash1(std::shared_ptr<ServerEntry> svr) {
    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
    NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);
    for (uint32_t i = 0; i < 10000; i++) {
        sess.setArgs({"hset", "a", std::to_string(i), std::to_string(i)});
        auto expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
    }
    for (uint32_t i = 0; i < 10000; i++) {
        sess.setArgs({"hget", "a", std::to_string(i)});
        auto expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtBulk(std::to_string(i)));

        sess.setArgs({"hexists", "a", std::to_string(i)});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtOne());
    }
    std::vector<std::string> args;
    args.push_back("hdel");
    args.push_back("a");
    for (uint32_t i = 0; i < 10000; i++) {
        args.push_back(std::to_string(2*i));
    }
    sess.setArgs(args);
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(5000));

    sess.setArgs({"hlen", "a"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(5000));

    for (uint32_t i = 0; i < 5000; i++) {
        sess.setArgs({"hget", "a", std::to_string(i)});
        auto expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        if (i % 2 == 1) {
            EXPECT_EQ(expect.value(), Command::fmtBulk(std::to_string(i)));
        } else {
            EXPECT_EQ(expect.value(), Command::fmtNull());
        }
        sess.setArgs({"hexists", "a", std::to_string(i)});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        if (i % 2 == 1) {
            EXPECT_EQ(expect.value(), Command::fmtOne());
        } else {
            EXPECT_EQ(expect.value(), Command::fmtZero());
        }
    }

    sess.setArgs({"hgetall", "a"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    std::stringstream ss;
    Command::fmtMultiBulkLen(ss, 10000);
    std::vector<std::string> vals;
    for (uint32_t i = 0; i < 5000; ++i) {
        vals.push_back(std::to_string(2*i+1));
        vals.push_back(std::to_string(2*i+1));
    }
    std::sort(vals.begin(), vals.end());
    for (const auto& v : vals) {
        Command::fmtBulk(ss, v);
    }
    EXPECT_EQ(ss.str(), expect.value());

    // hsetnx related
    for (uint32_t i = 0; i < 10000; i++) {
        sess.setArgs({"hsetnx", "a", std::to_string(i), std::to_string(0)});
        auto expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        if (i % 2 == 0) {
            EXPECT_EQ(expect.value(), Command::fmtOne());
        } else {
            EXPECT_EQ(expect.value(), Command::fmtZero());
        }
    }
    for (uint32_t i = 0; i < 10000; i++) {
        sess.setArgs({"hget", "a", std::to_string(i)});
        auto expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        if (i % 2 == 0) {
            EXPECT_EQ(expect.value(), Command::fmtBulk(std::to_string(0)));
        } else {
            EXPECT_EQ(expect.value(), Command::fmtBulk(std::to_string(i)));
        }
    }
}

void testZset2(std::shared_ptr<ServerEntry> svr) {
    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
    NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

    std::vector<uint64_t> keys;
    for (uint32_t i = 0; i < 100; i++) {
        keys.push_back(i);
    }
    std::random_shuffle(keys.begin(), keys.end());
    for (uint32_t i = 0; i < 100; i++) {
        sess.setArgs({"zadd",
                      "tzk2",
                      std::to_string(keys[i]),
                      std::to_string(keys[i])});
        auto expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        sess.setArgs({"zcount", "tzk2", "-inf", "+inf"});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtLongLong(i+1));
    }
    for (uint32_t i = 0; i < 100; i++) {
        for (uint32_t j = 0; j < 100; j++) {
            sess.setArgs({"zcount",
                          "tzk2",
                          std::to_string(i),
                          std::to_string(j)});
            auto expect = Command::runSessionCmd(&sess);
            EXPECT_TRUE(expect.ok());
            if (i > j) {
                EXPECT_EQ(expect.value(), Command::fmtZero());
            } else {
                EXPECT_EQ(expect.value(), Command::fmtLongLong(j-i+1))
                    << i << ' ' << j;
            }
        }
    }
}

void testZset3(std::shared_ptr<ServerEntry> svr) {
    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
    NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

    // NOTE(deyukong): zlexcount has undefined behavior when scores
    // are not all the same.

    // cases from redis.io
    sess.setArgs({"zadd",
                  "tzk3",
                  "0", "a", "0", "b", "0", "c", "0", "d", "0", "e"});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(5));
    sess.setArgs({"zadd",
                  "tzk3",
                  "0", "f", "0", "g"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(2));

    sess.setArgs({"zlexcount", "tzk3", "[b", "[f"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(5));
}

void testZset(std::shared_ptr<ServerEntry> svr) {
    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
    NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);
    // pk not exists
    {
        sess.setArgs({"zrank", "tzk1", std::to_string(0)});
        auto expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtNull());
    }
    for (uint32_t i = 1; i < 10000; i++) {
        sess.setArgs({"zadd", "tzk1", std::to_string(i), std::to_string(i)});
        auto expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
    }
    for (uint32_t i = 1; i < 10000; i++) {
        sess.setArgs({"zrank", "tzk1", std::to_string(i)});
        auto expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtLongLong(i-1));
    }
    {
        sess.setArgs({"zrank", "tzk1", std::to_string(0)});
        auto expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtNull());
    }
    {
        sess.setArgs({"zadd", "tzk1", std::to_string(1), std::to_string(9999)});
        auto expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtLongLong(0));
        sess.setArgs({"zrank", "tzk1", std::to_string(9999)});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtLongLong(1));
    }

    for (uint32_t i = 1; i < 10000; i++) {
        sess.setArgs({"zrem", "tzk1", std::to_string(i)});
        auto expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtLongLong(1));
        sess.setArgs({"zcard", "tzk1"});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtLongLong(9999-i));
        sess.setArgs({"exists", "tzk1"});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        if (i != 9999) {
            EXPECT_EQ(expect.value(), Command::fmtLongLong(1));
        } else {
            EXPECT_EQ(expect.value(), Command::fmtLongLong(0));
        }
    }
}

void testType(std::shared_ptr<ServerEntry> svr) {
    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext);
    NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);
    sess.setArgs({"type", "test_type_key"});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtBulk("none"));

    sess.setArgs({"set", "test_type_key", "a"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    sess.setArgs({"type", "test_type_key"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtBulk("string"));

    sess.setArgs({"hset", "test_type_key", "a", "b"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    sess.setArgs({"type", "test_type_key"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtBulk("string"));

    sess.setArgs({"hset", "test_type_key1", "a", "b"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    sess.setArgs({"type", "test_type_key1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtBulk("hash"));
}

void testKV(std::shared_ptr<ServerEntry> svr) {
    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext);
    NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

    // set
    sess.setArgs({"set", "a", "1"});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOK());
    sess.setArgs({"set", "a", "1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOK());
    sess.setArgs({"set", "a", "1", "nx"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtNull());
    sess.setArgs({"set", "a", "1", "xx"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOK());
    sess.setArgs({"set", "a", "1", "xx"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOK());
    sess.setArgs({"set", "a", "1", "xx", "px", "1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOK());
    std::this_thread::sleep_for(std::chrono::seconds(1));
    sess.setArgs({"set", "a", "1", "xx"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtNull());

    // setnx
    sess.setArgs({"setnx", "a", "1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOne());
    sess.setArgs({"setnx", "a", "1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtZero());
    sess.setArgs({"expire", "a", "1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOne());
    std::this_thread::sleep_for(std::chrono::seconds(2));
    sess.setArgs({"setnx", "a", "1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOne());

    // setex
    sess.setArgs({"setex", "a", "1", "b"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOK());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    sess.setArgs({"get", "a"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtBulk("b"));
    std::this_thread::sleep_for(std::chrono::milliseconds(600));
    sess.setArgs({"get", "a"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtNull());

    // exists
    sess.setArgs({"set", "expire_test_key", "a"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOK());
    sess.setArgs({"exists", "expire_test_key"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOne());
    sess.setArgs({"expire", "expire_test_key", "1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOne());
    std::this_thread::sleep_for(std::chrono::seconds(2));
    sess.setArgs({"exists", "expire_test_key"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtZero());

    // incrdecr
    sess.setArgs({"incr", "incrdecrkey"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOne());
    sess.setArgs({"incr", "incrdecrkey"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(2));
    sess.setArgs({"incrby", "incrdecrkey", "2"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(4));
    sess.setArgs({"incrby",
                  "incrdecrkey",
                  std::to_string(std::numeric_limits<int64_t>::max())});
    expect = Command::runSessionCmd(&sess);
    EXPECT_FALSE(expect.ok()) << expect.value();
    sess.setArgs({"incrby", "incrdecrkey", "-1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(3));
    sess.setArgs({"decr", "incrdecrkey"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(2));
    sess.setArgs({"decr", "incrdecrkey"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(1));
    sess.setArgs({"decrby", "incrdecrkey", "3"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(-2));
    sess.setArgs({"decrby",
                  "incrdecrkey",
                  std::to_string(std::numeric_limits<int64_t>::max())});
    expect = Command::runSessionCmd(&sess);
    EXPECT_FALSE(expect.ok()) << expect.value();

    // append
    sess.setArgs({"append", "appendkey", "abc"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(3));
    sess.setArgs({"append", "appendkey", "abc"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(6));
    sess.setArgs({"expire", "appendkey", "1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOne());
    sess.setArgs({"exists", "appendkey"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOne());
    std::this_thread::sleep_for(std::chrono::seconds(2));
    sess.setArgs({"append", "appendkey", "abc"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(3));

    // getset
    sess.setArgs({"getset", "getsetkey", "abc"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtNull());
    sess.setArgs({"getset", "getsetkey", "def"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtBulk("abc"));
    sess.setArgs({"expire", "getsetkey", "1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOne());
    sess.setArgs({"getset", "getsetkey", "abc"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtBulk("def"));
    std::this_thread::sleep_for(std::chrono::seconds(2));
    sess.setArgs({"exists", "appendkey"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOne());

    // setbit
    sess.setArgs({"setbit", "setbitkey", "7", "1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtZero());
    sess.setArgs({"setbit", "setbitkey", "7", "0"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOne());
    sess.setArgs({"get", "setbitkey"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    std::string setbitRes;
    setbitRes.push_back(0);
    EXPECT_EQ(expect.value(), Command::fmtBulk(setbitRes));

    // setrange
    sess.setArgs({"setrange", "setrangekey", "7", "abc"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(10));
    std::string setrangeRes;
    setrangeRes.resize(10, 0);
    setrangeRes[7] = 'a';
    setrangeRes[8] = 'b';
    setrangeRes[9] = 'c';
    sess.setArgs({"get", "setrangekey"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtBulk(setrangeRes));
    sess.setArgs({"setrange", "setrangekey", "8", "aaa"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(11));
    setrangeRes.resize(11);
    setrangeRes[8] = 'a';
    setrangeRes[9] = 'a';
    setrangeRes[10] = 'a';
    sess.setArgs({"get", "setrangekey"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtBulk(setrangeRes));

    // bitcount
    sess.setArgs({"bitcount", "bitcountkey"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtZero());
    sess.setArgs({"set", "bitcountkey", "foobar"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOK());
    sess.setArgs({"bitcount", "bitcountkey"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(26));
    sess.setArgs({"bitcount", "bitcountkey", "0", "-1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(26));
    sess.setArgs({"bitcount", "bitcountkey", "2", "1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(0));
    std::vector<int> bitcountarr{4, 6, 6, 3, 3, 4};
    for (size_t i = 0; i < bitcountarr.size(); ++i) {
        sess.setArgs({"bitcount",
                      "bitcountkey",
                      std::to_string(i),
                      std::to_string(i)});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtLongLong(bitcountarr[i]));
    }

    // bitpos
    sess.setArgs({"bitpos", "bitposkey", "0"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(-1));
    sess.setArgs({"bitpos", "bitposkey", "1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(-1));
    sess.setArgs({"set", "bitposkey", {"\xff\xf0\x00", 3}});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOK());
    sess.setArgs({"bitpos", "bitposkey", "0"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(12));
    sess.setArgs({"set", "bitposkey", {"\x00\xff\xf0", 3}});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOK());
    sess.setArgs({"bitpos", "bitposkey", "1", "0"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(8));
    sess.setArgs({"bitpos", "bitposkey", "1", "2"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(16));
    sess.setArgs({"set", "bitposkey", {"\x00\x00\x00", 3}});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOK());
    sess.setArgs({"bitpos", "bitposkey", "1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(-1));
    sess.setArgs({"bitpos", "bitposkey", "0"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(0));

    // mget/mset
    sess.setArgs({"mset", "msetkey0", "0", "msetkey1", "1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOK());
    sess.setArgs({"mget", "msetkey0", "msetkey1", "msetkey2"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    std::stringstream ss;
    Command::fmtMultiBulkLen(ss, 3);
    Command::fmtBulk(ss, "0");
    Command::fmtBulk(ss, "1");
    Command::fmtNull(ss);
    EXPECT_EQ(ss.str(), expect.value());
}

void testExpire(std::shared_ptr<ServerEntry> svr) {
    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
    NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

    // bounder for optimistic del/pessimistic del
    for (auto v : {1000u, 10000u}) {
        for (uint32_t i = 0; i < v; i++) {
            sess.setArgs({"lpush", "a", std::to_string(2*i)});
            auto expect = Command::runSessionCmd(&sess);
            EXPECT_TRUE(expect.ok());
            EXPECT_EQ(expect.value(), Command::fmtLongLong(i+1));
        }

        sess.setArgs({"expire", "a", std::to_string(1)});
        auto expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtOne());

        sess.setArgs({"llen", "a"});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtLongLong(v));

        std::this_thread::sleep_for(std::chrono::seconds(2));
        sess.setArgs({"llen", "a"});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtZero());
    }
}

void testExpire1(std::shared_ptr<ServerEntry> svr) {
    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
    NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

    // bounder for optimistic del/pessimistic del
    for (auto v : {1000u, 10000u}) {
        for (uint32_t i = 0; i < v; i++) {
            sess.setArgs({"lpush", "a", std::to_string(2*i)});
            auto expect = Command::runSessionCmd(&sess);
            EXPECT_TRUE(expect.ok());
            EXPECT_EQ(expect.value(), Command::fmtLongLong(i+1));
        }

        sess.setArgs({"expire", "a", std::to_string(-1)});
        auto expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtOne());

        sess.setArgs({"llen", "a"});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtZero());

        sess.setArgs({"expire", "a", std::to_string(-1)});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtZero());
    }
}

void testExpire2(std::shared_ptr<ServerEntry> svr) {
    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
    NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

    // bounder for optimistic del/pessimistic del
    for (auto v : {1000u, 10000u}) {
        for (uint32_t i = 0; i < v; i++) {
            sess.setArgs({"lpush", "a", std::to_string(2*i)});
            auto expect = Command::runSessionCmd(&sess);
            EXPECT_TRUE(expect.ok());
            EXPECT_EQ(expect.value(), Command::fmtLongLong(i+1));
        }

        for (uint32_t i = 0; i < v; i++) {
            sess.setArgs({"hset", "a", std::to_string(i), std::to_string(i)});
            auto expect = Command::runSessionCmd(&sess);
            EXPECT_TRUE(expect.ok());
            EXPECT_EQ(expect.value(), Command::fmtOne());
        }

        sess.setArgs({"llen", "a"});
        auto expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtLongLong(v));

        sess.setArgs({"hlen", "a"});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtLongLong(v));

        sess.setArgs({"expire", "a", std::to_string(-1)});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtOne());

        sess.setArgs({"llen", "a"});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtZero());

        sess.setArgs({"hlen", "a"});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtZero());
    }
}

void testSetRetry(std::shared_ptr<ServerEntry> svr) {
    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
    NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);
    NetSession sess1(svr, std::move(socket1), 1, false, nullptr, nullptr);

    uint32_t cnt = 0;
    const auto guard = MakeGuard([] {
        SyncPoint::GetInstance()->ClearAllCallBacks();
    });
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack(
        "setGeneric::SetKV::1", [&](void* arg) {
            ++cnt;
            if (cnt % 2 == 1) {
                sess1.setArgs({"set", "a", "1"});
                auto expect = Command::runSessionCmd(&sess1);
                EXPECT_TRUE(expect.ok());
                EXPECT_EQ(expect.value(), Command::fmtOK());
            }
        });

    sess.setArgs({"set", "a", "1"});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_EQ(cnt, uint32_t(6));
    EXPECT_EQ(expect.status().code(), ErrorCodes::ERR_COMMIT_RETRY);
}

void testDel(std::shared_ptr<ServerEntry> svr) {
    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
    NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

    // bounder for optimistic del/pessimistic del
    for (auto v : {1000u, 10000u}) {
        sess.setArgs({"set", "a", "b"});
        auto expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtOK());

        for (uint32_t i = 0; i < v; i++) {
            sess.setArgs({"lpush", "a", std::to_string(2*i)});
            auto expect = Command::runSessionCmd(&sess);
            EXPECT_TRUE(expect.ok());
            EXPECT_EQ(expect.value(), Command::fmtLongLong(i+1));
        }

        sess.setArgs({"expire", "a", std::to_string(1)});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtOne());

        sess.setArgs({"del", "a"});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtOne());

        sess.setArgs({"llen", "a"});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtZero());

        sess.setArgs({"get", "a"});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtNull());
    }
    for (auto v : {1000u, 10000u}) {
        for (uint32_t i = 0; i < v; i++) {
            sess.setArgs({"lpush", "a", std::to_string(2*i)});
            auto expect = Command::runSessionCmd(&sess);
            EXPECT_TRUE(expect.ok());
            EXPECT_EQ(expect.value(), Command::fmtLongLong(i+1));
        }

        sess.setArgs({"expire", "a", std::to_string(1)});
        auto expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtOne());

        std::this_thread::sleep_for(std::chrono::seconds(2));
        sess.setArgs({"del", "a"});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtZero());
    }
}

/*
TEST(Command, del) {
    auto cfg = genParams();
    EXPECT_TRUE(filesystem::create_directory("db"));
    // EXPECT_TRUE(filesystem::create_directory("db/0"));
    EXPECT_TRUE(filesystem::create_directory("log"));
    const auto guard = MakeGuard([] {
        filesystem::remove_all("./log");
        filesystem::remove_all("./db");
    });
    auto blockCache =
        rocksdb::NewLRUCache(cfg->rocksBlockcacheMB * 1024 * 1024LL, 4);

    auto server = std::make_shared<ServerEntry>();
    std::vector<PStore> tmpStores;
    for (size_t i = 0; i < KVStore::INSTANCE_NUM; ++i) {
        std::stringstream ss;
        ss << i;
        std::string dbId = ss.str();
        tmpStores.emplace_back(std::unique_ptr<KVStore>(
            new RocksKVStore(dbId, cfg, blockCache)));
    }
    server->installStoresInLock(tmpStores);
    auto segMgr = std::unique_ptr<SegmentMgr>(
            new SegmentMgrFnvHash64(tmpStores));
    server->installSegMgrInLock(std::move(segMgr));
    auto tmpPessimisticMgr = std::make_unique<PessimisticMgr>(
        KVStore::INSTANCE_NUM);
    server->installPessimisticMgrInLock(std::move(tmpPessimisticMgr));

    testDel(server);
}

TEST(Command, expire) {
    auto cfg = genParams();
    EXPECT_TRUE(filesystem::create_directory("db"));
    // EXPECT_TRUE(filesystem::create_directory("db/0"));
    EXPECT_TRUE(filesystem::create_directory("log"));
    const auto guard = MakeGuard([] {
        filesystem::remove_all("./log");
        filesystem::remove_all("./db");
    });
    auto blockCache =
        rocksdb::NewLRUCache(cfg->rocksBlockcacheMB * 1024 * 1024LL, 4);

    auto server = std::make_shared<ServerEntry>();
    std::vector<PStore> tmpStores;
    for (size_t i = 0; i < KVStore::INSTANCE_NUM; ++i) {
        std::stringstream ss;
        ss << i;
        std::string dbId = ss.str();
        tmpStores.emplace_back(std::unique_ptr<KVStore>(
            new RocksKVStore(dbId, cfg, blockCache)));
    }
    server->installStoresInLock(tmpStores);
    auto segMgr = std::unique_ptr<SegmentMgr>(
            new SegmentMgrFnvHash64(tmpStores));
    server->installSegMgrInLock(std::move(segMgr));
    auto tmpPessimisticMgr = std::make_unique<PessimisticMgr>(
        KVStore::INSTANCE_NUM);
    server->installPessimisticMgrInLock(std::move(tmpPessimisticMgr));

    testExpire(server);
    testExpire1(server);
    testExpire2(server);
}
*/

TEST(Command, common) {
    auto cfg = genParams();
    EXPECT_TRUE(filesystem::create_directory("db"));
    // EXPECT_TRUE(filesystem::create_directory("db/0"));
    EXPECT_TRUE(filesystem::create_directory("log"));
    const auto guard = MakeGuard([] {
        filesystem::remove_all("./log");
        filesystem::remove_all("./db");
    });
    auto blockCache =
        rocksdb::NewLRUCache(cfg->rocksBlockcacheMB * 1024 * 1024LL, 4);

    auto server = std::make_shared<ServerEntry>();
    std::vector<PStore> tmpStores;
    for (size_t i = 0; i < KVStore::INSTANCE_NUM; ++i) {
        std::stringstream ss;
        ss << i;
        std::string dbId = ss.str();
        tmpStores.emplace_back(std::unique_ptr<KVStore>(
            new RocksKVStore(dbId, cfg, blockCache)));
    }
    server->installStoresInLock(tmpStores);
    auto segMgr = std::unique_ptr<SegmentMgr>(
            new SegmentMgrFnvHash64(tmpStores));
    server->installSegMgrInLock(std::move(segMgr));
    auto tmpPessimisticMgr = std::make_unique<PessimisticMgr>(
        KVStore::INSTANCE_NUM);
    server->installPessimisticMgrInLock(std::move(tmpPessimisticMgr));

    /*
    testKV(server);
    testSetRetry(server);
    testType(server);
    testHash1(server);
    testHash2(server);
    testList(server);
    // zadd/zrem/zrank
    testZset(server);
    // zcount
    testZset2(server);
    */
    // zlexcount
    testZset3(server);
}

}  // namespace tendisplus
