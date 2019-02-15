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

    // case from redis.io, lrange
    std::stringstream ss;
    sess.setArgs({"rpush", "lrangekey", "one"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(1));
    sess.setArgs({"rpush", "lrangekey", "two"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(2));
    sess.setArgs({"rpush", "lrangekey", "three"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(3));
    sess.setArgs({"lrange", "lrangekey", "0", "0"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    Command::fmtMultiBulkLen(ss, 1);
    Command::fmtBulk(ss, "one");
    EXPECT_EQ(expect.value(), ss.str());
    ss.str("");
    sess.setArgs({"lrange", "lrangekey", "-3", "2"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    Command::fmtMultiBulkLen(ss, 3);
    Command::fmtBulk(ss, "one");
    Command::fmtBulk(ss, "two");
    Command::fmtBulk(ss, "three");
    EXPECT_EQ(expect.value(), ss.str());
    ss.str("");
    sess.setArgs({"lrange", "lrangekey", "-100", "100"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    Command::fmtMultiBulkLen(ss, 3);
    Command::fmtBulk(ss, "one");
    Command::fmtBulk(ss, "two");
    Command::fmtBulk(ss, "three");
    EXPECT_EQ(expect.value(), ss.str());
    ss.str("");
    sess.setArgs({"lrange", "lrangekey", "5", "10"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtZeroBulkLen());

    // case from redis.io, ltrim
    sess.setArgs({"rpush", "ltrimkey", "one"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(1));
    sess.setArgs({"rpush", "ltrimkey", "two"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(2));
    sess.setArgs({"rpush", "ltrimkey", "three"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(3));
    sess.setArgs({"ltrim", "ltrimkey", "1", "-1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOK());
    ss.str("");
    sess.setArgs({"lrange", "ltrimkey", "0", "-1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    Command::fmtMultiBulkLen(ss, 2);
    Command::fmtBulk(ss, "two");
    Command::fmtBulk(ss, "three");
    EXPECT_EQ(expect.value(), ss.str());
    sess.setArgs({"llen", "ltrimkey"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(2));
    sess.setArgs({"ltrim", "ltrimkey", "2", "2"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOK());
    sess.setArgs({"exists", "ltrimkey"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(0));

    // case from redis.io rpoplpush 
    sess.setArgs({"rpush", "rpoplpushkey", "one"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(1));
    sess.setArgs({"rpush", "rpoplpushkey", "two"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(2));
    sess.setArgs({"rpush", "rpoplpushkey", "three"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(3));
    ss.str("");
    sess.setArgs({"rpoplpush", "rpoplpushkey", "rpoplpushkey2"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtBulk("three"));
    ss.str("");
    sess.setArgs({"lrange", "rpoplpushkey", "0", "-1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    Command::fmtMultiBulkLen(ss, 2);
    Command::fmtBulk(ss, "one");
    Command::fmtBulk(ss, "two");
    EXPECT_EQ(expect.value(), ss.str());
    ss.str("");
    sess.setArgs({"lrange", "rpoplpushkey2", "0", "-1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    Command::fmtMultiBulkLen(ss, 1);
    Command::fmtBulk(ss, "three");
    EXPECT_EQ(expect.value(), ss.str());
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

    // hmcas key, cmp, vsn, [subkey1, op1, val1]
    sess.setArgs({"hmcas", "hmcaskey1", "1", "123", "subkey1", "0", "subval1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(Command::fmtOne(), expect.value());
    sess.setArgs({"hmcas", "hmcaskey1", "1", "123", "subkey1", "0", "subval1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(Command::fmtOne(), expect.value());
    sess.setArgs({"hmcas", "hmcaskey1", "1", "124", "subkey1", "0", "subval2"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(Command::fmtOne(), expect.value());
    sess.setArgs({"hget", "hmcaskey1", "subkey1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtBulk("subval2"));
    sess.setArgs({"hlen", "hmcaskey1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(1));
    sess.setArgs({"hmcas", "hmcaskey1", "0", "999", "subkey1", "0", "subval2"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(Command::fmtOne(), expect.value());

    // parse int failed
    sess.setArgs({"hmcas", "hmcaskey1", "1", "999", "subkey1", "1", "10"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_FALSE(expect.ok());

    sess.setArgs({"hmcas", "hmcaskey1", "1", "999", "subkey1", "0", "-100"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(Command::fmtOne(), expect.value());

    sess.setArgs({"hmcas", "hmcaskey1", "1", "1000", "subkey1", "1", "100"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(Command::fmtOne(), expect.value());

    sess.setArgs({"hget", "hmcaskey1", "subkey1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtBulk("0"));
}

void testHash1(std::shared_ptr<ServerEntry> svr) {
    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
    NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);
    for (uint32_t i = 0; i < 10000; i++) {
        sess.setArgs({"hset", "a", std::to_string(i), std::to_string(i)});
        auto expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok()) << expect.status().toString();
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

    for (uint32_t i = 0; i < 1000; i++) {
        sess.setArgs({"hmset",
                      "hmsetkey",
                      std::to_string(i),
                      std::to_string(i),
                      std::to_string(i+1),
                      std::to_string(i+1)});
        auto expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        sess.setArgs({"hlen", "hmsetkey"});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtLongLong(i+2));
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

void testZset4(std::shared_ptr<ServerEntry> svr) {
    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
    NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

    sess.setArgs({"zadd", "tzk4.1",
                 "1", "one", "2", "two", "3", "three", "4", "four",
                 "5", "five", "6", "six", "7", "seven"});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(7));

    sess.setArgs({"zremrangebyrank", "tzk4.1", "0", "1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(2));
    sess.setArgs({"zrevrange", "tzk4.1", "0", "-1", "withscores"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok()) << expect.status().toString();
    std::stringstream ss;
    Command::fmtMultiBulkLen(ss, 10);
    Command::fmtBulk(ss, "seven");
    Command::fmtBulk(ss, "7");
    Command::fmtBulk(ss, "six");
    Command::fmtBulk(ss, "6");
    Command::fmtBulk(ss, "five");
    Command::fmtBulk(ss, "5");
    Command::fmtBulk(ss, "four");
    Command::fmtBulk(ss, "4");
    Command::fmtBulk(ss, "three");
    Command::fmtBulk(ss, "3");
    EXPECT_EQ(expect.value(), ss.str());
    ss.str("");

    sess.setArgs({"zadd", "tzk4.2",
                 "0", "aaaa", "0", "b", "0", "c", "0", "d",
                 "0", "e"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    sess.setArgs({"zadd", "tzk4.2",
                "0", "foo", "0", "zap", "0", "zip", "0", "ALPHA", "0", "alpha"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());

    sess.setArgs({"zremrangebylex", "tzk4.2", "[alpha", "[omega"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(6));

    ss.str("");
    sess.setArgs({"zrange", "tzk4.2", "0", "-1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    Command::fmtMultiBulkLen(ss, 4);
    Command::fmtBulk(ss, "ALPHA");
    Command::fmtBulk(ss, "aaaa");
    Command::fmtBulk(ss, "zap");
    Command::fmtBulk(ss, "zip");
    EXPECT_EQ(expect.value(), ss.str());

    // TODO(deyukong): test zremrangebyscore
}

void testZset3(std::shared_ptr<ServerEntry> svr) {
    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
    NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

    // NOTE(deyukong): zlexcount has undefined behavior when scores
    // are not all the same.

    // zlexcount case from redis.io
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

    // zrange case from redis.io
    sess.setArgs({"zadd",
                  "tzk3.1",
                  "1", "one", "2", "two", "3", "three"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(3));

    sess.setArgs({"zrange",
                  "tzk3.1",
                  "0", "-1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok()) << expect.status().toString();
    std::stringstream ss;
    Command::fmtMultiBulkLen(ss, 3);
    Command::fmtBulk(ss, "one");
    Command::fmtBulk(ss, "two");
    Command::fmtBulk(ss, "three");
    EXPECT_EQ(expect.value(), ss.str());
    ss.str("");

    sess.setArgs({"zrange",
                  "tzk3.1",
                  "2", "3"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    Command::fmtMultiBulkLen(ss, 1);
    Command::fmtBulk(ss, "three");
    EXPECT_EQ(expect.value(), ss.str());
    ss.str("");

    sess.setArgs({"zrange",
                  "tzk3.1",
                  "-2", "-1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    Command::fmtMultiBulkLen(ss, 2);
    Command::fmtBulk(ss, "two");
    Command::fmtBulk(ss, "three");
    EXPECT_EQ(expect.value(), ss.str());

    // zrange random tests
    std::vector<uint64_t> keys;
    for (uint32_t i = 0; i < 100; i++) {
        keys.push_back(i);
    }
    std::random_shuffle(keys.begin(), keys.end());
    for (uint32_t i = 0; i < 100; i++) {
        sess.setArgs({"zadd",
                      "tzk3.2",
                      std::to_string(keys[i]),
                      std::to_string(keys[i])});
        auto expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
    }
    for (uint32_t i = 0; i < 100; i++) {
        for (uint32_t j = 0; j < 100; j++) {
            sess.setArgs({"zrange",
                          "tzk3.2",
                          std::to_string(i),
                          std::to_string(j)});
            expect = Command::runSessionCmd(&sess);
            EXPECT_TRUE(expect.ok());
            std::stringstream ss;
            if (i > j) {
                EXPECT_EQ(expect.value(), Command::fmtZeroBulkLen());
            } else {
                Command::fmtMultiBulkLen(ss, j-i+1);
                for (uint32_t k = i; k <= j; ++k) {
                    Command::fmtBulk(ss, std::to_string(k));
                }
                EXPECT_EQ(expect.value(), ss.str());
            }
        }
    }
    for (uint32_t i = 0; i < 100; i++) {
        for (uint32_t j = 0; j < 100; j++) {
            sess.setArgs({"zrevrange",
                          "tzk3.2",
                          std::to_string(i),
                          std::to_string(j)});
            expect = Command::runSessionCmd(&sess);
            EXPECT_TRUE(expect.ok());
            std::stringstream ss;
            if (i > j) {
                EXPECT_EQ(expect.value(), Command::fmtZeroBulkLen());
            } else {
                Command::fmtMultiBulkLen(ss, j-i+1);
                for (uint32_t k = i; k <= j; ++k) {
                    Command::fmtBulk(ss, std::to_string(100-1-k));
                }
                EXPECT_EQ(expect.value(), ss.str());
            }
        }
    }

    // zrangebylex cases from redis.io
    ss.str("");
    sess.setArgs({"zadd", "tzk3.3",
                 "0", "a", "0", "b", "0", "c", "0", "d",
                 "0", "e", "0", "f", "0", "g"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    sess.setArgs({"zrangebylex", "tzk3.3", "-", "[c"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    Command::fmtMultiBulkLen(ss, 3);
    Command::fmtBulk(ss, "a");
    Command::fmtBulk(ss, "b");
    Command::fmtBulk(ss, "c");
    EXPECT_EQ(expect.value(), ss.str());
   
    ss.str("");
    sess.setArgs({"zrangebylex", "tzk3.3", "-", "(c"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    Command::fmtMultiBulkLen(ss, 2);
    Command::fmtBulk(ss, "a");
    Command::fmtBulk(ss, "b");
    EXPECT_EQ(expect.value(), ss.str());

    ss.str("");
    sess.setArgs({"zrangebylex", "tzk3.3", "[aaa", "(g"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    Command::fmtMultiBulkLen(ss, 5);
    Command::fmtBulk(ss, "b");
    Command::fmtBulk(ss, "c");
    Command::fmtBulk(ss, "d");
    Command::fmtBulk(ss, "e");
    Command::fmtBulk(ss, "f");
    EXPECT_EQ(expect.value(), ss.str());

    // zrangebyscore cases from redis.io
    ss.str("");
    sess.setArgs({"zadd", "tzk3.4",
                 "1", "one", "2", "two", "3", "three", "4", "four",
                 "5", "five", "6", "six", "7", "seven"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());

    sess.setArgs({"zrangebyscore", "tzk3.4", "-inf", "+inf"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    Command::fmtMultiBulkLen(ss, 7);
    Command::fmtBulk(ss, "one");
    Command::fmtBulk(ss, "two");
    Command::fmtBulk(ss, "three");
    Command::fmtBulk(ss, "four");
    Command::fmtBulk(ss, "five");
    Command::fmtBulk(ss, "six");
    Command::fmtBulk(ss, "seven");
    EXPECT_EQ(expect.value(), ss.str());
    
    ss.str("");
    sess.setArgs({"zrangebyscore", "tzk3.4", "1", "2"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    Command::fmtMultiBulkLen(ss, 2);
    Command::fmtBulk(ss, "one");
    Command::fmtBulk(ss, "two");
    EXPECT_EQ(expect.value(), ss.str());

    ss.str("");
    sess.setArgs({"zrangebyscore", "tzk3.4", "(1", "2"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    Command::fmtMultiBulkLen(ss, 1);
    Command::fmtBulk(ss, "two");
    EXPECT_EQ(expect.value(), ss.str());

    ss.str("");
    sess.setArgs({"zrangebyscore", "tzk3.4", "(1", "(2"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    Command::fmtMultiBulkLen(ss, 0);
    EXPECT_EQ(expect.value(), ss.str());
}

void testSet(std::shared_ptr<ServerEntry> svr) {
    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
    NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

    sess.setArgs({"sadd", "settestkey1", "one", "two", "three"});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(3));
    sess.setArgs({"spop", "settestkey1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtBulk("one")) << expect.status().toString();
    sess.setArgs({"scard", "settestkey1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(2)) << expect.status().toString();

    // srandmember
    sess.setArgs({"sadd", "settestkey2", "one", "two", "three"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(3));
    sess.setArgs({"srandmember", "settestkey2"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_TRUE(expect.value() == Command::fmtBulk("one") ||
                expect.value() == Command::fmtBulk("two") ||
                expect.value() == Command::fmtBulk("three"));

    sess.setArgs({"srandmember", "settestkey2", "1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    std::stringstream ss1, ss2, ss3;
    Command::fmtMultiBulkLen(ss1, 1);
    Command::fmtBulk(ss1, "one");
    Command::fmtMultiBulkLen(ss2, 1);
    Command::fmtBulk(ss2, "two");
    Command::fmtMultiBulkLen(ss3, 1);
    Command::fmtBulk(ss3, "three");
    EXPECT_TRUE(expect.value() == ss1.str() ||
                expect.value() == ss2.str() ||
                expect.value() == ss3.str());
    sess.setArgs({"srandmember", "settestkey2", "2"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    ss1.str(""), ss2.str("");
    Command::fmtMultiBulkLen(ss1, 2);
    Command::fmtBulk(ss1, "one");
    Command::fmtBulk(ss1, "three");
    Command::fmtMultiBulkLen(ss2, 2);
    Command::fmtBulk(ss2, "three");
    Command::fmtBulk(ss2, "two");
    EXPECT_TRUE(expect.value() == ss1.str() ||
                expect.value() == ss2.str());

    // smembers
    sess.setArgs({"sadd", "settestkey3", "hello", "world"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(2));
    sess.setArgs({"smembers", "settestkey3"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    ss1.str("");
    Command::fmtMultiBulkLen(ss1, 2);
    Command::fmtBulk(ss1, "hello");
    Command::fmtBulk(ss1, "world");
    EXPECT_EQ(ss1.str(), expect.value());
    sess.setArgs({"smembers", "settestkey4"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtZeroBulkLen());

    // sdiff
    sess.setArgs({"sadd", "sdiffkey1", "a", "b", "c", "d"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    sess.setArgs({"sadd", "sdiffkey2", "c"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    sess.setArgs({"sadd", "sdiffkey3", "a", "c", "e"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    sess.setArgs({"sdiff", "sdiffkey1", "sdiffkey2", "sdiffkey3"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    ss1.str("");
    Command::fmtMultiBulkLen(ss1, 2);
    Command::fmtBulk(ss1, "b");
    Command::fmtBulk(ss1, "d");
    EXPECT_EQ(expect.value(), ss1.str());
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

    {
        sess.setArgs({"zscore", "notfoundkey", "subkey"});
        auto expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtNull());

        sess.setArgs({"zadd", "tzk1.1", std::to_string(9999), "subkey"});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
 
        sess.setArgs({"zscore", "tzk1.1", "notfoundsubkey"});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtNull());

        sess.setArgs({"zscore", "tzk1.1", "subkey"});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtBulk(std::to_string(9999)));
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

    // getrange
    sess.setArgs({"getrange", "setrangekey", "0", "-1"});
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

    // cas/getvsn
    sess.setArgs({"set", "caskey", "1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOK());
    sess.setArgs({"getvsn", "caskey"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    ss.str("");
    Command::fmtMultiBulkLen(ss, 2);
    Command::fmtLongLong(ss, 0);
    Command::fmtBulk(ss, "1");
    EXPECT_EQ(expect.value(), ss.str());
    sess.setArgs({"cas", "caskey", "0", "2"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOK());
    sess.setArgs({"cas", "caskey", "0", "2"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_FALSE(expect.ok());
    EXPECT_EQ(expect.status().code(), ErrorCodes::ERR_CAS);
    sess.setArgs({"getvsn", "caskey"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    ss.str("");
    Command::fmtMultiBulkLen(ss, 2);
    Command::fmtLongLong(ss, 1);
    Command::fmtBulk(ss, "2");

    // bitop
    sess.setArgs({"set", "bitopkey1", "foobar"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOK());
    sess.setArgs({"set", "bitopkey2", "abcdef"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOK());
    sess.setArgs({"bitop", "and", "bitopdestkey", "bitopkey1", "bitopkey2"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(6));
    sess.setArgs({"get", "bitopdestkey"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtBulk("`bc`ab"));
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

    for (int i = 0; i < 10000; ++i) {
        sess.setArgs({"zadd",
                      "testzsetdel",
                      std::to_string(i),
                      std::to_string(i)});
        auto expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
    }
    const auto guard = MakeGuard([] {
        SyncPoint::GetInstance()->ClearAllCallBacks();
    });
    std::cout<< "begin delete zset" << std::endl;
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack(
        "delKeyPessimistic::TotalCount", [&](void* arg) {
            uint64_t v = *(static_cast<uint64_t*>(arg));
            EXPECT_EQ(v, 20001U);
        });
    sess.setArgs({"del", "testzsetdel"});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
}

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

/*
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

void testScan(std::shared_ptr<ServerEntry> svr) {
    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
    NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

    sess.setArgs({"sadd", "scanset", "a", "b", "c", "d", "e", "f",
                  "g", "h", "i", "j", "k", "l", "m", "n", "o"});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());

    sess.setArgs({"sscan", "scanset", "0"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    std::stringstream ss;
    Command::fmtMultiBulkLen(ss, 2);
    std::string cursor = "000585A800000000733733363336313645373336353734006B0700";
    Command::fmtBulk(ss, cursor);
    Command::fmtMultiBulkLen(ss, 10);
    for (int i = 0; i < 10; ++i) {
        std::string tmp;
        tmp.push_back('a' + i);
        Command::fmtBulk(ss, tmp);
    }
    EXPECT_EQ(ss.str(), expect.value());

    sess.setArgs({"sscan", "scanset", cursor});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok()) << expect.status().toString();
    ss.str("");
    Command::fmtMultiBulkLen(ss, 2);
    cursor = "0";
    Command::fmtBulk(ss, cursor);
    Command::fmtMultiBulkLen(ss, 5);
    for (int i = 0; i < 5; ++i) {
        std::string tmp;
        tmp.push_back('a' + 10 + i);
        Command::fmtBulk(ss, tmp);
    }
    EXPECT_EQ(ss.str(), expect.value());
}

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

    testList(server);
    testKV(server);

    // testSetRetry only works in TXN_OPT mode
    // testSetRetry(server);
    testType(server);
    testHash1(server);
    testHash2(server);
    testSet(server);
    // zadd/zrem/zrank/zscore
    testZset(server);
    // zcount
    testZset2(server);
    // zlexcount, zrange, zrangebylex, zrangebyscore
    testZset3(server);
    // zremrangebyrank, zremrangebylex, zremrangebyscore
    testZset4(server);
    testScan(server);
}

/*
TEST(Command, keys) {
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

    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext);
    NetSession sess(server, std::move(socket), 1, false, nullptr, nullptr);

    sess.setArgs({"set", "a", "a"});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOK());
    sess.setArgs({"set", "b", "b"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOK());
    sess.setArgs({"set", "c", "c"});
    expect = Command::runSessionCmd(&sess);

    sess.setArgs({"keys", "*"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    std::stringstream ss;
    Command::fmtMultiBulkLen(ss, 3);
    Command::fmtBulk(ss, "a");
    Command::fmtBulk(ss, "b");
    Command::fmtBulk(ss, "c");
    EXPECT_EQ(expect.value(), ss.str());

    sess.setArgs({"keys", "a*"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    ss.str("");
    Command::fmtMultiBulkLen(ss, 1);
    Command::fmtBulk(ss, "a");
    EXPECT_EQ(expect.value(), ss.str());
}
*/
}  // namespace tendisplus
