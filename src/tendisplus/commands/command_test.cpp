#include <fstream>
#include <utility>
#include <memory>
#include <vector>
#include <limits>
#include <algorithm>
#include <random>
#include "gtest/gtest.h"
#include "tendisplus/utils/status.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/utils/portable.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/test_util.h"
#include "tendisplus/storage/rocks/rocks_kvstore.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/server/server_params.h"
#include "tendisplus/server/server_entry.h"
#include "tendisplus/utils/string.h"

namespace tendisplus {

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

        sess.setArgs({"del", "a"});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtOne());

        for (uint32_t i = 0; i < v; i++) {
            sess.setArgs({"lpush", "a", std::to_string(2*i)});
            auto expect = Command::runSessionCmd(&sess);
            EXPECT_TRUE(expect.ok());
            EXPECT_EQ(expect.value(), Command::fmtLongLong(i+1));
        }

        sess.setArgs({"get", "a"});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(!expect.ok());

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
    const auto guard = MakeGuard([] {
        destroyEnv();
    });

    EXPECT_TRUE(setupEnv());
    auto cfg = makeServerParam();
    auto server = makeServerEntry(cfg);

    testDel(server);
}

TEST(Command, expire) {
    const auto guard = MakeGuard([] {
        destroyEnv();
    });

    EXPECT_TRUE(setupEnv());

    auto cfg = makeServerParam();
    auto server = makeServerEntry(cfg);

    testExpire(server);
    testExpire1(server);
    testExpire2(server);
}

void testExtendProtocol(std::shared_ptr<ServerEntry> svr) {
    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
    NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

    sess.setArgs({ "config", "set", "session",
                            "tendis_protocol_extend", "1" });
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());

    sess.setArgs({ "sadd", "ss", "a", "100", "100", "v1" });
    auto s = sess.processExtendProtocol();
    EXPECT_TRUE(s.ok());
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(sess.getServerEntry()->getTsEp(), 100);

    sess.setArgs({ "sadd", "ss", "b", "101", "101", "v1" });
    s = sess.processExtendProtocol();
    EXPECT_TRUE(s.ok());
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(sess.getServerEntry()->getTsEp(), 101);

    sess.setArgs({ "sadd", "ss", "c", "102", "a", "v1" });
    s = sess.processExtendProtocol();
    EXPECT_TRUE(!s.ok());
    EXPECT_EQ(sess.getServerEntry()->getTsEp(), 101);

    std::stringstream ss1;
    sess.setArgs({"smembers", "ss", "102", "102", "v1"});
    s = sess.processExtendProtocol();
    EXPECT_TRUE(s.ok());
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    ss1.str("");
    Command::fmtMultiBulkLen(ss1, 2);
    Command::fmtBulk(ss1, "a");
    Command::fmtBulk(ss1, "b");
    EXPECT_EQ(ss1.str(), expect.value());

    // version ep behaviour test -- hash
    {
        sess.setArgs({"hset", "hash", "key", "1000", "100", "100", "v1"});
        s = sess.processExtendProtocol();
        EXPECT_TRUE(s.ok());
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());

        // for normal occasion, smaller version can't overwrite greater op.
        sess.setArgs({"hset", "hash", "key", "999", "101", "99", "v1"});
        s = sess.processExtendProtocol();
        EXPECT_TRUE(s.ok());
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(!expect.ok());

        // cmd with no EP can modify key's which version is not -1
        sess.setArgs({"hset", "hash", "key1", "10"});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());

        // cmd with greater version is allowed.
        sess.setArgs({"hset", "hash", "key1", "1080", "102", "102", "v1"});
        s = sess.processExtendProtocol();
        EXPECT_TRUE(s.ok());
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        sess.setArgs({"hget", "hash", "key1", "103", "103", "v1"});
        s = sess.processExtendProtocol();
        EXPECT_TRUE(s.ok());
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(Command::fmtBulk("1080"), expect.value());

        sess.setArgs({"hincrby", "hash", "key1", "1", "101", "101", "v1"});
        s = sess.processExtendProtocol();
        EXPECT_TRUE(s.ok());
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(!expect.ok());
        sess.setArgs({"hincrby", "hash", "key1", "2", "103", "103", "v1"});
        s = sess.processExtendProtocol();
        EXPECT_TRUE(s.ok());
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        sess.setArgs({"hget", "hash", "key1", "104", "104", "v1"});
        s = sess.processExtendProtocol();
        EXPECT_TRUE(s.ok());
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(Command::fmtBulk("1082"), expect.value());

        sess.setArgs({"hset", "hash2", "key2", "ori"});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());

        // overwrite version.
        sess.setArgs({"hset", "hash2", "key2", "EPset", "100", "100", "v1"});
        s = sess.processExtendProtocol();
        EXPECT_TRUE(s.ok());
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());

        sess.setArgs({"hset", "hash2", "key2", "naked"});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());

        sess.setArgs({"hget", "hash2", "key2", "100", "100", "v1"});
        s = sess.processExtendProtocol();
        EXPECT_TRUE(s.ok());
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(Command::fmtBulk("naked"), expect.value());
    }

    {
        sess.setArgs({"zadd", "zset1", "5", "foo", "100", "100", "v1"});
        s = sess.processExtendProtocol();
        EXPECT_TRUE(s.ok());
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());

        sess.setArgs({"zadd", "zset1", "6", "bar", "100", "100", "v1"});
        s = sess.processExtendProtocol();
        EXPECT_TRUE(s.ok());
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(!expect.ok());

        sess.setArgs({"zrange", "zset1", "0", "-1", "101", "101", "v1"});
        s = sess.processExtendProtocol();
        EXPECT_TRUE(s.ok());
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        ss1.str("");
        Command::fmtMultiBulkLen(ss1, 1);
        Command::fmtBulk(ss1, "foo");
        EXPECT_EQ(ss1.str(), expect.value());

        sess.setArgs({"zadd", "zset1", "7", "baz", "101", "101", "v1"});
        s = sess.processExtendProtocol();
        EXPECT_TRUE(s.ok());
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());

        sess.setArgs({"zrange", "zset1", "0", "-1", "102", "102", "v1"});
        s = sess.processExtendProtocol();
        EXPECT_TRUE(s.ok());
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        ss1.str("");
        Command::fmtMultiBulkLen(ss1, 2);
        Command::fmtBulk(ss1, "foo");
        Command::fmtBulk(ss1, "baz");
        EXPECT_EQ(ss1.str(), expect.value());

        sess.setArgs({"zrem", "zset1", "baz", "100", "100", "v1"});
        s = sess.processExtendProtocol();
        EXPECT_TRUE(s.ok());
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(!expect.ok());

        sess.setArgs({"zrem", "zset1", "foo", "102", "102", "v1"});
        s = sess.processExtendProtocol();
        EXPECT_TRUE(s.ok());
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());

        sess.setArgs({"zrange", "zset1", "0", "-1", "103", "103", "v1"});
        s = sess.processExtendProtocol();
        EXPECT_TRUE(s.ok());
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        ss1.str("");
        Command::fmtMultiBulkLen(ss1, 1);
        Command::fmtBulk(ss1, "baz");
        EXPECT_EQ(ss1.str(), expect.value());
    }

    {
        sess.setArgs({"rpush", "list1", "a", "b", "c", "100", "100", "v1"});
        s = sess.processExtendProtocol();
        EXPECT_TRUE(s.ok());
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());

        sess.setArgs({"rpop", "list1", "99", "99", "v1"});
        s = sess.processExtendProtocol();
        EXPECT_TRUE(s.ok());
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(!expect.ok());

        sess.setArgs({"lpop", "list1", "101", "101", "v1"});
        s = sess.processExtendProtocol();
        EXPECT_TRUE(s.ok());
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());

        sess.setArgs({"lrange", "list1", "0", "-1", "102", "102", "v1"});
        s = sess.processExtendProtocol();
        EXPECT_TRUE(s.ok());
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        ss1.str("");
        Command::fmtMultiBulkLen(ss1, 2);
        Command::fmtBulk(ss1, "b");
        Command::fmtBulk(ss1, "c");
        EXPECT_EQ(ss1.str(), expect.value());

        sess.setArgs({"rpush", "list1", "z", "100", "100", "v1"});
        s = sess.processExtendProtocol();
        EXPECT_TRUE(s.ok());
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(!expect.ok());

        sess.setArgs({"lpush", "list1", "d", "102", "102", "v1"});
        s = sess.processExtendProtocol();
        EXPECT_TRUE(s.ok());
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());

        sess.setArgs({"lrange", "list1", "0", "-1", "103", "103", "v1"});
        s = sess.processExtendProtocol();
        EXPECT_TRUE(s.ok());
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        ss1.str("");
        Command::fmtMultiBulkLen(ss1, 3);
        Command::fmtBulk(ss1, "d");
        Command::fmtBulk(ss1, "b");
        Command::fmtBulk(ss1, "c");
        EXPECT_EQ(ss1.str(), expect.value());
    }
}

void testLockMulti(std::shared_ptr<ServerEntry> svr) {

    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
    NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

    for (int i = 0; i < 100; i++) {
        std::vector<std::string> vec;
        std::vector<int> index;

        for (int j = 0; j < 100; j++) {
            // different string
            vec.emplace_back(randomStr(20, true) + std::to_string(j));
            index.emplace_back(j);
        }

        for (int j = 0; j < 100; j++) {
            auto rng = std::default_random_engine{};
            std::shuffle(vec.begin(), vec.end(), rng);

            auto locklist = svr->getSegmentMgr()->getAllKeysLocked(&sess, vec, index, mgl::LockMode::LOCK_X);
            EXPECT_TRUE(locklist.ok());

            uint32_t id = 0;
            std::string key = "";
            auto list = std::move(locklist.value());
            for (auto& l : list) {
                if (l->getStoreId() == id) {
                    EXPECT_TRUE(l->getKey() > key);
                }

                EXPECT_TRUE(l->getStoreId() >= id);

                key = l->getKey();
                id = l->getStoreId();
            }
        }
    }

}

void testCheckKeyType(std::shared_ptr<ServerEntry> svr) {
    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
    NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

    sess.setArgs({ "sadd", "ss", "a"});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());

    sess.setArgs({ "set", "ss", "b"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());

    sess.setArgs({ "set", "ss1", "b"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
}

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
    std::string cursor = getBulkValue(expect.value(), 0);
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

void testSync(std::shared_ptr<ServerEntry> svr) {
    auto fmtSyncVerRes = [](std::stringstream& ss,
        uint64_t ts, uint64_t ver) {
        ss.str("");
        Command::fmtMultiBulkLen(ss, 2);
        Command::fmtLongLong(ss, ts);
        Command::fmtLongLong(ss, ver);
    };

    asio::io_context ioCtx;
    asio::ip::tcp::socket socket(ioCtx), socket1(ioCtx);
    NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

    sess.setArgs({"syncversion", "unittest", "?", "?", "v1"});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_FALSE(expect.ok());

    sess.setArgs({"syncversion", "unittest", "100", "100", "v1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());

    sess.setArgs({"syncversion", "unittest", "?", "?", "v1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    std::stringstream ss1;
    fmtSyncVerRes(ss1, 100, 100);
    EXPECT_EQ(ss1.str(), expect.value());

   sess.setArgs({"syncversion", "unittest", "105", "102", "v1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());

    sess.setArgs({"syncversion", "unittest", "?", "?", "v1"});
    expect = Command::runSessionCmd(&sess);
    fmtSyncVerRes(ss1, 105, 102);
    EXPECT_EQ(ss1.str(), expect.value());
}

TEST(Command, common) {
    const auto guard = MakeGuard([] {
        destroyEnv();
    });

    EXPECT_TRUE(setupEnv());
    auto cfg = makeServerParam();
    auto server = makeServerEntry(cfg);

    testPf(server);
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
}

TEST(Command, common_scan) {
    const auto guard = MakeGuard([] {
        destroyEnv();
    });

    EXPECT_TRUE(setupEnv());
    auto cfg = makeServerParam();
    auto server = makeServerEntry(cfg);

    testScan(server);
}

TEST(Command, tendisex) {
    const auto guard = MakeGuard([] {
        destroyEnv();
    });

    EXPECT_TRUE(setupEnv());
    auto cfg = makeServerParam();
    // need 420000
    //cfg->chunkSize = 420000;
    auto server = makeServerEntry(cfg);

    testExtendProtocol(server);
    testSync(server);
}

TEST(Command, checkKeyTypeForSetKV) {
    const auto guard = MakeGuard([] {
        destroyEnv();
    });

    EXPECT_TRUE(setupEnv());
    auto cfg = makeServerParam();
    cfg->checkKeyTypeForSet = true;
    auto server = makeServerEntry(cfg);

    testCheckKeyType(server);
    testMset(server);
}

TEST(Command, lockMulti) {
    const auto guard = MakeGuard([] {
        destroyEnv();
    });

    EXPECT_TRUE(setupEnv());
    auto cfg = makeServerParam();
    auto server = makeServerEntry(cfg);

    testLockMulti(server);

}

/*
TEST(Command, keys) {
    const auto guard = MakeGuard([] {
       destroyEnv();
    });

    EXPECT_TRUE(setupEnv());
    auto cfg = makeServerParam();
    auto server = makeServerEntry(cfg);

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
