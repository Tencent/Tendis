#include <fstream>
#include <utility>
#include <memory>
#include <vector>
#include "gtest/gtest.h"
#include "tendisplus/utils/status.h"
#include "tendisplus/utils/scopeguard.h"
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
        EXPECT_EQ(expect.value(), Command::fmtBulk(std::to_string(2*i)));

        sess.setArgs({"llen", "a"});
        expect = Command::runSessionCmd(&sess);
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtLongLong(i+1));
    }
}

void testHash(std::shared_ptr<ServerEntry> svr) {
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
}


void testSet(std::shared_ptr<ServerEntry> svr) {
    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext);
    NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);
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
            sess.setArgs({"hset", "a", std::to_string(2*i)});
            auto expect = Command::runSessionCmd(&sess);
            EXPECT_TRUE(expect.ok());
            EXPECT_EQ(expect.value(), Command::fmtLongLong(i+1));
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

    testSet(server);
    testSetRetry(server);
    testHash(server);
    testList(server);
}

}  // namespace tendisplus
