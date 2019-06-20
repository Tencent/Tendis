// Copyright [2017] <eliotwang>
#include <memory>
#include <utility>

#include "gtest/gtest.h"
#include "glog/logging.h"
#include "tendisplus/server/server_entry.h"
#include "tendisplus/server/index_manager.h"
#include "tendisplus/server/segment_manager.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/network/network.h"
#include "tendisplus/utils/test_util.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/invariant.h"

namespace tendisplus {

uint32_t countTTLIndex(std::shared_ptr<ServerEntry> server,
                       std::shared_ptr<NetSession> session,
                       uint64_t ttl) {
    uint32_t scanned = 0;
    for (uint32_t i = 0; i < server->getKVStoreCount(); ++i) {
        auto expdb = server->getSegmentMgr()->getDb(
            session.get(), i, mgl::LockMode::LOCK_IS);
        EXPECT_TRUE(expdb.ok());
        PStore kvstore = expdb.value().store;
        auto exptxn = kvstore->createTransaction();
        EXPECT_TRUE(exptxn.ok());

        auto txn = std::move(exptxn.value());
        auto until = msSinceEpoch() + ttl * 1000 + 120 * 1000;
        auto cursor = txn->createTTLIndexCursor(until);
        while (true) {
            auto record = cursor->next();
            if (record.ok()) {
                scanned++;
            } else {
                if (record.status().code() != ErrorCodes::ERR_EXHAUST) {
                    LOG(WARNING) << record.status().toString();
                }
                break;
            }
        }
    }

    return scanned;
}

AllKeys initData(std::shared_ptr<ServerEntry>& server,
                uint32_t count) {
    auto ctx1 = std::make_shared<asio::io_context>();
    auto sess1 = makeSession(server, ctx1);
    WorkLoad work(server, sess1);
    work.init();

    AllKeys all_keys;

    auto kv_keys = work.writeWork(RecordType::RT_KV, count);
    all_keys.emplace_back(kv_keys);

    auto list_keys = work.writeWork(RecordType::RT_LIST_META, count, 50);
    all_keys.emplace_back(list_keys);

    auto hash_keys = work.writeWork(RecordType::RT_HASH_META, count, 50);
    all_keys.emplace_back(hash_keys);

    auto set_keys = work.writeWork(RecordType::RT_SET_META, count, 50);
    all_keys.emplace_back(set_keys);

    auto zset_keys = work.writeWork(RecordType::RT_ZSET_META, count, 50);
    all_keys.emplace_back(zset_keys);

    return std::move(all_keys);
}

void waitSlaveCatchup(const std::shared_ptr<ServerEntry>& master,
    const std::shared_ptr<ServerEntry>& slave) {
    auto ctx1 = std::make_shared<asio::io_context>();
    auto sess1 = makeSession(master, ctx1);
    WorkLoad work1(master, sess1);
    work1.init();

    auto ctx2 = std::make_shared<asio::io_context>();
    auto sess2 = makeSession(slave, ctx2);
    WorkLoad work2(master, sess2);
    work2.init();

    INVARIANT(master->getKVStoreCount() == slave->getKVStoreCount());

    for (size_t i = 0; i < master->getKVStoreCount(); i++) {
        auto binlogPos1 = work1.getIntResult({ "binlogpos", std::to_string(i) });
        while (true) {
            auto binlogPos2 = work2.getIntResult({ "binlogpos", std::to_string(i) });
            if (!binlogPos2.ok()) {
                EXPECT_TRUE(binlogPos2.status().code() == ErrorCodes::ERR_EXHAUST);
                EXPECT_TRUE(binlogPos1.status().code() == ErrorCodes::ERR_EXHAUST);
                break;
            }
            if (binlogPos2.value() < binlogPos1.value()) {
                LOG(WARNING) << "store id " << i << " : binlogpos (" << binlogPos1.value()
                    << ">" << binlogPos2.value() << ");";
                std::this_thread::sleep_for(std::chrono::seconds(1));
            } else {
                EXPECT_EQ(binlogPos1.value(), binlogPos2.value());
                break;
            }
        }
    }
}

void compareData(const std::shared_ptr<ServerEntry>& master,
    const std::shared_ptr<ServerEntry>& slave) {
    INVARIANT(master->getKVStoreCount() == slave->getKVStoreCount());

    for (size_t i = 0; i < master->getKVStoreCount(); i++) {
        uint64_t count1 = 0;
        uint64_t count2 = 0;
        auto kvstore1 = master->getStores()[i];
        auto kvstore2 = slave->getStores()[i];

        auto ptxn2 = kvstore2->createTransaction();
        EXPECT_TRUE(ptxn2.ok());
        std::unique_ptr<Transaction> txn2 = std::move(ptxn2.value());

        auto ptxn1 = kvstore1->createTransaction();
        EXPECT_TRUE(ptxn1.ok());
        std::unique_ptr<Transaction> txn1 = std::move(ptxn1.value());
        auto cursor1 = txn1->createCursor();
        cursor1->seek("");
        while (true) {
            Expected<Record> exptRcd1 = cursor1->next();
            if (exptRcd1.status().code() == ErrorCodes::ERR_EXHAUST) {
                break;
            }
            INVARIANT(exptRcd1.ok());
            count1++;

            // check the binlog together
            auto exptRcdv2 = kvstore2->getKV(exptRcd1.value().getRecordKey(), txn2.get());
            EXPECT_TRUE(exptRcdv2.ok());
            EXPECT_EQ(exptRcd1.value().getRecordValue(), exptRcdv2.value());
        }

        auto cursor2 = txn2->createCursor();
        cursor2->seek("");
        while (true) {
            Expected<Record> exptRcd2 = cursor2->next();
            if (exptRcd2.status().code() == ErrorCodes::ERR_EXHAUST) {
                break;
            }
            INVARIANT(exptRcd2.ok());
            count2++;
        }

        EXPECT_EQ(count1, count2);
        LOG(INFO) << "compare data: store " << i << " record count " << count1;
    }
}

void testInsertOrDelKeys(std::shared_ptr<ServerEntry> server,
                        std::shared_ptr<NetSession> session,
                        uint32_t count, uint64_t ttl) {
    uint64_t pttl = msSinceEpoch() + ttl * 1000;

    WorkLoad work(server, session);
    work.init();

    AllKeys all_keys = initData(server, 10);

    // wait slave 

    //work.expireKeys(all_keys, ttl);

    for (auto k : all_keys) {
        work.delKeys(k);
    }

    ASSERT_EQ(0u, countTTLIndex(server, session, pttl));
}

void testScanJobRunning(std::shared_ptr<ServerEntry> server,
                        std::shared_ptr<ServerParams> cfg) {
    SyncPoint::GetInstance()->LoadDependency({
        {"BeforeIndexManagerLoop", "ScanThreadRunning"},
    });

    std::atomic<bool> status = { true };
    SyncPoint::GetInstance()->SetCallBack(
        "BeforeIndexManagerLoop",
        [&](void *arg) {
            auto param = reinterpret_cast<std::atomic<bool>*>(arg);
            status.store(param->load(std::memory_order_relaxed));
        });
    SyncPoint::GetInstance()->EnableProcessing();

    EXPECT_TRUE(filesystem::is_empty("./db"));
    EXPECT_TRUE(filesystem::is_empty("./log"));

    auto s = server->startup(cfg);
    ASSERT_TRUE(s.ok());

    TEST_SYNC_POINT("ScanThreadRunning");
    ASSERT_TRUE(status.load());

    server->stop();

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
}

void testScanIndex(std::shared_ptr<ServerEntry> server,
                   std::shared_ptr<ServerParams> cfg,
                   uint32_t count, uint64_t ttl,
                   bool sharename,
                   uint64_t* totalEnqueue,
                   uint64_t* totalDequeue) {
    SyncPoint::GetInstance()->SetCallBack("InspectTotalEnqueue",
        [&](void *arg) mutable {
            uint64_t *tmp = reinterpret_cast<uint64_t *>(arg);
            *totalEnqueue = *tmp;
        });
    SyncPoint::GetInstance()->SetCallBack("InspectTotalDequeue",
        [&](void *arg) mutable {
            uint64_t *tmp  = reinterpret_cast<uint64_t *>(arg);
            *totalDequeue = *tmp;
        });
    SyncPoint::GetInstance()->EnableProcessing();

    auto s = server->startup(cfg);
    ASSERT_TRUE(s.ok());

    auto ctx = std::make_shared<asio::io_context>();
    auto session = makeSession(server, ctx);

    WorkLoad work(server, session);
    work.init();

    AllKeys keys_written;
    for (auto &type : {
         RecordType::RT_KV,
         RecordType::RT_LIST_META,
         RecordType::RT_HASH_META,
         RecordType::RT_SET_META,
         RecordType::RT_ZSET_META}) {
        auto keys = work.writeWork(type, count,
                   type == RecordType::RT_KV ? 0 : 32, sharename);
        keys_written.emplace_back(keys);
    }

    TEST_SYNC_POINT("BeforeGenerateTTLIndex");
    work.expireKeys(keys_written, ttl);
    TEST_SYNC_POINT("AfterGenerateTTLIndex");

    while (true) {
        auto cnt = countTTLIndex(server, session, ttl);
        if (cnt > 0) {
            LOG(WARNING) << "TTL index count: " << cnt;
            std::this_thread::sleep_for(std::chrono::seconds(5));
        } else {
            break;
        }
    }

    return;
}

void replSimple(
    std::shared_ptr<ServerEntry> master,
    std::shared_ptr<ServerEntry> slave) {

    auto ctx = std::make_shared<asio::io_context>();
    auto session = makeSession(master, ctx);

    WorkLoad work(master, session);
    work.init();

    AllKeys keys_written;
    for (auto &type : {
        RecordType::RT_KV,
        RecordType::RT_LIST_META,
        RecordType::RT_HASH_META,
        RecordType::RT_SET_META,
        RecordType::RT_ZSET_META }) {
        auto keys = work.writeWork(type, 10,
            type == RecordType::RT_KV ? 0 : 32);
        keys_written.emplace_back(keys);
    }

    //TEST_SYNC_POINT("BeforeGenerateTTLIndex");
    //work.expireKeys(keys_written, ttl);
    //TEST_SYNC_POINT("AfterGenerateTTLIndex");

    //while (true) {
    //    auto cnt = countTTLIndex(server, session, ttl);
    //    if (cnt > 0) {
    //        LOG(WARNING) << "TTL index count: " << cnt;
    //        std::this_thread::sleep_for(std::chrono::seconds(5));
    //    }
    //    else {
    //        break;
    //    }
    //}

    return;
}

std::pair<std::shared_ptr<ServerEntry>, std::shared_ptr<ServerEntry>> 
makeReplEnv(uint32_t storeCnt) {
    EXPECT_TRUE(setupReplEnv());

    auto cfg1 = makeServerParam(1111, storeCnt, "master");
    auto cfg2 = makeServerParam(1112, storeCnt, "slave");

    auto master = std::make_shared<ServerEntry>(cfg1);
    auto s = master->startup(cfg1);
    INVARIANT(s.ok());

    auto slave = std::make_shared<ServerEntry>(cfg2);
    s = slave->startup(cfg2);
    INVARIANT(s.ok());
    {
        auto ctx = std::make_shared<asio::io_context>();
        auto session = makeSession(slave, ctx);

        WorkLoad work(slave, session);
        work.init();
        work.slaveof("127.0.0.1", 1111);
    }

    return std::make_pair(master, slave);
}

std::shared_ptr<ServerEntry>
makeAnotherSlave(const std::string& name, uint32_t storeCnt, uint32_t port) {
    INVARIANT(name != "master" && name != "slave");
	INVARIANT(port != 1111 && port != 1112);
    EXPECT_TRUE(setupEnv(name));

    auto cfg1 = makeServerParam(port, storeCnt, name);

    auto slave = std::make_shared<ServerEntry>(cfg1);
    auto s = slave->startup(cfg1);
    INVARIANT(s.ok());

    {
        auto ctx = std::make_shared<asio::io_context>();
        auto session = makeSession(slave, ctx);

        WorkLoad work(slave, session);
        work.init();
        work.slaveof("127.0.0.1", 1111);
    }

    return slave;
}

#ifdef _WIN32 
size_t recordSize = 10;
#else
size_t recordSize = 1000;
#endif // 


TEST(Repl, oneStore) {
    const auto guard = MakeGuard([] {
        destroyReplEnv();
        destroyEnv("slave1");
    });

    auto hosts = makeReplEnv(1);

    auto& master = hosts.first;
    auto& slave = hosts.second;

    auto allKeys = initData(master, recordSize);

    waitSlaveCatchup(master, slave);
    compareData(master, slave);

    auto& slave1 = makeAnotherSlave("slave1", 1, 2111);

    // delete all the keys
    auto ctx1 = std::make_shared<asio::io_context>();
    auto sess1 = makeSession(master, ctx1);
    WorkLoad work(master, sess1);
    work.init();
    for (auto k : allKeys) {
        work.delKeys(k);
    }

    testAll(master);

    waitSlaveCatchup(master, slave);
    compareData(master, slave);

    waitSlaveCatchup(master, slave1);
    compareData(master, slave1);

    master->stop();
    slave->stop();
    slave1->stop();

    ASSERT_EQ(slave.use_count(), 1);
    ASSERT_EQ(master.use_count(), 1);
    ASSERT_EQ(slave1.use_count(), 1);
}

TEST(Repl, MultiStore) {
    const auto guard = MakeGuard([] {
        destroyReplEnv();
    });

    auto hosts = makeReplEnv(10);

    auto& master = hosts.first;
    auto& slave = hosts.second;

    auto allKeys = initData(master, recordSize);

    waitSlaveCatchup(master, slave);
    compareData(master, slave);

    // delete all the keys
    auto ctx1 = std::make_shared<asio::io_context>();
    auto sess1 = makeSession(master, ctx1);
    WorkLoad work(master, sess1);
    work.init();
    for (auto k : allKeys) {
        work.delKeys(k);
    }

    waitSlaveCatchup(master, slave);
    compareData(master, slave);

    master->stop();
    slave->stop();

    ASSERT_EQ(slave.use_count(), 1);
    ASSERT_EQ(master.use_count(), 1);
}

}  // namespace tendisplus

