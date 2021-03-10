// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

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

namespace tendisplus {

uint32_t countTTLIndex(std::shared_ptr<ServerEntry> server,
                       std::shared_ptr<NetSession> session,
                       uint64_t ttl) {
  uint32_t scanned = 0;
  for (uint32_t i = 0; i < server->getKVStoreCount(); ++i) {
    auto expdb =
      server->getSegmentMgr()->getDb(session.get(), i, mgl::LockMode::LOCK_IS);
    EXPECT_TRUE(expdb.ok());
    PStore kvstore = expdb.value().store;
    auto exptxn = kvstore->createTransaction(session.get());
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

void testCreateDelIndex(std::shared_ptr<ServerEntry> server,
                        std::shared_ptr<NetSession> session,
                        uint32_t count,
                        uint64_t ttl) {
  uint64_t pttl = msSinceEpoch() + ttl * 1000;

  WorkLoad work(server, session);
  work.init();

  AllKeys all_keys;

  auto kv_keys = work.writeWork(RecordType::RT_KV, count);
  all_keys.emplace_back(kv_keys);

  auto list_keys = work.writeWork(RecordType::RT_LIST_META, count, 10);
  all_keys.emplace_back(list_keys);

  auto hash_keys = work.writeWork(RecordType::RT_HASH_META, count, 10);
  all_keys.emplace_back(hash_keys);

  auto set_keys = work.writeWork(RecordType::RT_SET_META, count, 10);
  all_keys.emplace_back(set_keys);

  auto zset_keys = work.writeWork(RecordType::RT_ZSET_META, count, 10);
  all_keys.emplace_back(zset_keys);

  work.expireKeys(all_keys, ttl);

  work.delKeys(kv_keys);
  work.delKeys(list_keys);
  work.delKeys(hash_keys);
  work.delKeys(set_keys);
  work.delKeys(zset_keys);

  ASSERT_EQ(0u, countTTLIndex(server, session, pttl));
}

void testScanJobRunning(std::shared_ptr<ServerEntry> server,
                        std::shared_ptr<ServerParams> cfg) {
  SyncPoint::GetInstance()->LoadDependency({
    {"BeforeIndexManagerLoop", "ScanThreadRunning"},
  });

  std::atomic<bool> status = {true};
  SyncPoint::GetInstance()->SetCallBack(
    "BeforeIndexManagerLoop", [&](void* arg) {
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
                   uint32_t count,
                   uint64_t ttl,
                   bool sharename,
                   uint64_t* totalEnqueue,
                   uint64_t* totalDequeue) {
  SyncPoint::GetInstance()->SetCallBack(
    "InspectTotalEnqueue", [&](void* arg) mutable {
      uint64_t* tmp = reinterpret_cast<uint64_t*>(arg);
      *totalEnqueue = *tmp;
    });
  SyncPoint::GetInstance()->SetCallBack(
    "InspectTotalDequeue", [&](void* arg) mutable {
      uint64_t* tmp = reinterpret_cast<uint64_t*>(arg);
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
  for (auto& type : {RecordType::RT_KV,
                     RecordType::RT_LIST_META,
                     RecordType::RT_HASH_META,
                     RecordType::RT_SET_META,
                     RecordType::RT_ZSET_META}) {
    auto keys = work.writeWork(
      type, count, type == RecordType::RT_KV ? 0 : 32, sharename);
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

TEST(IndexManager, generateIndex) {
  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());

  auto cfg = makeServerParam();
  auto server = std::make_shared<ServerEntry>(cfg);
  auto s = server->startup(cfg);
  ASSERT_TRUE(s.ok());

  {
    auto ctx = std::make_shared<asio::io_context>();
    auto session = makeSession(server, ctx);

    testCreateDelIndex(server, session, 1024 * 4, 1000);
  }

  server->stop();
  ASSERT_EQ(server.use_count(), 1);
}


TEST(IndexManager, changePauseTime) {
  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());

  auto cfg = makeServerParam();
  cfg->pauseTimeIndexMgr = 1;
  auto server = std::make_shared<ServerEntry>(cfg);
  auto s = server->startup(cfg);
  ASSERT_TRUE(s.ok());

  {
    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext);
    NetSession sess(server, std::move(socket), 1, false, nullptr, nullptr);

    sess.setArgs({"config", "set", "pauseTimeIndexMgr", "1000000"});
    auto expect = Command::runSessionCmd(&sess);
    auto val = expect.value();
    EXPECT_TRUE(expect.ok());

    sess.setArgs({"sadd", "a", "b", "c", "d"});
    expect = Command::runSessionCmd(&sess);
    val = expect.value();
    EXPECT_TRUE(expect.ok());

    sess.setArgs({"pexpire", "a", "10"});
    expect = Command::runSessionCmd(&sess);
    val = expect.value();
    EXPECT_TRUE(expect.ok());

    std::this_thread::sleep_for(std::chrono::seconds(10));

    // index manager is sleeping, scanExpiredCount == 0
    EXPECT_EQ(server->getIndexMgr()->scanExpiredCount(), 0);

    // wake up index manager
    sess.setArgs({"config", "set", "pauseTimeIndexMgr", "1"});
    expect = Command::runSessionCmd(&sess);
    val = expect.value();
    EXPECT_TRUE(expect.ok());

    std::this_thread::sleep_for(std::chrono::seconds(10));
    EXPECT_EQ(server->getIndexMgr()->scanExpiredCount(), 1);
  }

  server->stop();
  ASSERT_EQ(server.use_count(), 1);
}

TEST(IndexManager, scanJobRunning) {
  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());

  auto cfg = makeServerParam();
  auto server = std::make_shared<ServerEntry>(cfg);

  testScanJobRunning(server, cfg);

  ASSERT_EQ(server.use_count(), 1);
}

TEST(IndexManager, scanIndexAfterExpire) {
  uint64_t totalDequeue = 0;
  uint64_t totalEnqueue = 0;

  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());

  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->LoadDependency({
    {"AfterGenerateTTLIndex", "BeforeIndexManagerLoop"},
  });

  auto cfg = makeServerParam();
  auto server = std::make_shared<ServerEntry>(cfg);

  testScanIndex(server, cfg, 2048 * 4, 10, true, &totalEnqueue, &totalDequeue);

  server->stop();

  ASSERT_EQ(totalDequeue, 2048 * 4 * 4u);
  ASSERT_EQ(totalEnqueue, 2048 * 4 * 4u);

  ASSERT_EQ(server.use_count(), 1);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST(IndexManager, scanIndexWhileExpire) {
  uint64_t totalDequeue = 0;
  uint64_t totalEnqueue = 0;

  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());

  SyncPoint::GetInstance()->LoadDependency({
    {"BeforeGenerateTTLIndex", "BeforeIndexManagerLoop"},
  });

  auto cfg = makeServerParam();
  auto server = std::make_shared<ServerEntry>(cfg);

  testScanIndex(server, cfg, 2048 * 4, 1, true, &totalEnqueue, &totalDequeue);

  server->stop();

  ASSERT_EQ(totalDequeue, 2048 * 4 * 4u);
  ASSERT_EQ(totalEnqueue, 2048 * 4 * 4u);

  ASSERT_EQ(server.use_count(), 1);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST(IndexManager, singleJobRunning) {
  uint64_t totalDequeue = 0;
  uint64_t totalEnqueue = 0;

  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());

  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->LoadDependency({
    {"BeforeGenerateTTLIndex", "BeforeIndexManagerLoop"},
  });

  SyncPoint::GetInstance()->SetCallBack("InspectScanJobCnt", [&](void* arg) {
    auto scanJobCnt = reinterpret_cast<std::atomic<uint32_t>*>(arg);
    ASSERT_EQ(*scanJobCnt, 1);
  });

  SyncPoint::GetInstance()->SetCallBack("InspectDelJobCnt", [&](void* arg) {
    auto delJobCnt = reinterpret_cast<std::atomic<uint32_t>*>(arg);
    ASSERT_EQ(*delJobCnt, 1);
  });

  auto cfg = makeServerParam();
  cfg->scanCntIndexMgr = 1000;
  cfg->scanJobCntIndexMgr = 8;
  cfg->delCntIndexMgr = 2000;
  cfg->delJobCntIndexMgr = 8;
  cfg->pauseTimeIndexMgr = 1;
  bool running = true;


  auto server = std::make_shared<ServerEntry>(cfg);

  std::thread thd0([&server, &running]() {
    while (running) {
      auto s = runCommand(server, {"info", "indexmanager"});
      LOG(INFO) << s;

      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  testScanIndex(server, cfg, 2048 * 4, 1, true, &totalEnqueue, &totalDequeue);
  running = false;
  thd0.join();

  server->stop();

  ASSERT_EQ(totalEnqueue, 2048 * 4 * 4u);
  ASSERT_EQ(totalDequeue, 2048 * 4 * 4u);

  ASSERT_EQ(server.use_count(), 1);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}
}  // namespace tendisplus
