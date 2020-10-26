// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <stdlib.h>
#include <memory>
#include <utility>
#include <thread>  // NOLINT
#include <string>
#include <vector>
#include <algorithm>

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

static const char master1_dir[] = "migratetest_master1";
static const char master2_dir[] = "migratetest_master2";
uint32_t master1_port = 1131;
uint32_t master2_port = 1132;
uint32_t chunkid1 = 3300;
uint32_t chunkid2 = 15495;


AllKeys initData(const std::shared_ptr<ServerEntry>& server,
                 uint32_t count,
                 const char* key_suffix) {
  auto ctx1 = std::make_shared<asio::io_context>();
  auto sess1 = makeSession(server, ctx1);
  WorkLoad work(server, sess1);
  work.init();

  AllKeys all_keys;

  auto kv_keys = work.writeWork(RecordType::RT_KV, count, 0, true, key_suffix);
  all_keys.emplace_back(kv_keys);

  auto list_keys =
    work.writeWork(RecordType::RT_LIST_META, count, 2, true, key_suffix);
  all_keys.emplace_back(list_keys);

  auto hash_keys =
    work.writeWork(RecordType::RT_HASH_META, count, 2, true, key_suffix);
  all_keys.emplace_back(hash_keys);

  auto set_keys =
    work.writeWork(RecordType::RT_SET_META, count, 2, true, key_suffix);
  all_keys.emplace_back(set_keys);

  auto zset_keys =
    work.writeWork(RecordType::RT_ZSET_META, count, 2, true, key_suffix);
  all_keys.emplace_back(zset_keys);

  return all_keys;
}

void migrate(const std::shared_ptr<ServerEntry>& server1,
             const std::shared_ptr<ServerEntry>& server2,
             uint32_t chunkid) {
  auto ctx1 = std::make_shared<asio::io_context>();
  auto sess1 = makeSession(server1, ctx1);
  // migrating command change,not work here
  /*
  auto expect = server1->getMigrateManager()->migrating(chunkid,
          server2->getParams()->bindIp, server2->getParams()->port);
  EXPECT_TRUE(expect.ok());

  auto ctx2 = std::make_shared<asio::io_context>();
  auto sess2 = makeSession(server2, ctx2);

  args.clear();
  args.push_back("cluster");
  args.push_back("setslot");

  args.push_back("importing");
  std::string nodeName2 =
  server1->getClusterMgr()->getClusterState()->getMyselfName(); LOG(INFO) <<
  "importing nodes:" << nodeName2; args.push_back(nodeName2);
  args.push_back(std::to_string(chunkid));
  sess2->setArgs(args);
  expect = Command::runSessionCmd(sess2.get());

  expect = server2->getMigrateManager()->importing(chunkid,
          server1->getParams()->bindIp, server1->getParams()->port);
  EXPECT_TRUE(expect.ok());
  */
}

void waitMigrateEnd(const std::shared_ptr<ServerEntry>& server1,
                    const std::shared_ptr<ServerEntry>& server2,
                    uint32_t chunkid) {
  std::this_thread::sleep_for(3s);
}

void checkDataMigrated(const std::shared_ptr<ServerEntry>& master,
                 const std::shared_ptr<ServerEntry>& slave) {
  INVARIANT(master->getKVStoreCount() == slave->getKVStoreCount());

  for (size_t i = 0; i < master->getKVStoreCount(); i++) {
    uint64_t count1 = 0;
    uint64_t count2 = 0;
    auto kvstore1 = master->getStores()[i];
    auto kvstore2 = slave->getStores()[i];

    auto ptxn2 = kvstore2->createTransaction(nullptr);
    EXPECT_TRUE(ptxn2.ok());
    std::unique_ptr<Transaction> txn2 = std::move(ptxn2.value());

    auto ptxn1 = kvstore1->createTransaction(nullptr);
    EXPECT_TRUE(ptxn1.ok());
    std::unique_ptr<Transaction> txn1 = std::move(ptxn1.value());
    auto cursor1 = txn1->createAllDataCursor();
    while (true) {
      Expected<Record> exptRcd1 = cursor1->next();
      if (exptRcd1.status().code() == ErrorCodes::ERR_EXHAUST) {
        break;
      }
      INVARIANT(exptRcd1.ok());
      count1++;
      if (exptRcd1.value().getRecordKey().getChunkId() == chunkid1 ||
          exptRcd1.value().getRecordKey().getChunkId() == chunkid2) {
        LOG(ERROR) << "migrate error happen. "
                   << static_cast<int>(
                        exptRcd1.value().getRecordKey().getRecordType())
                   << " " << exptRcd1.value().getRecordKey().getChunkId() << " "
                   << exptRcd1.value().getRecordKey().getPrimaryKey() << " "
                   << exptRcd1.value().getRecordKey().getDbId();
      }
      // LOG(INFO) << "exptRcd1.value().getRecordKey().getChunkId() is:"
      //     << exptRcd1.value().getRecordKey().getChunkId();
      INVARIANT(exptRcd1.value().getRecordKey().getChunkId() != chunkid1);
      INVARIANT(exptRcd1.value().getRecordKey().getChunkId() != chunkid2);

      auto exptRcdv2 =
        kvstore2->getKV(exptRcd1.value().getRecordKey(), txn2.get());
      EXPECT_TRUE(!exptRcdv2.ok());
    }

    auto cursor2 = txn2->createAllDataCursor();
    while (true) {
      Expected<Record> exptRcd2 = cursor2->next();
      if (exptRcd2.status().code() == ErrorCodes::ERR_EXHAUST) {
        break;
      }
      INVARIANT(exptRcd2.ok());
      INVARIANT(exptRcd2.value().getRecordKey().getChunkId() == chunkid1 ||
                exptRcd2.value().getRecordKey().getChunkId() == chunkid2);
      count2++;
    }

    LOG(INFO) << "checkDataMigrated: store " << i << " recordCount1:" << count1
              << " recordCount2:" << count2;
  }
}

std::pair<std::shared_ptr<ServerEntry>, std::shared_ptr<ServerEntry>>
makeMigrateEnv(uint32_t storeCnt) {
  EXPECT_TRUE(setupEnv(master1_dir));
  EXPECT_TRUE(setupEnv(master2_dir));

  auto cfg1 = makeServerParam(master1_port, storeCnt, master1_dir);
  auto cfg2 = makeServerParam(master2_port, storeCnt, master2_dir);
  cfg1->maxBinlogKeepNum = 1;
  cfg2->maxBinlogKeepNum = 1;
  cfg1->minBinlogKeepSec = 0;
  cfg2->minBinlogKeepSec = 0;

  cfg1->clusterEnabled = true;
  cfg2->clusterEnabled = true;

#ifdef _WIN32
  cfg1->executorThreadNum = 1;
  cfg1->netIoThreadNum = 1;
  cfg1->incrPushThreadnum = 1;
  cfg1->fullPushThreadnum = 1;
  cfg1->fullReceiveThreadnum = 1;
  cfg1->logRecycleThreadnum = 1;

  cfg1->migrateSenderThreadnum = 1;
  cfg1->migrateReceiveThreadnum = 1;

  cfg2->executorThreadNum = 1;
  cfg2->netIoThreadNum = 1;
  cfg2->incrPushThreadnum = 1;
  cfg2->fullPushThreadnum = 1;
  cfg2->fullReceiveThreadnum = 1;
  cfg2->logRecycleThreadnum = 1;

  cfg2->migrateSenderThreadnum = 1;
  cfg2->migrateReceiveThreadnum = 1;
#endif

  auto master1 = std::make_shared<ServerEntry>(cfg1);
  auto s = master1->startup(cfg1);
  INVARIANT(s.ok());

  auto master2 = std::make_shared<ServerEntry>(cfg2);
  s = master2->startup(cfg2);
  return std::make_pair(master1, master2);
}


#ifdef _WIN32
size_t recordSize = 10;
#else
size_t recordSize = 10000;
#endif


TEST(Migrate, Common) {
#ifdef _WIN32
  size_t i = 1;
  {
#else
  for (size_t i = 0; i < 2; i++) {
#endif
    LOG(INFO) << ">>>>>> test store count:" << i;

    const auto guard = MakeGuard([] {
      destroyEnv(master1_dir);
      destroyEnv(master2_dir);
      std::this_thread::sleep_for(std::chrono::seconds(1));
    });

    auto hosts = makeMigrateEnv(i);
    auto& master1 = hosts.first;
    auto& master2 = hosts.second;

    auto allKeys1 = initData(master1, recordSize, "suffix1");
    LOG(INFO) << ">>>>>> master1 initData 1st end;";
    migrate(master1, master2, chunkid1);
    migrate(master1, master2, chunkid2);
    // auto allKeys2 = initData(master1, recordSize, "suffix2");
    waitMigrateEnd(master1, master2, chunkid1);
    waitMigrateEnd(master1, master2, chunkid2);
    LOG(INFO) << ">>>>>> waitMigrateEnd success;";
    checkDataMigrated(master1, master2);  // check data + binlog
    LOG(INFO) << ">>>>>> checkDataMigrated 1st end;";

#ifndef _WIN32
    master1->stop();
    master2->stop();
    ASSERT_EQ(master1.use_count(), 1);
    ASSERT_EQ(master2.use_count(), 1);
#endif
    LOG(INFO) << ">>>>>> test store count:" << i << " end;";
  }
}
#ifdef _WIN32
uint32_t storeCnt = 2;
#else
uint32_t storeCnt = 2;
#endif  //


std::shared_ptr<ServerEntry> makeClusterNode(const std::string& dir,
                                             uint32_t port,
                                             uint32_t storeCnt = 10) {
  auto mDir = dir;
  auto mport = port;
  EXPECT_TRUE(setupEnv(mDir));

  auto cfg1 = makeServerParam(mport, storeCnt, mDir);
  cfg1->clusterEnabled = true;

#ifdef _WIN32
  cfg1->executorThreadNum = 1;
  cfg1->netIoThreadNum = 1;
  cfg1->incrPushThreadnum = 1;
  cfg1->fullPushThreadnum = 1;
  cfg1->fullReceiveThreadnum = 1;
  cfg1->logRecycleThreadnum = 1;

  cfg1->migrateSenderThreadnum = 1;
  cfg1->migrateReceiveThreadnum = 1;
#endif

  auto master = std::make_shared<ServerEntry>(cfg1);
  auto s = master->startup(cfg1);
  INVARIANT(s.ok());

  return master;
}

}  // namespace tendisplus
