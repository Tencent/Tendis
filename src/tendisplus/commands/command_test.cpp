// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

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
#include "tendisplus/utils/invariant.h"

namespace tendisplus {

Expected<std::string> recordList2Aof(const std::list<Record>& list);
Expected<std::string> key2Aof(Session* sess, const std::string& key);

void testSetRetry(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);
  NetSession sess1(svr, std::move(socket1), 1, false, nullptr, nullptr);

  uint32_t cnt = 0;
  const auto guard =
    MakeGuard([] { SyncPoint::GetInstance()->ClearAllCallBacks(); });
  SyncPoint::GetInstance()->EnableProcessing();
  SyncPoint::GetInstance()->SetCallBack("setGeneric::SetKV::1", [&](void* arg) {
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
      sess.setArgs({"lpush", "a", std::to_string(2 * i)});
      auto expect = Command::runSessionCmd(&sess);
      EXPECT_TRUE(expect.ok());
      EXPECT_EQ(expect.value(), Command::fmtLongLong(i + 1));
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
      sess.setArgs({"lpush", "a", std::to_string(2 * i)});
      auto expect = Command::runSessionCmd(&sess);
      EXPECT_TRUE(expect.ok());
      EXPECT_EQ(expect.value(), Command::fmtLongLong(i + 1));
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
    sess.setArgs({"zadd", "testzsetdel", std::to_string(i), std::to_string(i)});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
  }
  const auto guard =
    MakeGuard([] { SyncPoint::GetInstance()->ClearAllCallBacks(); });
  std::cout << "begin delete zset" << std::endl;
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
  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());
  auto cfg = makeServerParam();
  auto server = makeServerEntry(cfg);

  testDel(server);

#ifndef _WIN32
  server->stop();
  EXPECT_EQ(server.use_count(), 1);
#endif
}

TEST(Command, expire) {
  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());

  auto cfg = makeServerParam();
  auto server = makeServerEntry(cfg);

  testExpire(server);
  testExpire1(server);
  testExpire2(server);
  testExpireCommandWhenNoexpireTrue(server);
  testExpireKeyWhenGet(server);
  testExpireKeyWhenCompaction(server);

#ifndef _WIN32
  server->stop();
  EXPECT_EQ(server.use_count(), 1);
#endif
}

void testExtendProtocol(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  sess.setArgs({"config", "set", "session", "tendis_protocol_extend", "1"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"sadd", "ss", "a", "100", "100", "v1"});
  auto s = sess.processExtendProtocol();
  EXPECT_TRUE(s.ok());
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(sess.getServerEntry()->getTsEp(), 100);

  sess.setArgs({"sadd", "ss", "b", "101", "101", "v1"});
  s = sess.processExtendProtocol();
  EXPECT_TRUE(s.ok());
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(sess.getServerEntry()->getTsEp(), 101);

  sess.setArgs({"sadd", "ss", "c", "102", "a", "v1"});
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

    sess.setArgs({"lpush", "list1", "c", "104", "104", "v1"});
    s = sess.processExtendProtocol();
    EXPECT_TRUE(s.ok());
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());

    sess.setArgs({"rpush", "list1", "d", "105", "105", "v1"});
    s = sess.processExtendProtocol();
    EXPECT_TRUE(s.ok());
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());

    sess.setArgs({"linsert", "list1", "after", "c", "f", "106", "106", "v1"});
    s = sess.processExtendProtocol();
    EXPECT_TRUE(s.ok());
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());

    sess.setArgs({"linsert", "list1", "before", "d", "e", "107", "107", "v1"});
    s = sess.processExtendProtocol();
    EXPECT_TRUE(s.ok());
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());

    sess.setArgs({"lrange", "list1", "0", "-1", "108", "108", "v1"});
    s = sess.processExtendProtocol();
    EXPECT_TRUE(s.ok());
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    ss1.str("");
    Command::fmtMultiBulkLen(ss1, 7);
    Command::fmtBulk(ss1, "c");
    Command::fmtBulk(ss1, "f");
    Command::fmtBulk(ss1, "e");
    Command::fmtBulk(ss1, "d");
    Command::fmtBulk(ss1, "b");
    Command::fmtBulk(ss1, "c");
    Command::fmtBulk(ss1, "d");
    EXPECT_EQ(ss1.str(), expect.value());
  }
}

void testLockMulti(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  for (int i = 0; i < 10; i++) {
    std::vector<std::string> vec;
    std::vector<int> index;

    LOG(INFO) << "testLockMulti " << i;

    for (int j = 0; j < 100; j++) {
      // different string
      vec.emplace_back(randomStr(20, true) + std::to_string(j));
      index.emplace_back(j);
    }

    for (int j = 0; j < 100; j++) {
      auto rng = std::default_random_engine{};
      std::shuffle(vec.begin(), vec.end(), rng);

      auto locklist = svr->getSegmentMgr()->getAllKeysLocked(
        &sess, vec, index, mgl::LockMode::LOCK_X);
      EXPECT_TRUE(locklist.ok());

      uint32_t id = 0;
      uint32_t chunkid = 0;
      std::string key = "";
      auto list = std::move(locklist.value());
      for (auto& l : list) {
        if (l->getStoreId() == id) {
          EXPECT_TRUE(l->getChunkId() >= chunkid);
          if (l->getChunkId() == chunkid) {
            EXPECT_TRUE(l->getKey() > key);
          }
        }

        EXPECT_TRUE(l->getStoreId() >= id);

        key = l->getKey();
        id = l->getStoreId();
        chunkid = l->getChunkId();
      }
    }
  }
}

void testCheckKeyType(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  sess.setArgs({"sadd", "ss", "a"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"set", "ss", "b"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"set", "ss1", "b"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
}

void testScan(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  sess.setArgs({"sadd",
                "scanset",
                "a",
                "b",
                "c",
                "d",
                "e",
                "f",
                "g",
                "h",
                "i",
                "j",
                "k",
                "l",
                "m",
                "n",
                "o"});
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

void testMulti(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioCtx;
  asio::ip::tcp::socket socket(ioCtx), socket1(ioCtx);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  sess.setArgs({"config", "set", "session", "tendis_protocol_extend", "1"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"hset", "multitest", "initkey", "initval", "1", "1", "v1"});
  auto s = sess.processExtendProtocol();
  EXPECT_TRUE(s.ok());
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  // Command with version equal to key is not allowed to perform.
  sess.setArgs({"hset", "multitest", "dupver", "dupver", "1", "1", "v1"});
  s = sess.processExtendProtocol();
  EXPECT_TRUE(s.ok());
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(!expect.ok());

  sess.setArgs({"multi", "2", "2", "v1"});
  s = sess.processExtendProtocol();
  EXPECT_TRUE(s.ok());
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  // Out multi/exec doesn't behaviour like what redis does.
  // each command between multi and exec will be executed immediately.
  sess.setArgs({"hset", "multitest", "multi1", "multi1", "2", "2", "v1"});
  s = sess.processExtendProtocol();
  EXPECT_TRUE(s.ok());
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"hset", "multitest", "multi2", "multi2", "2", "2", "v1"});
  s = sess.processExtendProtocol();
  EXPECT_TRUE(s.ok());
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"hset", "multitest", "multi3", "multi3", "2", "2", "v1"});
  s = sess.processExtendProtocol();
  EXPECT_TRUE(s.ok());
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  // Exec will just return ok, no array reply.
  sess.setArgs({"exec", "2", "2", "v1"});
  s = sess.processExtendProtocol();
  EXPECT_TRUE(s.ok());
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"multi", "3", "3", "v1"});
  s = sess.processExtendProtocol();
  EXPECT_TRUE(s.ok());
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"hset", "multitest", "multi4", "multi4", "3", "3", "v1"});
  s = sess.processExtendProtocol();
  EXPECT_TRUE(s.ok());
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  // version check: exec with version not same as txn will fail.
  sess.setArgs({"exec", "4", "4", "v1"});
  s = sess.processExtendProtocol();
  EXPECT_TRUE(s.ok());
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(!expect.ok());
}

void testMaxClients(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);
  uint32_t i = 30;
  sess.setArgs({"config", "get", "maxclients"});
  auto expect = Command::runSessionCmd(&sess);
  std::stringstream ss;
  Command::fmtMultiBulkLen(ss, 2);
  Command::fmtBulk(ss, "maxclients");
  Command::fmtBulk(ss, "10000");
  EXPECT_EQ(ss.str(), expect.value());
  ss.clear();
  ss.str("");

  sess.setArgs({"config", "set", "maxclients", std::to_string(i)});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"config", "get", "maxclients"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  Command::fmtMultiBulkLen(ss, 2);
  Command::fmtBulk(ss, "maxclients");
  Command::fmtBulk(ss, std::to_string(i));
  EXPECT_EQ(ss.str(), expect.value());
  ss.clear();
  ss.str("");

  sess.setArgs({"config", "set", "masterauth", "testauth"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  sess.setArgs({"config", "get", "masterauth"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  Command::fmtMultiBulkLen(ss, 2);
  Command::fmtBulk(ss, "masterauth");
  Command::fmtBulk(ss, "testauth");
  EXPECT_EQ(ss.str(), expect.value());
}

void testSlowLog(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  uint32_t i = 0;
  sess.setArgs({"config", "set", "slowlog-log-slower-than", std::to_string(i)});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"sadd", "ss", "a"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"set", "ss", "b"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"set", "ss1", "b"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"config", "get", "slowlog-log-slower-than"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"config", "set", "slowlog-file-enabled", "0"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"set", "ss2", "a"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"config", "set", "slowlog-file-enabled", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"set", "ss2", "b"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
}

void testGlobStylePattern(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  sess.setArgs({"config", "set", "slowlog-flush-interval", "1"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"config", "set", "slowlog-log-slower-than", "100000"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"config", "set", "slowlog-max-len", "1024"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"config", "get", "*slow*"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_EQ(
    "*10\r\n$7\r\nslowlog\r\n$11\r\n\"./"
    "slowlog\"\r\n$20\r\nslowlog-file-enabled\r\n$3\r\nyes\r\n$"
    "22\r\nslowlog-"
    "flush-interval\r\n$1\r\n1\r\n$23\r\nslowlog-log-slower-than\r\n$"
    "6\r\n100000\r\n$15\r\nslowlog-max-len\r\n$4\r\n1024\r\n",
    expect.value());

  sess.setArgs({"config", "get", "?lowlog"});
  expect = Command::runSessionCmd(&sess);
  std::stringstream ss;
  Command::fmtMultiBulkLen(ss, 2);
  Command::fmtBulk(ss, "slowlog");
  Command::fmtBulk(ss, "\"./slowlog\"");
  EXPECT_EQ(ss.str(), expect.value());

  sess.setArgs({"config", "get", "no_exist_key"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_EQ(Command::fmtZeroBulkLen(), expect.value());

  sess.setArgs({"config", "get", "a", "b"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(!expect.ok());
}

void testConfigRewrite(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  sess.setArgs({"config", "set", "slowlog-flush-interval", "2000"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"config", "set", "maxbinlogkeepnum", "1500000"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"config", "rewrite"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  auto confile = svr->getParams()->getConfFile();
  std::ifstream file(confile);
  if (!file.is_open()) {
    EXPECT_TRUE(0);
  }
  std::string line;
  std::string text;
  std::vector<std::string> tokens;
  bool find1 = false;
  bool find2 = false;
  try {
    line.clear();
    while (std::getline(file, line)) {
      line = trim(line);
      if (line == "slowlog-flush-interval 2000") {
        // modify data
        text += "slowlog-flush-interval 3000\n";
      } else {
        text += line + "\n";
      }
      if (line.size() == 0 || line[0] == '#') {
        continue;
      }
      std::stringstream ss(line);
      tokens.clear();
      std::string tmp;
      while (std::getline(ss, tmp, ' ')) {
        tokens.emplace_back(tmp);
      }
      if (tokens.size() == 2) {
        if (tokens[0] == "slowlog-flush-interval" && tokens[1] == "2000") {
          find1 = true;
        } else if (tokens[0] == "maxbinlogkeepnum" && tokens[1] == "1500000") {
          find2 = true;
        }
      }
    }
  } catch (const std::exception& ex) {
    EXPECT_TRUE(0);
    return;
  }
  EXPECT_TRUE(find1);
  EXPECT_TRUE(find2);
  file.close();

  ofstream out;
  out.open(confile);
  out.flush();
  out << text;
  out.close();

  sess.setArgs({"config", "rewrite"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  bool correct = false;
  std::ifstream check(confile);
  if (!check.is_open()) {
    EXPECT_TRUE(0);
  }
  try {
    line.clear();
    while (std::getline(check, line)) {
      line = trim(line);
      if (line.size() == 0 || line[0] == '#') {
        continue;
      }
      std::stringstream ss(line);
      tokens.clear();
      std::string tmp;
      while (std::getline(ss, tmp, ' ')) {
        tokens.emplace_back(tmp);
      }
      if (tokens.size() == 2) {
        if (tokens[0] == "slowlog-flush-interval" && tokens[1] == "2000") {
          correct = true;
          break;
        }
      }
    }
  } catch (const std::exception& ex) {
    EXPECT_TRUE(0);
    return;
  }
  EXPECT_TRUE(correct);
  check.close();
}

void testCommand(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  sess.setArgs({"command", "getkeys", "set", "a", "b"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(
    "*1\r\n"
    "$1\r\na\r\n",
    expect.value());

  sess.setArgs({"COMMAND", "GETKEYS", "SET", "a", "b"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(
    "*1\r\n"
    "$1\r\na\r\n",
    expect.value());
}

void testObject(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  sess.setArgs({"set", "a", "b"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"object", "encoding", "a"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"OBJECT", "ENCODING", "a"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
}

TEST(Command, common) {
  const auto guard = MakeGuard([] { destroyEnv(); });

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

#ifndef _WIN32
  server->stop();
  EXPECT_EQ(server.use_count(), 1);
#endif
}

TEST(Command, common_scan) {
  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());
  auto cfg = makeServerParam();
  auto server = makeServerEntry(cfg);

  testScan(server);

#ifndef _WIN32
  server->stop();
  EXPECT_EQ(server.use_count(), 1);
#endif
}

TEST(Command, tendisex) {
  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());
  auto cfg = makeServerParam();
  // need 420000
  // cfg->chunkSize = 420000;
  auto server = makeServerEntry(cfg);

  testExtendProtocol(server);
  testSync(server);
  testMulti(server);

#ifndef _WIN32
  server->stop();
  EXPECT_EQ(server.use_count(), 1);
#endif
}

TEST(Command, checkKeyTypeForSetKV) {
  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());
  auto cfg = makeServerParam();
  cfg->checkKeyTypeForSet = true;
  auto server = makeServerEntry(cfg);

  testCheckKeyType(server);
  testMset(server);

#ifndef _WIN32
  server->stop();
  EXPECT_EQ(server.use_count(), 1);
#endif
}

TEST(Command, lockMulti) {
  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());
  auto cfg = makeServerParam();
  auto server = makeServerEntry(cfg);

  testHash2(server);
  testLockMulti(server);

#ifndef _WIN32
  server->stop();
  EXPECT_EQ(server.use_count(), 1);
#endif
}

TEST(Command, maxClients) {
  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());
  auto cfg = makeServerParam();
  auto server = makeServerEntry(cfg);

  testMaxClients(server);

#ifndef _WIN32
  server->stop();
  EXPECT_EQ(server.use_count(), 1);
#endif
}

#ifndef _WIN32
TEST(Command, slowlog) {
  const auto guard = MakeGuard([] { destroyEnv(); });
  char line[100];
  FILE* fp;
  std::string clear =
    "echo "
    " > ./slowlogtest";
  const char* clearCommand = clear.data();
  if ((fp = popen(clearCommand, "r")) == NULL) {
    std::cout << "error" << std::endl;
    return;
  }

  {
    EXPECT_TRUE(setupEnv());
    auto cfg = makeServerParam();
    cfg->slowlogPath = "slowlogtest";
    auto server = makeServerEntry(cfg);

    testSlowLog(server);

#ifndef _WIN32
    server->stop();
    EXPECT_EQ(server.use_count(), 1);
#endif
  }


  std::string cmd = "grep -Ev '^$|[#;]' ./slowlogtest";
  const char* sysCommand = cmd.data();
  if ((fp = popen(sysCommand, "r")) == NULL) {
    std::cout << "error" << std::endl;
    return;
  }

  char *ptr;
  ptr = fgets(line, sizeof(line) - 1, fp);
  EXPECT_STRCASEEQ(line, "[] config set slowlog-log-slower-than 0 \n");
  ptr = fgets(line, sizeof(line) - 1, fp);
  EXPECT_STRCASEEQ(line, "[] sadd ss a \n");
  ptr = fgets(line, sizeof(line) - 1, fp);
  EXPECT_STRCASEEQ(line, "[] set ss b \n");
  ptr = fgets(line, sizeof(line) - 1, fp);
  EXPECT_STRCASEEQ(line, "[] set ss1 b \n");
  ptr = fgets(line, sizeof(line) - 1, fp);
  EXPECT_STRCASEEQ(line, "[] config get slowlog-log-slower-than \n");
  ptr = fgets(line, sizeof(line) - 1, fp);
  EXPECT_STRCASEEQ(line, "[] config set slowlog-file-enabled 1 \n");
  ptr = fgets(line, sizeof(line) - 1, fp);
  EXPECT_STRCASEEQ(line, "[] set ss2 b \n");
  pclose(fp);
}
#endif  // !

TEST(Command, testGlobStylePattern) {
  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());
  auto cfg = makeServerParam();
  auto server = makeServerEntry(cfg);

  testGlobStylePattern(server);

#ifndef _WIN32
  server->stop();
  EXPECT_EQ(server.use_count(), 1);
#endif
}

TEST(Command, testConfigRewrite) {
  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());

  auto cfg = makeServerParam();
  auto server = makeServerEntry(cfg);

  testConfigRewrite(server);

  remove(cfg->getConfFile().c_str());

#ifndef _WIN32
  server->stop();
  EXPECT_EQ(server.use_count(), 1);
#endif
}

TEST(Command, testCommand) {
  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());

  auto cfg = makeServerParam();
  auto server = makeServerEntry(cfg);

  testCommand(server);

  remove(cfg->getConfFile().c_str());

#ifndef _WIN32
  server->stop();
  EXPECT_EQ(server.use_count(), 1);
#endif
}

TEST(Command, testObject) {
  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());

  auto cfg = makeServerParam();
  auto server = makeServerEntry(cfg);

  testObject(server);

  remove(cfg->getConfFile().c_str());

#ifndef _WIN32
  server->stop();
  EXPECT_EQ(server.use_count(), 1);
#endif
}

void testRenameCommand(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  sess.setArgs({"set"});
  auto eprecheck = Command::precheck(&sess);
  EXPECT_EQ(Command::fmtErr("unknown command 'set'"),
            eprecheck.status().toString());

  sess.setArgs({"set_rename", "a", "1"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_EQ(Command::fmtOK(), expect.value());

  sess.setArgs({"dbsize"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(Command::fmtLongLong(0), expect.value());

  sess.setArgs({"keys"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  std::stringstream ss;
  Command::fmtMultiBulkLen(ss, 0);
  EXPECT_EQ(ss.str(), expect.value());
}

void testTendisadminSleep(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext, ioContext2;
  asio::ip::tcp::socket socket(ioContext), socket2(ioContext2);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);
  NetSession sess2(svr, std::move(socket2), 1, false, nullptr, nullptr);

  int i = 4;
  std::thread thd1([&sess2, &i]() {
    uint32_t now = msSinceEpoch();
    sess2.setArgs({"tendisadmin", "sleep", std::to_string(i)});
    auto expect = Command::runSessionCmd(&sess2);
    auto val = expect.value();
    EXPECT_TRUE(expect.ok());
    uint32_t end = msSinceEpoch();
    EXPECT_TRUE(end - now > (unsigned)(i - 1) * 1000);
  });

  std::this_thread::sleep_for(std::chrono::seconds(1));

  std::thread thd2([&svr, &i]() {
    uint32_t now = msSinceEpoch();
    runCommand(svr, {"ping"});
    runCommand(svr, {"info"});
    runCommand(svr, {"info", "replication"});
    runCommand(svr, {"info", "all"});
    uint32_t end = msSinceEpoch();

    EXPECT_LT(end - now, 500);
    LOG(INFO) << "info used " << end - now
              << "ms when running tendisadmin sleep ";
  });

  sess.setArgs({"set", "a", "b"});
  uint32_t now = msSinceEpoch();
  auto expect = Command::runSessionCmd(&sess);

  EXPECT_TRUE(expect.ok());
  uint32_t end = msSinceEpoch();
  EXPECT_TRUE(end - now > (unsigned)(i - 2) * 1000);
  thd1.join();
  thd2.join();
}

void testDbEmptyCommand(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext, ioContext2;
  asio::ip::tcp::socket socket(ioContext), socket2(ioContext2);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);
  NetSession sess2(svr, std::move(socket2), 1, false, nullptr, nullptr);

  sess.setArgs({"set", "key", "value"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess2.setArgs({"dbempty"});
  expect = Command::runSessionCmd(&sess2);
  std::stringstream ss;
  Command::fmtMultiBulkLen(ss, 0);
  EXPECT_EQ(ss.str(), expect.value());

  sess.setArgs({"del", "key"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess2.setArgs({"dbempty"});
  expect = Command::runSessionCmd(&sess2);
  std::stringstream ss2;
  Command::fmtMultiBulkLen(ss2, 1);
  EXPECT_EQ(ss2.str(), expect.value());
}

void testCommandCommand(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  sess.setArgs({"command"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  auto& cmdmap = commandMap();
  sess.setArgs({"command", "count"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(Command::fmtLongLong(cmdmap.size()), expect.value());

  sess.setArgs({"command", "set"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(Command::fmtLongLong(cmdmap.size()), expect.value());


  sess.setArgs({"keys"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  std::stringstream ss;
  Command::fmtMultiBulkLen(ss, 0);
  EXPECT_EQ(ss.str(), expect.value());
}

TEST(Command, TendisadminCommand) {
  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());
  auto cfg = makeServerParam();
  auto server = makeServerEntry(cfg);

  testTendisadminSleep(server);

#ifndef _WIN32
  server->stop();
  EXPECT_EQ(server.use_count(), 1);
#endif
}


void testDelTTLIndex(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext, ioContext2;
  asio::ip::tcp::socket socket(ioContext), socket2(ioContext2);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  sess.setArgs({"zadd", "zset1", "10", "a"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"expire", "zset1", "3"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_EQ(Command::fmtLongLong(1), expect.value());

  sess.setArgs({"zrem", "zset1", "a"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_EQ(Command::fmtLongLong(1), expect.value());

  {
    sess.setArgs({"sadd", "set2", "three"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());

    sess.setArgs({"expire", "set2", "3"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_EQ(Command::fmtLongLong(1), expect.value());

    sess.setArgs({"srem", "set2", "three"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_EQ(Command::fmtLongLong(1), expect.value());
  }

  {
    sess.setArgs({"sadd", "set1", "one"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());

    sess.setArgs({"expire", "set1", "3"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_EQ(Command::fmtLongLong(1), expect.value());

    sess.setArgs({"spop", "set1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
  }

  {
    // srem not exist key
    sess.setArgs({"srem", "setxxx", "three"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_EQ(Command::fmtZero(), expect.value());

    // srem expire key
    sess.setArgs({"sadd", "setxxx1", "one"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());

    sess.setArgs({"expire", "setxxx1", "1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_EQ(Command::fmtLongLong(1), expect.value());

    std::this_thread::sleep_for(std::chrono::seconds(2));

    sess.setArgs({"srem", "setxxx1", "one"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_EQ(Command::fmtZero(), expect.value());
  }

  {
    sess.setArgs({"rpush", "list1", "one"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());

    sess.setArgs({"expire", "list1", "3"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_EQ(Command::fmtLongLong(1), expect.value());

    sess.setArgs({"lrem", "list1", "0", "one"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_EQ(Command::fmtLongLong(1), expect.value());
  }

  {
    sess.setArgs({"rpush", "list2", "one"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());

    sess.setArgs({"expire", "list2", "3"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_EQ(Command::fmtLongLong(1), expect.value());

    sess.setArgs({"ltrim", "list2", "1", "-1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
  }

  {
    sess.setArgs({"rpush", "list3", "one"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());

    sess.setArgs({"expire", "list3", "2"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_EQ(Command::fmtLongLong(1), expect.value());

    sess.setArgs({"lpop", "list3"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
  }

  {
    sess.setArgs({"hset", "hash1", "hh", "one"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());

    sess.setArgs({"expire", "hash1", "2"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_EQ(Command::fmtLongLong(1), expect.value());

    sess.setArgs({"hdel", "hash1", "hh"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_EQ(Command::fmtLongLong(1), expect.value());
  }

  std::this_thread::sleep_for(std::chrono::seconds(3));

  sess.setArgs({"dbsize"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(Command::fmtLongLong(0), expect.value());

  {
    sess.setArgs({"zadd", "zset1", "10", "a"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());

    sess.setArgs({"sadd", "set2", "three"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());

    sess.setArgs({"sadd", "set1", "one"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());

    sess.setArgs({"rpush", "list1", "one"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());

    sess.setArgs({"rpush", "list2", "one"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());

    sess.setArgs({"rpush", "list3", "one"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());

    sess.setArgs({"hset", "hash1", "hh", "one"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
  }

  std::this_thread::sleep_for(std::chrono::seconds(3));

  sess.setArgs({"dbsize"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(Command::fmtLongLong(7), expect.value());
}

void testRenameCommandTTL(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext, ioContext2;
  asio::ip::tcp::socket socket(ioContext), socket2(ioContext2);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  sess.setArgs({"zadd", "ss", "10", "a"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"expire", "ss", "3"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_EQ(Command::fmtLongLong(1), expect.value());

  sess.setArgs({"rename", "ss", "sa"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_EQ(Command::fmtOK(), expect.value());

  sess.setArgs({"dbsize"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(Command::fmtLongLong(1), expect.value());

  std::this_thread::sleep_for(std::chrono::seconds(4));

  sess.setArgs({"dbsize"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(Command::fmtLongLong(0), expect.value());

  sess.setArgs({"zadd", "ss", "3", "a"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  std::this_thread::sleep_for(std::chrono::seconds(3));

  sess.setArgs({"dbsize"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(Command::fmtLongLong(1), expect.value());
}

TEST(Command, DelTTLIndex) {
  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());
  auto cfg = makeServerParam();
  auto server = makeServerEntry(cfg);

  testDelTTLIndex(server);

#ifndef _WIN32
  server->stop();
  EXPECT_EQ(server.use_count(), 1);
#endif
}

TEST(Command, RenameCommandTTL) {
  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());
  auto cfg = makeServerParam();
  auto server = makeServerEntry(cfg);

  testRenameCommandTTL(server);

#ifndef _WIN32
  server->stop();
  EXPECT_EQ(server.use_count(), 1);
#endif
}

void testRenameCommandDelete(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext, ioContext2;
  asio::ip::tcp::socket socket(ioContext), socket2(ioContext2);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  sess.setArgs({"zadd", "ss{a}", "10", "a"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"zadd", "zz{a}", "101", "ab"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"rename", "ss{a}", "ss"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"zcount", "zz{a}", "0", "1000"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(Command::fmtLongLong(1), expect.value());
}

TEST(Command, RenameCommandDelete) {
  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());
  auto cfg = makeServerParam();
  auto server = makeServerEntry(cfg);

  testRenameCommandDelete(server);

#ifndef _WIN32
  server->stop();
  EXPECT_EQ(server.use_count(), 1);
#endif
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


void testCommandArray(std::shared_ptr<ServerEntry> svr,
                      const std::vector<std::vector<std::string>>& arr,
                      bool isError) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  for (auto& args : arr) {
    sess.setArgs(args);

    // need precheck for args check, after exp.ok(), can execute runSessionCmd()
    // EXPECT_FALSE when !exp.ok()
    auto exp = Command::precheck(&sess);
    if (!exp.ok()) {
      std::stringstream ss;
      for (auto& str : args) {
        ss << str << " ";
      }
      LOG(INFO) << ss.str() << "ERROR:" << exp.status().toString();
      EXPECT_FALSE(exp.ok());
      continue;
    }

    auto expect = Command::runSessionCmd(&sess);
    if (!expect.ok()) {
      std::stringstream ss;
      for (auto& str : args) {
        ss << str << " ";
      }
      LOG(INFO) << ss.str() << "ERROR:" << expect.status().toString();
    }

    if (isError) {
      EXPECT_FALSE(expect.ok());
    } else {
      EXPECT_TRUE(expect.ok());
    }
  }
}

void testCommandArrayResult(
  std::shared_ptr<ServerEntry> svr,
  const std::vector<std::pair<std::vector<std::string>, std::string>>& arr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  for (auto& p : arr) {
    sess.setArgs(p.first);
    auto expect = Command::runSessionCmd(&sess);
    INVARIANT_D(expect.ok());
    auto ret = expect.value();
    EXPECT_EQ(p.second, ret);
  }
}

TEST(Command, syncversion) {
  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());
  auto cfg = makeServerParam();
  cfg->kvStoreCount = 5;
  auto server = makeServerEntry(cfg);

  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext);
  NetSession sess(server, std::move(socket), 1, false, nullptr, nullptr);

  sess.setArgs({"syncversion", "k", "?", "?", "v1"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), "*2\r\n:-1\r\n:-1\r\n");

  sess.setArgs({"syncversion", "k", "25000", "1", "v1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());

  sess.setArgs({"syncversion", "k", "?", "?", "v1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), "*2\r\n:25000\r\n:1\r\n");

  sess.setArgs({"syncversion", "*", "?", "?", "v1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(),
            "*5\r\n*1\r\n*3\r\n$1\r\nk\r\n:25000\r\n:1\r\n"
            "*1\r\n*3\r\n$1\r\nk\r\n:25000\r\n:1\r\n"
            "*1\r\n*3\r\n$1\r\nk\r\n:25000\r\n:1\r\n"
            "*1\r\n*3\r\n$1\r\nk\r\n:25000\r\n:1\r\n"
            "*1\r\n*3\r\n$1\r\nk\r\n:25000\r\n:1\r\n");
}

TEST(Command, info) {
  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());
  auto cfg = makeServerParam();
  auto server = makeServerEntry(cfg);

  std::vector<std::vector<std::string>> correctArr = {
    {"info", "all"},
    {"info", "default"},
    {"info", "server"},
    {"info", "clients"},
    {"info", "memory"},
    {"info", "persistence"},
    {"info", "stats"},
    {"info", "replication"},
    {"info", "binloginfo"},
    {"info", "cpu"},
    {"info", "commandstats"},
    {"info", "cluster"},
    {"info", "keyspace"},
    {"info", "backup"},
    {"info", "dataset"},
    {"info", "compaction"},
    {"info", "levelstats"},
    {"info", "rocksdbstats"},
    {"info", "rocksdbperfstats"},
    {"info", "rocksdbbgerror"},
    {"info", "invalid"},  // it's ok
    {"rocksproperty", "rocksdb.base-level", "0"},
    {"rocksproperty", "all", "0"},
    {"rocksproperty", "rocksdb.base-level"},
    {"rocksproperty", "all"},
  };

  std::vector<std::pair<std::vector<std::string>, std::string>> okArr = {
    {{"config", "set", "session", "perf_level", "enable_count"},
     Command::fmtOK()},
    {{"config", "set", "session", "perf_level", "enable_time_expect_for_mutex"},
     Command::fmtOK()},
    {{"config",
      "set",
      "session",
      "perf_level",
      "enable_time_and_cputime_expect_for_mutex"},
     Command::fmtOK()},
    {{"config", "set", "session", "perf_level", "enable_time"},
     Command::fmtOK()},
    {{"config", "resetstat", "all"}, Command::fmtOK()},
    {{"config", "resetstat", "unseencommands"}, Command::fmtOK()},
    {{"config", "resetstat", "commandstats"}, Command::fmtOK()},
    {{"config", "resetstat", "stats"}, Command::fmtOK()},
    {{"config", "resetstat", "rocksdbstats"}, Command::fmtOK()},
    {{"config", "resetstat", "invalid"}, Command::fmtOK()},  // it's ok
    {{"tendisadmin", "sleep", "1"}, Command::fmtOK()},
    {{"tendisadmin", "recovery"}, Command::fmtOK()},
  };

  std::vector<std::vector<std::string>> wrongArr = {
    {"info", "all", "1"},
    {"rocksproperty", "rocks.base_level", "100"},
    {"rocksproperty", "all1", "0"},
    {"rocksproperty", "rocks.base_level1"},
    {"rocksproperty", "all1"},
    {"config", "set", "session", "perf_level", "invalid"},
    {"config", "set", "session", "invalid", "invalid"},
    {"config", "set", "session", "perf_level"},
    {"tendisadmin", "sleep"},
    {"tendisadmin", "sleep", "1", "2"},
    {"tendisadmin", "recovery", "1"},
    {"tendisadmin", "invalid"},
  };

  testCommandArray(server, correctArr, false);
  testCommandArrayResult(server, okArr);
  testCommandArray(server, wrongArr, true);

#ifndef _WIN32
  server->stop();
  EXPECT_EQ(server.use_count(), 1);
#endif
}

TEST(Command, command) {
  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());
  auto cfg = makeServerParam();
  auto server = makeServerEntry(cfg);

  std::vector<std::vector<std::string>> correctArr = {
    {"command"},
    {"command", "info"},
    {"command", "info", "get"},
    {"command", "info", "get", "set"},
    {"command", "info", "get", "set", "wrongcommand"},
    {"command", "count"},
    {"command", "getkeys", "get", "a"},
    {"command", "getkeys", "set", "a", "b"},
    {"command", "getkeys", "mset", "a", "b", "c", "d"},
  };

  std::vector<std::vector<std::string>> wrongArr = {
    {"command", "invalid"},
    {"command", "count", "invalid"},
    {"command", "getkeys"},
    {"command", "getkeys", "get", "a", "c"},
  };

  std::vector<std::pair<std::vector<std::string>, std::string>> resultArr = {
    {{"command", "info", "get"},
     "*1\r\n*6\r\n$3\r\nget\r\n:2\r\n*2\r\n+readonly\r\n+fast\r\n:1\r\n:1\r\n:"
     "1\r\n"},
    {{"command", "getkeys", "get", "a"}, "*1\r\n$1\r\na\r\n"},
    {{"command", "getkeys", "mset", "a", "b", "c", "d"},
     "*2\r\n$1\r\na\r\n$1\r\nc\r\n"},
  };

  testCommandArray(server, correctArr, false);
  testCommandArray(server, wrongArr, true);
  testCommandArrayResult(server, resultArr);

#ifndef _WIN32
  server->stop();
  EXPECT_EQ(server.use_count(), 1);
#endif
}

void testRevisionCommand(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  sess.setArgs({"set", "a", "b"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());

  // 1893430861000 :: 2030/1/1 1:1:1
  sess.setArgs({"revision", "a", "100", "1893430861000"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());

  sess.setArgs({"object", "revision", "a"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), ":100\r\n");

  sess.setArgs({"set", "key_1", "b"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());

  // 1577811661000 :: 2010/1/1 1:1:1 key should be deleted
  sess.setArgs({"revision", "key_1", "110", "1577811661000"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());

  sess.setArgs({"exists", "key_1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtZero());
}

TEST(Command, revision) {
  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());
  auto cfg = makeServerParam();
  auto server = makeServerEntry(cfg);

  testRevisionCommand(server);

#ifndef _WIN32
  server->stop();
  EXPECT_EQ(server.use_count(), 1);
#endif
}


AllKeys initData(std::shared_ptr<ServerEntry> server,
                 uint32_t count,
                 const char* key_suffix) {
  auto ctx1 = std::make_shared<asio::io_context>();
  auto sess1 = makeSession(server, ctx1);
  WorkLoad work(server, sess1);
  work.init();
  auto maxEleCnt = 2500;

  AllKeys all_keys;

  auto kv_keys = work.writeWork(RecordType::RT_KV, count, 0, true, key_suffix);
  all_keys.emplace_back(kv_keys);

  auto list_keys = work.writeWork(
    RecordType::RT_LIST_META, count, maxEleCnt, true, key_suffix);
  all_keys.emplace_back(list_keys);

  auto hash_keys = work.writeWork(
    RecordType::RT_HASH_META, count, maxEleCnt, true, key_suffix);
  all_keys.emplace_back(hash_keys);

  auto set_keys =
    work.writeWork(RecordType::RT_SET_META, count, maxEleCnt, true, key_suffix);
  all_keys.emplace_back(set_keys);

  auto zset_keys = work.writeWork(
    RecordType::RT_ZSET_META, count, maxEleCnt, true, key_suffix);
  all_keys.emplace_back(zset_keys);

  for (const auto& keyset : all_keys) {
    for (const auto& key : keyset) {
      if (std::rand() % 3 == 0) {
        auto ttl = std::rand() % 1000 + 1000;
        sess1->setArgs({"expire", key, std::to_string(ttl)});
        auto expect = Command::runSessionCmd(sess1.get());
        EXPECT_TRUE(expect.ok());
        EXPECT_EQ(expect.value(), Command::fmtOne());
      }
    }
  }

  return all_keys;
}

TEST(Command, restorevalue) {
  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv("restore1"));
  auto port1 = 5438;
  auto cfg1 = makeServerParam(port1, 0, "restore1");
  auto server1 = makeServerEntry(cfg1);

  EXPECT_TRUE(setupEnv("restore2"));
  auto port2 = 5439;
  auto cfg2 = makeServerParam(port2, 0, "restore2");
  auto server2 = makeServerEntry(cfg2);

  auto allkeys = initData(server1, 1000, "restorevalue_");

  for (const auto& keyset : allkeys) {
    for (const auto& key : keyset) {
      asio::io_context ioContext;
      asio::ip::tcp::socket socket(ioContext);
      NoSchedNetSession sess(
        server1, std::move(socket), 1, false, nullptr, nullptr);

      sess.setArgs({"restorevalue", key});
      auto expect = Command::runSessionCmd(&sess);
      EXPECT_TRUE(expect.ok());
      auto cmdvec = sess.getResponse();
      cmdvec.emplace_back(expect.value());

      asio::io_context ioContext2;
      asio::ip::tcp::socket socket2(ioContext2);
      NoSchedNetSession sess2(
        server2, std::move(socket2), 1, false, nullptr, nullptr);

      // skip the first and last cmd
      for (uint32_t i = 1; i < cmdvec.size() - 1; i++) {
        auto cmd = cmdvec[i];
        sess2.setArgsFromAof(cmd);

        auto expect = Command::runSessionCmd(&sess2);
        EXPECT_TRUE(expect.ok());
      }
    }
  }

  // compare data
  for (const auto& keyset : allkeys) {
    for (const auto& key : keyset) {
      asio::io_context ioContext;
      asio::ip::tcp::socket socket(ioContext);
      NoSchedNetSession sess(
        server1, std::move(socket), 1, false, nullptr, nullptr);

      auto keystr1 = key2Aof(&sess, key);
      INVARIANT_D(keystr1.ok());

      asio::io_context ioContext2;
      asio::ip::tcp::socket socket2(ioContext2);
      NoSchedNetSession sess2(
        server2, std::move(socket2), 1, false, nullptr, nullptr);

      auto keystr2 = key2Aof(&sess2, key);
      INVARIANT_D(keystr2.ok());

      EXPECT_EQ(keystr1.value(), keystr2.value());
    }
  }

#ifndef _WIN32
  server1->stop();
  EXPECT_EQ(server1.use_count(), 1);

  server2->stop();
  EXPECT_EQ(server2.use_count(), 1);
#endif
}

TEST(Command, dexec) {
  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());
  auto cfg = makeServerParam();
  auto server = makeServerEntry(cfg);

  std::vector<std::pair<std::vector<std::string>, std::string>> resultArr = {
    {{"set", "a", "b"}, Command::fmtOK()},
    {{"dexec", "2", "get", "a"},
     "*3\r\n$7\r\ndreturn\r\n$1\r\n2\r\n$7\r\n$1\r\nb\r\n\r\n"},
    {{"dexec", "-1", "set", "a", "c"},
     "*3\r\n$7\r\ndreturn\r\n$2\r\n-1\r\n$5\r\n+OK\r\n\r\n"},
    {{"dexec", "-1", "cluster", "nodes"},
     "*3\r\n$7\r\ndreturn\r\n$2\r\n-1\r\n$56\r\n-ERR:18,msg:This instance "
     "has cluster support disabled\r\n\r\n"},
    {{"dexec", "1", "dexec", "2", "get", "a"},
     "*3\r\n$7\r\ndreturn\r\n$1\r\n1\r\n$37\r\n*3\r\n$7\r\ndreturn\r\n$"
     "1\r\n2\r\n$7\r\n$1\r\nc\r\n\r\n\r\n"},
  };

  testCommandArrayResult(server, resultArr);

#ifndef _WIN32
  server->stop();
  EXPECT_EQ(server.use_count(), 1);
#endif
}

void testRocksOptionCommand(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  sess.setArgs({"CONFIG", "SET", "rocks.max_background_compactions", "3"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  for (uint32_t i = 0; i < svr->getKVStoreCount(); i++) {
    auto exptDb = svr->getSegmentMgr()->getDb(&sess, 0, mgl::LockMode::LOCK_IS);
    EXPECT_TRUE(exptDb.ok());

    auto store = exptDb.value().store;
    EXPECT_EQ(store->getOption("rocks.max_background_compactions"), 3);
  }

  sess.setArgs({"CONFIG", "SET", "rocks.max_open_files", "3000"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  for (uint32_t i = 0; i < svr->getKVStoreCount(); i++) {
    auto exptDb = svr->getSegmentMgr()->getDb(&sess, 0, mgl::LockMode::LOCK_IS);
    EXPECT_TRUE(exptDb.ok());

    auto store = exptDb.value().store;
    EXPECT_EQ(store->getOption("rocks.max_open_files"), 3000);
  }

  sess.setArgs({"CONFIG", "SET", "rocks.max_open_files", "-1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  for (uint32_t i = 0; i < svr->getKVStoreCount(); i++) {
    auto exptDb = svr->getSegmentMgr()->getDb(&sess, 0, mgl::LockMode::LOCK_IS);
    EXPECT_TRUE(exptDb.ok());

    auto store = exptDb.value().store;
    EXPECT_EQ(store->getOption("rocks.max_open_files"), -1);
  }

  sess.setArgs({"CONFIG", "SET", "rocks.abc", "-1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_FALSE(expect.ok());
}

void testResizeCommand(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  sess.setArgs({"CONFIG", "SET", "incrPushThreadnum", "8"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_EQ(svr->getParams()->incrPushThreadnum, 8);

  sess.setArgs({"CONFIG", "SET", "fullPushThreadnum", "8"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_EQ(svr->getParams()->fullPushThreadnum, 8);

  sess.setArgs({"CONFIG", "SET", "fullReceiveThreadnum", "8"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_EQ(svr->getParams()->fullReceiveThreadnum, 8);

  sess.setArgs({"CONFIG", "SET", "logRecycleThreadnum", "8"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_EQ(svr->getParams()->logRecycleThreadnum, 8);

  //     need _enable_cluster flag on.
  sess.setArgs({"CONFIG", "SET", "migrateSenderThreadnum", "8"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_EQ(svr->getParams()->migrateSenderThreadnum, 8);

  sess.setArgs({"CONFIG", "SET", "garbageDeleteThreadnum", "8"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_EQ(svr->getParams()->garbageDeleteThreadnum, 8);

  sess.setArgs({"CONFIG", "SET", "migrateReceiveThreadnum", "8"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_EQ(svr->getParams()->migrateReceiveThreadnum, 8);

  // index manager
  sess.setArgs({"CONFIG", "SET", "scanJobCntIndexMgr", "8"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_EQ(svr->getParams()->scanJobCntIndexMgr, 8);

  sess.setArgs({"CONFIG", "SET", "delJobCntIndexMgr", "8"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_EQ(svr->getParams()->delJobCntIndexMgr, 8);

  // total sleep 10s to wait thread resize ok
  sleep(10);
  EXPECT_EQ(svr->getReplManager()->incrPusherSize(), 8);
  EXPECT_EQ(svr->getReplManager()->fullPusherSize(), 8);
  EXPECT_EQ(svr->getReplManager()->fullReceiverSize(), 8);
  EXPECT_EQ(svr->getReplManager()->logRecycleSize(), 8);
  EXPECT_EQ(svr->getMigrateManager()->migrateSenderSize(), 8);
  EXPECT_EQ(svr->getGcMgr()->garbageDeleterSize(), 8);
  EXPECT_EQ(svr->getMigrateManager()->migrateReceiverSize(), 8);
  EXPECT_EQ(svr->getIndexMgr()->indexScannerSize(), 8);
  EXPECT_EQ(svr->getIndexMgr()->keyDeleterSize(), 8);

  sess.setArgs({"CONFIG", "SET", "fullPushThreadnum", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_EQ(svr->getParams()->fullPushThreadnum, 1);

  sess.setArgs({"CONFIG", "SET", "fullReceiveThreadnum", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_EQ(svr->getParams()->fullReceiveThreadnum, 1);

  sess.setArgs({"CONFIG", "SET", "logRecycleThreadnum", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_EQ(svr->getParams()->fullReceiveThreadnum, 1);

  sess.setArgs({"CONFIG", "SET", "migrateSenderThreadnum", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_EQ(svr->getParams()->migrateSenderThreadnum, 1);

  sess.setArgs({"CONFIG", "SET", "garbageDeleteThreadnum", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_EQ(svr->getParams()->garbageDeleteThreadnum, 1);

  sess.setArgs({"CONFIG", "SET", "incrPushThreadnum", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_EQ(svr->getParams()->incrPushThreadnum, 1);

  sess.setArgs({"CONFIG", "SET", "migrateReceiveThreadnum", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_EQ(svr->getParams()->migrateReceiveThreadnum, 1);

  // index manager
  sess.setArgs({"CONFIG", "SET", "scanJobCntIndexMgr", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_EQ(svr->getParams()->scanJobCntIndexMgr, 1);

  sess.setArgs({"CONFIG", "SET", "delJobCntIndexMgr", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_EQ(svr->getParams()->delJobCntIndexMgr, 1);

  // total sleep 10s to wait thread resize ok
  sleep(10);
  EXPECT_EQ(svr->getReplManager()->incrPusherSize(), 1);
  EXPECT_EQ(svr->getReplManager()->fullPusherSize(), 1);
  EXPECT_EQ(svr->getReplManager()->fullReceiverSize(), 1);
  EXPECT_EQ(svr->getReplManager()->logRecycleSize(), 1);
  EXPECT_EQ(svr->getMigrateManager()->migrateSenderSize(), 1);
  EXPECT_EQ(svr->getGcMgr()->garbageDeleterSize(), 1);
  EXPECT_EQ(svr->getMigrateManager()->migrateReceiverSize(), 1);
  EXPECT_EQ(svr->getIndexMgr()->indexScannerSize(), 1);
  EXPECT_EQ(svr->getIndexMgr()->keyDeleterSize(), 1);
}

TEST(Command, resizeCommand) {
  const auto guard = MakeGuard([]() { destroyEnv(); });
  EXPECT_TRUE(setupEnv());
  auto cfg = makeServerParam();

  // note: migrate resize needs cluster-enabled flag on, default is off.
  cfg->clusterEnabled = true;
  getGlobalServer() = makeServerEntry(cfg);

  testResizeCommand(getGlobalServer());

#ifndef _WIN32
  getGlobalServer()->stop();
  EXPECT_EQ(getGlobalServer().use_count(), 1);
#endif
}

TEST(Command, adminSet_Get_DelCommand) {
  const auto guard = MakeGuard([] { destroyEnv(); });
  EXPECT_TRUE(setupEnv());
  auto cfg = makeServerParam();
  cfg->kvStoreCount = 3;
  auto server = makeServerEntry(cfg);

  std::vector<std::vector<std::string>> wrongArr = {
          {"ADMINSET"},
          {"ADMINSET", "test"},

          {"ADMINGET"},
          {"ADMINGET", "test", "storeid",
           std::to_string(cfg->kvStoreCount + 1)},
          {"ADMINGET", "test", "storeid", "("},

          {"ADMINDEL"},
  };

  std::vector<std::pair<std::vector<std::string>, std::string>> resultArr = {
          {{"ADMINSET", "test", "xx"}, Command::fmtOK()},

          {{"ADMINGET", "test"}, "*3\r\n*2\r\n$1\r\n0\r\n$2\r\nxx\r\n"
               "*2\r\n$1\r\n1\r\n$2\r\nxx\r\n*2\r\n$1\r\n2\r\n$2\r\nxx\r\n"},
          {{"ADMINGET", "test", "storeid", "2"},
                "*1\r\n*2\r\n$1\r\n2\r\n$2\r\nxx\r\n"},

          {{"ADMINDEL", "test"}, Command::fmtOne()},
          {{"ADMINDEL", "test"}, Command::fmtZero()},
          {{"ADMINGET", "test"}, "*3\r\n*2\r\n$1\r\n0\r\n$-1\r\n*2\r\n"
                                 "$1\r\n1\r\n$-1\r\n*2\r\n$1\r\n2\r\n$-1\r\n"},
  };

  testCommandArray(server, wrongArr, true);
  testCommandArrayResult(server, resultArr);

#ifndef _WIN32
  server->stop();
  EXPECT_EQ(server.use_count(), 1);
#endif
}

TEST(Command, rocksdbOptionsCommand) {
  const auto guard = MakeGuard([]() { destroyEnv(); });
  EXPECT_TRUE(setupEnv());
  auto cfg = makeServerParam();

  getGlobalServer() = makeServerEntry(cfg);

  testRocksOptionCommand(getGlobalServer());

#ifndef _WIN32
  getGlobalServer()->stop();
  EXPECT_EQ(getGlobalServer().use_count(), 1);
#endif
}

// NOTE(takenliu): renameCommand may change command's name or behavior, so put
// it in the end
extern string gRenameCmdList;
extern string gMappingCmdList;
TEST(Command, renameCommand) {
  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());
  auto cfg = makeServerParam();
  auto server = makeServerEntry(cfg);
  gRenameCmdList += ",set set_rename";
  gMappingCmdList += ",dbsize emptyint,keys emptymultibulk";
  Command::changeCommand(gRenameCmdList, "rename");
  Command::changeCommand(gMappingCmdList, "mapping");

  testRenameCommand(server);

  gRenameCmdList = "";
  gMappingCmdList = "";

#ifndef _WIN32
  server->stop();
  EXPECT_EQ(server.use_count(), 1);
#endif
}

void testSort(bool clusterEnabled) {
  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());
  auto cfg = makeServerParam();
  cfg->clusterEnabled = clusterEnabled;
  cfg->generalLog = true;
  cfg->logLevel = "debug";
  auto server = makeServerEntry(cfg);

  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext);
  NetSession sess(server, std::move(socket), 1, false, nullptr, nullptr);

  if (clusterEnabled) {
    sess.setArgs({"cluster", "addslots", "{0..16383}"});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
  }
  std::this_thread::sleep_for(std::chrono::seconds(3));

  // uid
  sess.setArgs({"LPUSH", "uid", "2", "3", "1"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  // name
  sess.setArgs({"set", "user_name_1", "admin"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  sess.setArgs({"set", "user_name_2", "jack"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  sess.setArgs({"set", "user_name_3", "mary"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  // level
  sess.setArgs({"set", "user_level_1", "10"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  sess.setArgs({"set", "user_level_2", "5"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  sess.setArgs({"set", "user_level_3", "8"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  // sort
  sess.setArgs({"sort", "uid"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_EQ(expect.value(), "*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n");

  // sort by
  sess.setArgs({"sort", "uid", "by", "user_level_*"});
  expect = Command::runSessionCmd(&sess);
  if (!clusterEnabled) {
    EXPECT_EQ(expect.value(),
            "*3\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n1\r\n");
  } else {
    EXPECT_EQ(expect.status().toString(),
            "-ERR BY option of SORT denied in Cluster mode.\r\n");
  }

  // sort get
  sess.setArgs({"sort", "uid", "get", "user_name_*"});
  expect = Command::runSessionCmd(&sess);
  if (!clusterEnabled) {
    EXPECT_EQ(expect.value(),
            "*3\r\n$5\r\nadmin\r\n$4\r\njack\r\n$4\r\nmary\r\n");
  } else {
    EXPECT_EQ(expect.status().toString(),
            "-ERR GET option of SORT denied in Cluster mode.\r\n");
  }

#ifndef _WIN32
  server->stop();
  EXPECT_EQ(server.use_count(), 1);
#endif
}

TEST(Command, sort_cluster) {
  testSort(false);
  testSort(true);
}

}  // namespace tendisplus
