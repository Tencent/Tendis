// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <algorithm>
#include "gtest/gtest.h"
#include "glog/logging.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/test_util.h"


namespace tendisplus {

TEST(Lua, Common) {
  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());

  auto cfg = makeServerParam();
  auto server = std::make_shared<ServerEntry>(cfg);
  auto s = server->startup(cfg);
  ASSERT_TRUE(s.ok());
  int32_t testNum = 100000;
  std::thread th1(
    [&server, testNum]() {
    auto ctx = std::make_shared<asio::io_context>();
    auto session = makeSession(server, ctx);
    WorkLoad work(server, session);
    work.init();

    int i = 0;
    while (i++ < testNum) {
      auto ret = work.getStringResult({"eval",
        "redis.call('set',KEYS[1],'value1');return redis.call('get',KEYS[1]);",
        "1", "key1"});
      ASSERT_EQ(ret, "$6\r\nvalue1\r\n");
    }
  });
  std::thread th2(
    [&server, testNum]() {
      auto ctx = std::make_shared<asio::io_context>();
      auto session = makeSession(server, ctx);
      WorkLoad work(server, session);
      work.init();

      int i = 0;
      while (i++ < testNum) {
        auto ret = work.getStringResult({"eval",
          "redis.call('set',KEYS[1],'value2');"
          "return redis.call('get',KEYS[1]);",
          "1", "key1"});
        ASSERT_EQ(ret, "$6\r\nvalue2\r\n");
      }
    });
  std::thread th3(
    [&server, testNum]() {
      auto ctx = std::make_shared<asio::io_context>();
      auto session = makeSession(server, ctx);
      WorkLoad work(server, session);
      work.init();

      int i = 0;
      int num_value1 = 0;
      int num_value2 = 0;
      int num_value3 = 0;
      while (i++ < testNum) {
        auto ret = work.getStringResult({"set", "key1", "value3"});
        ret = work.getStringResult({"get", "key1"});
        ASSERT_TRUE(ret == "$6\r\nvalue1\r\n" ||
                            ret == "$6\r\nvalue2\r\n" ||
                            ret == "$6\r\nvalue3\r\n");
        if (ret == "$6\r\nvalue1\r\n") {
          num_value1++;
        } else if (ret == "$6\r\nvalue2\r\n") {
          num_value2++;
        } else if (ret == "$6\r\nvalue3\r\n") {
          num_value3++;
        }
      }
      LOG(INFO) << "==========num_value1:" << num_value1
            << " num_value2:" << num_value2
            << " num_value3:" << num_value3 << "===========";
      ASSERT_NE(num_value1, 0);
      ASSERT_NE(num_value2, 0);
      ASSERT_NE(num_value3, 0);
    });
  std::thread th4(
    [&server]() {
      auto ctx = std::make_shared<asio::io_context>();
      auto session = makeSession(server, ctx);
      WorkLoad work(server, session);
      work.init();

      auto ret = work.getStringResult({"set", "key_incr", "0"});
      ret = work.getStringResult({"eval",
                                       "local n=1;"
                                       "for i=10000,1,-1 do"
                                       "  redis.call('incr', KEYS[1]);"
                                       "end;"
                                       "return redis.call('get',KEYS[1]);",
                                       "1", "key_incr"});
      ASSERT_TRUE(ret == "$5\r\n10000\r\n" || ret == "$5\r\n20000\r\n");
    });
  std::thread th5(
    [&server]() {
      auto ctx = std::make_shared<asio::io_context>();
      auto session = makeSession(server, ctx);
      WorkLoad work(server, session);
      work.init();

      int i = 0;
      while (i++ < 10000) {
        auto ret = work.getStringResult({"set", "key_incr", "10000"});
        ASSERT_EQ(ret, "+OK\r\n");
      }
    });

  th1.join();
  th2.join();
  th3.join();
  th4.join();
  th5.join();

  server->stop();
  ASSERT_EQ(server.use_count(), 1);
}

TEST(Lua, LuaStateMaxIdleTime) {
  const auto guard = MakeGuard([] { destroyEnv(); });

  EXPECT_TRUE(setupEnv());

  auto cfg = makeServerParam();
  cfg->luaStateMaxIdleTime = 1000;  // 1s
  cfg->generalLog = true;
  auto server = std::make_shared<ServerEntry>(cfg);
  auto s = server->startup(cfg);
  ASSERT_TRUE(s.ok());

  LOG(INFO) << "first add data begin.";
  auto ctx = std::make_shared<asio::io_context>();
  auto session = makeSession(server, ctx);
  WorkLoad work(server, session);
  work.init();
  int i = 0;
  while (i++ < 200) {
    auto ret = work.getStringResult({"eval",
      "redis.call('set',KEYS[1],'value1');return redis.call('get',KEYS[1]);",
      "1", "key1"});
    ASSERT_EQ(ret, "$6\r\nvalue1\r\n");
  }
  LOG(INFO) << "first add data end.";

  // sleep longer than luaStateMaxIdleTime
  uint32_t sleepSec = cfg->luaStateMaxIdleTime/1000 + 2;
  std::this_thread::sleep_for(std::chrono::seconds(sleepSec));

  LOG(INFO) << "second add data begin.";
  i = 0;
  while (i++ < 200) {
    auto ret = work.getStringResult({"eval",
      "redis.call('set',KEYS[1],'value1');return redis.call('get',KEYS[1]);",
      "1", "key1"});
    ASSERT_EQ(ret, "$6\r\nvalue1\r\n");
  }
  LOG(INFO) << "second add data end.";

  server->stop();
  ASSERT_EQ(server.use_count(), 1);
}

}  // namespace tendisplus
