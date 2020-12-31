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
  std::thread th1(
    [&server]() {
    auto ctx = std::make_shared<asio::io_context>();
    auto session = makeSession(server, ctx);
    WorkLoad work(server, session);
    work.init();

    int i = 0;
    while (i++ < 1000) {
      auto ret = work.getStringResult({"eval",
        "redis.call('set',KEYS[1],'value1');return redis.call('get',KEYS[1]);",
        "1", "key1"});
      ASSERT_EQ(ret, "$6\r\nvalue1\r\n");
    }
  });
  std::thread th2(
    [&server]() {
      auto ctx = std::make_shared<asio::io_context>();
      auto session = makeSession(server, ctx);
      WorkLoad work(server, session);
      work.init();

      int i = 0;
      while (i++ < 1000) {
        auto ret = work.getStringResult({"eval",
          "redis.call('set',KEYS[1],'value2');"
          "return redis.call('get',KEYS[1]);",
          "1", "key1"});
        ASSERT_EQ(ret, "$6\r\nvalue2\r\n");
      }
  });
  std::thread th3(
    [&server]() {
      auto ctx = std::make_shared<asio::io_context>();
      auto session = makeSession(server, ctx);
      WorkLoad work(server, session);
      work.init();

      int i = 0;
      int num_value1 = 0;
      int num_value2 = 0;
      int num_value3 = 0;
      while (i++ < 1000) {
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
      LOG(INFO) << "num_value1:" << num_value1 << " num_value2:" << num_value2
              << " num_value3:" << num_value3;
      ASSERT_NE(num_value1, 0);
      ASSERT_NE(num_value2, 0);
      ASSERT_NE(num_value3, 0);
    });
  th1.join();
  th2.join();
  th3.join();

  server->stop();
  ASSERT_EQ(server.use_count(), 1);
}

}  // namespace tendisplus
