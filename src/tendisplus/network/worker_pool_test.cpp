// Copyright (C) 2020 Tencent.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.
#include <memory>
#include <utility>

#include <memory>
#include <utility>

#include "gtest/gtest.h"

#include "tendisplus/network/worker_pool.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/test_util.h"

TEST(Workerpool, resize) {
  auto matrix = std::make_shared<tendisplus::PoolMatrix>();
  tendisplus::WorkerPool pool("test-pool", matrix);
  EXPECT_TRUE(tendisplus::setupEnv());

  std::thread t([&pool]() { pool.startup(5); });

  // note: startup need necessary time to get ready
  usleep(10000);
  ASSERT_EQ(pool.size(), 5);

  pool.resize(10);
  ASSERT_EQ(pool.size(), 10);

  // note: thread resize to decrease is async op, need time to complete.
  pool.resize(5);
  usleep(10000);
  ASSERT_EQ(pool.size(), 5);

  pool.stop();
  t.join();

  auto guard = tendisplus::MakeGuard([]() { tendisplus::destroyEnv(); });
}

TEST(Workerpool, isFull) {
  auto matrix = std::make_shared<tendisplus::PoolMatrix>();
  tendisplus::WorkerPool pool("test-pool", matrix);
  EXPECT_TRUE(tendisplus::setupEnv());

  std::thread t([&pool]() { pool.startup(5); });

  // note: usleep() a short time to wait pool get ready
  // post tasks( >5 ) to make queue become full
  usleep(10000);
  for (size_t i = 0; i < 8; ++i) {
    auto task = []() { sleep(5); };
    pool.schedule(std::move(task));
  }
  ASSERT_EQ(pool.size(), 5);
  ASSERT_TRUE(pool.isFull());

  pool.stop();
  t.join();

  auto guard = tendisplus::MakeGuard([]() { tendisplus::destroyEnv(); });
}

TEST(Workerpool, schedule) {
  auto matrix = std::make_shared<tendisplus::PoolMatrix>();
  tendisplus::WorkerPool pool("test-pool", matrix);

  std::thread t([&pool]() { pool.startup(3); });

  std::atomic<int> val{5};
  pool.schedule([&val]() { val.store(10); });

  usleep(10000);
  ASSERT_EQ(val.load(), 10);

  pool.stop();
  t.join();
  auto guard = tendisplus::MakeGuard([]() { tendisplus::destroyEnv(); });
}

TEST(Workerpool, addTimer) {
  auto matrix = std::make_shared<tendisplus::PoolMatrix>();
  auto pool = std::make_shared<tendisplus::WorkerPool>("test-pool", matrix);
  pool->startup(3);

  std::atomic<int> val{5};
  pool->timer_add([&val]() { val.store(10, std::memory_order_seq_cst); },
                  std::chrono::milliseconds(100));
  std::this_thread::sleep_for(std::chrono::seconds(1));
  ASSERT_EQ(val.load(std::memory_order_seq_cst), 10);
  pool->stop();
  auto guard = tendisplus::MakeGuard([]() { tendisplus::destroyEnv(); });
}

TEST(Workerpool, cancelTimer) {
  auto matrix = std::make_shared<tendisplus::PoolMatrix>();
  auto pool = std::make_shared<tendisplus::WorkerPool>("test-pool", matrix);
  std::thread t([&pool]() { pool->startup(3); });
  std::atomic<int> val{1};
  auto timerId =
    pool->timer_add([&val]() { val.store(42, std::memory_order_seq_cst); },
                    std::chrono::seconds(2));
  pool->timer_cancel(timerId);
  std::this_thread::sleep_for(std::chrono::seconds(3));
  ASSERT_EQ(val.load(std::memory_order_seq_cst), 1);
  pool->stop();
  t.join();
  auto guard = tendisplus::MakeGuard([]() { tendisplus::destroyEnv(); });
}
