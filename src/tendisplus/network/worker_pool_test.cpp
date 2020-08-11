
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "tendisplus/network/worker_pool.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/test_util.h"

TEST(Workerpool, resize) {
    using namespace tendisplus;
    auto matrix = std::make_shared<PoolMatrix>();
    WorkerPool pool("test-pool", matrix);
    EXPECT_TRUE(setupEnv());

    std::thread t([&pool](){
        pool.startup(5);
    });

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

    auto guard = MakeGuard([]() {
      destroyEnv();
    });
}

TEST(Workerpool, isFull) {
    using namespace tendisplus;
    auto matrix = std::make_shared<PoolMatrix>();
    WorkerPool pool("test-pool", matrix);
    EXPECT_TRUE(setupEnv());

    std::thread t([&pool]() {
        pool.startup(5);
    });

    // note: usleep() a short time to wait pool get ready
    //     post tasks( >5 ) to make queue become full
    usleep(10000);
    for (size_t i = 0; i < 8; ++i) {
        auto task = [](){ sleep (5); };
        pool.schedule(std::move(task));
    }
    ASSERT_EQ(pool.size(), 5);
    ASSERT_TRUE(pool.isFull());

    pool.stop();
    t.join();

    auto guard = MakeGuard([]() {
        destroyEnv();
    });
}

TEST(Workerpool, schedule) {
    using namespace tendisplus;
    auto matrix = std::make_shared<PoolMatrix>();
    WorkerPool pool("test-pool", matrix);

    std::thread t([&pool]() {
        pool.startup(3);
    });

    int val = 5;
    pool.schedule([&val]() {
        val = 10;
    });

    usleep(10000);
    ASSERT_EQ(val, 10);

    pool.stop();
    t.join();

    auto guard = MakeGuard([]() {
        destroyEnv();
    });
}