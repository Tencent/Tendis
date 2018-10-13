#include <iostream>
#include <string>
#include <algorithm>
#include <thread>
#include "gtest/gtest.h"
#include "tendisplus/lock/lock.h"

namespace tendisplus {

TEST(Lock, Common) {
    bool runFlag1 = true, runFlag2 = true;
    bool locked1 = false, locked2 = false;

    std::thread thd1([&runFlag1, &locked1]() {
        StoresLock v(mgl::LockMode::LOCK_IS);
        locked1 = true;
        while (runFlag1) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    });

    std::this_thread::sleep_for(std::chrono::seconds(1));

    std::thread thd2([&runFlag2, &locked2]() {
        StoresLock v(mgl::LockMode::LOCK_X);
        locked2 = true;
        while (runFlag2) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    });

    EXPECT_TRUE(locked1);
    EXPECT_FALSE(locked2);
    runFlag1 = false;
    thd1.join();

    std::this_thread::sleep_for(std::chrono::seconds(1));
    EXPECT_TRUE(locked2);
    runFlag2 = false;
    thd2.join();
}

TEST(Lock, Parent) {
    bool runFlag1 = true, runFlag2 = true;
    bool locked1 = false, locked2 = false;

    auto sess = std::make_shared<LocalSession>(nullptr);
    std::thread thd1([&runFlag1, &locked1, sess]() {
        StoreLock v(1, mgl::LockMode::LOCK_IS, sess.get());
        StoreLock v1(2, mgl::LockMode::LOCK_IS, sess.get());
        locked1 = true;
        while (runFlag1) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    });

    std::this_thread::sleep_for(std::chrono::seconds(1));

    std::thread thd2([&runFlag2, &locked2]() {
        StoresLock v(mgl::LockMode::LOCK_X);
        locked2 = true;
        while (runFlag2) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    });

    EXPECT_TRUE(locked1);
    EXPECT_FALSE(locked2);
    runFlag1 = false;
    thd1.join();

    std::this_thread::sleep_for(std::chrono::seconds(1));
    EXPECT_TRUE(locked2);
    runFlag2 = false;
    thd2.join();
}
}  // namespace tendisplus
