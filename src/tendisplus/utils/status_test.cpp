// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <iostream>
#include <string>
#include "tendisplus/utils/status.h"
#include "gtest/gtest.h"

namespace tendisplus {

TEST(Status, Common) {
  auto s = Expected<std::string>(ErrorCodes::ERR_INTERNAL, "test");
  EXPECT_EQ(s.ok(), false);
  EXPECT_EQ(s.status().toString(), "-ERR:3,msg:test\r\n");
  auto s1 = makeExpected<std::string>("test");
  EXPECT_EQ(s1.ok(), true);
  EXPECT_EQ(s1.value(), "test");
}


size_t callCnt;
size_t moveCallCnt;
class Base {
 public:
  Base() {
    callCnt++;
    std::cout << "default constructor" << std::endl;
  }
  ~Base() {
    std::cout << "destructor" << std::endl;
  }
  Base(const Base& b) {
    callCnt++;
    std::cout << "copy constructor" << std::endl;
  }
  Base(Base&& b) {
    moveCallCnt++;
    std::cout << "move constructor" << std::endl;
  }
};

Expected<Base> getBase() {
  Base b;
  return b;
}

static constexpr uint32_t abc = 10;
Expected<uint32_t> getInt() {
  return abc;
}

TEST(Status, Move) {
  {
    auto eb = getBase();
    EXPECT_EQ(callCnt, 1);
    EXPECT_EQ(moveCallCnt, 1);
    callCnt = 0;
    moveCallCnt = 0;
  }

  std::cout << "============" << std::endl;

  {
    auto eb2 = makeExpected<Base>();
    EXPECT_EQ(callCnt, 1);
    EXPECT_EQ(moveCallCnt, 1);
    callCnt = 0;
    moveCallCnt = 0;
  }
  std::cout << "============" << std::endl;

  { auto eb = getInt(); }
}

TEST(StatusWith, nonDefaultConstructible) {
  class A {
    A() = delete;

   public:
    explicit A(int x) : _x{x} {};
    int _x{0};
  };

  auto v = makeExpected<A>(1);
  EXPECT_EQ(v.value()._x, 1);

  auto vv = Expected<A>(ErrorCodes::ERR_INTERNAL, "foo");
  EXPECT_EQ(vv.ok(), false);
}

}  // namespace tendisplus
