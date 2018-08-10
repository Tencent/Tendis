#include <iostream>
#include <string>
#include "tendisplus/utils/status.h"
#include "gtest/gtest.h"

namespace tendisplus {
// IndependentMethod is a test case - here, we have 2 tests for this 1 test case
TEST(Status, Common) {
    auto s = Expected<std::string>(ErrorCodes::INTERNAL_ERROR, "test");
    EXPECT_EQ(s.ok(), false);
    EXPECT_EQ(s.status().toString(), "test");
    auto s1 = makeExpected<std::string>("test");
    EXPECT_EQ(s1.ok(), true);
    EXPECT_EQ(s1.value(), "test");
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

    auto vv = Expected<A>(ErrorCodes::INTERNAL_ERROR, "foo");
    EXPECT_EQ(vv.ok(), false);
}

}  // namespace tendisplus
