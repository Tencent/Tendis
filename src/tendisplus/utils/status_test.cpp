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
