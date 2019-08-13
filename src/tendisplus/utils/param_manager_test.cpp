#include <iostream>
#include <string>
#include "tendisplus/utils/param_manager.h"
#include "gtest/gtest.h"

namespace tendisplus {

TEST(Base64, common) {
    ParamManager pm;
    const char* argv[] = {"--skey1=value",
        "--ikey1=123"};
    pm.init(2, (char**)argv);
    EXPECT_EQ(pm.getString("skey1"), "value");
    EXPECT_EQ(pm.getString("skey2"), "");
    EXPECT_EQ(pm.getString("skey3", "a"), "a");

    EXPECT_EQ(pm.getUint64("ikey1"), 123);
    EXPECT_EQ(pm.getUint64("ikey2"), 0);
    EXPECT_EQ(pm.getUint64("ikey3", 1), 1);
}

}  // namespace tendisplus
