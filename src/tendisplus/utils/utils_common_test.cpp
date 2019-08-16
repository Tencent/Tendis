#include <iostream>
#include <string>
#include <fstream>
#include <utility>
#include <memory>
#include <vector>
#include <limits>
#include <algorithm>
#include <random>
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/time.h"
#include "tendisplus/utils/param_manager.h"
#include "tendisplus/utils/base64.h"
#include "gtest/gtest.h"
#include "unistd.h"

namespace tendisplus {

    int genRand() {
        int grand = 0;
        uint32_t ms = (uint32_t)nsSinceEpoch();
        grand = rand_r(reinterpret_cast<unsigned int *>(&ms));
        return grand;
    }

    std::string randomStr(size_t s, bool maybeEmpty) {
        if (s == 0) {
            s = genRand() % 256;
        }
        if (!maybeEmpty) {
            s++;
        }
        std::vector<uint8_t> v;
        for (size_t i = 0; i < s; i++) {
            v.emplace_back(genRand() % 256);
        }
        return std::string(reinterpret_cast<const char*>(v.data()), v.size());
    }

TEST(String, Common) {
    std::stringstream ss;
    char buf[20000];

    for (size_t i = 0; i < 1000; i++) {
        ss.str("");
        auto orig = randomStr(genRand() % 20000, 1);
        auto size = lenStrEncode(ss, orig);
        auto s1 = lenStrEncode(orig);

        auto size2 = lenStrEncode(buf, sizeof(buf), orig);

        auto s2 = ss.str();
        EXPECT_EQ(s2.size(), size);
        EXPECT_EQ(s2.size(), size2);
        EXPECT_EQ(s2, s1);

        auto ed = lenStrDecode(s2);
        auto ed2 = lenStrDecode(buf, size2);
        EXPECT_EQ(ed.value().first, orig);
        EXPECT_EQ(ed2.value().first, orig);
    }
}

TEST(String, Split) {
    {
        auto v = stringSplit("set a b", " ");
        std::vector <std::string> v2 = { "set", "a", "b" };
        EXPECT_EQ(v, v2);
    }
    {
        auto v = stringSplit("set a b ", " ");
        std::vector <std::string> v2 = { "set", "a", "b" };
        EXPECT_EQ(v, v2);
    }
    {
        auto v = stringSplit("set", " ");
        std::vector <std::string> v2 = { "set"};
        EXPECT_EQ(v, v2);
    }
    {
        auto v = stringSplit("set ", " ");
        std::vector <std::string> v2 = { "set"};
        EXPECT_EQ(v, v2);
    }
    {
        auto v = stringSplit("", " ");
        std::vector <std::string> v2 = {};
        EXPECT_EQ(v, v2);
    }

}

TEST(Base64, common) {
    std::string data = "aa";
    std::string encode = Base64::Encode((unsigned char*)data.c_str(), data.size());
    std::string decode = Base64::Decode(encode.c_str(), encode.size());
    EXPECT_EQ(data, decode);

    data = "MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM2222222222222222222222222222222222";
    encode = Base64::Encode((unsigned char*)data.c_str(), data.size());
    decode = Base64::Decode(encode.c_str(), encode.size());
    EXPECT_EQ(data, decode);

    data = "MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM22222222222222222222222222222222 \
        ------------------------------****************************************************** \
        ########################################zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";
    encode = Base64::Encode((unsigned char*)data.c_str(), data.size());
    decode = Base64::Decode(encode.c_str(), encode.size());
    EXPECT_EQ(data, decode);
}

TEST(ParamManager, common) {
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
