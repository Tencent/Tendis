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

    for (size_t i = 0; i < 10; i++) {
        ss.str("");
        auto orig = randomStr(genRand() % 20000, 1);
        auto size = encodeLenStr(ss, orig);
        auto s1 = encodeLenStr(orig);

        auto s2 = ss.str();
        EXPECT_EQ(s2.size(), size);
        EXPECT_EQ(s2, s1);

        auto ed = decodeLenStr(s2);
        EXPECT_EQ(ed.value().first, orig);
    }
}
}  // namespace tendisplus
