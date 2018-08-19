#include <vector>
#include <algorithm>
#include "tendisplus/storage/varint.h"
#include "gtest/gtest.h"

namespace tendisplus {

void testVarint(uint64_t val, std::vector<uint8_t> bytes) {
    EXPECT_EQ(bytes, varintEncode(val));
    EXPECT_EQ(varintDecodeFwd(bytes.data(), bytes.size()), val);
    std::reverse(bytes.begin(), bytes.end());
    EXPECT_EQ(varintDecodeRvs(bytes.data()+bytes.size()-1, bytes.size()), val);
}

TEST(Varint, Common) {
    testVarint(0, {0});
    testVarint(1, {1});
    testVarint(127, {127});
    testVarint(128, {0x80, 0x01});
    testVarint(300, {0xac, 0x02});
    testVarint(16383, {0xff, 0x7f});
    testVarint(16384, {0x80, 0x80, 0x01});

    testVarint(static_cast<uint32_t>(-1),
        {0xff, 0xff, 0xff, 0xff, 0x0f});
    testVarint(static_cast<uint64_t>(-1),
        {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01});
}

}  // namespace tendisplus
