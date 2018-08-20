#include <time.h>
#include <cstdlib>
#include <string>
#include <vector>
#include "tendisplus/storage/record.h"
#include "gtest/gtest.h"

namespace tendisplus {

static int genRand() {
    static int grand = 0;
    grand = rand_r(reinterpret_cast<unsigned int *>(&grand));
    return grand;
}

RecordType randomType() {
    switch ((genRand() % 4)) {
        case 0:
            return RecordType::RT_META;
        case 1:
            return RecordType::RT_KV;
        case 2:
            return RecordType::RT_LIST_META;
        case 3:
            return RecordType::RT_LIST_ELE;
        default:
            return RecordType::RT_INVALID;
    }
}

std::string randomStr(bool maybeEmpty) {
    size_t s = genRand() % 256;
    if (!maybeEmpty) {
        s++;
    }
    std::vector<uint8_t> v;
    for (size_t i = 0; i < s; i++) {
        v.emplace_back(genRand() % 256);
    }
    return std::string(reinterpret_cast<const char*>(v.data()), v.size());
}

std::string overflip(const std::string& s) {
    std::vector<uint8_t> buf;
    buf.insert(buf.end(), s.begin(), s.end());
    size_t size = buf.size();
    auto ori1 = buf[size-9];
    while (ori1 == buf[size-9]) {
        buf[size-9] = genRand() % 256;
    }
    return std::string(
        reinterpret_cast<const char *>(buf.data()), buf.size());
}

TEST(Record, Common) {
    srand(time(NULL));
    for (size_t i = 0; i < 100000; i++) {
        uint32_t dbid = genRand();
        auto type = randomType();
        auto pk = randomStr(false);
        auto sk = randomStr(true);
        uint32_t ttl = genRand();
        auto val = randomStr(false);
        auto rcd = Record(dbid, type, pk, sk, val, ttl);
        auto kv = rcd.encode();
        auto prcd1 = Record::decode(kv.first, kv.second);
        EXPECT_TRUE(prcd1.ok());
        EXPECT_EQ(*prcd1.value(), rcd);
    }

    for (size_t i = 0; i < 100000; i++) {
        uint32_t dbid = genRand();
        auto type = randomType();
        auto pk = randomStr(false);
        auto sk = randomStr(true);
        uint32_t ttl = genRand();
        auto val = randomStr(false);
        auto rcd = Record(dbid, type, pk, sk, val, ttl);
        auto kv = rcd.encode();
        auto prcd1 = Record::decode(overflip(kv.first), kv.second);
        EXPECT_EQ(prcd1.status().code(), ErrorCodes::ERR_DECODE);
    }
}

}  // namespace tendisplus
