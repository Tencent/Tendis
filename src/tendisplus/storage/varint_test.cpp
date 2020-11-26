// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <vector>
#include <algorithm>
#include <limits>
#include "tendisplus/storage/varint.h"
#include "gtest/gtest.h"

namespace tendisplus {

void testVarint(uint64_t val, std::vector<uint8_t> bytes) {
  EXPECT_EQ(bytes, varintEncode(val));

  auto str = varintEncodeStr(val);
  EXPECT_EQ(str.size(), bytes.size());
  EXPECT_EQ(
    memcmp(const_cast<void*>(reinterpret_cast<const void*>(str.c_str())),
           const_cast<void*>(reinterpret_cast<const void*>(bytes.data())),
           str.size()),
    0);
  EXPECT_EQ(str.size(), varintEncodeSize(val));

  auto expt = varintDecodeFwd(bytes.data(), bytes.size());
  EXPECT_TRUE(expt.ok());
  EXPECT_EQ(expt.value().first, val);
  EXPECT_EQ(expt.value().second, bytes.size());

  std::reverse(bytes.begin(), bytes.end());
  expt = varintDecodeRvs(bytes.data() + bytes.size() - 1, bytes.size());
  EXPECT_TRUE(expt.ok());
  EXPECT_EQ(expt.value().first, val);
  EXPECT_EQ(expt.value().second, bytes.size());

  // then, test trailing bytes
  std::reverse(bytes.begin(), bytes.end());
  // varint64 has a maxsize of 10
  uint8_t buf[10];
  memcpy(buf, bytes.data(), 10);
  uint8_t fills[] = {0, 0x7f, 0x80, 0xff};
  for (auto& v : fills) {
    memset(buf + bytes.size(), v, 10 - bytes.size());
    std::vector<uint8_t> tmp(buf, buf + 10);
    expt = varintDecodeFwd(tmp.data(), tmp.size());
    EXPECT_TRUE(expt.ok());
    EXPECT_EQ(expt.value().first, val);
    EXPECT_EQ(expt.value().second, bytes.size());

    std::reverse(tmp.begin(), tmp.end());
    expt = varintDecodeRvs(tmp.data() + tmp.size() - 1, tmp.size());
    EXPECT_TRUE(expt.ok());
    EXPECT_EQ(expt.value().first, val);
    EXPECT_EQ(expt.value().second, bytes.size());
  }
}

TEST(Varint, Common) {
  // NOTE(deyukong): the testdata are stolen from
  // facebook folly's varint testcases.
  testVarint(0, {0});
  testVarint(1, {1});
  testVarint(127, {127});
  testVarint(128, {0x80, 0x01});
  testVarint(300, {0xac, 0x02});
  testVarint(16383, {0xff, 0x7f});
  testVarint(16384, {0x80, 0x80, 0x01});

  testVarint(static_cast<uint32_t>(-1), {0xff, 0xff, 0xff, 0xff, 0x0f});
  testVarint(static_cast<uint64_t>(-1),
             {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01});
}

double genDouble() {
  // static int rank = 0;
  std::srand((int32_t)time(0));
  int r = std::rand();

  std::srand((int32_t)time(0));
  int r2 = std::rand();

  int x = r % 1111;
  int y = r2 % 111;
  return static_cast<double>(x * y) / 1111;
}

void testdouble(double val) {
  auto en = doubleEncode(val);
  auto ret = doubleDecode(en.data(), en.size());
  EXPECT_EQ(val, ret.value());

  // test align
  uint8_t buf[2 * sizeof(double)];
  for (size_t i = 0; i < sizeof(double); i++) {
    memcpy(buf + i, en.data(), en.size());
    auto ret1 = doubleDecode(buf + i, en.size());
    EXPECT_EQ(val, ret1.value());
  }
}

TEST(Double, Common) {
  testdouble(1);
  testdouble(1.0);
  testdouble(1.1);
  testdouble(0.00001);
  testdouble(10e2);

  for (int i = 0; i < 100000; i++) {
    double v = genDouble();
    testdouble(v);
  }
}


uint64_t genInt() {
  // static int rank = 0;
  std::srand(time(0));
  uint64_t r1 = std::rand();

  std::srand(time(0));
  uint64_t r2 = std::rand();

  uint64_t r = r1 * r2;

  if (r2 % 5 == 0) {
    r += std::numeric_limits<uint32_t>::max();
  }

  return r;
}

void testInt(uint64_t val) {
  char buf[sizeof(uint64_t) * 2];

  {
    uint16_t val16 = (uint16_t)val;

    auto v1 = int16Encode(val16);
    memcpy(buf, &v1, sizeof(v1));
    auto v2 = int16Decode(buf);
    EXPECT_EQ(v2, val16);

    auto s = int16Encode(buf, val16);
    EXPECT_EQ(s, sizeof(val16));
    v2 = int16Decode(buf);
    EXPECT_EQ(v2, val16);

    // test align
    for (size_t i = 0; i < sizeof(uint16_t); i++) {
      memcpy(buf + i, &v1, sizeof(v1));
      auto ret1 = int16Decode(buf + i);
      EXPECT_EQ(val16, ret1);
    }
  }

  {
    uint32_t val32 = (uint32_t)val;
    auto v1 = int32Encode(val32);
    memcpy(buf, &v1, sizeof(v1));
    auto v2 = int32Decode(buf);
    EXPECT_EQ(v2, val32);

    auto s = int32Encode(buf, val32);
    EXPECT_EQ(s, sizeof(val32));
    v2 = int32Decode(buf);
    EXPECT_EQ(v2, val32);

    // test align
    for (size_t i = 0; i < sizeof(uint32_t); i++) {
      memcpy(buf + i, &v1, sizeof(v1));
      auto ret1 = int32Decode(buf + i);
      EXPECT_EQ(val32, ret1);
    }
  }

  {
    uint64_t val64 = (uint64_t)val;
    auto v1 = int64Encode(val64);
    memcpy(buf, &v1, sizeof(v1));
    auto v2 = int64Decode(buf);
    EXPECT_EQ(v2, val64);

    auto s = int64Encode(buf, val64);
    EXPECT_EQ(s, sizeof(val64));
    v2 = int64Decode(buf);
    EXPECT_EQ(v2, val64);

    // test align
    for (size_t i = 0; i < sizeof(uint64_t); i++) {
      memcpy(buf + i, &v1, sizeof(v1));
      auto ret1 = int64Decode(buf + i);
      EXPECT_EQ(val64, ret1);
    }
  }
}

TEST(Int, Common) {
  testInt(1);
  testInt(1000);
  testInt(-1);
  testInt(-2);

  for (int i = 0; i < 100000; i++) {
    auto v = genInt();
    testInt(v);
  }
}

}  // namespace tendisplus
