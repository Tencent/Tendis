// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <unistd.h>
#include <iostream>
#include <string>
#include <fstream>
#include <utility>
#include <memory>
#include <vector>
#include <limits>
#include <algorithm>
#include <bitset>
#include <random>
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/time.h"
#include "tendisplus/utils/param_manager.h"
#include "tendisplus/utils/test_util.h"
#include "tendisplus/cluster/cluster_manager.h"
#include "tendisplus/utils/base64.h"
#include "tendisplus/utils/cursor_map.h"
#include "gtest/gtest.h"
#include "glog/logging.h"

namespace tendisplus {

int genRand() {
  int grand = 0;
  uint32_t ms = (uint32_t)nsSinceEpoch();
  grand = rand_r(reinterpret_cast<unsigned int*>(&ms));
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

TEST(Time, Common) {
  auto t3 = sinceEpoch();
  auto t4 = nsSinceEpoch();

  auto n = SCLOCK::now();
  auto t1 = sinceEpoch(n);
  auto t2 = nsSinceEpoch(n);

  EXPECT_EQ(t1, t2 / 1000000000);
  EXPECT_TRUE(t1 >= t3);
  EXPECT_TRUE(t2 >= t4);
  EXPECT_TRUE(msEpochToDatetime(t2 / 1000000) == nsEpochToDatetime(t2));

  LOG(INFO) << epochToDatetime(t1);
  LOG(INFO) << epochToDatetime(t3);
  LOG(INFO) << epochToDatetime(t4 / 1000000000);
  LOG(INFO) << epochToDatetime(t2 / 1000000000);
  LOG(INFO) << msEpochToDatetime(t2 / 1000000);
  LOG(INFO) << nsEpochToDatetime(t2);
}

std::bitset<CLUSTER_SLOTS> genBitMap() {
  std::bitset<CLUSTER_SLOTS> bitmap;

  auto count = genRand() % 5;
  while (count--) {
    size_t start = genRand() % CLUSTER_SLOTS;
    size_t length = genRand() % (CLUSTER_SLOTS - start);
    for (size_t j = start; j < start + length; j++) {
      bitmap.set(j);
    }
  }
  return bitmap;
}

TEST(String, Common) {
  std::stringstream ss;
  char buf[21000];

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
    std::vector<std::string> v2 = {"set", "a", "b"};
    EXPECT_EQ(v, v2);
  }
  {
    auto v = stringSplit("set a b ", " ");
    std::vector<std::string> v2 = {"set", "a", "b"};
    EXPECT_EQ(v, v2);
  }
  {
    auto v = stringSplit("set", " ");
    std::vector<std::string> v2 = {"set"};
    EXPECT_EQ(v, v2);
  }
  {
    auto v = stringSplit("set ", " ");
    std::vector<std::string> v2 = {"set"};
    EXPECT_EQ(v, v2);
  }
  {
    auto v = stringSplit("", " ");
    std::vector<std::string> v2 = {};
    EXPECT_EQ(v, v2);
  }
}

TEST(Base64, common) {
  std::string data = "aa";
  std::string encode =
    Base64::Encode((unsigned char*)data.c_str(), data.size());
  std::string decode = Base64::Decode(encode.c_str(), encode.size());
  EXPECT_EQ(data, decode);

  data =
    "MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM2222222222222222222222"
    "222222222222";
  encode = Base64::Encode((unsigned char*)data.c_str(), data.size());
  decode = Base64::Decode(encode.c_str(), encode.size());
  EXPECT_EQ(data, decode);

  data =
    "MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM22222222222222222222222" \
    "222222222 ------------------------------********************************"\
    "********************** ########################################zzzzzzzzz"\
    "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";
  encode = Base64::Encode((unsigned char*)data.c_str(), data.size());
  decode = Base64::Decode(encode.c_str(), encode.size());
  EXPECT_EQ(data, decode);
}

TEST(bitsetEncode, common) {
  std::bitset<CLUSTER_SLOTS> bitmap;
  for (size_t j = 0; j < bitmap.size(); j++) {
    if (j % 2 == 0) {
      bitmap.set(j);
    }
  }
  auto encodeStr = bitsetEncode(bitmap);
  auto v1 = bitsetDecode<CLUSTER_SLOTS>(encodeStr);
  EXPECT_EQ(v1.ok(), true);
  EXPECT_EQ(bitmap, v1.value());

  std::bitset<CLUSTER_SLOTS> bitmap2;
  for (size_t j = 0; j < bitmap2.size(); j++) {
    if (j % 2 == 0) {
      bitmap2.set(j);
    }
  }
  encodeStr = bitsetEncode(bitmap2);
  v1 = bitsetDecode<CLUSTER_SLOTS>(encodeStr);
  EXPECT_EQ(v1.ok(), true);
  EXPECT_EQ(bitmap2, v1.value());

  for (int i = 0; i < 100; i++) {
    std::bitset<CLUSTER_SLOTS> data = genBitMap();
    std::vector<uint16_t> encode = bitsetEncodeVec(data);
    auto v = bitsetDecodeVec<CLUSTER_SLOTS>(encode);
    EXPECT_EQ(v.ok(), true);
    std::bitset<CLUSTER_SLOTS> decode = v.value();
    EXPECT_EQ(data, decode);

    data = genBitMap();
    auto encodeStr = bitsetEncode(data);
    auto v1 = bitsetDecode<CLUSTER_SLOTS>(encodeStr);
    EXPECT_EQ(v1.ok(), true);
    EXPECT_EQ(data, v1.value());

    data = genBitMap();
    std::string bitmapStr = bitsetStrEncode(data);
    auto t = bitsetStrDecode<CLUSTER_SLOTS>(bitmapStr);
    EXPECT_EQ(t.ok(), true);
    EXPECT_EQ(data, t.value());
  }
}

TEST(ParamManager, common) {
  ParamManager pm;
  const char* argv[] = {"--skey1=value", "--ikey1=123"};
  pm.init(2, const_cast<char**>(argv));
  EXPECT_EQ(pm.getString("skey1"), "value");
  EXPECT_EQ(pm.getString("skey2"), "");
  EXPECT_EQ(pm.getString("skey3", "a"), "a");

  EXPECT_EQ(pm.getUint64("ikey1"), 123);
  EXPECT_EQ(pm.getUint64("ikey2"), 0);
  EXPECT_EQ(pm.getUint64("ikey3", 1), 1);
}

TEST(CursorMap, addmapping) {
  CursorMap map(5);
  auto &map_ = map.getMap();

  map.addMapping(1, {1, "1"});
  map.addMapping(2, {2, "2"});
  map.addMapping(3, {3, "3"});
  map.addMapping(4, {4, "4"});
  map.addMapping(5, {5, "5"});
  EXPECT_EQ(map_.size(), 5);
  EXPECT_TRUE(map_.count(1));
  EXPECT_TRUE(map_.count(2));
  EXPECT_TRUE(map_.count(3));
  EXPECT_TRUE(map_.count(4));
  EXPECT_TRUE(map_.count(5));

  map.addMapping(6, {6, "6"});
  EXPECT_EQ(map_.size(), 5);
  EXPECT_TRUE(map_.count(2));
  EXPECT_TRUE(map_.count(3));
  EXPECT_TRUE(map_.count(4));
  EXPECT_TRUE(map_.count(5));
  EXPECT_TRUE(map_.count(6));
}

TEST(CursorMap, getMapping) {
  CursorMap map(5);

  map.addMapping(1, {1, "1"});
  map.addMapping(2, {2, "2"});
  map.addMapping(3, {3, "3"});
  map.addMapping(4, {4, "4"});
  map.addMapping(5, {5, "5"});
  EXPECT_EQ(map.getMapping(1).value().kvstoreId, 1);
  EXPECT_FALSE(map.getMapping(10).ok());

  std::cout << std::endl << std::endl;

  map.addMapping(10, {10, "10"});

  EXPECT_EQ(map.getMapping(10).value().lastScanKey, "10");
  EXPECT_FALSE(map.getMapping(1).ok());
}

}  // namespace tendisplus
