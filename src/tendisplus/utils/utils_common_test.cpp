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
  CursorMap map(5, 2);
  auto &map_ = map.getMap();

  // CASE: mapping is not full, but one of alive sessions its mapping is full,
  //  evict session itself LRU mapping
  map.addMapping("1", 1, "1", 0);
  map.addMapping("2", 2, "2", 0);
  map.addMapping("3", 3, "3", 1);
  map.addMapping("4", 4, "4", 1);
  map.addMapping("5", 5, "5", 1);
  EXPECT_EQ(map_.size(), 4);
  EXPECT_TRUE(map_.count("1"));
  EXPECT_TRUE(map_.count("2"));
  EXPECT_FALSE(map_.count("3"));
  EXPECT_TRUE(map_.count("4"));
  EXPECT_TRUE(map_.count("5"));

  // CASE: mapping is NOT full, session its own mapping is NOT full,
  // can add mapping successfully
  map.addMapping("6", 6, "6", 2);
  EXPECT_EQ(map_.size(), 5);
  EXPECT_TRUE(map_.count("1"));
  EXPECT_TRUE(map_.count("2"));
  EXPECT_FALSE(map_.count("3"));
  EXPECT_TRUE(map_.count("4"));
  EXPECT_TRUE(map_.count("5"));
  EXPECT_TRUE(map_.count("6"));

  // CASE: new session comes,
  // but all mapping is full => evict LRU mapping which belong to other session
  map.addMapping("7", 7, "7", 3);
  EXPECT_EQ(map_.size(), 5);
  EXPECT_FALSE(map_.count("1"));
  EXPECT_TRUE(map_.count("2"));
  EXPECT_FALSE(map_.count("3"));
  EXPECT_TRUE(map_.count("4"));
  EXPECT_TRUE(map_.count("5"));
  EXPECT_TRUE(map_.count("6"));
  EXPECT_TRUE(map_.count("7"));

  auto expMapping = map.getMapping("7");
  EXPECT_TRUE(expMapping.ok());
  EXPECT_EQ(expMapping.value().kvstoreId, 7);
  EXPECT_EQ(expMapping.value().lastScanPos, "7");
  EXPECT_EQ(expMapping.value().sessionId, 3);

  // CASE: the same cursor has been added by different session
  // when different session comes, cursor-mapping will overwrite.
  map.addMapping("7", 10, "x", 4);
  EXPECT_EQ(map_.size(), 4);  // evict LRU mapping by global-level
  EXPECT_TRUE(map_.count("7"));

  expMapping = map.getMapping("7");
  EXPECT_TRUE(expMapping.ok());
  EXPECT_EQ(expMapping.value().kvstoreId, 10);
  EXPECT_EQ(expMapping.value().lastScanPos, "x");
  EXPECT_EQ(expMapping.value().sessionId, 4);
}

TEST(CursorMap, getMapping) {
  CursorMap map(5, 5);

  map.addMapping("1", 1, "1", 0);
  map.addMapping("2", 2, "2", 0);
  map.addMapping("3", 3, "3", 0);
  map.addMapping("4" , 4, "4", 0);
  map.addMapping("5", 5, "5", 0);
  EXPECT_EQ(map.getMapping("1").value().kvstoreId, 1);
  EXPECT_FALSE(map.getMapping("10").ok());

  map.addMapping("10", 10, "10", 0);
  EXPECT_EQ(map.getMapping("10").value().lastScanPos, "10");
  EXPECT_FALSE(map.getMapping("1").ok());
}

TEST(CursorMap, evictMapping) {
  CursorMap map(5, 5, 1);  // expired in one second

  map.addMapping("1", 1, "1", 0);
  map.addMapping("2", 2, "2", 0);
  map.addMapping("3", 3, "3", 0);
  map.addMapping("4", 4, "4", 0);
  map.addMapping("5", 5, "5", 0);
  EXPECT_EQ(map.getMapping("1").value().kvstoreId, 1);
  EXPECT_FALSE(map.getMapping("10").ok());

  std::this_thread::sleep_for(1s);
  auto m1 = map.getMapping("1");    // expired
  EXPECT_FALSE(m1.ok());
  EXPECT_TRUE(m1.status().toString().find("Mapping expired"));
  EXPECT_EQ(map.getMap().size(), 0);  // all mapping should be expired

  map.addMapping("1", 1, "1", 0);
  map.addMapping("2", 2, "2", 0);
  map.addMapping("3", 3, "3", 0);

  std::this_thread::sleep_for(1s);

  map.addMapping("4", 4, "4", 0);
  map.addMapping("5", 5, "5", 0);

  EXPECT_TRUE(map.getMapping("4").ok());    // not expired
  EXPECT_EQ(map.getMap().size(), 5);  // no mapping should be expired

  auto mm1 = map.getMapping("1");  // expired
  EXPECT_FALSE(mm1.ok());
  EXPECT_TRUE(mm1.status().toString().find("Mapping expired"));
  EXPECT_EQ(map.getMap().size(), 2);  // some mapping should be expired
}

/**
 * @brief used for simulate scan operation
 * @param totalScanSession max scan session
 * @param totalScanTimes each session scan times
 * @param map map pointer, use map reference isn't a good design.
 */
void testSimulateScanCmd(uint64_t totalScanSession,
                         uint64_t totalScanTimes,
                         CursorMap *map) {
  using namespace std::chrono_literals;  // NOLINT

  auto simulateScanCmd = [&](size_t step, size_t id) {
    thread_local static uint64_t cursor = 0;    // static data
    if (cursor) {
      auto expMapping = map->getMapping(std::to_string(cursor));
    //TODO(pecochen): check expMapping.ok()    // NOLINT
    //  ASSERT_TRUE(expMapping.ok());
    //  ASSERT_EQ(expMapping.value().lastScanKey, std::to_string(cursor));
    }
    cursor += step;                // simulate cursor by add step
    map->addMapping(std::to_string(cursor), 1, std::to_string(cursor), id);
  };

  std::vector<std::thread> threads;
  auto awakeTime = std::chrono::steady_clock::now() + 5s;

  // simulate scan operations, multi scan session at the same time.
  for (size_t session = 1; session <= totalScanSession; ++session) {
    threads.emplace_back([=]() {
      std::this_thread::sleep_until(awakeTime);
      for (size_t times = 0; times < totalScanTimes; ++times) {
        if ((totalScanSession < map->maxCursorCount() / map->maxSessionLimit())
            && (times == 10)) {
          std::this_thread::sleep_for(10s);
        }
        auto step = session;
        simulateScanCmd(step, session);
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }
}

TEST(CursorMap, simulateScanSessions) {
  // CASE 1: sessions < max support session (default is 100)
  //      scan times < maxSessionLimit
  // Expected result: _cursorMap.size() <= 50 * 100 (5000)
  //                 each set in _sessionTs, set.size() < 100
  {
    CursorMap map(10000, 100);
    const auto &map_ = map.getMap();
    const auto &sessionTs_ = map.getSessionTs();

    testSimulateScanCmd(50, 50, &map);

    EXPECT_LE(map_.size(), 5000);
    for (const auto &v : sessionTs_) {
      EXPECT_LT(v.second.size(), 100);
    }
  }

  // CASE 2: session < max support session (default is 100)
  //     scan times >> maxSessionLimit
  // Expected result: _cursorMap.size() <= 50 * 100 (5000)
  //                 each set in _sessionTs, set.size() <= 100
  {
    CursorMap map(10000, 100);
    const auto &map_ = map.getMap();
    const auto &sessionTs_ = map.getSessionTs();

    testSimulateScanCmd(50, 10000, &map);

    EXPECT_LE(map_.size(), 5000);
    for (const auto &v : sessionTs_) {
      EXPECT_LE(v.second.size(), 100);
    }
  }

  // CASE 3: session >> max support session (default is 100)
  //        scan times < maxSessionLimit
  // Expected result: _cursorMap.size() == 10000 (because of evict operation)
  //              each set in _sessionTs, set.size() < 100,
  //              it means, _cursorMap contains session > default 100 actually
  {
    CursorMap map(10000, 100);
    const auto &map_ = map.getMap();
    const auto &sessionTs_ = map.getSessionTs();

    testSimulateScanCmd(1000, 50, &map);

    EXPECT_LE(map_.size(), 10000);
    for (const auto &v : sessionTs_) {
      EXPECT_LT(v.second.size(), 100);
    }
  }

  // CASE 4: session >> max support session (default is 100)
  //         scan times >> maxSessionLimit
  // Expected result: _cursorMap.size() == 10000 (because of evict operation)
  //                each set in _sessionTs, set.size() == 100
  {
    CursorMap map(10000, 100);
    const auto &map_ = map.getMap();
    const auto &sessionTs_ = map.getSessionTs();

    testSimulateScanCmd(1000, 10000, &map);

    EXPECT_LE(map_.size(), 10000);
    for (const auto &v : sessionTs_) {
      // may cause 900 pair {id, set (=> .size() -> 0)},
      // should clean up useless pair
      EXPECT_LE(v.second.size(), 100);
    }
  }
}

TEST(KeyCursorMap, addmapping) {
  KeyCursorMap map(5, 2);

  // CASE: mapping is not full, but one of alive sessions its mapping is full,
  //  evict session itself LRU mapping
  map.addMapping("key", 1, 1, "1", 0);
  map.addMapping("key", 2, 2, "2", 0);
  map.addMapping("key", 3, 3, "3", 1);
  map.addMapping("key", 4, 4, "4", 1);
  map.addMapping("key", 5, 5, "5", 1);
  EXPECT_TRUE(map.getMapping("key", 1).ok());
  EXPECT_TRUE(map.getMapping("key", 2).ok());
  EXPECT_FALSE(map.getMapping("key", 3).ok());
  EXPECT_TRUE(map.getMapping("key", 4).ok());
  EXPECT_TRUE(map.getMapping("key", 5).ok());

  // CASE: mapping is NOT full, session its own mapping is NOT full,
  // can add mapping successfully
  map.addMapping("key", 6, 6, "6", 2);
  EXPECT_TRUE(map.getMapping("key", 1).ok());
  EXPECT_TRUE(map.getMapping("key", 2).ok());
  EXPECT_FALSE(map.getMapping("key", 3).ok());
  EXPECT_TRUE(map.getMapping("key", 4).ok());
  EXPECT_TRUE(map.getMapping("key", 5).ok());
  EXPECT_TRUE(map.getMapping("key", 6).ok());

  // CASE: new session comes,
  // but all mapping is full => evict LRU mapping which belong to other session
  map.addMapping("key", 7, 7, "7", 3);
  EXPECT_FALSE(map.getMapping("key", 1).ok());
  EXPECT_TRUE(map.getMapping("key", 2).ok());
  EXPECT_FALSE(map.getMapping("key", 3).ok());
  EXPECT_TRUE(map.getMapping("key", 4).ok());
  EXPECT_TRUE(map.getMapping("key", 5).ok());
  EXPECT_TRUE(map.getMapping("key", 6).ok());
  EXPECT_TRUE(map.getMapping("key", 7).ok());

  auto expMapping = map.getMapping("key", 7);
  EXPECT_TRUE(expMapping.ok());
  EXPECT_EQ(expMapping.value().kvstoreId, 7);
  EXPECT_EQ(expMapping.value().lastScanPos, "7");
  EXPECT_EQ(expMapping.value().sessionId, 3);

  // CASE: add different key cursor, even if the cursormap of
  // "key" is full, but "key1" use different cursormap
  map.addMapping("key1", 7, 7, "7", 3);
  EXPECT_FALSE(map.getMapping("key", 1).ok());
  EXPECT_TRUE(map.getMapping("key", 2).ok());
  EXPECT_FALSE(map.getMapping("key", 3).ok());
  EXPECT_TRUE(map.getMapping("key", 4).ok());
  EXPECT_TRUE(map.getMapping("key", 5).ok());
  EXPECT_TRUE(map.getMapping("key", 6).ok());
  EXPECT_TRUE(map.getMapping("key", 7).ok());
  EXPECT_TRUE(map.getMapping("key1", 7).ok());

  // CASE: the same cursor has been added by different session
  // when different session comes, cursor-mapping will overwrite.
  map.addMapping("key", 7, 10, "x", 4);
  EXPECT_TRUE(map.getMapping("key", 7).ok());

  expMapping = map.getMapping("key", 7);
  EXPECT_TRUE(expMapping.ok());
  EXPECT_EQ(expMapping.value().kvstoreId, 10);
  EXPECT_EQ(expMapping.value().lastScanPos, "x");
  EXPECT_EQ(expMapping.value().sessionId, 4);
}

}  // namespace tendisplus
