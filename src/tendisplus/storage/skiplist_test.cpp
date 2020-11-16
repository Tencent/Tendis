// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <fstream>
#include <utility>
#include <algorithm>
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "tendisplus/utils/status.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/portable.h"
#include "tendisplus/storage/skiplist.h"
#include "tendisplus/storage/kvstore.h"
#include "tendisplus/storage/rocks/rocks_kvstore.h"
#include "tendisplus/server/server_params.h"

namespace tendisplus {

std::shared_ptr<ServerParams> genParams() {
  srand(time(0));
  const auto guard = MakeGuard([] { remove("a.cfg"); });
  std::ofstream myfile;
  myfile.open("a.cfg");
  myfile << "bind 127.0.0.1\n";
  myfile << "port 8903\n";
  myfile << "loglevel debug\n";
  myfile << "logdir ./log\n";
  myfile << "storage rocks\n";
  myfile << "dir ./db\n";
  myfile << "rocks.blockcachemb 4096\n";
  myfile.close();
  auto cfg = std::make_shared<ServerParams>();
  auto s = cfg->parseFile("a.cfg");
  EXPECT_EQ(s.ok(), true) << s.toString();
  return cfg;
}

TEST(SkipList, BackWardTail) {
  auto cfg = genParams();
  EXPECT_TRUE(filesystem::create_directory("db"));
  EXPECT_TRUE(filesystem::create_directory("log"));
  const auto guard = MakeGuard([] {
    filesystem::remove_all("./log");
    filesystem::remove_all("./db");
  });
  auto blockCache =
    rocksdb::NewLRUCache(cfg->rocksBlockcacheMB * 1024 * 1024LL, 4);
  auto store = std::shared_ptr<KVStore>(new RocksKVStore("0", cfg, blockCache));
  auto eTxn1 = store->createTransaction(nullptr);
  EXPECT_TRUE(eTxn1.ok());

  ZSlMetaValue meta(1, 1, 0);
  RecordValue rv(meta.encode(), RecordType::RT_ZSET_META, -1);
  RecordKey mk(0, 0, RecordType::RT_ZSET_META, "test", "");
  Status s = store->setKV(mk, rv, eTxn1.value().get());
  EXPECT_TRUE(s.ok());

  RecordKey head(0,
                 0,
                 RecordType::RT_ZSET_S_ELE,
                 "test",
                 std::to_string(ZSlMetaValue::HEAD_ID));
  ZSlEleValue headVal;
  RecordValue subRv(headVal.encode(), RecordType::RT_ZSET_S_ELE, -1);

  s = store->setKV(head, subRv, eTxn1.value().get());
  EXPECT_TRUE(s.ok());

  Expected<uint64_t> commitStatus = eTxn1.value()->commit();
  EXPECT_TRUE(commitStatus.ok());

  SkipList sl(0, 0, "test", meta, store);
  constexpr uint32_t CNT = 1000;
  std::vector<uint32_t> keys;
  for (uint32_t i = 1; i <= CNT; ++i) {
    keys.push_back(i);
  }

  uint32_t currMax = 0;
  std::random_shuffle(keys.begin(), keys.end());
  // check tail always points to the max num
  for (auto& i : keys) {
    currMax = std::max(currMax, i);
    auto eTxn = store->createTransaction(nullptr);
    EXPECT_TRUE(eTxn.ok());
    Status s = sl.insert(i, std::to_string(i), eTxn.value().get());
    EXPECT_TRUE(s.ok());
    s = sl.save(eTxn.value().get(), {ErrorCodes::ERR_NOTFOUND, ""}, -1);
    EXPECT_TRUE(s.ok());

    std::string pointerStr = std::to_string(sl.getTail());
    RecordKey rk(0, 0, RecordType::RT_ZSET_S_ELE, "test", pointerStr);
    Expected<RecordValue> rv = store->getKV(rk, eTxn.value().get());
    EXPECT_TRUE(rv.ok()) << rv.status().toString();

    const std::string& ss = rv.value().getValue();
    auto result = ZSlEleValue::decode(ss);
    EXPECT_TRUE(result.ok()) << result.status().toString();
    EXPECT_EQ(result.value().getScore(), currMax);

    Expected<uint64_t> commitStatus = eTxn.value()->commit();
    EXPECT_TRUE(commitStatus.ok());
  }

  // randomly erase 500 elements
  std::random_shuffle(keys.begin(), keys.end());
  for (uint32_t i = 0; i < CNT / 2; ++i) {
    auto eTxn = store->createTransaction(nullptr);
    EXPECT_TRUE(eTxn.ok());
    Status s = sl.remove(
      keys[CNT - i - 1], std::to_string(keys[CNT - i - 1]), eTxn.value().get());
    EXPECT_TRUE(s.ok());
    s = sl.save(eTxn.value().get(), {ErrorCodes::ERR_NOTFOUND, ""}, -1);
    EXPECT_TRUE(s.ok());

    keys.pop_back();
    currMax = *std::max_element(keys.begin(), keys.end());

    std::string pointerStr = std::to_string(sl.getTail());
    RecordKey rk(0, 0, RecordType::RT_ZSET_S_ELE, "test", pointerStr);
    Expected<RecordValue> rv = store->getKV(rk, eTxn.value().get());
    EXPECT_TRUE(rv.ok()) << rv.status().toString();

    const std::string& ss = rv.value().getValue();
    auto result = ZSlEleValue::decode(ss);
    EXPECT_TRUE(result.ok()) << result.status().toString();
    EXPECT_EQ(result.value().getScore(), currMax);

    Expected<uint64_t> commitStatus = eTxn.value()->commit();
    EXPECT_TRUE(commitStatus.ok());
  }

  // reload
  auto eTxn2 = store->createTransaction(nullptr);
  EXPECT_TRUE(eTxn2.ok());
  Expected<RecordValue> eMeta = store->getKV(mk, eTxn2.value().get());
  auto eMetaContent = ZSlMetaValue::decode(eMeta.value().getValue());
  EXPECT_TRUE(eMetaContent.ok());
  meta = eMetaContent.value();
  EXPECT_EQ(meta.getCount(), CNT / 2 + 1);
  SkipList sl2(mk.getChunkId(), mk.getDbId(), mk.getPrimaryKey(), meta, store);
  std::sort(keys.begin(), keys.end());
  uint64_t now = sl2.getTail();
  for (uint32_t i = CNT / 2; i >= 1; --i) {
    std::string pointerStr = std::to_string(now);
    RecordKey rk(0, 0, RecordType::RT_ZSET_S_ELE, "test", pointerStr);
    Expected<RecordValue> rv = store->getKV(rk, eTxn2.value().get());
    EXPECT_TRUE(rv.ok()) << rv.status().toString();

    const std::string& ss = rv.value().getValue();
    auto result = ZSlEleValue::decode(ss);
    EXPECT_TRUE(result.ok()) << result.status().toString();
    EXPECT_EQ(result.value().getScore(), keys[i - 1]);
    now = result.value().getBackward();
  }
}

TEST(SkipList, Mix) {
  auto cfg = genParams();
  EXPECT_TRUE(filesystem::create_directory("db"));
  EXPECT_TRUE(filesystem::create_directory("log"));
  const auto guard = MakeGuard([] {
    filesystem::remove_all("./log");
    filesystem::remove_all("./db");
  });
  auto blockCache =
    rocksdb::NewLRUCache(cfg->rocksBlockcacheMB * 1024 * 1024LL, 4);
  auto store = std::shared_ptr<KVStore>(new RocksKVStore("0", cfg, blockCache));
  auto eTxn1 = store->createTransaction(nullptr);
  EXPECT_TRUE(eTxn1.ok());

  ZSlMetaValue meta(1, 1, 0);
  RecordValue rv(meta.encode(), RecordType::RT_ZSET_META, -1);
  RecordKey mk(0, 0, RecordType::RT_ZSET_META, "test", "");
  Status s = store->setKV(mk, rv, eTxn1.value().get());
  EXPECT_TRUE(s.ok());

  RecordKey head(0,
                 0,
                 RecordType::RT_ZSET_S_ELE,
                 "test",
                 std::to_string(ZSlMetaValue::HEAD_ID));
  ZSlEleValue headVal;
  RecordValue subRv(headVal.encode(), RecordType::RT_ZSET_S_ELE, -1);

  s = store->setKV(head, subRv, eTxn1.value().get());
  EXPECT_TRUE(s.ok());

  Expected<uint64_t> commitStatus = eTxn1.value()->commit();
  EXPECT_TRUE(commitStatus.ok());

  SkipList sl(0, 0, "test", meta, store);

  std::vector<uint32_t> keys;

  constexpr uint32_t CNT = 1000;
  for (uint32_t i = 1; i <= CNT; ++i) {
    keys.push_back(i);
  }
  std::random_shuffle(keys.begin(), keys.end());
  for (auto& i : keys) {
    auto eTxn = store->createTransaction(nullptr);
    EXPECT_TRUE(eTxn.ok());
    Status s = sl.insert(i, std::to_string(i), eTxn.value().get());
    EXPECT_TRUE(s.ok());
    s = sl.save(eTxn.value().get(), {ErrorCodes::ERR_NOTFOUND, ""}, -1);
    EXPECT_TRUE(s.ok());
    Expected<uint64_t> commitStatus = eTxn.value()->commit();
    EXPECT_TRUE(commitStatus.ok());
  }

  auto eTxn = store->createTransaction(nullptr);
  EXPECT_TRUE(eTxn.ok());

  s = sl.remove(5, std::to_string(5), eTxn.value().get());
  EXPECT_TRUE(s.ok());
  s = sl.save(eTxn.value().get(), {ErrorCodes::ERR_NOTFOUND, ""}, -1);
  EXPECT_TRUE(s.ok());

  s = sl.insert(1, std::to_string(5), eTxn.value().get());
  EXPECT_TRUE(s.ok());

  Expected<uint32_t> expRank =
    sl.rank(1, std::to_string(5), eTxn.value().get());
  EXPECT_TRUE(expRank.ok());
  EXPECT_EQ(expRank.value(), 2U);

  auto sc = eTxn.value()->commit();
  EXPECT_TRUE(sc.ok());
}

TEST(SkipList, InsertDelSameKeys) {
  auto cfg = genParams();
  EXPECT_TRUE(filesystem::create_directory("db"));
  EXPECT_TRUE(filesystem::create_directory("log"));
  const auto guard = MakeGuard([] {
    filesystem::remove_all("./log");
    filesystem::remove_all("./db");
  });
  auto blockCache =
    rocksdb::NewLRUCache(cfg->rocksBlockcacheMB * 1024 * 1024LL, 4);
  auto store = std::shared_ptr<KVStore>(new RocksKVStore("0", cfg, blockCache));
  auto eTxn1 = store->createTransaction(nullptr);
  EXPECT_TRUE(eTxn1.ok());

  // init a key
  ZSlMetaValue meta(1, 1, 0);
  RecordValue rv(meta.encode(), RecordType::RT_ZSET_META, -1);
  RecordKey mk(0, 0, RecordType::RT_ZSET_META, "skiplistkey", "");
  Status s = store->setKV(mk, rv, eTxn1.value().get());
  EXPECT_TRUE(s.ok());

  RecordKey head(0,
                 0,
                 RecordType::RT_ZSET_S_ELE,
                 "skiplistkey",
                 std::to_string(ZSlMetaValue::HEAD_ID));
  ZSlEleValue headVal;
  RecordValue subRv(headVal.encode(), RecordType::RT_ZSET_S_ELE, -1);

  s = store->setKV(head, subRv, eTxn1.value().get());
  EXPECT_TRUE(s.ok());

  Expected<uint64_t> commitStatus = eTxn1.value()->commit();
  EXPECT_TRUE(commitStatus.ok());

  bool exists1 = false;
  bool exists2 = false;
  uint64_t score1 = 0;
  uint64_t score2 = 0;
  for (int i = 0; i < 10; ++i) {
    for (const auto& k : {std::string("k1"), std::string("k2")}) {
      RecordKey mk(0, 0, RecordType::RT_ZSET_META, "skiplistkey", "");
      auto eTxn = store->createTransaction(nullptr);
      EXPECT_TRUE(eTxn.ok());
      Expected<RecordValue> eMeta = store->getKV(mk, eTxn.value().get());
      auto eMetaContent = ZSlMetaValue::decode(eMeta.value().getValue());
      EXPECT_TRUE(eMetaContent.ok());
      auto meta = eMetaContent.value();
      SkipList sl(0, 0, "skiplistkey", meta, store);
      Status s;
      unsigned int seed = 123;
      if (k == "k1") {
        if (exists1) {
          s = sl.remove(score1, k, eTxn.value().get());
          EXPECT_TRUE(s.ok()) << k << ' ' << s.toString();
          exists1 = false;
          score1 = 0;
        } else {
          exists1 = true;
          score1 = rand_r(&seed);
          s = sl.insert(score1, k, eTxn.value().get());
          EXPECT_TRUE(s.ok()) << k << ' ' << s.toString();
        }
      } else {
        if (exists2) {
          s = sl.remove(score2, k, eTxn.value().get());
          EXPECT_TRUE(s.ok()) << k << ' ' << s.toString();
          exists2 = false;
          score2 = 0;
        } else {
          exists2 = true;
          score2 = rand_r(&seed);
          s = sl.insert(score2, k, eTxn.value().get());
          EXPECT_TRUE(s.ok()) << k << ' ' << s.toString();
        }
      }
      std::stringstream ss;
      // if (!s.ok()) {
      sl.traverse(ss, eTxn.value().get());
      // }
      LOG(INFO) << "\n" << ss.str();
      s = sl.save(eTxn.value().get(), {ErrorCodes::ERR_NOTFOUND, ""}, -1);
      EXPECT_TRUE(s.ok()) << s.toString();
      eTxn.value()->commit();
    }
  }
}

TEST(SkipList, Common) {
  auto cfg = genParams();
  EXPECT_TRUE(filesystem::create_directory("db"));
  EXPECT_TRUE(filesystem::create_directory("log"));
  const auto guard = MakeGuard([] {
    filesystem::remove_all("./log");
    filesystem::remove_all("./db");
  });
  auto blockCache =
    rocksdb::NewLRUCache(cfg->rocksBlockcacheMB * 1024 * 1024LL, 4);
  auto store = std::shared_ptr<KVStore>(new RocksKVStore("0", cfg, blockCache));
  auto eTxn1 = store->createTransaction(nullptr);
  EXPECT_TRUE(eTxn1.ok());

  ZSlMetaValue meta(1, 1, 0);
  RecordValue rv(meta.encode(), RecordType::RT_ZSET_META, -1);
  RecordKey mk(0, 0, RecordType::RT_ZSET_META, "test", "");
  Status s = store->setKV(mk, rv, eTxn1.value().get());
  EXPECT_TRUE(s.ok());

  RecordKey head(0,
                 0,
                 RecordType::RT_ZSET_S_ELE,
                 "test",
                 std::to_string(ZSlMetaValue::HEAD_ID));
  ZSlEleValue headVal;
  RecordValue subRv(headVal.encode(), RecordType::RT_ZSET_S_ELE, -1);

  s = store->setKV(head, subRv, eTxn1.value().get());
  EXPECT_TRUE(s.ok());

  Expected<uint64_t> commitStatus1 = eTxn1.value()->commit();
  EXPECT_TRUE(commitStatus1.ok());

  SkipList sl(0, 0, "test", meta, store);

  std::vector<uint32_t> keys;
  std::vector<uint32_t> sortedkeys;

  // sl.save one time
  uint32_t CNT = 1000;
  for (uint32_t i = 1; i <= CNT; ++i) {
    keys.push_back(i);
  }
  std::random_shuffle(keys.begin(), keys.end());
  auto eTxn2 = store->createTransaction(nullptr);
  EXPECT_TRUE(eTxn2.ok());
  for (auto& i : keys) {
    Status s = sl.insert(i, std::to_string(i), eTxn2.value().get());
    EXPECT_TRUE(s.ok());
  }
  s = sl.save(eTxn2.value().get(), {ErrorCodes::ERR_NOTFOUND, ""}, -1);
  EXPECT_TRUE(s.ok());
  Expected<uint64_t> commitStatus2 = eTxn2.value()->commit();
  EXPECT_TRUE(commitStatus2.ok());

  EXPECT_EQ(sl.getCount(), sl.nUpdated);

  // sl.save every insert
  keys.clear();
  uint32_t tmp = CNT;
  for (uint32_t i = 1; i <= CNT; ++i) {
    keys.push_back(i + tmp);
  }
  std::random_shuffle(keys.begin(), keys.end());
  for (auto& i : keys) {
    auto eTxn = store->createTransaction(nullptr);
    EXPECT_TRUE(eTxn.ok());
    Status s = sl.insert(i, std::to_string(i), eTxn.value().get());
    EXPECT_TRUE(s.ok());
    s = sl.save(eTxn.value().get(), {ErrorCodes::ERR_NOTFOUND, ""}, -1);
    EXPECT_TRUE(s.ok());
    Expected<uint64_t> commitStatus = eTxn.value()->commit();
    EXPECT_TRUE(commitStatus.ok());
  }
  CNT += tmp;

  for (uint32_t i = 1; i <= CNT; ++i) {
    auto eTxn = store->createTransaction(nullptr);
    EXPECT_TRUE(eTxn.ok());
    Expected<uint32_t> expRank =
      sl.rank(i, std::to_string(i), eTxn.value().get());
    EXPECT_TRUE(expRank.ok());
    EXPECT_EQ(expRank.value(), i);
  }
  std::cout << "SkipList:"
            << " size(" << sl.getCount()
            << ") "
               "level("
            << (int32_t)sl.getLevel() << ") " << std::endl;
  {
    // auto eTxn = store->createTransaction();
    // std::stringstream ss;
    // sl.traverse(ss, eTxn.value().get());
    // std::cout << ss.str() << std::endl;
  }  // head also counts
  EXPECT_EQ(sl.getCount(), CNT + 1);
  EXPECT_EQ(sl.getAlloc(), CNT + ZSlMetaValue::MIN_POS);
  for (uint32_t i = 1; i <= CNT; ++i) {
    auto eTxn = store->createTransaction(nullptr);
    EXPECT_TRUE(eTxn.ok());

    Expected<uint32_t> expRank =
      sl.rank(CNT, std::to_string(CNT), eTxn.value().get());
    EXPECT_TRUE(expRank.ok());
    EXPECT_EQ(expRank.value(), CNT - i + 1);

    for (size_t j = CNT; j > i; j--) {
      Expected<uint32_t> expRank =
        sl.rank(j, std::to_string(j), eTxn.value().get());
      EXPECT_TRUE(expRank.ok());
      EXPECT_EQ(expRank.value(), j - i + 1);
    }

    Status s = sl.remove(i, std::to_string(i), eTxn.value().get());
    EXPECT_TRUE(s.ok());
    s = sl.save(eTxn.value().get(), {ErrorCodes::ERR_NOTFOUND, ""}, -1);
    EXPECT_TRUE(s.ok());

    // std::stringstream ss;
    // sl.traverse(ss, eTxn.value().get());
    // std::cout<<ss.str() << std::endl;

    Expected<uint64_t> commitStatus = eTxn.value()->commit();
    EXPECT_TRUE(commitStatus.ok());
  }
  EXPECT_EQ(sl.getCount(), 1U);
  EXPECT_EQ(sl.getAlloc(), CNT + ZSlMetaValue::MIN_POS);
  LOG(INFO) << "skiplist level:" << static_cast<uint32_t>(sl.getLevel());
}

}  // namespace tendisplus
