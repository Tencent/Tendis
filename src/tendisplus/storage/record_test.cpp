// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <time.h>
#include <cstdlib>
#include <string>
#include <vector>
#include <set>
#include <algorithm>
#include <limits>
#include "tendisplus/storage/record.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/test_util.h"
#include "tendisplus/utils/time.h"
#include "gtest/gtest.h"

namespace tendisplus {

int genRand() {
  int grand = 0;
  uint32_t ms = nsSinceEpoch();
  grand = rand_r(reinterpret_cast<unsigned int*>(&ms));
  return grand;
}

RecordType randomType() {
  switch ((genRand() % 13)) {
    case 0:
      return RecordType::RT_META;
    case 1:
      return RecordType::RT_KV;
    case 2:
      return RecordType::RT_LIST_META;
    case 3:
      return RecordType::RT_LIST_ELE;
    case 4:
      return RecordType::RT_HASH_ELE;
    case 5:
      return RecordType::RT_HASH_META;
    case 6:
      return RecordType::RT_SET_ELE;
    case 7:
      return RecordType::RT_SET_META;
    case 8:
      return RecordType::RT_ZSET_H_ELE;
    case 9:
      return RecordType::RT_ZSET_S_ELE;
    case 10:
      return RecordType::RT_ZSET_META;
    case 11:
      return RecordType::RT_TTL_INDEX;
    case 12:
      return RecordType::RT_BINLOG;
    default:
      return RecordType::RT_INVALID;
  }
}

ReplFlag randomReplFlag() {
  switch ((genRand() % 4)) {
    case 0:
      return ReplFlag::REPL_GROUP_MID;
    case 1:
      return ReplFlag::REPL_GROUP_START;
    case 2:
      return ReplFlag::REPL_GROUP_END;
    case 3:
      return static_cast<ReplFlag>((uint16_t)ReplFlag::REPL_GROUP_START |
                                   (uint16_t)ReplFlag::REPL_GROUP_END);
    default:
      INVARIANT(0);
      // void compiler complain
      return ReplFlag::REPL_GROUP_MID;
  }
}

ReplOp randomReplOp() {
  switch ((genRand() % 3)) {
    case 0:
      return ReplOp::REPL_OP_NONE;
    case 1:
      return ReplOp::REPL_OP_SET;
    case 2:
      return ReplOp::REPL_OP_DEL;
    default:
      INVARIANT(0);
      // void compiler complain
      return ReplOp::REPL_OP_NONE;
  }
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

std::string overflip(const std::string& s) {
  std::vector<uint8_t> buf;
  buf.insert(buf.end(), s.begin(), s.end());
  size_t size = buf.size();
  auto ori1 = buf[size - 2];
  while (ori1 == buf[size - 2]) {
    buf[size - 2] = genRand() % 256;
  }
  return std::string(reinterpret_cast<const char*>(buf.data()), buf.size());
}

TEST(Record, MinRec) {
  auto rk = RecordKey(0, 0, RecordType::RT_DATA_META, "", "");
  auto rv = RecordValue("", RecordType::RT_KV, -1);

  auto minSize = rk.encode().size();
  EXPECT_TRUE(minSize == RecordKey::minSize());
  minSize = rv.encode().size();
  EXPECT_TRUE(minSize == RecordValue::minSize());
}

TEST(Record, Common) {
  srand((unsigned int)time(NULL));
#ifdef _WIN32
  for (size_t i = 0; i < 100; i++) {
#else
  for (size_t i = 0; i < 1000000; i++) {
#endif
    uint32_t dbid = genRand();
    uint32_t chunkid = genRand();
    auto type = randomType();
    auto pk = randomStr(5, false);
    auto sk = randomStr(5, true);
    uint64_t ttl =
      static_cast<uint64_t>(genRand()) * static_cast<uint64_t>(genRand());
    uint64_t cas =
      static_cast<uint64_t>(genRand()) * static_cast<uint64_t>(genRand());
    uint64_t version = 0;
    uint64_t versionEP =
      static_cast<uint64_t>(genRand()) * static_cast<uint64_t>(genRand());
    auto val = randomStr(5, true);
    uint64_t pieceSize = (uint64_t)-1;
    if (!isDataMetaType(type)) {
      versionEP = -1;
      ttl = 0;
      cas = -1;
    }

    auto rk = RecordKey(chunkid, dbid, type, pk, sk);
    auto rv = RecordValue(val, type, versionEP, ttl, cas, version, pieceSize);
    auto rcd = Record(rk, rv);
    auto kv = rcd.encode();

    auto validateK = RecordKey::validate(kv.first);
    EXPECT_TRUE(validateK.ok());
    EXPECT_TRUE(validateK.value());

    auto validateV = RecordValue::validate(kv.second);
    EXPECT_TRUE(validateV.ok());
    EXPECT_TRUE(validateV.value());

    auto hdrSize = RecordValue::decodeHdrSize(kv.second);
    EXPECT_TRUE(hdrSize.ok());
    EXPECT_EQ(hdrSize.value() + val.size(), kv.second.size());
    if (!isDataMetaType(type)) {
      EXPECT_EQ(hdrSize.value(),
                RecordValue::decodeHdrSizeNoMeta(kv.second).value());
    }

    auto prcd1 = Record::decode(kv.first, kv.second);
    auto type_ = RecordKey::decodeType(kv.first.c_str(), kv.first.size());
    auto ttl_ = RecordValue::decodeTtl(kv.second.c_str(), kv.second.size());

    EXPECT_EQ(cas, rv.getCas());
    EXPECT_EQ(version, rv.getVersion());
    EXPECT_EQ(versionEP, rv.getVersionEP());
    EXPECT_EQ(pieceSize, rv.getPieceSize());
    EXPECT_EQ(type, rv.getRecordType());
    EXPECT_EQ(ttl, rv.getTtl());
    EXPECT_EQ(rk.getRecordValueType(), type);
    EXPECT_EQ(rk.getRecordValueType(), rv.getRecordType());
    EXPECT_EQ(getRealKeyType(rk.getRecordValueType()), rk.getRecordType());

    EXPECT_EQ(getRealKeyType(type), rk.getRecordType());
    EXPECT_EQ(type_, getRealKeyType(type));
    EXPECT_EQ(ttl_, ttl);
    EXPECT_TRUE(prcd1.ok());
    EXPECT_EQ(prcd1.value(), rcd);
  }
}

TEST(ReplRecordV2, Prefix) {
  uint64_t binlogid =
    (uint64_t)genRand() + std::numeric_limits<uint32_t>::max();
  auto rlk = ReplLogKeyV2(binlogid);
  RecordKey rk(ReplLogKeyV2::CHUNKID,
               ReplLogKeyV2::DBID,
               RecordType::RT_BINLOG,
               rlk.encode(),
               "");
  const std::string s = rk.encode();
  EXPECT_EQ(s[0], '\xff');
  EXPECT_EQ(s[1], '\xff');
  EXPECT_EQ(s[2], '\xff');
  EXPECT_EQ(s[3], '\x01');
  EXPECT_EQ(s[4], '\xff');
  EXPECT_EQ(s[5], '\xff');
  EXPECT_EQ(s[6], '\xff');
  EXPECT_EQ(s[7], '\xff');
  EXPECT_EQ(s[8], '\x01');
  const std::string& prefix = RecordKey::prefixReplLogV2();
  EXPECT_EQ(prefix[0], '\xff');
  EXPECT_EQ(prefix[1], '\xff');
  EXPECT_EQ(prefix[2], '\xff');
  EXPECT_EQ(prefix[3], '\x01');
  EXPECT_EQ(prefix[4], '\xff');
  EXPECT_EQ(prefix[5], '\xff');
  EXPECT_EQ(prefix[6], '\xff');
  EXPECT_EQ(prefix[7], '\xff');
  EXPECT_EQ(prefix[8], '\x01');
}

TEST(ReplRecordV2, Common) {
  srand(time(NULL));
#ifdef _WIN32
  for (size_t i = 0; i < 10; i++) {
#else
  for (size_t i = 0; i < 1000; i++) {
#endif
    uint64_t txnid = uint64_t(genRand()) * uint64_t(genRand());
    uint64_t binlogid = uint64_t(genRand()) * uint64_t(genRand());
    uint64_t versionEp = uint64_t(genRand()) * uint64_t(genRand());
    uint32_t chunkid = genRand() % 16384;

    ReplFlag flag = randomReplFlag();
    uint64_t timestamp =
      (uint64_t)genRand() + std::numeric_limits<uint32_t>::max() + 1;

    auto rk = ReplLogKeyV2(binlogid);
    auto rkStr = rk.encode();
    auto prk = ReplLogKeyV2::decode(rkStr);
    EXPECT_TRUE(prk.ok());
    EXPECT_EQ(prk.value(), rk);

    size_t count = genRand() % 12345;

    std::vector<ReplLogValueEntryV2> vec;
    vec.reserve(count);
    for (size_t j = 0; j < count; j++) {
      uint64_t timestamp =
        (uint64_t)genRand() + std::numeric_limits<uint32_t>::max() + 1;

      size_t keyLen = genRand() % 128;
      size_t valLen = genRand() % 1024;

      ReplLogValueEntryV2 entry;
      if (genRand() % 2 == 0) {
        entry = ReplLogValueEntryV2(ReplOp::REPL_OP_SET,
                                    timestamp,
                                    randomStr(keyLen, false),
                                    randomStr(valLen, true));
      } else {
        entry = ReplLogValueEntryV2(
          ReplOp::REPL_OP_DEL, timestamp, randomStr(keyLen, false), "");
      }

      timestamp += 1;
      auto entryStr = entry.encode();
      size_t size = 0;
      auto pentry =
        ReplLogValueEntryV2::decode(entryStr.c_str(), entryStr.size(), &size);
      EXPECT_TRUE(pentry.ok());
      EXPECT_EQ(pentry.value(), entry);
      EXPECT_EQ(pentry.value().encodeSize(), entryStr.size());
      EXPECT_EQ(pentry.value().encodeSize(), size);

      vec.emplace_back(entry);
    }

    auto cmd = randomStr(50, false);
    auto rv = ReplLogValueV2(
      chunkid, flag, txnid, timestamp, versionEp, cmd, nullptr, 0);
    auto rvStr = rv.encode(vec);
    auto prv = ReplLogValueV2::decode(rvStr);
    EXPECT_TRUE(prv.ok());
    EXPECT_TRUE(rv.isEqualHdr(prv.value()));
    EXPECT_EQ(prv.value().getChunkId(), chunkid);
    EXPECT_EQ(prv.value().getReplFlag(), flag);
    EXPECT_EQ(prv.value().getTxnId(), txnid);
    EXPECT_EQ(prv.value().getTimestamp(), timestamp);
    EXPECT_EQ(prv.value().getVersionEp(), versionEp);
    EXPECT_EQ(prv.value().getCmd(), cmd);

    size_t offset = prv.value().getHdrSize();
    auto desc = prv.value().getData();
    size_t datasize = prv.value().getDataSize();

    size_t j = 0;
    while (offset < datasize) {
      const ReplLogValueEntryV2& entry = vec[j++];
      size_t size = 0;
      auto v = ReplLogValueEntryV2::decode(
        (const char*)desc + offset, datasize - offset, &size);
      EXPECT_TRUE(v.ok());
      offset += size;
      EXPECT_EQ(size, v.value().encodeSize());
      EXPECT_EQ(entry, v.value());
    }
    EXPECT_EQ(offset, datasize);

    auto expRepllog = ReplLogV2::decode(rkStr, rvStr);
    EXPECT_TRUE(expRepllog.ok());
    EXPECT_TRUE(expRepllog.value().getReplLogValue().isEqualHdr(rv));
    EXPECT_EQ(expRepllog.value().getReplLogValueEntrys().size(), vec.size());
    EXPECT_EQ(expRepllog.value().getReplLogValue().getCmd(), cmd);
    for (j = 0; j < vec.size(); j++) {
      const ReplLogValueEntryV2& entry = vec[j];
      const ReplLogValueEntryV2& entry2 =
        expRepllog.value().getReplLogValueEntrys()[j];
      EXPECT_EQ(entry, entry2);
    }
  }
}

TEST(TTLIndex, Prefix) {
  auto rlk = TTLIndex("abc", RecordType::RT_KV, 0, 10);
  RecordKey rk(TTLIndex::CHUNKID,
               TTLIndex::DBID,
               RecordType::RT_TTL_INDEX,
               rlk.encode(),
               "");
  const std::string s = rk.encode();
  char rt = rt2Char(RecordType::RT_TTL_INDEX);
  EXPECT_EQ(s[0], '\xff');
  EXPECT_EQ(s[1], '\xff');
  EXPECT_EQ(s[2], '\x00');
  EXPECT_EQ(s[3], '\x00');
  EXPECT_EQ(s[4], rt);
  EXPECT_EQ(s[5], '\xff');
  EXPECT_EQ(s[6], '\xff');
  EXPECT_EQ(s[7], '\x00');
  EXPECT_EQ(s[8], '\x00');
  const std::string& prefix = RecordKey::prefixTTLIndex();
  EXPECT_EQ(prefix[0], '\xff');
  EXPECT_EQ(prefix[1], '\xff');
  EXPECT_EQ(prefix[2], '\x00');
  EXPECT_EQ(prefix[3], '\x00');
  EXPECT_EQ(prefix[4], rt);
  EXPECT_EQ(prefix[5], '\xff');
  EXPECT_EQ(prefix[6], '\xff');
  EXPECT_EQ(prefix[7], '\x00');
  EXPECT_EQ(prefix[8], '\x00');
}

TEST(ZSl, Common) {
  srand(time(NULL));
#ifdef _WIN32
  size_t num = 100;
#else
  size_t num = 1000000;
#endif
  for (size_t i = 0; i < num; i++) {
    // uint8_t maxLvl = genRand() % std::numeric_limits<uint8_t>::max();
    uint8_t maxLvl = ZSlMetaValue::MAX_LAYER;
    uint8_t lvl = genRand() % maxLvl;
    uint32_t count = static_cast<uint32_t>(genRand());
    uint64_t tail = static_cast<uint64_t>(genRand()) * genRand();
    ZSlMetaValue m(lvl, count, tail);
    EXPECT_EQ(m.getMaxLevel(), maxLvl);
    EXPECT_EQ(m.getLevel(), lvl);
    EXPECT_EQ(m.getCount(), count);
    EXPECT_EQ(m.getTail(), tail);
    std::string s = m.encode();
    Expected<ZSlMetaValue> expm = ZSlMetaValue::decode(s);
    EXPECT_TRUE(expm.ok());
    EXPECT_EQ(expm.value().getMaxLevel(), maxLvl);
    EXPECT_EQ(expm.value().getLevel(), lvl);
    EXPECT_EQ(expm.value().getCount(), count);
    EXPECT_EQ(expm.value().getTail(), tail);
  }

  for (size_t i = 0; i < num; i++) {
    ZSlEleValue v(genRand(), randomStr(256, false));
    for (uint8_t i = 1; i <= ZSlMetaValue::MAX_LAYER; ++i) {
      v.setForward(i, genRand());
      v.setSpan(i, genRand());
    }
    std::string s = v.encode();
    Expected<ZSlEleValue> expv = ZSlEleValue::decode(s);
    EXPECT_TRUE(expv.ok());
    for (uint8_t i = 1; i <= ZSlMetaValue::MAX_LAYER; ++i) {
      EXPECT_EQ(expv.value().getForward(i), v.getForward(i));
      EXPECT_EQ(expv.value().getSpan(i), v.getSpan(i));
    }
    EXPECT_EQ(expv.value().getScore(), v.getScore());
    EXPECT_EQ(expv.value().getSubKey(), v.getSubKey());
  }
}

TEST(VersionMeta, Compare) {
  auto meta1 = VersionMeta(0, 0, "sync_1");
  auto meta2 = VersionMeta(0, -1, "sync_1");
  auto meta3 = VersionMeta(0, UINT64_MAX, "sync_1");
  auto meta4 = VersionMeta(0, UINT64_MAX - 1, "sync_1");

  EXPECT_LT(meta2, meta1);
  EXPECT_LT(meta3, meta1);
  EXPECT_LT(meta1, meta4);
}

}  // namespace tendisplus
