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

static int genRand() {
    int grand = 0;
    uint32_t ms = nsSinceEpoch();
    grand = rand_r(reinterpret_cast<unsigned int *>(&ms));
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
            return static_cast<ReplFlag>((uint16_t)ReplFlag::REPL_GROUP_START | (uint16_t)ReplFlag::REPL_GROUP_END);
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
    auto ori1 = buf[size-2];
    while (ori1 == buf[size-2]) {
        buf[size-2] = genRand() % 256;
    }
    return std::string(
        reinterpret_cast<const char *>(buf.data()), buf.size());
}

TEST(Record, Common) {
    srand((unsigned int)time(NULL));
    for (size_t i = 0; i < 1000000; i++) {
        uint32_t dbid = genRand();
        uint32_t chunkid = genRand();
        auto type = randomType();
        auto pk = randomStr(5, false);
        auto sk = randomStr(5, true);
        uint64_t ttl = genRand()*genRand();
        uint64_t cas = genRand()*genRand();
        uint64_t version = genRand()*genRand();
        uint64_t versionEP = genRand()*genRand();
        auto val = randomStr(5, true);
        uint64_t pieceSize = (uint64_t)-1;
        if (val.size() % 2 == 0) {
            pieceSize = val.size() + 1;
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

        auto prcd1 = Record::decode(kv.first, kv.second);
        auto type_ = RecordKey::getRecordTypeRaw(kv.first.c_str(), 
            kv.first.size());
        auto ttl_ = RecordValue::getTtlRaw(kv.second.c_str(), 
            kv.second.size());
                
        EXPECT_EQ(cas, rv.getCas());
        EXPECT_EQ(version, rv.getVersion());
        EXPECT_EQ(versionEP, rv.getVersionEP());
        EXPECT_EQ(pieceSize, rv.getPieceSize());
        EXPECT_EQ(type, rv.getRecordType());
        EXPECT_EQ(ttl, rv.getTtl());
        EXPECT_EQ(rk.getRecordValueType(), type);
        EXPECT_EQ(rk.getRecordValueType(), rv.getRecordType());
        EXPECT_EQ(getRealKeyType(rk.getRecordValueType()),
                    rk.getRecordType());

        EXPECT_EQ(getRealKeyType(type), rk.getRecordType());
        EXPECT_EQ(type_, getRealKeyType(type));
        EXPECT_EQ(ttl_, ttl);
        EXPECT_TRUE(prcd1.ok());
        EXPECT_EQ(prcd1.value(), rcd);
    }

    //for (size_t i = 0; i < 1000000; i++) {
    //    uint32_t dbid = genRand();
    //    uint32_t chunkid = genRand();
    //    auto type = randomType();
    //    auto pk = randomStr(5, false);
    //    auto sk = randomStr(5, true);
    //    uint64_t ttl = genRand()*genRand();
    //    uint64_t cas = genRand()*genRand();
    //    auto val = randomStr(5, true);
    //    auto rk = RecordKey(chunkid, dbid, type, pk, sk);
    //    auto rv = RecordValue(val, ttl, cas);
    //    auto rcd = Record(rk, rv);
    //    auto kv = rcd.encode();
    //    auto prcd1 = Record::decode(overflip(kv.first), kv.second);
    //    EXPECT_TRUE(
    //        prcd1.status().code() == ErrorCodes::ERR_DECODE ||
    //        !(prcd1.value().getRecordKey() == rk));
    //}
}
#ifdef BINLOG_V1
TEST(ReplRecord, Prefix) {
    uint64_t timestamp = (uint64_t)genRand() + std::numeric_limits<uint32_t>::max();
    auto rlk = ReplLogKey(genRand(), 0, randomReplFlag(), timestamp);
    RecordKey rk(ReplLogKey::CHUNKID, ReplLogKey::DBID,
                 RecordType::RT_BINLOG, rlk.encode(), "");
    const std::string s = rk.encode();
    EXPECT_EQ(s[0], '\xff');
    EXPECT_EQ(s[1], '\xff');
    EXPECT_EQ(s[2], '\xff');
    EXPECT_EQ(s[3], '\x00');
    EXPECT_EQ(s[4], '\xff');
    EXPECT_EQ(s[5], '\xff');
    EXPECT_EQ(s[6], '\xff');
    EXPECT_EQ(s[7], '\x00');
    EXPECT_EQ(s[8], '\xff');
    const std::string& prefix = RecordKey::prefixReplLog();
    for (int i = 0; i < 100000; ++i) {
        EXPECT_TRUE(randomStr(5, false) <= prefix);
    }
}

TEST(ReplRecord, Common) {
    srand(time(NULL));
    std::vector<ReplLogKey> logKeys;
    for (size_t i = 0; i < 100000; i++) {
        uint64_t txnid = uint64_t(genRand())*uint64_t(genRand());
        uint16_t localid = uint16_t(genRand());
        ReplFlag flag = randomReplFlag();
        uint64_t timestamp = (uint64_t)genRand()+std::numeric_limits<uint32_t>::max() + 1;
        auto rk = ReplLogKey(txnid, localid, flag, timestamp);
        auto rkStr = rk.encode();
        auto prk = ReplLogKey::decode(rkStr);
        EXPECT_TRUE(prk.ok());
        EXPECT_EQ(prk.value(), rk);
        logKeys.emplace_back(std::move(rk));
    }
    std::sort(logKeys.begin(), logKeys.end(),
        [](const ReplLogKey& a, const ReplLogKey& b) {
            return a.encode() < b.encode();
        });
    for (size_t i = 0; i < logKeys.size()-1; ++i) {
        EXPECT_TRUE(logKeys[i].getTxnId() < logKeys[i+1].getTxnId()
            ||(logKeys[i].getTxnId() == logKeys[i+1].getTxnId() &&
            logKeys[i].getLocalId() <= logKeys[i+1].getLocalId()));
    }

    ReplLogValue rlv(ReplOp::REPL_OP_SET, "a", "b");
    std::string s = rlv.encode();
    Expected<ReplLogValue> erlv = ReplLogValue::decode(s);
    EXPECT_TRUE(erlv.ok());
}
#else
TEST(ReplRecordV2, Prefix) {
    uint64_t binlogid = (uint64_t)genRand() + std::numeric_limits<uint32_t>::max();
    auto rlk = ReplLogKeyV2(binlogid);
    RecordKey rk(ReplLogKeyV2::CHUNKID, ReplLogKeyV2::DBID,
        RecordType::RT_BINLOG, rlk.encode(), "");
    const std::string s = rk.encode();
    EXPECT_EQ(s[0], '\xff');
    EXPECT_EQ(s[1], '\xff');
    EXPECT_EQ(s[2], '\xff');
    EXPECT_EQ(s[3], '\x01');
    EXPECT_EQ(s[4], '\xff');
    EXPECT_EQ(s[5], '\xff');
    EXPECT_EQ(s[6], '\xff');
    EXPECT_EQ(s[7], '\x01');
    EXPECT_EQ(s[8], '\xff');
    const std::string& prefix = RecordKey::prefixReplLogV2();
    EXPECT_EQ(prefix[0], '\xff');
    EXPECT_EQ(prefix[1], '\xff');
    EXPECT_EQ(prefix[2], '\xff');
    EXPECT_EQ(prefix[3], '\x01');
    EXPECT_EQ(prefix[4], '\xff');
    EXPECT_EQ(prefix[5], '\xff');
    EXPECT_EQ(prefix[6], '\xff');
    EXPECT_EQ(prefix[7], '\x01');
    EXPECT_EQ(prefix[8], '\xff');
}

TEST(ReplRecordV2, Common) {
    srand(time(NULL));
    for (size_t i = 0; i < 1000; i++) {
        uint64_t txnid = uint64_t(genRand())*uint64_t(genRand());
        uint64_t binlogid = uint64_t(genRand())*uint64_t(genRand());
        uint32_t chunkid = genRand() % 16384;

        ReplFlag flag = randomReplFlag();
        uint64_t timestamp = (uint64_t)genRand() + std::numeric_limits<uint32_t>::max() + 1;

        auto rk = ReplLogKeyV2(binlogid);
        auto rkStr = rk.encode();
        auto prk = ReplLogKeyV2::decode(rkStr);
        EXPECT_TRUE(prk.ok());
        EXPECT_EQ(prk.value(), rk); 

        size_t count = 1000;
        
        std::vector<ReplLogValueEntryV2> vec;
        vec.reserve(count);
        for (size_t j = 0; j < count; j++) {
            uint64_t timestamp = (uint64_t)genRand() + std::numeric_limits<uint32_t>::max() + 1;

            size_t keyLen = genRand() % 128;
            size_t valLen = genRand() % 1024;

            ReplLogValueEntryV2 entry;
            if (genRand() % 2 == 0) {
                entry = ReplLogValueEntryV2(ReplOp::REPL_OP_SET, timestamp, randomStr(keyLen, false), randomStr(valLen, true));
            } else {
                entry = ReplLogValueEntryV2(ReplOp::REPL_OP_DEL, timestamp, randomStr(keyLen, false), "");
            }

            size_t size = 0;
            auto entryStr = entry.encode();
            auto pentry = ReplLogValueEntryV2::decode(entryStr.c_str(), entryStr.size(), size);
            EXPECT_TRUE(pentry.ok());
            EXPECT_EQ(pentry.value(), entry);
            EXPECT_EQ(size, entryStr.size());

            vec.emplace_back(entry);
        }

        auto rv = ReplLogValueV2(chunkid, flag, txnid, count, nullptr, 0);
        auto rvStr = rv.encode(vec);
        auto prv = ReplLogValueV2::decode(rvStr);
        EXPECT_TRUE(prv.ok());
        EXPECT_TRUE(rv.isEqualHdr(prv.value()));
        EXPECT_EQ(prv.value().getEntryCount(), count);
        
        size_t offset = ReplLogValueV2::fixedHeaderSize();
        auto desc = prv.value().getData();
        size_t datasize = prv.value().getDataSize();

        for (size_t j = 0; j < prv.value().getEntryCount(); j++) {
            const ReplLogValueEntryV2& entry = vec[j];
            size_t size = 0;
            auto v = ReplLogValueEntryV2::decode((const char*)desc + offset, datasize - offset, size);
            EXPECT_TRUE(v.ok());
            offset += size;
            EXPECT_EQ(entry, v.value());
        }
    }
}
#endif

TEST(ZSl, Common) {
    srand(time(NULL));
    for (size_t i = 0; i < 1000000; i++) {
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

    for (size_t i = 0; i < 1000000; i++) {
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

}  // namespace tendisplus
