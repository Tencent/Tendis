#include <fstream>
#include <utility>
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
    const auto guard = MakeGuard([] {
        remove("a.cfg");
    });
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

TEST(SkipList, Reload) {
    auto cfg = genParams();
    EXPECT_TRUE(filesystem::create_directory("db"));
    EXPECT_TRUE(filesystem::create_directory("log"));
    const auto guard = MakeGuard([] {
        filesystem::remove_all("./log");
        filesystem::remove_all("./db");
    });
    auto blockCache =
        rocksdb::NewLRUCache(cfg->rocksBlockcacheMB * 1024 * 1024LL, 4);
    auto store = std::shared_ptr<KVStore>(
        new RocksKVStore("0", cfg, blockCache));
    auto eTxn1 = store->createTransaction();
    EXPECT_TRUE(eTxn1.ok());

    ZSlMetaValue meta(1, 20, 1);
    RecordValue rv(meta.encode());
    RecordKey mk(0, RecordType::RT_ZSET_META, "test", "");
    Status s = store->setKV(mk, rv, eTxn1.value().get());
    EXPECT_TRUE(s.ok());

    RecordKey head(0,
                   RecordType::RT_ZSET_S_ELE,
                   "test",
                   std::to_string(ZSlMetaValue::HEAD_ID));
    ZSlEleValue headVal;
    RecordValue subRv(headVal.encode());

    s = store->setKV(head, subRv, eTxn1.value().get());
    EXPECT_TRUE(s.ok());

    Expected<uint64_t> commitStatus = eTxn1.value()->commit();
    EXPECT_TRUE(commitStatus.ok());
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
    auto store = std::shared_ptr<KVStore>(
        new RocksKVStore("0", cfg, blockCache));
    auto eTxn1 = store->createTransaction();
    EXPECT_TRUE(eTxn1.ok());

    ZSlMetaValue meta(1, 20, 1);
    RecordValue rv(meta.encode());
    RecordKey mk(0, RecordType::RT_ZSET_META, "test", "");
    Status s = store->setKV(mk, rv, eTxn1.value().get());
    EXPECT_TRUE(s.ok());

    RecordKey head(0,
                   RecordType::RT_ZSET_S_ELE,
                   "test",
                   std::to_string(ZSlMetaValue::HEAD_ID));
    ZSlEleValue headVal;
    RecordValue subRv(headVal.encode());

    s = store->setKV(head, subRv, eTxn1.value().get());
    EXPECT_TRUE(s.ok());

    Expected<uint64_t> commitStatus = eTxn1.value()->commit();
    EXPECT_TRUE(commitStatus.ok());

    SkipList sl(0, "test", meta, store);

    std::vector<uint32_t> keys;

    constexpr uint32_t CNT = 1000;
    for (uint32_t i = 1; i <= CNT; ++i) {
        keys.push_back(i);
    }
    std::random_shuffle(keys.begin(), keys.end());
    for (auto& i : keys) {
        auto eTxn = store->createTransaction();
        EXPECT_TRUE(eTxn.ok());
        Status s = sl.insert(i, std::to_string(i), eTxn.value().get());
        EXPECT_TRUE(s.ok());
        s = sl.save(eTxn.value().get());
        EXPECT_TRUE(s.ok());
        Expected<uint64_t> commitStatus = eTxn.value()->commit();
        EXPECT_TRUE(commitStatus.ok());
    }

    auto eTxn = store->createTransaction();
    EXPECT_TRUE(eTxn.ok());

    s = sl.remove(5, std::to_string(5), eTxn.value().get());
    EXPECT_TRUE(s.ok());
    s = sl.save(eTxn.value().get());
    EXPECT_TRUE(s.ok());

    s = sl.insert(1, std::to_string(5), eTxn.value().get());
    EXPECT_TRUE(s.ok());

    Expected<uint32_t> expRank =
        sl.rank(1, std::to_string(5), eTxn.value().get());
    EXPECT_TRUE(expRank.ok());
    EXPECT_EQ(expRank.value(), 2U);
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
    auto store = std::shared_ptr<KVStore>(
        new RocksKVStore("0", cfg, blockCache));
    auto eTxn1 = store->createTransaction();
    EXPECT_TRUE(eTxn1.ok());

    ZSlMetaValue meta(1, 20, 1);
    RecordValue rv(meta.encode());
    RecordKey mk(0, RecordType::RT_ZSET_META, "test", "");
    Status s = store->setKV(mk, rv, eTxn1.value().get());
    EXPECT_TRUE(s.ok());

    RecordKey head(0,
                   RecordType::RT_ZSET_S_ELE,
                   "test",
                   std::to_string(ZSlMetaValue::HEAD_ID));
    ZSlEleValue headVal;
    RecordValue subRv(headVal.encode());

    s = store->setKV(head, subRv, eTxn1.value().get());
    EXPECT_TRUE(s.ok());

    Expected<uint64_t> commitStatus = eTxn1.value()->commit();
    EXPECT_TRUE(commitStatus.ok());

    SkipList sl(0, "test", meta, store);

    std::vector<uint32_t> keys;
    std::vector<uint32_t> sortedkeys;

    constexpr uint32_t CNT = 1000;
    for (uint32_t i = 1; i <= CNT; ++i) {
        keys.push_back(i);
    }
    std::random_shuffle(keys.begin(), keys.end());
    for (auto& i : keys) {
        auto eTxn = store->createTransaction();
        EXPECT_TRUE(eTxn.ok());
        Status s = sl.insert(i, std::to_string(i), eTxn.value().get());
        EXPECT_TRUE(s.ok());
        s = sl.save(eTxn.value().get());
        EXPECT_TRUE(s.ok());
        Expected<uint64_t> commitStatus = eTxn.value()->commit();
        EXPECT_TRUE(commitStatus.ok());
    }
    for (uint32_t i = 1; i <= CNT; ++i) {
        auto eTxn = store->createTransaction();
        EXPECT_TRUE(eTxn.ok());
        Expected<uint32_t> expRank =
            sl.rank(i, std::to_string(i), eTxn.value().get());
        EXPECT_TRUE(expRank.ok());
        EXPECT_EQ(expRank.value(), i);
    }
    // head also counts
    EXPECT_EQ(sl.getCount(), CNT+1);
    EXPECT_EQ(sl.getAlloc(), CNT+ZSlMetaValue::MIN_POS);
    for (uint32_t i = 1; i <= CNT; ++i) {
        auto eTxn = store->createTransaction();
        EXPECT_TRUE(eTxn.ok());

        Expected<uint32_t> expRank =
            sl.rank(CNT, std::to_string(CNT), eTxn.value().get());
        EXPECT_TRUE(expRank.ok());
        EXPECT_EQ(expRank.value(), CNT-i+1);

        Status s = sl.remove(i, std::to_string(i), eTxn.value().get());
        EXPECT_TRUE(s.ok());
        s = sl.save(eTxn.value().get());
        EXPECT_TRUE(s.ok());

        // std::stringstream ss;
        // sl.traverse(ss, eTxn.value().get());
        // std::cout<<ss.str() << std::endl;

        Expected<uint64_t> commitStatus = eTxn.value()->commit();
        EXPECT_TRUE(commitStatus.ok());
    }
    EXPECT_EQ(sl.getCount(), 1U);
    EXPECT_EQ(sl.getAlloc(), CNT+ZSlMetaValue::MIN_POS);
    LOG(INFO) << "skiplist level:" << static_cast<uint32_t>(sl.getLevel());
}

}  // namespace tendisplus
