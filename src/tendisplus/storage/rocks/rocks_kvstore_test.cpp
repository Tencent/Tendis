#include <fstream>
#include <utility>
#include "gtest/gtest.h"
#include "tendisplus/utils/status.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/portable.h"
#include "tendisplus/storage/rocks/rocks_kvstore.h"
#include "tendisplus/server/server_params.h"

namespace tendisplus {

static std::shared_ptr<ServerParams> genParams() {
    const auto guard = MakeGuard([] {
        remove("a.cfg");
    });
    std::ofstream myfile;
    myfile.open("a.cfg");
    myfile << "bind 127.0.0.1\n";
    myfile << "port 8903\n";
    myfile << "loglevel debug\n";
    myfile << "logdir ./log\n";
    myfile << "storageEngine rocks\n";
    myfile << "dbPath ./db\n";
    myfile << "rocksBlockCacheMB 4096\n";
    myfile.close();
    auto cfg = std::make_shared<ServerParams>();
    auto s = cfg->parseFile("a.cfg");
    EXPECT_EQ(s.ok(), true) << s.toString();
    return cfg;
}

TEST(RocksKVStore, Common) {
    auto cfg = genParams();
    EXPECT_TRUE(filesystem::create_directory("db"));
    EXPECT_TRUE(filesystem::create_directory("db/0"));
    EXPECT_TRUE(filesystem::create_directory("log"));
    const auto guard = MakeGuard([] {
        filesystem::remove_all("./log");
        filesystem::remove_all("./db");
    });
    auto blockCache =
        rocksdb::NewLRUCache(cfg->rocksBlockcacheMB * 1024 * 1024LL, 4);
    auto kvstore = std::make_unique<RocksKVStore>(
        "0",
        cfg,
        blockCache);
    auto eTxn1 = kvstore->createTransaction();
    auto eTxn2 = kvstore->createTransaction();
    EXPECT_EQ(eTxn1.ok(), true);
    EXPECT_EQ(eTxn2.ok(), true);
    auto txn1 = std::move(eTxn1.value());
    auto txn2 = std::move(eTxn2.value());
    auto s = kvstore->setKV("a", "txn1", txn1.get());
    EXPECT_EQ(s.ok(), true);
    auto e = kvstore->getKV("a", txn1.get());
    EXPECT_EQ(e.ok(), true);
    EXPECT_EQ(e.value(), "txn1");
    e = kvstore->getKV("a", txn2.get());
    EXPECT_EQ(e.status().code(), ErrorCodes::ERR_NOTFOUND);
    s = kvstore->setKV("a", "txn2", txn2.get());
    s = txn2->commit();
    EXPECT_EQ(s.ok(), true);
    s = txn1->commit();
    EXPECT_EQ(s.code(), ErrorCodes::ERR_COMMIT_RETRY);
    txn1.reset();
    txn2.reset();
}

}  // namespace tendisplus
