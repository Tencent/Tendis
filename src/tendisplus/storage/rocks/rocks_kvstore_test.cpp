#include <fstream>
#include <utility>
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "tendisplus/utils/status.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/portable.h"
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

TEST(RocksKVStore, BinlogRightMost) {
    auto cfg = genParams();
    EXPECT_TRUE(filesystem::create_directory("db"));
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
    EXPECT_EQ(eTxn1.ok(), true);
    std::unique_ptr<Transaction> txn1 = std::move(eTxn1.value());

    RecordKey rk(0, 1, RecordType::RT_KV, "a", "");
    RecordValue rv("txn1");
    Status s = kvstore->setKV(rk, rv, txn1.get());
    EXPECT_EQ(s.ok(), true);

    Expected<uint64_t> exptCommitId = txn1->commit();
    EXPECT_EQ(exptCommitId.ok(), true);

    auto eTxn2 = kvstore->createTransaction();
    EXPECT_EQ(eTxn2.ok(), true);
    std::unique_ptr<Transaction> txn2 = std::move(eTxn2.value());
    auto bcursor = txn2->createBinlogCursor(0);
    bcursor->seekToLast();
    auto v = bcursor->next();
    EXPECT_EQ(v.ok(), true);
    if (v.ok()) {
        EXPECT_EQ(v.value().getReplLogValue().getOpKey(), rk.encode());
        EXPECT_EQ(v.value().getReplLogValue().getOpValue(), rv.encode());
    }
}

TEST(RocksKVStore, BinlogCursor) {
    auto cfg = genParams();
    EXPECT_TRUE(filesystem::create_directory("db"));
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
    EXPECT_EQ(eTxn1.ok(), true);
    std::unique_ptr<Transaction> txn1 = std::move(eTxn1.value());

    Status s = kvstore->setKV(
        Record(
            RecordKey(0, 0, RecordType::RT_KV, "a", ""),
            RecordValue("txn1")),
        txn1.get());
    EXPECT_EQ(s.ok(), true);

    s = kvstore->setKV(
        Record(
            RecordKey(0, 0, RecordType::RT_KV, "ab", ""),
            RecordValue("txn1")),
        txn1.get());
    EXPECT_EQ(s.ok(), true);

    s = kvstore->setKV(
        Record(
            RecordKey(0, 0, RecordType::RT_KV, "abc", ""),
            RecordValue("txn1")),
        txn1.get());
    EXPECT_EQ(s.ok(), true);

    Expected<uint64_t> exptCommitId = txn1->commit();
    EXPECT_TRUE(exptCommitId.ok());
    EXPECT_EQ(exptCommitId.value(), 1U);

    auto eTxn2 = kvstore->createTransaction();
    EXPECT_EQ(eTxn2.ok(), true);
    std::unique_ptr<Transaction> txn2 = std::move(eTxn2.value());
    auto bcursor = txn2->createBinlogCursor(1);

    auto eTxn3 = kvstore->createTransaction();
    EXPECT_EQ(eTxn3.ok(), true);
    std::unique_ptr<Transaction> txn3 = std::move(eTxn3.value());

    s = kvstore->setKV(
        Record(
            RecordKey(0, 0, RecordType::RT_KV, "b", ""),
            RecordValue("txn3")),
        txn3.get());
    EXPECT_EQ(s.ok(), true);

    exptCommitId = txn3->commit();
    EXPECT_EQ(exptCommitId.ok(), true);
    EXPECT_EQ(exptCommitId.value(), 3U);

    int32_t cnt = 0;
    while (true) {
        auto v = bcursor->next();
        if (!v.ok()) {
            EXPECT_EQ(v.status().code(), ErrorCodes::ERR_EXHAUST);
            break;
        }
        cnt += 1;
    }
    EXPECT_EQ(cnt, 3);
    exptCommitId = txn2->commit();
    EXPECT_TRUE(exptCommitId.ok());
    EXPECT_EQ(exptCommitId.value(), 2U);

    cnt = 0;
    auto eTxn4 = kvstore->createTransaction();
    EXPECT_EQ(eTxn4.ok(), true);
    std::unique_ptr<Transaction> txn4 = std::move(eTxn4.value());
    auto bcursor1 = txn4->createBinlogCursor(2);
    std::vector<ReplLog> binlogs;
    while (true) {
        auto v = bcursor1->next();
        if (!v.ok()) {
            EXPECT_EQ(v.status().code(), ErrorCodes::ERR_EXHAUST);
            break;
        }
        cnt += 1;
        binlogs.emplace_back(std::move(v.value()));
    }
    EXPECT_EQ(cnt, 1);
    if (cnt == 1) {
        EXPECT_EQ(binlogs[0].getReplLogKey().getTxnId(), uint64_t(3));
        EXPECT_EQ(binlogs[0].getReplLogKey().getLocalId(), uint16_t(0));
        // 3 == REPL_GROUP_START | REPL_GROUP_END
        EXPECT_EQ(
            static_cast<uint16_t>(binlogs[0].getReplLogKey().getFlag()),
            uint16_t(3));
    }
}

void cursorVisibleRoutine(RocksKVStore* kvstore) {
    auto eTxn1 = kvstore->createTransaction();
    EXPECT_EQ(eTxn1.ok(), true);
    std::unique_ptr<Transaction> txn1 = std::move(eTxn1.value());

    Status s = kvstore->setKV(
        Record(
            RecordKey(0, 0, RecordType::RT_KV, "a", ""),
            RecordValue("txn1")),
        txn1.get());
    EXPECT_EQ(s.ok(), true);

    s = kvstore->setKV(
        Record(
            RecordKey(0, 0, RecordType::RT_KV, "ab", ""),
            RecordValue("txn1")),
        txn1.get());
    EXPECT_EQ(s.ok(), true);

    s = kvstore->setKV(
        Record(
            RecordKey(0, 0, RecordType::RT_KV, "abc", ""),
            RecordValue("txn1")),
        txn1.get());
    EXPECT_EQ(s.ok(), true);

    s = kvstore->setKV(
        Record(
            RecordKey(0, 0, RecordType::RT_KV, "b", ""),
            RecordValue("txn1")),
        txn1.get());
    EXPECT_EQ(s.ok(), true);

    s = kvstore->setKV(
        Record(
            RecordKey(0, 0, RecordType::RT_KV, "bac", ""),
            RecordValue("txn1")),
        txn1.get());
    EXPECT_EQ(s.ok(), true);

    std::unique_ptr<Cursor> cursor = txn1->createCursor();
    int32_t cnt = 0;
    while (true) {
        auto v = cursor->next();
        if (!v.ok()) {
            EXPECT_EQ(v.status().code(), ErrorCodes::ERR_EXHAUST);
            break;
        }
        cnt += 1;
    }
    EXPECT_EQ(cnt, 5);

    cnt = 0;
    RecordKey rk(0, 0, RecordType::RT_KV, "b", "");
    cursor->seek(rk.prefixPk());
    while (true) {
        auto v = cursor->next();
        if (!v.ok()) {
            EXPECT_EQ(v.status().code(), ErrorCodes::ERR_EXHAUST);
            break;
        }
        cnt += 1;
    }
    EXPECT_EQ(cnt, 2);
}

TEST(RocksKVStore, OptCursorVisible) {
    auto cfg = genParams();
    EXPECT_TRUE(filesystem::create_directory("db"));
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
        blockCache,
        RocksKVStore::TxnMode::TXN_OPT);
    cursorVisibleRoutine(kvstore.get());
}

TEST(RocksKVStore, PesCursorVisible) {
    auto cfg = genParams();
    EXPECT_TRUE(filesystem::create_directory("db"));
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
        blockCache,
        RocksKVStore::TxnMode::TXN_PES);
    cursorVisibleRoutine(kvstore.get());
}

TEST(RocksKVStore, Backup) {
    auto cfg = genParams();
    EXPECT_TRUE(filesystem::create_directory("db"));
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
    EXPECT_EQ(eTxn1.ok(), true);
    std::unique_ptr<Transaction> txn1 = std::move(eTxn1.value());
    Status s = kvstore->setKV(
        Record(
            RecordKey(0, 0, RecordType::RT_KV, "a", ""),
            RecordValue("txn1")),
        txn1.get());
    EXPECT_EQ(s.ok(), true);
    Expected<uint64_t> exptCommitId = txn1->commit();
    EXPECT_EQ(exptCommitId.ok(), true);

    Expected<BackupInfo> expBk = kvstore->backup(
        kvstore->dftBackupDir(), KVStore::BackupMode::BACKUP_CKPT);
    EXPECT_TRUE(expBk.ok()) << expBk.status().toString();
    for (auto& bk : expBk.value().getFileList()) {
        LOG(INFO) << "backupInfo:[" << bk.first << "," << bk.second << "]";
    }

    // backup failed, it should clean the remaining files, and set the backup state to false
    Expected<BackupInfo> expBk1 = kvstore->backup(
        kvstore->dftBackupDir(), KVStore::BackupMode::BACKUP_CKPT);
    EXPECT_FALSE(expBk1.ok());

    Expected<BackupInfo> expBk2 = kvstore->backup(
        kvstore->dftBackupDir(), KVStore::BackupMode::BACKUP_CKPT);
    EXPECT_TRUE(expBk2.ok());

    s = kvstore->stop();
    EXPECT_TRUE(s.ok());

    s = kvstore->clear();
    EXPECT_TRUE(s.ok());

    uint64_t lastCommitId = exptCommitId.value();
    exptCommitId = kvstore->restart(true);
    EXPECT_TRUE(exptCommitId.ok()) << exptCommitId.status().toString();
    EXPECT_EQ(exptCommitId.value(), lastCommitId);

    eTxn1 = kvstore->createTransaction();
    EXPECT_EQ(eTxn1.ok(), true);
    txn1 = std::move(eTxn1.value());
    Expected<RecordValue> e = kvstore->getKV(
        RecordKey(0, 0, RecordType::RT_KV, "a", ""),
        txn1.get());
    EXPECT_EQ(e.ok(), true);
}

TEST(RocksKVStore, Stop) {
    auto cfg = genParams();
    EXPECT_TRUE(filesystem::create_directory("db"));
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
    EXPECT_EQ(eTxn1.ok(), true);

    auto s = kvstore->stop();
    EXPECT_FALSE(s.ok());

    s = kvstore->clear();
    EXPECT_FALSE(s.ok());

    Expected<uint64_t> exptCommitId = kvstore->restart(false);
    EXPECT_FALSE(exptCommitId.ok());

    eTxn1.value().reset();

    s = kvstore->stop();
    EXPECT_TRUE(s.ok());

    s = kvstore->clear();
    EXPECT_TRUE(s.ok());

    exptCommitId = kvstore->restart(false);
    EXPECT_TRUE(exptCommitId.ok());
}

void commonRoutine(RocksKVStore *kvstore) {
    auto eTxn1 = kvstore->createTransaction();
    auto eTxn2 = kvstore->createTransaction();
    EXPECT_EQ(eTxn1.ok(), true);
    EXPECT_EQ(eTxn2.ok(), true);
    std::unique_ptr<Transaction> txn1 = std::move(eTxn1.value());
    std::unique_ptr<Transaction> txn2 = std::move(eTxn2.value());

    std::set<uint64_t> uncommitted = kvstore->getUncommittedTxns();
    EXPECT_NE(uncommitted.find(
        dynamic_cast<RocksTxn*>(txn1.get())->getTxnId()),
        uncommitted.end());
    EXPECT_NE(uncommitted.find(
        dynamic_cast<RocksTxn*>(txn2.get())->getTxnId()),
        uncommitted.end());

    Status s = kvstore->setKV(
        Record(
            RecordKey(0, 0, RecordType::RT_KV, "a", ""),
            RecordValue("txn1")),
        txn1.get());
    EXPECT_EQ(s.ok(), true);
    Expected<RecordValue> e = kvstore->getKV(
        RecordKey(0, 0, RecordType::RT_KV, "a", ""),
        txn1.get());
    EXPECT_EQ(e.ok(), true);
    EXPECT_EQ(e.value(), RecordValue("txn1"));

    Expected<RecordValue> e1 = kvstore->getKV(
        RecordKey(0, 0, RecordType::RT_KV, "a", ""),
        txn2.get());
    EXPECT_EQ(e1.status().code(), ErrorCodes::ERR_NOTFOUND);
    s = kvstore->setKV(
        Record(
            RecordKey(0, 0, RecordType::RT_KV, "a", ""),
            RecordValue("txn2")),
        txn2.get());
    if (kvstore->getTxnMode() == RocksKVStore::TxnMode::TXN_OPT) {
        EXPECT_EQ(s.code(), ErrorCodes::ERR_OK);
        Expected<uint64_t> exptCommitId = txn2->commit();
        EXPECT_EQ(exptCommitId.ok(), true);
        exptCommitId = txn1->commit();
        EXPECT_EQ(exptCommitId.status().code(), ErrorCodes::ERR_COMMIT_RETRY);
        uncommitted = kvstore->getUncommittedTxns();
        EXPECT_EQ(uncommitted.find(
            dynamic_cast<RocksTxn*>(txn1.get())->getTxnId()),
            uncommitted.end());
        EXPECT_EQ(uncommitted.find(
            dynamic_cast<RocksTxn*>(txn2.get())->getTxnId()),
            uncommitted.end());
    } else {
        EXPECT_EQ(s.code(), ErrorCodes::ERR_INTERNAL);
        s = txn2->rollback();
        EXPECT_EQ(s.code(), ErrorCodes::ERR_OK);
        Expected<uint64_t> exptCommitId = txn1->commit();
        EXPECT_EQ(exptCommitId.ok(), true);
        uncommitted = kvstore->getUncommittedTxns();
        EXPECT_EQ(uncommitted.find(
            dynamic_cast<RocksTxn*>(txn1.get())->getTxnId()),
            uncommitted.end());
        EXPECT_EQ(uncommitted.find(
            dynamic_cast<RocksTxn*>(txn2.get())->getTxnId()),
            uncommitted.end());
    }
}

TEST(RocksKVStore, OptCommon) {
    auto cfg = genParams();
    EXPECT_TRUE(filesystem::create_directory("db"));
    // EXPECT_TRUE(filesystem::create_directory("db/0"));
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
        blockCache,
        RocksKVStore::TxnMode::TXN_OPT);
    commonRoutine(kvstore.get());
}

TEST(RocksKVStore, PesCommon) {
    auto cfg = genParams();
    EXPECT_TRUE(filesystem::create_directory("db"));
    // EXPECT_TRUE(filesystem::create_directory("db/0"));
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
        blockCache,
        RocksKVStore::TxnMode::TXN_PES);
    commonRoutine(kvstore.get());
}

}  // namespace tendisplus
