#include <fstream>
#include <utility>
#include <limits>
#include <thread>
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "tendisplus/utils/status.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/portable.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/storage/rocks/rocks_kvstore.h"
#include "tendisplus/server/server_params.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/time.h"

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

ReplFlag randomReplFlag() {
    switch ((genRand() % 3)) {
        case 0:
            return ReplFlag::REPL_GROUP_MID;
        case 1:
            return ReplFlag::REPL_GROUP_START;
        case 2:
            return ReplFlag::REPL_GROUP_END;
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

size_t genData(RocksKVStore* kvstore, uint32_t count, uint64_t ttl,
            bool allDiff) {
    size_t kvCount = 0;
    srand((unsigned int)time(NULL));

    // easy to be different
    static size_t i = 0;
    size_t end = i + count;
    for (; i < end ; i++) {
        uint32_t dbid = genRand();
        uint32_t chunkid = genRand();
        auto type = randomType();
        if (type == RecordType::RT_KV) {
            kvCount++;
        }
        std::string pk;
        if (allDiff) {
            pk.append(std::to_string(i)).append(randomStr(5, false));
        } else {
            pk.append(randomStr(5, false));
        }
        auto sk = randomStr(5, true);
        uint64_t cas = genRand()*genRand();
        auto val = randomStr(5, true);
        auto rk = RecordKey(chunkid, dbid, type, pk, sk);
        auto rv = RecordValue(val, type, ttl, cas);

        auto eTxn1 = kvstore->createTransaction();
        EXPECT_EQ(eTxn1.ok(), true);
        std::unique_ptr<Transaction> txn1 = std::move(eTxn1.value());

        Status s = kvstore->setKV(rk, rv, txn1.get());
        EXPECT_EQ(s.ok(), true);

        Expected<uint64_t> exptCommitId = txn1->commit();
        EXPECT_EQ(exptCommitId.ok(), true);
    }

    return kvCount;
}

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
    RecordValue rv("txn1", RecordType::RT_KV);
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
            RecordValue("txn1", RecordType::RT_KV)),
        txn1.get());
    EXPECT_EQ(s.ok(), true);

    s = kvstore->setKV(
        Record(
            RecordKey(0, 0, RecordType::RT_KV, "ab", ""),
            RecordValue("txn1", RecordType::RT_KV)),
        txn1.get());
    EXPECT_EQ(s.ok(), true);

    s = kvstore->setKV(
        Record(
            RecordKey(0, 0, RecordType::RT_KV, "abc", ""),
            RecordValue("txn1", RecordType::RT_KV)),
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
            RecordValue("txn3", RecordType::RT_KV)),
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
            RecordValue("txn1", RecordType::RT_KV)),
        txn1.get());
    EXPECT_EQ(s.ok(), true);

    s = kvstore->setKV(
        Record(
            RecordKey(0, 0, RecordType::RT_KV, "ab", ""),
            RecordValue("txn1", RecordType::RT_KV)),
        txn1.get());
    EXPECT_EQ(s.ok(), true);

    s = kvstore->setKV(
        Record(
            RecordKey(0, 0, RecordType::RT_KV, "abc", ""),
            RecordValue("txn1", RecordType::RT_KV)),
        txn1.get());
    EXPECT_EQ(s.ok(), true);

    s = kvstore->setKV(
        Record(
            RecordKey(0, 0, RecordType::RT_KV, "b", ""),
            RecordValue("txn1", RecordType::RT_KV)),
        txn1.get());
    EXPECT_EQ(s.ok(), true);

    s = kvstore->setKV(
        Record(
            RecordKey(0, 0, RecordType::RT_KV, "bac", ""),
            RecordValue("txn1", RecordType::RT_KV)),
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
        KVStore::StoreMode::READ_WRITE,
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
        KVStore::StoreMode::READ_WRITE,
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
            RecordValue("txn1", RecordType::RT_KV)),
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

    // backup failed, it should clean the remaining files,
    // and set the backup state to false
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
            RecordValue("txn1", RecordType::RT_KV)),
        txn1.get());
    EXPECT_EQ(s.ok(), true);
    Expected<RecordValue> e = kvstore->getKV(
        RecordKey(0, 0, RecordType::RT_KV, "a", ""),
        txn1.get());
    EXPECT_EQ(e.ok(), true);
    EXPECT_EQ(e.value(), RecordValue("txn1", RecordType::RT_KV));

    Expected<RecordValue> e1 = kvstore->getKV(
        RecordKey(0, 0, RecordType::RT_KV, "a", ""),
        txn2.get());
    EXPECT_EQ(e1.status().code(), ErrorCodes::ERR_NOTFOUND);
    s = kvstore->setKV(
        Record(
            RecordKey(0, 0, RecordType::RT_KV, "a", ""),
            RecordValue("txn2", RecordType::RT_KV)),
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
        KVStore::StoreMode::READ_WRITE,
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
        KVStore::StoreMode::READ_WRITE,
        RocksKVStore::TxnMode::TXN_PES);
    commonRoutine(kvstore.get());
}

uint64_t getBinlogCount(Transaction *txn) {
    auto bcursor = txn->createBinlogCursor(0);
    uint64_t cnt = 0;
    while (true) {
        auto v = bcursor->next();
        if (!v.ok()) {
            INVARIANT(v.status().code() == ErrorCodes::ERR_EXHAUST);
            break;
        }
        cnt += 1;
    }
    return cnt;
}

TEST(RocksKVStore, PesTruncateBinlog) {
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
        KVStore::StoreMode::READ_WRITE,
        RocksKVStore::TxnMode::TXN_PES,
        1 /* max keep logs */);

    uint64_t firstBinlog = 1;
    {
        auto eTxn1 = kvstore->createTransaction();
        EXPECT_EQ(eTxn1.ok(), true);
        std::unique_ptr<Transaction> txn1 = std::move(eTxn1.value());

        RecordKey rk(0, 1, RecordType::RT_KV, std::to_string(0), "");
        RecordValue rv("txn1", RecordType::RT_KV);
        Status s = kvstore->setKV(rk, rv, txn1.get());
        EXPECT_EQ(s.ok(), true);

        Expected<uint64_t> exptCommitId = txn1->commit();
        EXPECT_EQ(exptCommitId.ok(), true);
    }

    {
        auto eTxn1 = kvstore->createTransaction();
        EXPECT_EQ(eTxn1.ok(), true);
        std::unique_ptr<Transaction> txn1 = std::move(eTxn1.value());

        auto newFirst = kvstore->getTruncateLog(firstBinlog,
            std::numeric_limits<uint64_t>::max(), txn1.get());
        EXPECT_EQ(newFirst.ok(), true);
        EXPECT_EQ(newFirst.value().first, 1U);
    }
    {
        auto eTxn1 = kvstore->createTransaction();
        EXPECT_EQ(eTxn1.ok(), true);
        std::unique_ptr<Transaction> txn1 = std::move(eTxn1.value());

        RecordKey rk(0, 1, RecordType::RT_KV, std::to_string(1), "");
        RecordValue rv("txn1", RecordType::RT_KV);
        Status s = kvstore->setKV(rk, rv, txn1.get());
        EXPECT_EQ(s.ok(), true);

        Expected<uint64_t> exptCommitId = txn1->commit();
        EXPECT_EQ(exptCommitId.ok(), true);

        auto eTxn2 = kvstore->createTransaction();
        EXPECT_EQ(eTxn2.ok(), true);
        std::unique_ptr<Transaction> txn2 = std::move(eTxn2.value());
        uint64_t currentCnt = getBinlogCount(txn2.get());
        EXPECT_EQ(currentCnt, 2U);
    }
    {
        auto eTxn1 = kvstore->createTransaction();
        EXPECT_EQ(eTxn1.ok(), true);
        std::unique_ptr<Transaction> txn1 = std::move(eTxn1.value());
        auto newFirst1 = kvstore->getTruncateLog(firstBinlog,
            std::numeric_limits<uint64_t>::max(), txn1.get());
        EXPECT_EQ(newFirst1.ok(), true);
        EXPECT_NE(newFirst1.value().first, firstBinlog);
        auto s = kvstore->truncateBinlog(newFirst1.value().second, txn1.get());
        EXPECT_TRUE(s.ok());
        firstBinlog = newFirst1.value().first;
        uint64_t currentCnt = getBinlogCount(txn1.get());
        EXPECT_EQ(currentCnt, 1U);

        Expected<uint64_t> exptCommitId = txn1->commit();
        EXPECT_EQ(exptCommitId.ok(), true);
    }
    for (auto range : {10, 100, 1000, 10000, 100000}) {
        for (int i = 0; i < range; ++i) {
            auto eTxn1 = kvstore->createTransaction();
            EXPECT_EQ(eTxn1.ok(), true);
            std::unique_ptr<Transaction> txn1 = std::move(eTxn1.value());

            RecordKey rk(0, 1, RecordType::RT_KV, std::to_string(i), "");
            RecordValue rv("txn1", RecordType::RT_KV);
            Status s = kvstore->setKV(rk, rv, txn1.get());
            EXPECT_EQ(s.ok(), true);

            Expected<uint64_t> exptCommitId = txn1->commit();
            EXPECT_EQ(exptCommitId.ok(), true);
        }
        auto eTxn2 = kvstore->createTransaction();
        EXPECT_EQ(eTxn2.ok(), true);
        std::unique_ptr<Transaction> txn2 = std::move(eTxn2.value());
        uint64_t currentCnt = getBinlogCount(txn2.get());
        auto newFirst = kvstore->getTruncateLog(firstBinlog,
               std::numeric_limits<uint64_t>::max(), txn2.get());
        EXPECT_EQ(newFirst.ok(), true) << ' '
            << range << ' ' << firstBinlog << newFirst.status().toString();
        EXPECT_NE(newFirst.value().first, firstBinlog);
        auto s = kvstore->truncateBinlog(newFirst.value().second, txn2.get());
        EXPECT_TRUE(s.ok());
        uint64_t currentCnt1 = getBinlogCount(txn2.get());
        EXPECT_EQ(currentCnt1, currentCnt - newFirst.value().second.size());
        firstBinlog = newFirst.value().first;
        Expected<uint64_t> exptCommitId = txn2->commit();
        EXPECT_EQ(exptCommitId.ok(), true);
    }
}

TEST(RocksKVStore, Compaction) {
    auto cfg = genParams();
    EXPECT_TRUE(filesystem::create_directory("db"));
    // EXPECT_TRUE(filesystem::create_directory("db/0"));
    EXPECT_TRUE(filesystem::create_directory("log"));
    const auto guard = MakeGuard([] {
        filesystem::remove_all("./log");
        filesystem::remove_all("./db");
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearAllCallBacks();
    });
    auto blockCache =
        rocksdb::NewLRUCache(cfg->rocksBlockcacheMB * 1024 * 1024LL, 4);
    auto kvstore = std::make_unique<RocksKVStore>(
        "0",
        cfg,
        blockCache,
        KVStore::StoreMode::READ_WRITE,
        RocksKVStore::TxnMode::TXN_PES);

    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();

    uint64_t totalFilter = 0;
    SyncPoint::GetInstance()->SetCallBack("InspectKvTtlFilterCount",
        [&](void *arg) mutable {
        uint64_t *tmp = reinterpret_cast<uint64_t *>(arg);
        totalFilter = *tmp;
    });

    uint64_t totalExpired = 0;
    bool hasCalled = false;
    SyncPoint::GetInstance()->SetCallBack("InspectKvTtlExpiredCount",
        [&](void *arg) mutable {
        hasCalled = true;
        uint64_t *tmp = reinterpret_cast<uint64_t *>(arg);
        totalExpired = *tmp;
    });

    uint32_t waitSec = 10;
    // if we want to check the totalFilter, all data should be different
    genData(kvstore.get(), 1000, 0, true);
    size_t kvCount = genData(kvstore.get(), 1000, msSinceEpoch(), true);
    size_t kvCount2 = genData(kvstore.get(), 1000,
                    msSinceEpoch() + waitSec * 1000, true);

    std::this_thread::sleep_for(std::chrono::seconds(1));

    auto status = kvstore->fullCompact();
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(hasCalled);
    // because there are repl_log for each set(), it should * 2 here
    EXPECT_EQ(totalFilter, 3000*2);
    EXPECT_EQ(totalExpired, kvCount);

    std::this_thread::sleep_for(std::chrono::seconds(waitSec));

    status = kvstore->fullCompact();
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(hasCalled);
    // because there are repl_log for each set(), it should * 2 here
    EXPECT_EQ(totalFilter, 3000*2 - kvCount);
    EXPECT_EQ(totalExpired, kvCount2);
}

}  // namespace tendisplus
