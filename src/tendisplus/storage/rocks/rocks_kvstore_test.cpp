// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <fstream>
#include <utility>
#include <limits>
#include <thread>  // NOLINT

#include "glog/logging.h"
#include "gtest/gtest.h"

#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/utilities/backupable_db.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/options.h"

#include "tendisplus/utils/status.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/portable.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/storage/rocks/rocks_kvstore.h"
#include "tendisplus/storage/kvstore.h"
#include "tendisplus/server/server_params.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/time.h"
#include "tendisplus/network/session_ctx.h"

namespace tendisplus {

static int genRand() {
  static int grand = 0;
  grand = rand_r(reinterpret_cast<unsigned int*>(&grand));
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

size_t genData(RocksKVStore* kvstore,
               uint32_t count,
               uint64_t ttl,
               bool allDiff) {
  size_t kvCount = 0;
  srand((unsigned int)time(NULL));

  // easy to be different
  static size_t i = 0;
  size_t end = i + count;
  for (; i < end; i++) {
    uint32_t dbid = genRand();
    uint32_t chunkid = genRand();
    auto type = randomType();
    uint64_t this_ttl = ttl;
    if (type == RecordType::RT_KV) {
      kvCount++;
    } else if (!isDataMetaType(type)) {
      this_ttl = 0;
    }
    std::string pk;
    if (allDiff) {
      pk.append(std::to_string(i)).append(randomStr(5, false));
    } else {
      pk.append(randomStr(5, false));
    }
    auto sk = randomStr(5, true);
    auto val = randomStr(5, true);
    auto rk = RecordKey(chunkid, dbid, type, pk, sk);
    auto rv = RecordValue(val, type, -1, this_ttl);

    auto eTxn1 = kvstore->createTransaction(nullptr);
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
#ifdef _WIN32
  myfile << "rocks.compress_type none\n";
#endif
  myfile.close();
  auto cfg = std::make_shared<ServerParams>();
  auto s = cfg->parseFile("a.cfg");
  EXPECT_EQ(s.ok(), true) << s.toString();
  return cfg;
}

std::shared_ptr<ServerParams> genParamsRocks() {
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
  myfile << "rocks.disable_wal 1\n";
  myfile << "rocks.flush_log_at_trx_commit 0\n";
  myfile << "rocks.blockcache_strict_capacity_limit 1\n";
  myfile << "rocks.max_write_buffer_number 5\n";
  myfile << "rocks.write_buffer_size 4096000\n";
  myfile << "rocks.create_if_missing 1\n";
  myfile << "rocks.cache_index_and_filter_blocks 1\n";
#ifdef _WIN32
  myfile << "rocks.compress_type none\n";
#endif

  myfile.close();
  auto cfg = std::make_shared<ServerParams>();
  auto s = cfg->parseFile("a.cfg");
  EXPECT_EQ(s.ok(), true) << s.toString();
  return cfg;
}

void testMaxBinlogId(const std::unique_ptr<RocksKVStore>& kvstore) {
  auto eTxn1 = kvstore->createTransaction(nullptr);
  EXPECT_EQ(eTxn1.ok(), true);
  std::unique_ptr<Transaction> txn1 = std::move(eTxn1.value());

  auto expMax = RepllogCursorV2::getMaxBinlogId(txn1.get());
  EXPECT_TRUE(expMax.ok());

  EXPECT_EQ(expMax.value() + 1, kvstore->getNextBinlogSeq());
}

TEST(RocksKVStore, RocksOptions) {
  auto cfg = genParamsRocks();

  if (!filesystem::exists("db")) {
    EXPECT_TRUE(filesystem::create_directory("db"));
  }
  if (!filesystem::exists("log")) {
    EXPECT_TRUE(filesystem::create_directory("log"));
  }
  const auto guard = MakeGuard([] {
    filesystem::remove_all("./log");
    filesystem::remove_all("./db");
  });
  auto blockCache =
    rocksdb::NewLRUCache(cfg->rocksBlockcacheMB * 1024 * 1024LL, 4);
  auto kvstore = std::make_unique<RocksKVStore>("0", cfg, blockCache);

  EXPECT_EQ(kvstore->getUnderlayerPesDB()->GetOptions().max_write_buffer_number,
            5);
  if (cfg->binlogUsingDefaultCF) {
    EXPECT_EQ(kvstore->getUnderlayerPesDB()->GetOptions().write_buffer_size,
              4096000);
  } else {
    EXPECT_EQ(kvstore->getUnderlayerPesDB()->GetOptions().write_buffer_size,
              4096000 / 2);
  }
  EXPECT_EQ(kvstore->getUnderlayerPesDB()->GetOptions().create_if_missing,
            true);
#if ROCKSDB_MAJOR == 6 && ROCKSDB_MINOR > 13
  rocksdb::BlockBasedTableOptions* option =
    (rocksdb::BlockBasedTableOptions*)kvstore->getUnderlayerPesDB()
      ->GetOptions()
      .table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
  EXPECT_EQ(option->cache_index_and_filter_blocks, true);
#else
  rocksdb::BlockBasedTableOptions* option =
    (rocksdb::BlockBasedTableOptions*)kvstore->getUnderlayerPesDB()
      ->GetOptions()
      .table_factory->GetOptions();
  EXPECT_EQ(option->cache_index_and_filter_blocks, true);
#endif
  LocalSessionGuard sg(nullptr);
  uint64_t ts = genRand();
  uint64_t versionep = genRand();
  sg.getSession()->getCtx()->setExtendProtocolValue(ts, versionep);
  auto eTxn1 = kvstore->createTransaction(sg.getSession());
  EXPECT_EQ(eTxn1.ok(), true);
  std::unique_ptr<Transaction> txn1 = std::move(eTxn1.value());

  RocksTxn* rtxn = dynamic_cast<RocksTxn*>(txn1.get());
  EXPECT_EQ(rtxn->getRocksdbTxn()->GetWriteOptions()->disableWAL, true);
  EXPECT_EQ(rtxn->getRocksdbTxn()->GetWriteOptions()->sync, false);

  RecordKey rk(0, 1, RecordType::RT_KV, "a", "");
  RecordValue rv("txn1", RecordType::RT_KV, -1);
  Status s = kvstore->setKV(rk, rv, txn1.get());
  EXPECT_EQ(s.ok(), true);

  Expected<uint64_t> exptCommitId = txn1->commit();
  EXPECT_EQ(exptCommitId.ok(), true);
}

TEST(RocksKVStore, BinlogRightMost) {
  auto cfg = genParams();

  // filesystem::remove_all("./log");
  // filesystem::remove_all("./db");

  EXPECT_TRUE(filesystem::create_directory("db"));
  EXPECT_TRUE(filesystem::create_directory("log"));
  const auto guard = MakeGuard([] {
    filesystem::remove_all("./log");
    filesystem::remove_all("./db");
  });
  auto blockCache =
    rocksdb::NewLRUCache(cfg->rocksBlockcacheMB * 1024 * 1024LL, 4);
  auto kvstore = std::make_unique<RocksKVStore>("0", cfg, blockCache);

  // default values
  EXPECT_EQ(kvstore->getUnderlayerPesDB()->GetOptions().max_write_buffer_number,
            2);
  EXPECT_EQ(kvstore->getUnderlayerPesDB()->GetOptions().create_if_missing,
            true);
#if ROCKSDB_MAJOR == 6 && ROCKSDB_MINOR > 13
  rocksdb::BlockBasedTableOptions* option =
    (rocksdb::BlockBasedTableOptions*)kvstore->getUnderlayerPesDB()
      ->GetOptions()
      .table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
  EXPECT_EQ(option->cache_index_and_filter_blocks, false);
#else
  rocksdb::BlockBasedTableOptions* option =
    (rocksdb::BlockBasedTableOptions*)kvstore->getUnderlayerPesDB()
      ->GetOptions()
      .table_factory->GetOptions();
  EXPECT_EQ(option->cache_index_and_filter_blocks, false);
#endif
  LocalSessionGuard sg(nullptr);
  uint64_t ts = genRand();
  uint64_t versionep = genRand();
  sg.getSession()->getCtx()->setExtendProtocolValue(ts, versionep);
  auto eTxn1 = kvstore->createTransaction(sg.getSession());
  EXPECT_EQ(eTxn1.ok(), true);
  std::unique_ptr<Transaction> txn1 = std::move(eTxn1.value());

  // default value for transaction
  RocksTxn* rtxn = dynamic_cast<RocksTxn*>(txn1.get());
  EXPECT_EQ(rtxn->getRocksdbTxn()->GetWriteOptions()->disableWAL, false);
  EXPECT_EQ(rtxn->getRocksdbTxn()->GetWriteOptions()->sync, false);

  {
    auto expMin = RepllogCursorV2::getMinBinlogId(txn1.get());
    EXPECT_TRUE(!expMin.ok());
    EXPECT_TRUE(expMin.status().code() == ErrorCodes::ERR_EXHAUST);

    auto expMax = RepllogCursorV2::getMaxBinlogId(txn1.get());
    EXPECT_TRUE(!expMax.ok());
    EXPECT_TRUE(expMax.status().code() == ErrorCodes::ERR_EXHAUST);

    auto expMinB = RepllogCursorV2::getMinBinlog(txn1.get());
    EXPECT_TRUE(!expMinB.ok());
    EXPECT_TRUE(expMinB.status().code() == ErrorCodes::ERR_EXHAUST);
  }

  RecordKey rk(0, 1, RecordType::RT_KV, "a", "");
  RecordValue rv("txn1", RecordType::RT_KV, -1);
  Status s = kvstore->setKV(rk, rv, txn1.get());
  EXPECT_EQ(s.ok(), true);

  Expected<uint64_t> exptCommitId = txn1->commit();
  EXPECT_EQ(exptCommitId.ok(), true);

  auto eTxn2 = kvstore->createTransaction(sg.getSession());
  EXPECT_EQ(eTxn2.ok(), true);
  std::unique_ptr<Transaction> txn2 = std::move(eTxn2.value());
  {
    auto expMin = RepllogCursorV2::getMinBinlogId(txn2.get());
    EXPECT_TRUE(expMin.ok());
    EXPECT_EQ(expMin.value(), 1);

    auto expMax = RepllogCursorV2::getMaxBinlogId(txn2.get());
    EXPECT_TRUE(expMax.ok());
    EXPECT_EQ(expMax.value(), 1);

    auto expMinB = RepllogCursorV2::getMinBinlog(txn2.get());
    EXPECT_TRUE(expMinB.ok());
    EXPECT_EQ(expMinB.value().getBinlogId(), 1);
    EXPECT_EQ(expMinB.value().getVersionEp(), versionep);
  }
  auto bcursor = txn2->createRepllogCursorV2(Transaction::MIN_VALID_TXNID);
  auto ss = bcursor->seekToLast();
  EXPECT_TRUE(ss.ok());
  auto v = bcursor->nextV2();
  EXPECT_TRUE(v.ok());
  if (v.ok()) {
    ReplLogV2& log = v.value();
    EXPECT_EQ(log.getReplLogValueEntrys().size(), 1);
    EXPECT_EQ(log.getReplLogValue().getVersionEp(), versionep);

    EXPECT_EQ(log.getReplLogValueEntrys()[0].getOpKey(), rk.encode());
    EXPECT_EQ(log.getReplLogValueEntrys()[0].getOpValue(), rv.encode());
  }

  Expected<uint64_t> exptCommitId2 = txn2->commit();
  EXPECT_EQ(exptCommitId2.ok(), true);
  testMaxBinlogId(kvstore);
}

TEST(RocksKVStore, RepllogCursorV2) {
  auto cfg = genParams();
  EXPECT_TRUE(filesystem::create_directory("db"));
  EXPECT_TRUE(filesystem::create_directory("log"));

  uint64_t ts0 = msSinceEpoch();
  const auto guard = MakeGuard([] {
    filesystem::remove_all("./log");
    filesystem::remove_all("./db");
  });
  auto blockCache =
    rocksdb::NewLRUCache(cfg->rocksBlockcacheMB * 1024 * 1024LL, 4);
  auto kvstore = std::make_unique<RocksKVStore>("0", cfg, blockCache);

  LocalSessionGuard sg(nullptr);
  uint64_t tsep = genRand();
  uint64_t versionep = genRand();
  sg.getSession()->getCtx()->setExtendProtocolValue(tsep, versionep);
  auto eTxn1 = kvstore->createTransaction(sg.getSession());
  EXPECT_EQ(eTxn1.ok(), true);
  std::unique_ptr<Transaction> txn1 = std::move(eTxn1.value());

  Status s = kvstore->setKV(Record(RecordKey(0, 0, RecordType::RT_KV, "a", ""),
                                   RecordValue("txn1", RecordType::RT_KV, -1)),
                            txn1.get());
  EXPECT_EQ(s.ok(), true);

  s = kvstore->setKV(Record(RecordKey(0, 0, RecordType::RT_KV, "ab", ""),
                            RecordValue("txn1", RecordType::RT_KV, -1)),
                     txn1.get());
  EXPECT_EQ(s.ok(), true);

  // different chunk
  s = kvstore->setKV(Record(RecordKey(1, 0, RecordType::RT_KV, "abc", ""),
                            RecordValue("txn1", RecordType::RT_KV, -1)),
                     txn1.get());
  EXPECT_EQ(s.ok(), true);

  Expected<uint64_t> exptCommitId = txn1->commit();
  EXPECT_TRUE(exptCommitId.ok());
  EXPECT_EQ(exptCommitId.value(), 1U);
  EXPECT_EQ(kvstore->getNextBinlogSeq(), 2U);

  auto eTxn2 = kvstore->createTransaction(sg.getSession());
  EXPECT_EQ(eTxn2.ok(), true);
  std::unique_ptr<Transaction> txn2 = std::move(eTxn2.value());
  auto bcursor = txn2->createRepllogCursorV2(1);

  auto eTxn3 = kvstore->createTransaction(sg.getSession());
  EXPECT_EQ(eTxn3.ok(), true);
  std::unique_ptr<Transaction> txn3 = std::move(eTxn3.value());

  uint64_t ts = msSinceEpoch();
  uint64_t chunkId = genRand() % 1000;
  uint64_t binlogid = kvstore->getNextBinlogSeq();
  s = kvstore->setKV(Record(RecordKey(chunkId, 0, RecordType::RT_KV, "b", ""),
                            RecordValue("txn3", RecordType::RT_KV, -1)),
                     txn3.get());
  EXPECT_EQ(s.ok(), true);

  exptCommitId = txn3->commit();
  EXPECT_EQ(exptCommitId.ok(), true);
  EXPECT_EQ(exptCommitId.value(), 3U);
  EXPECT_EQ(kvstore->getNextBinlogSeq(), 3U);

  int32_t cnt = 0;
  while (true) {
    // the cursor can't get the last binlog because of the snapshot
    auto v = bcursor->next();
    if (!v.ok()) {
      EXPECT_EQ(v.status().code(), ErrorCodes::ERR_EXHAUST);
      break;
    }
    EXPECT_LE(v.value().getTimestamp(), ts);
    EXPECT_GE(v.value().getTimestamp(), ts0);
    EXPECT_EQ(v.value().getVersionEp(), versionep);
    EXPECT_LE(v.value().getBinlogId(), binlogid - 1);
    uint64_t invalid = Transaction::TXNID_UNINITED;
    EXPECT_NE(v.value().getBinlogId(), invalid);
    // different chunk
    uint64_t multi = Transaction::CHUNKID_MULTI;
    EXPECT_EQ(v.value().getChunkId(), multi);

    cnt += 1;
  }
  EXPECT_EQ(cnt, 1);
  exptCommitId = txn2->commit();
  EXPECT_TRUE(exptCommitId.ok());
  EXPECT_EQ(exptCommitId.value(), 2U);
  EXPECT_EQ(kvstore->getNextBinlogSeq(), 3U);

  cnt = 0;
  auto eTxn4 = kvstore->createTransaction(sg.getSession());
  EXPECT_EQ(eTxn4.ok(), true);
  std::unique_ptr<Transaction> txn4 = std::move(eTxn4.value());
  auto bcursor1 = txn4->createRepllogCursorV2(2);
  while (true) {
    auto v = bcursor1->nextV2();
    if (!v.ok()) {
      EXPECT_EQ(v.status().code(), ErrorCodes::ERR_EXHAUST);
      break;
    } else {
      ReplLogV2& log = v.value();

      EXPECT_EQ(log.getReplLogValueEntrys().size(), 1);
      EXPECT_EQ(log.getReplLogKey().getBinlogId(), 2);
      EXPECT_EQ((uint16_t)log.getReplLogValue().getReplFlag(),
                (uint16_t)ReplFlag::REPL_GROUP_START |
                  (uint16_t)ReplFlag::REPL_GROUP_END);  // NOLINT
      EXPECT_EQ(log.getReplLogValue().getTxnId(), 3);
      EXPECT_EQ(log.getReplLogValue().getVersionEp(), versionep);
      uint64_t multi = Transaction::CHUNKID_MULTI;
      EXPECT_NE(log.getReplLogValue().getChunkId(), multi);
      EXPECT_EQ(log.getReplLogValue().getChunkId(), chunkId);
    }

    cnt += 1;
  }
  EXPECT_EQ(cnt, 1);
  testMaxBinlogId(kvstore);
}

void cursorVisibleRoutine(RocksKVStore* kvstore) {
  auto eTxn1 = kvstore->createTransaction(nullptr);
  EXPECT_EQ(eTxn1.ok(), true);
  std::unique_ptr<Transaction> txn1 = std::move(eTxn1.value());

  Status s = kvstore->setKV(Record(RecordKey(0, 0, RecordType::RT_KV, "a", ""),
                                   RecordValue("txn1", RecordType::RT_KV, -1)),
                            txn1.get());
  EXPECT_EQ(s.ok(), true);

  s = kvstore->setKV(Record(RecordKey(0, 0, RecordType::RT_KV, "ab", ""),
                            RecordValue("txn1", RecordType::RT_KV, -1)),
                     txn1.get());
  EXPECT_EQ(s.ok(), true);

  s = kvstore->setKV(Record(RecordKey(0, 0, RecordType::RT_KV, "abc", ""),
                            RecordValue("txn1", RecordType::RT_KV, -1)),
                     txn1.get());
  EXPECT_EQ(s.ok(), true);

  s = kvstore->setKV(Record(RecordKey(0, 0, RecordType::RT_KV, "b", ""),
                            RecordValue("txn1", RecordType::RT_KV, -1)),
                     txn1.get());
  EXPECT_EQ(s.ok(), true);

  s = kvstore->setKV(Record(RecordKey(0, 0, RecordType::RT_KV, "bac", ""),
                            RecordValue("txn1", RecordType::RT_KV, -1)),
                     txn1.get());
  EXPECT_EQ(s.ok(), true);

  std::unique_ptr<BasicDataCursor> cursor = txn1->createDataCursor();
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

  auto s1 = txn1->commit();
  EXPECT_TRUE(s1.ok());
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
  auto kvstore = std::make_unique<RocksKVStore>("0",
                                                cfg,
                                                blockCache,
                                                true,
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
  auto kvstore = std::make_unique<RocksKVStore>("0",
                                                cfg,
                                                blockCache,
                                                true,
                                                KVStore::StoreMode::READ_WRITE,
                                                RocksKVStore::TxnMode::TXN_PES);
  cursorVisibleRoutine(kvstore.get());
}

void setKV(RocksKVStore* kvstore,
           uint32_t chunkid,
           const string& prefix,
           uint32_t num) {
  auto eTxn1 = kvstore->createTransaction(nullptr);
  EXPECT_EQ(eTxn1.ok(), true);
  std::unique_ptr<Transaction> txn1 = std::move(eTxn1.value());
  for (uint32_t i = 0; i < num; i++) {
    string key = prefix + to_string(i);
    Status s = kvstore->setKV(
      Record(RecordKey(chunkid, 0, RecordType::RT_KV, key, ""),
             RecordValue("12345abcdefghijklmn", RecordType::RT_KV, -1)),
      txn1.get());
    EXPECT_EQ(s.ok(), true);
  }
  txn1->commit();
}

TEST(RocksKVStore, CursorUpperBound) {
  auto cfg = genParams();
  EXPECT_TRUE(filesystem::create_directory("db"));
  EXPECT_TRUE(filesystem::create_directory("log"));
  const auto guard = MakeGuard([] {
    filesystem::remove_all("./log");
    filesystem::remove_all("./db");
  });
  auto blockCache =
    rocksdb::NewLRUCache(cfg->rocksBlockcacheMB * 1024 * 1024LL, 4);
  auto kvstore = std::make_unique<RocksKVStore>("0", cfg, blockCache);

  setKV(kvstore.get(), 0, "a", 10000);
  setKV(kvstore.get(), 0, "b", 10000);
  setKV(kvstore.get(), 0, "c", 10000);
  setKV(kvstore.get(), 1, "d", 10000);

  auto eTxn2 = kvstore->createTransaction(nullptr);
  EXPECT_EQ(eTxn2.ok(), true);
  std::unique_ptr<Transaction> txn2 = std::move(eTxn2.value());
  RecordKey upper(1, 0, RecordType::RT_INVALID, "", "");
  string upperBound = upper.prefixChunkid();
  // NOTE(takenliu) RocksTxn::createCursor not be public anymore
  std::unique_ptr<SlotCursor> cursor =
    txn2->createSlotCursor(0);

  int32_t cnt = 0;
  while (true) {
    Expected<Record> v = cursor->next();
    if (!v.ok()) {
      EXPECT_EQ(v.status().code(), ErrorCodes::ERR_EXHAUST);
      break;
    }
    cnt += 1;
  }
  EXPECT_EQ(cnt, 30000);

  cursor = txn2->createSlotCursor(1);
  cnt = 0;
  while (true) {
    Expected<Record> v = cursor->next();
    if (!v.ok()) {
      EXPECT_EQ(v.status().code(), ErrorCodes::ERR_EXHAUST);
      break;
    }
    cnt += 1;
  }
  EXPECT_EQ(cnt, 10000);
}

TEST(RocksKVStore, BackupCkptInter) {
  auto cfg = genParams();
  EXPECT_TRUE(filesystem::create_directory("db"));
  EXPECT_TRUE(filesystem::create_directory("log"));
  const auto guard = MakeGuard([] {
    filesystem::remove_all("./log");
    filesystem::remove_all("./db");
  });
  auto blockCache =
    rocksdb::NewLRUCache(cfg->rocksBlockcacheMB * 1024 * 1024LL, 4);
  auto kvstore = std::make_unique<RocksKVStore>("0", cfg, blockCache);

  auto eTxn1 = kvstore->createTransaction(nullptr);
  EXPECT_EQ(eTxn1.ok(), true);
  std::unique_ptr<Transaction> txn1 = std::move(eTxn1.value());
  Status s = kvstore->setKV(Record(RecordKey(0, 0, RecordType::RT_KV, "a", ""),
                                   RecordValue("txn1", RecordType::RT_KV, -1)),
                            txn1.get());
  EXPECT_EQ(s.ok(), true);
  Expected<uint64_t> exptCommitId = txn1->commit();
  EXPECT_EQ(exptCommitId.ok(), true);

  Expected<BackupInfo> expBk = kvstore->backup(
    kvstore->dftBackupDir(),
    KVStore::BackupMode::BACKUP_CKPT_INTER,
    cfg->binlogUsingDefaultCF ? BinlogVersion::BINLOG_VERSION_1
                              : BinlogVersion::BINLOG_VERSION_2);
  EXPECT_TRUE(expBk.ok()) << expBk.status().toString();
  for (auto& bk : expBk.value().getFileList()) {
    LOG(INFO) << "backupInfo:[" << bk.first << "," << bk.second << "]";
  }
  EXPECT_TRUE(filesystem::exists(kvstore->dftBackupDir() + "/backup_meta"));

  // backup failed, set the backup state to false
  Expected<BackupInfo> expBk1 = kvstore->backup(
    kvstore->dftBackupDir(),
    KVStore::BackupMode::BACKUP_CKPT_INTER,
    cfg->binlogUsingDefaultCF ? BinlogVersion::BINLOG_VERSION_1
                              : BinlogVersion::BINLOG_VERSION_2);
  EXPECT_FALSE(expBk1.ok());
  EXPECT_TRUE(filesystem::exists(kvstore->dftBackupDir() + "/backup_meta"));

  // backup failed, set the backup state to false
  Expected<BackupInfo> expBk2 = kvstore->backup(
    "wrong_dir",
    KVStore::BackupMode::BACKUP_CKPT_INTER,
    cfg->binlogUsingDefaultCF ? BinlogVersion::BINLOG_VERSION_1
                              : BinlogVersion::BINLOG_VERSION_2);
  EXPECT_FALSE(expBk2.ok());

  s = kvstore->stop();
  EXPECT_TRUE(s.ok());

  s = kvstore->clear();
  EXPECT_TRUE(s.ok());

  uint64_t lastCommitId = exptCommitId.value();
  exptCommitId = kvstore->restart(true);
  EXPECT_TRUE(exptCommitId.ok()) << exptCommitId.status().toString();
  EXPECT_EQ(exptCommitId.value(), lastCommitId);

  eTxn1 = kvstore->createTransaction(nullptr);
  EXPECT_EQ(eTxn1.ok(), true);
  txn1 = std::move(eTxn1.value());
  Expected<RecordValue> e =
    kvstore->getKV(RecordKey(0, 0, RecordType::RT_KV, "a", ""), txn1.get());
  EXPECT_EQ(e.ok(), true);
  testMaxBinlogId(kvstore);
}

TEST(RocksKVStore, BackupCkpt) {
  auto cfg = genParams();
  string backup_dir = "backup";
  EXPECT_TRUE(filesystem::create_directory("db"));
  EXPECT_TRUE(filesystem::create_directory("log"));
  // EXPECT_TRUE(filesystem::create_directory(backup_dir));

  const auto guard = MakeGuard([backup_dir] {
    filesystem::remove_all("./log");
    filesystem::remove_all("./db");
    filesystem::remove_all(backup_dir);
  });
  auto blockCache =
    rocksdb::NewLRUCache(cfg->rocksBlockcacheMB * 1024 * 1024LL, 4);
  auto kvstore = std::make_unique<RocksKVStore>("0", cfg, blockCache);

  auto eTxn1 = kvstore->createTransaction(nullptr);
  EXPECT_EQ(eTxn1.ok(), true);
  std::unique_ptr<Transaction> txn1 = std::move(eTxn1.value());
  Status s = kvstore->setKV(Record(RecordKey(0, 0, RecordType::RT_KV, "a", ""),
                                   RecordValue("txn1", RecordType::RT_KV, -1)),
                            txn1.get());
  EXPECT_EQ(s.ok(), true);
  Expected<uint64_t> exptCommitId = txn1->commit();
  EXPECT_EQ(exptCommitId.ok(), true);

  auto binlogversion = cfg->binlogUsingDefaultCF
    ? BinlogVersion::BINLOG_VERSION_1
    : BinlogVersion::BINLOG_VERSION_2;

  Expected<BackupInfo> expBk0 = kvstore->backup(
    kvstore->dftBackupDir(), KVStore::BackupMode::BACKUP_CKPT, binlogversion);
  EXPECT_EQ(expBk0.status().toString(),
            "-ERR:3,msg:BACKUP_CKPT|BACKUP_COPY cant equal "
            "dftBackupDir:./db/0_bak\r\n");

  Expected<BackupInfo> expBk1 = kvstore->backup(
    backup_dir, KVStore::BackupMode::BACKUP_CKPT, binlogversion);
  EXPECT_TRUE(expBk1.ok()) << expBk1.status().toString();
  for (auto& bk : expBk1.value().getFileList()) {
    LOG(INFO) << "backupInfo:[" << bk.first << "," << bk.second << "]";
  }
  EXPECT_TRUE(filesystem::exists(backup_dir + "/backup_meta"));

  Expected<BackupInfo> expBk2 = kvstore->backup(
    backup_dir, KVStore::BackupMode::BACKUP_CKPT, binlogversion);
  EXPECT_EQ(expBk2.status().toString(),
            "-ERR:3,msg:Invalid argument: Directory exists\r\n");

  s = kvstore->stop();
  EXPECT_TRUE(s.ok());

  s = kvstore->clear();
  EXPECT_TRUE(s.ok());

  Expected<std::string> ret = kvstore->restoreBackup(backup_dir);
  EXPECT_TRUE(ret.ok());

  uint64_t lastCommitId = exptCommitId.value();
  exptCommitId = kvstore->restart(false);
  EXPECT_TRUE(exptCommitId.ok()) << exptCommitId.status().toString();
  EXPECT_EQ(exptCommitId.value(), lastCommitId);

  eTxn1 = kvstore->createTransaction(nullptr);
  EXPECT_EQ(eTxn1.ok(), true);
  txn1 = std::move(eTxn1.value());
  Expected<RecordValue> e =
    kvstore->getKV(RecordKey(0, 0, RecordType::RT_KV, "a", ""), txn1.get());
  EXPECT_EQ(e.ok(), true);
  testMaxBinlogId(kvstore);
}

TEST(RocksKVStore, BackupCopy) {
  auto cfg = genParams();
  string backup_dir = "backup";
  EXPECT_TRUE(filesystem::create_directory("db"));
  EXPECT_TRUE(filesystem::create_directory("log"));
  EXPECT_TRUE(filesystem::create_directory(backup_dir));
  const auto guard = MakeGuard([backup_dir] {
    filesystem::remove_all("./log");
    filesystem::remove_all("./db");
    filesystem::remove_all(backup_dir);
  });
  auto blockCache =
    rocksdb::NewLRUCache(cfg->rocksBlockcacheMB * 1024 * 1024LL, 4);
  auto kvstore = std::make_unique<RocksKVStore>("0", cfg, blockCache);

  auto eTxn1 = kvstore->createTransaction(nullptr);
  EXPECT_EQ(eTxn1.ok(), true);
  std::unique_ptr<Transaction> txn1 = std::move(eTxn1.value());
  Status s = kvstore->setKV(Record(RecordKey(0, 0, RecordType::RT_KV, "a", ""),
                                   RecordValue("txn1", RecordType::RT_KV, -1)),
                            txn1.get());
  EXPECT_EQ(s.ok(), true);
  Expected<uint64_t> exptCommitId = txn1->commit();
  EXPECT_EQ(exptCommitId.ok(), true);

  auto binlogversion = cfg->binlogUsingDefaultCF
    ? BinlogVersion::BINLOG_VERSION_1
    : BinlogVersion::BINLOG_VERSION_2;
  Expected<BackupInfo> expBk0 = kvstore->backup(
    kvstore->dftBackupDir(), KVStore::BackupMode::BACKUP_COPY, binlogversion);
  EXPECT_FALSE(expBk0.ok());

  Expected<BackupInfo> expBk1 = kvstore->backup(
    backup_dir, KVStore::BackupMode::BACKUP_COPY, binlogversion);
  EXPECT_TRUE(expBk1.ok());
  for (auto& bk : expBk1.value().getFileList()) {
    LOG(INFO) << "backupInfo:[" << bk.first << "," << bk.second << "]";
  }
  EXPECT_TRUE(filesystem::exists(backup_dir + "/backup_meta"));

  Expected<BackupInfo> expBk2 = kvstore->backup(
    backup_dir, KVStore::BackupMode::BACKUP_COPY, binlogversion);
  // BackupEngine will delete dir if not null.
  EXPECT_TRUE(expBk2.ok());
  EXPECT_TRUE(filesystem::exists(backup_dir + "/backup_meta"));


  s = kvstore->stop();
  EXPECT_TRUE(s.ok());

  s = kvstore->clear();
  EXPECT_TRUE(s.ok());

  Expected<std::string> ret = kvstore->restoreBackup(backup_dir);
  EXPECT_TRUE(ret.ok());

  uint64_t lastCommitId = exptCommitId.value();
  exptCommitId = kvstore->restart(false);
  EXPECT_TRUE(exptCommitId.ok()) << exptCommitId.status().toString();
  EXPECT_EQ(exptCommitId.value(), lastCommitId);

  eTxn1 = kvstore->createTransaction(nullptr);
  EXPECT_EQ(eTxn1.ok(), true);
  txn1 = std::move(eTxn1.value());
  Expected<RecordValue> e =
    kvstore->getKV(RecordKey(0, 0, RecordType::RT_KV, "a", ""), txn1.get());
  EXPECT_EQ(e.ok(), true);
  testMaxBinlogId(kvstore);
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
  auto kvstore = std::make_unique<RocksKVStore>("0", cfg, blockCache);
  auto eTxn1 = kvstore->createTransaction(nullptr);
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

void commonRoutine(RocksKVStore* kvstore) {
  auto eTxn1 = kvstore->createTransaction(nullptr);
  auto eTxn2 = kvstore->createTransaction(nullptr);
  EXPECT_EQ(eTxn1.ok(), true);
  EXPECT_EQ(eTxn2.ok(), true);
  std::unique_ptr<Transaction> txn1 = std::move(eTxn1.value());
  std::unique_ptr<Transaction> txn2 = std::move(eTxn2.value());

  std::set<uint64_t> uncommitted = kvstore->getUncommittedTxns();
  EXPECT_NE(uncommitted.find(dynamic_cast<RocksTxn*>(txn1.get())->getTxnId()),
            uncommitted.end());
  EXPECT_NE(uncommitted.find(dynamic_cast<RocksTxn*>(txn2.get())->getTxnId()),
            uncommitted.end());

  Status s = kvstore->setKV(Record(RecordKey(0, 0, RecordType::RT_KV, "a", ""),
                                   RecordValue("txn1", RecordType::RT_KV, -1)),
                            txn1.get());
  EXPECT_EQ(s.ok(), true);
  Expected<RecordValue> e =
    kvstore->getKV(RecordKey(0, 0, RecordType::RT_KV, "a", ""), txn1.get());
  EXPECT_EQ(e.ok(), true);
  EXPECT_EQ(e.value(), RecordValue("txn1", RecordType::RT_KV, -1));

  Expected<RecordValue> e1 =
    kvstore->getKV(RecordKey(0, 0, RecordType::RT_KV, "a", ""), txn2.get());
  EXPECT_EQ(e1.status().code(), ErrorCodes::ERR_NOTFOUND);
  s = kvstore->setKV(Record(RecordKey(0, 0, RecordType::RT_KV, "a", ""),
                            RecordValue("txn2", RecordType::RT_KV, -1)),
                     txn2.get());
  if (kvstore->getTxnMode() == RocksKVStore::TxnMode::TXN_OPT) {
    EXPECT_EQ(s.code(), ErrorCodes::ERR_OK);
    Expected<uint64_t> exptCommitId = txn2->commit();
    EXPECT_EQ(exptCommitId.ok(), true);
    exptCommitId = txn1->commit();
    EXPECT_EQ(exptCommitId.status().code(), ErrorCodes::ERR_COMMIT_RETRY);
    uncommitted = kvstore->getUncommittedTxns();
    EXPECT_EQ(uncommitted.find(dynamic_cast<RocksTxn*>(txn1.get())->getTxnId()),
              uncommitted.end());
    EXPECT_EQ(uncommitted.find(dynamic_cast<RocksTxn*>(txn2.get())->getTxnId()),
              uncommitted.end());
  } else {
    EXPECT_EQ(s.code(), ErrorCodes::ERR_INTERNAL);
    s = txn2->rollback();
    EXPECT_EQ(s.code(), ErrorCodes::ERR_OK);
    Expected<uint64_t> exptCommitId = txn1->commit();
    EXPECT_EQ(exptCommitId.ok(), true);
    uncommitted = kvstore->getUncommittedTxns();
    // TODO(qingping209): check txn1.get() is nullptr
    EXPECT_EQ(uncommitted.find(dynamic_cast<RocksTxn*>(txn1.get())->getTxnId()),
              uncommitted.end());
    EXPECT_EQ(uncommitted.find(dynamic_cast<RocksTxn*>(txn2.get())->getTxnId()),
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
  auto kvstore = std::make_unique<RocksKVStore>("0",
                                                cfg,
                                                blockCache,
                                                true,
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
  auto kvstore = std::make_unique<RocksKVStore>("0",
                                                cfg,
                                                blockCache,
                                                true,
                                                KVStore::StoreMode::READ_WRITE,
                                                RocksKVStore::TxnMode::TXN_PES);
  commonRoutine(kvstore.get());
}

uint64_t getBinlogCount(Transaction* txn) {
  auto bcursor = txn->createRepllogCursorV2(Transaction::MIN_VALID_TXNID, true);
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
  uint64_t keepBinlog = 1;
  cfg->maxBinlogKeepNum = keepBinlog;
  cfg->minBinlogKeepSec = 0;
  auto kvstore = std::make_unique<RocksKVStore>("0",
                                                cfg,
                                                blockCache,
                                                true,
                                                KVStore::StoreMode::READ_WRITE,
                                                RocksKVStore::TxnMode::TXN_PES);

  LocalSessionGuard sg(nullptr);
  uint64_t firstBinlog = 1;
  {
    auto eTxn1 = kvstore->createTransaction(sg.getSession());
    EXPECT_EQ(eTxn1.ok(), true);
    std::unique_ptr<Transaction> txn1 = std::move(eTxn1.value());

    RecordKey rk(0, 1, RecordType::RT_KV, std::to_string(0), "");
    RecordValue rv("txn1", RecordType::RT_KV, -1);
    Status s = kvstore->setKV(rk, rv, txn1.get());
    EXPECT_EQ(s.ok(), true);

    Expected<uint64_t> exptCommitId = txn1->commit();
    EXPECT_EQ(exptCommitId.ok(), true);
  }

  {
    auto eTxn1 = kvstore->createTransaction(sg.getSession());
    EXPECT_EQ(eTxn1.ok(), true);
    std::unique_ptr<Transaction> txn1 = std::move(eTxn1.value());

    auto expMin = RepllogCursorV2::getMinBinlogId(txn1.get());
    EXPECT_TRUE(expMin.ok());
    EXPECT_EQ(expMin.value(), 1U);

    auto expMax = RepllogCursorV2::getMaxBinlogId(txn1.get());
    EXPECT_TRUE(expMax.ok());
    EXPECT_EQ(expMax.value(), 1U);
  }
  {
    auto eTxn1 = kvstore->createTransaction(sg.getSession());
    EXPECT_EQ(eTxn1.ok(), true);
    std::unique_ptr<Transaction> txn1 = std::move(eTxn1.value());

    RecordKey rk(0, 1, RecordType::RT_KV, std::to_string(1), "");
    RecordValue rv("txn1", RecordType::RT_KV, -1);
    Status s = kvstore->setKV(rk, rv, txn1.get());
    EXPECT_EQ(s.ok(), true);

    Expected<uint64_t> exptCommitId = txn1->commit();
    EXPECT_EQ(exptCommitId.ok(), true);

    auto eTxn2 = kvstore->createTransaction(sg.getSession());
    EXPECT_EQ(eTxn2.ok(), true);
    std::unique_ptr<Transaction> txn2 = std::move(eTxn2.value());
    auto currentCnt = kvstore->getBinlogCnt(txn2.get());
    EXPECT_TRUE(currentCnt.ok());
    EXPECT_EQ(currentCnt.value(), 2U);
  }
  {
    auto eTxn1 = kvstore->createTransaction(sg.getSession());
    EXPECT_EQ(eTxn1.ok(), true);
    std::unique_ptr<Transaction> txn1 = std::move(eTxn1.value());
    uint64_t ts = 0;
    uint64_t written = 0;
    uint64_t deleten = 0;
    // TODO(takenliu): save binlog
    auto s = kvstore->truncateBinlogV2(
      firstBinlog, firstBinlog, firstBinlog, txn1.get(), nullptr, 0, false);
    EXPECT_TRUE(s.ok());
    ts = s.value().timestamp;
    written = s.value().written;
    deleten = s.value().deleten;
    EXPECT_GT(ts, std::numeric_limits<uint32_t>::max());
    EXPECT_EQ(written, 0);
    EXPECT_GT(s.value().newStart, firstBinlog);
    EXPECT_EQ(s.value().newStart, 2U);
    EXPECT_EQ(deleten, s.value().newStart - firstBinlog);
    firstBinlog = s.value().newStart;
    uint64_t currentCnt = kvstore->getBinlogCnt(txn1.get()).value();
    EXPECT_EQ(currentCnt, keepBinlog);


    Expected<uint64_t> exptCommitId = txn1->commit();
    EXPECT_EQ(exptCommitId.ok(), true);
  }
  {
    auto eTxn = kvstore->createTransaction(sg.getSession());
    EXPECT_EQ(eTxn.ok(), true);
    std::unique_ptr<Transaction> txn = std::move(eTxn.value());
    uint64_t currentCnt = kvstore->getBinlogCnt(txn.get()).value();
    EXPECT_EQ(currentCnt, 1U);

    auto expMin1 = RepllogCursorV2::getMinBinlogId(txn.get());
    EXPECT_TRUE(expMin1.ok());
    firstBinlog = expMin1.value();

    Expected<uint64_t> exptCommitId = txn->commit();
    EXPECT_EQ(exptCommitId.ok(), true);

    uint32_t txnCnt = 0;
    for (auto range : {10, 100, 1000}) {
      for (int i = 0; i < range; ++i) {
        auto eTxn1 = kvstore->createTransaction(sg.getSession());
        EXPECT_EQ(eTxn1.ok(), true);
        std::unique_ptr<Transaction> txn1 = std::move(eTxn1.value());

        size_t cnt = genRand() % 123 + 1;
        for (size_t j = 0; j < cnt; j++) {
          RecordKey rk(0, 1, RecordType::RT_KV, std::to_string(j * range), "");
          RecordValue rv("txn1", RecordType::RT_KV, -1);
          Status s;
          if (j % 2 == 0) {
            s = kvstore->setKV(rk, rv, txn1.get());
          } else {
            s = kvstore->delKV(rk, txn1.get());
          }
          EXPECT_EQ(s.ok(), true);
        }

        Expected<uint64_t> exptCommitId = txn1->commit();
        EXPECT_EQ(exptCommitId.ok(), true);
        txnCnt++;
      }
    }
    uint64_t endBinlog = 0;
    {
      auto eTxn2 = kvstore->createTransaction(sg.getSession());
      EXPECT_EQ(eTxn2.ok(), true);
      std::unique_ptr<Transaction> txn2 = std::move(eTxn2.value());
      auto cnt = kvstore->getBinlogCnt(txn2.get());
      EXPECT_TRUE(cnt.ok());
      EXPECT_EQ(currentCnt + txnCnt, cnt.value());
      currentCnt = cnt.value();

      auto m = RepllogCursorV2::getMaxBinlogId(txn2.get());
      EXPECT_TRUE(m.ok());
      EXPECT_EQ(firstBinlog + txnCnt, m.value());
      endBinlog = m.value();

      auto s = kvstore->validateAllBinlog(txn2.get());
      EXPECT_TRUE(s.ok());
      EXPECT_TRUE(s.value());

      Expected<uint64_t> exptCommitId2 = txn2->commit();
      EXPECT_EQ(exptCommitId2.ok(), true);
    }

    uint64_t ts = 0;
    uint64_t written = 0;
    uint64_t deleten = 0;

    uint64_t lastFirstBinlog = 0;

    while (firstBinlog != lastFirstBinlog) {
      lastFirstBinlog = firstBinlog;

      auto eTxn2 = kvstore->createTransaction(sg.getSession());
      EXPECT_EQ(eTxn2.ok(), true);
      std::unique_ptr<Transaction> txn2 = std::move(eTxn2.value());
      // TODO(takenliu): save binlog
      auto s = kvstore->truncateBinlogV2(firstBinlog,
                                         endBinlog - keepBinlog,
                                         firstBinlog,
                                         txn2.get(),
                                         nullptr,
                                         0,
                                         false);
      EXPECT_TRUE(s.ok());
      if (!s.value().deleten) {
        EXPECT_EQ(s.value().newStart, firstBinlog);
        firstBinlog = s.value().newStart;
        break;
      }
      ts = s.value().timestamp;
      written = s.value().written;
      deleten = s.value().deleten;
      EXPECT_GT(ts, std::numeric_limits<uint32_t>::max());
      EXPECT_EQ(written, 0);
      EXPECT_GT(s.value().newStart, firstBinlog);
      EXPECT_EQ(deleten, s.value().newStart - firstBinlog);
      firstBinlog = s.value().newStart;

      Expected<uint64_t> exptCommitId2 = txn2->commit();
      EXPECT_EQ(exptCommitId2.ok(), true);
    }
    {
      auto eTxn2 = kvstore->createTransaction(sg.getSession());
      EXPECT_EQ(eTxn2.ok(), true);
      std::unique_ptr<Transaction> txn2 = std::move(eTxn2.value());
      auto cnt = kvstore->getBinlogCnt(txn2.get());
      EXPECT_TRUE(cnt.ok());
      EXPECT_EQ(cnt.value(), keepBinlog);

      auto s = kvstore->validateAllBinlog(txn2.get());
      EXPECT_TRUE(s.ok());
      EXPECT_TRUE(s.value());

      Expected<uint64_t> exptCommitId2 = txn2->commit();
      EXPECT_EQ(exptCommitId2.ok(), true);
    }
  }
  testMaxBinlogId(kvstore);
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
  auto kvstore = std::make_unique<RocksKVStore>("0",
                                                cfg,
                                                blockCache,
                                                true,
                                                KVStore::StoreMode::READ_WRITE,
                                                RocksKVStore::TxnMode::TXN_PES);

  SyncPoint::GetInstance()->EnableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  uint64_t totalFilter = 0;
  SyncPoint::GetInstance()->SetCallBack(
    "InspectKvTtlFilterCount", [&](void* arg) mutable {
      uint64_t* tmp = reinterpret_cast<uint64_t*>(arg);
      totalFilter = *tmp;
    });

  uint64_t totalExpired = 0;
  bool hasCalled = false;
  SyncPoint::GetInstance()->SetCallBack(
    "InspectKvTtlExpiredCount", [&](void* arg) mutable {
      hasCalled = true;
      uint64_t* tmp = reinterpret_cast<uint64_t*>(arg);
      totalExpired = *tmp;
    });

  uint32_t waitSec = 10;
  // if we want to check the totalFilter, all data should be different
  genData(kvstore.get(), 1000, 0, true);
  size_t kvCount = genData(kvstore.get(), 1000, msSinceEpoch(), true);
  size_t kvCount2 =
    genData(kvstore.get(), 1000, msSinceEpoch() + waitSec * 1000, true);

  std::this_thread::sleep_for(std::chrono::seconds(1));
  // compact data in the default column family
  auto status = kvstore->compactRange(
    ColumnFamilyNumber::ColumnFamily_Default, nullptr, nullptr);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(hasCalled);

  if (cfg->binlogUsingDefaultCF == true) {
    EXPECT_EQ(totalFilter, 3000 * 2);
  } else {
    EXPECT_EQ(totalFilter, 3000);
  }
  EXPECT_EQ(totalExpired, kvCount);

  std::this_thread::sleep_for(std::chrono::seconds(waitSec));

  status = kvstore->compactRange(
    ColumnFamilyNumber::ColumnFamily_Default, nullptr, nullptr);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(hasCalled);

  if (cfg->binlogUsingDefaultCF == true) {
    EXPECT_EQ(totalFilter, 3000 * 2 - kvCount);
  } else {
    EXPECT_EQ(totalFilter, 3000 - kvCount);
  }
  EXPECT_EQ(totalExpired, kvCount2);

  testMaxBinlogId(kvstore);
}

TEST(RocksKVStore, CompactionWithNoexpire) {
  auto cfg = genParams();
  cfg->noexpire = true;
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
  auto kvstore = std::make_unique<RocksKVStore>("0",
                                                cfg,
                                                blockCache,
                                                true,
                                                KVStore::StoreMode::READ_WRITE,
                                                RocksKVStore::TxnMode::TXN_PES);

  SyncPoint::GetInstance()->EnableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  uint64_t totalFilter = 0;
  SyncPoint::GetInstance()->SetCallBack(
    "InspectKvTtlFilterCount", [&](void* arg) mutable {
      uint64_t* tmp = reinterpret_cast<uint64_t*>(arg);
      totalFilter = *tmp;
    });

  uint64_t totalExpired = 0;
  bool hasCalled = false;
  SyncPoint::GetInstance()->SetCallBack(
    "InspectKvTtlExpiredCount", [&](void* arg) mutable {
      hasCalled = true;
      uint64_t* tmp = reinterpret_cast<uint64_t*>(arg);
      totalExpired = *tmp;
    });

  uint32_t waitSec = 10;
  // if we want to check the totalFilter, all data should be different
  genData(kvstore.get(), 1000, 0, true);
  size_t kvCount = genData(kvstore.get(), 1000, msSinceEpoch(), true);
  size_t kvCount2 =
    genData(kvstore.get(), 1000, msSinceEpoch() + waitSec * 1000, true);


  std::this_thread::sleep_for(std::chrono::seconds(1));
  // compact data in the default column family
  auto status = kvstore->compactRange(
    ColumnFamilyNumber::ColumnFamily_Default, nullptr, nullptr);
  EXPECT_TRUE(status.ok());
  EXPECT_FALSE(hasCalled);

  EXPECT_EQ(totalFilter, 0);
  EXPECT_EQ(totalExpired, 0);

  std::this_thread::sleep_for(std::chrono::seconds(waitSec));

  status = kvstore->compactRange(
    ColumnFamilyNumber::ColumnFamily_Default, nullptr, nullptr);
  EXPECT_TRUE(status.ok());
  EXPECT_FALSE(hasCalled);

  EXPECT_EQ(totalFilter, 0);
  EXPECT_EQ(totalExpired, 0);

  testMaxBinlogId(kvstore);

  // set noexpire = false, compaction filter will be call
  cfg->noexpire = false;
  status = kvstore->compactRange(
    ColumnFamilyNumber::ColumnFamily_Default, nullptr, nullptr);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(hasCalled);

  if (cfg->binlogUsingDefaultCF) {
    EXPECT_EQ(totalFilter, 3000 * 2);
  } else {
    EXPECT_EQ(totalFilter, 3000);
  }
  EXPECT_EQ(totalExpired, kvCount + kvCount2);

  testMaxBinlogId(kvstore);
}

}  // namespace tendisplus
