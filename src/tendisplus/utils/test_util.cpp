// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.
#include <fstream>
#include <utility>
#include <memory>
#include <vector>
#include <limits>
#include <algorithm>
#include <random>

#include "gtest/gtest.h"
#include "glog/logging.h"
#include "tendisplus/utils/test_util.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/portable.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/storage/rocks/rocks_kvstore.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/utils/string.h"

namespace tendisplus {

Expected<std::string> key2Aof(Session* sess, const std::string& key);

std::shared_ptr<ServerParams> makeServerParam(uint32_t port,
                                              uint32_t storeCnt,
                                              const std::string& dir,
                                              bool general_log) {
  std::stringstream ss_tmp_conf_file;
  ss_tmp_conf_file << getpid() << "_";
  ss_tmp_conf_file << port << "_test.cfg";
  std::string tmp_conf_file = ss_tmp_conf_file.str();

  const auto guard =
    MakeGuard([tmp_conf_file] { remove(tmp_conf_file.c_str()); });

  std::ofstream myfile;
  myfile.open(tmp_conf_file);
  myfile << "bind 127.0.0.1\n";
  myfile << "port " << port << "\n";
  myfile << "loglevel debug\n";
  if (dir != "") {
    myfile << "logdir ./" << dir << "/log\n";
    myfile << "dir ./" << dir << "/db\n";
    myfile << "dumpdir ./" << dir << "/dump\n";
    myfile << "pidfile ./" << dir << "/tendisplus.pid\n";
  } else {
    myfile << "logdir ./log\n";
    myfile << "dir ./db\n";
    myfile << "dumpdir ./dump\n";
    myfile << "pidfile ./tendisplus.pid\n";
  }
  myfile << "storage rocks\n";
  myfile << "rocks.blockcachemb 4096\n";
  myfile << "rocks.write_buffer_size 4096000\n";
  if (general_log) {
    myfile << "generallog on\n";
  }
  myfile << "slowlog-flush-interval 1\n";
  // TODO(vinchen): should it always be on?
  myfile << "checkkeytypeforsetcmd on\n";
  if (storeCnt != 0) {
    myfile << "kvStoreCount " << storeCnt << "\n";
  }
  myfile << "maxBinlogKeepNum 1000000\n";
  myfile << "slaveBinlogKeepNum 1000000\n";
#ifdef _WIN32
  myfile << "rocks.compress_type none\n";
#endif
  myfile.close();

  auto cfg = std::make_shared<ServerParams>();
  auto s = cfg->parseFile(tmp_conf_file);
  LOG(INFO) << "params:" << endl << cfg->showAll();
  EXPECT_EQ(s.ok(), true);

  return cfg;
}

bool setupEnv() {
  std::error_code ec;

  filesystem::remove_all("./log", ec);
  // EXPECT_EQ(ec.value(), 0);
  EXPECT_TRUE(ec.value() == 0 || ec.value() == 2);

  filesystem::remove_all("./db", ec);
  // EXPECT_EQ(ec.value(), 0);
  EXPECT_TRUE(ec.value() == 0 || ec.value() == 2);

  EXPECT_TRUE(filesystem::create_directory("./db"));
  EXPECT_TRUE(filesystem::create_directory("./log"));

  return true;
}

void destroyEnv() {
  std::error_code ec;
  filesystem::remove_all("./log", ec);
  filesystem::remove_all("./db", ec);
}

bool setupEnv(const std::string& v) {
  std::error_code ec;
  std::stringstream ss;
  ss << "./" << v << "/log";
  filesystem::remove_all(ss.str(), ec);
  EXPECT_TRUE(ec.value() == 0 || ec.value() == 2);
  EXPECT_TRUE(filesystem::create_directories(ss.str()));

  ss.str("");
  ss << "./" << v << "/db";
  filesystem::remove_all(ss.str(), ec);
  EXPECT_TRUE(ec.value() == 0 || ec.value() == 2);
  EXPECT_TRUE(filesystem::create_directories(ss.str()));

  ss.str("");
  ss << "./" << v << "/dump";
  filesystem::remove_all(ss.str(), ec);
  EXPECT_TRUE(ec.value() == 0 || ec.value() == 2);
  EXPECT_TRUE(filesystem::create_directories(ss.str()));

  return true;
}

void destroyEnv(const std::string& v) {
  std::error_code ec;
  std::stringstream ss;
  ss << "./" << v << "/log";
  filesystem::remove_all(ss.str(), ec);
  // EXPECT_TRUE(ec.value() == 0 || ec.value() == 2);

  ss.str("");
  ss << "./" << v << "/db";
  filesystem::remove_all(ss.str(), ec);
  // EXPECT_TRUE(ec.value() == 0 || ec.value() == 2);

  ss.str("");
  ss << "./" << v << "/dump";
  filesystem::remove_all(ss.str(), ec);
  // EXPECT_TRUE(ec.value() == 0 || ec.value() == 2);
}

bool setupReplEnv() {
  auto vec = {"master", "slave"};

  for (auto v : vec) {
    setupEnv(v);
  }
  return true;
}

void destroyReplEnv() {
  auto vec = {"master", "slave"};

  for (auto v : vec) {
    destroyEnv(v);
  }
}

std::string getBulkValue(const std::string& reply, uint32_t index) {
  INVARIANT(index == 0);
  auto ptr = reply.c_str();
  std::string buf;
  buf.reserve(128);

  size_t i = 0;
  size_t size = 0;

  if (ptr[i] == '*') {
    while (ptr[++i] != '\r') {
    }

    i += 2;  // skip the '\n'
  }

  switch (ptr[i]) {
    case '$':
      while (ptr[++i] != '\r') {
        buf.append(1, ptr[i]);
      }
      size = std::stol(buf);
      i += 2;  // skip the '\n'
      break;
    default:
      INVARIANT(0);
      break;
  }
  buf.clear();
  INVARIANT(reply.size() > i + size);
  buf.insert(buf.end(), reply.begin() + i, reply.begin() + i + size);
  INVARIANT(ptr[i + size] == '\r');

  return buf;
}

std::shared_ptr<ServerEntry> makeServerEntry(
  const std::shared_ptr<ServerParams>& cfg) {
  auto master = std::make_shared<ServerEntry>(cfg);
  auto s = master->startup(cfg);
  INVARIANT(s.ok());

  return master;
}

std::shared_ptr<ServerEntry> makeServerEntryOld(
  const std::shared_ptr<ServerParams>& cfg) {
  auto block_cache =
    rocksdb::NewLRUCache(cfg->rocksBlockcacheMB * 1024 * 1024LL, 4);
  auto server = std::make_shared<ServerEntry>(cfg);

  uint32_t kvStoreCount = cfg->kvStoreCount;
  uint32_t chunkSize = cfg->chunkSize;

  // catalog init
  auto catalog = std::make_unique<Catalog>(
    std::move(std::unique_ptr<KVStore>(
      new RocksKVStore(CATALOG_NAME, cfg, nullptr, false))),
    kvStoreCount,
    chunkSize,
    cfg->binlogUsingDefaultCF);
  server->installCatalog(std::move(catalog));

  std::vector<PStore> tmpStores;
  for (size_t dbId = 0; dbId < kvStoreCount; ++dbId) {
    KVStore::StoreMode mode = KVStore::StoreMode::READ_WRITE;

    auto meta = server->getCatalog()->getStoreMainMeta(dbId);
    if (meta.ok()) {
      mode = meta.value()->storeMode;
    } else if (meta.status().code() == ErrorCodes::ERR_NOTFOUND) {
      auto pMeta = std::unique_ptr<StoreMainMeta>(
        new StoreMainMeta(dbId, KVStore::StoreMode::READ_WRITE));
      Status s = server->getCatalog()->setStoreMainMeta(*pMeta);
      if (!s.ok()) {
        LOG(FATAL) << "catalog setStoreMainMeta error:" << s.toString();
        return nullptr;
      }
    } else {
      LOG(FATAL) << "catalog getStoreMainMeta error:"
                 << meta.status().toString();
      return nullptr;
    }

    tmpStores.emplace_back(std::unique_ptr<KVStore>(
      new RocksKVStore(std::to_string(dbId), cfg, block_cache, true, mode)));
  }
  server->installStoresInLock(tmpStores);
  auto seg_mgr =
    std::unique_ptr<SegmentMgr>(new SegmentMgrFnvHash64(tmpStores, chunkSize));
  server->installSegMgrInLock(std::move(seg_mgr));

  auto tmpPessimisticMgr = std::make_unique<PessimisticMgr>(kvStoreCount);
  server->installPessimisticMgrInLock(std::move(tmpPessimisticMgr));

  auto tmpMGLockMgr = std::make_unique<mgl::MGLockMgr>();
  server->installMGLockMgrInLock(std::move(tmpMGLockMgr));

  // server->initSlowlog(cfg->slowlogPath);
  server->getSlowlogStat().initSlowlogFile(cfg->slowlogPath);

  return server;
}

std::shared_ptr<NetSession> makeSession(std::shared_ptr<ServerEntry> server,
                                        std::shared_ptr<asio::io_context> ctx) {
  asio::ip::tcp::socket socket(*ctx);
  return std::make_shared<NetSession>(
    server, std::move(socket), 0, false, nullptr, nullptr);
}

std::string randomKey(size_t maxlen) {
  std::string key;

  int len = std::rand() % maxlen + 1;
  for (int i = 0; i < len; ++i) {
    key.push_back(std::rand() % ('z' - 'a') + (std::rand() % 2 ? 'A' : 'a'));
  }

  return key;
}

bool isExpired(PStore store, const RecordKey& key, const RecordValue& value) {
  // NOTE(takenliu): use real time
  uint64_t currentTs = msSinceEpoch();
  // uint64_t currentTs = store->getCurrentTime();
  uint64_t ttl;
  if (key.getRecordType() == RecordType::RT_DATA_META &&
      value.getRecordType() == RecordType::RT_KV) {
    ttl = value.getTtl();
    if (ttl > 0 && ttl < currentTs) {
      // Expired
      return true;
    }
  }
  return false;
}

void compareData(const std::shared_ptr<ServerEntry>& master,
                 const std::shared_ptr<ServerEntry>& slave,
                 bool compare_binlog) {
  INVARIANT(master->getKVStoreCount() == slave->getKVStoreCount());
  bool aofMode =
    master->getParams()->aofEnabled && slave->getParams()->aofEnabled;

  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext);
  NoSchedNetSession sess1(
    master, std::move(socket), 1, false, nullptr, nullptr);

  asio::io_context ioContext2;
  asio::ip::tcp::socket socket2(ioContext2);
  NoSchedNetSession sess2(
    slave, std::move(socket2), 1, false, nullptr, nullptr);

  for (size_t i = 0; i < master->getKVStoreCount(); i++) {
    uint64_t count1 = 0;
    uint64_t count2 = 0;
    auto kvstore1 = master->getStores()[i];
    auto kvstore2 = slave->getStores()[i];

    auto ptxn2 = kvstore2->createTransaction(nullptr);
    EXPECT_TRUE(ptxn2.ok());
    std::unique_ptr<Transaction> txn2 = std::move(ptxn2.value());

    auto ptxn1 = kvstore1->createTransaction(nullptr);
    EXPECT_TRUE(ptxn1.ok());
    std::unique_ptr<Transaction> txn1 = std::move(ptxn1.value());
    auto cursor1 = txn1->createAllDataCursor();
    // check the data
    while (true) {
      Expected<Record> exptRcd1 = cursor1->next();
      if (exptRcd1.status().code() == ErrorCodes::ERR_EXHAUST) {
        break;
      }
      INVARIANT(exptRcd1.ok());

      auto masterKey = exptRcd1.value().getRecordKey();
      if (isExpired(kvstore1, masterKey, exptRcd1.value().getRecordValue())) {
        continue;
      }

      count1++;

      auto type = masterKey.getRecordType();
      /* AOF PSYNC only compare primary keys*/
      if (aofMode && type != RecordType::RT_DATA_META) {
        continue;
      }

      auto exptRcdv2 = kvstore2->getKV(masterKey, txn2.get());
      EXPECT_TRUE(exptRcdv2.ok());
      if (!exptRcdv2.ok()) {
        LOG(INFO) << "key:" << masterKey.getPrimaryKey()
                  << ",type:" << static_cast<int>(type) << " not found!";
        INVARIANT(0);
      }
      EXPECT_TRUE(exptRcdv2.ok());

      if (aofMode) {
        auto keystr1 = key2Aof(&sess1, masterKey.getPrimaryKey());
        EXPECT_TRUE(keystr1.ok());

        auto keystr2 = key2Aof(&sess2, masterKey.getPrimaryKey());
        EXPECT_TRUE(keystr2.ok());

        EXPECT_EQ(keystr1.value(), keystr2.value());
      } else {
        EXPECT_EQ(exptRcd1.value().getRecordValue(), exptRcdv2.value());
      }
    }
    int count1_data = count1;

    if (compare_binlog && !aofMode) {
      // check the binlog
      auto cursor1_binlog = txn1->createBinlogCursor();
      while (true) {
        Expected<Record> exptRcd1 = cursor1_binlog->next();
        if (exptRcd1.status().code() == ErrorCodes::ERR_EXHAUST) {
          break;
        }
        INVARIANT(exptRcd1.ok());
        count1++;

        auto exptRcdv2 =
          kvstore2->getKV(exptRcd1.value().getRecordKey(), txn2.get());
        EXPECT_TRUE(exptRcdv2.ok());
        EXPECT_EQ(exptRcd1.value().getRecordValue(), exptRcdv2.value());
      }
    }

    auto cursor2 = txn2->createAllDataCursor();
    while (true) {
      Expected<Record> exptRcd2 = cursor2->next();
      if (exptRcd2.status().code() == ErrorCodes::ERR_EXHAUST) {
        break;
      }
      INVARIANT(exptRcd2.ok());

      auto slaveKey = exptRcd2.value().getRecordKey();
      if (isExpired(kvstore2, slaveKey, exptRcd2.value().getRecordValue())) {
        continue;
      }
      count2++;

      if (aofMode && slaveKey.getRecordType() != RecordType::RT_DATA_META) {
        continue;
      }

      // check the data
      auto exptRcdv1 = kvstore1->getKV(slaveKey, txn1.get());
      EXPECT_TRUE(exptRcdv1.ok());
      if (!exptRcdv1.ok()) {
        LOG(INFO) << exptRcd2.value().toString()
                  << " error:" << exptRcdv1.status().toString();
        continue;
      }
      if (aofMode) {
        auto keystr1 = key2Aof(&sess1, slaveKey.getPrimaryKey());
        EXPECT_TRUE(keystr1.ok());

        auto keystr2 = key2Aof(&sess2, slaveKey.getPrimaryKey());
        EXPECT_TRUE(keystr2.ok());

        EXPECT_EQ(keystr1.value(), keystr2.value());
      } else {
        EXPECT_EQ(exptRcd2.value().getRecordValue(), exptRcdv1.value());
      }
    }

    int count2_data = count2;

    if (compare_binlog && !aofMode) {
      auto cursor2_binlog = txn2->createBinlogCursor();
      while (true) {
        Expected<Record> exptRcd2 = cursor2_binlog->next();
        if (exptRcd2.status().code() == ErrorCodes::ERR_EXHAUST) {
          break;
        }
        INVARIANT(exptRcd2.ok());
        count2++;
      }
    }
    EXPECT_EQ(count1_data, count2_data);
    EXPECT_EQ(count1, count2);
    LOG(INFO) << "compare data: store " << i << " record count " << count1;
  }
}

KeysWritten WorkLoad::writeWork(RecordType type,
                                uint32_t count,
                                uint32_t maxlen,
                                bool sharename,
                                const char* key_suffix) {
  uint32_t total = 0;
  KeysWritten keys;

  for (uint32_t i = 0; i < count; ++i) {
    std::string key = randomKey(32) + "_" + std::to_string(i);
    key.push_back('_');
    key.push_back(static_cast<char>(rt2Char(type)));
    if (key_suffix != NULL) {
      key += '_' + key_suffix;
    }

    if (type == RecordType::RT_KV) {
      _session->setArgs({"set", key, std::to_string(i)});
      auto expect = Command::runSessionCmd(_session.get());
      EXPECT_TRUE(expect.status().ok());
      if (expect.status().ok()) {
        EXPECT_EQ(expect.value(), Command::fmtOK());
      }
      total++;
    } else {
      uint32_t len = static_cast<uint32_t>(std::rand() % maxlen) + 1;

      for (uint32_t j = 0; j < len; ++j) {
        if (type == RecordType::RT_LIST_META) {
          _session->setArgs({"lpush", key, std::to_string(j)});
          auto expect = Command::runSessionCmd(_session.get());
          EXPECT_TRUE(expect.status().ok());
          if (expect.status().ok()) {
            EXPECT_EQ(expect.value(),
                      Command::fmtLongLong(static_cast<uint64_t>(j + 1)));
          }
        } else {
          if (type == RecordType::RT_HASH_META) {
            _session->setArgs(
              {"hset", key, "subkey" + std::to_string(j), std::to_string(j)});
          } else if (type == RecordType::RT_SET_META) {
            _session->setArgs({"sadd", key, std::to_string(j)});
          } else if (type == RecordType::RT_ZSET_META) {
            _session->setArgs(
              {"zadd", key, std::to_string(std::rand()), std::to_string(j)});
          } else {
            INVARIANT(0);
          }

          auto expect = Command::runSessionCmd(_session.get());
          EXPECT_TRUE(expect.status().ok());
          if (expect.status().ok()) {
            EXPECT_EQ(expect.value(), Command::fmtOne());
          }
        }
      }

      if (len > 0) {
        total++;
      }
    }

    keys.emplace(key);
  }

  LOG(WARNING) << total << " key written into";

  return keys;
}

void WorkLoad::expireKeys(const AllKeys& all_keys, uint64_t ttl) {
  for (size_t i = 0; i < all_keys.size(); ++i) {
    for (auto& key : all_keys[i]) {
      if (i > 0) {
        bool duplicate = false;
        for (size_t j = i; j > 0; --j) {
          auto pos = all_keys[j - 1].find(key);
          if (pos != all_keys[j - 1].end()) {
            duplicate = true;
            break;
          }
        }

        if (duplicate) {
          continue;
        }
      }

      _session->setArgs({"expire", key, std::to_string(ttl)});
      auto expect = Command::runSessionCmd(_session.get());
      EXPECT_TRUE(expect.ok());
      EXPECT_EQ(expect.value(), Command::fmtOne());
    }
  }

  return;
}

void WorkLoad::slaveof(const std::string& ip, uint32_t port) {
  _session->setArgs({"slaveof", ip, std::to_string(port)});
  auto expect = Command::runSessionCmd(_session.get());
  EXPECT_TRUE(expect.ok());
}

void WorkLoad::flush() {
  _session->setArgs({"flushalldisk"});
  auto expect = Command::runSessionCmd(_session.get());
  EXPECT_TRUE(expect.ok());
}

Expected<uint64_t> WorkLoad::getIntResult(
  const std::vector<std::string>& args) {
  _session->setArgs(args);
  auto expect = Command::runSessionCmd(_session.get());
  if (!expect.ok()) {
    return expect.status();
  }

  return Command::getInt64FromFmtLongLong(expect.value());
}

void WorkLoad::addClusterSession(const string& addr, TestSession sess) {
  LOG(INFO) << "addClusterSession:" << addr;
  _clusterSessions[addr] = sess;
}

// TODO(takenliu): change other api to call runCommand.
// support MOVED
Expected<string> WorkLoad::runCommand(const std::vector<std::string>& args) {
  TestSession sess = _session;
  int depth = 0;
  while (true) {
    sess->setArgs(args);
    auto expect = Command::runSessionCmd(sess.get());
    if (expect.ok()) {
      return expect.value();
    }
    if (expect.status().code() != ErrorCodes::ERR_MOVED) {
      LOG(ERROR) << expect.status().toString();
      return expect.value();
    }
    LOG(INFO) << "moved depth:" << depth << " " << expect.status().toString();
    auto infos = stringSplit(expect.status().toString(), " ");
    if (infos.size() == 3) {
      string addr = infos[2].replace(infos[2].find("\r\n"), 2, "");
      auto iter = _clusterSessions.find(addr);
      if (iter != _clusterSessions.end()) {
        sess = iter->second;
      } else {
        LOG(ERROR) << "MOVED and has no session for:" << addr;
        return {ErrorCodes::ERR_UNKNOWN, "MOVED and has no session"};
      }
    } else {
      LOG(ERROR) << "MOVED and info not right:" << expect.status().toString();
      return {ErrorCodes::ERR_UNKNOWN, "MOVED and info not right"};
    }
  }
}

std::string WorkLoad::getStringResult(const std::vector<std::string>& args) {
  auto expect = runCommand(args);
  if (!expect.ok()) {
    LOG(ERROR) << expect.status().toString();
  }
  EXPECT_TRUE(expect.ok());

  return expect.value();
}

void WorkLoad::delKeys(const KeysWritten& keys) {
  for (auto& key : keys) {
    _session->setArgs({"del", key});
    auto expect = Command::runSessionCmd(_session.get());
    EXPECT_TRUE(expect.ok());
  }
}

void WorkLoad::clusterMeet(const std::string& ip,
                           uint32_t port,
                           uint32_t cport) {
  if (cport != 0) {
    _session->setArgs(
      {"cluster", "meet", ip, std::to_string(port), std::to_string(cport)});
  } else {
    _session->setArgs({"cluster", "meet", ip, std::to_string(port)});
  }

  auto expect = Command::runSessionCmd(_session.get());
  EXPECT_TRUE(expect.ok());
}

void WorkLoad::clusterNodes() {
  _session->setArgs({"cluster", "nodes"});

  auto expect = Command::runSessionCmd(_session.get());
  EXPECT_TRUE(expect.ok());
}

void WorkLoad::addSlots(const std::string& slotsBuff) {
  _session->setArgs({"cluster", "addslots", slotsBuff});

  auto expect = Command::runSessionCmd(_session.get());
  EXPECT_TRUE(expect.ok());
}

void WorkLoad::replicate(const std::string& nodeName) {
  _session->setArgs({"cluster", "replicate", nodeName});

  auto expect = Command::runSessionCmd(_session.get());
  EXPECT_TRUE(expect.ok());
}

void WorkLoad::lockDb(mstime_t locktime) {
  _session->setArgs({"tendisadmin", "lockdb", std::to_string(locktime)});
  auto expect = Command::runSessionCmd(_session.get());
  EXPECT_TRUE(expect.ok());
}

bool WorkLoad::manualFailover() {
  _session->setArgs({"cluster", "failover"});
  auto expect = Command::runSessionCmd(_session.get());
  if (expect.ok()) {
    return true;
  }
  return false;
}

void WorkLoad::stopMigrate(const std::string& taskid) {
  _session->setArgs({"cluster", "setslot", "stop", taskid});
  auto expect = Command::runSessionCmd(_session.get());
  EXPECT_TRUE(expect.ok());
}

void WorkLoad::stopAllMigTasks() {
  _session->setArgs({"cluster", "setslot", "stopall"});
  auto expect = Command::runSessionCmd(_session.get());
  EXPECT_TRUE(expect.ok());
}

void WorkLoad::restartAllMigTasks() {
  _session->setArgs({"cluster", "setslot", "restartall"});
  auto expect = Command::runSessionCmd(_session.get());
  EXPECT_TRUE(expect.ok());
}

int genRand() {
  int grand = 0;
  uint32_t ms = (uint32_t)nsSinceEpoch();
  grand = rand_r(reinterpret_cast<unsigned int*>(&ms));
  return grand;
}

std::string randomIp() {
  return "192.168.1.1";
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

std::bitset<CLUSTER_SLOTS> genBitMap() {
  std::bitset<CLUSTER_SLOTS> bitmap;
  uint16_t start = genRand() % CLUSTER_SLOTS;
  uint16_t length = genRand() % (CLUSTER_SLOTS - start);
  for (size_t j = start; j < start + length; j++) {
    bitmap.set(j);
  }
  return bitmap;
}

void testList(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  sess.setArgs({"lindex", "testList", std::to_string(0)});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtNull());

  for (uint32_t i = 0; i < 10000; i++) {
    sess.setArgs({"lpush", "testList", std::to_string(2 * i)});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(i + 1));

    sess.setArgs({"lindex", "testList", std::to_string(i)});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtBulk(std::to_string(0)));

    sess.setArgs({"llen", "testList"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(i + 1));
  }

  // case from redis.io, lrange
  std::stringstream ss;
  sess.setArgs({"rpush", "lrangekey", "one"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(1));
  sess.setArgs({"rpush", "lrangekey", "two"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(2));
  sess.setArgs({"rpush", "lrangekey", "three"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(3));
  sess.setArgs({"lrange", "lrangekey", "0", "0"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  Command::fmtMultiBulkLen(ss, 1);
  Command::fmtBulk(ss, "one");
  EXPECT_EQ(expect.value(), ss.str());
  ss.str("");
  sess.setArgs({"lrange", "lrangekey", "-3", "2"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  Command::fmtMultiBulkLen(ss, 3);
  Command::fmtBulk(ss, "one");
  Command::fmtBulk(ss, "two");
  Command::fmtBulk(ss, "three");
  EXPECT_EQ(expect.value(), ss.str());
  ss.str("");
  sess.setArgs({"lrange", "lrangekey", "-100", "100"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  Command::fmtMultiBulkLen(ss, 3);
  Command::fmtBulk(ss, "one");
  Command::fmtBulk(ss, "two");
  Command::fmtBulk(ss, "three");
  EXPECT_EQ(expect.value(), ss.str());
  ss.str("");
  sess.setArgs({"lrange", "lrangekey", "5", "10"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtZeroBulkLen());

  // case from redis.io, ltrim
  sess.setArgs({"rpush", "ltrimkey", "one"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(1));
  sess.setArgs({"rpush", "ltrimkey", "two"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(2));
  sess.setArgs({"rpush", "ltrimkey", "three"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(3));
  sess.setArgs({"ltrim", "ltrimkey", "1", "-1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());
  ss.str("");
  sess.setArgs({"lrange", "ltrimkey", "0", "-1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  Command::fmtMultiBulkLen(ss, 2);
  Command::fmtBulk(ss, "two");
  Command::fmtBulk(ss, "three");
  EXPECT_EQ(expect.value(), ss.str());
  sess.setArgs({"llen", "ltrimkey"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(2));
  sess.setArgs({"ltrim", "ltrimkey", "2", "2"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());
  sess.setArgs({"exists", "ltrimkey"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(0));

  // case from redis.io rpoplpush
  sess.setArgs({"rpush", "rpoplpushkey", "one"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(1));
  sess.setArgs({"rpush", "rpoplpushkey", "two"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(2));
  sess.setArgs({"rpush", "rpoplpushkey", "three"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(3));
  ss.str("");
  sess.setArgs({"rpoplpush", "rpoplpushkey", "rpoplpushkey2"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtBulk("three"));
  ss.str("");
  sess.setArgs({"lrange", "rpoplpushkey", "0", "-1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  Command::fmtMultiBulkLen(ss, 2);
  Command::fmtBulk(ss, "one");
  Command::fmtBulk(ss, "two");
  EXPECT_EQ(expect.value(), ss.str());
  ss.str("");
  sess.setArgs({"lrange", "rpoplpushkey2", "0", "-1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  Command::fmtMultiBulkLen(ss, 1);
  Command::fmtBulk(ss, "three");
  EXPECT_EQ(expect.value(), ss.str());

  sess.setArgs({"rpush", "mylist", "a", "hello", "c"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(3));

  ss.str("");
  sess.setArgs({"rpoplpush", "mylist", "mylist"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtBulk("c"));
  ss.str("");
  sess.setArgs({"lrange", "mylist", "0", "-1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  Command::fmtMultiBulkLen(ss, 3);
  Command::fmtBulk(ss, "c");
  Command::fmtBulk(ss, "a");
  Command::fmtBulk(ss, "hello");
  EXPECT_EQ(expect.value(), ss.str());

  // wrong type
  sess.setArgs({"set", "mylist3", "a"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"rpoplpush", "mylist", "mylist3"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(!expect.ok());
  ss.str("");
  sess.setArgs({"lrange", "mylist", "0", "-1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  Command::fmtMultiBulkLen(ss, 3);
  Command::fmtBulk(ss, "c");
  Command::fmtBulk(ss, "a");
  Command::fmtBulk(ss, "hello");
  EXPECT_EQ(expect.value(), ss.str());
}

void testHash2(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  int sum = 0;
  for (int i = 0; i < 1000; ++i) {
    int cur = rand() % 100 - 50;  // NOLINT(runtime/threadsafe_fn)
    sum += cur;
    sess.setArgs({"hincrby", "testkey", "testsubkey", std::to_string(cur)});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(sum));

    sess.setArgs({"hget", "testkey", "testsubkey"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtBulk(std::to_string(sum)));

    sess.setArgs({"hlen", "testkey"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(1));
  }

  int64_t delta = 0;
  if (sum > 0) {
    delta = std::numeric_limits<int64_t>::max();
  } else if (sum == 0) {
    int cur = 1;
    sum += cur;
    sess.setArgs({"hincrby", "testkey", "testsubkey", std::to_string(cur)});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(sum));
    delta = std::numeric_limits<int64_t>::max();
  } else {
    delta = std::numeric_limits<int64_t>::min();
  }
  sess.setArgs({"hincrby", "testkey", "testsubkey", std::to_string(delta)});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_FALSE(expect.ok());
  EXPECT_EQ(expect.status().code(), ErrorCodes::ERR_OVERFLOW);

  const long double pi = 3.14159265358979323846L;
  long double floatSum = sum + pi;
  sess.setArgs(
    {"hincrbyfloat", "testkey", "testsubkey", tendisplus::ldtos(pi, true)});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  std::string result = tendisplus::ldtos(floatSum, true);
  EXPECT_EQ(Command::fmtBulk(result), expect.value());

  // hmcas key, cmp, vsn, [subkey1, op1, val1]
  sess.setArgs({"hmcas", "hmcaskey1", "1", "123", "subkey1", "0", "subval1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(Command::fmtOne(), expect.value());
  sess.setArgs({"hmcas", "hmcaskey1", "1", "124", "subkey1", "0", "subval2"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(Command::fmtOne(), expect.value());
  sess.setArgs({"hget", "hmcaskey1", "subkey1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtBulk("subval2"));
  sess.setArgs({"hlen", "hmcaskey1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(1));
  sess.setArgs({"hmcas", "hmcaskey1", "0", "999", "subkey1", "0", "subval2"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(Command::fmtOne(), expect.value());

  // parse int failed
  sess.setArgs({"hmcas", "hmcaskey1", "1", "1000", "subkey1", "1", "10"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_FALSE(expect.ok());

  sess.setArgs({"hmcas", "hmcaskey1", "1", "1000", "subkey1", "0", "-100"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(Command::fmtOne(), expect.value());

  sess.setArgs({"hmcas", "hmcaskey1", "1", "1001", "subkey1", "1", "100"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(Command::fmtOne(), expect.value());

  sess.setArgs({"hget", "hmcaskey1", "subkey1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtBulk("0"));
}

void testHash1(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  {
    sess.setArgs({"del", "testHash1"});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok()) << expect.status().toString();
  }

  for (uint32_t i = 0; i < 10000; i++) {
    sess.setArgs({"hset", "testHash1", std::to_string(i), std::to_string(i)});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok()) << expect.status().toString();
  }
  for (uint32_t i = 0; i < 10000; i++) {
    sess.setArgs({"hget", "testHash1", std::to_string(i)});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtBulk(std::to_string(i)));

    sess.setArgs({"hexists", "testHash1", std::to_string(i)});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOne());
  }
  std::vector<std::string> args;
  args.push_back("hdel");
  args.push_back("testHash1");
  for (uint32_t i = 0; i < 10000; i++) {
    args.push_back(std::to_string(2 * i));
  }
  sess.setArgs(args);
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(5000));

  sess.setArgs({"hlen", "testHash1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(5000));

  for (uint32_t i = 0; i < 5000; i++) {
    sess.setArgs({"hget", "testHash1", std::to_string(i)});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    if (i % 2 == 1) {
      EXPECT_EQ(expect.value(), Command::fmtBulk(std::to_string(i)));
    } else {
      EXPECT_EQ(expect.value(), Command::fmtNull());
    }
    sess.setArgs({"hexists", "testHash1", std::to_string(i)});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    if (i % 2 == 1) {
      EXPECT_EQ(expect.value(), Command::fmtOne());
    } else {
      EXPECT_EQ(expect.value(), Command::fmtZero());
    }
  }

  sess.setArgs({"hgetall", "testHash1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  std::stringstream ss;
  Command::fmtMultiBulkLen(ss, 10000);
  std::vector<std::string> vals;
  for (uint32_t i = 0; i < 5000; ++i) {
    vals.push_back(std::to_string(2 * i + 1));
    vals.push_back(std::to_string(2 * i + 1));
  }
  std::sort(vals.begin(), vals.end());
  for (const auto& v : vals) {
    Command::fmtBulk(ss, v);
  }
  EXPECT_EQ(ss.str(), expect.value());

  // hsetnx related
  for (uint32_t i = 0; i < 10000; i++) {
    sess.setArgs({"hsetnx", "testHash1", std::to_string(i), std::to_string(0)});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    if (i % 2 == 0) {
      EXPECT_EQ(expect.value(), Command::fmtOne());
    } else {
      EXPECT_EQ(expect.value(), Command::fmtZero());
    }
  }
  for (uint32_t i = 0; i < 10000; i++) {
    sess.setArgs({"hget", "testHash1", std::to_string(i)});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    if (i % 2 == 0) {
      EXPECT_EQ(expect.value(), Command::fmtBulk(std::to_string(0)));
    } else {
      EXPECT_EQ(expect.value(), Command::fmtBulk(std::to_string(i)));
    }
  }

  for (uint32_t i = 0; i < 1000; i++) {
    sess.setArgs({"hmset",
                  "hmsetkey",
                  std::to_string(i),
                  std::to_string(i),
                  std::to_string(i + 1),
                  std::to_string(i + 1)});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOK());
    sess.setArgs({"hlen", "hmsetkey"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(i + 2));
  }

  sess.setArgs({"hset",
                "hsetkey1",
                std::to_string(-1),
                std::to_string(-1),
                std::to_string(0),
                std::to_string(0)});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(2));

  for (uint32_t i = 0; i < 1000; i++) {
    sess.setArgs({"hset",
                  "hsetkey1",
                  std::to_string(i),
                  std::to_string(i * i),
                  std::to_string(i + 1),
                  std::to_string(i + 1)});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(1));
    sess.setArgs({"hlen", "hsetkey1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(i + 3));
  }
}

void testZset2(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  std::vector<uint64_t> keys;
  for (uint32_t i = 0; i < 100; i++) {
    keys.push_back(i);
  }
  std::random_shuffle(keys.begin(), keys.end());
  for (uint32_t i = 0; i < 100; i++) {
    sess.setArgs(
      {"zadd", "tzk2", std::to_string(keys[i]), std::to_string(keys[i])});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    sess.setArgs({"zcount", "tzk2", "-inf", "+inf"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(i + 1));
  }
  for (uint32_t i = 0; i < 100; i++) {
    for (uint32_t j = 0; j < 100; j++) {
      sess.setArgs({"zcount", "tzk2", std::to_string(i), std::to_string(j)});
      auto expect = Command::runSessionCmd(&sess);
      EXPECT_TRUE(expect.ok());
      if (i > j) {
        EXPECT_EQ(expect.value(), Command::fmtZero());
      } else {
        EXPECT_EQ(expect.value(), Command::fmtLongLong(j - i + 1))
          << i << ' ' << j;
      }
    }
  }
}

void testZset4(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  sess.setArgs({"zadd",
                "tzk4.1",
                "1",
                "one",
                "2",
                "two",
                "3",
                "three",
                "4",
                "four",
                "5",
                "five",
                "6",
                "six",
                "7",
                "seven"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(7));

  sess.setArgs({"zremrangebyrank", "tzk4.1", "0", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(2));
  sess.setArgs({"zrevrange", "tzk4.1", "0", "-1", "withscores"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok()) << expect.status().toString();
  std::stringstream ss;
  Command::fmtMultiBulkLen(ss, 10);
  Command::fmtBulk(ss, "seven");
  Command::fmtBulk(ss, "7");
  Command::fmtBulk(ss, "six");
  Command::fmtBulk(ss, "6");
  Command::fmtBulk(ss, "five");
  Command::fmtBulk(ss, "5");
  Command::fmtBulk(ss, "four");
  Command::fmtBulk(ss, "4");
  Command::fmtBulk(ss, "three");
  Command::fmtBulk(ss, "3");
  EXPECT_EQ(expect.value(), ss.str());
  ss.str("");

  sess.setArgs(
    {"zadd", "tzk4.2", "0", "aaaa", "0", "b", "0", "c", "0", "d", "0", "e"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  sess.setArgs({"zadd",
                "tzk4.2",
                "0",
                "foo",
                "0",
                "zap",
                "0",
                "zip",
                "0",
                "ALPHA",
                "0",
                "alpha"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"zremrangebylex", "tzk4.2", "[alpha", "[omega"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(6));

  ss.str("");
  sess.setArgs({"zrange", "tzk4.2", "0", "-1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  Command::fmtMultiBulkLen(ss, 4);
  Command::fmtBulk(ss, "ALPHA");
  Command::fmtBulk(ss, "aaaa");
  Command::fmtBulk(ss, "zap");
  Command::fmtBulk(ss, "zip");
  EXPECT_EQ(expect.value(), ss.str());

  // TODO(deyukong): test zremrangebyscore
}

void testZset3(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  // NOTE(deyukong): zlexcount has undefined behavior when scores
  // are not all the same.

  // zlexcount case from redis.io
  sess.setArgs(
    {"zadd", "tzk3", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(5));
  sess.setArgs({"zadd", "tzk3", "0", "f", "0", "g"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(2));

  sess.setArgs({"zlexcount", "tzk3", "[b", "[f"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(5));

  // zrange case from redis.io
  sess.setArgs({"zadd", "tzk3.1", "1", "one", "2", "two", "3", "three"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(3));

  sess.setArgs({"zrange", "tzk3.1", "0", "-1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok()) << expect.status().toString();
  std::stringstream ss;
  Command::fmtMultiBulkLen(ss, 3);
  Command::fmtBulk(ss, "one");
  Command::fmtBulk(ss, "two");
  Command::fmtBulk(ss, "three");
  EXPECT_EQ(expect.value(), ss.str());
  ss.str("");

  sess.setArgs({"zrange", "tzk3.1", "2", "3"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  Command::fmtMultiBulkLen(ss, 1);
  Command::fmtBulk(ss, "three");
  EXPECT_EQ(expect.value(), ss.str());
  ss.str("");

  sess.setArgs({"zrange", "tzk3.1", "-2", "-1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  Command::fmtMultiBulkLen(ss, 2);
  Command::fmtBulk(ss, "two");
  Command::fmtBulk(ss, "three");
  EXPECT_EQ(expect.value(), ss.str());

  // zrange random tests
  std::vector<uint64_t> keys;
  for (uint32_t i = 0; i < 100; i++) {
    keys.push_back(i);
  }
  std::random_shuffle(keys.begin(), keys.end());
  for (uint32_t i = 0; i < 100; i++) {
    sess.setArgs(
      {"zadd", "tzk3.2", std::to_string(keys[i]), std::to_string(keys[i])});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
  }
  for (uint32_t i = 0; i < 100; i++) {
    for (uint32_t j = 0; j < 100; j++) {
      sess.setArgs({"zrange", "tzk3.2", std::to_string(i), std::to_string(j)});
      expect = Command::runSessionCmd(&sess);
      EXPECT_TRUE(expect.ok());
      std::stringstream ss;
      if (i > j) {
        EXPECT_EQ(expect.value(), Command::fmtZeroBulkLen());
      } else {
        Command::fmtMultiBulkLen(ss, j - i + 1);
        for (uint32_t k = i; k <= j; ++k) {
          Command::fmtBulk(ss, std::to_string(k));
        }
        EXPECT_EQ(expect.value(), ss.str());
      }
    }
  }
  for (uint32_t i = 0; i < 100; i++) {
    for (uint32_t j = 0; j < 100; j++) {
      sess.setArgs(
        {"zrevrange", "tzk3.2", std::to_string(i), std::to_string(j)});
      expect = Command::runSessionCmd(&sess);
      EXPECT_TRUE(expect.ok());
      std::stringstream ss;
      if (i > j) {
        EXPECT_EQ(expect.value(), Command::fmtZeroBulkLen());
      } else {
        Command::fmtMultiBulkLen(ss, j - i + 1);
        for (uint32_t k = i; k <= j; ++k) {
          Command::fmtBulk(ss, std::to_string(100 - 1 - k));
        }
        EXPECT_EQ(expect.value(), ss.str());
      }
    }
  }

  // zrangebylex cases from redis.io
  ss.str("");
  sess.setArgs({"zadd",
                "tzk3.3",
                "0",
                "a",
                "0",
                "b",
                "0",
                "c",
                "0",
                "d",
                "0",
                "e",
                "0",
                "f",
                "0",
                "g"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  sess.setArgs({"zrangebylex", "tzk3.3", "-", "[c"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  Command::fmtMultiBulkLen(ss, 3);
  Command::fmtBulk(ss, "a");
  Command::fmtBulk(ss, "b");
  Command::fmtBulk(ss, "c");
  EXPECT_EQ(expect.value(), ss.str());

  ss.str("");
  sess.setArgs({"zrangebylex", "tzk3.3", "-", "(c"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  Command::fmtMultiBulkLen(ss, 2);
  Command::fmtBulk(ss, "a");
  Command::fmtBulk(ss, "b");
  EXPECT_EQ(expect.value(), ss.str());

  ss.str("");
  sess.setArgs({"zrangebylex", "tzk3.3", "[aaa", "(g"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  Command::fmtMultiBulkLen(ss, 5);
  Command::fmtBulk(ss, "b");
  Command::fmtBulk(ss, "c");
  Command::fmtBulk(ss, "d");
  Command::fmtBulk(ss, "e");
  Command::fmtBulk(ss, "f");
  EXPECT_EQ(expect.value(), ss.str());

  // zrangebyscore cases from redis.io
  ss.str("");
  sess.setArgs({"zadd",
                "tzk3.4",
                "1",
                "one",
                "2",
                "two",
                "3",
                "three",
                "4",
                "four",
                "5",
                "five",
                "6",
                "six",
                "7",
                "seven"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"zrangebyscore", "tzk3.4", "-inf", "+inf"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  Command::fmtMultiBulkLen(ss, 7);
  Command::fmtBulk(ss, "one");
  Command::fmtBulk(ss, "two");
  Command::fmtBulk(ss, "three");
  Command::fmtBulk(ss, "four");
  Command::fmtBulk(ss, "five");
  Command::fmtBulk(ss, "six");
  Command::fmtBulk(ss, "seven");
  EXPECT_EQ(expect.value(), ss.str());

  ss.str("");
  sess.setArgs({"zrangebyscore", "tzk3.4", "1", "2"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  Command::fmtMultiBulkLen(ss, 2);
  Command::fmtBulk(ss, "one");
  Command::fmtBulk(ss, "two");
  EXPECT_EQ(expect.value(), ss.str());

  ss.str("");
  sess.setArgs({"zrangebyscore", "tzk3.4", "(1", "2"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  Command::fmtMultiBulkLen(ss, 1);
  Command::fmtBulk(ss, "two");
  EXPECT_EQ(expect.value(), ss.str());

  ss.str("");
  sess.setArgs({"zrangebyscore", "tzk3.4", "(1", "(2"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  Command::fmtMultiBulkLen(ss, 0);
  EXPECT_EQ(expect.value(), ss.str());
}

void testSet(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  sess.setArgs({"sadd", "settestkey1", "one", "two", "three"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(3));
  sess.setArgs({"spop", "settestkey1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtBulk("one"))
    << expect.status().toString();
  sess.setArgs({"scard", "settestkey1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(2))
    << expect.status().toString();

  // srandmember
  sess.setArgs({"sadd", "settestkey2", "one", "two", "three"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(3));
  sess.setArgs({"srandmember", "settestkey2"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_TRUE(expect.value() == Command::fmtBulk("one") ||
              expect.value() == Command::fmtBulk("two") ||
              expect.value() == Command::fmtBulk("three"));

  sess.setArgs({"srandmember", "settestkey2", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  std::stringstream ss1, ss2, ss3;
  Command::fmtMultiBulkLen(ss1, 1);
  Command::fmtBulk(ss1, "one");
  Command::fmtMultiBulkLen(ss2, 1);
  Command::fmtBulk(ss2, "two");
  Command::fmtMultiBulkLen(ss3, 1);
  Command::fmtBulk(ss3, "three");
  EXPECT_TRUE(expect.value() == ss1.str() || expect.value() == ss2.str() ||
              expect.value() == ss3.str());
  sess.setArgs({"srandmember", "settestkey2", "2"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  ss1.str(""), ss2.str("");
  Command::fmtMultiBulkLen(ss1, 2);
  Command::fmtBulk(ss1, "one");
  Command::fmtBulk(ss1, "three");
  Command::fmtMultiBulkLen(ss2, 2);
  Command::fmtBulk(ss2, "three");
  Command::fmtBulk(ss2, "two");
  EXPECT_TRUE(expect.value() == ss1.str() || expect.value() == ss2.str());

  // smembers
  sess.setArgs({"sadd", "settestkey3", "hello", "world"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(2));
  sess.setArgs({"smembers", "settestkey3"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  ss1.str("");
  Command::fmtMultiBulkLen(ss1, 2);
  Command::fmtBulk(ss1, "hello");
  Command::fmtBulk(ss1, "world");
  EXPECT_EQ(ss1.str(), expect.value());
  sess.setArgs({"smembers", "settestkey4"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtZeroBulkLen());

  // sdiff
  sess.setArgs({"sadd", "sdiffkey1", "a", "b", "c", "d"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  sess.setArgs({"sadd", "sdiffkey2", "c"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  sess.setArgs({"sadd", "sdiffkey3", "a", "c", "e"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  sess.setArgs({"sdiff", "sdiffkey1", "sdiffkey2", "sdiffkey3"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  ss1.str("");
  Command::fmtMultiBulkLen(ss1, 2);
  Command::fmtBulk(ss1, "b");
  Command::fmtBulk(ss1, "d");
  EXPECT_EQ(expect.value(), ss1.str());

  // smove
  sess.setArgs({"sadd", "myset", "a", "b", "c"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(3));

  sess.setArgs({"sadd", "myset1", "d", "e", "f"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(3));

  sess.setArgs({"smove", "myset", "myset1", "c"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(1));

  ss1.str("");
  Command::fmtMultiBulkLen(ss1, 2);
  Command::fmtBulk(ss1, "a");
  Command::fmtBulk(ss1, "b");
  sess.setArgs({"smembers", "myset"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), ss1.str());

  ss1.str("");
  Command::fmtMultiBulkLen(ss1, 4);
  Command::fmtBulk(ss1, "c");
  Command::fmtBulk(ss1, "d");
  Command::fmtBulk(ss1, "e");
  Command::fmtBulk(ss1, "f");
  sess.setArgs({"smembers", "myset1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), ss1.str());

  sess.setArgs({"smove", "myset", "myset1", "x"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(0));

  ss1.str("");
  Command::fmtMultiBulkLen(ss1, 2);
  Command::fmtBulk(ss1, "a");
  Command::fmtBulk(ss1, "b");
  sess.setArgs({"smembers", "myset"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), ss1.str());

  ss1.str("");
  Command::fmtMultiBulkLen(ss1, 4);
  Command::fmtBulk(ss1, "c");
  Command::fmtBulk(ss1, "d");
  Command::fmtBulk(ss1, "e");
  Command::fmtBulk(ss1, "f");
  sess.setArgs({"smembers", "myset1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), ss1.str());

  sess.setArgs({"smove", "myset", "myset", "a"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(1));

  ss1.str("");
  Command::fmtMultiBulkLen(ss1, 2);
  Command::fmtBulk(ss1, "a");
  Command::fmtBulk(ss1, "b");
  sess.setArgs({"smembers", "myset"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), ss1.str());

  // wrong type
  sess.setArgs({"set", "myset3", "b"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"smove", "myset", "myset3", "a"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(!expect.ok());

  ss1.str("");
  Command::fmtMultiBulkLen(ss1, 2);
  Command::fmtBulk(ss1, "a");
  Command::fmtBulk(ss1, "b");
  sess.setArgs({"smembers", "myset"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), ss1.str());
}

void testZset(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);
  // pk not exists
  {
    sess.setArgs({"zrank", "tzk1", std::to_string(0)});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtNull());
  }
  for (uint32_t i = 1; i < 10000; i++) {
    sess.setArgs({"zadd", "tzk1", std::to_string(i), std::to_string(i)});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
  }
  for (uint32_t i = 1; i < 10000; i++) {
    sess.setArgs({"zrank", "tzk1", std::to_string(i)});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(i - 1));
  }
  {
    sess.setArgs({"zrank", "tzk1", std::to_string(0)});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtNull());
  }
  {
    sess.setArgs({"zadd", "tzk1", std::to_string(1), std::to_string(9999)});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(0));
    sess.setArgs({"zrank", "tzk1", std::to_string(9999)});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(1));
  }

  for (uint32_t i = 1; i < 10000; i++) {
    sess.setArgs({"zrem", "tzk1", std::to_string(i)});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(1));
    sess.setArgs({"zcard", "tzk1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(9999 - i));
    sess.setArgs({"exists", "tzk1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    if (i != 9999) {
      EXPECT_EQ(expect.value(), Command::fmtLongLong(1));
    } else {
      EXPECT_EQ(expect.value(), Command::fmtLongLong(0));
    }
  }

  {
    sess.setArgs({"zscore", "notfoundkey", "subkey"});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtNull());

    sess.setArgs({"zadd", "tzk1.1", std::to_string(9999), "subkey"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());

    sess.setArgs({"zscore", "tzk1.1", "notfoundsubkey"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtNull());

    sess.setArgs({"zscore", "tzk1.1", "subkey"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtBulk(std::to_string(9999)));
  }
}

void testPf(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  srand((unsigned int)time(NULL));

  // pk not exists
  {
    // non-exist pf
    sess.setArgs({"pfcount", "nepf"});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtZero());

    sess.setArgs({"set", "nonpf", "1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());

    sess.setArgs({"pfcount", "nonpf"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(!expect.ok());

    sess.setArgs({"pfadd", "pfempty"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOne());


    sess.setArgs({"pfadd", "pf1", "a", "b", "c"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOne());

    sess.setArgs({"pfadd", "pf1", "a", "b", "c"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtZero());

    sess.setArgs({"pfcount", "pf1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(3));

    sess.setArgs({"pfadd", "pf2", "", "e", "c"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOne());

    sess.setArgs({"pfcount", "pf2"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(3));

    sess.setArgs({"pfcount", "pf1", "pf2"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(5));

    sess.setArgs({"pfcount", "nepf", "nonpf", "pf1", "pf2"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(!expect.ok());

    sess.setArgs({"pfmerge", "nepf", "nonpf", "pf1", "pf2"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(!expect.ok());

    sess.setArgs({"pfmerge", "newpf", "pf1", "pf2"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOK());

    sess.setArgs({"pfcount", "newpf"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(5));

    sess.setArgs({"pfmerge", "pf1", "pf2"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOK());

    sess.setArgs({"pfcount", "pf1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(5));

    sess.setArgs({"get", "pf1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    auto pf1 = expect.value();

    sess.setArgs({"get", "newpf"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    auto newpf = expect.value();

    EXPECT_EQ(pf1, newpf);
  }

  // expire a pf
  {
    sess.setArgs({"pfadd", "expf", "a"});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOne());

    sess.setArgs({"expire", "expf", "5"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());

    sess.setArgs({"pfadd", "expf", "b"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOne());

    std::this_thread::sleep_for(std::chrono::seconds(5));

    sess.setArgs({"pfcount", "expf"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtZero());
  }

  // pfdebug
  {
    sess.setArgs({"pfadd", "pfxxx", "d", "b", "c"});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOne());

    sess.setArgs({"pfadd", "pfxxx2", "d", "b", "c"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOne());

    sess.setArgs({"pfdebug", "encoding", "pfxxx"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtStatus("sparse"));

    sess.setArgs({"pfdebug", "decode", "pfxxx"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(),
              Command::fmtBulk(
                "Z:7292 v:1,1 Z:1143 v:1,1 Z:7343 v:1,1 Z:603"));  // NOLINT

    sess.setArgs({"pfdebug", "todense", "pfxxx"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOne());

    sess.setArgs({"pfdebug", "encoding", "pfxxx"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtStatus("dense"));

    sess.setArgs({"pfdebug", "todense", "pfxxx"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtZero());

    sess.setArgs({"pfdebug", "decode", "pfxxx"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(!expect.ok());

    sess.setArgs({"pfdebug", "getreg", "pfxxx"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    auto getreg = expect.value();

    sess.setArgs({"pfdebug", "getreg", "pfxxx2"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    auto getreg1 = expect.value();

    EXPECT_EQ(getreg, getreg1);
  }

  // pfselftest
  for (uint32_t i = 0; i < 10; i++) {
    sess.setArgs({"pfselftest"});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOK());
  }

  ///////////////////////////////

  // 1. init a two small dense pf
  sess.setArgs({"pfadd", "bigpf", "a", "b", "c"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());

  sess.setArgs({"pfadd", "bigpf1", "a", "b", "c"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());


  sess.setArgs({"pfdebug", "todense", "bigpf"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());

  sess.setArgs({"pfdebug", "todense", "bigpf1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());

  // 2. check whether they are the same
  sess.setArgs({"get", "bigpf"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  auto estr = expect.value();

  sess.setArgs({"get", "bigpf1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  auto estr1 = expect.value();

  EXPECT_EQ(estr, estr1);


  uint32_t count = 0;
  std::vector<std::string> strvec;
  for (uint32_t i = 0; i < 10000; i++) {
    std::string rs = randomStr(genRand() % 20, true);
    strvec.emplace_back(rs);
    sess.setArgs({"pfadd", "bigpf", rs});
    // std::cout << "pfadd bigpf \"" << rs << "\"" << std::endl;
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    if (expect.value() == Command::fmtOne()) {
      count++;
    }
  }

  auto rng = std::default_random_engine{};
  std::shuffle(strvec.begin(), strvec.end(), rng);

  uint32_t count1 = 0;
  for (auto rs : strvec) {
    sess.setArgs({"pfadd", "bigpf1", rs});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    if (expect.value() == Command::fmtOne()) {
      count1++;
    }
  }

  // EXPECT_EQ(count, count1);

  sess.setArgs({"pfcount", "bigpf"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  auto ecnt = expect.value();

  sess.setArgs({"pfcount", "bigpf1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  auto ecnt1 = expect.value();

  sess.setArgs({"pfcount", "bigpf", "bigpf1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  auto ecnt2 = expect.value();

  EXPECT_EQ(ecnt, ecnt1);
  EXPECT_EQ(ecnt, ecnt2);

  sess.setArgs({"get", "bigpf"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  estr = expect.value();

  sess.setArgs({"get", "bigpf1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  estr1 = expect.value();

  EXPECT_EQ(estr, estr1);
}

void testType(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);
  sess.setArgs({"type", "test_type_key"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtStatus("none"));

  sess.setArgs({"set", "test_type_key", "a"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  sess.setArgs({"type", "test_type_key"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtStatus("string"));

  sess.setArgs({"hset", "test_type_key", "a", "b"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(!expect.ok());
  sess.setArgs({"type", "test_type_key"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtStatus("string"));

  sess.setArgs({"hset", "test_type_key1", "a", "b"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  sess.setArgs({"type", "test_type_key1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtStatus("hash"));
}

void testMset(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  std::stringstream ss;

  sess.setArgs({"mset", "ma", "0", "mb", "1", "mc", "2"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());

  sess.setArgs({"mget", "ma", "mb", "mc", "md"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  ss.str("");
  Command::fmtMultiBulkLen(ss, 4);
  Command::fmtBulk(ss, "0");
  Command::fmtBulk(ss, "1");
  Command::fmtBulk(ss, "2");
  Command::fmtNull(ss);
  EXPECT_EQ(ss.str(), expect.value());

  sess.setArgs({"msetnx", "md", "-1", "ma", "1", "mb", "2", "mc", "3"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtZero());

  sess.setArgs({"mget", "md", "ma", "mb", "mc"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  ss.str("");
  Command::fmtMultiBulkLen(ss, 4);
  Command::fmtNull(ss);
  Command::fmtBulk(ss, "0");
  Command::fmtBulk(ss, "1");
  Command::fmtBulk(ss, "2");
  EXPECT_EQ(ss.str(), expect.value());

  sess.setArgs({"mset", "ma", "10", "mb", "11", "mc", "20", "ma", "100"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());

  sess.setArgs({"mget", "md", "ma", "mb", "mc"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  ss.str("");
  Command::fmtMultiBulkLen(ss, 4);
  Command::fmtNull(ss);
  Command::fmtBulk(ss, "100");
  Command::fmtBulk(ss, "11");
  Command::fmtBulk(ss, "20");
  EXPECT_EQ(ss.str(), expect.value());

  sess.setArgs({"sadd", "sa", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"type", "sa"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtStatus("set"));

  // wrong type
  sess.setArgs({"mset", "sa", "100", "ma", "1000"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());

  sess.setArgs({"mget", "md", "ma", "mb", "mc"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  ss.str("");
  Command::fmtMultiBulkLen(ss, 4);
  Command::fmtNull(ss);
  Command::fmtBulk(ss, "1000");
  Command::fmtBulk(ss, "11");
  Command::fmtBulk(ss, "20");
  EXPECT_EQ(ss.str(), expect.value());

  sess.setArgs({"type", "sa"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtStatus("string"));

  sess.setArgs({"msetnx", "n1", "1", "n2", "2"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());

  sess.setArgs({"mget", "n1", "n2"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  ss.str("");
  Command::fmtMultiBulkLen(ss, 2);
  Command::fmtBulk(ss, "1");
  Command::fmtBulk(ss, "2");
  EXPECT_EQ(ss.str(), expect.value());
}

void testKV(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  // set
  sess.setArgs({"del", "testKV"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"set", "testKV", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());
  sess.setArgs({"set", "testKV", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());
  sess.setArgs({"set", "testKV", "1", "nx"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtNull());
  sess.setArgs({"set", "testKV", "1", "xx"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());
  sess.setArgs({"set", "testKV", "1", "xx"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());
  sess.setArgs({"set", "testKV", "1", "xx", "px", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());
  std::this_thread::sleep_for(std::chrono::seconds(1));
  sess.setArgs({"set", "testKV", "1", "xx"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtNull());

  // setnx
  sess.setArgs({"setnx", "testKV", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());
  sess.setArgs({"setnx", "testKV", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtZero());
  sess.setArgs({"expire", "testKV", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());
  std::this_thread::sleep_for(std::chrono::seconds(2));
  sess.setArgs({"setnx", "testKV", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());

  sess.setArgs({"sadd", "testKV1", "abc"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  sess.setArgs({"setnx", "testKV1", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtZero());
  sess.setArgs({"set", "testKV1", "1", "nx"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtNull());
  sess.setArgs({"del", "testKV1", "testKV"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  // set xx
  sess.setArgs({"set", "testKV", "1", "xx"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtNull());
  sess.setArgs({"set", "testKV", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());
  sess.setArgs({"set", "testKV", "2", "xx"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());
  sess.setArgs({"get", "testKV"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtBulk("2"));
  sess.setArgs({"sadd", "testKV1", "abc"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  sess.setArgs({"set", "testKV1", "1", "xx"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());
  sess.setArgs({"get", "testKV1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtBulk("1"));
  sess.setArgs({"del", "testKV1", "testKV"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  // setex
  sess.setArgs({"setex", "testKV", "1", "b"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  sess.setArgs({"get", "testKV"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtBulk("b"));
  std::this_thread::sleep_for(std::chrono::milliseconds(600));
  sess.setArgs({"get", "testKV"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtNull());

  sess.setArgs({"setex", "testKV", "-1", "b"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(!expect.ok());

  sess.setArgs({"psetex", "testKV", "-1", "b"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(!expect.ok());

  sess.setArgs({"set", "testKV", "b", "ex", "0"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(!expect.ok());

  sess.setArgs({"set", "testKV", "b", "px", "0"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(!expect.ok());

  // persist
  sess.setArgs({"setex", "testKV", "10", "b"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());

  sess.setArgs({"persist", "testKV"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());

  sess.setArgs({"persist", "a_nonexist"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtZero());

  sess.setArgs({"pttl", "testKV"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(-1));

  // exists
  sess.setArgs({"set", "expire_test_key0", "a"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());
  sess.setArgs({"set", "expire_test_key", "a"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());
  sess.setArgs({"exists", "expire_test_key"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());

  sess.setArgs(
    {"exists", "expire_test_key", "expire_test_key0", "expire_test_key1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(2));

  sess.setArgs({"expire", "expire_test_key", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());
  std::this_thread::sleep_for(std::chrono::seconds(2));
  sess.setArgs({"exists", "expire_test_key"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtZero());

  // object
  sess.setArgs({"object", "help"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"object", "encoding", "expire_test_key0"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtBulk("raw"));

  sess.setArgs({"object", "encoding", "expire_test_key1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtNull());

  // incrdecr
  sess.setArgs({"incr", "incrdecrkey"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());
  sess.setArgs({"incr", "incrdecrkey"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(2));
  sess.setArgs({"incrby", "incrdecrkey", "2"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(4));
  sess.setArgs({"incrby",
                "incrdecrkey",
                std::to_string(std::numeric_limits<int64_t>::max())});
  expect = Command::runSessionCmd(&sess);
  EXPECT_FALSE(expect.ok()) << expect.value();
  sess.setArgs({"incrby", "incrdecrkey", "-1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(3));
  sess.setArgs({"decr", "incrdecrkey"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(2));
  sess.setArgs({"decr", "incrdecrkey"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(1));
  sess.setArgs({"decrby", "incrdecrkey", "3"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(-2));
  sess.setArgs({"decrby",
                "incrdecrkey",
                std::to_string(std::numeric_limits<int64_t>::max())});
  expect = Command::runSessionCmd(&sess);
  EXPECT_FALSE(expect.ok()) << expect.value();

  // append
  sess.setArgs({"append", "appendkey", "abc"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(3));
  sess.setArgs({"append", "appendkey", "abc"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(6));
  sess.setArgs({"expire", "appendkey", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());
  sess.setArgs({"exists", "appendkey"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());
  std::this_thread::sleep_for(std::chrono::seconds(2));
  sess.setArgs({"append", "appendkey", "abc"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(3));

  // getset
  sess.setArgs({"getset", "getsetkey", "abc"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtNull());
  sess.setArgs({"getset", "getsetkey", "def"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtBulk("abc"));
  sess.setArgs({"expire", "getsetkey", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());
  sess.setArgs({"getset", "getsetkey", "abc"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtBulk("def"));
  std::this_thread::sleep_for(std::chrono::seconds(2));
  sess.setArgs({"exists", "appendkey"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());

  // setbit
  sess.setArgs({"setbit", "setbitkey", "7", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtZero());
  sess.setArgs({"setbit", "setbitkey", "7", "0"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());
  sess.setArgs({"get", "setbitkey"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  std::string setbitRes;
  setbitRes.push_back(0);
  EXPECT_EQ(expect.value(), Command::fmtBulk(setbitRes));

  // setrange
  sess.setArgs({"setrange", "setrangekey", "7", "abc"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(10));
  std::string setrangeRes;
  setrangeRes.resize(10, 0);
  setrangeRes[7] = 'a';
  setrangeRes[8] = 'b';
  setrangeRes[9] = 'c';
  sess.setArgs({"get", "setrangekey"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtBulk(setrangeRes));
  sess.setArgs({"setrange", "setrangekey", "8", "aaa"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(11));
  setrangeRes.resize(11);
  setrangeRes[8] = 'a';
  setrangeRes[9] = 'a';
  setrangeRes[10] = 'a';
  sess.setArgs({"get", "setrangekey"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtBulk(setrangeRes));

  // getrange
  sess.setArgs({"getrange", "setrangekey", "0", "-1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtBulk(setrangeRes));

  // bitcount
  sess.setArgs({"bitcount", "bitcountkey"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtZero());
  sess.setArgs({"set", "bitcountkey", "foobar"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());
  sess.setArgs({"bitcount", "bitcountkey"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(26));
  sess.setArgs({"bitcount", "bitcountkey", "0", "-1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(26));
  sess.setArgs({"bitcount", "bitcountkey", "2", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(0));
  std::vector<int> bitcountarr{4, 6, 6, 3, 3, 4};
  for (size_t i = 0; i < bitcountarr.size(); ++i) {
    sess.setArgs(
      {"bitcount", "bitcountkey", std::to_string(i), std::to_string(i)});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(bitcountarr[i]));
  }

  // bitpos
  sess.setArgs({"bitpos", "bitposkey", "0"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(0));
  sess.setArgs({"bitpos", "bitposkey", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(-1));
  sess.setArgs({"set", "bitposkey", {"\xff\xf0\x00", 3}});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());
  sess.setArgs({"bitpos", "bitposkey", "0"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(12));
  sess.setArgs({"set", "bitposkey", {"\x00\xff\xf0", 3}});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());
  sess.setArgs({"bitpos", "bitposkey", "1", "0"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(8));
  sess.setArgs({"bitpos", "bitposkey", "1", "2"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(16));
  sess.setArgs({"set", "bitposkey", {"\x00\x00\x00", 3}});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());
  sess.setArgs({"bitpos", "bitposkey", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(-1));
  sess.setArgs({"bitpos", "bitposkey", "0"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(0));

  // mget/mset
  sess.setArgs({"mset", "msetkey0", "0", "msetkey1", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());
  sess.setArgs({"mget", "msetkey0", "msetkey1", "msetkey2"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  std::stringstream ss;
  Command::fmtMultiBulkLen(ss, 3);
  Command::fmtBulk(ss, "0");
  Command::fmtBulk(ss, "1");
  Command::fmtNull(ss);
  EXPECT_EQ(ss.str(), expect.value());

  // cas/getvsn
  sess.setArgs({"set", "caskey", "1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());
  sess.setArgs({"getvsn", "caskey"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  ss.str("");
  Command::fmtMultiBulkLen(ss, 2);
  Command::fmtLongLong(ss, -1);
  Command::fmtBulk(ss, "1");
  EXPECT_EQ(expect.value(), ss.str());
  sess.setArgs({"cas", "caskey", "0", "2"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());
  sess.setArgs({"cas", "caskey", "0", "2"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_FALSE(expect.ok());
  EXPECT_EQ(expect.status().code(), ErrorCodes::ERR_CAS);
  sess.setArgs({"getvsn", "caskey"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  ss.str("");
  Command::fmtMultiBulkLen(ss, 2);
  Command::fmtLongLong(ss, 1);
  Command::fmtBulk(ss, "2");

  // bitop
  sess.setArgs({"set", "bitopkey1", "foobar"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());
  sess.setArgs({"set", "bitopkey2", "abcdef"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());
  sess.setArgs({"bitop", "and", "bitopdestkey", "bitopkey1", "bitopkey2"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtLongLong(6));
  sess.setArgs({"get", "bitopdestkey"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtBulk("`bc`ab"));
}

void testExpire(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  // bounder for optimistic del/pessimistic del
  for (auto v : {1000u, 10000u}) {
    for (uint32_t i = 0; i < v; i++) {
      sess.setArgs({"lpush", "testExpire", std::to_string(2 * i)});
      auto expect = Command::runSessionCmd(&sess);
      EXPECT_TRUE(expect.ok());
      EXPECT_EQ(expect.value(), Command::fmtLongLong(i + 1));
    }

    sess.setArgs({"expire", "testExpire", std::to_string(1)});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOne());

    sess.setArgs({"llen", "testExpire"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(v));

    std::this_thread::sleep_for(std::chrono::seconds(2));
    sess.setArgs({"llen", "testExpire"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtZero());
  }
}

void testExpire1(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  // bounder for optimistic del/pessimistic del
  for (auto v : {1000u, 10000u}) {
    for (uint32_t i = 0; i < v; i++) {
      sess.setArgs({"lpush", "testExpire1", std::to_string(2 * i)});
      auto expect = Command::runSessionCmd(&sess);
      EXPECT_TRUE(expect.ok());
      EXPECT_EQ(expect.value(), Command::fmtLongLong(i + 1));
    }

    sess.setArgs({"expire", "testExpire1", std::to_string(-1)});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOne());

    sess.setArgs({"llen", "testExpire1"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtZero());

    sess.setArgs({"expire", "testExpire1", std::to_string(-1)});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtZero());
  }
}

void testExpire2(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  // bounder for optimistic del/pessimistic del
  for (auto v : {1000u, 10000u}) {
    for (uint32_t i = 0; i < v; i++) {
      sess.setArgs({"lpush", "testExpire2la", std::to_string(2 * i)});
      auto expect = Command::runSessionCmd(&sess);
      EXPECT_TRUE(expect.ok());
      EXPECT_EQ(expect.value(), Command::fmtLongLong(i + 1));
    }

    for (uint32_t i = 0; i < v; i++) {
      sess.setArgs(
        {"hset", "testExpire2ha", std::to_string(i), std::to_string(i)});
      auto expect = Command::runSessionCmd(&sess);
      EXPECT_TRUE(expect.ok());
      EXPECT_EQ(expect.value(), Command::fmtOne());
    }

    sess.setArgs({"llen", "testExpire2la"});
    auto expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(v));

    sess.setArgs({"hlen", "testExpire2ha"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtLongLong(v));

    sess.setArgs({"expire", "testExpire2la", std::to_string(-1)});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOne());

    sess.setArgs({"expire", "testExpire2ha", std::to_string(-1)});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtOne());


    sess.setArgs({"llen", "testExpire2la"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtZero());

    sess.setArgs({"hlen", "testExpire2ha"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_TRUE(expect.ok());
    EXPECT_EQ(expect.value(), Command::fmtZero());
  }
}

void testExpireCommandWhenNoexpireTrue(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  sess.setArgs({"config", "set", "noexpire", "no"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());

  sess.setArgs({"set", "key", "xxx", "PX", std::to_string(1)});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());

  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  sess.setArgs({"expire", "key", std::to_string(1)});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtZero());

  sess.setArgs({"config", "set", "noexpire", "yes"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());

  sess.setArgs({"set", "key", "xxx", "PX", std::to_string(1)});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());

  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  sess.setArgs({"expire", "key", std::to_string(1)});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());
}

void testExpireKeyWhenGet(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  sess.setArgs({"config", "set", "noexpire", "yes"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());

  // string
  sess.setArgs({"set", "key", "xxx", "PX", std::to_string(1)});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());

  // hash
  sess.setArgs({"hset", "myhash", "k", "v"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());

  sess.setArgs({"pexpire", "myhash", std::to_string(1)});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());

  // set
  sess.setArgs({"sadd", "myset", "v"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());

  sess.setArgs({"pexpire", "myset", std::to_string(1)});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());

  // zset
  sess.setArgs({"zadd", "myzset", std::to_string(100), "k"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());

  sess.setArgs({"pexpire", "myzset", std::to_string(1)});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());

  // list
  sess.setArgs({"lpush", "mylist", "v"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());

  sess.setArgs({"pexpire", "mylist", std::to_string(1)});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());

  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  // we can get expired key, if noexpire true
  sess.setArgs({"get", "key"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtBulk("xxx"));

  sess.setArgs({"hget", "myhash", "k"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtBulk("v"));

  sess.setArgs({"smembers", "myset"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  std::stringstream ss;
  Command::fmtMultiBulkLen(ss, 1);
  Command::fmtBulk(ss, "v");
  EXPECT_EQ(expect.value(), ss.str());

  sess.setArgs({"zrange", "myzset", "0", "-1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  ss.str("");
  Command::fmtMultiBulkLen(ss, 1);
  Command::fmtBulk(ss, "k");
  EXPECT_EQ(expect.value(), ss.str());

  sess.setArgs({"lindex", "mylist", "0"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtBulk("v"));

  // delete expired key when get
  sess.setArgs({"config", "set", "noexpire", "no"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());

  sess.setArgs({"get", "key"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtNull());

  sess.setArgs({"hget", "myhash", "k"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtNull());

  sess.setArgs({"smembers", "myset"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtZeroBulkLen());

  sess.setArgs({"zrange", "myzset", "0", "-1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtZeroBulkLen());

  sess.setArgs({"lindex", "mylist", "0"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtNull());
}

void testExpireKeyWhenCompaction(std::shared_ptr<ServerEntry> svr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  sess.setArgs({"config", "set", "noexpire", "yes"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());

  // string
  sess.setArgs({"set", "key", "xxx", "PX", std::to_string(1)});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());

  // hash
  sess.setArgs({"hset", "myhash", "k", "v"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());

  sess.setArgs({"pexpire", "myhash", std::to_string(1)});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());

  // set
  sess.setArgs({"sadd", "myset", "v"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());

  sess.setArgs({"pexpire", "myset", std::to_string(1)});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());

  // zset
  sess.setArgs({"zadd", "myzset", std::to_string(100), "k"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());

  sess.setArgs({"pexpire", "myzset", std::to_string(1)});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());

  // list
  sess.setArgs({"lpush", "mylist", "v"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());

  sess.setArgs({"pexpire", "mylist", std::to_string(1)});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOne());

  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  // we can get expired key, if noexpire true
  sess.setArgs({"get", "key"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtBulk("xxx"));

  sess.setArgs({"hget", "myhash", "k"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtBulk("v"));

  sess.setArgs({"smembers", "myset"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  std::stringstream ss;
  Command::fmtMultiBulkLen(ss, 1);
  Command::fmtBulk(ss, "v");
  EXPECT_EQ(expect.value(), ss.str());

  sess.setArgs({"zrange", "myzset", "0", "-1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  ss.str("");
  Command::fmtMultiBulkLen(ss, 1);
  Command::fmtBulk(ss, "k");
  EXPECT_EQ(expect.value(), ss.str());

  sess.setArgs({"lindex", "mylist", "0"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtBulk("v"));


  // delete expired key by compaction and indexMgr
  sess.setArgs({"config", "set", "noexpire", "no"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtOK());

  for (const auto& store : svr->getStores()) {
    store->fullCompact();
  }
  std::this_thread::sleep_for(
    std::chrono::seconds(2 * svr->getParams()->pauseTimeIndexMgr));

  // test if key exist
  svr->getParams()->noexpire = true;
  sess.setArgs({"get", "key"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtNull());

  sess.setArgs({"hget", "myhash", "k"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtNull());

  sess.setArgs({"smembers", "myset"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtZeroBulkLen());

  sess.setArgs({"zrange", "myzset", "0", "-1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtZeroBulkLen());

  sess.setArgs({"lindex", "mylist", "0"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  EXPECT_EQ(expect.value(), Command::fmtNull());
}

void testSync(std::shared_ptr<ServerEntry> svr) {
  auto fmtSyncVerRes = [](std::stringstream& ss, uint64_t ts, uint64_t ver) {
    ss.str("");
    Command::fmtMultiBulkLen(ss, 2);
    Command::fmtLongLong(ss, ts);
    Command::fmtLongLong(ss, ver);
  };

  asio::io_context ioCtx;
  asio::ip::tcp::socket socket(ioCtx), socket1(ioCtx);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  sess.setArgs({"syncversion", "unittest", "?", "?", "v1"});
  auto expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"syncversion", "unittest", "100", "100", "v1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"syncversion", "unittest", "?", "?", "v1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());
  std::stringstream ss1;
  fmtSyncVerRes(ss1, 100, 100);
  EXPECT_EQ(ss1.str(), expect.value());

  sess.setArgs({"syncversion", "unittest", "105", "102", "v1"});
  expect = Command::runSessionCmd(&sess);
  EXPECT_TRUE(expect.ok());

  sess.setArgs({"syncversion", "unittest", "?", "?", "v1"});
  expect = Command::runSessionCmd(&sess);
  fmtSyncVerRes(ss1, 105, 102);
  EXPECT_EQ(ss1.str(), expect.value());
}

void testAll(std::shared_ptr<ServerEntry> svr) {
  testExpire1(svr);
  testExpire2(svr);
  testExpire(svr);
  testKV(svr);
  // need cfg->checkKeyTypeForSet = true;
  // testMset(svr);
  testType(svr);
  testPf(svr);
  testZset(svr);
  testSet(svr);
  testZset3(svr);
  testZset4(svr);
  testZset2(svr);
  testHash1(svr);
  testHash2(svr);
  testList(svr);
  testSync(svr);
}

void waitSlaveCatchup(const std::shared_ptr<ServerEntry>& master,
                      const std::shared_ptr<ServerEntry>& slave) {
  auto ctx1 = std::make_shared<asio::io_context>();
  auto sess1 = makeSession(master, ctx1);
  WorkLoad work1(master, sess1);
  work1.init();

  auto ctx2 = std::make_shared<asio::io_context>();
  auto sess2 = makeSession(slave, ctx2);
  WorkLoad work2(master, sess2);
  work2.init();

  INVARIANT(master->getKVStoreCount() == slave->getKVStoreCount());

  for (size_t i = 0; i < master->getKVStoreCount(); i++) {
    auto binlogPos1 = work1.getIntResult({"binlogpos", std::to_string(i)});
    while (true) {
      auto binlogPos2 = work2.getIntResult({"binlogpos", std::to_string(i)});
      if (!binlogPos2.ok()) {
        EXPECT_TRUE(binlogPos2.status().code() == ErrorCodes::ERR_EXHAUST);
        EXPECT_TRUE(binlogPos1.status().code() == ErrorCodes::ERR_EXHAUST);
        break;
      }
      if (binlogPos2.value() < binlogPos1.value()) {
        LOG(WARNING) << "store id " << i << " : binlogpos ("
                     << binlogPos1.value() << ">" << binlogPos2.value() << ");";
        std::this_thread::sleep_for(std::chrono::seconds(1));
      } else if (binlogPos1.value() < binlogPos2.value()) {
        // NOTE(takenliu): flush command maybe let slave's binlogpos
        // bigger,
        //     but it will be set to equal master's binlogpos later
        LOG(WARNING) << "store id " << i << " : binlogpos ("
                     << binlogPos1.value() << "<" << binlogPos2.value() << ");";
        std::this_thread::sleep_for(std::chrono::seconds(1));
      } else {
        EXPECT_EQ(binlogPos1.value(), binlogPos2.value());
        break;
      }
    }
  }
}

std::string runCommand(std::shared_ptr<ServerEntry> svr,
                       std::vector<std::string> args) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  sess.setArgs(args);
  auto expect = Command::runSessionCmd(&sess);
  INVARIANT_D(expect.ok());

  return expect.value();
}

void runBgCommand(std::shared_ptr<ServerEntry> svr) {
  runCommand(svr, {"info", "all"});
  runCommand(svr, {"client", "list"});
  runCommand(svr, {"show", "processlist", "all"});
}

void NoSchedNetSession::setArgsFromAof(const std::string& cmd) {
  std::lock_guard<std::mutex> lk(_mutex);
  _queryBuf.clear();
  _args.clear();
  std::copy(cmd.begin(), cmd.end(), std::back_inserter(_queryBuf));
  _queryBufPos = _queryBuf.size();
  processMultibulkBuffer();

  INVARIANT_D(_args.size() > 0);
}

std::vector<std::string> NoSchedNetSession::getResponse() {
  std::lock_guard<std::mutex> lk(_mutex);
  std::vector<std::string> ret;
  for (auto sb : _sendBuffer) {
    ret.emplace_back(std::string(sb->buffer.data(), sb->buffer.size()));
  }

  return std::move(ret);
}

}  // namespace tendisplus
