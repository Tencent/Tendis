// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <memory>
#include <utility>
#include <thread>  // NOLINT

#include "gtest/gtest.h"
#include "glog/logging.h"

#include "tendisplus/server/server_entry.h"
#include "tendisplus/server/index_manager.h"
#include "tendisplus/server/segment_manager.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/network/network.h"
#include "tendisplus/utils/test_util.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/invariant.h"

namespace tendisplus {

const char master_dir[] = "repltest_master";
const char slave_dir[] = "repltest_slave";
const char slave_slave_dir[] = "repltest_slave_slave";
const char slave1_dir[] = "repltest_slave1";
const char slave2_dir[] = "repltest_slave2";
const char single_dir[] = "repltest_single";
const char single_dir2[] = "repltest_single2";
uint32_t master_port = 1111;
uint32_t slave_port = 1112;
uint32_t slave_slave_port = 1113;
uint32_t slave1_port = 2111;
uint32_t slave2_port = 2112;
uint32_t single_port = 2113;

AllKeys initData(std::shared_ptr<ServerEntry>& server, uint32_t count) {
  auto ctx1 = std::make_shared<asio::io_context>();
  auto sess1 = makeSession(server, ctx1);
  WorkLoad work(server, sess1);
  work.init();

  AllKeys all_keys;

  auto kv_keys = work.writeWork(RecordType::RT_KV, count);
  all_keys.emplace_back(kv_keys);

  auto list_keys = work.writeWork(RecordType::RT_LIST_META, count, 50);
  all_keys.emplace_back(list_keys);

  // wait binlog dump to disk
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // #ifdef _WIN32
  work.flush();
  // #endif

  auto hash_keys = work.writeWork(RecordType::RT_HASH_META, count, 50);
  all_keys.emplace_back(hash_keys);

  auto set_keys = work.writeWork(RecordType::RT_SET_META, count, 50);
  all_keys.emplace_back(set_keys);

  auto zset_keys = work.writeWork(RecordType::RT_ZSET_META, count, 50);
  all_keys.emplace_back(zset_keys);

  return std::move(all_keys);
}

std::pair<std::shared_ptr<ServerEntry>, std::shared_ptr<ServerEntry>>
makeReplEnv(uint32_t storeCnt) {
  EXPECT_TRUE(setupEnv(master_dir));
  EXPECT_TRUE(setupEnv(slave_dir));

  auto cfg1 = makeServerParam(master_port, storeCnt, master_dir, false);
  auto cfg2 = makeServerParam(slave_port, storeCnt, slave_dir, false);

  auto master = std::make_shared<ServerEntry>(cfg1);
  auto s = master->startup(cfg1);
  INVARIANT(s.ok());

  auto slave = std::make_shared<ServerEntry>(cfg2);
  s = slave->startup(cfg2);
  INVARIANT(s.ok());
  {
    auto ctx = std::make_shared<asio::io_context>();
    auto session = makeSession(slave, ctx);

    WorkLoad work(slave, session);
    work.init();
    work.slaveof("127.0.0.1", master_port);
  }

  return std::make_pair(master, slave);
}

std::shared_ptr<ServerEntry> makeAnotherSlave(const std::string& name,
                                              uint32_t storeCnt,
                                              uint32_t port,
                                              uint32_t mport) {
  INVARIANT(name != master_dir && name != slave_dir);
  INVARIANT(port != mport && port != slave_port);
  EXPECT_TRUE(setupEnv(name));

  auto cfg1 = makeServerParam(port, storeCnt, name, false);

  auto slave = std::make_shared<ServerEntry>(cfg1);
  auto s = slave->startup(cfg1);
  INVARIANT(s.ok());

  {
    auto ctx = std::make_shared<asio::io_context>();
    auto session = makeSession(slave, ctx);

    WorkLoad work(slave, session);
    work.init();
    work.slaveof("127.0.0.1", mport);
  }

  return slave;
}

#ifdef _WIN32
size_t recordSize = 10;
#else
size_t recordSize = 1000;
#endif

TEST(Repl, Common) {
#ifdef _WIN32
  size_t i = 0;
  {
#else
  for (size_t i = 0; i < 3; i++) {
#endif
    LOG(INFO) << ">>>>>> test store count:" << i;
    const auto guard = MakeGuard([] {
      destroyReplEnv();
      destroyEnv(slave1_dir);
      destroyEnv(slave2_dir);
      std::this_thread::sleep_for(std::chrono::seconds(5));
    });

    auto hosts = makeReplEnv(i);

    auto& master = hosts.first;
    auto& slave = hosts.second;
    bool running = true;

    // make sure slaveof is ok
    std::this_thread::sleep_for(std::chrono::seconds(5));
    auto ctx1 = std::make_shared<asio::io_context>();
    auto sess1 = makeSession(master, ctx1);
    WorkLoad work(master, sess1);
    work.init();
    work.flush();

    auto allKeys = initData(master, recordSize);

    waitSlaveCatchup(master, slave);
    sleep(3);  // wait recycle binlog
    compareData(master, slave);

    auto slave1 = makeAnotherSlave(slave1_dir, i, slave1_port, master_port);
    // chain slave
    auto slave_slave =
      makeAnotherSlave(slave_slave_dir, i, slave_slave_port, slave_port);

    std::thread thd0([&master, &slave, &slave1, &slave_slave, &running]() {
      while (running) {
        auto s = runCommand(master, {"info", "replication"});
        LOG(INFO) << s;
        s = runCommand(slave, {"info", "replication"});
        LOG(INFO) << s;
        s = runCommand(slave_slave, {"info", "replication"});
        LOG(INFO) << s;
        s = runCommand(slave1, {"info", "replication"});
        LOG(INFO) << s;

        runBgCommand(master);
        runBgCommand(slave);
        runBgCommand(slave_slave);
        runBgCommand(slave1);

        std::this_thread::sleep_for(std::chrono::seconds(1));
      }
    });

    // delete all the keys
    for (auto k : allKeys) {
      if (genRand() % 4 == 0) {
        work.delKeys(k);
      }
    }

    std::thread thd1([&master]() {
#ifndef _WIN32
      testAll(master);
#endif
    });

    std::thread thd2([&master]() {
#ifndef _WIN32
      std::this_thread::sleep_for(std::chrono::seconds(genRand() % 50));
      auto ctx1 = std::make_shared<asio::io_context>();
      auto sess1 = makeSession(master, ctx1);
      WorkLoad work(master, sess1);
      work.init();
      // work.flush();
#endif
    });


    std::this_thread::sleep_for(std::chrono::seconds(genRand() % 10 + 5));
    auto slave2 = makeAnotherSlave(slave2_dir, i, slave2_port, master_port);

    LOG(INFO) << "waiting thd1 to exited";
    running = false;
    thd0.join();
    thd1.join();
    thd2.join();

    sleep(2);  // wait recycle binlog

    waitSlaveCatchup(master, slave);
    compareData(master, slave);

    waitSlaveCatchup(master, slave1);
    compareData(master, slave1);

    waitSlaveCatchup(master, slave2);
    compareData(master, slave2);

    waitSlaveCatchup(slave, slave_slave);
    compareData(slave, slave_slave);

#ifndef _WIN32
    master->stop();
    slave->stop();
    slave_slave->stop();
    slave1->stop();
    slave2->stop();

    ASSERT_EQ(slave.use_count(), 1);
    // ASSERT_EQ(master.use_count(), 1);
    ASSERT_EQ(slave_slave.use_count(), 1);
    ASSERT_EQ(slave1.use_count(), 1);
    ASSERT_EQ(slave2.use_count(), 1);
#endif

    LOG(INFO) << ">>>>>> test store count:" << i << " end;";
  }
}

void runCmd(std::shared_ptr<ServerEntry> svr,
            const std::vector<std::string>& args) {
  auto ctx = std::make_shared<asio::io_context>();
  auto session = makeSession(svr, ctx);
  session->setArgs(args);
  auto expect = Command::runSessionCmd(session.get());
  EXPECT_EQ(expect.ok(), true);
}

TEST(Repl, SlaveOfNeedEmpty) {
  size_t i = 0;
  {
    LOG(INFO) << ">>>>>> test store count:" << i;
    const auto guard = MakeGuard([] {
      destroyEnv(master_dir);
      destroyEnv(slave_dir);
      std::this_thread::sleep_for(std::chrono::seconds(5));
    });

    EXPECT_TRUE(setupEnv(master_dir));
    EXPECT_TRUE(setupEnv(slave_dir));

    auto cfg1 = makeServerParam(master_port, i, master_dir, false);
    auto cfg2 = makeServerParam(slave_port, i, slave_dir, false);

    auto master = std::make_shared<ServerEntry>(cfg1);
    auto s = master->startup(cfg1);
    INVARIANT(s.ok());

    auto slave = std::make_shared<ServerEntry>(cfg2);
    s = slave->startup(cfg2);
    INVARIANT(s.ok());
    {
      auto ctx0 = std::make_shared<asio::io_context>();
      auto session0 = makeSession(master, ctx0);
      session0->setArgs({"set", "a", "2"});
      auto expect = Command::runSessionCmd(session0.get());
      EXPECT_TRUE(expect.ok());

      auto ctx = std::make_shared<asio::io_context>();
      auto session = makeSession(slave, ctx);

      session->setArgs({"set", "a", "1"});
      expect = Command::runSessionCmd(session.get());
      EXPECT_TRUE(expect.ok());

      session->setArgs({"slaveof", "127.0.0.1", std::to_string(master_port)});
      expect = Command::runSessionCmd(session.get());
      EXPECT_EQ(expect.ok(), false);
      EXPECT_EQ(expect.status().toString(), "-ERR:4,msg:store not empty\r\n");

      session->setArgs({
        "flushall",
      });
      expect = Command::runSessionCmd(session.get());
      EXPECT_EQ(expect.ok(), true);

      session->setArgs({"slaveof", "127.0.0.1", std::to_string(master_port)});
      expect = Command::runSessionCmd(session.get());
      EXPECT_EQ(expect.ok(), true);

      waitSlaveCatchup(master, slave);

      session->setArgs({"get", "a"});
      expect = Command::runSessionCmd(session.get());
      EXPECT_EQ(expect.value(), "$1\r\n2\r\n");
    }

#ifndef _WIN32
    master->stop();
    slave->stop();

    ASSERT_EQ(slave.use_count(), 1);
#endif

    LOG(INFO) << ">>>>>> test store count:" << i << " end;";
  }
}

void checkBinlogFile(string dir, bool hasBinlog, uint32_t storeCount) {
  for (uint32_t i = 0; i < storeCount; ++i) {
    std::string fullFileName = dir + "/dump/" + std::to_string(i) + "/";

    uint32_t maxsize = 0;
    for (auto& DirectoryIter : filesystem::directory_iterator(fullFileName)) {
      uint32_t size = filesystem::file_size(DirectoryIter.path());
      LOG(INFO) << "checkBinlogFile, path:" << DirectoryIter.path()
                << " size:" << size;
      if (size > maxsize) {
        maxsize = size;
      }
    }
    if (hasBinlog) {
      EXPECT_GT(maxsize, BINLOG_HEADER_V2_LEN);
    } else {
      EXPECT_TRUE(maxsize == BINLOG_HEADER_V2_LEN || maxsize == 0);
    }
  }
}

TEST(Repl, MasterDontSaveBinlog) {
  size_t i = 1;
  {
    LOG(INFO) << ">>>>>> test store count:" << i;
    const auto guard = MakeGuard([] {
      destroyEnv(master_dir);
      destroyEnv(slave_dir);
      destroyEnv(slave1_dir);
      destroyEnv(single_dir);
      std::this_thread::sleep_for(std::chrono::seconds(5));
    });

    EXPECT_TRUE(setupEnv(master_dir));
    EXPECT_TRUE(setupEnv(slave_dir));
    EXPECT_TRUE(setupEnv(slave1_dir));
    EXPECT_TRUE(setupEnv(single_dir));

    auto cfg1 = makeServerParam(master_port, i, master_dir, false);
    auto cfg2 = makeServerParam(slave_port, i, slave_dir, false);
    auto cfg3 = makeServerParam(slave1_port, i, slave1_dir, false);
    auto cfg4 = makeServerParam(single_port, i, single_dir, false);
    cfg1->maxBinlogKeepNum = 1;
    cfg1->minBinlogKeepSec = 0;
    cfg2->maxBinlogKeepNum = 1;
    cfg2->minBinlogKeepSec = 0;
    cfg2->slaveBinlogKeepNum = 1;
    cfg3->maxBinlogKeepNum = 1;
    cfg3->minBinlogKeepSec = 0;
    cfg3->slaveBinlogKeepNum = 1;
    cfg4->maxBinlogKeepNum = 1;
    cfg4->minBinlogKeepSec = 0;

    auto master = std::make_shared<ServerEntry>(cfg1);
    auto s = master->startup(cfg1);
    INVARIANT(s.ok());

    auto slave = std::make_shared<ServerEntry>(cfg2);
    s = slave->startup(cfg2);
    INVARIANT(s.ok());

    auto slave1 = std::make_shared<ServerEntry>(cfg3);
    s = slave1->startup(cfg3);
    INVARIANT(s.ok());

    auto single = std::make_shared<ServerEntry>(cfg4);
    s = single->startup(cfg4);
    INVARIANT(s.ok());

    {
      runCmd(slave, {"slaveof", "127.0.0.1", std::to_string(master_port)});
      // slaveof need about 3 seconds to transfer file.
      std::this_thread::sleep_for(std::chrono::seconds(5));

      runCmd(slave1, {"slaveof", "127.0.0.1", std::to_string(slave_port)});
      std::this_thread::sleep_for(std::chrono::seconds(5));

      runCmd(master, {"set", "a", "1"});
      runCmd(master, {"set", "a", "2"});

      runCmd(single, {"set", "a", "1"});
      runCmd(single, {"set", "a", "2"});
    }
    // need wait enough time.
    std::this_thread::sleep_for(std::chrono::seconds(5));
    {
      runCmd(master, {"binlogflush", "all"});
      runCmd(slave, {"binlogflush", "all"});
      runCmd(slave1, {"binlogflush", "all"});
      runCmd(single, {"binlogflush", "all"});
    }
    std::this_thread::sleep_for(std::chrono::seconds(5));
    checkBinlogFile(master_dir, false, i);
    checkBinlogFile(slave_dir, true, i);
    checkBinlogFile(slave1_dir, true, i);
    checkBinlogFile(single_dir, true, i);

#ifndef _WIN32
    master->stop();
    slave->stop();
    slave1->stop();
    single->stop();

    ASSERT_EQ(slave.use_count(), 1);
#endif

    LOG(INFO) << ">>>>>> test store count:" << i << " end;";
  }
}

TEST(Repl, SlaveCantModify) {
  size_t i = 1;
  {
    LOG(INFO) << ">>>>>> test store count:" << i;
    const auto guard = MakeGuard([] {
      destroyEnv(master_dir);
      destroyEnv(slave_dir);
      destroyEnv(slave1_dir);
      std::this_thread::sleep_for(std::chrono::seconds(5));
    });

    EXPECT_TRUE(setupEnv(master_dir));
    EXPECT_TRUE(setupEnv(slave_dir));
    EXPECT_TRUE(setupEnv(slave1_dir));

    auto cfg1 = makeServerParam(master_port, i, master_dir, false);
    auto cfg2 = makeServerParam(slave_port, i, slave_dir, false);
    auto cfg3 = makeServerParam(slave1_port, i, slave1_dir, false);

    auto master = std::make_shared<ServerEntry>(cfg1);
    auto s = master->startup(cfg1);
    INVARIANT(s.ok());

    auto slave = std::make_shared<ServerEntry>(cfg2);
    s = slave->startup(cfg2);
    INVARIANT(s.ok());

    auto slave1 = std::make_shared<ServerEntry>(cfg3);
    s = slave1->startup(cfg3);
    INVARIANT(s.ok());

    {
      runCmd(slave, {"slaveof", "127.0.0.1", std::to_string(master_port)});
      // slaveof need about 3 seconds to transfer file.
      std::this_thread::sleep_for(std::chrono::seconds(5));

      runCmd(slave1, {"slaveof", "127.0.0.1", std::to_string(slave_port)});
      std::this_thread::sleep_for(std::chrono::seconds(5));

      auto ctx1 = std::make_shared<asio::io_context>();
      auto session1 = makeSession(master, ctx1);
      session1->setArgs({"set", "a", "2"});
      auto expect = Command::runSessionCmd(session1.get());
      EXPECT_TRUE(expect.ok());

      waitSlaveCatchup(master, slave);
      waitSlaveCatchup(slave, slave1);

      auto ctx2 = std::make_shared<asio::io_context>();
      auto session2 = makeSession(slave, ctx2);
      session2->setArgs({"set", "a", "3"});
      expect = Command::runSessionCmd(session2.get());
      EXPECT_EQ(expect.status().toString(), "-ERR:3,msg:txn is replOnly\r\n");

      session2->setArgs({"get", "a"});
      expect = Command::runSessionCmd(session2.get());
      EXPECT_EQ(expect.value(), "$1\r\n2\r\n");

      auto ctx3 = std::make_shared<asio::io_context>();
      auto session3 = makeSession(slave1, ctx3);
      session3->setArgs({"set", "a", "4"});
      expect = Command::runSessionCmd(session3.get());
      EXPECT_EQ(expect.status().toString(), "-ERR:3,msg:txn is replOnly\r\n");

      session3->setArgs({"get", "a"});
      expect = Command::runSessionCmd(session3.get());
      EXPECT_EQ(expect.value(), "$1\r\n2\r\n");
    }

#ifndef _WIN32
    master->stop();
    slave->stop();
    slave1->stop();

    ASSERT_EQ(slave.use_count(), 1);
#endif

    LOG(INFO) << ">>>>>> test store count:" << i << " end;";
  }
}

TEST(Repl, slaveofBenchmarkingMaster) {
  size_t i = 0;
  {
    LOG(INFO) << ">>>>>> test store count:" << i;
    const auto guard = MakeGuard([] {
      destroyEnv(master_dir);
      destroyEnv(slave_dir);
      std::this_thread::sleep_for(std::chrono::seconds(5));
    });

    EXPECT_TRUE(setupEnv(master_dir));
    EXPECT_TRUE(setupEnv(slave_dir));

    auto cfg1 = makeServerParam(master_port, i, master_dir, false);
    auto cfg2 = makeServerParam(slave_port, i, slave_dir, false);

    auto master = std::make_shared<ServerEntry>(cfg1);
    auto s = master->startup(cfg1);
    INVARIANT(s.ok());

    auto slave = std::make_shared<ServerEntry>(cfg2);
    s = slave->startup(cfg2);
    INVARIANT(s.ok());

    LOG(INFO) << ">>>>>> master add data begin.";
    auto thread = std::thread([this, master]() {
      testAll(master);  // need about 40 seconds
    });
    uint32_t sleep_time = genRand() % 20 + 10;  // 10-30 seconds
    sleep(sleep_time);

    LOG(INFO) << ">>>>>> slaveof begin.";
    runCmd(slave, {"slaveof", "127.0.0.1", std::to_string(master_port)});
    // slaveof need about 3 seconds to transfer file.
    std::this_thread::sleep_for(std::chrono::seconds(5));

    thread.join();
    waitSlaveCatchup(master, slave);
    sleep(3);  // wait recycle binlog
    compareData(master, slave);
    LOG(INFO) << ">>>>>> compareData end.";
#ifndef _WIN32
    master->stop();
    slave->stop();
    ASSERT_EQ(slave.use_count(), 1);
#endif

    LOG(INFO) << ">>>>>> test store count:" << i << " end;";
  }
}
// TODO(wayenchen) test again when psynenable finish
TEST(Repl, slaveofBenchmarkingMasterAOF) {
  size_t i = 0;
  {
    LOG(INFO) << ">>>>>> test store count:" << i;

    const auto guard = MakeGuard([] {
      destroyEnv(master_dir);
      destroyEnv(slave_dir);
      std::this_thread::sleep_for(std::chrono::seconds(5));
    });

    EXPECT_TRUE(setupEnv(master_dir));
    EXPECT_TRUE(setupEnv(slave_dir));

    auto cfg1 = makeServerParam(master_port, i, master_dir, true);
    auto cfg2 = makeServerParam(slave_port, i, slave_dir, true);
    cfg1->aofEnabled = true;
    cfg2->aofEnabled = true;
    cfg1->psyncEnabled = true;
    cfg2->psyncEnabled = true;

    auto master = std::make_shared<ServerEntry>(cfg1);
    auto s = master->startup(cfg1);
    INVARIANT(s.ok());

    auto slave = std::make_shared<ServerEntry>(cfg2);
    s = slave->startup(cfg2);
    INVARIANT(s.ok());

    LOG(INFO) << ">>>>>> slaveof begin.";
    runCmd(slave, {"slaveof", "127.0.0.1", std::to_string(master_port)});
    std::this_thread::sleep_for(std::chrono::seconds(5));
    LOG(INFO) << ">>>>>> slaveof end.";
    LOG(INFO) << ">>>>>> master add data begin.";

    auto thread = std::thread([this, master]() { testKV(master); });
    std::this_thread::sleep_for(std::chrono::seconds(1));

    asio::io_context ioContext;
    asio::ip::tcp::socket socket(ioContext);
    NetSession sess(slave, std::move(socket), 1, false, nullptr, nullptr);
    sess.setArgs({"set", "test", "1", "nx"});
    auto expect = Command::runSessionCmd(&sess);
    /* NOTE(wayenchen) only master client could write,others should fail*/
    EXPECT_FALSE(expect.ok());
    DLOG(INFO) << "write to slave should fail" << expect.status().toString();

    thread.join();

    std::this_thread::sleep_for(std::chrono::seconds(10));
    compareData(master, slave, false);

    auto thread2 = std::thread([this, master]() { testSet(master); });

    sess.setArgs({"sadd", "settestkey", "one", "two", "three"});
    expect = Command::runSessionCmd(&sess);
    EXPECT_FALSE(expect.ok());

    DLOG(INFO) << "write to slave should fail" << expect.status().toString();
    thread2.join();
    std::this_thread::sleep_for(std::chrono::seconds(10));
    compareData(master, slave, false);

    auto thread3 = std::thread([this, master]() { testHash1(master); });
    std::this_thread::sleep_for(std::chrono::seconds(1));
    for (uint32_t i = 0; i < 100; i++) {
      sess.setArgs({"hset", "testHash", std::to_string(i), std::to_string(i)});
      auto expect = Command::runSessionCmd(&sess);

      EXPECT_FALSE(expect.ok());
      DLOG(INFO) << "write to slave should fail" << expect.status().toString();
    }

    thread3.join();
    std::this_thread::sleep_for(std::chrono::seconds(20));
    compareData(master, slave, false);
    LOG(INFO) << ">>>>>> compareData end.";

#ifndef _WIN32
    master->stop();
    slave->stop();
    ASSERT_EQ(slave.use_count(), 1);
#endif
  }
}

void checkBinlogKeepNum(std::shared_ptr<ServerEntry> svr, uint32_t num) {
  auto ctx = std::make_shared<asio::io_context>();
  auto session = makeSession(svr, ctx);
  for (size_t i = 0; i < svr->getKVStoreCount(); i++) {
    session->setArgs({"binlogpos", to_string(i)});
    auto expect = Command::runSessionCmd(session.get());
    uint64_t binlogpos =
      Command::getInt64FromFmtLongLong(expect.value()).value();

    session->setArgs({"binlogstart", to_string(i)});
    expect = Command::runSessionCmd(session.get());
    uint64_t binlogstart =
      Command::getInt64FromFmtLongLong(expect.value()).value();
    LOG(INFO) << "checkBinlogKeepNum, port:" << svr->getParams()->port
              << " store:" << i << " binlogpos:" << binlogpos
              << " binlogstart:" << binlogstart << " num:" << num;
    // if data not full, binlogpos-binlogstart may be smaller than num.
    if (svr->getParams()->binlogDelRange == 1 ||
        svr->getParams()->binlogDelRange == 0) {
      EXPECT_EQ(binlogpos - binlogstart + 1, num);
    } else {
      EXPECT_LT(binlogpos - binlogstart + 1,
                num + svr->getParams()->binlogDelRange);
    }
  }
}

TEST(Repl, BinlogKeepNum_Test) {
  size_t i = 0;
  {
    for (int j = 0; j < 2; j++) {
      LOG(INFO) << ">>>>>> test store count:" << i;
      const auto guard = MakeGuard([] {
        destroyEnv(master_dir);
        destroyEnv(slave_dir);
        destroyEnv(slave1_dir);
        destroyEnv(single_dir);
        std::this_thread::sleep_for(std::chrono::seconds(5));
      });

      EXPECT_TRUE(setupEnv(master_dir));
      EXPECT_TRUE(setupEnv(slave_dir));
      EXPECT_TRUE(setupEnv(slave1_dir));
      EXPECT_TRUE(setupEnv(single_dir));

      auto cfg1 = makeServerParam(master_port, i, master_dir, false);
      auto cfg2 = makeServerParam(slave_port, i, slave_dir, false);
      auto cfg3 = makeServerParam(slave1_port, i, slave1_dir, false);
      auto cfg4 = makeServerParam(single_port, i, single_dir, false);
      uint64_t masterBinlogNum = 10;
      cfg1->maxBinlogKeepNum = masterBinlogNum;
      cfg1->minBinlogKeepSec = 0;
      cfg2->maxBinlogKeepNum = masterBinlogNum;
      cfg2->minBinlogKeepSec = 0;
      cfg2->slaveBinlogKeepNum = 1;
      cfg3->slaveBinlogKeepNum = 1;
      cfg3->minBinlogKeepSec = 0;
      cfg4->maxBinlogKeepNum = masterBinlogNum;
      cfg4->minBinlogKeepSec = 0;
      if (j == 1) {
        cfg1->binlogDelRange = 5000;
        cfg2->binlogDelRange = 5000;
        cfg3->binlogDelRange = 5000;
        cfg4->binlogDelRange = 5000;
        cfg1->compactRangeAfterDeleteRange = true;
      }

      auto master = std::make_shared<ServerEntry>(cfg1);
      auto s = master->startup(cfg1);
      INVARIANT(s.ok());

      auto slave = std::make_shared<ServerEntry>(cfg2);
      s = slave->startup(cfg2);
      INVARIANT(s.ok());

      auto slave1 = std::make_shared<ServerEntry>(cfg3);
      s = slave1->startup(cfg3);
      INVARIANT(s.ok());

      auto single = std::make_shared<ServerEntry>(cfg4);
      s = single->startup(cfg4);
      INVARIANT(s.ok());

      runCmd(slave, {"slaveof", "127.0.0.1", std::to_string(master_port)});
      // slaveof need about 3 seconds to transfer file.
      std::this_thread::sleep_for(std::chrono::seconds(5));

      auto allKeys = initData(master, recordSize);
      initData(single, recordSize);

      waitSlaveCatchup(master, slave);
      sleep(3);  // wait recycle binlog

      LOG(INFO) << ">>>>>> checkBinlogKeepNum begin.";
      checkBinlogKeepNum(master, masterBinlogNum);
      checkBinlogKeepNum(slave, 1);
      checkBinlogKeepNum(single, masterBinlogNum);


      LOG(INFO) << ">>>>>> master add data begin.";
      auto thread = std::thread([this, master]() {
        testAll(master);  // need about 40 seconds
      });
      uint32_t sleep_time = genRand() % 20 + 10;  // 10-30 seconds
      sleep(sleep_time);

      LOG(INFO) << ">>>>>> slaveof begin.";
      runCmd(slave1, {"slaveof", "127.0.0.1", std::to_string(slave_port)});
      // slaveof need about 3 seconds to transfer file.
      std::this_thread::sleep_for(std::chrono::seconds(5));

      thread.join();
      initData(master, recordSize);  // add data every store
      waitSlaveCatchup(master, slave);
      waitSlaveCatchup(slave, slave1);
      sleep(3);  // wait recycle binlog
      LOG(INFO) << ">>>>>> checkBinlogKeepNum begin.";
      checkBinlogKeepNum(master, masterBinlogNum);
      checkBinlogKeepNum(slave, masterBinlogNum);
      checkBinlogKeepNum(slave1, 1);
      LOG(INFO) << ">>>>>> checkBinlogKeepNum end.";

#ifndef _WIN32
      master->stop();
      slave->stop();
      slave1->stop();
      single->stop();

      ASSERT_EQ(slave.use_count(), 1);
      ASSERT_EQ(master.use_count(), 1);
      ASSERT_EQ(slave1.use_count(), 1);
      ASSERT_EQ(single.use_count(), 1);
#endif

      LOG(INFO) << ">>>>>> test store count:" << i << " end;";
    }
  }
}

Status scan(const std::string& logfile) {
  FILE* pf = fopen(logfile.c_str(), "r");
  if (pf == NULL) {
    return {ErrorCodes::ERR_INTERNAL, "fopen failed:" + logfile};
  }
  const uint32_t kBuffLen = 4096;
  char buff[kBuffLen];
  uint64_t id = 0;

  int ret = fread(buff, BINLOG_HEADER_V2_LEN, 1, pf);
  if (ret != 1 || strstr(buff, BINLOG_HEADER_V2) != buff) {
    cerr << "read head failed." << endl;
    fclose(pf);
    return {ErrorCodes::ERR_INTERNAL, "read head failed."};
  }
  buff[BINLOG_HEADER_V2_LEN] = '\0';
  uint32_t storeId =
    be32toh(*reinterpret_cast<uint32_t*>(buff + strlen(BINLOG_HEADER_V2)));
  EXPECT_EQ(storeId, 0);

  while (!feof(pf)) {
    // keylen
    uint32_t keylen = 0;
    ret = fread(buff, sizeof(uint32_t), 1, pf);
    if (ret != 1) {
      if (feof(pf)) {
        fclose(pf);
        return {ErrorCodes::ERR_OK, ""};  // read file end.
      }
      fclose(pf);
      return {ErrorCodes::ERR_INTERNAL, "read keylen failed."};
    }
    buff[sizeof(uint32_t)] = '\0';
    keylen = int32Decode(buff);

    // key
    string key;
    key.resize(keylen);
    ret = fread(const_cast<char*>(key.c_str()), keylen, 1, pf);
    if (ret != 1) {
      fclose(pf);
      return {ErrorCodes::ERR_INTERNAL, "read key failed."};
    }

    // valuelen
    uint32_t valuelen = 0;
    ret = fread(buff, sizeof(uint32_t), 1, pf);
    if (ret != 1) {
      fclose(pf);
      return {ErrorCodes::ERR_INTERNAL, "read valuelen failed."};
    }
    buff[sizeof(uint32_t)] = '\0';
    valuelen = int32Decode(buff);

    // value
    string value;
    value.resize(valuelen);
    ret = fread(const_cast<char*>(value.c_str()), valuelen, 1, pf);
    if (ret != 1) {
      fclose(pf);
      return {ErrorCodes::ERR_INTERNAL, "read value failed."};
    }

    Expected<ReplLogKeyV2> logkey = ReplLogKeyV2::decode(key);
    if (!logkey.ok()) {
      fclose(pf);
      return {ErrorCodes::ERR_INTERNAL, "decode logkey failed."};
    }

    Expected<ReplLogValueV2> logValue = ReplLogValueV2::decode(value);
    if (!logValue.ok()) {
      fclose(pf);
      return {ErrorCodes::ERR_INTERNAL, "decode logvalue failed."};
    }

    if (id == 0) {
      id = logkey.value().getBinlogId() + 1;
      continue;
    }

    if (id != logkey.value().getBinlogId() &&
        logValue.value().getCmd() == "flushalldisk") {
      LOG(INFO) << "flushalldisk reset id, id:" << id
                << " logkey:" << logkey.value().getBinlogId();
      id = logkey.value().getBinlogId();
    }

    if (id != logkey.value().getBinlogId()) {
      LOG(ERROR) << "id:" << id << " logkey:" << logkey.value().getBinlogId()
                 << " cmd:" << logValue.value().getCmd();
      fclose(pf);
      return {ErrorCodes::ERR_INTERNAL, "binlogId error."};
    }

    id++;
  }

  fclose(pf);
  return {ErrorCodes::ERR_OK, ""};
}

TEST(Repl, coreDumpWhenSaveBinlog) {
  size_t i = 0;
  {
    LOG(INFO) << ">>>>>> test store count:" << i;
    bool clear = false;  // dont clear if failed
    const auto guard = MakeGuard([clear] {
      if (clear) {
        destroyEnv(single_dir2);
      }
      std::this_thread::sleep_for(std::chrono::seconds(5));
    });

    EXPECT_TRUE(setupEnv(single_dir2));

    auto cfg = makeServerParam(single_port, i, single_dir2, false);
    uint64_t masterBinlogNum = 10;
    cfg->maxBinlogKeepNum = masterBinlogNum;
    cfg->minBinlogKeepSec = 0;
    cfg->binlogDelRange = 5000;

    {
      auto single = std::make_shared<ServerEntry>(cfg);
      auto s = single->startup(cfg);
      INVARIANT(s.ok());

      initData(single, recordSize);

      single->stop();

      sleep(2);
      LOG(INFO) << ">>>>>> scanDumpFile begin.";
      std::string subpath = "./" + string(single_dir2) + "/dump/0/";
      try {
        for (auto& p : filesystem::recursive_directory_iterator(subpath)) {
          const filesystem::path& path = p.path();
          if (!filesystem::is_regular_file(p)) {
            LOG(INFO) << "maxDumpFileSeq ignore:" << p.path();
            continue;
          }
          // assert path with dir prefix
          INVARIANT(path.string().find(subpath) == 0);
          std::string relative = path.string().erase(0, subpath.size());
          if (relative.substr(0, 6) != "binlog") {
            LOG(INFO) << "maxDumpFileSeq ignore:" << relative;
            continue;
          }
          LOG(INFO) << "scan begin, path:" << path;
          auto s = scan(path.string());
          if (!s.ok()) {
            LOG(ERROR) << "scan failed:" << s.toString();
          }
          EXPECT_TRUE(s.ok());
        }
      } catch (const std::exception& ex) {
        LOG(ERROR) << " get fileno failed:" << ex.what();
        EXPECT_TRUE(0);
        return;
      }
    }
    sleep(3);

    auto single = std::make_shared<ServerEntry>(cfg);
    auto s = single->startup(cfg);
    INVARIANT(s.ok());

    sleep(3);

    LOG(INFO) << ">>>>>> checkBinlogKeepNum begin.";
    checkBinlogKeepNum(single, masterBinlogNum);

#ifndef _WIN32
    single->stop();

    ASSERT_EQ(single.use_count(), 1);
#endif

    sleep(2);
    LOG(INFO) << ">>>>>> scanDumpFile begin.";
    std::string subpath = "./" + string(single_dir2) + "/dump/0/";
    try {
      for (auto& p : filesystem::recursive_directory_iterator(subpath)) {
        const filesystem::path& path = p.path();
        if (!filesystem::is_regular_file(p)) {
          LOG(INFO) << "maxDumpFileSeq ignore:" << p.path();
          continue;
        }
        // assert path with dir prefix
        INVARIANT(path.string().find(subpath) == 0);
        std::string relative = path.string().erase(0, subpath.size());
        if (relative.substr(0, 6) != "binlog") {
          LOG(INFO) << "maxDumpFileSeq ignore:" << relative;
          continue;
        }
        LOG(INFO) << "scan begin, path:" << path;
        auto s = scan(path.string());
        if (!s.ok()) {
          LOG(ERROR) << "scan failed:" << s.toString()
                     << " file:" << path.string();
        }
        EXPECT_TRUE(s.ok());
      }
    } catch (const std::exception& ex) {
      LOG(ERROR) << " get fileno failed:" << ex.what();
      EXPECT_TRUE(0);
      return;
    }
    clear = true;
    LOG(INFO) << ">>>>>> test store count:" << i << " end;";
  }
}

TEST(Repl, BinlogVersion) {
  const auto guard = MakeGuard([] {
    destroyEnv(master_dir);
    destroyEnv(slave_dir);
    destroyEnv(slave1_dir);
    std::this_thread::sleep_for(std::chrono::seconds(5));
  });

  EXPECT_TRUE(setupEnv(master_dir));
  uint32_t kvstoreNum = 1;
  auto cfg = makeServerParam(master_port, kvstoreNum, master_dir, false);
  cfg->binlogUsingDefaultCF = true;
  auto version1_master = std::make_shared<ServerEntry>(cfg);
  auto s = version1_master->startup(cfg);
  INVARIANT(s.ok());
  initData(version1_master, recordSize);
  EXPECT_EQ(version1_master->getCatalog()->getBinlogVersion(),
            BinlogVersion::BINLOG_VERSION_1);

  // 1. check version2 slaveof version1
  EXPECT_TRUE(setupEnv(slave_dir));
  auto cfg2 = makeServerParam(slave_port, kvstoreNum, slave_dir, false);
  cfg2->binlogUsingDefaultCF = false;
  auto version2_slave = std::make_shared<ServerEntry>(cfg2);
  s = version2_slave->startup(cfg2);
  INVARIANT(s.ok());
  EXPECT_EQ(version2_slave->getCatalog()->getBinlogVersion(),
            BinlogVersion::BINLOG_VERSION_2);

  {
    auto ctx = std::make_shared<asio::io_context>();
    auto session = makeSession(version2_slave, ctx);

    WorkLoad work(version2_slave, session);
    work.init();
    work.slaveof("127.0.0.1", master_port);
  }

  waitSlaveCatchup(version1_master, version2_slave);
  sleep(3);  // wait recycle binlog
  EXPECT_EQ(version2_slave->getCatalog()->getBinlogVersion(),
            BinlogVersion::BINLOG_VERSION_2);
  compareData(version1_master, version2_slave, false);

  auto binlogHighest = version1_master->getStores()[0]->getHighestBinlogId();
  auto binlogMax = version1_master->getStores()[0]->getNextBinlogSeq();
  version1_master->stop();
  ASSERT_EQ(version1_master.use_count(), 1);

#ifndef _WIN32
  version2_slave->stop();
  ASSERT_EQ(version2_slave.use_count(), 1);
#endif

  // 2. check version2 startup with version1 datafile
  cfg->binlogUsingDefaultCF = false;
  auto version2_master = std::make_shared<ServerEntry>(cfg);
  s = version2_master->startup(cfg);
  INVARIANT(s.ok());
  EXPECT_EQ(version2_master->getCatalog()->getBinlogVersion(),
            BinlogVersion::BINLOG_VERSION_2);
  EXPECT_EQ(version2_master->getStores()[0]->getHighestBinlogId(),
            binlogHighest);
  EXPECT_EQ(version2_master->getStores()[0]->getNextBinlogSeq(), binlogMax);
  initData(version2_master, recordSize);

  // 3. check version2 slaveof version2
  EXPECT_TRUE(setupEnv(slave1_dir));
  auto cfg3 = makeServerParam(slave1_port, kvstoreNum, slave1_dir, false);
  cfg3->binlogUsingDefaultCF = false;
  auto version2_slave2 = std::make_shared<ServerEntry>(cfg3);
  s = version2_slave2->startup(cfg3);
  INVARIANT(s.ok());
  EXPECT_EQ(version2_slave2->getCatalog()->getBinlogVersion(),
            BinlogVersion::BINLOG_VERSION_2);

  {
    auto ctx = std::make_shared<asio::io_context>();
    auto session = makeSession(version2_slave2, ctx);

    WorkLoad work(version2_slave2, session);
    work.init();
    work.slaveof("127.0.0.1", master_port);
  }

  waitSlaveCatchup(version2_master, version2_slave2);
  sleep(3);  // wait recycle binlog
  EXPECT_EQ(version2_slave2->getCatalog()->getBinlogVersion(),
            BinlogVersion::BINLOG_VERSION_2);
  compareData(version2_master, version2_slave2, false);

  version2_master->stop();
  ASSERT_EQ(version2_master.use_count(), 1);

  version2_slave2->stop();
  ASSERT_EQ(version2_slave2.use_count(), 1);
}

}  // namespace tendisplus
