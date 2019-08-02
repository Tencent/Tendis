// Copyright [2019] <takenliu>
#include <stdlib.h>
#include <memory>
#include <utility>
#include <thread>
#include <string>
#include <vector>
#include <algorithm>
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

AllKeys initData(std::shared_ptr<ServerEntry>& server,
                uint32_t count, const char* key_suffix) {
    auto ctx1 = std::make_shared<asio::io_context>();
    auto sess1 = makeSession(server, ctx1);
    WorkLoad work(server, sess1);
    work.init();

    AllKeys all_keys;

    auto kv_keys = work.writeWork(RecordType::RT_KV, count,
        0, true, key_suffix);
    all_keys.emplace_back(kv_keys);

    auto list_keys = work.writeWork(RecordType::RT_LIST_META, count,
        2, true, key_suffix);
    all_keys.emplace_back(list_keys);

    auto hash_keys = work.writeWork(RecordType::RT_HASH_META, count,
        2, true, key_suffix);
    all_keys.emplace_back(hash_keys);

    auto set_keys = work.writeWork(RecordType::RT_SET_META, count,
        2, true, key_suffix);
    all_keys.emplace_back(set_keys);

    auto zset_keys = work.writeWork(RecordType::RT_ZSET_META, count,
        2, true, key_suffix);
    all_keys.emplace_back(zset_keys);

    return all_keys;
}

AllKeys initKvData(const std::shared_ptr<ServerEntry>& server,
                uint32_t count, const char* key_suffix) {
    auto ctx1 = std::make_shared<asio::io_context>();
    auto sess1 = makeSession(server, ctx1);
    WorkLoad work(server, sess1);
    work.init();

    AllKeys all_keys;

    auto kv_keys = work.writeWork(
        RecordType::RT_KV, count, 0, true, key_suffix);
    all_keys.emplace_back(kv_keys);

    return std::move(all_keys);
}

void addOneKeyEveryKvstore(const std::shared_ptr<ServerEntry>& server,
        const char* key) {
    for (size_t i = 0; i < server->getKVStoreCount(); i++) {
        auto kvstore = server->getStores()[i];
        auto eTxn1 = kvstore->createTransaction(nullptr);
        EXPECT_EQ(eTxn1.ok(), true);
        std::unique_ptr<Transaction> txn1 = std::move(eTxn1.value());

        Status s = kvstore->setKV(
            Record(
                RecordKey(0, 0, RecordType::RT_KV, key, ""),
                RecordValue("txn1", RecordType::RT_KV, -1)),
            txn1.get());
        EXPECT_EQ(s.ok(), true);

        Expected<uint64_t> exptCommitId = txn1->commit();
        EXPECT_TRUE(exptCommitId.ok());
    }
}

void backup(const std::shared_ptr<ServerEntry>& server) {
    auto ctx = std::make_shared<asio::io_context>();
    auto sess = makeSession(server, ctx);

    // clear data
    const char* dir = "./back_test";
    if (filesystem::exists(dir)) {
        filesystem::remove_all(dir);
    }
    filesystem::create_directory(dir);

    std::vector<std::string> args;
    args.push_back("backup");
    args.push_back(dir);
    sess->setArgs(args);
    auto expect = Command::runSessionCmd(sess.get());
    EXPECT_TRUE(expect.ok());
}

void restoreBackup(const std::shared_ptr<ServerEntry>& server) {
    auto ctx = std::make_shared<asio::io_context>();
    auto sess = makeSession(server, ctx);

    std::vector<std::string> args;
    args.push_back("restorebackup");
    args.push_back("all");
    args.push_back("./back_test");
    sess->setArgs(args);
    auto expect = Command::runSessionCmd(sess.get());
    EXPECT_TRUE(expect.ok());
}

void flushBinlog(const std::shared_ptr<ServerEntry>& server) {
    auto ctx = std::make_shared<asio::io_context>();
    auto sess = makeSession(server, ctx);

    for (size_t i = 0; i < server->getKVStoreCount(); i++) {
        std::vector<std::string> args;
        args.push_back("binlogflush");
        args.push_back(std::to_string(i));
        sess->setArgs(args);
        auto expect = Command::runSessionCmd(sess.get());
        EXPECT_TRUE(expect.ok());
    }
}

void restoreBinlog(const std::shared_ptr<ServerEntry>& server,
    uint64_t end_ts = UINT64_MAX) {
    for (size_t i = 0; i < server->getKVStoreCount(); i++) {
        auto kvstore = server->getStores()[i];
        uint64_t binglogPos = kvstore->getHighestBinlogId();

        std::string subpath = "./master1/dump/" + std::to_string(i) + "/";
        std::vector<std::string> loglist;
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
            LOG(INFO) << "binlog file:" << p.path();
            loglist.push_back(p.path());
        }
        if (loglist.size() <= 0) {
            continue;
        }
        std::sort(loglist.begin(), loglist.end());
        for (size_t j = 0; j < loglist.size(); ++j) {
            std::string cmd = "./binlog_tool";
            cmd += " --logfile=" + loglist[j];
            cmd += " --mode=base64";
            cmd += " --start-position=" + std::to_string(binglogPos);
            cmd += " --end-datetime=" + std::to_string(end_ts);
            cmd += "| ../../../redis-2.8.17/src/redis-cli -p 1122";
            LOG(INFO) << cmd;
            int ret = system(cmd.c_str());
            EXPECT_EQ(ret, 0);
        }
    }
}

void waitBinlogDump(const std::shared_ptr<ServerEntry>& server) {
    for (size_t i = 0; i < server->getKVStoreCount(); i++) {
        auto kvstore = server->getStores()[i];

        while (true) {
            auto ptxn = kvstore->createTransaction(nullptr);
            EXPECT_TRUE(ptxn.ok());
            std::unique_ptr<Transaction> txn = std::move(ptxn.value());
            uint64_t minBinlogId = 0;
            auto expBinlogidMin = RepllogCursorV2::getMinBinlogId(txn.get());
            if (expBinlogidMin.status().code() == ErrorCodes::ERR_EXHAUST) {
                minBinlogId = 0;
            } else {
                EXPECT_TRUE(expBinlogidMin.ok());
                minBinlogId = expBinlogidMin.value();
            }

            uint64_t maxBinlogId = 0;
            auto expBinlogidMax = RepllogCursorV2::getMaxBinlogId(txn.get());
            if (expBinlogidMax.status().code() == ErrorCodes::ERR_EXHAUST) {
                maxBinlogId = 0;
            } else {
                EXPECT_TRUE(expBinlogidMin.ok());
                maxBinlogId = expBinlogidMax.value();
            }

            // wait only one binlog left in rocksdb
            if (minBinlogId == maxBinlogId) {
                break;
            } else {
                usleep(10000);  // 10ms
            }
        }
    }
}
void compareData(const std::shared_ptr<ServerEntry>& master,
    const std::shared_ptr<ServerEntry>& slave, bool com_binlog = true) {
    INVARIANT(master->getKVStoreCount() == slave->getKVStoreCount());

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
        auto cursor1 = txn1->createCursor();
        cursor1->seek("");
        while (true) {
            Expected<Record> exptRcd1 = cursor1->next();
            if (exptRcd1.status().code() == ErrorCodes::ERR_EXHAUST) {
                break;
            }
            if (!com_binlog && exptRcd1.value().getRecordKey().getRecordType()
                == RecordType::RT_BINLOG) {
                continue;
            }
            INVARIANT(exptRcd1.ok());
            count1++;

            // check the binlog together
            auto exptRcdv2 = kvstore2->getKV(
                exptRcd1.value().getRecordKey(), txn2.get());
            EXPECT_TRUE(exptRcdv2.ok());
            EXPECT_EQ(exptRcd1.value().getRecordValue(), exptRcdv2.value());
        }

        auto cursor2 = txn2->createCursor();
        cursor2->seek("");
        while (true) {
            Expected<Record> exptRcd2 = cursor2->next();
            if (exptRcd2.status().code() == ErrorCodes::ERR_EXHAUST) {
                break;
            }
            if (!com_binlog && exptRcd2.value().getRecordKey().getRecordType()
                == RecordType::RT_BINLOG) {
                continue;
            }

            INVARIANT(exptRcd2.ok());
            count2++;
        }

        EXPECT_EQ(count1, count2);
        LOG(INFO) << "compare data: store " << i << " record count " << count1;
    }
}

void compareAllowNotFound(const std::shared_ptr<ServerEntry>& master,
    const std::shared_ptr<ServerEntry>& slave) {
    INVARIANT(master->getKVStoreCount() == slave->getKVStoreCount());

    for (size_t i = 0; i < master->getKVStoreCount(); i++) {
        uint64_t count1 = 0;
        uint64_t count2 = 0;
        uint64_t notFoundNum = 0;
        auto kvstore1 = master->getStores()[i];
        auto kvstore2 = slave->getStores()[i];

        auto ptxn2 = kvstore2->createTransaction(nullptr);
        EXPECT_TRUE(ptxn2.ok());
        std::unique_ptr<Transaction> txn2 = std::move(ptxn2.value());

        auto ptxn1 = kvstore1->createTransaction(nullptr);
        EXPECT_TRUE(ptxn1.ok());
        std::unique_ptr<Transaction> txn1 = std::move(ptxn1.value());
        auto cursor1 = txn1->createCursor();
        cursor1->seek("");
        while (true) {
            Expected<Record> exptRcd1 = cursor1->next();
            if (exptRcd1.status().code() == ErrorCodes::ERR_EXHAUST) {
                break;
            }
            INVARIANT(exptRcd1.ok());
            count1++;

            // check the binlog together
            auto exptRcdv2 = kvstore2->getKV(
                exptRcd1.value().getRecordKey(), txn2.get());
            if (exptRcdv2.ok()) {
                EXPECT_EQ(exptRcd1.value().getRecordValue(), exptRcdv2.value());
            } else {
                notFoundNum++;
            }
        }

        auto cursor2 = txn2->createCursor();
        cursor2->seek("");
        while (true) {
            Expected<Record> exptRcd2 = cursor2->next();
            if (exptRcd2.status().code() == ErrorCodes::ERR_EXHAUST) {
                break;
            }
            INVARIANT(exptRcd2.ok());
            count2++;
        }
        if (count1 == 0) {
            EXPECT_EQ(count1, count2);
        } else {
            if (count1 != count2) {
                if (count1 == 2) {
                    // master2 will dont has datakey and binlogkey.
                    EXPECT_EQ(count2, 0);
                } else {
                    // master2 datakey num will be one less, binlogkey num be the same.
                    EXPECT_EQ(count1, count2 + 1);
                }
                // the last datakey and the binlogkey will cant found.
                EXPECT_EQ(notFoundNum, 2);
            }
        }
        LOG(INFO) << "compare data: store " << i
            << " count1:" << count1 << " count2:" << count2
            << " notFoundNum:" << notFoundNum;
    }
}

std::vector<uint32_t> getKeyNum(const std::shared_ptr<ServerEntry>& master) {
    std::vector<uint32_t> keyNum;
    for (size_t i = 0; i < master->getKVStoreCount(); i++) {
        uint64_t count = 0;
        auto kvstore = master->getStores()[i];

        auto ptxn = kvstore->createTransaction(nullptr);
        EXPECT_TRUE(ptxn.ok());
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        auto cursor = txn->createCursor();
        cursor->seek("");
        while (true) {
            Expected<Record> exptRcd = cursor->next();
            if (exptRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
                break;
            }
            INVARIANT(exptRcd.ok());
            count++;
        }
        keyNum.push_back(count);
    }
    return keyNum;
}

void checkNumAllowDiff(std::vector<uint32_t> nums1,
    std::vector<uint32_t> nums2) {
    EXPECT_EQ(nums1.size(), nums2.size());
    for (size_t i = 0; i < nums1.size(); ++i) {
        // if 2nd time not add, num will be equal
        if (nums1[i] != nums2[i]) {
            // master has one datakey less.
            // if only store one key, master2 has no datakey and binlogkey, so be 2 num less.
            EXPECT_TRUE(nums1[i] == nums2[i] + 1 || nums1[i] == nums2[i] + 2);
        }
    }
}

std::pair<std::shared_ptr<ServerEntry>, std::shared_ptr<ServerEntry>>
makeRestoreEnv(uint32_t storeCnt) {
    EXPECT_TRUE(setupEnv("master1"));
    EXPECT_TRUE(setupEnv("master2"));

    auto cfg1 = makeServerParam(1121, storeCnt, "master1");
    auto cfg2 = makeServerParam(1122, storeCnt, "master2");
    cfg1->maxBinlogKeepNum = 1;
    cfg2->maxBinlogKeepNum = 1;

    auto master1 = std::make_shared<ServerEntry>(cfg1);
    auto s = master1->startup(cfg1);
    INVARIANT(s.ok());

    auto master2 = std::make_shared<ServerEntry>(cfg2);
    s = master2->startup(cfg2);
    return std::make_pair(master1, master2);
}


#ifdef _WIN32
size_t recordSize = 10;
#else
size_t recordSize = 300;
#endif

TEST(Restore, Common) {
#ifdef _WIN32
    size_t i = 1;
    {
#else
    for (size_t i = 0; i < 9; i++) {
#endif
        LOG(INFO) << ">>>>>> test store count:" << i;

        const auto guard = MakeGuard([] {
                destroyEnv("master1");
                destroyEnv("master2");
                std::this_thread::sleep_for(std::chrono::seconds(1));
                });

        auto hosts = makeRestoreEnv(i);
        auto& master1 = hosts.first;
        auto& master2 = hosts.second;

        auto allKeys1 = initData(master1, recordSize, "suffix1");
        LOG(INFO) << ">>>>>> master1 initData 1st end;";
        backup(master1);
        restoreBackup(master2);
        LOG(INFO) << ">>>>>> master2 restoreBackup end;";
        // compareData(highest1, highest2);
        compareData(master1, master2);  // compare data + binlog
        LOG(INFO) << ">>>>>> compareData 1st end;";


        uint32_t part1_num = std::rand() % recordSize;
        part1_num = part1_num == 0 ? 1 : part1_num;
        uint32_t part2_num = recordSize - part1_num;
        // add kv only
        auto partKeys2 = initKvData(master1, part1_num, "suffix21");
        waitBinlogDump(master1);
        std::vector<uint32_t> m1_keynum1 = getKeyNum(master1);
        LOG(INFO) << ">>>>>> master1 initKvData 1st end;";
        uint64_t ts = msSinceEpoch();
        LOG(INFO) << "ms:" << ts;
        sleep(1);  // wait ts changed
        // add kv only
        auto partKeys3 = initKvData(master1, part2_num, "suffix22");
        waitBinlogDump(master1);
        std::vector<uint32_t> m1_keynum2 = getKeyNum(master1);
        LOG(INFO) << ">>>>>> master1 initKvData 2st end;";
        // waitBinlogDump(master1);
        flushBinlog(master1);
        restoreBinlog(master2, ts);
        LOG(INFO) << ">>>>>> master2 restoreBinlog 1st end;";
        waitBinlogDump(master2);
        std::vector<uint32_t> m2_keynum1 = getKeyNum(master2);
        // if a kvstore write some key in the second time,
        // the keynum will equal, otherwise the keynum will be one less
        checkNumAllowDiff(m1_keynum1, m2_keynum1);  // check num only
        flushBinlog(master1);
        restoreBinlog(master2, UINT64_MAX);
        LOG(INFO) << ">>>>>> master2 restoreBinlog 2st end;";
        waitBinlogDump(master2);
        std::vector<uint32_t> m2_keynum2 = getKeyNum(master2);
        checkNumAllowDiff(m1_keynum2, m2_keynum2);  // check num only
        compareAllowNotFound(master1, master2);
        LOG(INFO) << ">>>>>> compareData 2st end;";

        testAll(master1);
        addOneKeyEveryKvstore(master1, "restore_test_key1");
        waitBinlogDump(master1);
        flushBinlog(master1);
        restoreBinlog(master2, UINT64_MAX);
        addOneKeyEveryKvstore(master2, "restore_test_key1");
        waitBinlogDump(master2);
        compareData(master1, master2, false);  // compare data only

        master1->stop();
        master2->stop();
        ASSERT_EQ(master1.use_count(), 1);
        ASSERT_EQ(master2.use_count(), 1);
        LOG(INFO) << ">>>>>> test store count:" << i << " end;";
    }
}

}  // namespace tendisplus

