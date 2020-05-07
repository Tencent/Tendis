#include <fstream>
#include <memory>
#include "gtest/gtest.h"
#include "tendisplus/utils/status.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/server/server_params.h"

namespace tendisplus {

int paramUpdateValue = 0;
void paramOnUpdate() {
    paramUpdateValue  = 1;
};

TEST(ServerParams, Common) {
    std::ofstream myfile;
    myfile.open("a.cfg");
    myfile << "bind 127.0.0.1\n";
    myfile << "port 8903\n";
    myfile << "loglevel debug\n";
    myfile << "logdir ./Log\n";
    myfile.close();
    const auto guard = MakeGuard([] {
        remove("a.cfg");
    });
    auto cfg = std::make_unique<ServerParams>();
    auto s = cfg->parseFile("a.cfg");
    EXPECT_EQ(s.ok(), true) << s.toString();
    EXPECT_EQ(cfg->bindIp, "127.0.0.1");
    EXPECT_EQ(cfg->port, 8903);
    EXPECT_EQ(cfg->logLevel, "debug");
    EXPECT_EQ(cfg->logDir, "./Log");

    EXPECT_EQ(cfg->setVar("binlogRateLimitMB", "100", NULL), true);
    EXPECT_EQ(cfg->binlogRateLimitMB, 100);
    EXPECT_EQ(cfg->setVar("binlogratelimitmb", "200", NULL), true);
    EXPECT_EQ(cfg->binlogRateLimitMB, 200);
    EXPECT_EQ(cfg->setVar("noargs", "abc", NULL), false);

    EXPECT_EQ(cfg->registerOnupdate("noargs", paramOnUpdate), false);
    EXPECT_EQ(cfg->registerOnupdate("chunkSize", paramOnUpdate), true);
    EXPECT_EQ(cfg->setVar("chunkSize", "300", NULL), true);
    EXPECT_EQ(cfg->chunkSize, 300);
    EXPECT_EQ(paramUpdateValue, 1);

    EXPECT_EQ(cfg->setVar("logLevel", "warNING", NULL), true);
    EXPECT_EQ(cfg->logLevel, "warning");
    EXPECT_EQ(cfg->setVar("logLevel", "nothavelevel", NULL), false);
    EXPECT_EQ(cfg->logLevel, "warning");

    EXPECT_EQ(cfg->setVar("rocks.compress_type", "None", NULL), true);
    EXPECT_EQ(cfg->rocksCompressType, "none");
    EXPECT_EQ(cfg->setVar("rocks.compress_type", "nothavelevel", NULL), false);
    EXPECT_EQ(cfg->rocksCompressType, "none");

    EXPECT_EQ(cfg->setVar("logDir", "\"./\"", NULL), true);
    EXPECT_EQ(cfg->logDir, "./");
    EXPECT_EQ(cfg->setVar("logDir", "\"./", NULL), true);
    EXPECT_EQ(cfg->logDir, "\"./");

    EXPECT_EQ(cfg->setVar("kvStoreCount", "12abc", NULL), true);
    EXPECT_EQ(cfg->kvStoreCount, 12);
    EXPECT_EQ(cfg->setVar("kvStoreCount", "aa12abc", NULL), false);

    float testFloat;
    FloatVar testFloatVar("testFloatVar", &testFloat, NULL, NULL, true);
    EXPECT_EQ(testFloatVar.setVar("1.5"), true);
    EXPECT_EQ(testFloat, 1.5);
    EXPECT_EQ(testFloatVar.setVar("abc2.5abc"), false);
    EXPECT_EQ(testFloat, 1.5);
}

TEST(ServerParams, Include) {
    std::ofstream myfile;

    myfile.open("gtest_serverparams_include1.cfg");
    myfile << "bind 127.0.0.1\n";
    myfile << "port 8903\n";
    myfile << "include gtest_serverparams_include2.cfg\n";
    myfile << "loglevel debug\n";
    myfile << "logdir ./\n";
    myfile.close();

    myfile.open("gtest_serverparams_include2.cfg");
    myfile << "chunkSize 200\n";
    myfile.close();

    const auto guard = MakeGuard([] {
        remove("gtest_serverparams_include1.cfg");
        remove("gtest_serverparams_include2.cfg");
    });
    auto cfg = std::make_unique<ServerParams>();
    auto s = cfg->parseFile("gtest_serverparams_include1.cfg");
    EXPECT_EQ(s.ok(), true) << s.toString();
    EXPECT_EQ(cfg->bindIp, "127.0.0.1");
    EXPECT_EQ(cfg->port, 8903);
    EXPECT_EQ(cfg->logLevel, "debug");
    EXPECT_EQ(cfg->logDir, "./");
    EXPECT_EQ(cfg->chunkSize, 200);
    EXPECT_EQ(cfg->getConfFile(), "gtest_serverparams_include1.cfg");
}

TEST(ServerParams, IncludeRecycle) {
    std::ofstream myfile;

    myfile.open("gtest_serverparams_includerecycle1.cfg");
    myfile << "include gtest_serverparams_includerecycle2.cfg\n";
    myfile.close();

    myfile.open("gtest_serverparams_includerecycle2.cfg");
    myfile << "include gtest_serverparams_includerecycle1.cfg\n";
    myfile.close();

    const auto guard = MakeGuard([] {
        remove("gtest_serverparams_includerecycle1.cfg");
        remove("gtest_serverparams_includerecycle2.cfg");
    });
    auto cfg = std::make_unique<ServerParams>();
    auto s = cfg->parseFile("gtest_serverparams_includerecycle1.cfg");
    EXPECT_EQ(s.ok(), false ) << s.toString();
    EXPECT_EQ(s.toString(), "-ERR include has recycle!\r\n");
}

TEST(ServerParams, DynamicSet) {
    std::ofstream myfile;
    myfile.open("gtest_serverparams_dynamicset.cfg");
    myfile << "port 8903\n";
    myfile << "masterauth testpw\n";
    myfile.close();

    const auto guard = MakeGuard([] {
        remove("gtest_serverparams_dynamicset.cfg");
    });
    auto cfg = std::make_unique<ServerParams>();
    auto s = cfg->parseFile("gtest_serverparams_dynamicset.cfg");
    EXPECT_EQ(s.ok(), true) << s.toString();

    string errinfo;
    EXPECT_EQ(cfg->setVar("port", "8904", &errinfo, false), false);
    EXPECT_EQ(cfg->port, 8903);
    EXPECT_EQ(errinfo, "not allow dynamic set");
    EXPECT_EQ(cfg->setVar("maxBinlogKeepNum", "100", NULL, false), true);
    EXPECT_EQ(cfg->maxBinlogKeepNum, 100);
}

TEST(ServerParams, RocksOption) {
    std::ofstream myfile;
    myfile.open("a.cfg");
    myfile << "bind 127.0.0.1\n";
    myfile << "port 8903\n";
    myfile << "loglevel debug\n";
    myfile << "logdir ./\n";
    myfile << "rocks.disable_wal 1\n";
    myfile << "rocks.wal_dir \"/Abc/dfg\"\n";
    myfile << "rocks.compress_type LZ4\n";
    myfile << "rocks.flush_log_at_trx_commit 1\n";
    myfile << "rocks.blockcache_strict_capacity_limit 1\n";
    myfile << "rocks.max_write_buffer_number 1\n";
    myfile << "rocks.cache_index_and_filter_blocks 1\n";
    myfile.close();
    const auto guard = MakeGuard([] {
        remove("a.cfg");
        });
    auto cfg = std::make_unique<ServerParams>();
    auto s = cfg->parseFile("a.cfg");
    EXPECT_EQ(s.ok(), true) << s.toString();
    EXPECT_EQ(cfg->bindIp, "127.0.0.1");
    EXPECT_EQ(cfg->port, 8903);
    EXPECT_EQ(cfg->logLevel, "debug");
    EXPECT_EQ(cfg->logDir, "./");
    EXPECT_EQ(cfg->rocksDisableWAL, 1);
    EXPECT_EQ(cfg->rocksFlushLogAtTrxCommit, 1);
    EXPECT_EQ(cfg->rocksStrictCapacityLimit, 1);
    EXPECT_EQ(cfg->rocksCompressType, "lz4");
    EXPECT_EQ(cfg->rocksWALDir, "/Abc/dfg");
    EXPECT_TRUE(cfg->getRocksdbOptions().find("max_write_buffer_number") != cfg->getRocksdbOptions().end());
    EXPECT_TRUE(cfg->getRocksdbOptions().find("cache_index_and_filter_blocks") != cfg->getRocksdbOptions().end());
    EXPECT_EQ(cfg->getRocksdbOptions().size(), 2);
}

TEST(ServerParams, DefaultValue) {
    auto cfg = std::make_unique<ServerParams>();
    // NOTO(takenliu): add new param or change default value, please change here.
    //EXPECT_EQ(cfg->paramsNum(), 56);

    EXPECT_EQ(cfg->bindIp, "127.0.0.1");
    EXPECT_EQ(cfg->port, 8903);
    EXPECT_EQ(cfg->logLevel, "");
    EXPECT_EQ(cfg->logDir, "./");

    EXPECT_EQ(cfg->storageEngine, "rocks");
    EXPECT_EQ(cfg->dbPath, "./db");
    EXPECT_EQ(cfg->dumpPath, "./dump");
    EXPECT_EQ(cfg->requirepass, "");
    EXPECT_EQ(cfg->masterauth, "");
    EXPECT_EQ(cfg->pidFile, "./tendisplus.pid");
    EXPECT_EQ(cfg->versionIncrease, true);
    EXPECT_EQ(cfg->generalLog, false);
    EXPECT_EQ(cfg->checkKeyTypeForSet, false);

    EXPECT_EQ(cfg->chunkSize, 0x4000);  // same as rediscluster
    EXPECT_EQ(cfg->kvStoreCount, 10);

    EXPECT_EQ(cfg->scanCntIndexMgr, 1000);
    EXPECT_EQ(cfg->scanJobCntIndexMgr, 1);
    EXPECT_EQ(cfg->delCntIndexMgr, 10000);
    EXPECT_EQ(cfg->delJobCntIndexMgr, 1);
    EXPECT_EQ(cfg->pauseTimeIndexMgr, 10);

    EXPECT_EQ(cfg->protoMaxBulkLen, CONFIG_DEFAULT_PROTO_MAX_BULK_LEN);
    EXPECT_EQ(cfg->dbNum, CONFIG_DEFAULT_DBNUM);

    EXPECT_EQ(cfg->noexpire, false);
    EXPECT_EQ(cfg->maxBinlogKeepNum, 1000000);
    EXPECT_EQ(cfg->minBinlogKeepSec, 0);
    EXPECT_EQ(cfg->slaveBinlogKeepNum, 1);

    EXPECT_EQ(cfg->maxClients, CONFIG_DEFAULT_MAX_CLIENTS);
    EXPECT_EQ(cfg->slowlogPath, "./slowlog");
    EXPECT_EQ(cfg->slowlogLogSlowerThan, CONFIG_DEFAULT_SLOWLOG_LOG_SLOWER_THAN);
    EXPECT_EQ(cfg->slowlogFlushInterval, CONFIG_DEFAULT_SLOWLOG_FLUSH_INTERVAL);
    EXPECT_EQ(cfg->slowlogMaxLen, 128);
    EXPECT_EQ(cfg->netIoThreadNum, 0);
    EXPECT_EQ(cfg->executorThreadNum, 0);

    EXPECT_EQ(cfg->binlogRateLimitMB, 64);
    EXPECT_EQ(cfg->netBatchSize, 1024*1024);
    EXPECT_EQ(cfg->netBatchTimeoutSec, 10);
    EXPECT_EQ(cfg->timeoutSecBinlogWaitRsp, 30);
    EXPECT_EQ(cfg->incrPushThreadnum, 4);
    EXPECT_EQ(cfg->fullPushThreadnum, 4);
    EXPECT_EQ(cfg->fullReceiveThreadnum, 4);
    EXPECT_EQ(cfg->logRecycleThreadnum, 4);
    EXPECT_EQ(cfg->truncateBinlogIntervalMs, 1000);
    EXPECT_EQ(cfg->truncateBinlogNum, 50000);
    EXPECT_EQ(cfg->binlogFileSizeMB, 64);
    EXPECT_EQ(cfg->binlogFileSecs, 20*60);
    EXPECT_EQ(cfg->lockWaitTimeOut, 3600);

    EXPECT_EQ(cfg->rocksBlockcacheMB, 4096);
    EXPECT_EQ(cfg->rocksDisableWAL, false);
    EXPECT_EQ(cfg->rocksFlushLogAtTrxCommit, false);
    EXPECT_EQ(cfg->rocksWALDir, "");
    EXPECT_EQ(cfg->rocksCompressType, "snappy");
    EXPECT_EQ(cfg->rocksStrictCapacityLimit, false);
    EXPECT_EQ(cfg->getRocksdbOptions().size(), 0);
    EXPECT_EQ(cfg->level0Compress, false);
    EXPECT_EQ(cfg->level0Compress, false);

    EXPECT_EQ(cfg->bingLogSendBatch, 256);
    EXPECT_EQ(cfg->bingLogSendBytes, 16 * 1024 * 1024);

    EXPECT_EQ(cfg->migrateSenderThreadnum, 4);
    EXPECT_EQ(cfg->migrateClearThreadnum, 4);
    EXPECT_EQ(cfg->migrateReceiveThreadnum, 4);
    EXPECT_EQ(cfg->migrateCheckThreadnum, 2);
    EXPECT_EQ(cfg->clusterEnabled, false);
    EXPECT_EQ(cfg->migrateTaskSlotsLimit, 10);
    EXPECT_EQ(cfg->migrateDistance, 150);
    EXPECT_EQ(cfg->clusterNodeTimeout, 15000);
    EXPECT_EQ(cfg->clusterRequireFullCoverage, true);
    EXPECT_EQ(cfg->clusterSlaveNoFailover, false);
    EXPECT_EQ(cfg->clusterMigrationBarrier, 1);
    EXPECT_EQ(cfg->clusterSlaveValidityFactor, 50);
}
}  // namespace tendisplus
