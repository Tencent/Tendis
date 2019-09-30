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
    myfile << "logdir ./\n";
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

    EXPECT_EQ(cfg->setVar("kvStoreCount", "12abc", NULL), true);
    EXPECT_EQ(cfg->kvStoreCount, 12);
    EXPECT_EQ(cfg->setVar("kvStoreCount", "aa12abc", NULL), false);

    float testFloat;
    FloatVar testFloatVar("testFloatVar", &testFloat, NULL);
    EXPECT_EQ(testFloatVar.set("1.5"), true);
    EXPECT_EQ(testFloat, 1.5);
    EXPECT_EQ(testFloatVar.set("abc2.5abc"), false);
    EXPECT_EQ(testFloat, 1.5);
}

TEST(ServerParams, DefaultValue) {
    auto cfg = std::make_unique<ServerParams>();
    // NOTO(takenliu): add new param or change default value, please change here.
    EXPECT_EQ(cfg->paramsNum(), 48);

    EXPECT_EQ(cfg->bindIp, "127.0.0.1");
    EXPECT_EQ(cfg->port, 8903);
    EXPECT_EQ(cfg->logLevel, "");
    EXPECT_EQ(cfg->logDir, "./");

    EXPECT_EQ(cfg->storageEngine, "rocks");
    EXPECT_EQ(cfg->dbPath, "./db");
    EXPECT_EQ(cfg->dumpPath, "./dump");
    EXPECT_EQ(cfg-> rocksBlockcacheMB, 4096);
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

    EXPECT_EQ(cfg->maxClients, CONFIG_DEFAULT_MAX_CLIENTS);
    EXPECT_EQ(cfg->slowlogPath, "./slowlog");
    EXPECT_EQ(cfg->slowlogLogSlowerThan, CONFIG_DEFAULT_SLOWLOG_LOG_SLOWER_THAN);
    EXPECT_EQ(cfg->slowlogFlushInterval, CONFIG_DEFAULT_SLOWLOG_FLUSH_INTERVAL);
    EXPECT_EQ(cfg->netIoThreadNum, 0);
    EXPECT_EQ(cfg->executorThreadNum, 0);

    EXPECT_EQ(cfg->binlogRateLimitMB, 64);
    EXPECT_EQ(cfg->netBatchSize, 1024*1024);
    EXPECT_EQ(cfg->netBatchTimeoutSec, 10);
    EXPECT_EQ(cfg->timeoutSecBinlogWaitRsp, 10);
    EXPECT_EQ(cfg->incrPushThreadnum, 50);
    EXPECT_EQ(cfg->fullPushThreadnum, 4);
    EXPECT_EQ(cfg->fullReceiveThreadnum, 4);
}
}  // namespace tendisplus
