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
}

}  // namespace tendisplus
