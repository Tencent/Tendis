#include <fstream>
#include "gtest/gtest.h"
#include "tendisplus/utils/status.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/server/server_params.h"

namespace tendisplus {

TEST(ServerParams, Common) {
    std::ofstream myfile;
    myfile.open("a.cfg");
    myfile << "bind 127.0.0.1\n";
    myfile << "port 8903\n";
    myfile << "loglevel debug\n";
    myfile << "logDir ./\n";
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
}

}  // namespace tendisplus
