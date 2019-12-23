#include <time.h>
#include <math.h>
#include <cstdlib>
#include <string>
#include <vector>
#include <set>
#include <algorithm>
#include <limits>

#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/test_util.h"
#include "tendisplus/utils/time.h"
#include "tendisplus/server/server_entry.h"
#include "tendisplus/cluster/cluster_manager.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/invariant.h"
#include "gtest/gtest.h"

namespace tendisplus {

//int genRand() {
//    int grand = 0;
//    uint32_t ms = nsSinceEpoch();
//    grand = rand_r(reinterpret_cast<unsigned int *>(&ms));
//    return grand;
//}

std::shared_ptr<ServerEntry> 
makeClusterNode(const std::string& dir, uint32_t port, uint32_t storeCnt = 10) {
    auto mDir = dir;
    auto mport = port;
    EXPECT_TRUE(setupEnv(mDir));

    auto cfg1 = makeServerParam(mport, storeCnt, mDir);
    cfg1->enableCluster = true;

#ifdef _WIN32
    cfg1->executorThreadNum = 1;
    cfg1->netIoThreadNum = 1;
    cfg1->incrPushThreadnum = 1;
    cfg1->fullPushThreadnum = 1;
    cfg1->fullReceiveThreadnum = 1;
    cfg1->logRecycleThreadnum = 1;

    cfg1->migrateSenderThreadnum = 1;
    cfg1->migrateClearThreadnum = 1;
    cfg1->migrateReceiveThreadnum = 1;
    cfg1->migrateCheckThreadnum = 1;
#endif

    auto master = std::make_shared<ServerEntry>(cfg1);
    auto s = master->startup(cfg1);
    INVARIANT(s.ok());

    return master;
}

uint16_t randomNodeFlag() {
    switch ((genRand() % 10)) {
        case 0:
            return CLUSTER_NODE_MASTER;
        case 1:
            return CLUSTER_NODE_PFAIL;
        case 2:
            return CLUSTER_NODE_FAIL;
        case 3:
            return CLUSTER_NODE_MYSELF|CLUSTER_NODE_MASTER;
        case 4:
            return CLUSTER_NODE_HANDSHAKE;
        case 5:
            return CLUSTER_NODE_HANDSHAKE;
        default:
            // void compiler complain
            return CLUSTER_NODE_MYSELF;
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
#ifdef _WIN32
size_t gcount = 10;
#else 
size_t gcount = 1000;
#endif

TEST(ClusterMsg, Common) {
    for (size_t i = 0; i < gcount; i++) {
        std::string sig = "RCmb";
        uint32_t totlen = genRand()*genRand();
        uint16_t port = genRand() % 55535;
        auto type1 = ClusterMsg::Type::PING;
        uint16_t count = 1;
        uint16_t ver = ClusterMsg::CLUSTER_PROTO_VER;
        uint64_t  currentEpoch =  genRand()*genRand();
        uint64_t  configEpoch =  genRand()*genRand();
        uint64_t  offset =   genRand()*genRand();

        std::string sender = getUUid(20);
        std::bitset<CLUSTER_SLOTS> slots = genBitMap();
        std::string slaveof = getUUid(20);
        std::string myIp = randomIp();

        uint16_t  cport = port+10000;
        uint16_t  flags = randomNodeFlag();
        auto s = ClusterHealth::CLUSTER_OK;

        auto headGossip = std::make_shared<ClusterMsgHeader>(port,
            count, currentEpoch, configEpoch,
            offset, sender, slots, slaveof, myIp, cport, flags, s);


        std::string gossipName = getUUid(20);
        uint32_t  pingSent = genRand();
        uint32_t  pongR = genRand();
        std::string gossipIp = "192.122.22.111";
        uint16_t  gPort = 8001;
        uint16_t  gCport = 18001;
        uint16_t  gFlags = randomNodeFlag();

        auto vs = ClusterGossip(gossipName, pingSent, pongR,
                gossipIp, gPort, gCport, gFlags);


        auto GossipMsg = ClusterMsgDataGossip();
        GossipMsg.addGossipMsg(vs);

        auto msgGossipPtr = std::make_shared<ClusterMsgDataGossip>
                    (std::move(GossipMsg));

        ClusterMsg gMsg(sig, totlen, type1, headGossip, msgGossipPtr);

        std::string gbuff = gMsg.msgEncode();
        uint32_t msgSize = gMsg.getTotlen();

        auto eMsg = ClusterMsg::msgDecode(gbuff);
        INVARIANT(eMsg.ok());

        auto decodegMsg = eMsg.value();
        auto decodegHeader = decodegMsg.getHeader();

        EXPECT_EQ(msgSize, decodegMsg.getTotlen());
        EXPECT_EQ(ver , decodegHeader->_ver);
        EXPECT_EQ(sender, decodegHeader->_sender);
        EXPECT_EQ(port, decodegHeader->_port);
        EXPECT_EQ(type1, decodegMsg.getType());
        EXPECT_EQ(slots, decodegHeader->_slots);
        EXPECT_EQ(slaveof, decodegHeader->_slaveOf);

        EXPECT_EQ(myIp, decodegHeader->_myIp);
        EXPECT_EQ(offset, decodegHeader->_offset);

        auto decodeGossip = decodegMsg.getData();
    //  std::vector<ClusterGossip> msgList2 =  decodeGossip._

        std::shared_ptr<ClusterMsgDataGossip> gPtr =
                std::dynamic_pointer_cast<ClusterMsgDataGossip>(decodeGossip);


        std::vector<ClusterGossip> msgList =  gPtr->getGossipList();
        auto gossip = msgList[0];

    //    auto  gossip= msgList[0];
        EXPECT_EQ(pingSent, gossip._pingSent);
        EXPECT_EQ(pongR, gossip._pongReceived);

        EXPECT_EQ(gossipIp, gossip._gossipIp);
        EXPECT_EQ(gPort, gossip._gossipPort);
        EXPECT_EQ(gCport, gossip._gossipCport);
   }
}


TEST(ClusterMsg, CommonMoreGossip) {
    std::string sig = "RCmb";
    uint32_t totlen = genRand()*genRand();
    uint16_t port = genRand() % 55535;
    auto type1 = ClusterMsg::Type::PING;
    uint16_t count = gcount;
    uint64_t  currentEpoch = genRand()*genRand();
    uint64_t  configEpoch = genRand()*genRand();
    uint64_t  offset = genRand()*genRand();
    uint16_t ver = ClusterMsg::CLUSTER_PROTO_VER;
    std::string sender = getUUid(20);
    std::bitset<CLUSTER_SLOTS> slots = genBitMap();
    std::string slaveof = getUUid(20);
    std::string myIp = randomIp();

    uint16_t  cport = port + 10000;
    uint16_t  flags = randomNodeFlag();
    auto s = ClusterHealth::CLUSTER_OK;

    auto headGossip = std::make_shared<ClusterMsgHeader>(port,
        count, currentEpoch, configEpoch,
        offset, sender, slots, slaveof, myIp, cport, flags, s);

    auto GossipMsg = ClusterMsgDataGossip();
    std::vector<ClusterGossip> test;
    for (size_t i = 0; i < gcount; i++) {
        std::string gossipName = getUUid(20);
        uint32_t  pingSent = genRand();
        uint32_t  pongR = genRand();
        std::string gossipIp = "192.122.22.111";
        uint16_t  gPort = 8001;
        uint16_t  gCport = 18001;
        uint16_t  gFlags = randomNodeFlag();

        auto vs = ClusterGossip(gossipName, pingSent, pongR,
            gossipIp, gPort, gCport, gFlags);
        test.push_back(vs);
        GossipMsg.addGossipMsg(vs);
    }

    auto msgGossipPtr = std::make_shared<ClusterMsgDataGossip>
        (std::move(GossipMsg));

    ClusterMsg gMsg(sig, totlen, type1, headGossip, msgGossipPtr);

    std::string gbuff = gMsg.msgEncode();
    uint32_t msgSize = gMsg.getTotlen();

    auto eMsg = ClusterMsg::msgDecode(gbuff);
    INVARIANT(eMsg.ok());

    auto decodegMsg = eMsg.value();
    auto decodegHeader = decodegMsg.getHeader();

    EXPECT_EQ(msgSize, decodegMsg.getTotlen());
    EXPECT_EQ(ver, decodegHeader->_ver);
    EXPECT_EQ(sender, decodegHeader->_sender);
    EXPECT_EQ(port, decodegHeader->_port);
    EXPECT_EQ(type1, decodegMsg.getType());
    EXPECT_EQ(slots, decodegHeader->_slots);
    EXPECT_EQ(slaveof, decodegHeader->_slaveOf);

    EXPECT_EQ(myIp, decodegHeader->_myIp);
    EXPECT_EQ(offset, decodegHeader->_offset);


    auto decodeGossip = decodegMsg.getData();

    std::shared_ptr<ClusterMsgDataGossip> gPtr =
        std::dynamic_pointer_cast<ClusterMsgDataGossip>(decodeGossip);

    std::vector<ClusterGossip> msgList = gPtr->getGossipList();

    for (size_t i = 0; i < count; i++) {
        auto gossip = msgList[i];
        auto origin = test[i];

        //    auto  gossip= msgList[0];
        EXPECT_EQ(origin._pingSent, gossip._pingSent);
        EXPECT_EQ(origin._pongReceived, gossip._pongReceived);

        EXPECT_EQ(origin._gossipIp, gossip._gossipIp);
        EXPECT_EQ(origin._gossipPort, gossip._gossipPort);
        EXPECT_EQ(origin._gossipCport, gossip._gossipCport);
    }
}


TEST(ClusterMsg, CommonUpdate) {
    uint16_t ver = ClusterMsg::CLUSTER_PROTO_VER;
    std::string sig = "RCmb";
    ClusterHealth  s = ClusterHealth::CLUSTER_OK;
    for (size_t i = 0; i < gcount; i++) {
        uint32_t totlen = genRand();
        uint16_t port = 8000;
        auto type2 = ClusterMsg::Type::UPDATE;
        uint64_t  currentEpoch = genRand()*genRand();
        uint64_t  configEpoch = genRand()*genRand();
        uint64_t  offset = genRand()*genRand();
        std::string sender = getUUid(20);
        std::bitset<CLUSTER_SLOTS> slots = genBitMap();
        std::string slaveof = getUUid(20);
        std::string myIp = "192.168.1.1";

        uint16_t  cport = port + 10000;
        uint16_t  flags = randomNodeFlag();

        auto headUpdate = std::make_shared<ClusterMsgHeader>(port, 0, currentEpoch, configEpoch,
            offset, sender, slots, slaveof, myIp, cport, flags, s);

        auto uConfigEpoch = genRand()*genRand();
        std::bitset<CLUSTER_SLOTS> uSlots = genBitMap();
        std::string uName = getUUid(20);

        auto msgUpdatePtr = std::make_shared<ClusterMsgDataUpdate>(uConfigEpoch, uName, uSlots);

        std::shared_ptr<ClusterMsgData> msgDataPtr(msgUpdatePtr);

        ClusterMsg uMsg(sig, totlen, type2, headUpdate, msgUpdatePtr);

        std::string buff = uMsg.msgEncode();

        uint32_t msgSize = uMsg.getTotlen();
        ClusterMsg decodeuMsg = ClusterMsg::msgDecode(buff).value();

        std::shared_ptr<ClusterMsgHeader> decodeHeader = decodeuMsg.getHeader();
        std::shared_ptr<ClusterMsgData> decodeUpdate = decodeuMsg.getData();

        EXPECT_EQ(msgSize, decodeuMsg.getTotlen());
        EXPECT_EQ(ver, decodeHeader->_ver);
        EXPECT_EQ(sender, decodeHeader->_sender);
        EXPECT_EQ(port, decodeHeader->_port);
        EXPECT_EQ(type2, decodeuMsg.getType());
        EXPECT_EQ(slots, decodeHeader->_slots);
        EXPECT_EQ(slaveof, decodeHeader->_slaveOf);

        EXPECT_EQ(myIp, decodeHeader->_myIp);
        EXPECT_EQ(offset, decodeHeader->_offset);

        auto updatePtr = std::dynamic_pointer_cast
            <ClusterMsgDataUpdate>(decodeUpdate);


        EXPECT_EQ(uConfigEpoch, updatePtr->getConfigEpoch());
        EXPECT_EQ(uSlots, updatePtr->getSlots());
        EXPECT_EQ(uName, updatePtr->getNodeName());
    }
}

//check meet
bool compareClusterInfo(std::shared_ptr<ServerEntry> svr1, std::shared_ptr<ServerEntry> svr2) {
    auto cs1 = svr1->getClusterMgr()->getClusterState();
    auto cs2 = svr2->getClusterMgr()->getClusterState();

    auto nodelist1 = cs1->getNodesList();
    auto nodelist2 = cs2->getNodesList();

    EXPECT_EQ(cs1->getNodeCount(), cs2->getNodeCount());
    EXPECT_EQ(cs1->getCurrentEpoch(), cs2->getCurrentEpoch());

    for(auto nodep : nodelist1) {
        auto node1 = nodep.second;
        
        auto node2 = cs2->clusterLookupNode(node1->getNodeName());
        EXPECT_TRUE(node2 != nullptr);
        EXPECT_EQ(*node1.get(), *node2.get());
    }

    return false;
}

#ifdef _WIN32 
uint32_t storeCnt = 2;
#else 
uint32_t storeCnt = 2;
#endif // 

MYTEST(Cluster, Simple_MEET) {
    std::vector<std::string> dirs = { "node1", "node2", "node3" };
    uint32_t startPort = 11000;

    const auto guard = MakeGuard([dirs] {
        for (auto dir : dirs) {
            destroyEnv(dir);
        }
        std::this_thread::sleep_for(std::chrono::seconds(5));
    });

    std::vector<std::shared_ptr<ServerEntry>> servers;

    uint32_t index = 0;
    for (auto dir : dirs) {
        uint32_t nodePort = startPort + index++;
        servers.emplace_back(std::move(makeClusterNode(dir, nodePort, storeCnt)));
    }

    auto& node1 = servers[0];
    auto& node2 = servers[1];
    auto& node3 = servers[2];

    auto ctx1 = std::make_shared<asio::io_context>();
    auto sess1 = makeSession(node1, ctx1);
    WorkLoad work1(node1, sess1);
    work1.init();

    // meet _myself
    //work1.clusterMeet(node1->getParams()->bindIp, node1->getParams()->port);
    //std::this_thread::sleep_for(std::chrono::seconds(10));

    work1.clusterMeet(node2->getParams()->bindIp, node2->getParams()->port);
    work1.clusterMeet(node3->getParams()->bindIp, node3->getParams()->port);

    std::this_thread::sleep_for(std::chrono::seconds(10));
    for (auto svr : servers) {
        compareClusterInfo(svr, node1);
    }

    work1.clusterNodes();
#ifndef _WIN32
    for (auto svr : servers) {
        svr->stop();
        LOG(INFO) << "stop " <<  svr->getParams()->port << " success";
    }
#endif

    servers.clear();
}

MYTEST(Cluster, Sequence_Meet) {
    //std::vector<std::string> dirs = { "node1", "node2", "node3", "node4", "node5",
    //                "node6", "node7", "node8", "node9", "node10" };
    std::vector<std::string> dirs;
    uint32_t startPort = 11000;

    for (uint32_t i = 0; i < 10; i++) {
        dirs.push_back("node" + std::to_string(i));
    }

    const auto guard = MakeGuard([dirs] {
        for (auto dir : dirs) {
            destroyEnv(dir);
        }
        std::this_thread::sleep_for(std::chrono::seconds(5));
    });

    std::vector<std::shared_ptr<ServerEntry>> servers;

    uint32_t index = 0;
    for (auto dir : dirs) {
        uint32_t nodePort = startPort + index++;
        servers.emplace_back(std::move(makeClusterNode(dir, nodePort, storeCnt)));
    }

    auto node = servers[0];

    auto ctx = std::make_shared<asio::io_context>();
    auto sess = makeSession(node, ctx);
    WorkLoad work(node, sess);
    work.init();

    for (auto node2 : servers) {
        work.clusterMeet(node2->getParams()->bindIp, node2->getParams()->port);
    }

    std::this_thread::sleep_for(std::chrono::seconds(50));
    for (auto svr : servers) {
        compareClusterInfo(svr, node);
    }

#ifndef _WIN32
    for (auto svr : servers) {
        svr->stop();
        LOG(INFO) << "stop " <<  svr->getParams()->port << " success";
        //ASSERT_EQ(svr.use_count(), 1);
    }
#endif

    servers.clear();
}

TEST(Cluster, Random_Meet) {
    //std::vector<std::string> dirs = { "node1", "node2", "node3", "node4", "node5",
    //                "node6", "node7", "node8", "node9", "node10" };
    std::vector<std::string> dirs;
    uint32_t startPort = 11000;

    for (uint32_t i = 0; i < 10; i++) {
        dirs.push_back("node" + std::to_string(i));
    }

    const auto guard = MakeGuard([dirs] {
        for (auto dir : dirs) {
            destroyEnv(dir);
        }
        std::this_thread::sleep_for(std::chrono::seconds(5));
    });

    std::vector<std::shared_ptr<ServerEntry>> servers;

    uint32_t index = 0;
    for (auto dir : dirs) {
        uint32_t nodePort = startPort + index++;
        servers.emplace_back(std::move(makeClusterNode(dir, nodePort, storeCnt)));
    }

    auto node = servers[0];
    while (node->getClusterMgr()->getClusterState()->getNodeCount() != servers.size()) {
        auto node1 = servers[genRand() % servers.size()];
        auto node2 = servers[genRand() % servers.size()];

        auto ctx1 = std::make_shared<asio::io_context>();
        auto sess1 = makeSession(node1, ctx1);
        WorkLoad work1(node1, sess1);
        work1.init();

        work1.clusterMeet(node2->getParams()->bindIp, node2->getParams()->port);
    }

    // random meet non exist node;
    for (uint32_t i = 0; i < servers.size(); i++) {
        auto node1 = servers[genRand() % servers.size()];
        auto port = startPort - 100;

        auto ctx1 = std::make_shared<asio::io_context>();
        auto sess1 = makeSession(node1, ctx1);
        WorkLoad work1(node1, sess1);
        work1.init();

        // meet one non exists node
        work1.clusterMeet(node1->getParams()->bindIp, port);
    }

    std::this_thread::sleep_for(std::chrono::seconds(50));
    for (auto svr : servers) {
        compareClusterInfo(svr, node);
    }

#ifndef _WIN32
    for (auto svr : servers) {
        svr->stop();
        LOG(INFO) << "stop " <<  svr->getParams()->port << " success";
        //ASSERT_EQ(svr.use_count(), 1);
    }
#endif

    servers.clear();
}

}  // namespace tendisplus


