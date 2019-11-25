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
#include "tendisplus/server/cluster_manager.h"
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


TEST(ClusterMsg, Common) {
    std::string sig = "RCmb";
    uint64_t totlen = genRand()*genRand();
    uint16_t port = genRand() % 55535;
    auto type1 = ClusterMsg::Type::PING;
    uint16_t count = 1;
    uint64_t  currentEpoch =  genRand()*genRand();
    uint64_t  configEpoch =  genRand()*genRand();
    uint64_t  offset =   genRand()*genRand();

    std::string sender = getUUid(20);
    std::bitset<CLUSTER_SLOTS> slots
        (std::string("100000000000000000000000000000000000000111"));
    std::string slaveof = getUUid(20);
    std::string myIp = randomIp();

    uint16_t  cport = port+10000;
    uint16_t  flags = randomNodeFlag();
    auto s = ClusterHealth::CLUSTER_OK;

    auto headGossip = std::make_shared<ClusterMsgHeader>(sig, totlen, port,
        type1, count, currentEpoch, configEpoch,
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

    std::vector<ClusterGossip> gossipVec;

/*
    std::string gossipNameV2 = getUUid(20);
    uint32_t  pingSentV2 = genRand();
    uint32_t  pongRV2 = genRand();
    std::string gossipIp = "192.122.22.131";
    uint16_t  gPort = 8011;
    uint16_t  gCport = 18011;
    uint16_t  gFlags = randomNodeFlag();

    auto vs = ClusterGossip(gossipName, pingSent, pongR,
            gossipIp, gPort, gCport, gFlags);
*/

    gossipVec.push_back(std::move(vs));

    auto GossipMsg = ClusterMsgDataGossip(std::move(gossipVec));

    auto gossipVector = GossipMsg.getGossipList();
    auto end =  gossipVector.end();
    LOG(INFO) << "vector gossip size: " << gossipVector.size();


    auto msgGossipPtr = std::make_shared<ClusterMsgDataGossip>
                (std::move(GossipMsg));

    ClusterMsg gMsg(headGossip, msgGossipPtr);

    std::string gbuff = gMsg.msgEncode();

    auto eMsg = ClusterMsg::msgDecode(gbuff);
    INVARIANT(eMsg.ok());

    auto decodeugMsg = eMsg.value();
    auto decodegHeader = decodeugMsg.getHeader();

    EXPECT_EQ(sender, decodegHeader->_sender);
    EXPECT_EQ(port, decodegHeader->_port);
    EXPECT_EQ(type1, decodegHeader->_type);
    EXPECT_EQ(slots, decodegHeader->_slots);
    EXPECT_EQ(slaveof, decodegHeader->_slaveOf);

    EXPECT_EQ(myIp, decodegHeader->_myIp);
    EXPECT_EQ(offset, decodegHeader->_offset);


    auto decodeGossip = decodeugMsg.getData();

    LOG(INFO) << "gossip message decode finish ";
  //  std::vector<ClusterGossip> msgList2 =  decodeGossip._

    std::shared_ptr<ClusterMsgDataGossip> d23 =
            std::dynamic_pointer_cast<ClusterMsgDataGossip>(decodeGossip);


    std::vector<ClusterGossip> msgList =  d23->getGossipList();
    auto gossip = msgList[0];

    LOG(INFO) << "decode ip" << gossip._gossipIp;

//    auto  gossip= msgList[0];
     EXPECT_EQ(pingSent, gossip._pingSent);
     EXPECT_EQ(pongR, gossip._pongReceived);

     EXPECT_EQ(gossipIp, gossip._gossipIp);
     EXPECT_EQ(gPort, gossip._gossipPort);
     EXPECT_EQ(gCport, gossip._gossipCport);
}

TEST(ClusterMsg, Common2) {
    std::string a = "test";
    EXPECT_EQ(a, "test");

    std::string sig = "RCmb";
    uint64_t totlen = genRand()*genRand();
    uint16_t port = 8000;
    auto type2 = ClusterMsg::Type::UPDATE;
    uint64_t  currentEpoch = genRand()*genRand();
    uint64_t  configEpoch = genRand()*genRand();
    uint64_t  offset = genRand()*genRand();

    std::string sender = getUUid(20);
    LOG(INFO) << "gen sender " << sender << "size of :" << sender.size();
    std::bitset<CLUSTER_SLOTS> slots(std::string("10001100000111"));
    std::string slaveof = getUUid(20);
    LOG(INFO) << "gen slaveof " << slaveof;
    std::string myIp = "192.168.1.1";

    uint16_t  cport = port + 10000;
    uint16_t  flags = randomNodeFlag();
    ClusterHealth  s = ClusterHealth::CLUSTER_OK;

    auto head2 = ClusterMsgHeader(sig, totlen, port, type2,
        0, currentEpoch, configEpoch, offset, sender,
        slots, slaveof, myIp, cport, flags, s);

    LOG(INFO) << "Head create on " << sender;
    LOG(INFO) << "Head create IP " << head2._myIp;

    auto headUpdate = std::make_shared<ClusterMsgHeader>
        (std::move(head2));


    auto configEpoch2 = genRand()*genRand();
    std::bitset<CLUSTER_SLOTS> slots2
    (std::string("010111100101100011111100000000000000"));
    std::string name = getUUid(20);

    LOG(INFO) << "update message create begin ";
    auto UpdateMsg = ClusterMsgDataUpdate(configEpoch2, name, slots2);

    auto msgUpdatePtr = std::make_shared<ClusterMsgDataUpdate>
        (std::move(UpdateMsg));

    std::shared_ptr<ClusterMsgData> msgDataPtr(msgUpdatePtr);


    ClusterMsg uMsg = ClusterMsg(headUpdate, msgDataPtr);


    std::string buff = uMsg.msgEncode();

    ClusterMsg decodeuMsg = ClusterMsg::msgDecode(buff).value();

    std::shared_ptr<ClusterMsgHeader> decodeHeader = decodeuMsg.getHeader();
    std::shared_ptr<ClusterMsgData> decodeUpdate = decodeuMsg.getData();

    //  EXPECT_STREQ(sig.c_str(),decodeHeader->_sig);
    EXPECT_EQ(sender, decodeHeader->_sender);
    EXPECT_EQ(port, decodeHeader->_port);
    EXPECT_EQ(type2, decodeHeader->_type);
    EXPECT_EQ(slots, decodeHeader->_slots);
    EXPECT_EQ(slaveof, decodeHeader->_slaveOf);

    EXPECT_EQ(myIp, decodeHeader->_myIp);
    EXPECT_EQ(offset, decodeHeader->_offset);
    auto updatePtr = std::dynamic_pointer_cast
        <ClusterMsgDataUpdate>(decodeUpdate);


    EXPECT_EQ(configEpoch2, updatePtr->getConfigEpoch());
    EXPECT_EQ(slots2, updatePtr->getSlots());
    EXPECT_EQ(name, updatePtr->getNodeName());
}

// check meet

TEST(Cluster, MEET) {
    std::string node1Dir= "node1";
    std::string node2Dir = "node2";
    uint32_t node1Port = 11000;
    uint32_t node2Port = 11001;
    const auto guard = MakeGuard([&] {
        destroyEnv(node1Dir);
        destroyEnv(node2Dir);
        std::this_thread::sleep_for(std::chrono::seconds(5));
    });

#ifdef _WIN32 
    uint32_t storeCnt = 2;
#else 
    uint32_t storeCnt = 0;
#endif // 


    auto node1 = makeClusterNode(node1Dir, node1Port, storeCnt);
    auto node2 = makeClusterNode(node2Dir, node2Port, storeCnt);

    auto ctx1 = std::make_shared<asio::io_context>();
    auto sess1 = makeSession(node1, ctx1);
    WorkLoad work1(node1, sess1);
    work1.init();

    work1.clusterMeet(node2->getParams()->bindIp, node2Port);

    auto ctx2 = std::make_shared<asio::io_context>();
    auto sess2 = makeSession(node1, ctx1);
    WorkLoad work2(node1, sess1);
    work2.init();
    //work2.flush();

    std::this_thread::sleep_for(std::chrono::seconds(500));

}




}  // namespace tendisplus


