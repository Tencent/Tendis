// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <time.h>
#include <math.h>
#include <cstdlib>
#include <string>
#include <vector>
#include <set>
#include <algorithm>
#include <limits>

#include "gtest/gtest.h"

#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/test_util.h"
#include "tendisplus/utils/time.h"
#include "tendisplus/server/server_entry.h"
#include "tendisplus/cluster/cluster_manager.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

void testCommandArrayResult(
  std::shared_ptr<ServerEntry> svr,
  const std::vector<std::pair<std::vector<std::string>, std::string>>& arr) {
  asio::io_context ioContext;
  asio::ip::tcp::socket socket(ioContext), socket1(ioContext);
  NetSession sess(svr, std::move(socket), 1, false, nullptr, nullptr);

  for (auto& p : arr) {
    sess.setArgs(p.first);
    auto expect = Command::runSessionCmd(&sess);
    if (expect.ok()) {
      auto ret = expect.value();
      EXPECT_EQ(p.second, ret);
    } else {
      auto ret = expect.status().toString();
      EXPECT_EQ(p.second, ret);
    }
  }
}

std::shared_ptr<ServerEntry> makeClusterNode(const std::string& dir,
                                             uint32_t port,
                                             uint32_t storeCnt = 10,
                                             bool general_log = true,
                                             bool singleNode = false) {
  auto mDir = dir;
  auto mport = port;
  EXPECT_TRUE(setupEnv(mDir));

  auto cfg1 = makeServerParam(mport, storeCnt, mDir, general_log);
  cfg1->clusterEnabled = true;
  cfg1->pauseTimeIndexMgr = 1;
  cfg1->rocksBlockcacheMB = 24;
  cfg1->clusterSingleNode = singleNode;

#ifdef _WIN32
  cfg1->executorThreadNum = 1;
  cfg1->netIoThreadNum = 1;
  cfg1->incrPushThreadnum = 1;
  cfg1->fullPushThreadnum = 1;
  cfg1->fullReceiveThreadnum = 1;
  cfg1->logRecycleThreadnum = 1;
  cfg1->migrateSenderThreadnum = 1;
  cfg1->migrateReceiveThreadnum = 1;
#endif

  auto master = std::make_shared<ServerEntry>(cfg1);
  auto s = master->startup(cfg1);
  if (!s.ok()) {
    LOG(ERROR) << "server start fail:" << s.toString();
  }
  INVARIANT(s.ok());

  return master;
}

std::vector<std::shared_ptr<ServerEntry>>
#ifdef _WIN32
makeCluster(uint32_t startPort,
            uint32_t nodeNum = 3,
            uint32_t storeCnt = 1,
            bool withSlave = false) {
#else
makeCluster(uint32_t startPort,
            uint32_t nodeNum = 3,
            uint32_t storeCnt = 10,
            bool withSlave = false) {
#endif
  LOG(INFO) << "Make Cluster begin.";
  std::vector<std::string> dirs;
  uint32_t totalNodeNum = nodeNum;
  if (withSlave) {
    totalNodeNum *= 2;
  }

  for (uint32_t i = 0; i < totalNodeNum; ++i) {
    dirs.push_back("node" + to_string(i));
  }

  std::vector<std::shared_ptr<ServerEntry>> servers;

  uint32_t index = 0;
  for (auto dir : dirs) {
    // TODO(wayenchen): find a available port
    uint32_t nodePort = startPort + index++;
    servers.emplace_back(std::move(makeClusterNode(dir, nodePort, storeCnt)));
  }

  auto node0 = servers[0];
  auto ctx0 = std::make_shared<asio::io_context>();
  auto sess0 = makeSession(node0, ctx0);
  WorkLoad work0(node0, sess0);
  work0.init();

  for (auto node : servers) {
    work0.clusterMeet(node->getParams()->bindIp, node->getParams()->port);
  }

  uint32_t step = CLUSTER_SLOTS / nodeNum;
  uint32_t firstslot = 0;
  uint32_t lastslot = 0;
  uint32_t idx = 0;

  // addSlots
  for (uint32_t i = 0; i < nodeNum; ++i) {
    auto node = servers[i];
    auto ctx = std::make_shared<asio::io_context>();
    auto sess = makeSession(node, ctx);
    WorkLoad work(node, sess);
    work.init();

    if (lastslot > 0)
      firstslot = lastslot + 1;

    lastslot = firstslot + step;
    if (idx == nodeNum - 1) {
      lastslot = CLUSTER_SLOTS - 1;
    }

    char buf[128];
    snprintf(buf, 128, "{%u..%u}", firstslot, lastslot);  // NOLINT

    std::string slotstr(buf);
    LOG(INFO) << "ADD SLOTS:" << slotstr;
    work.addSlots(slotstr);

    idx++;
  }

  std::this_thread::sleep_for(std::chrono::seconds(10));
  // slaveof
  for (uint32_t i = nodeNum; i < totalNodeNum; ++i) {
    auto node = servers[i];
    auto ctx = std::make_shared<asio::io_context>();
    auto sess = makeSession(node, ctx);
    WorkLoad work(node, sess);
    work.init();

    auto node2 = servers[i - nodeNum];
    auto ctx2 = std::make_shared<asio::io_context>();
    auto sess2 = makeSession(node2, ctx2);
    WorkLoad work2(node2, sess2);
    work2.init();
    auto masterid = work2.getStringResult({"cluster", "myid"});
    auto master = getBulkValue(masterid, 0);

    LOG(INFO) << "cluster replicate:" << master;
    work.replicate(master);

    idx++;
  }

  auto t = msSinceEpoch();
  bool isok = true;
  LOG(INFO) << "waiting servers cluster state changed to ok ";
  while (true) {
    isok = true;
    for (auto node : servers) {
      if (!node->getClusterMgr()->getClusterState()->clusterIsOK()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        isok = false;
        break;
      }
    }
    if (isok) {
      break;
    }
    if (msSinceEpoch() - t > 100 * 1000) {
      // take too long time
      INVARIANT_D(0);
    }
  }
  LOG(INFO) << "waiting servers ok using " << (msSinceEpoch() - t) << "ms.";

  return std::move(servers);
}

std::vector<std::shared_ptr<ServerEntry>> makeSingleCluster(
  uint32_t startPort, uint32_t storeCnt = 10) {
  LOG(INFO) << "Make single Cluster begin.";
  std::vector<std::string> dirs;
  uint32_t totalNodeNum = 4;

#ifdef _WIN32
  storeCnt = 1;
#endif

  for (uint32_t i = 0; i < totalNodeNum; ++i) {
    dirs.push_back("node" + to_string(i));
  }

  std::vector<std::shared_ptr<ServerEntry>> servers;

  uint32_t index = 0;
  for (auto dir : dirs) {
    uint32_t nodePort = startPort + index++;
    servers.emplace_back(
      std::move(makeClusterNode(dir, nodePort, storeCnt, true, true)));
  }

  auto node0 = servers[0];
  auto ctx0 = std::make_shared<asio::io_context>();
  auto sess0 = makeSession(node0, ctx0);
  WorkLoad work0(node0, sess0);
  work0.init();

  for (auto node : servers) {
    work0.clusterMeet(node->getParams()->bindIp, node->getParams()->port);
  }

  auto node = servers[0];
  auto ctx = std::make_shared<asio::io_context>();
  auto sess = makeSession(node, ctx);

  /* try to add a incorrect slots, for test */
  sess->setArgs({"cluster", "addslots", "{0..5000}"});
  auto expect = Command::runSessionCmd(sess.get());
  EXPECT_TRUE(!expect.ok());
  LOG(INFO) << expect.status().toString();

  WorkLoad work(node, sess);
  work.init();
  work.addSlots("{0..16383}");

  auto masterid = work.getStringResult({"cluster", "myid"});
  auto master = getBulkValue(masterid, 0);
  LOG(INFO) << "master is:" << master;

  std::this_thread::sleep_for(std::chrono::seconds(5));
  // slaveof
  {
    auto node1 = servers[1];
    auto ctx1 = std::make_shared<asio::io_context>();
    auto sess1 = makeSession(node1, ctx1);
    WorkLoad work1(node1, sess1);
    work1.init();

    LOG(INFO) << "cluster replicate:" << master;
    work1.replicate(master);


    auto eid = work.getStringResult({"cluster", "myid"});
    auto slave = getBulkValue(eid, 0);
    LOG(INFO) << "slave is:" << slave;
  }

  std::string arbiter = "";
  for (uint32_t i = 2; i < totalNodeNum; i++) {
    auto nodei = servers[i];
    auto ctxi = std::make_shared<asio::io_context>();
    auto sessi = makeSession(nodei, ctxi);

    sessi->setArgs({"cluster", "asarbiter"});
    auto expecti = Command::runSessionCmd(sessi.get());
    EXPECT_TRUE(expecti.ok());

    WorkLoad worki(nodei, sessi);
    worki.init();

    auto eid = work.getStringResult({"cluster", "myid"});
    arbiter = getBulkValue(eid, 0);
    LOG(INFO) << "aribter is:" << arbiter;
  }

  auto t = msSinceEpoch();
  bool isok = true;
  LOG(INFO) << "waiting servers cluster state changed to ok ";
  while (true) {
    isok = true;
    for (auto node : servers) {
      if (!node->getClusterMgr()->getClusterState()->clusterIsOK()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        isok = false;
        break;
      }
    }
    if (isok) {
      break;
    }
    if (msSinceEpoch() - t > 100 * 1000) {
      // take too long time
      INVARIANT_D(0);
    }
  }
  LOG(INFO) << "waiting servers ok using " << (msSinceEpoch() - t) << "ms.";

  return std::move(servers);
}

void waitNodeFail(std::shared_ptr<ClusterState>& state,
                  const std::string& nodeName) {
  auto start = msSinceEpoch();
  LOG(INFO) << "waiting servers cluster state changed to ok ";

  auto targetNode = state->clusterLookupNode(nodeName);
  while (!targetNode->nodeFailed()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    if (msSinceEpoch() - start > 40 * 1000) {
      // take too long time
      INVARIANT_D(0);
    }
  }
  LOG(INFO) << "wait node fail state cost time"
            << (msSinceEpoch() - start) / 1000 << "s";
}

void destroyCluster(uint32_t nodeNum) {
  for (uint32_t i = 0; i < nodeNum; ++i) {
    destroyEnv("node" + to_string(i));
  }
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
      return CLUSTER_NODE_MYSELF | CLUSTER_NODE_MASTER;
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
    uint32_t totlen = genRand() * genRand();
    uint16_t port = genRand() % 55535;
    auto type1 = ClusterMsg::Type::PING;
    uint16_t count = 1;
    uint16_t ver = ClusterMsg::CLUSTER_PROTO_VER;
    uint64_t currentEpoch =
      static_cast<uint64_t>(genRand()) * static_cast<uint64_t>(genRand());
    uint64_t configEpoch =
      static_cast<uint64_t>(genRand()) * static_cast<uint64_t>(genRand());
    uint64_t offset =
      static_cast<uint64_t>(genRand()) * static_cast<uint64_t>(genRand());

    std::string sender = getUUid(20);
    std::bitset<CLUSTER_SLOTS> slots = genBitMap();
    std::string slaveof = getUUid(20);
    std::string myIp = randomIp();

    uint16_t cport = port + 10000;
    uint16_t flags = randomNodeFlag();
    auto s = ClusterHealth::CLUSTER_OK;

    auto headGossip = std::make_shared<ClusterMsgHeader>(port,
                                                         count,
                                                         currentEpoch,
                                                         configEpoch,
                                                         offset,
                                                         sender,
                                                         slots,
                                                         slaveof,
                                                         myIp,
                                                         cport,
                                                         flags,
                                                         s);


    std::string gossipName = getUUid(20);
    uint32_t pingSent = genRand();
    uint32_t pongR = genRand();
    std::string gossipIp = "192.122.22.111";
    uint16_t gPort = 8001;
    uint16_t gCport = 18001;
    uint16_t gFlags = randomNodeFlag();

    auto vs = ClusterGossip(
      gossipName, pingSent, pongR, gossipIp, gPort, gCport, gFlags);


    auto GossipMsg = ClusterMsgDataGossip();
    GossipMsg.addGossipMsg(vs);

    auto msgGossipPtr =
      std::make_shared<ClusterMsgDataGossip>(std::move(GossipMsg));

    ClusterMsg gMsg(
      sig, totlen, type1, CLUSTERMSG_FLAG0_PAUSED, headGossip, msgGossipPtr);

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
    EXPECT_EQ(CLUSTERMSG_FLAG0_PAUSED, gMsg.getMflags());
    EXPECT_EQ(slots, decodegHeader->_slots);
    EXPECT_EQ(slaveof, decodegHeader->_slaveOf);

    EXPECT_EQ(myIp, decodegHeader->_myIp);
    EXPECT_EQ(offset, decodegHeader->_offset);

    auto decodeGossip = decodegMsg.getData();
    //  std::vector<ClusterGossip> msgList2 =  decodeGossip._

    std::shared_ptr<ClusterMsgDataGossip> gPtr =
      std::dynamic_pointer_cast<ClusterMsgDataGossip>(decodeGossip);


    std::vector<ClusterGossip> msgList = gPtr->getGossipList();
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
  uint32_t totlen = genRand() * genRand();
  uint16_t port = genRand() % 55535;
  auto type1 = ClusterMsg::Type::PING;
  uint16_t count = gcount;
  uint64_t currentEpoch =
    static_cast<uint64_t>(genRand()) * static_cast<uint64_t>(genRand());
  uint64_t configEpoch =
    static_cast<uint64_t>(genRand()) * static_cast<uint64_t>(genRand());
  uint64_t offset =
    static_cast<uint64_t>(genRand()) * static_cast<uint64_t>(genRand());
  uint16_t ver = ClusterMsg::CLUSTER_PROTO_VER;
  std::string sender = getUUid(20);
  std::bitset<CLUSTER_SLOTS> slots = genBitMap();
  std::string slaveof = getUUid(20);
  std::string myIp = randomIp();

  uint16_t cport = port + 10000;
  uint16_t flags = randomNodeFlag();
  auto s = ClusterHealth::CLUSTER_OK;

  auto headGossip = std::make_shared<ClusterMsgHeader>(port,
                                                       count,
                                                       currentEpoch,
                                                       configEpoch,
                                                       offset,
                                                       sender,
                                                       slots,
                                                       slaveof,
                                                       myIp,
                                                       cport,
                                                       flags,
                                                       s);

  auto GossipMsg = ClusterMsgDataGossip();
  std::vector<ClusterGossip> test;
  for (size_t i = 0; i < gcount; i++) {
    std::string gossipName = getUUid(20);
    uint32_t pingSent = genRand();
    uint32_t pongR = genRand();
    std::string gossipIp = "192.122.22.111";
    uint16_t gPort = 8001;
    uint16_t gCport = 18001;
    uint16_t gFlags = randomNodeFlag();

    auto vs = ClusterGossip(
      gossipName, pingSent, pongR, gossipIp, gPort, gCport, gFlags);
    test.push_back(vs);
    GossipMsg.addGossipMsg(vs);
  }

  auto msgGossipPtr =
    std::make_shared<ClusterMsgDataGossip>(std::move(GossipMsg));

  ClusterMsg gMsg(
    sig, totlen, type1, CLUSTERMSG_FLAG0_PAUSED, headGossip, msgGossipPtr);

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
  EXPECT_EQ(CLUSTERMSG_FLAG0_PAUSED, decodegMsg.getMflags());
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
  ClusterHealth s = ClusterHealth::CLUSTER_OK;
  for (size_t i = 0; i < gcount; i++) {
    uint32_t totlen = genRand();
    uint16_t port = 8000;
    auto type2 = ClusterMsg::Type::UPDATE;
    uint64_t currentEpoch =
      static_cast<uint64_t>(genRand()) * static_cast<uint64_t>(genRand());
    uint64_t configEpoch =
      static_cast<uint64_t>(genRand()) * static_cast<uint64_t>(genRand());
    uint64_t offset =
      static_cast<uint64_t>(genRand()) * static_cast<uint64_t>(genRand());
    std::string sender = getUUid(20);
    std::bitset<CLUSTER_SLOTS> slots = genBitMap();
    std::string slaveof = getUUid(20);
    std::string myIp = "192.168.1.1";

    uint16_t cport = port + 10000;
    uint16_t flags = randomNodeFlag();

    auto headUpdate = std::make_shared<ClusterMsgHeader>(port,
                                                         0,
                                                         currentEpoch,
                                                         configEpoch,
                                                         offset,
                                                         sender,
                                                         slots,
                                                         slaveof,
                                                         myIp,
                                                         cport,
                                                         flags,
                                                         s);

    auto uConfigEpoch = genRand() * genRand();
    std::bitset<CLUSTER_SLOTS> uSlots = genBitMap();
    std::string uName = getUUid(20);

    auto msgUpdatePtr =
      std::make_shared<ClusterMsgDataUpdate>(uConfigEpoch, uName, uSlots);

    std::shared_ptr<ClusterMsgData> msgDataPtr(msgUpdatePtr);

    ClusterMsg uMsg(
      sig, totlen, type2, CLUSTERMSG_FLAG0_PAUSED, headUpdate, msgUpdatePtr);

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
    EXPECT_EQ(CLUSTERMSG_FLAG0_PAUSED, decodeuMsg.getMflags());
    EXPECT_EQ(slots, decodeHeader->_slots);
    EXPECT_EQ(slaveof, decodeHeader->_slaveOf);

    EXPECT_EQ(myIp, decodeHeader->_myIp);
    EXPECT_EQ(offset, decodeHeader->_offset);

    auto updatePtr =
      std::dynamic_pointer_cast<ClusterMsgDataUpdate>(decodeUpdate);


    EXPECT_EQ(uConfigEpoch, updatePtr->getConfigEpoch());
    EXPECT_EQ(uSlots, updatePtr->getSlots());
    EXPECT_EQ(uName, updatePtr->getNodeName());
  }
}

// check meet
bool compareClusterInfo(std::shared_ptr<ServerEntry> svr1,
                        std::shared_ptr<ServerEntry> svr2) {
  auto cs1 = svr1->getClusterMgr()->getClusterState();
  auto cs2 = svr2->getClusterMgr()->getClusterState();

  auto nodelist1 = cs1->getNodesList();
  auto nodelist2 = cs2->getNodesList();

  EXPECT_EQ(cs1->getNodeCount(), cs2->getNodeCount());
  EXPECT_EQ(cs1->getCurrentEpoch(), cs2->getCurrentEpoch());

  for (auto nodep : nodelist1) {
    auto node1 = nodep.second;

    auto node2 = cs2->clusterLookupNode(node1->getNodeName());
    EXPECT_TRUE(node2 != nullptr);
    EXPECT_EQ(*node1.get(), *node2.get());
  }

  return false;
}


// if slot set successfully , return ture
bool checkSlotInfo(std::shared_ptr<ClusterNode> node, std::string slots) {
  auto slotInfo = node->getSlots();
  if ((slots.find('{') != string::npos) && (slots.find('}') != string::npos)) {
    slots = slots.substr(1, slots.size() - 2);
    std::vector<std::string> s = stringSplit(slots, "..");
    auto startSlot = ::tendisplus::stoul(s[0]);
    EXPECT_EQ(startSlot.ok(), true);
    auto endSlot = ::tendisplus::stoul(s[1]);
    EXPECT_EQ(endSlot.ok(), true);
    auto start = startSlot.value();
    auto end = endSlot.value();
    if (start < end) {
      for (size_t i = start; i < end; i++) {
        if (!slotInfo.test(i)) {
          LOG(ERROR) << "set slot" << i << "fail";
          return false;
        }
      }
      return true;
    } else {
      LOG(ERROR) << "checkt Slot: Invalid range slot";
      return false;
    }
  } else {
    auto slot = ::tendisplus::stoul(slots);
    // EXPECT_EQ(slot.ok(), true);
    if (!slotInfo.test(slot.value())) {
      LOG(ERROR) << "set slot " << slot.value() << "fail";
      return false;
    } else {
      return true;
    }
  }
  return false;
}

Expected<std::string> migrate(const std::shared_ptr<ServerEntry>& server1,
                              const std::shared_ptr<ServerEntry>& server2,
                              const std::bitset<CLUSTER_SLOTS>& slots,
                              bool retry = false) {
  std::vector<std::string> args;

  auto ctx = std::make_shared<asio::io_context>();
  auto sess = makeSession(server2, ctx);

  args.push_back("cluster");
  args.push_back("setslot");
  if (retry) {
    args.push_back("restart");
  } else {
    args.push_back("importing");
  }
  std::string nodeName =
    server1->getClusterMgr()->getClusterState()->getMyselfName();

  args.push_back(nodeName);

  for (size_t id = 0; id < slots.size(); id++) {
    if (slots.test(id)) {
      args.push_back(std::to_string(id));
    }
  }

  sess->setArgs(args);
  auto expectId = Command::runSessionCmd(sess.get());

  if (!expectId.ok()) {
    return expectId.status();
  }
  return expectId.value();
}


#ifdef _WIN32
uint32_t storeCnt = 2;
uint32_t storeCntx = 6;
#else
uint32_t storeCnt = 2;
#endif  //
uint32_t storeCnt1 = 6;
uint32_t storeCnt2 = 10;

MYTEST(Cluster, Simple_MEET) {
  std::vector<std::string> dirs = {"node1", "node2", "node3"};
  uint32_t startPort = 15000;

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
  // work1.clusterMeet(node1->getParams()->bindIp, node1->getParams()->port);
  // std::this_thread::sleep_for(std::chrono::seconds(10));

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
    LOG(INFO) << "stop " << svr->getParams()->port << " success";
  }
#endif

  servers.clear();
}

MYTEST(Cluster, Sequence_Meet) {
  // std::vector<std::string> dirs = { "node1", "node2", "node3", "node4",
  // "node5",
  //                "node6", "node7", "node8", "node9", "node10" };
  std::vector<std::string> dirs;
  uint32_t startPort = 15000;

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
    LOG(INFO) << "stop " << svr->getParams()->port << " success";
    // ASSERT_EQ(svr.use_count(), 1);
  }
#endif

  servers.clear();
}

TEST(Cluster, Random_Meet) {
  // std::vector<std::string> dirs = { "node1", "node2", "node3", "node4",
  // "node5",
  //                "node6", "node7", "node8", "node9", "node10" };
  std::vector<std::string> dirs;
  uint32_t startPort = 15000;

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
  while (node->getClusterMgr()->getClusterState()->getNodeCount() !=
         servers.size()) {
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
    LOG(INFO) << "stop " << svr->getParams()->port << " success";
    // ASSERT_EQ(svr.use_count(), 1);
  }
#endif

  servers.clear();
}

TEST(Cluster, AddSlot) {
  std::vector<std::string> dirs = {"node1", "node2"};
  uint32_t startPort = 15000;

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

  auto ctx1 = std::make_shared<asio::io_context>();
  auto sess1 = makeSession(node1, ctx1);
  WorkLoad work1(node1, sess1);
  work1.init();

  work1.clusterMeet(node2->getParams()->bindIp, node2->getParams()->port);
  std::this_thread::sleep_for(std::chrono::seconds(10));

  std::vector<std::string> slots = {"{0..8000}", "{8001..16383}"};

  work1.addSlots(slots[0]);
  std::this_thread::sleep_for(std::chrono::seconds(10));

  auto ctx2 = std::make_shared<asio::io_context>();
  auto sess2 = makeSession(node2, ctx2);
  WorkLoad work2(node2, sess2);
  work2.init();
  work2.addSlots(slots[1]);

  std::this_thread::sleep_for(std::chrono::seconds(10));

  for (size_t i = 0; i < slots.size(); i++) {
    auto nodePtr =
      servers[i]->getClusterMgr()->getClusterState()->getMyselfNode();
    bool s = checkSlotInfo(nodePtr, slots[i]);
    EXPECT_TRUE(s);
  }

  std::this_thread::sleep_for(std::chrono::seconds(10));
  for (auto svr : servers) {
    compareClusterInfo(svr, node1);
  }

#ifndef _WIN32
  for (auto svr : servers) {
    svr->stop();
    LOG(INFO) << "stop " << svr->getParams()->port << " success";
  }
#endif
  servers.clear();
}

bool nodeIsMySlave(std::shared_ptr<ServerEntry> svr1,
                   std::shared_ptr<ServerEntry> svr2) {
  if (svr1->getParams()->clusterEnabled && svr2->getParams()->clusterEnabled) {
    CNodePtr myself = svr1->getClusterMgr()->getClusterState()->getMyselfNode();
    CNodePtr node2 = svr2->getClusterMgr()->getClusterState()->getMyselfNode();

    LOG(INFO) << "myself name:" << myself->getNodeName()
              << "node2 master name:" << node2->getMaster()->getNodeName();
    auto masterName = node2->getMaster()->getNodeName();
    if (masterName == myself->getNodeName()) {
      return true;
    }
  }
  return false;
}

bool nodeIsMaster(std::shared_ptr<ServerEntry> svr) {
  if (svr->getParams()->clusterEnabled) {
    CNodePtr myself = svr->getClusterMgr()->getClusterState()->getMyselfNode();
    if (myself->nodeIsMaster()) {
      return true;
    }
  }
  return false;
}

void setNodeAsMySlave(std::shared_ptr<ServerEntry> svr1,
                      std::shared_ptr<ServerEntry> svr2) {
  if (svr1->getParams()->clusterEnabled) {
    CNodePtr exptMaster =
      svr1->getClusterMgr()->getClusterState()->getMyselfNode();
    if (exptMaster != nullptr) {
      auto state = svr2->getClusterMgr()->getClusterState();
      state->clusterSetMaster(exptMaster, true);
    }
  }
}

bool clusterOk(std::shared_ptr<ClusterState> state) {
  return state->getClusterState() == ClusterHealth::CLUSTER_OK;
}

TEST(Cluster, failover) {
  std::vector<std::string> dirs = {"node1", "node2", "node3", "node4", "node5"};
  uint32_t startPort = 15000;

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
    servers.emplace_back(std::move(makeClusterNode(dir, nodePort, storeCnt1)));
  }
  // 3 master and 2 slave *, make one master fail
  auto& node1 = servers[0];
  auto& node2 = servers[1];
  auto& node3 = servers[2];
  auto& node4 = servers[3];
  auto& node5 = servers[4];
  //   auto& node6 = servers[5];

  auto ctx1 = std::make_shared<asio::io_context>();
  auto sess1 = makeSession(node1, ctx1);
  WorkLoad work1(node1, sess1);
  work1.init();

  work1.clusterMeet(node2->getParams()->bindIp, node2->getParams()->port);
  work1.clusterMeet(node3->getParams()->bindIp, node3->getParams()->port);
  work1.clusterMeet(node4->getParams()->bindIp, node4->getParams()->port);
  work1.clusterMeet(node5->getParams()->bindIp, node5->getParams()->port);
  //   work1.clusterMeet(node6->getParams()->bindIp,
  //   node6->getParams()->port);
  std::this_thread::sleep_for(std::chrono::seconds(10));

  std::vector<std::string> slots = {
    "{0..5000}", "{9001..16383}", "{5001..9000}"};

  work1.addSlots(slots[0]);
  std::this_thread::sleep_for(std::chrono::seconds(10));

  auto ctx2 = std::make_shared<asio::io_context>();
  auto sess2 = makeSession(node2, ctx2);
  WorkLoad work2(node2, sess2);
  work2.init();
  work2.addSlots(slots[1]);

  auto ctx5 = std::make_shared<asio::io_context>();
  auto sess5 = makeSession(node5, ctx5);
  WorkLoad work5(node5, sess5);
  work5.init();
  work5.addSlots(slots[2]);


  auto ctx3 = std::make_shared<asio::io_context>();
  auto sess3 = makeSession(node3, ctx3);
  WorkLoad work3(node3, sess3);
  work3.init();
  auto nodeName1 = node1->getClusterMgr()->getClusterState()->getMyselfName();
  work3.replicate(nodeName1);

  auto ctx4 = std::make_shared<asio::io_context>();
  auto sess4 = makeSession(node4, ctx4);
  WorkLoad work4(node4, sess4);
  work4.init();
  auto state = node1->getClusterMgr()->getClusterState();
  auto nodeName2 = node2->getClusterMgr()->getClusterState()->getMyselfName();
  work4.replicate(nodeName2);
  auto nodeName3 = node3->getClusterMgr()->getClusterState()->getMyselfName();
  auto nodeName4 = node4->getClusterMgr()->getClusterState()->getMyselfName();
  std::this_thread::sleep_for(std::chrono::seconds(15));

  ASSERT_TRUE(nodeIsMySlave(node1, node3));
  ASSERT_TRUE(nodeIsMySlave(node2, node4));

  // make node2 fail，it is
  node2->stop();
  // master node2 mark fail
  waitNodeFail(state, nodeName2);
  std::this_thread::sleep_for(std::chrono::seconds(10));
  // slave become master
  ASSERT_EQ(nodeIsMaster(node4), true);
  // cluster work ok after vote sucessful
  ASSERT_EQ(clusterOk(state), true);

#ifndef _WIN32
  for (auto svr : servers) {
    svr->stop();
    LOG(INFO) << "stop " << svr->getParams()->port << " success";
  }
#endif

  servers.clear();
}

bool checkSlotsBlong(const std::bitset<CLUSTER_SLOTS>& slots,
                     std::shared_ptr<ServerEntry> svr,
                     std::string nodeid) {
  auto state = svr->getClusterMgr()->getClusterState();
  CNodePtr node = state->clusterLookupNode(nodeid);

  for (size_t id = 0; id < slots.size(); id++) {
    if (slots.test(id)) {
      if (state->getNodeBySlot(id) != node) {
        LOG(ERROR) << "slot:" << id << " not belong to: " << nodeid;
        return false;
      }
    }
  }
  return true;
}

std::bitset<CLUSTER_SLOTS> getBitSet(std::vector<uint32_t> vec) {
  std::bitset<CLUSTER_SLOTS> slots;
  for (auto& vs : vec) {
    slots.set(vs);
  }
  return slots;
}

TEST(Cluster, migrate) {
  std::vector<std::string> dirs = {"node1", "node2"};
  uint32_t startPort = 15000;

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
    EXPECT_TRUE(setupEnv(dir));

    auto cfg1 = makeServerParam(nodePort, storeCnt, dir, true);
    cfg1->clusterEnabled = true;
    cfg1->pauseTimeIndexMgr = 1;
    cfg1->rocksBlockcacheMB = 24;
    // this test will migrate back
    cfg1->enableGcInMigate = true;

    auto master = std::make_shared<ServerEntry>(cfg1);
    auto s = master->startup(cfg1);
    if (!s.ok()) {
      LOG(ERROR) << "server start fail:" << s.toString();
    }
    INVARIANT(s.ok());
    servers.emplace_back(std::move(master));
  }

  auto& srcNode = servers[0];
  auto& dstNode = servers[1];

  auto ctx1 = std::make_shared<asio::io_context>();
  auto sess1 = makeSession(srcNode, ctx1);
  WorkLoad work1(srcNode, sess1);
  work1.init();
  auto ctx2 = std::make_shared<asio::io_context>();
  auto sess2 = makeSession(dstNode, ctx2);
  WorkLoad work2(dstNode, sess2);
  work2.init();

  auto ret = work1.getStringResult(
    {"syncversion", "nodeid", std::to_string(100), std::to_string(120), "v1"});
  EXPECT_EQ(ret, "+OK\r\n");

  ret = work2.getStringResult(
    {"syncversion", "nodeid", std::to_string(10), std::to_string(12), "v1"});
  EXPECT_EQ(ret, "+OK\r\n");

  // addSlots
  LOG(INFO) << "begin meet";
  work1.clusterMeet(dstNode->getParams()->bindIp, dstNode->getParams()->port);
  std::this_thread::sleep_for(std::chrono::seconds(10));

  std::vector<std::string> slots = {"{0..9300}", "{9301..16383}"};

  // addSlots
  LOG(INFO) << "begin addSlots.";
  work1.addSlots(slots[0]);
  work2.addSlots(slots[1]);
  LOG(INFO) << "add slots sucess";
  std::this_thread::sleep_for(std::chrono::seconds(10));

  std::vector<uint32_t> slotsList = {5970, 5980, 6000, 6234, 6522, 7000, 8373};

  auto bitmap = getBitSet(slotsList);

  const uint32_t numData = 20000;
  // for support MOVED
  string srcAddr =
    srcNode->getParams()->bindIp + ":" + to_string(srcNode->getParams()->port);
  string dstAddr =
    dstNode->getParams()->bindIp + ":" + to_string(dstNode->getParams()->port);
  work1.addClusterSession(srcAddr, sess1);
  work1.addClusterSession(dstAddr, sess2);
  work2.addClusterSession(srcAddr, sess1);
  work2.addClusterSession(dstAddr, sess2);

  for (size_t j = 0; j < numData; ++j) {
    string key;
    if (j % 2) {
      // write to slot 8373
      key = getUUid(8) + "{12}";
    } else {
      // write to slot 5970
      key = getUUid(8) + "{123}";
    }
    string value = getUUid(7);
    auto ret = work1.getStringResult({"set", key, value});
    EXPECT_EQ(ret, "+OK\r\n");

    // begin to migate when  half data been writen
    if (j == numData / 2) {
      uint32_t keysize = 0;
      for (auto& vs : slotsList) {
        keysize += srcNode->getClusterMgr()->countKeysInSlot(vs);
      }
      LOG(INFO) << "before migrate keys num:" << keysize;
      auto s = migrate(srcNode, dstNode, bitmap);
      EXPECT_TRUE(s.ok());
    }
  }

  std::this_thread::sleep_for(10s);

  uint32_t keysize1 = 0;
  uint32_t keysize2 = 0;
  for (auto& vs : slotsList) {
    LOG(INFO) << "node2->getClusterMgr()->countKeysInSlot:" << vs
              << "is:" << dstNode->getClusterMgr()->countKeysInSlot(vs);
    keysize2 += dstNode->getClusterMgr()->countKeysInSlot(vs);
  }

  // bitmap should belong to dstNode
  ASSERT_EQ(checkSlotsBlong(
              bitmap,
              srcNode,
              srcNode->getClusterMgr()->getClusterState()->getMyselfName()),
            false);
  ASSERT_EQ(checkSlotsBlong(
              bitmap,
              dstNode,
              dstNode->getClusterMgr()->getClusterState()->getMyselfName()),
            true);
  // dstNode should contain the keys
  ASSERT_EQ(keysize2, numData);

  // migrate from dstNode to srcNode back
  keysize1 = 0;
  keysize2 = 0;

  for (size_t j = 0; j < numData; ++j) {
    string key;
    if (j % 2) {
      // write to slot 8373
      key = getUUid(8) + "{12}";
    } else {
      // write to slot 5970
      key = getUUid(8) + "{123}";
    }
    string value = getUUid(7);
    auto ret = work2.getStringResult({"set", key, value});
    EXPECT_EQ(ret, "+OK\r\n");

    // begin to migate when  half data been writen
    if (j == numData / 2) {
      auto s = migrate(dstNode, srcNode, bitmap);
      EXPECT_TRUE(s.ok());
    }
  }
  std::this_thread::sleep_for(10s);

  for (auto& vs : slotsList) {
    keysize1 += dstNode->getClusterMgr()->countKeysInSlot(vs);
    keysize2 += srcNode->getClusterMgr()->countKeysInSlot(vs);
  }

  // bitmap should belong to dstNode
  ASSERT_EQ(checkSlotsBlong(
              bitmap,
              srcNode,
              srcNode->getClusterMgr()->getClusterState()->getMyselfName()),
            true);
  ASSERT_EQ(checkSlotsBlong(
              bitmap,
              dstNode,
              dstNode->getClusterMgr()->getClusterState()->getMyselfName()),
            false);
  // srcNode should contain the keys
  ASSERT_EQ(keysize2, numData * 2);
  auto meta1 = work1.getStringResult({"syncversion", "nodeid", "?", "?", "v1"});
  auto meta2 = work2.getStringResult({"syncversion", "nodeid", "?", "?", "v1"});
  ASSERT_EQ(meta1, meta2);
  std::this_thread::sleep_for(5s);


#ifndef _WIN32
  for (auto svr : servers) {
    svr->stop();
    LOG(INFO) << "stop " << svr->getParams()->port << " success";
  }
#endif
  LOG(INFO) << "stop servers here";
  servers.clear();
}

TEST(Cluster, stopMigrate) {
  std::vector<std::string> dirs = {"node1", "node2"};
  uint32_t startPort = 15000;

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

  auto& srcNode = servers[0];
  auto& dstNode = servers[1];

  auto ctx1 = std::make_shared<asio::io_context>();
  auto sess1 = makeSession(srcNode, ctx1);
  WorkLoad work1(srcNode, sess1);
  work1.init();
  auto ctx2 = std::make_shared<asio::io_context>();
  auto sess2 = makeSession(dstNode, ctx2);
  WorkLoad work2(dstNode, sess2);
  work2.init();

  auto ret = work1.getStringResult(
    {"syncversion", "nodeid", std::to_string(100), std::to_string(120), "v1"});
  EXPECT_EQ(ret, "+OK\r\n");

  ret = work2.getStringResult(
    {"syncversion", "nodeid", std::to_string(10), std::to_string(12), "v1"});
  EXPECT_EQ(ret, "+OK\r\n");

  // meet
  work1.clusterMeet(dstNode->getParams()->bindIp, dstNode->getParams()->port);
  std::this_thread::sleep_for(std::chrono::seconds(10));

  std::vector<std::string> slots = {"{0..9300}", "{9301..16383}"};
  // addSlots
  work1.addSlots(slots[0]);
  work2.addSlots(slots[1]);

  std::this_thread::sleep_for(std::chrono::seconds(10));

  std::vector<uint32_t> slotsList = {5970, 5980, 6000, 6234, 6522, 7000, 8373};

  auto bitmap = getBitSet(slotsList);

  const uint32_t numData = 20000;
  std::string taskid;
  for (size_t j = 0; j < numData; ++j) {
    string key;
    if (j % 2) {
      // write to slot 8373
      key = getUUid(8) + "{12}";
    } else {
      // write to slot 5970
      key = getUUid(8) + "{123}";
    }
    string value = getUUid(7);
    auto ret = work1.getStringResult({"set", key, value});
    EXPECT_EQ(ret, "+OK\r\n");
  }
  auto exptTaskid = migrate(srcNode, dstNode, bitmap);
  EXPECT_TRUE(exptTaskid.ok());
  /* NOTE(wayenchen) first we stop migrate by new command(cluster setslot stop),
   * the working task num of this taskid should be 0,
   * than use (cluster setslot retry) to continue jobs
   * finally all the tasks should be done */
  std::this_thread::sleep_for(500ms);
  work2.stopMigrate(taskid);
  std::this_thread::sleep_for(3s);

  auto taskNum1 = srcNode->getMigrateManager()->getTaskNum(taskid);
  EXPECT_EQ(taskNum1, 0);
  auto taskNum2 = dstNode->getMigrateManager()->getTaskNum(taskid);
  EXPECT_EQ(taskNum2, 0);

  exptTaskid = migrate(srcNode, dstNode, bitmap, true);
  EXPECT_TRUE(exptTaskid.ok());
  std::this_thread::sleep_for(40s);

  uint32_t keysize = 0;
  for (auto& vs : slotsList) {
    LOG(INFO) << "node2->getClusterMgr()->countKeysInSlot:" << vs
              << "is:" << dstNode->getClusterMgr()->countKeysInSlot(vs);
    keysize += dstNode->getClusterMgr()->countKeysInSlot(vs);
  }

  // bitmap should belong to dstNode
  ASSERT_EQ(checkSlotsBlong(
              bitmap,
              srcNode,
              srcNode->getClusterMgr()->getClusterState()->getMyselfName()),
            false);
  ASSERT_EQ(checkSlotsBlong(
              bitmap,
              dstNode,
              dstNode->getClusterMgr()->getClusterState()->getMyselfName()),
            true);
  // dstNode should contain the keys
  ASSERT_EQ(keysize, numData);

#ifndef _WIN32
  for (auto svr : servers) {
    svr->stop();
    LOG(INFO) << "stop " << svr->getParams()->port << " success";
  }
#endif
  LOG(INFO) << "stop servers here";
  servers.clear();
}

TEST(Cluster, stopAllMigrate) {
  std::vector<std::string> dirs = {"node1", "node2"};
  uint32_t startPort = 15000;

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

  auto& srcNode = servers[0];
  auto& dstNode = servers[1];

  auto ctx1 = std::make_shared<asio::io_context>();
  auto sess1 = makeSession(srcNode, ctx1);
  WorkLoad work1(srcNode, sess1);
  work1.init();
  auto ctx2 = std::make_shared<asio::io_context>();
  auto sess2 = makeSession(dstNode, ctx2);
  WorkLoad work2(dstNode, sess2);
  work2.init();

  auto ret = work1.getStringResult(
    {"syncversion", "nodeid", std::to_string(100), std::to_string(120), "v1"});
  EXPECT_EQ(ret, "+OK\r\n");

  ret = work2.getStringResult(
    {"syncversion", "nodeid", std::to_string(10), std::to_string(12), "v1"});
  EXPECT_EQ(ret, "+OK\r\n");

  // meet
  work1.clusterMeet(dstNode->getParams()->bindIp, dstNode->getParams()->port);
  std::this_thread::sleep_for(std::chrono::seconds(10));

  std::vector<std::string> slots = {"{0..9300}", "{9301..16383}"};
  // addSlots
  work1.addSlots(slots[0]);
  work2.addSlots(slots[1]);

  std::this_thread::sleep_for(std::chrono::seconds(10));

  std::vector<uint32_t> slotsList = {5970, 5980, 6000, 6234, 6522, 7000, 8373};

  auto bitmap = getBitSet(slotsList);

  const uint32_t numData = 30000;
  std::string taskid;
  for (size_t j = 0; j < numData; ++j) {
    string key;
    if (j % 2) {
      // write to slot 8373
      key = getUUid(8) + "{12}";
    } else {
      // write to slot 5970
      key = getUUid(8) + "{123}";
    }
    string value = getUUid(7);
    auto ret = work1.getStringResult({"set", key, value});
    EXPECT_EQ(ret, "+OK\r\n");
  }
  auto exptTaskid = migrate(srcNode, dstNode, bitmap);
  EXPECT_TRUE(exptTaskid.ok());
  /* NOTE(wayenchen) first we stop migrate by new command(cluster setslot
   * stopall), the working task num of this taskid should be 0, than use
   * (cluster setslot restartall) to continue jobs
   * finally all the tasks should be done */
  std::this_thread::sleep_for(500ms);
  work2.stopAllMigTasks();
  std::this_thread::sleep_for(1s);
  work1.stopAllMigTasks();
  std::this_thread::sleep_for(3s);
  auto taskNum1 = srcNode->getMigrateManager()->getTaskNum(taskid);
  EXPECT_EQ(taskNum1, 0);
  auto taskNum2 = dstNode->getMigrateManager()->getTaskNum(taskid);
  EXPECT_EQ(taskNum2, 0);

  work2.restartAllMigTasks();
  std::this_thread::sleep_for(40s);
  uint32_t keysize = 0;
  for (auto& vs : slotsList) {
    LOG(INFO) << "node2->getClusterMgr()->countKeysInSlot:" << vs
              << "is:" << dstNode->getClusterMgr()->countKeysInSlot(vs);
    keysize += dstNode->getClusterMgr()->countKeysInSlot(vs);
  }
  // dstNode should contain the keys
  ASSERT_EQ(keysize, numData);
  // bitmap should belong to dstNode
  ASSERT_EQ(checkSlotsBlong(
              bitmap,
              srcNode,
              srcNode->getClusterMgr()->getClusterState()->getMyselfName()),
            false);
  ASSERT_EQ(checkSlotsBlong(
              bitmap,
              dstNode,
              dstNode->getClusterMgr()->getClusterState()->getMyselfName()),
            true);


#ifndef _WIN32
  for (auto svr : servers) {
    svr->stop();
    LOG(INFO) << "stop " << svr->getParams()->port << " success";
  }
#endif
  LOG(INFO) << "stop servers here";
  servers.clear();
}

TEST(Cluster, migrateAndImport) {
  std::vector<std::string> dirs = {"node1", "node2", "node3"};
  uint32_t startPort = 14100;

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

  auto& srcNode = servers[0];
  auto& dstNode1 = servers[1];
  auto& dstNode2 = servers[2];

  auto ctx1 = std::make_shared<asio::io_context>();
  auto sess1 = makeSession(srcNode, ctx1);
  WorkLoad work1(srcNode, sess1);
  work1.init();
  auto ctx2 = std::make_shared<asio::io_context>();
  auto sess2 = makeSession(dstNode1, ctx2);
  WorkLoad work2(dstNode1, sess2);
  work2.init();
  auto ctx3 = std::make_shared<asio::io_context>();
  auto sess3 = makeSession(dstNode2, ctx3);
  WorkLoad work3(dstNode2, sess3);
  work3.init();

  // addSlots
  LOG(INFO) << "begin meet";
  work1.clusterMeet(dstNode1->getParams()->bindIp, dstNode1->getParams()->port);
  work1.clusterMeet(dstNode2->getParams()->bindIp, dstNode2->getParams()->port);
  std::this_thread::sleep_for(std::chrono::seconds(10));

  std::vector<std::string> slots = {
    "{0..4700}", "{4701..10000}", "{10001..16383}"};

  // addSlots
  LOG(INFO) << "begin addSlots.";
  work1.addSlots(slots[1]);
  work2.addSlots(slots[0]);
  work3.addSlots(slots[2]);

  LOG(INFO) << "add slots sucess";
  std::this_thread::sleep_for(std::chrono::seconds(10));

  std::vector<uint32_t> slotsList1 = {5970, 5980, 6000, 6234, 6522, 7000, 8373};
  std::vector<uint32_t> slotsList2 = {513, 1000, 1239, 2000, 4640};
  auto bitmap1 = getBitSet(slotsList1);
  auto bitmap2 = getBitSet(slotsList2);
  const uint32_t numData = 10000;

  // for support MOVED
  string srcAddr =
    srcNode->getParams()->bindIp + ":" + to_string(srcNode->getParams()->port);
  string dstAddr1 = dstNode1->getParams()->bindIp + ":" +
    to_string(dstNode1->getParams()->port);
  string dstAddr2 = dstNode2->getParams()->bindIp + ":" +
    to_string(dstNode2->getParams()->port);
  work1.addClusterSession(srcAddr, sess1);
  work1.addClusterSession(dstAddr1, sess2);
  work1.addClusterSession(dstAddr2, sess3);
  work2.addClusterSession(srcAddr, sess1);
  work2.addClusterSession(dstAddr1, sess2);
  work2.addClusterSession(dstAddr2, sess3);
  work3.addClusterSession(srcAddr, sess1);
  work3.addClusterSession(dstAddr1, sess2);
  work3.addClusterSession(dstAddr2, sess3);

  for (size_t j = 0; j < numData; ++j) {
    string key;
    string key2;
    if (j % 2) {
      // write to slot 8373
      key = getUUid(8) + "{12}";
      // write to slot 5970
      key2 = getUUid(8) + "{123}";
    } else {
      // write to slot 4640
      key = getUUid(8) + "{112}";
      // write to slot 513
      key2 = getUUid(8) + "{113}";
    }
    string value = getUUid(7);
    auto ret = work1.getStringResult({"set", key, value});
    EXPECT_EQ(ret, "+OK\r\n");
    ret = work1.getStringResult({"set", key2, value});
    EXPECT_EQ(ret, "+OK\r\n");

    // begin to migate when  half data been writen
    if (j == numData / 2) {
      uint32_t keysize = 0;
      for (auto& vs : slotsList1) {
        keysize += srcNode->getClusterMgr()->countKeysInSlot(vs);
      }
      LOG(INFO) << "before first migrate keys num:" << keysize;
      auto s1 = migrate(srcNode, dstNode1, bitmap1);
      EXPECT_TRUE(s1.ok());

      std::this_thread::sleep_for(1s);
      uint32_t keysize2 = 0;
      for (auto& vs : slotsList2) {
        keysize2 += dstNode1->getClusterMgr()->countKeysInSlot(vs);
      }
      LOG(INFO) << "before second migrate keys num:" << keysize;
      auto s2 = migrate(dstNode1, dstNode2, bitmap2);
      EXPECT_TRUE(s2.ok());
    }
  }

  std::this_thread::sleep_for(10s);

  uint32_t keysize1 = 0;
  uint32_t keysize2 = 0;
  for (auto& vs : slotsList1) {
    LOG(INFO) << "first migrate src slot:" << vs
              << "is:" << srcNode->getClusterMgr()->countKeysInSlot(vs);
    keysize1 += srcNode->getClusterMgr()->countKeysInSlot(vs);
    LOG(INFO) << "first migrate dst slot:" << vs
              << "is:" << dstNode1->getClusterMgr()->countKeysInSlot(vs);
    keysize2 += dstNode1->getClusterMgr()->countKeysInSlot(vs);
  }
  // bitmap should belong to dstNode
  ASSERT_EQ(checkSlotsBlong(
              bitmap1,
              srcNode,
              srcNode->getClusterMgr()->getClusterState()->getMyselfName()),
            false);
  ASSERT_EQ(checkSlotsBlong(
              bitmap1,
              dstNode1,
              dstNode1->getClusterMgr()->getClusterState()->getMyselfName()),
            true);
  // dstNode should contain the keys
  ASSERT_EQ(keysize2, numData);

  keysize1 = 0;
  keysize2 = 0;
  for (auto& vs : slotsList2) {
    LOG(INFO) << "second migrate src slot:" << vs
              << "is:" << dstNode1->getClusterMgr()->countKeysInSlot(vs);
    keysize1 += dstNode1->getClusterMgr()->countKeysInSlot(vs);
    LOG(INFO) << "second migrate dst slot:" << vs
              << "is:" << dstNode2->getClusterMgr()->countKeysInSlot(vs);
    keysize2 += dstNode2->getClusterMgr()->countKeysInSlot(vs);
  }

  // bitmap should belong to dstNode
  ASSERT_EQ(checkSlotsBlong(
              bitmap2,
              dstNode1,
              dstNode1->getClusterMgr()->getClusterState()->getMyselfName()),
            false);
  ASSERT_EQ(checkSlotsBlong(
              bitmap2,
              dstNode2,
              dstNode2->getClusterMgr()->getClusterState()->getMyselfName()),
            true);
  // dstNode should contain the keys
  // NOTE(wayenchen) delelte key may delay in master, not expected zero here
  ASSERT_EQ(keysize2, numData);

#ifndef _WIN32
  for (auto svr : servers) {
    svr->stop();
    LOG(INFO) << "stop " << svr->getParams()->port << " success";
  }
#endif

  servers.clear();
}

void testDeleteChunks(std::shared_ptr<ServerEntry> svr,
                      std::vector<uint32_t> slotsList) {
  for (size_t i = 0; i < slotsList.size(); ++i) {
    uint64_t c = svr->getClusterMgr()->countKeysInSlot(slotsList[i]);
    LOG(INFO) << "slot:" << slotsList[i] << " keys count before delete:" << c;
  }
  auto bitmap = getBitSet(slotsList);
  svr->getGcMgr()->deleteBitMap(bitmap, 0);
  // TODO(wayenchen) ccheck deleteTask state
  std::this_thread::sleep_for(std::chrono::seconds(10));

  for (size_t i = 0; i < slotsList.size(); ++i) {
    uint64_t c = svr->getClusterMgr()->countKeysInSlot(slotsList[i]);
    EXPECT_EQ(c, 0);
  }
}

void testDeleteRange(std::shared_ptr<ServerEntry> svr,
                     uint32_t storeid,
                     uint32_t start,
                     uint32_t end) {
  svr->getGcMgr()->deleteChunks(storeid, start, end);
  std::this_thread::sleep_for(std::chrono::seconds(8));
  for (size_t i = start; i <= end; ++i) {
    if (svr->getSegmentMgr()->getStoreid(i) == storeid) {
      uint64_t c = svr->getClusterMgr()->countKeysInSlot(i);
      EXPECT_EQ(c, 0);
    }
  }
}

TEST(Cluster, deleteChunks) {
  std::vector<std::string> dirs = {"node1"};
  uint32_t startPort = 14200;

  const auto guard = MakeGuard([dirs] {
    for (auto dir : dirs) {
      destroyEnv(dir);
    }
    std::this_thread::sleep_for(std::chrono::seconds(5));
  });

  std::vector<std::shared_ptr<ServerEntry>> servers;

  uint32_t index = 0;
  storeCnt = 10;
  for (auto dir : dirs) {
    uint32_t nodePort = startPort + index++;
    servers.emplace_back(
      std::move(makeClusterNode(dir, nodePort, storeCnt, false)));
  }

  auto& srcNode = servers[0];

  auto ctx1 = std::make_shared<asio::io_context>();
  auto sess1 = makeSession(srcNode, ctx1);
  WorkLoad work1(srcNode, sess1);
  work1.init();

  // addSlots
  LOG(INFO) << "begin addSlots.";
  work1.addSlots("{0..16383}");
  LOG(INFO) << "add slots sucess";
  std::this_thread::sleep_for(std::chrono::seconds(6));

  const uint32_t numData = 1000000;

  LOG(INFO) << "begin add data.";
  auto kv_keys = work1.writeWork(RecordType::RT_KV, numData);
  LOG(INFO) << "end add data.";

  std::this_thread::sleep_for(5s);

  testDeleteChunks(srcNode, {5000});
  testDeleteChunks(srcNode, {5200, 5210, 5220, 5280});
  testDeleteChunks(srcNode, {5130, 5131, 5132, 5133, 5134, 5140, 5151, 5142});
  testDeleteChunks(
    srcNode,
    {5300, 5310, 5320, 5333, 5964, 5740, 5251, 5261, 5271, 9900, 9910, 8888});
  testDeleteChunks(srcNode,
                   {5200,
                    5210,
                    5220,
                    5280,
                    5300,
                    5310,
                    5320,
                    5330,
                    5340,
                    3000,
                    3010,
                    3020,
                    3088,
                    2033,
                    9000,
                    9010});

  auto storeid1 = srcNode->getSegmentMgr()->getStoreid(6000);
  auto storeid2 = srcNode->getSegmentMgr()->getStoreid(6200);

  EXPECT_TRUE(storeid1 == storeid2);
  testDeleteRange(srcNode, storeid1, 6000, 6200);

#ifndef _WIN32
  for (auto svr : servers) {
    svr->stop();
    LOG(INFO) << "stop " << svr->getParams()->port << " success";
  }
#endif
  servers.clear();
}

TEST(Cluster, ErrStoreNum) {
  std::vector<std::string> dirs = {"node1", "node2"};
  uint32_t startPort = 17500;

  const auto guard = MakeGuard([dirs] {
    for (auto dir : dirs) {
      destroyEnv(dir);
    }
    std::this_thread::sleep_for(std::chrono::seconds(5));
  });

  std::vector<std::shared_ptr<ServerEntry>> servers;

  uint32_t index = 0;
  // make server store number different
  for (auto dir : dirs) {
    uint32_t nodePort = startPort + index++;
    if (nodePort % 2) {
      servers.emplace_back(
        std::move(makeClusterNode(dir, nodePort, storeCnt1)));
    } else {
      servers.emplace_back(
        std::move(makeClusterNode(dir, nodePort, storeCnt2)));
    }
  }

  auto& srcNode = servers[0];
  auto& dstNode = servers[1];

  auto ctx1 = std::make_shared<asio::io_context>();
  auto sess1 = makeSession(srcNode, ctx1);
  WorkLoad work1(srcNode, sess1);
  work1.init();

  work1.clusterMeet(dstNode->getParams()->bindIp, dstNode->getParams()->port);
  std::this_thread::sleep_for(std::chrono::seconds(10));

  std::vector<std::string> slots = {"{0..9300}", "{9301..16383}"};

  work1.addSlots(slots[0]);
  std::this_thread::sleep_for(std::chrono::seconds(10));


  auto ctx2 = std::make_shared<asio::io_context>();
  auto sess2 = makeSession(dstNode, ctx2);
  WorkLoad work2(dstNode, sess2);
  work2.init();
  work2.addSlots(slots[1]);

  std::this_thread::sleep_for(std::chrono::seconds(10));

  std::vector<uint32_t> slotsList = {5970, 5980, 6000, 6234, 6522, 7000, 8373};

  auto bitmap = getBitSet(slotsList);

  auto s = migrate(srcNode, dstNode, bitmap);
  EXPECT_TRUE(!s.ok());

  std::this_thread::sleep_for(3s);
  // migrte should fail
  ASSERT_EQ(checkSlotsBlong(
              bitmap,
              srcNode,
              srcNode->getClusterMgr()->getClusterState()->getMyselfName()),
            true);
  ASSERT_EQ(checkSlotsBlong(
              bitmap,
              dstNode,
              dstNode->getClusterMgr()->getClusterState()->getMyselfName()),
            false);

#ifndef _WIN32
  for (auto svr : servers) {
    svr->stop();
    LOG(INFO) << "stop " << svr->getParams()->port << " success";
  }
#endif

  servers.clear();
}

void checkEpoch(std::vector<std::shared_ptr<ServerEntry>> servers,
                uint32_t nodeNum,
                uint32_t migrateSlot,
                uint32_t srcNodeIndex,
                uint32_t dstNodeIndex) {
  int32_t num = 0;
  int32_t begin = INT32_MAX;
  int32_t end = 0;
  while (num++ < 300) {
    uint32_t oldNodeNum = 0;
    uint32_t updatedNodeNum = 0;
    auto dstNodeName = servers[dstNodeIndex]
                         ->getClusterMgr()
                         ->getClusterState()
                         ->getMyselfName();
    auto srcNodeName = servers[srcNodeIndex]
                         ->getClusterMgr()
                         ->getClusterState()
                         ->getMyselfName();
    for (uint32_t i = 0; i < servers.size(); ++i) {
      auto state = servers[i]->getClusterMgr()->getClusterState();
      CNodePtr dstNode = state->clusterLookupNode(dstNodeName);
      CNodePtr srcNode = state->clusterLookupNode(srcNodeName);

      if (dstNode != nullptr && state->getNodeBySlot(migrateSlot) == dstNode) {
        updatedNodeNum++;
      } else if (srcNode != nullptr &&
                 state->getNodeBySlot(migrateSlot) == srcNode) {
        oldNodeNum++;
      }
    }
    LOG(INFO) << "checkEpoch, updatedNodeNum:" << updatedNodeNum
              << " oldNodeNum:" << oldNodeNum;
    if (updatedNodeNum != 0 && begin == INT32_MAX) {
      begin = num;
    }
    std::map<uint32_t, uint32_t> mapCurrentEpoch;
    for (uint32_t i = 0; i < servers.size(); ++i) {
      uint32_t currentEpoch =
        servers[i]->getClusterMgr()->getClusterState()->getCurrentEpoch();
      if (mapCurrentEpoch.find(currentEpoch) == mapCurrentEpoch.end()) {
        mapCurrentEpoch[currentEpoch] = 1;
      } else {
        mapCurrentEpoch[currentEpoch]++;
      }
    }
    stringstream ss;
    for (auto epoch : mapCurrentEpoch) {
      ss << " " << epoch.first << "|" << epoch.second;
    }
    LOG(INFO) << "checkEpoch, currentEpoch|nodeNum pairs:" << ss.str();
    if (updatedNodeNum == servers.size()) {
      end = num;
      LOG(INFO) << "checkEpoch, all updated, time:" << end - begin
                << " begin:" << begin << " end:" << end;
      break;
    }
    std::this_thread::sleep_for(1s);
  }
  EXPECT_NE(begin, INT32_MAX);
  EXPECT_NE(end, 0);
  EXPECT_LT((end - begin), 60);
}

// Convergence rate test
TEST(Cluster, ConvergenceRate) {
  uint32_t nodeNum = 30;
  uint32_t migrateSlot = 8373;
  uint32_t startPort = 14300;
  uint32_t dstNodeIndex = 0;
  uint32_t srcNodeIndex = migrateSlot / (CLUSTER_SLOTS / nodeNum);

  LOG(INFO) << "ConvergenceRate nodeNum:" << nodeNum
            << " migrateSlot:" << migrateSlot
            << " srcNodeIndex:" << srcNodeIndex
            << " dstNodeIndex:" << dstNodeIndex;
  std::vector<std::string> dirs;
  for (uint32_t i = 0; i < nodeNum; ++i) {
    dirs.push_back("node" + to_string(i));
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

  std::thread th1(
    [&servers, nodeNum, migrateSlot, srcNodeIndex, dstNodeIndex]() {
      checkEpoch(servers, nodeNum, migrateSlot, srcNodeIndex, dstNodeIndex);
    });

  // meet
  LOG(INFO) << "begin meet.";
  for (uint32_t i = 1; i < nodeNum; ++i) {
    auto ctx = std::make_shared<asio::io_context>();
    auto sess = makeSession(servers[0], ctx);
    WorkLoad work(servers[0], sess);
    work.init();
    work.clusterMeet(servers[i]->getParams()->bindIp,
                     servers[i]->getParams()->port);
  }
  std::this_thread::sleep_for(std::chrono::seconds(10));

  // addSlots
  LOG(INFO) << "begin addSlots.";
  for (uint32_t i = 0; i < nodeNum; ++i) {
    auto ctx = std::make_shared<asio::io_context>();
    auto sess = makeSession(servers[i], ctx);
    WorkLoad work(servers[i], sess);
    work.init();
    uint32_t start = CLUSTER_SLOTS / nodeNum * i;
    uint32_t end = start + CLUSTER_SLOTS / nodeNum - 1;
    if (i == nodeNum - 1) {
      end = CLUSTER_SLOTS - 1;
    }
    std::string slots = "{" + to_string(start) + ".." + to_string(end) + "}";
    work.addSlots(slots);
    LOG(INFO) << "addSlots " << i << " " << slots;
  }
  // 30 nodes, wait 20 seconds is not long enough
  std::this_thread::sleep_for(std::chrono::seconds(50));

  auto& srcNode = servers[srcNodeIndex];
  auto& dstNode = servers[dstNodeIndex];

  auto ctx1 = std::make_shared<asio::io_context>();
  auto sess1 = makeSession(srcNode, ctx1);
  WorkLoad work1(srcNode, sess1);
  work1.init();

  auto ctx2 = std::make_shared<asio::io_context>();
  auto sess2 = makeSession(dstNode, ctx2);
  WorkLoad work2(dstNode, sess2);
  work2.init();

  std::vector<uint32_t> slotsList = {
    migrateSlot - 1, migrateSlot, migrateSlot + 1};
  auto bitmap = getBitSet(slotsList);

  // for support MOVED
  string srcAddr =
    srcNode->getParams()->bindIp + ":" + to_string(srcNode->getParams()->port);
  string dstAddr =
    dstNode->getParams()->bindIp + ":" + to_string(dstNode->getParams()->port);
  work1.addClusterSession(srcAddr, sess1);
  work1.addClusterSession(dstAddr, sess2);
  work2.addClusterSession(srcAddr, sess1);
  work2.addClusterSession(dstAddr, sess2);

  LOG(INFO) << "begin add keys.";
  const uint32_t numData = 1000;
  for (size_t j = 0; j < numData; ++j) {
    string key;
    key = to_string(j) + "{12}";
    string value = getUUid(7);
    auto ret = work1.getStringResult({"set", key, value});
    EXPECT_EQ(ret, "+OK\r\n");

    // begin to migrate when half data been writen
    if (j == numData / 2) {
      uint32_t keysize = 0;
      for (auto& vs : slotsList) {
        keysize += srcNode->getClusterMgr()->countKeysInSlot(vs);
      }
      LOG(INFO) << "before migrate keys num:" << keysize;
      auto s = migrate(srcNode, dstNode, bitmap);
      EXPECT_TRUE(s.ok());
    }
  }
  LOG(INFO) << "end add keys.";

  th1.join();

  std::this_thread::sleep_for(2s);

  LOG(INFO) << "srdNode MovedNum:" << srcNode->getSegmentMgr()->getMovedNum();
  uint32_t keysize1 = 0;
  uint32_t keysize2 = 0;
  for (auto& slot : slotsList) {
    LOG(INFO) << "srdNode slot:" << slot
              << " keys:" << srcNode->getClusterMgr()->countKeysInSlot(slot);
    keysize1 += srcNode->getClusterMgr()->countKeysInSlot(slot);
    LOG(INFO) << "dstNode slot:" << slot
              << " keys:" << dstNode->getClusterMgr()->countKeysInSlot(slot);
    keysize2 += dstNode->getClusterMgr()->countKeysInSlot(slot);
  }

  // bitmap should belong to dstNode
  ASSERT_EQ(checkSlotsBlong(
              bitmap,
              srcNode,
              srcNode->getClusterMgr()->getClusterState()->getMyselfName()),
            false);
  ASSERT_EQ(checkSlotsBlong(
              bitmap,
              dstNode,
              dstNode->getClusterMgr()->getClusterState()->getMyselfName()),
            true);
  // dstNode should contain the keys
  ASSERT_EQ(keysize2, numData);

#ifndef _WIN32
  for (auto svr : servers) {
    svr->stop();
    LOG(INFO) << "stop " << svr->getParams()->port << " success";
  }
#endif
  servers.clear();
}

TEST(Cluster, MigrateTTLIndex) {
  uint32_t nodeNum = 2;
  uint32_t migrateSlot = 8373;
  uint32_t startPort = 19000;

  LOG(INFO) << "MigrateTTLIndex begin.";
  std::vector<std::string> dirs;
  for (uint32_t i = 0; i < nodeNum; ++i) {
    dirs.push_back("node" + to_string(i));
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

  auto ctx1 = std::make_shared<asio::io_context>();
  auto sess1 = makeSession(servers[0], ctx1);
  WorkLoad work1(servers[0], sess1);
  work1.init();
  auto ctx2 = std::make_shared<asio::io_context>();
  auto sess2 = makeSession(servers[1], ctx2);
  WorkLoad work2(servers[1], sess2);
  work2.init();

  // meet
  LOG(INFO) << "begin meet.";
  work1.clusterMeet(servers[1]->getParams()->bindIp,
                    servers[1]->getParams()->port);
  std::this_thread::sleep_for(std::chrono::seconds(10));

  // addSlots
  LOG(INFO) << "begin addSlots.";
  work1.addSlots("{0..16382}");
  work2.addSlots("16383");
  // TODO(takenliu): why need 7 seconds for cluster state change to ok,
  // "CLUSTERDOWN" ???
  std::this_thread::sleep_for(std::chrono::seconds(10));

  LOG(INFO) << "begin add keys.";
  const uint32_t numData = 10;
  for (size_t j = 0; j < numData; ++j) {
    // write to slot 8373
    string key = to_string(j) + "{12}";
    string listkey = "list" + to_string(j) + "{12}";

    auto ret = work1.getStringResult({"set", key, "value"});
    EXPECT_EQ(ret, "+OK\r\n");

    ret = work1.getStringResult({"expire", key, "10"});
    EXPECT_EQ(ret, ":1\r\n");

    ret = work1.getStringResult({"lpush", listkey, "1", "2", "3"});
    EXPECT_EQ(ret, ":3\r\n");

    ret = work1.getStringResult({"expire", listkey, "10"});
    EXPECT_EQ(ret, ":1\r\n");
  }
  LOG(INFO) << "end add keys.";

  // migrate
  std::vector<uint32_t> slotsList = {
    migrateSlot - 1, migrateSlot, migrateSlot + 1};
  auto bitmap = getBitSet(slotsList);
  auto s = migrate(servers[0], servers[1], bitmap);
  std::this_thread::sleep_for(2s);

  auto dbsize =
    work2.getIntResult({"dbsize", "containexpire", "containsubkey"});
  // {key, list_meta, list_ele * 3} * numData
  EXPECT_EQ(dbsize.value(), numData + numData * 4);

  // tryDelExpiredKeysJob() is called every 10s
  std::this_thread::sleep_for(12s);

  dbsize = work2.getIntResult({"dbsize", "containexpire", "containsubkey"});
  // RT_LIST_META and RT_LIST_ELE will be deleted.
  EXPECT_EQ(dbsize.value(), numData);

  auto ret = work2.getStringResult({"compactSlots", "8000", "10000"});
  EXPECT_EQ(ret, Command::fmtOK());

  dbsize = work2.getIntResult({"dbsize"});
  // all is expired.
  EXPECT_EQ(dbsize.value(), 0);

#ifndef _WIN32
  for (auto svr : servers) {
    svr->stop();
    LOG(INFO) << "stop " << svr->getParams()->port << " success";
  }
#endif
  servers.clear();
}

TEST(Cluster, ChangeMaster) {
  uint32_t nodeNum = 3;
  uint32_t startPort = 15200;

  const auto guard = MakeGuard([&nodeNum] {
    destroyCluster(nodeNum);
    std::this_thread::sleep_for(std::chrono::seconds(5));
  });

  auto servers = makeCluster(startPort, nodeNum, 10, true);
  // 3 master and 3 slave *, make one master fail
  auto& node1 = servers[0];
  auto& node2 = servers[3];
  // add one slave
  auto node7 = makeClusterNode("node6", startPort + 6, storeCnt2);
  auto ctx1 = std::make_shared<asio::io_context>();
  auto sess1 = makeSession(node1, ctx1);
  WorkLoad work1(node1, sess1);
  work1.init();

  work1.clusterMeet(node7->getParams()->bindIp, node7->getParams()->port);
  std::this_thread::sleep_for(10s);

  auto ctx2 = std::make_shared<asio::io_context>();
  auto sess2 = makeSession(node7, ctx2);
  WorkLoad work2(node7, sess2);
  work2.init();
  work2.clusterMeet(node1->getParams()->bindIp, node1->getParams()->port);
  auto nodeName1 = node1->getClusterMgr()->getClusterState()->getMyselfName();
  work2.replicate(nodeName1);

  auto ctx3 = std::make_shared<asio::io_context>();
  auto sess3 = makeSession(node2, ctx3);
  WorkLoad work3(node2, sess3);
  work3.init();

  std::this_thread::sleep_for(10s);
  // lock the node6 , && make the node2 to be master by cluster failover command
  work2.lockDb(10);

  work3.manualFailover();
  std::this_thread::sleep_for(3s);
  // expect node2 to be new master
  auto state = node1->getClusterMgr()->getClusterState();
  auto nodeName2 = node2->getClusterMgr()->getClusterState()->getMyselfName();
  auto nodeName7 = node7->getClusterMgr()->getClusterState()->getMyselfName();
  CNodePtr node2Ptr = state->clusterLookupNode(nodeName2);
  CNodePtr node7Ptr = state->clusterLookupNode(nodeName7);

  // slave node2 become new master
  EXPECT_EQ(node2Ptr->nodeIsMaster(), true);
  // EXPECT_EQ(node7Ptr->getMaster()->getNodeName(), nodeName2);
  ASSERT_TRUE(nodeIsMySlave(node2, node7));
  // lockdb over, the replication should be fixed
  std::this_thread::sleep_for(10s);
  auto masterHost =
    node2->getClusterMgr()->getClusterState()->getMyselfNode()->getNodeIp();
  auto masterPort =
    node2->getClusterMgr()->getClusterState()->getMyselfNode()->getPort();
  // check all storeid is right
  auto vecCheck =
    node7->getReplManager()->checkMasterHost(masterHost, masterPort);
  EXPECT_EQ(vecCheck.size(), 0);

#ifndef _WIN32
  for (auto svr : servers) {
    svr->stop();
    LOG(INFO) << "stop " << svr->getParams()->port << " success";
  }
  node7->stop();
#endif
  servers.push_back(std::move(node7));
  servers.clear();
}

TEST(Cluster, FixReplication) {
  uint32_t nodeNum = 3;
  uint32_t startPort = 15200;

  const auto guard = MakeGuard([&nodeNum] {
    destroyCluster(nodeNum);
    std::this_thread::sleep_for(std::chrono::seconds(5));
  });

  auto servers = makeCluster(startPort, nodeNum, 10, true);
  // 3 master and 3 slave *, make one master fail
  auto& node1 = servers[0];
  auto& node2 = servers[3];
  // add one slave
  auto node7 = makeClusterNode("node7", startPort + 7, 10);
  auto ctx1 = std::make_shared<asio::io_context>();
  auto sess1 = makeSession(node1, ctx1);
  WorkLoad work1(node1, sess1);
  work1.init();
  work1.clusterMeet(node7->getParams()->bindIp, node7->getParams()->port);
  std::this_thread::sleep_for(3s);

  auto ctx2 = std::make_shared<asio::io_context>();
  auto sess2 = makeSession(node7, ctx2);
  WorkLoad work2(node7, sess2);
  work2.init();
  auto nodeName1 = node1->getClusterMgr()->getClusterState()->getMyselfName();
  work2.replicate(nodeName1);
  std::this_thread::sleep_for(10s);
  // EXPECT_EQ(node7->getReplManager()->isSlaveFullSyncDone(), true);

  auto ctx3 = std::make_shared<asio::io_context>();
  auto sess3 = makeSession(node2, ctx3);
  WorkLoad work3(node2, sess3);
  work3.init();
  // make the node7 to be master by cluster failover command
  work2.manualFailover();
  // lock the node2 , so the replication will set new Master will fail
  work3.lockDb(10);
  std::this_thread::sleep_for(5s);
  // expect node7 to be new master
  auto state = node1->getClusterMgr()->getClusterState();
  auto nodeName7 = node7->getClusterMgr()->getClusterState()->getMyselfName();
  CNodePtr node1Ptr = state->clusterLookupNode(nodeName1);
  CNodePtr node7Ptr = state->clusterLookupNode(nodeName7);
  // slave node2 become new master

  EXPECT_EQ(node7Ptr->nodeIsMaster(), true);
  ASSERT_TRUE(nodeIsMySlave(node7, node1));
  ASSERT_TRUE(nodeIsMySlave(node7, node2));
  auto masterHost =
    node7->getClusterMgr()->getClusterState()->getMyselfNode()->getNodeIp();
  auto masterPort =
    node7->getClusterMgr()->getClusterState()->getMyselfNode()->getPort();

  auto vecCheck1 =
    node2->getReplManager()->checkMasterHost(masterHost, masterPort);

  // lockdb over, the replication should be fixed
  std::this_thread::sleep_for(5s);
  ASSERT_TRUE(nodeIsMySlave(node7, node2));

  // check all storeid is right，gossip cron fix the replicatipn data
  auto vecCheck =
    node2->getReplManager()->checkMasterHost(masterHost, masterPort);
  EXPECT_EQ(vecCheck.size(), 0);

#ifndef _WIN32
  for (auto svr : servers) {
    svr->stop();
    LOG(INFO) << "stop " << svr->getParams()->port << " success";
  }
  node7->stop();
#endif
  servers.push_back(std::move(node7));
  servers.clear();
}

TEST(Cluster, ManualfailoverCheck) {
  uint32_t nodeNum = 3;
  uint32_t startPort = 15200;

  const auto guard = MakeGuard([&nodeNum] {
    destroyCluster(nodeNum);
    std::this_thread::sleep_for(std::chrono::seconds(5));
  });

  auto servers = makeCluster(startPort, nodeNum, 10, true);
  // 3 master and 3 slave *, make one master fail
  auto& master = servers[0];
  // add one slave
  auto slave = makeClusterNode("node7", startPort + 7, 10);
  auto ctx1 = std::make_shared<asio::io_context>();
  auto sess1 = makeSession(master, ctx1);
  WorkLoad work1(master, sess1);
  work1.init();
  work1.clusterMeet(slave->getParams()->bindIp, slave->getParams()->port);
  std::this_thread::sleep_for(3s);

  auto ctx2 = std::make_shared<asio::io_context>();
  auto sess2 = makeSession(slave, ctx2);
  WorkLoad work2(slave, sess2);
  work2.init();
  // just set cluster meta, ignore the replication
  setNodeAsMySlave(master, slave);
  // replication is error , so manual failover should not ok
  bool res = work2.manualFailover();
  ASSERT_FALSE(res);
  // slave node2 become new master
#ifndef _WIN32
  for (auto svr : servers) {
    svr->stop();
    LOG(INFO) << "stop " << svr->getParams()->port << " success";
  }
  slave->stop();
#endif
  servers.push_back(std::move(slave));
  servers.clear();
}

TEST(Cluster, lockConfict) {
  uint32_t nodeNum = 3;
  uint32_t startPort = 15300;

  const auto guard = MakeGuard([&nodeNum] {
    destroyCluster(nodeNum);
    std::this_thread::sleep_for(std::chrono::seconds(5));
  });

  auto servers = makeCluster(startPort, nodeNum);
  auto server = servers[0];

  auto ctx = std::make_shared<asio::io_context>();
  auto sess = makeSession(server, ctx);
  WorkLoad work(server, sess);
  work.init();
  work.lockDb(60);  // 60 seconds is enough

  std::this_thread::sleep_for(15s);

  auto server2 = servers[1];
  EXPECT_EQ(server2->getClusterMgr()->getClusterState()->clusterIsOK(), true);

#ifndef _WIN32
  for (auto svr : servers) {
    svr->stop();
    LOG(INFO) << "stop " << svr->getParams()->port << " success";
  }
#endif
  servers.clear();
}

TEST(Cluster, CrossSlot) {
  uint32_t nodeNum = 2;
  uint32_t startPort = 15000;
  bool withSlave = true;

  const auto guard = MakeGuard([&nodeNum, &withSlave] {
    if (withSlave) {
      destroyCluster(nodeNum * 2);
    } else {
      destroyCluster(nodeNum);
    }
    std::this_thread::sleep_for(std::chrono::seconds(5));
  });

  auto servers = makeCluster(startPort, nodeNum, 10, withSlave);
  auto server = servers[0];
  std::this_thread::sleep_for(std::chrono::seconds(10));

  std::vector<std::pair<std::vector<std::string>, std::string>> resultArr = {
    {{"set", "a{1}", "b"}, "-MOVED 9842 127.0.0.1:15001\r\n"},
    {{"mset", "a{1}", "b", "c{2}", "d"},
     "-CROSSSLOT Keys in request don't hash to the same slot\r\n"},
    {{"mset", "a{2}", "b", "c{2}", "d"}, Command::fmtOK()},
    {{"mset", "a{1}", "b", "c{1}", "d"}, "-MOVED 9842 127.0.0.1:15001\r\n"},
    {{"mget", "a{1}", "c{2}"},
     "-CROSSSLOT Keys in request don't hash to the same slot\r\n"},
    {{"exists", "a{1}", "c{2}"},
     "-CROSSSLOT Keys in request don't hash to the same slot\r\n"},
    {{"exists", "a{2}", "c{2}"}, ":2\r\n"},
    {{"rename", "a{1}", "d{2}"},
     "-CROSSSLOT Keys in request don't hash to the same slot\r\n"},
    {{"rename", "a{2}", "d{2}"}, Command::fmtOK()},
    {{"sadd", "s1{2}", "1", "2", "3"}, ":3\r\n"},
    {{"smove", "s1{2}", "s2{1}", "1"},
     "-CROSSSLOT Keys in request don't hash to the same slot\r\n"},
    {{"smove", "s1{2}", "s2{2}", "1"}, ":1\r\n"},
  };

  testCommandArrayResult(server, resultArr);

  // readonly, readwrite
  auto serverMaster = servers[1];
  std::vector<std::pair<std::vector<std::string>, std::string>> resultArr2 = {
    {{"set", "a{1}", "b"}, "+OK\r\n"},
  };
  testCommandArrayResult(serverMaster, resultArr2);
  std::this_thread::sleep_for(std::chrono::seconds(2));
  auto serverSlave = servers[3];
  std::vector<std::pair<std::vector<std::string>, std::string>> resultArr3 = {
    {{"set", "a{1}", "b"}, "-MOVED 9842 127.0.0.1:15001\r\n"},
    {{"get", "a{1}"}, "-MOVED 9842 127.0.0.1:15001\r\n"},
    {{"readonly"}, "+OK\r\n"},
    {{"set", "a{1}", "b"}, "-MOVED 9842 127.0.0.1:15001\r\n"},
    {{"get", "a{1}"}, "$1\r\nb\r\n"},
    {{"readwrite"}, "+OK\r\n"},
    {{"get", "a{1}"}, "-MOVED 9842 127.0.0.1:15001\r\n"},
  };
  testCommandArrayResult(serverSlave, resultArr3);

#ifndef _WIN32
  for (auto svr : servers) {
    svr->stop();
    LOG(INFO) << "stop " << svr->getParams()->port << " success";
  }
#endif
  servers.clear();
}

TEST(ClusterMsg, bitsetEncodeSize) {
  SlotsBitmap taskmap;
  taskmap.set(16383);
  string s = bitsetStrEncode(taskmap);
  ASSERT_EQ(s, " 16383 ");

  taskmap.set(0);
  s = bitsetStrEncode(taskmap);
  ASSERT_EQ(s, " 0 16383 ");

  taskmap.set(100);
  taskmap.set(101);
  taskmap.set(102);
  s = bitsetStrEncode(taskmap);
  ASSERT_EQ(s, " 0 100-102 16383 ");
}

TEST(Cluster, singleNode) {
  uint32_t nodeNum = 4;
  uint32_t startPort = 15500;

  const auto guard = MakeGuard([&nodeNum] {
    destroyCluster(nodeNum);
    std::this_thread::sleep_for(std::chrono::seconds(5));
  });

  auto servers = makeSingleCluster(startPort, nodeNum);
  auto server = servers[0];
  std::this_thread::sleep_for(std::chrono::seconds(5));

  std::vector<std::pair<std::vector<std::string>, std::string>> resultArr = {
    {{"mset", "a{2}", "b", "c{10}", "d"}, Command::fmtOK()},
  };
  testCommandArrayResult(server, resultArr);
#ifndef _WIN32
  for (auto svr : servers) {
    svr->stop();
    LOG(INFO) << "stop " << svr->getParams()->port << " success";
  }
#endif
  servers.clear();
}

TEST(Cluster, failoverNeedFullSyncDone) {
  uint32_t nodeNum = 3;
  uint32_t startPort = 15500;
  bool withSlave = true;

  std::this_thread::sleep_for(std::chrono::seconds(5));
  const auto guard = MakeGuard([&nodeNum, &withSlave] {
    if (withSlave) {
      destroyCluster(nodeNum * 2);
    } else {
      destroyCluster(nodeNum);
    }
    std::this_thread::sleep_for(std::chrono::seconds(5));
  });

  auto servers = makeCluster(startPort, nodeNum, 10, withSlave);
  // server[0] is master of server[3]
  auto originMaster = servers[0];
  auto originSlave = servers[3];
  auto node = servers[1];

  auto masterName =
    originMaster->getClusterMgr()->getClusterState()->getMyselfName();

  auto state = node->getClusterMgr()->getClusterState();

  auto slaveName =
    originSlave->getClusterMgr()->getClusterState()->getMyselfName();

  // kill master, and restart it after failover happen
  std::this_thread::sleep_for(std::chrono::seconds(5));
  originMaster->stop();
  CNodePtr nodePtr1 = state->clusterLookupNode(masterName);
  waitNodeFail(state, masterName);

  std::this_thread::sleep_for(std::chrono::seconds(10));
  CNodePtr nodePtr2 = state->clusterLookupNode(slaveName);
  // slave become master
  ASSERT_EQ(nodeIsMaster(originSlave), true);
  std::string newMasterName = slaveName;
  // cluster work ok after vote sucessful
  ASSERT_EQ(clusterOk(state), true);

  // make new master locked, so the origin master can not fullsync
  auto _lockThread = std::make_unique<std::thread>(
    [](std::shared_ptr<ServerEntry>&& server) {
      auto ctx = std::make_shared<asio::io_context>();
      auto sess = makeSession(server, ctx);
      WorkLoad work(server, sess);
      work.init();
      work.lockDb(100);
    },
    originSlave);

  // restart origin master
  auto cfg1 = makeServerParam(startPort, 10, "node" + to_string(0), true);
  cfg1->clusterEnabled = true;
  cfg1->pauseTimeIndexMgr = 1;
  cfg1->rocksBlockcacheMB = 24;
  cfg1->clusterSingleNode = false;

  originMaster = std::make_shared<ServerEntry>(cfg1);
  auto s = originMaster->startup(cfg1);
  INVARIANT(s.ok());
  // stop the neew master
  originSlave->stop();
  _lockThread->detach();
  _lockThread.reset();
  // new master should marked as fail
  auto newMasterPtr = state->clusterLookupNode(newMasterName);
  waitNodeFail(state, newMasterName);
  // origin master is still slave, can not won the vote beacause it has not
  // finish fullsync
  std::this_thread::sleep_for(std::chrono::seconds(10));
  ASSERT_EQ(nodeIsMaster(originMaster), false);
  ASSERT_EQ(
    originMaster->getClusterMgr()->getClusterState()->isDataAgeTooLarge(),
    true);
  ASSERT_EQ(clusterOk(state), false);
#ifndef _WIN32
  for (auto svr : servers) {
    svr->stop();
    LOG(INFO) << "stop " << svr->getParams()->port << " success";
  }
  originMaster->stop();
#endif
  servers.emplace_back(std::move(originMaster));
  servers.clear();
}

}  // namespace tendisplus
