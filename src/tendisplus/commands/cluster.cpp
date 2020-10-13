
#ifndef _WIN32
#include <sys/time.h>
#include <sys/utsname.h>
#endif

#include <string>
#include <utility>
#include <memory>
#include <algorithm>
#include <cctype>
#include <vector>
#include <clocale>
#include <map>
#include <list>
#include "glog/logging.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/base64.h"
#include "tendisplus/storage/varint.h"
#include "tendisplus/server/segment_manager.h"
#include "tendisplus/server/server_entry.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/error/en.h"

namespace tendisplus {

class ClusterCommand : public Command {
 public:
  ClusterCommand() : Command("cluster", "rs") {}

  ssize_t arity() const {
    return -1;
  }

  int32_t firstkey() const {
    return 0;
  }

  int32_t lastkey() const {
    return 0;
  }

  int32_t keystep() const {
    return 0;
  }

  bool sameWithRedis() const {
    return false;
  }

  Expected<std::string> run(Session* sess) final {
    auto svr = sess->getServerEntry();
    INVARIANT(svr != nullptr);
    if (!svr->isClusterEnabled()) {
      return {ErrorCodes::ERR_CLUSTER,
              "This instance has cluster support disabled"};
    }

    auto replMgr = svr->getReplManager();
    INVARIANT(replMgr != nullptr);

    auto gcMgr = svr->getGcMgr();
    INVARIANT(gcMgr != nullptr);

    auto migrateMgr = svr->getMigrateManager();
    INVARIANT(migrateMgr != nullptr);
    const std::shared_ptr<tendisplus::ClusterState>& clusterState =
      svr->getClusterMgr()->getClusterState();
    const auto& myself = clusterState->getMyselfNode();
    const auto& args = sess->getArgs();

    const std::string arg1 = toLower(args[1]);
    auto argSize = sess->getArgs().size();

    if (arg1 == "setslot" && argSize >= 3) {
      Status s;
      const std::string arg2 = toLower(args[2]);
      if (arg2 == "importing" && argSize >= 5) {
        if (myself->nodeIsArbiter() || myself->nodeIsSlave()) {
          return {ErrorCodes::ERR_CLUSTER,
                  "Can't importing slots to slave or arbiter node "};
        }
        if (!clusterState->clusterIsOK()) {
          return {ErrorCodes::ERR_CLUSTER,
                  "Can't importing slots when cluster fail"};
        }

        if (svr->getParams()->clusterSingleNode) {
          return {ErrorCodes::ERR_CLUSTER,
                  "Can't importing slots when cluster-single-node is on"};
        }

        std::string nodeId = args[3];
        /* CLUSTER SETSLOT IMPORTING nodename chunkid */
        std::bitset<CLUSTER_SLOTS> slotsMap;

        LOG(INFO) << sess->getCmdStr();
        auto srcNode = clusterState->clusterLookupNode(nodeId);
        if (!srcNode) {
          LOG(ERROR) << "import nodeid:" << nodeId << "not exist in cluster";
          return {ErrorCodes::ERR_CLUSTER, "import node not find"};
        }
        if (srcNode->nodeIsArbiter()) {
          return {ErrorCodes::ERR_CLUSTER,
                  "Can't importing slots from arbiter node."};
        }

        for (size_t i = 4; i < args.size(); i++) {
          Expected<int64_t> exptSlot = ::tendisplus::stoll(args[i]);
          if (!exptSlot.ok()) {
            return exptSlot.status();
          }
          int32_t slot = (int32_t)exptSlot.value();

          if (slot > CLUSTER_SLOTS - 1 || slot < 0) {
            LOG(ERROR) << "slot" << slot
                       << " ERR Invalid or out of range slot ";
            return {ErrorCodes::ERR_CLUSTER, "Invalid migrate slot position"};
          }
          if (svr->getGcMgr()->slotIsDeleting(slot)) {
            LOG(ERROR) << "slot:" << slot
                       << " ERR being deleting before migration";
            return {ErrorCodes::ERR_CLUSTER, "slot in deleting task"};
          }
          if (!svr->getClusterMgr()->emptySlot(slot)) {
            LOG(ERROR) << "slot" << slot << " ERR not empty before migration";
            return {ErrorCodes::ERR_CLUSTER, "slot not empty"};
          }
          // check meta data
          auto thisnode = clusterState->getNodeBySlot(slot);
          if (thisnode == myself) {
            LOG(ERROR) << "slot:" << slot << "already belong to dstNode";
            return {ErrorCodes::ERR_CLUSTER,
                    "I'm already the owner of hash slot" + dtos(slot)};
          }
          if (thisnode != srcNode) {
            DLOG(ERROR) << "slot:" << slot
                        << "is not belong to srcNode : " << nodeId;
            return {ErrorCodes::ERR_CLUSTER,
                    "Source node is not the owner of hash slot" + dtos(slot)};
          }
          // check if slot already been migrating
          if (migrateMgr->slotInTask(slot)) {
            LOG(ERROR) << "migrate task already start on slot:" << slot;
            return {ErrorCodes::ERR_CLUSTER, "already importing" + dtos(slot)};
          }
          slotsMap.set(slot);
        }

        s = startImportingTasks(svr, srcNode, slotsMap);
        if (!s.ok()) {
          return s;
        }
        return Command::fmtOK();

      } else if (arg2 == "info" && argSize == 3) {
        Expected<std::string> migrateInfo = migrateMgr->getMigrateInfo();
        if (migrateInfo.ok()) {
          return migrateInfo.value();
        } else {
          return {ErrorCodes::ERR_CLUSTER, "Invalid migrate info"};
        }
      } else if (arg2 == "tasks" && argSize == 3) {
        Expected<std::string> taskInfo = migrateMgr->getTaskInfo();
        if (taskInfo.ok()) {
          return taskInfo;
        } else {
          return {ErrorCodes::ERR_CLUSTER, "Invalid migrate info"};
        }
      }
    } else if (arg1 == "meet" && (argSize == 4 || argSize == 5)) {
      /* CLUSTER MEET <ip> <port> [cport] */
      uint64_t port, cport;

      auto& host = args[2];
      auto eport = ::tendisplus::stoul(args[3]);
      if (!eport.ok()) {
        return {ErrorCodes::ERR_CLUSTER,
                "Invalid TCP base port specified " + args[3]};
      }
      port = eport.value();

      if (argSize == 5) {
        auto ecport = ::tendisplus::stoul(args[4]);
        if (!ecport.ok()) {
          return {ErrorCodes::ERR_CLUSTER,
                  "Invalid TCP bus port specified " + args[4]};
        }
        cport = ecport.value();
      } else {
        cport = port + CLUSTER_PORT_INCR;
      }

      if (!clusterState->clusterStartHandshake(host, port, cport)) {
        return {ErrorCodes::ERR_CLUSTER,
                "Invalid node address specified:" + host +
                  std::to_string(port)};
      }
      return Command::fmtOK();
    } else if (arg1 == "asarbiter" && argSize == 2) {
      if (myself->nodeIsMaster() && myself->nodeIsArbiter()) {
        return Command::fmtOK();
      }
      if (!myself->nodeIsMaster()) {
        return {ErrorCodes::ERR_CLUSTER, "Only master can became arbiter."};
      }
      if (myself->getSlotNum()) {
        return {ErrorCodes::ERR_CLUSTER,
                "To set an arbiter, the node must be empty."};
      }
      myself->_flags |= CLUSTER_NODE_ARBITER;
      LOG(INFO) << "set myself as arbiter";
      return Command::fmtOK();
    } else if (arg1 == "nodes" && argSize <= 3) {
      bool simple = false;
      if (argSize == 3 && args[2] == "simple") {
        simple = true;
      }
      std::string eNodeInfo = clusterState->clusterGenNodesDescription(
        CLUSTER_NODE_HANDSHAKE, simple);

      if (eNodeInfo.size() > 0) {
        return eNodeInfo;
      } else {
        return {ErrorCodes::ERR_CLUSTER, "Invalid cluster nodes info"};
      }
    } else if ((arg1 == "addslots" || arg1 == "delslots") && argSize >= 3) {
      if (myself->nodeIsSlave()) {
        return {ErrorCodes::ERR_CLUSTER,
                "slave node can not be addslot or delslot"};
      }
      if (myself->nodeIsArbiter()) {
        return {ErrorCodes::ERR_CLUSTER, "Can not add/del slots on arbiter."};
      }

      for (size_t i = 2; i < argSize; ++i) {
        if ((args[i].find('{') != string::npos) &&
            (args[i].find('}') != string::npos)) {
          std::string str = args[i];
          str = str.substr(1, str.size() - 2);

          if (str.find("..") == std::string::npos) {
            return {ErrorCodes::ERR_CLUSTER, "Invalid range input withot .."};
          }
          auto vs = stringSplit(str, "..");

          if (vs.size() != 2) {
            return {ErrorCodes::ERR_CLUSTER, "no find start and end position"};
          }
          auto startSlot = ::tendisplus::stoul(vs[0]);
          auto endSlot = ::tendisplus::stoul(vs[1]);

          if (!startSlot.ok() || !endSlot.ok()) {
            LOG(ERROR) << "ERR Invalid or out of range slot ";
            return {ErrorCodes::ERR_CLUSTER,
                    "Invalid slot position " + args[i]};
          }
          uint32_t start = startSlot.value();
          uint32_t end = endSlot.value();

          if (end >= CLUSTER_SLOTS) {
            return {ErrorCodes::ERR_CLUSTER,
                    "Invalid slot position " + std::to_string(end)};
          }

          if (svr->getParams()->clusterSingleNode &&
              (end - start) != (CLUSTER_SLOTS - 1)) {
            return {
              ErrorCodes::ERR_CLUSTER,
              "You can only addslot 0..16383 when cluster-single-node is on"};
          }

          Status s = changeSlots(start, end, arg1, clusterState, myself);
          if (!s.ok()) {
            LOG(ERROR) << "addslots fail from:" << start << "to:" << end;
            return s;
          }
        } else {
          auto slotInfo = ::tendisplus::stoul(args[i]);

          if (!slotInfo.ok()) {
            return {ErrorCodes::ERR_CLUSTER,
                    "Invalid slot  specified " + args[i]};
          }
          uint32_t slot = static_cast<uint32_t>(slotInfo.value());
          Status s = changeSlot(slot, arg1, clusterState, myself);
          if (!s.ok()) {
            LOG(ERROR) << "addslots:" << slot << "fail";
            return s;
          }
        }
      }
      clusterState->clusterSaveNodes();
      clusterState->clusterUpdateState();
      return Command::fmtOK();

    } else if (arg1 == "replicate" && argSize == 3) {
      auto n = clusterState->clusterLookupNode(args[2]);
      if (!n) {
        return {ErrorCodes::ERR_CLUSTER, "Unknown node: " + args[2]};
      }

      if (n == myself) {
        return {ErrorCodes::ERR_CLUSTER, "Can't replicate myself"};
      }
      /* Can't replicate a slave. */
      if (n->nodeIsSlave()) {
        return {ErrorCodes::ERR_CLUSTER,
                "only replicate a master, not a slave"};
      }
      if (myself->nodeIsArbiter()) {
        return {ErrorCodes::ERR_CLUSTER,
                "I am an arbiter, can not replicate to others."};
      }
      if (n->nodeIsArbiter()) {
        return {ErrorCodes::ERR_CLUSTER, "can not replicate to an arbiter."};
      }
      if (myself->nodeIsMaster() &&
          (myself->getSlotNum() != 0 || nodeNotEmpty(svr, myself))) {
        DLOG(INFO) << "nodeNotEmpty(svr, myself):" << nodeNotEmpty(svr, myself)
                   << "myself slots:" << bitsetStrEncode(myself->getSlots());
        return {ErrorCodes::ERR_CLUSTER,
                "To set a master the node must be empty"};
      }

      Status s = clusterState->clusterSetMaster(n);

      if (!s.ok()) {
        LOG(ERROR) << "replicate node:" << n->getNodeName() << "fail!";
        return s;
      }
      clusterState->clusterSaveNodes();
      return Command::fmtOK();

    } else if (arg1 == "countkeysinslot" && argSize == 3) {
      auto eslot = ::tendisplus::stoul(args[2]);
      if (!eslot.ok()) {
        return {ErrorCodes::ERR_CLUSTER, "no slot info"};
      }
      auto slot = eslot.value();
      if (slot > CLUSTER_SLOTS) {
        return {ErrorCodes::ERR_CLUSTER, "Invalid slot"};
      }
      uint64_t keyNum = svr->getClusterMgr()->countKeysInSlot(slot);
      return Command::fmtBulk(to_string(keyNum));

    } else if (arg1 == "keyslot" && argSize == 3) {
      std::string key = args[2];
      if (key.size() < 1) {
        return {ErrorCodes::ERR_CLUSTER, "keyslot invalid!"};
      }
      uint32_t hash =
        uint32_t(redis_port::keyHashSlot(key.c_str(), key.size()));
      return Command::fmtBulk(to_string(hash));
    } else if (arg1 == "info" && argSize == 2) {
      std::string clusterInfo = clusterState->clusterGenStateDescription();
      if (clusterInfo.size() > 0) {
        return clusterInfo;
      } else {
        return {ErrorCodes::ERR_CLUSTER, "Invalid cluster info"};
      }
    } else if (arg1 == "flushslots" && argSize == 2) {
      //  db not empty
      bool notEmpty = nodeNotEmpty(svr, myself);
      if (notEmpty) {
        return {ErrorCodes::ERR_CLUSTER,
                "DB must be empty to perform CLUSTER FLUSHSLOTS"};
      }

      uint32_t expectNum = myself->getSlotNum();
      uint32_t delNum = clusterState->clusterDelNodeSlots(myself);
      if (delNum != expectNum) {
        LOG(ERROR) << "delslots:" << delNum << "expectedNUM:" << expectNum;
        return {ErrorCodes::ERR_CLUSTER,
                "del slots num not equal expected num"};
      }
      return Command::fmtOK();
    } else if (arg1 == "forget" && argSize == 3) {
      auto n = clusterState->clusterLookupNode(args[2]);
      if (n == nullptr) {
        return {ErrorCodes::ERR_CLUSTER, "forget node unkown"};
      } else if (n == myself) {
        return {ErrorCodes::ERR_CLUSTER,
                "I tried hard but I can't forget myself..."};
      } else if (myself->nodeIsSlave() && myself->_slaveOf == n) {
        return {ErrorCodes::ERR_CLUSTER, "Can't forget my master!"};
      }
      clusterState->clusterBlacklistAddNode(n);
      clusterState->clusterDelNode(n, true);
      clusterState->clusterUpdateState();

      return Command::fmtOK();
    } else if (arg1 == "getkeysinslot" && argSize == 4) {
      auto exptSlot = ::tendisplus::stoul(args[2]);
      if (!exptSlot.ok()) {
        return exptSlot.status();
      }
      uint32_t slot = exptSlot.value();
      auto ecount = ::tendisplus::stoul(args[3]);
      if (!ecount.ok()) {
        return {ErrorCodes::ERR_CLUSTER, "Invalid key num " + args[3]};
      }
      uint32_t count = ecount.value();
      // NOTE(wayenchen) count should be limited in case cost too mush
      // memory
      if (count > 5000) {
        return {ErrorCodes::ERR_CLUSTER, "key num should be less than 5000"};
      }
      if (slot >= CLUSTER_SLOTS) {
        return {ErrorCodes::ERR_CLUSTER, "Invalid slot or number of keys"};
      }

      std::string keysInfo = getKeys(svr, slot, count);

      return keysInfo;
    } else if (arg1 == "slaves" && argSize == 3) {
      auto n = clusterState->clusterLookupNode(args[2]);
      if (n == nullptr) {
        return {ErrorCodes::ERR_CLUSTER, "Unkown node" + args[2]};
      } else if (n->nodeIsSlave()) {
        return {ErrorCodes::ERR_CLUSTER, "The specified node is not a master"};
      }
      std::stringstream ss;
      uint16_t slavesNum = n->getSlaveNum();
      Command::fmtMultiBulkLen(ss, slavesNum);
      for (size_t i = 0; i < slavesNum; i++) {
        std::string nodeDescription =
          clusterState->clusterGenNodeDescription(n->_slaves[i]);
        Command::fmtBulk(ss, nodeDescription);
      }
      return ss.str();
    } else if (arg1 == "bumpepoch" && argSize == 2) {
      auto s = clusterState->clusterBumpConfigEpochWithoutConsensus();
      std::stringstream bumpinfo;
      std::string state = (s.ok()) ? "BUMPED" : "STILL";
      bumpinfo << state << myself->getConfigEpoch();
      return Command::fmtBulk(bumpinfo.str());
    } else if (arg1 == "set-config-epoch" && argSize == 3) {
      Expected<uint64_t> exptEpoch = ::tendisplus::stoul(args[2]);
      if (!exptEpoch.ok()) {
        return exptEpoch.status();
      }
      uint64_t configEpoch = exptEpoch.value();
      if (args[2][0] == '-') {
        return {ErrorCodes::ERR_CLUSTER,
                "Invalid config epoch specified:" + args[2]};
      } else if (clusterState->getNodeCount() > 1) {
        return {ErrorCodes::ERR_CLUSTER,
                "he user can assign a config"
                "epoch only when the node"
                "does not know any other node"};
      } else if (myself->getConfigEpoch() != 0) {
        return {ErrorCodes::ERR_CLUSTER,
                "Node config epoch is already non-zero"};
      } else {
        myself->setConfigEpoch(configEpoch);
        if (clusterState->getCurrentEpoch() < configEpoch) {
          clusterState->setCurrentEpoch(configEpoch);
        }
      }
      clusterState->clusterUpdateState();
      clusterState->clusterSaveNodes();
      return Command::fmtOK();
    } else if (arg1 == "reset" && (argSize == 2 || argSize == 3)) {
      uint16_t hard = 0;
      if (argSize == 3) {
        if (args[2] == "hard") {
          hard = 1;
        } else if (args[2] == "soft") {
          hard = 0;
        } else {
          return {ErrorCodes::ERR_CLUSTER, "error reset flag"};
        }
      }
      /* Slaves can be reset while containing data, but not master nodes
       * that must be empty. */
      if (myself->nodeIsMaster() && nodeNotEmpty(svr, myself)) {
        return {ErrorCodes::ERR_CLUSTER,
                "CLUSTER RESET can't be called with "
                "master nodes containing keys"};
      }
      auto clusterMgr = svr->getClusterMgr();
      auto s = clusterMgr->clusterReset(hard);
      if (!s.ok()) {
        return {ErrorCodes::ERR_CLUSTER, "cluster reset fail!"};
      }
      return Command::fmtOK();
    } else if (arg1 == "saveconfig" && argSize == 2) {
      auto s = clusterState->clusterSaveConfig();
      if (!s.ok()) {
        return {ErrorCodes::ERR_CLUSTER,
                "error saving the cluster node config" + s.toString()};
      }
      return Command::fmtOK();
    } else if (arg1 == "count-failure-reports" && argSize == 3) {
      auto n = clusterState->clusterLookupNode(args[2]);
      if (!n) {
        return {ErrorCodes::ERR_CLUSTER, "Unkown node" + args[2]};
      }
      std::uint32_t failNum = clusterState->clusterNodeFailureReportsCount(n);
      return Command::fmtLongLong(failNum);
    } else if (arg1 == "myid" && argSize == 2) {
      std::string nodeName = myself->getNodeName();
      return Command::fmtBulk(nodeName);
    } else if (arg1 == "slots" && argSize == 2) {
      auto exptSlotInfo = clusterState->clusterReplyMultiBulkSlots();
      if (!exptSlotInfo.ok()) {
        return exptSlotInfo.status();
      }
      return exptSlotInfo.value();
    } else if (arg1 == "failover" && (argSize == 2 || argSize == 3)) {
      bool force = false;
      bool takeover = false;

      if (argSize == 3) {
        if (args[2] == "force") {
          force = true;
        } else if (args[2] == "takeover") {
          takeover = true;
          force = true;
        } else {
          return Command::fmtErr("must be force or takeover ");
        }
      }

      if (myself->nodeIsMaster()) {
        return {ErrorCodes::ERR_CLUSTER,
                "You should send CLUSTER FAILOVER to a slave"};
      }

      CNodePtr master = myself->getMaster();
      if (!master) {
        return {ErrorCodes::ERR_CLUSTER,
                "I'm a slave but my master is unknown to me"};
      } else if (!force && (master->nodeFailed() || !master->getSession())) {
        return {ErrorCodes::ERR_CLUSTER,
                "Master is down or failed, please use CLUSTER FAILOVER "
                "FORCE"};
      }

      auto s = clusterState->forceFailover(force, takeover);
      if (!s.ok()) {
        return s;
      }
      return Command::fmtOK();
    } else if (arg1 == "clear" && argSize == 2) {
      if (myself->nodeIsSlave()) {
        return {ErrorCodes::ERR_CLUSTER,
                "You should send CLUSTER CLEAR to master"};
      }
      auto s = gcMgr->delGarbage();
      if (!s.ok()) {
        LOG(ERROR) << "delete garbage fail" << s.toString();
        return s;
      }
      return Command::fmtOK();
    }
    return {ErrorCodes::ERR_CLUSTER, "Invalid cluster command " + args[1]};
  }

 private:
  Status changeSlots(uint32_t start,
                     uint32_t end,
                     const std::string& arg,
                     const std::shared_ptr<ClusterState> clusterState,
                     const CNodePtr myself) {
    bool result = false;
    if (start < end) {
      for (size_t i = start; i < end + 1; i++) {
        uint32_t index = static_cast<uint32_t>(i);
        if (arg == "addslots") {
          if (clusterState->_allSlots[index] != nullptr) {
            LOG(ERROR) << "slot" << index << "already busy";
            continue;
          }
          result = clusterState->clusterAddSlot(myself, index);
        } else {
          if (clusterState->_allSlots[index] == nullptr) {
            LOG(ERROR) << "slot" << index << "already delete";
            continue;
          }
          result = clusterState->clusterDelSlot(index);
        }
        if (result == false) {
          return {ErrorCodes::ERR_CLUSTER, "del or add slot fail"};
        }
      }
    } else {
      LOG(ERROR) << "ERR Invalid or out of range slot";
      return {ErrorCodes::ERR_CLUSTER, "ERR Invalid or out of range slot"};
    }
    return {ErrorCodes::ERR_OK, "finish addslots"};
  }

  Status changeSlot(uint32_t slot,
                    const std::string& arg,
                    const std::shared_ptr<ClusterState> clusterState,
                    const CNodePtr myself) {
    bool result = false;
    if (arg == "addslots") {
      if (clusterState->_allSlots[slot] != nullptr) {
        return {ErrorCodes::ERR_CLUSTER, "Slot is already busy"};
      }
      result = clusterState->clusterAddSlot(myself, slot);
    } else {
      if (clusterState->_allSlots[slot] == nullptr) {
        LOG(WARNING) << "slot" << slot << "already delete";
        return {ErrorCodes::ERR_CLUSTER, "Slot is already delete"};
      }
      result = clusterState->clusterDelSlot(slot);
    }
    if (result == false) {
      return {ErrorCodes::ERR_CLUSTER, "del or add slot fail"};
    }
    return {ErrorCodes::ERR_OK, "finish add sigle slot"};
  }

  std::string getKeys(ServerEntry* svr, uint32_t slot, uint32_t count) {
    std::vector<std::string> keysList =
      svr->getClusterMgr()->getKeyBySlot(slot, count);
    std::stringstream keysInfo;
    uint32_t n = keysList.size();

    Command::fmtMultiBulkLen(keysInfo, n);
    for (auto&& vs : keysList) {
      Command::fmtBulk(keysInfo, vs);
    }
    return keysInfo.str();
  }

  bool nodeNotEmpty(ServerEntry* svr, CNodePtr node) {
    bool notEmpty = false;
    auto slots = node->getSlots();
    size_t idx = 0;
    while (idx < slots.size()) {
      if (slots.test(idx) && !svr->getClusterMgr()->emptySlot(idx)) {
        notEmpty = true;
        break;
      }
      ++idx;
    }
    return notEmpty;
  }

  Status startImportingTasks(ServerEntry* svr,
                             CNodePtr srcNode,
                             const std::bitset<CLUSTER_SLOTS>& slotsMap) {
    Status s;
    auto clusterState = svr->getClusterMgr()->getClusterState();
    auto migrateMgr = svr->getMigrateManager();

    auto ip = srcNode->getNodeIp();
    auto port = srcNode->getPort();

    std::shared_ptr<BlockingTcpClient> client =
      std::move(createClient(ip, port, svr));

    if (client == nullptr) {
      LOG(ERROR) << "Connect to sender:" << ip << ":" << port
                 << "failed, no valid client";
      return {ErrorCodes::ERR_NETWORK, "fail to connect to sender"};
    }
    std::stringstream ss;
    const std::string nodename = clusterState->getMyselfName();
    std::string bitmapStr = slotsMap.to_string();
    ss << "preparemigrate " << bitmapStr << " " << nodename << " "
       << svr->getKVStoreCount();
    s = client->writeLine(ss.str());
    if (!s.ok()) {
      LOG(ERROR) << "preparemigrate failed:" << s.toString();
      return s;
    }
    uint32_t timeoutSecs = svr->getParams()->timeoutSecBinlogWaitRsp;
    auto expRsp = client->readLine(std::chrono::seconds(timeoutSecs));
    if (!expRsp.ok()) {
      LOG(ERROR) << "preparemigrate error:" << expRsp.status().toString();
      return expRsp.status();
    }

    const std::string& json = expRsp.value();
    LOG(INFO) << "json content:" << json;
    rapidjson::Document doc;
    doc.Parse(json);
    if (doc.HasParseError()) {
      LOG(ERROR) << "parse task failed"
                 << rapidjson::GetParseError_En(doc.GetParseError());
      return {ErrorCodes::ERR_NETWORK, "json parse fail"};
    }
    if (!doc.HasMember("errMsg"))
      return {ErrorCodes::ERR_DECODE, "json contain no errMsg"};

    std::string errMsg = doc["errMsg"].GetString();
    if (errMsg != "+OK") {
      return {ErrorCodes::ERR_WRONG_TYPE, "json contain err:" + errMsg};
    }

    if (!doc.HasMember("taskinfo"))
      return {ErrorCodes::ERR_DECODE, "json contain no task information!"};

    rapidjson::Value& Array = doc["taskinfo"];

    if (!Array.IsArray())
      return {ErrorCodes::ERR_WRONG_TYPE, "information is not array!"};

    if (!doc.HasMember("finishMsg"))
      return {ErrorCodes::ERR_DECODE, "json contain no finishMsg!"};

    std::string finishMsg = doc["finishMsg"].GetString();
    if (finishMsg != "+OK") {
      return {ErrorCodes::ERR_WRONG_TYPE, "sender task not finish!"};
    }

    for (rapidjson::SizeType i = 0; i < Array.Size(); i++) {
      const rapidjson::Value& object = Array[i];

      if (!object.HasMember("storeid") || !object.HasMember("migrateSlots")) {
        return {ErrorCodes::ERR_DECODE, "json contain no slots info"};
      }
      if (!object["storeid"].IsUint64()) {
        return {ErrorCodes::ERR_WRONG_TYPE, "json storeid error type"};
      }
      uint32_t storeid = static_cast<uint64_t>(object["storeid"].GetUint64());
      std::vector<uint32_t> slotsVec;
      const rapidjson::Value& slotArray = object["migrateSlots"];
      if (!slotArray.IsArray())
        return {ErrorCodes::ERR_WRONG_TYPE, "json slotArray error type"};

      for (rapidjson::SizeType i = 0; i < slotArray.Size(); i++) {
        const rapidjson::Value& object = slotArray[i];
        auto element = static_cast<uint32_t>(object.GetUint64());
        slotsVec.push_back(element);
      }

      SlotsBitmap taskmap;
      for (const auto& vs : slotsVec) {
        taskmap.set(vs);
      }
      s = migrateMgr->startTask(taskmap, ip, port, storeid, true);
      if (!s.ok()) {
        return {ErrorCodes::ERR_CLUSTER, "migrate receive start task fail"};
      }
      migrateMgr->insertNodes(slotsVec, srcNode->getNodeName(), true);
    }
    return {ErrorCodes::ERR_OK, ""};
  }
} clusterCmd;

class PrepareMigrateCommand : public Command {
 public:
  PrepareMigrateCommand() : Command("preparemigrate", "a") {}

  ssize_t arity() const {
    return 4;
  }

  int32_t firstkey() const {
    return 0;
  }

  int32_t lastkey() const {
    return 0;
  }

  int32_t keystep() const {
    return 0;
  }

  bool isBgCmd() const {
    return true;
  }

  Expected<std::string> run(Session* sess) final {
    LOG(FATAL) << "prepareSender should not be called";
    // void compiler complain
    return {ErrorCodes::ERR_INTERNAL, "shouldn't be called"};
  }
} preparemigrateCommand;


class ReadymigrateCommand : public Command {
 public:
  ReadymigrateCommand() : Command("readymigrate", "a") {}

  ssize_t arity() const {
    return 5;
  }

  int32_t firstkey() const {
    return 0;
  }

  int32_t lastkey() const {
    return 0;
  }

  int32_t keystep() const {
    return 0;
  }

  bool isBgCmd() const {
    return true;
  }

  Expected<std::string> run(Session* sess) final {
    LOG(FATAL) << "readymigrate should not be called";
    // void compiler complain
    return {ErrorCodes::ERR_INTERNAL, "shouldn't be called"};
  }
} readymigrateCommand;


class MigrateendCommand : public Command {
 public:
  MigrateendCommand() : Command("migrateend", "rs") {}

  ssize_t arity() const {
    return 3;
  }

  int32_t firstkey() const {
    return 0;
  }

  int32_t lastkey() const {
    return 0;
  }

  int32_t keystep() const {
    return 0;
  }

  bool sameWithRedis() const {
    return false;
  }

  Expected<std::string> run(Session* sess) final {
    auto svr = sess->getServerEntry();
    INVARIANT(svr != nullptr);
    const auto& args = sess->getArgs();
    auto migrateMgr = svr->getMigrateManager();
    INVARIANT(migrateMgr != nullptr);

    std::string taskid = args[1];
    std::string sendBinlogResult = args[2];

    bool finishBinlog = (sendBinlogResult == "+OK") ? true : false;
    auto s = migrateMgr->supplyMigrateEnd(taskid, finishBinlog);
    if (!s.ok()) {
      return s;
    }
    return Command::fmtOK();
  }
} migrateendCmd;

class MigrateVersionMetaCommand : public Command {
 public:
  MigrateVersionMetaCommand() : Command("migrateversionmeta", "ws") {}

  ssize_t arity() const {
    return -1;
  }

  int32_t firstkey() const {
    return 0;
  }

  int32_t lastkey() const {
    return 0;
  }

  int32_t keystep() const {
    return 0;
  }

  bool sameWithRedis() const {
    return false;
  }

  Expected<std::string> run(Session* sess) final {
    auto svr = sess->getServerEntry();
    INVARIANT(svr != nullptr);
    const auto& args = sess->getArgs();
    const auto server = sess->getServerEntry();

    if (args.size() % 3 != 1) {
      return {ErrorCodes::ERR_INTERNAL, "args should be 3*n+1"};
    }

    for (size_t n = 1; n < args.size();) {
      const auto& name = args[n];
      auto eTs = tendisplus::stoull(args[n + 1]);
      if (!eTs.ok()) {
        return eTs.status();
      }
      auto eVersion = tendisplus::stoull(args[n + 2]);
      if (!eVersion.ok()) {
        return eVersion.status();
      }
      VersionMeta meta(eTs.value(), eVersion.value(), name);
      VersionMeta minMeta(UINT64_MAX - 1, UINT64_MAX - 1, name);
      for (size_t i = 0; i < server->getKVStoreCount(); i++) {
        auto expdb =
          server->getSegmentMgr()->getDb(sess, i, mgl::LockMode::LOCK_IS);
        if (!expdb.ok()) {
          return expdb.status();
        }
        PStore store = expdb.value().store;
        auto meta = store->getVersionMeta(name);
        if (!meta.ok()) {
          return meta.status();
        }
        VersionMeta metaV = meta.value();
        minMeta = std::min(metaV, minMeta);
      }

      // migrate version only dest node's versionmeta smaller than source
      // node
      if (minMeta < meta) {
        for (uint32_t i = 0; i < server->getKVStoreCount(); i++) {
          auto expdb =
            server->getSegmentMgr()->getDb(sess, i, mgl::LockMode::LOCK_IS);
          if (!expdb.ok()) {
            return expdb.status();
          }
          PStore store = expdb.value().store;
          auto s = store->setVersionMeta(name, eTs.value(), eVersion.value());
          if (!s.ok()) {
            return s;
          }
        }
      }
      n = n + 3;
    }

    return Command::fmtOK();
  }
} migrateVersionMetaCmd;

}  // namespace tendisplus