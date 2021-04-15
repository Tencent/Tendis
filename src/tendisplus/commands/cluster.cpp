// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

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
#include <random>
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
    return -2;
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
      if ((arg2 == "importing" || arg2 == "restart") && argSize >= 5) {
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
        auto srcNode = clusterState->clusterLookupNode(nodeId);
        if (!srcNode) {
          LOG(ERROR) << "import nodeid:" << nodeId << " not exist in cluster";
          return {ErrorCodes::ERR_CLUSTER, "import node not find"};
        }
        if (srcNode->nodeIsArbiter()) {
          return {ErrorCodes::ERR_CLUSTER,
                  "Can't importing slots from arbiter node."};
        }
        std::vector<std::string> vec(args.begin() + 4, args.end());
        /* CLUSTER SETSLOT retry nodename chunkid */
        std::bitset<CLUSTER_SLOTS> slotsMap;

        for (auto& vs : vec) {
          if ((vs.find('{') != string::npos) &&
              (vs.find('}') != string::npos)) {
            auto eRange = getSlotRange(vs);
            RET_IF_ERR_EXPECTED(eRange);

            uint32_t start = eRange.value().first;
            uint32_t end = eRange.value().second;

            for (uint32_t i = start; i <= end; i++) {
              slotsMap.set(i);
            }
          } else {
            Expected<int64_t> exptSlot = ::tendisplus::stoll(vs);
            RET_IF_ERR_EXPECTED(exptSlot);

            int32_t slot = (int32_t)exptSlot.value();

            if (slot > CLUSTER_SLOTS - 1 || slot < 0) {
              LOG(ERROR) << "slot" << slot
                         << " ERR Invalid or out of range slot ";
              return {ErrorCodes::ERR_CLUSTER, "Invalid migrate slot position"};
            }
            slotsMap.set(slot);
          }
        }

        bool needRetry = (arg2 == "restart");
        auto exptTaskid = startAllSlotsTasks(
          slotsMap, svr, nodeId, clusterState, srcNode, myself, needRetry);
        if (!exptTaskid.ok()) {
          LOG(ERROR) << "importing task fail:"
                     << exptTaskid.status().toString();
          return exptTaskid.status();
        }
        return Command::fmtBulk(exptTaskid.value());
      } else if (arg2 == "info" && argSize == 3) {
        Expected<std::string> migrateInfo = migrateMgr->getMigrateInfo();
        if (migrateInfo.ok()) {
          return migrateInfo.value();
        } else {
          return {ErrorCodes::ERR_CLUSTER, "Invalid migrate info"};
        }
      } else if (arg2 == "taskinfo" && (argSize >= 3 && argSize <= 5)) {
        Expected<std::string> taskInfo("");
        std::string arg3;
        if (argSize == 3) {
          taskInfo = migrateMgr->getTaskInfo();
        } else if (argSize == 4) {
          arg3 = toLower(args[3]);
          if (arg3 == "all") {
            taskInfo = migrateMgr->getTaskInfo(false);
          } else if (arg3 == "fail" || arg3 == "succ") {
            if (arg3 == "fail") {
              taskInfo = migrateMgr->getSuccFailInfo();
            } else {
              taskInfo = migrateMgr->getSuccFailInfo(true);
            }
          } else if (arg3 == "running") {
            taskInfo = migrateMgr->getTaskInfo();
          } else if (arg3 == "waiting") {
            taskInfo = migrateMgr->getTaskInfo(false, true);
          } else {
            return {ErrorCodes::ERR_INTERNAL, "invaild args"};
          }
        } else {
          arg3 = toLower(args[3]);
          const std::string arg4 = toLower(args[4]);
          if (arg3 == "all") {
            taskInfo = migrateMgr->getTaskInfo(arg4);
          } else if (arg3 == "fail" || arg3 == "succ") {
            if (arg3 == "fail") {
              taskInfo = migrateMgr->getSuccFailInfo(arg4);
            } else {
              taskInfo = migrateMgr->getSuccFailInfo(arg4, true);
            }
          } else if (arg3 == "running") {
            taskInfo = migrateMgr->getTaskInfo(arg4, true);
          } else if (arg3 == "waiting") {
            taskInfo = migrateMgr->getTaskInfo(arg4, false, true);
          } else {
            return {ErrorCodes::ERR_INTERNAL, "invaild args"};
          }
        }
        if (!taskInfo.ok()) {
          return Command::fmtNull();
        }
        return taskInfo.value();
      } else if (arg2 == "stop" && argSize > 3) {
        for (size_t i = 3; i < args.size(); i++) {
          LOG(INFO) << "stopping tasks, the parent taskid is:" << args[i];
          auto s = migrateMgr->stopTasks(args[i]);
          if (!s.ok()) {
            LOG(ERROR) << "error stop task" << s.toString();
            return s;
          }
        }
        return Command::fmtOK();
      } else if (arg2 == "stopall" && argSize == 3) {
        /* NOTE(wayenchen) stop all migrate tasks (save message of stop tasks),
         * work on both srcNode and dstNode */
        migrateMgr->stopAllTasks();
        return Command::fmtOK();
      } else if (arg2 == "cleanall" && argSize == 3) {
        /* NOTE(wayenchen) clean all migrate tasks, no save message*/
        migrateMgr->stopAllTasks(false);
        return Command::fmtOK();
      } else if (arg2 == "restartall" && argSize == 3) {
        /* NOTE(wayenchen) it is work on dstNode command */
        std::map<std::string, SlotsBitmap> remainSlotsMap =
          migrateMgr->getStopMap();

        if (remainSlotsMap.size() == 0) {
          return {ErrorCodes::ERR_CLUSTER, "no slot remain to restart"};
        }

        for (auto it = remainSlotsMap.begin(); it != remainSlotsMap.end();
             ++it) {
          std::string nodeId = (*it).first;
          auto taskMap = (*it).second;
          auto srcNode = clusterState->clusterLookupNode(nodeId);
          auto exptTaskid = startAllSlotsTasks(
            taskMap, svr, nodeId, clusterState, srcNode, myself, true);
          if (!exptTaskid.ok()) {
            LOG(ERROR) << "restart importing task fail:"
                       << exptTaskid.status().toString();
            return exptTaskid.status();
          }
          LOG(INFO) << "restart slots nodeid:" << nodeId
                    << " slots is:" << bitsetStrEncode(taskMap);
          migrateMgr->removeRestartSlots(nodeId, (*it).second);
        }

        return Command::fmtOK();
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
          auto eRange = getSlotRange(args[i]);

          RET_IF_ERR_EXPECTED(eRange);
          uint32_t start = eRange.value().first;
          uint32_t end = eRange.value().second;

          if (svr->getParams()->clusterSingleNode &&
              (end - start) != (CLUSTER_SLOTS - 1)) {
            return {
              ErrorCodes::ERR_CLUSTER,
              "You can only addslot 0..16383 when cluster-single-node is on"};
          }

          Status s = changeSlots(start, end, arg1, svr, clusterState, myself);
          if (!s.ok()) {
            LOG(ERROR) << "addslots fail from:" << start << "to:" << end;
            return s;
          }
        } else {
          auto slotInfo = ::tendisplus::stoul(args[i]);

          RET_IF_ERR_EXPECTED(slotInfo);
          uint32_t slot = static_cast<uint32_t>(slotInfo.value());
          Status s = changeSlot(slot, arg1, svr, clusterState, myself);
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

      if (clusterState->getMyMaster() == n) {
        LOG(INFO) << "I am already the slave of:" << args[2];
        return Command::fmtOK();
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
          (myself->getSlotNum() != 0 || !svr->isDbEmpty())) {
        DLOG(INFO) << "myself slots:" << bitsetStrEncode(myself->getSlots());
        return {ErrorCodes::ERR_CLUSTER,
                "To set a master the node must be empty"};
      }

      if (clusterState->getMyMaster() != nullptr) {
        LOG(ERROR) << "already have master:"
                   << clusterState->getMyMaster()->getNodeName();
        return {ErrorCodes::ERR_CLUSTER, "cluster reset before set new master"};
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
      bool notEmpty = !svr->isDbEmpty();
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

      auto s = svr->getClusterMgr()->clusterDelNodeMeta(args[2]);
      if (!s.ok()) {
        LOG(ERROR) << "delete metadata of :" << args[2]
                   << "fail when forget nodes";
        return {ErrorCodes::ERR_CLUSTER, "delete metadata fail"};
      }
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
      if (myself->nodeIsMaster() && !svr->isDbEmpty()) {
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
        const std::string arg2 = toLower(args[2]);
        if (arg2 == "force") {
          force = true;
        } else if (arg2 == "takeover") {
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
                     ServerEntry* svr,
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
          if (svr->getMigrateManager()->slotInTask(index)) {
            LOG(ERROR) << "slot" << index << "is migrating";
            continue;
          }
          result = clusterState->clusterDelSlot(index);
        }
        if (result == false) {
          return {ErrorCodes::ERR_CLUSTER, "del or add slot fail"};
        }
      }
    } else {
      LOG(ERROR) << "ERR Invalid or out of range slot from:" << start
                 << "to:" << end;
      return {ErrorCodes::ERR_CLUSTER, "ERR Invalid or out of range slot"};
    }
    return {ErrorCodes::ERR_OK, "finish addslots"};
  }

  Status changeSlot(uint32_t slot,
                    const std::string& arg,
                    ServerEntry* svr,
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
      /* NOTE(wayenchen) forbidden deleting slot when it is migrating*/
      if (svr->getMigrateManager()->slotInTask(slot)) {
        LOG(ERROR) << "slot" << slot << "is migrating";
        return {ErrorCodes::ERR_CLUSTER, "Slot is migrating"};
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

  Expected<std::string> startImportingTasks(
    ServerEntry* svr,
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
    // PARENT TASKID
    std::string pTaskId = migrateMgr->genPTaskid();

    ss << "preparemigrate " << bitmapStr << " " << nodename << " " << pTaskId
       << " " << svr->getKVStoreCount();
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
    rapidjson::Document doc;

    struct taskinfo {
      taskinfo(uint32_t storeid,
               const SlotsBitmap& taskmap,
               const std::string& taskid)
        : _storeid(storeid), _taskmap(taskmap), _taskid(taskid) {}
      uint32_t _storeid;
      SlotsBitmap _taskmap;
      std::string _taskid;
    };

    vector<taskinfo> taskInfoArray;

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
    auto pTaskPtr = std::make_shared<pTask>(pTaskId, srcNode->getNodeName());
    for (rapidjson::SizeType i = 0; i < Array.Size(); i++) {
      const rapidjson::Value& object = Array[i];

      if (!object.HasMember("storeid") || !object.HasMember("migrateSlots") ||
          !object.HasMember("taskid")) {
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

      if (!object["taskid"].IsString()) {
        return {ErrorCodes::ERR_WRONG_TYPE, "json taskid error type"};
      }

      auto myid = pTaskId + "-" + object["taskid"].GetString();
      taskInfoArray.emplace_back(storeid, taskmap, myid);
    }
    /*NOTE(wayenchen) shuffle receiver task before starting*/
    unsigned seed = TCLOCK::now().time_since_epoch().count();
    shuffle(taskInfoArray.begin(),
            taskInfoArray.end(),
            std::default_random_engine(seed));

    LOG(INFO) << "taskInfo array shuffle finished, tasksize is:"
              << taskInfoArray.size();
    for (auto& ele : taskInfoArray) {
      s = migrateMgr->startTask(
        ele._taskmap, ip, port, ele._taskid, ele._storeid, pTaskPtr, true);
      if (!s.ok()) {
        return {ErrorCodes::ERR_CLUSTER, "migrate receive start task fail"};
      }
      migrateMgr->insertNodes(ele._taskmap, srcNode->getNodeName(), true);
    }
    migrateMgr->addImportPTask(pTaskPtr);

    return pTaskId;
  }

  Expected<std::string> startAllSlotsTasks(
    const std::bitset<CLUSTER_SLOTS>& taskMap,
    ServerEntry* svr,
    const std::string& nodeId,
    const std::shared_ptr<ClusterState> clusterState,
    const CNodePtr srcNode,
    const CNodePtr myself,
    bool retryMigrate) {
    std::bitset<CLUSTER_SLOTS> slotsMap(taskMap);
    size_t slot = 0;
    while (slot < slotsMap.size()) {
      if (slotsMap.test(slot)) {
        if (retryMigrate && clusterState->getNodeBySlot(slot) == myself) {
          slotsMap.reset(slot);
          continue;
        }

        if (svr->getGcMgr()->slotIsDeleting(slot)) {
          LOG(ERROR) << "slot:" << slot
                     << " ERR being deleting before migration";
          return {ErrorCodes::ERR_CLUSTER,
                  "slot in deleting task" + dtos(slot)};
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
        if (svr->getMigrateManager()->slotInTask(slot)) {
          LOG(ERROR) << "migrate task already start on slot:" << slot;
          return {ErrorCodes::ERR_CLUSTER, "already importing" + dtos(slot)};
        }
      }
      slot++;
    }

    auto exptTaskid = startImportingTasks(svr, srcNode, slotsMap);
    if (!exptTaskid.ok()) {
      LOG(ERROR) << "error start import task:"
                 << exptTaskid.status().toString();
      return exptTaskid.status();
    }
    return exptTaskid.value();
  }
} clusterCmd;

class PrepareMigrateCommand : public Command {
 public:
  PrepareMigrateCommand() : Command("preparemigrate", "as") {}

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
    LOG(FATAL) << "prepareSender should not be called";
    // void compiler complain
    return {ErrorCodes::ERR_INTERNAL, "shouldn't be called"};
  }
} preparemigrateCommand;


class ReadymigrateCommand : public Command {
 public:
  ReadymigrateCommand() : Command("readymigrate", "as") {}

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

    bool finishBinlog = (sendBinlogResult == "+OK");
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
