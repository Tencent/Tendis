
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

namespace tendisplus {

class ClusterCommand: public Command {
public:
    ClusterCommand()
            : Command("cluster", "rs") {
    }

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

    Expected<std::string> run(Session *sess) final {
        auto svr = sess->getServerEntry();
        INVARIANT(svr != nullptr);
        if (!svr->isClusterEnabled()) {
            return {ErrorCodes::ERR_CLUSTER,
                    "This instance has cluster support disabled"};
        }
        auto replMgr = svr->getReplManager();
        INVARIANT(replMgr != nullptr);

        auto migrateMgr = svr->getMigrateManager();
        INVARIANT(migrateMgr != nullptr);
        const std::shared_ptr<tendisplus::ClusterState>
                &clusterState = svr->getClusterMgr()->getClusterState();
        const auto &myself = clusterState->getMyselfNode();
        const auto &args = sess->getArgs();

        const std::string arg1 = toLower(args[1]);
        auto argSize = sess->getArgs().size();

        if (arg1 == "setslot" && argSize >= 4) {
            Expected<uint64_t> exptChunkid = ::tendisplus::stoul(args[2]);
            if (!exptChunkid.ok()) {
                return exptChunkid.status();
            }
            uint32_t chunkid = (uint32_t) exptChunkid.value();

            string ip = args[4];
            Expected<uint64_t> exptPort = ::tendisplus::stoul(args[5]);
            if (!exptPort.ok()) {
                return exptPort.status();
            }
            uint16_t port = (uint16_t) exptPort.value();

            auto myself = clusterState->getMyselfNode();

            if (myself->nodeIsSlave()) {
                return {ErrorCodes::ERR_CLUSTER,
                        "slave node can not be setslot"};
            }
            Status s;
            if (args[3] == "migrating" && argSize == 5) {
                if (clusterState->_allSlots[chunkid] != myself) {
                    return {ErrorCodes::ERR_CLUSTER,
                            "I'm not the owner of hash slot"};
                }
                auto setNode = clusterState->clusterLookupNode(args[4]);
                if (setNode) {
                    clusterState->_migratingSlots[chunkid] = setNode;
                } else {
                    return {ErrorCodes::ERR_CLUSTER,
                            "setslot migrating not find node!"};
                }
                LOG(INFO) << "cluster setslot migrating," << chunkid << " " << ip << ":" << port;
                s = migrateMgr->migrating(chunkid, ip, port);
            } else if (args[3] == "importing" && argSize == 5) {
                if (clusterState->_allSlots[chunkid] != myself) {
                    return {ErrorCodes::ERR_CLUSTER,
                            "I'm not the owner of hash slot"};
                }
                auto setNode = clusterState->clusterLookupNode(args[4]);
                if (setNode) {
                    clusterState->_importingSlots[chunkid] = setNode;
                } else {
                    return {ErrorCodes::ERR_CLUSTER,
                            "setslot importing not find node!"};
                }
                LOG(INFO) << "cluster setslot importing," << chunkid << " " << ip << ":" << port;
                s = migrateMgr->importing(chunkid, ip, port);
            } else if (args[3] == "stable" && argSize == 4) {
                clusterState->_importingSlots[chunkid] = nullptr;
                clusterState->_migratingSlots[chunkid] = nullptr;
            } else if (args[3] == "node" && argSize == 5) {
                auto n = clusterState->clusterLookupNode(args[2]);
                if ( n == nullptr ) {
                    return  {ErrorCodes::ERR_CLUSTER, "Unknown node"+args[2]};
                }
                s = clusterState->setSlot(n, chunkid);

            }
            if (!s.ok()) {
                return Command::fmtErr(s.toString());
            }
            clusterState->clusterUpdateState();
            clusterState->clusterSaveNodes();
            return Command::fmtOK();


        } else if (arg1 == "meet" && (argSize == 4 || argSize == 5)) {
            /* CLUSTER MEET <ip> <port> [cport] */
            uint64_t port, cport;

            auto &host = args[2];
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
                        "Invalid node address specified: " + host + std::to_string(port)};
            }
            return Command::fmtOK();
        } else if (arg1 == "nodes" && argSize == 2) {
            std::string eNodeInfo = clusterState->clusterGenNodesDescription
                    (CLUSTER_NODE_HANDSHAKE);

            if (eNodeInfo.size() > 0) {
                return eNodeInfo;
            } else {
                return {ErrorCodes::ERR_CLUSTER,
                        "Invalid cluster nodes info"};
            }
        } else if ((arg1 == "addslots" || arg1 == "delslots") && argSize >= 3) {

            if (myself->nodeIsSlave()) {
                return {ErrorCodes::ERR_CLUSTER,
                        "slave node can not be addslot or delslot"};
            }

            for (size_t i = 2; i < argSize; ++i) {
                if ((args[i].find('{') != string::npos) &&
                    (args[i].find('}') != string::npos)) {

                    std::string str = args[i];
                    str = str.substr(1, str.size() - 2);

                    auto vs = stringSplit(str, "..");
                    auto startSlot = ::tendisplus::stoul(vs[0]);
                    auto endSlot = ::tendisplus::stoul(vs[1]);

                    if (!startSlot.ok() || !endSlot.ok()) {
                        LOG(ERROR) << "ERR Invalid or out of range slot ";
                        return {ErrorCodes::ERR_CLUSTER,
                                "Invalid slot position " + args[i]};
                    }
                    uint32_t start = startSlot.value();
                    uint32_t end = endSlot.value();
                    Status s = addSlots(start, end, arg1, clusterState, myself);
                    if (!s.ok()) {
                        LOG(ERROR) << "addslots fail from:"
                            << start << "to:" << end;
                        return s;
                    }
                } else {
                    auto slotInfo = ::tendisplus::stoul(args[i]);

                    if (!slotInfo.ok()) {
                        return {ErrorCodes::ERR_CLUSTER,
                                "Invalid slot  specified " + args[i]};
                    }
                    uint32_t slot = static_cast<uint32_t >(slotInfo.value());
                    Status s = addSingleSlot(slot, arg1, clusterState, myself);
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
                return { ErrorCodes::ERR_CLUSTER, "Unknown node: " + args[2] };
            }
            Status s = replicateNode(n, myself, svr, sess, replMgr, clusterState);
            if (!s.ok()) {
                LOG(ERROR) << "replicate node:" << n->getNodeName() << "fail!";
                return s;
            }
            clusterState->clusterSaveNodes();
            return Command::fmtOK();

        } else if (arg1 == "countkeysinslot" && argSize == 3) {
            auto eslot = ::tendisplus::stoul(args[2]);
            if (!eslot.ok()) {
                return {ErrorCodes::ERR_CLUSTER,
                        "no slot info"};
            }
            auto slot =  eslot.value();
            if (slot > CLUSTER_SLOTS) {
                return {ErrorCodes::ERR_CLUSTER,
                        "Invalid slot"};
            }
            uint64_t keyNum = svr->countKeysInSlot(slot);
            return Command::fmtBulk(to_string(keyNum));

        } else if (arg1 == "keyslot" && argSize == 3) {
            std::string key = args[2];
            if (key.size() < 1) {
                return {ErrorCodes::ERR_CLUSTER,
                        "keyslot invalid!"};
            }
            uint32_t hash = uint32_t(
                redis_port::keyHashSlot(key.c_str(), key.size()));
            return  Command::fmtBulk(to_string(hash));
        } else if (arg1 == "info" && argSize == 2) {
            std::string clusterInfo = clusterState->clusterGenStateDescription();
             if (clusterInfo.size() > 0) {
                return clusterInfo;
             } else {
                return {ErrorCodes::ERR_CLUSTER,
                    "Invalid cluster info"};
             }
        } else if (arg1 == "flushslots" && argSize == 2) {
            //  db not empty
            bool notEmpty = nodeNotEmpty(svr, myself);
            if (notEmpty) {
                return {ErrorCodes ::ERR_CLUSTER,
                        "DB must be empty to perform CLUSTER FLUSHSLOTS"};
            }

            uint32_t expectNum = myself->getSlotNum();
            uint32_t delNum = clusterState->clusterDelNodeSlots(myself);
            if (delNum != expectNum) {
                LOG(ERROR) << "delslots:" << delNum << "expectedNUM:" << expectNum;
                return {ErrorCodes::ERR_CLUSTER,
                        "del slots num not equal expected num"};
            }
            return  Command::fmtOK();
        } else if (arg1 == "forget" && argSize == 3) {
            auto n = clusterState->clusterLookupNode(args[2]);
            if (n == nullptr) {
                return  {ErrorCodes::ERR_CLUSTER, "forget node unkown"};
            } else if (n == myself) {
                return {ErrorCodes ::ERR_CLUSTER,
                            "I tried hard but I can't forget myself..."};
            } else if (myself->nodeIsSlave() && myself->_slaveOf == n) {
                return {ErrorCodes ::ERR_CLUSTER, "Can't forget my master!"};
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
                return {ErrorCodes::ERR_CLUSTER,
                        "Invalid key num " + args[3]};
            }

            uint32_t count = ecount.value();
            if (slot > CLUSTER_SLOTS || count > CLUSTER_SLOTS) {
                return {ErrorCodes ::ERR_CLUSTER,
                            "Invalid slot or number of keys"};
            }

            std::string keysInfo = getKeys(svr, slot, count);

            return  keysInfo;
        } else if (arg1 == "slaves" && argSize == 3) {
            auto n = clusterState->clusterLookupNode(args[2]);
            if (n == nullptr) {
                return  {ErrorCodes::ERR_CLUSTER, "Unkown node"+args[2]};
            } else if (n->nodeIsSlave()) {
                return {ErrorCodes ::ERR_CLUSTER,
                        "The specified node is not a master"};
            }
            std::stringstream ss;
            uint16_t slavesNum = n->getSlaveNum();
            Command::fmtMultiBulkLen(ss, slavesNum);
            for (size_t i = 0;  i < slavesNum; i++) {
                std::string nodeDescription = n->_slaves[i]->clusterGenNodeDescription();
                Command::fmtBulk(ss, nodeDescription);
            }
            return ss.str();
        } else if (arg1 == "bumpepoch" && argSize == 2) {
            auto s = clusterState->clusterBumpConfigEpochWithoutConsensus();
            std::stringstream bumpinfo;
            std::string state = (s.ok()) ? "BUMPED" : "STILL";
            bumpinfo << state << myself->getConfigEpoch();
            return  Command::fmtBulk(bumpinfo.str());
        } else if (arg1 == "set-config-epoch" && argSize == 3) {
            Expected<uint64_t> exptEpoch = ::tendisplus::stoul(args[2]);
            if (!exptEpoch.ok()) {
                return exptEpoch.status();
            }
            uint64_t configEpoch = exptEpoch.value();
            if (args[2][0] == '-') {
                return {ErrorCodes::ERR_CLUSTER,
                    "Invalid config epoch specified:"+args[2]};
            } else if (clusterState->_nodes.size() > 1) {
                return {ErrorCodes::ERR_CLUSTER, "he user can assign a config"
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
            if (myself->nodeIsMaster() && !nodeNotEmpty(svr, myself)) {
                return {ErrorCodes::ERR_CLUSTER,
                        "CLUSTER RESET can't be called with "
                        "master nodes containing keys"};
            }
            auto clusterMgr = svr->getClusterMgr();
            auto s = clusterMgr->clusterReset(hard);
            if (!s.ok()) {
                return {ErrorCodes::ERR_CLUSTER,
                        "cluster reset fail!"};
            }
            return Command::fmtOK();
        } else if (arg1 == "saveconfig" && argSize == 2) {
            auto s = clusterState->clusterSaveConfig();
            if (!s.ok()) {
                return {ErrorCodes::ERR_CLUSTER,
                        "error saving the cluster node config"+s.toString()};
            }
            return Command::fmtOK();
        } else if (arg1 == "myid" && argSize == 2) {
            std::string nodeName = myself->getNodeName();
            return  Command::fmtBulk(nodeName);
        } else if (arg1 == "slots" && argSize == 2) {
            std::string slotInfo = clusterReplyMultiBulkSlots(clusterState);

            return  slotInfo;
        }
        return {ErrorCodes::ERR_CLUSTER,
                "Invalid cluster command " + args[1]};
    }

private:
    Status addSlots(uint32_t start, uint32_t end, const std::string &arg,
                    const std::shared_ptr<ClusterState> clusterState,
                    const CNodePtr myself) {
        bool result = false;
        if (start < end) {
            for (size_t i = start; i < end + 1; i++) {
                uint32_t index = static_cast<uint32_t>(i);
                if (arg == "addslots") {
                    if (clusterState->_allSlots[index] != nullptr) {
                        LOG(WARNING) << "slot" << index
                                     << "already busy";
                        continue;
                    }
                    result = clusterState->clusterAddSlot(myself,
                                                          index);
                } else {
                    if (clusterState->_allSlots[index] == nullptr) {
                        LOG(WARNING) << "slot" << index
                                     << "already delete";
                        continue;
                    }
                    result = clusterState->clusterDelSlot(index);
                }
                if (result == false) {
                    return {ErrorCodes::ERR_CLUSTER,
                            "del or add slot fail"};
                }
            }
        } else {
            LOG(ERROR) << "ERR Invalid or out of range slot";
            return {ErrorCodes::ERR_CLUSTER,
                    "ERR Invalid or out of range slot"};
        }
        return {ErrorCodes::ERR_OK, "finish addslots"};
    }

    Status addSingleSlot(uint32_t slot, const std::string &arg,
                         const std::shared_ptr<ClusterState> clusterState,
                         const CNodePtr myself) {
        bool result = false;
        if (arg == "addslots") {
            if (clusterState->_allSlots[slot] != nullptr) {
                return {ErrorCodes::ERR_CLUSTER,
                        "Slot is already busy"};
            }
            result = clusterState->clusterAddSlot(myself, slot);
        } else {
            if (clusterState->_allSlots[slot] == nullptr) {
                LOG(WARNING) << "slot" << slot
                             << "already delete";
                return {ErrorCodes::ERR_CLUSTER,
                        "Slot is already delete"};
            }
            result = clusterState->clusterDelSlot(slot);
        }
        if (result == false) {
            return {ErrorCodes::ERR_CLUSTER,
                    "del or add slot fail"};
        }
        return {ErrorCodes::ERR_OK, "finish add sigle slot"};
    }

    Status replicateNode(CNodePtr n, CNodePtr myself,
                         ServerEntry *svr, Session *sess, ReplManager *replMgr,
                         const std::shared_ptr<ClusterState> clusterState) {
        INVARIANT_D(n != nullptr);
        if (n == myself) {
            return {ErrorCodes::ERR_CLUSTER, "Can't replicate myself"};
        }
        /* Can't replicate a slave. */
        if (n->nodeIsSlave()) {
            return {ErrorCodes::ERR_CLUSTER,
                    "only replicate a master, not a slave"};
        }
        bool notEmpty = false;
        // slave db not empty
        std::list<Expected<DbWithLock>> expdbList;
        auto port = n->getPort();
        auto ip = n->getNodeIp();
        for (uint32_t i = 0; i < svr->getKVStoreCount(); ++i) {
            auto expdb = svr->getSegmentMgr()->getDb(sess, i,
                                                     mgl::LockMode::LOCK_X, true);
            if (!expdb.ok()) {
                return expdb.status();
            }
            if (!expdb.value().store->isOpen()) {
                return {ErrorCodes::ERR_CLUSTER, "store not open"};
            }
            if (!expdb.value().store->isEmpty(true)) {
                notEmpty = true;
            }
            expdbList.push_back(std::move(expdb));
        }
        if (myself->nodeIsMaster() &&(myself->_numSlots != 0 || notEmpty)) {
            return {ErrorCodes::ERR_CLUSTER,
                    "To set a master the node must be empty"};
        }
        clusterState->clusterSetMaster(n);
        for (uint32_t i = 0; i < svr->getKVStoreCount(); ++i) {
            Status s = replMgr->changeReplSourceInLock(i, ip, port, i);
            if (!s.ok()) {
                return {ErrorCodes::ERR_CLUSTER,
                        "replicate kvstore fail"};
            }
        }
        return {ErrorCodes::ERR_OK, "finish replicte"+n->getNodeName()};
    }

    std::string getKeys(ServerEntry *svr , uint32_t slot, uint32_t count) {
        std::vector<std::string> keysList = svr->getKeyBySlot(slot, count);
        std::stringstream keysInfo;
        uint32_t n = keysList.size();

        Command::fmtMultiBulkLen(keysInfo, n);
        for (auto &&vs : keysList) {
            Command::fmtBulk(keysInfo, vs);
        }
        return keysInfo.str();

    }

    bool nodeNotEmpty(ServerEntry *svr, CNodePtr node) {
        bool notEmpty = false;
        auto slots = node->getSlots();
        size_t idx = 0;
        while (idx < slots.size()) {
            if (slots.test(idx) && !svr->emptySlot(idx)) {
                notEmpty = true;
                break;
            }
            ++idx;
        }
        return  notEmpty;
    }

    std::string clusterReplyMultiBulkSlots(const std::shared_ptr<tendisplus::ClusterState> state) {
        std::stringstream ss;

        uint32_t nodeNum = 0;
        std::vector<CNodePtr> nodes;

        for (const auto &v: state->_nodes) {
            CNodePtr node = v.second;
            if (!node->nodeIsMaster() || node->getSlotNum() == 0)
                continue;
            else {
                nodes.push_back(node);
                nodeNum++;
            }
        }
        Command::fmtMultiBulkLen(ss, nodeNum);

        for (const auto &node: nodes) {
            int32_t start = -1;

            uint16_t  slaveNUm = node->getSlaveNum();
            for (int32_t j = 0; j < CLUSTER_SLOTS; j++) {
                auto bit = node->getSlots().test(j);
                if (bit) {
                    if (start == -1) start = j;
                }
                if (start != -1 && (!bit || j == CLUSTER_SLOTS-1)) {
                    if (bit && j == CLUSTER_SLOTS - 1) j ++;

                    Command::fmtMultiBulkLen(ss, slaveNUm+3);

                    if (start == j - 1) {
                        Command::fmtLongLong(ss, start);
                        Command::fmtLongLong(ss, start);
                    } else {
                        Command::fmtLongLong(ss, start);
                        Command::fmtLongLong(ss, j - 1);
                    }

                    Command::fmtMultiBulkLen(ss, 3);
                    Command::fmtBulk(ss, node->getNodeIp());
                    Command::fmtLongLong(ss, node->getPort());
                    Command::fmtBulk(ss, node->getNodeName());

                    for (uint16_t  i = 0; i < slaveNUm; i++) {
                        Command::fmtMultiBulkLen(ss, 3);
                        CNodePtr  slave = node->_slaves[i];
                        Command::fmtBulk(ss, slave->getNodeIp());
                        Command::fmtLongLong(ss, slave->getPort());
                        Command::fmtBulk(ss, slave->getNodeName());
                    }
                    start = -1;
                    }
                }
            }
        return ss.str();
    }

} clusterCmd;

class ReadymigrateCommand: public Command {
public:
    ReadymigrateCommand()
            :Command("readymigrate", "a") {
    }

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

    Expected<std::string> run(Session *sess) final {
        LOG(FATAL) << "readymigrate should not be called";
        // void compiler complain
        return {ErrorCodes::ERR_INTERNAL, "shouldn't be called"};
    }
} readymigrateCommand;

class MigrateendCommand: public Command {
public:
    MigrateendCommand()
            :Command("migrateend", "rs") {
    }

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

        Expected<uint64_t> exptChunkid = ::tendisplus::stoul(args[1]);
        if (!exptChunkid.ok()) {
            return exptChunkid.status();
        }
        uint32_t chunkid = (uint32_t)exptChunkid.value();

        auto s = migrateMgr->supplyMigrateEnd(chunkid);
        if (!s.ok()) {
            return s;
        }
        return Command::fmtOK();
    }
} migrateendCmd;

}  // namespace tendisplus
