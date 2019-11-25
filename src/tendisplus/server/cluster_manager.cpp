#include <math.h>
#include "tendisplus/utils/time.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/server/cluster_manager.h"
#include "tendisplus/storage/catalog.h"
#include "tendisplus/storage/varint.h"
#include "tendisplus/utils/redis_port.h"

namespace tendisplus {

template <typename T>
void CopyUint(std::vector<uint8_t> *buf, T element) {
    for (size_t i = 0; i < sizeof(element); ++i) {
        buf->emplace_back((element>>((sizeof(element)-i-1)*8))&0xff);
    }
}

inline ClusterHealth int2ClusterHealth(const uint8_t t) {
    if (t == 1) {
        return ClusterHealth::CLUSTER_OK;
    } else {
        return ClusterHealth::CLUSTER_FAIL;
    }
}

ClusterNode::ClusterNode(const std::string& nodeName,
                    const uint16_t flags,
                    std::shared_ptr<ClusterState> cstate,
                    const std::string& host,
                    uint32_t port, uint32_t cport,
                    uint64_t pingSend, uint64_t pongReceived,
                    uint64_t epoch, const std::vector<std::string>& slots_)
    :_nodeName(nodeName),
     _configEpoch(epoch),
     _nodeIp(host),
     _nodePort(port),
     _nodeCport(cport),
     _clusterState(cstate),
     _nodeSession(nullptr),
     _ctime(msSinceEpoch()),
     _flags(flags),
     _numSlots(0),
     _numSlaves(0),
     _slaveOf(nullptr),
     _pingSent(0),
     _pongReceived(0),
     _votedTime(0),
     _replOffsetTime(0),
     _orphanedTime(0),
     _replOffset(0) {
    // TODO(wayenchen): handle the slot
}

void ClusterNode::prepareToFree() {
    /* If the node has associated slaves, we have to set
    * all the slaves->slaveof fields to NULL (unknown). */
    for (auto slave : _slaves) {
        slave->setMaster(nullptr);
    }

    /* Remove this node from the list of slaves of its master. */
    if (nodeIsSlave() && _slaveOf) {
        _slaveOf->removeSlave(shared_from_this());
    }

    if (_nodeSession) {
        // TODO(vinchen): free the session
        _nodeSession->setNode(nullptr);
    }
}

void ClusterNode::setNodeName(const std::string& name) {
    std::lock_guard<std::mutex> lk(_mutex);
    _nodeName = name;
}

void ClusterNode::setNodeIp(const std::string& ip) {
    std::lock_guard<std::mutex> lk(_mutex);
    _nodeIp = ip;
}
void ClusterNode::setNodePort(uint64_t port) {
    std::lock_guard<std::mutex> lk(_mutex);
    _nodePort = port;
}

void ClusterNode::setNodeCport(uint64_t cport) {
    std::lock_guard<std::mutex> lk(_mutex);
    _nodeCport = cport;
}

void ClusterNode::setConfigEpoch(uint64_t epoch) {
    std::lock_guard<std::mutex> lk(_mutex);
    _configEpoch = epoch;
}

Status ClusterNode::addSlot(uint32_t slot) {
    std::lock_guard<std::mutex> lk(_mutex);
    if (!_mySlots.test(slot)) {
        _mySlots.set(slot);
        _numSlots++;
        return  {ErrorCodes::ERR_OK, ""};
    } else {
        return  {ErrorCodes::ERR_NOTFOUND, ""};
    }
}

bool ClusterNode::setSlotBit(uint32_t slot, uint32_t masterSlavesCount) {
    std::lock_guard<std::mutex> lk(_mutex);
    bool old = _mySlots.test(slot);
    _mySlots.set(slot);
    if (!old) {
        _numSlots++;
        /* When a master gets its first slot, even if it has no slaves,
        * it gets flagged with MIGRATE_TO, that is, the master is a valid
        * target for replicas migration, if and only if at least one of
        * the other masters has slaves right now.
        *
        * Normally masters are valid targets of replica migration if:
        * 1. The used to have slaves (but no longer have).
        * 2. They are slaves failing over a master that used to have slaves.
        *
        * However new masters with slots assigned are considered valid
        * migration targets if the rest of the cluster is not a slave-less.
        *
        * See https://github.com/antirez/redis/issues/3043 for more info. */
        // if (_numSlots == 1 && _clusterState->clusterMastersHaveSlavesNoLock())
        if (_numSlots == 1 && masterSlavesCount)
            _flags |= CLUSTER_NODE_MIGRATE_TO;
    }

    return old;
}

/* Clear the slot bit and return the old value. */
bool ClusterNode::clearSlotBit(uint32_t slot) {
    std::lock_guard<std::mutex> lk(_mutex);
    bool old = _mySlots.test(slot);
    _mySlots.reset(slot);
    if (old) _numSlots--;

    return old;
}

bool ClusterNode::getSlotBit(uint32_t slot) const {
    std::lock_guard<std::mutex> lk(_mutex);
    return _mySlots.test(slot);
}

uint32_t ClusterNode::delAllSlotsNoLock() {
    INVARIANT_D(0);
    return 0;
}

// clusterDelNodeSlots
/* Delete all the slots associated with the specified node.
* The number of deleted slots is returned. */
uint32_t ClusterNode::delAllSlots() {
    std::lock_guard<std::mutex> lk(_mutex);
    return delAllSlotsNoLock();
}

Status ClusterNode::addSlave(std::shared_ptr<ClusterNode> slave) {
    std::lock_guard<std::mutex> lk(_mutex);
    for (auto v : _slaves) {
        if (v == slave)
            return{ ErrorCodes::ERR_NOTFOUND, "already is slave" };
    }

    _slaves.emplace_back(std::move(slave));
    _numSlaves++;
    _flags |= CLUSTER_NODE_MIGRATE_TO;
    return  {ErrorCodes::ERR_OK, ""};
}

Status ClusterNode::removeSlave(std::shared_ptr<ClusterNode> slave) {
    std::lock_guard<std::mutex> lk(_mutex);
    INVARIANT_D(0);
    return  {ErrorCodes::ERR_OK, ""};
}

// return true, mean add one new failure report
bool ClusterNode::addFailureReport(std::shared_ptr<ClusterNode> sender) {
    std::lock_guard<std::mutex> lk(_mutex);
    INVARIANT_D(0);
    return true;
}

bool ClusterNode::delFailureReport(std::shared_ptr<ClusterNode> sender) {
    std::lock_guard<std::mutex> lk(_mutex);
    //INVARIANT_D(0);
    cleanupFailureReportsNoLock();
    return false;  /* No failure report from this sender. */

}

uint32_t ClusterNode::getNonFailingSlavesCount() const {
    INVARIANT_D(0);
    return 0;
}

void ClusterNode::cleanupFailureReportsNoLock() {
    // the caller should guarantee _mutex has been locked
    //INVARIANT_D(0);
    // TODO(wayenchen)
}

uint32_t ClusterNode::failureReportsCount() {
    std::lock_guard<std::mutex> lk(_mutex);
    cleanupFailureReportsNoLock();

    return _failReport.size();
}

void ClusterNode::markAsFailingIfNeeded() {
    uint32_t failures;
    uint32_t needed_quorum = (_clusterState->getNodeCount() / 2) + 1;
    auto myself = _clusterState->getMyselfNode();

    if (!nodeTimedOut()) return; /* We can reach it. */
    if (nodeFailed()) return; /* Already FAILing. */

    failures = failureReportsCount();
    /* Also count myself as a voter if I'm a master. */
    if (myself->nodeIsMaster()) failures++;
    if (failures < needed_quorum) return; /* No weak agreement from masters. */

    redis_port::serverLog(LL_NOTICE,
        "Marking node %.40s as failing (quorum reached).", getNodeName().c_str());

    {
        std::lock_guard<std::mutex> lk(_mutex);

        /* Mark the node as failing. */
        _flags &= ~CLUSTER_NODE_PFAIL;
        _flags |= CLUSTER_NODE_FAIL;
        _failTime = msSinceEpoch();
    }

    /* Broadcast the failing node name to everybody, forcing all the other
    * reachable nodes to flag the node as FAIL. */
    if (myself->nodeIsMaster()) {
        _clusterState->clusterSendFail(_nodeName);
    }

    _clusterState->clusterSaveNodes();
    //clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE | CLUSTER_TODO_SAVE_CONFIG);
}


void ClusterNode::setAsMaster() {
    std::lock_guard<std::mutex> lk(_mutex);
    INVARIANT_D(0);

}

bool ClusterNode::changeToSlaveIfNeeded() {
    std::lock_guard<std::mutex> lk(_mutex);
    if (!nodeIsMaster()) {
        return false;
    }

    delAllSlotsNoLock();

    _flags &= ~(CLUSTER_NODE_MASTER |
        CLUSTER_NODE_MIGRATE_TO);
    _flags |= CLUSTER_NODE_SLAVE;

    return true;
}

void ClusterNode::setMaster(const std::shared_ptr<ClusterNode>& master) {
    std::lock_guard<std::mutex> lk(_mutex);
    _slaveOf = master;
}

bool  ClusterNode::nodeIsMaster() const {
    return (_flags & CLUSTER_NODE_MASTER) ? true : false;
}

bool  ClusterNode::nodeIsSlave() const {
    return (_flags & CLUSTER_NODE_SLAVE) ? true : false;
}

bool  ClusterNode::nodeHasAddr() const {
    return (_flags & CLUSTER_NODE_NOADDR) ? false : true;
}

bool  ClusterNode::nodeWithoutAddr() const {
    return (_flags & CLUSTER_NODE_NOADDR) ? true : false;
}

bool  ClusterNode::nodeIsMyself() const {
    return (_flags & CLUSTER_NODE_MYSELF) ? true : false;
}

bool ClusterNode::nodeInHandshake() const {
    return (_flags & CLUSTER_NODE_HANDSHAKE) ? true : false;
}

bool ClusterNode::nodeTimedOut() const {
    return (_flags & CLUSTER_NODE_PFAIL) ? true : false;
}

bool ClusterNode::nodeFailed() const {
    return (_flags & CLUSTER_NODE_FAIL) ? true : false;
}

bool ClusterNode::nodeCantFailover() const {
    return (_flags & CLUSTER_NODE_NOFAILOVER) ? true : false;
}

/* Update the node address to the IP address that can be extracted
* from link->fd, or if hdr->myip is non empty, to the address the node
* is announcing us. The port is taken from the packet header as well.
*
* If the address or port changed, disconnect the node link so that we'll
* connect again to the new address.
*
* If the ip/port pair are already correct no operation is performed at
* all.
*
* The function returns 0 if the node address is still the same,
* otherwise 1 is returned. */
bool ClusterNode::updateAddressIfNeeded(ClusterSession* sess, const ClusterMsg& msg) {
    // TODO(wayenchen)
    return false;
}

void ClusterNode::setSession(const std::shared_ptr<ClusterSession>& sess) {
    std::lock_guard<std::mutex> lk(_mutex);
    INVARIANT_D(!_nodeSession);
    _nodeSession = sess;
}

ClusterState::ClusterState(std::shared_ptr<ServerEntry> server)
    :_myself(nullptr),
     _currentEpoch(0),
     _server(server),
     _state(ClusterHealth::CLUSTER_FAIL),
     _size(0),
     _migratingSlots(),
     _importingSlots(),
     _allSlots(),
     _slotsKeysCount(),
     _failoverAuthTime(0),
     _failoverAuthCount(0),
     _failoverAuthSent(0),
     _failoverAuthRank(0),
     _failoverAuthEpoch(0),
     _cantFailoverReason(CLUSTER_CANT_FAILOVER_NONE),
     _mfEnd(0),
     _mfSlave(nullptr),
     _mfMasterOffset(0),
     _mfCanStart(0),
     _lastVoteEpoch(0),
     _todoBeforeSleep(0),
     _statsMessagesSent({}),
     _statsMessagesReceived({}),
     _statsPfailNodes(0) {
}


bool ClusterState::clusterHandshakeInProgress(const std::string& host, uint32_t port, uint32_t cport) {
    std::lock_guard<std::mutex> lk(_mutex);
    for (auto& pnode : _nodes) {
        auto node = pnode.second;
        if (!node->nodeInHandshake())
            continue;

        if (node->getNodeIp() == host &&
            node->getPort() == port &&
            node->getCport() == cport)
            return true;
    }

    return false;
}

bool ClusterState::clusterStartHandshake(const std::string& host, uint32_t port, uint32_t cport) {
    // TODO(wayenchen)
    /* IP sanity check */

    if (clusterHandshakeInProgress(host, port, cport)) {
        // errno = EAGAIN;
        // return 0;
        return false;
    }

    auto name = getUUid(20);
    auto node = std::make_shared<ClusterNode>(name, CLUSTER_NODE_HANDSHAKE | CLUSTER_NODE_MEET,
        shared_from_this(),
        host, port, cport);
    clusterAddNode(node);

    return true;
}

void ClusterState::clusterSendFail(const std::string& nodename) {
    ClusterMsg msg(ClusterMsg::Type::FAIL, shared_from_this(), _server);
    clusterBroadcastMessage(msg);
}

void ClusterState::clusterBroadcastMessage(const ClusterMsg& msg) {
    INVARIANT_D(0);
}

Status ClusterState::clusterUpdateSlotsConfigWith(CNodePtr sender,
    uint64_t senderConfigEpoch, const std::bitset<CLUSTER_SLOTS>& slots) {

    INVARIANT_D(0);
    return{ ErrorCodes::ERR_OK, "" };
}


Status ClusterState::clusterHandleConfigEpochCollision(CNodePtr sender) {
    INVARIANT_D(0);
    return{ ErrorCodes::ERR_OK, "" };

}

void ClusterState::setCurrentEpoch(uint64_t epoch) {
    std::lock_guard<std::mutex> lk(_mutex);
    _currentEpoch = epoch;
}

CNodePtr ClusterState::getNodeBySlot(uint32_t slot) const {
    std::lock_guard<std::mutex> lk(_mutex);
    return _allSlots[slot];
}

void ClusterState::setMyselfNode(CNodePtr node) {
    std::lock_guard<std::mutex> lk(_mutex);
    INVARIANT(node != nullptr);
    if (!_myself) {
        _myself = node;
    }
}

void ClusterState::clusterSaveNodesNoLock() {
    //INVARIANT_D(0);
    // TODO(waynenchen)
}

void ClusterState::clusterSaveNodes() {
    std::lock_guard<std::mutex> lk(_mutex);
    clusterSaveNodesNoLock();
}

void ClusterState::clusterBlacklistAddNode(CNodePtr node) {
    std::lock_guard<std::mutex> lk(_mutex);
    INVARIANT_D(0);
}

void ClusterState::clusterBlacklistCleanupNoLock() {
    INVARIANT_D(0);
}

bool ClusterState::clusterBlacklistExists(const std::string& nodeid) const {
    INVARIANT_D(0);
    return false;
}

void ClusterState::clusterAddNodeNoLock(CNodePtr node) {
    std::string nodeName = node->getNodeName();
    std::unordered_map<std::string, CNodePtr>::iterator it;
    if ((it = _nodes.find(nodeName)) != _nodes.end()) {
        _nodes[nodeName] = it->second;
    } else {
        _nodes.insert(std::make_pair(nodeName, node));
    }
}

void ClusterState::clusterAddNode(CNodePtr node, bool save) {
    std::lock_guard<std::mutex> lk(_mutex);

    clusterAddNodeNoLock(node);
    if (save) {
        clusterSaveNodesNoLock();
    }
}
void ClusterState::clusterDelNodeNoLock(CNodePtr delnode) {
    /* 1) Mark slots as unassigned. */
    for (uint32_t j = 0; j < CLUSTER_SLOTS; j++) {
        if (_importingSlots[j] == delnode ) {
            _importingSlots[j] = nullptr;
        }
        if (_migratingSlots[j] == delnode) {
            _migratingSlots[j] = nullptr;
        }
        if (_allSlots[j] == delnode) {
            // TODO(vinchen)
        }
    }


    /* 2) Remove failure reports. */
    for (const auto& nodep : _nodes) {
        const auto& node = nodep.second;
        if (node == delnode)
            continue;

        node->delFailureReport(delnode);
    }

    /* 3) Free the node, unlinking it from the cluster. */
    freeClusterNode(delnode);
}

uint32_t ClusterState::clusterMastersHaveSlavesNoLock() {
    uint32_t n_slaves = 0;
    for (const auto& nodep : _nodes) {
        const auto& node = nodep.second;

        if (node->nodeIsSlave())
            continue;

        // TODO(vinchen): it is safe here without lock?
        n_slaves += node->_numSlaves;
    }

    return n_slaves;
}

void ClusterState::freeClusterNode(CNodePtr delnode) {
    _nodes.erase(delnode->getNodeName());
    delnode->prepareToFree();
}

void ClusterState::clusterDelNode(CNodePtr node, bool save) {
    std::lock_guard<std::mutex> lk(_mutex);
    clusterDelNodeNoLock(node);

    if (save) {
        clusterSaveNodesNoLock();
    }
}

void ClusterState::clusterRenameNode(CNodePtr node, const std::string& newname, bool save) {
    redis_port::serverLog(LL_DEBUG, "Renaming node %.40s into %.40s",
        node->getNodeName(), newname);

    std::lock_guard<std::mutex> lk(_mutex);

    clusterDelNodeNoLock(node);
    node->setNodeName(newname);
    clusterAddNodeNoLock(node);

    if (save) {
        clusterSaveNodes();
    }
}

CNodePtr ClusterState::getRandomNode() const {
    std::lock_guard<std::mutex> lk(_mutex);
    auto rand = std::rand() % _nodes.size();
    auto random_it = std::next(std::begin(_nodes), rand);
    return random_it->second;
}

/* find if node in cluster, */
CNodePtr ClusterState::clusterLookupNode(const std::string& name) {
    std::lock_guard<std::mutex> lk(_mutex);
    std::unordered_map<std::string, CNodePtr>::iterator it;

    if ((it = _nodes.find(name)) != _nodes.end()) {
        return it->second;
    } else {
        return nullptr;
    }
}

const std::unordered_map<std::string, CNodePtr> ClusterState::getNodesList() const {
    std::lock_guard<std::mutex> lk(_mutex);
    return _nodes;
}

uint32_t ClusterState::getNodeCount() const {
    std::lock_guard<std::mutex> lk(_mutex);

    return _nodes.size();
}

bool ClusterState::setMfMasterOffsetIfNecessary(const CNodePtr& node) {
    std::lock_guard<std::mutex> lk(_mutex);
    if (_mfEnd &&
        _myself->nodeIsSlave() &&
        _myself->getMaster() == node &&
        //hdr->_mflags[0] & CLUSTERMSG_FLAG0_PAUSED &&
        _mfMasterOffset == 0) {
        _mfMasterOffset = node->_replOffset;
        return true;
    }
    return false;
    
}

/* Add the specified slot to the list of slots that node 'n' will
 * serve. Return true if the operation ended with success.
 * If the slot is already assigned to another instance this is considered
 * an error and false is returned. */

bool ClusterState::clusterAddSlot(CNodePtr node, const uint32_t slot) {
    std::lock_guard<std::mutex> lk(_mutex);
    if (_allSlots[slot] != nullptr/* || _allSlots[slot] != node*/) {
        return false;
    } else {
         node->addSlot(slot);
         _allSlots[slot] = node;
         return true;
    }
}

bool ClusterState::clusterDelSlotNoLock(const uint32_t slot) {
    auto n = _allSlots[slot];
    if (!n) {
        return false;
    }

    bool old = n->clearSlotBit(slot);
    INVARIANT(old);
    _allSlots[slot] = nullptr;
    return true;
}

/* Delete the specified slot marking it as unassigned.
* Returns true if the slot was assigned, otherwise if the slot was
* already unassigned false is returned. */
bool ClusterState::clusterDelSlot(const uint32_t slot) {
    std::lock_guard<std::mutex> lk(_mutex);

    return clusterDelSlotNoLock(slot);
}

/* Delete all the slots associated with the specified node.
* The number of deleted slots is returned. */
uint32_t ClusterState::clusterDelNodeSlots(CNodePtr node) {
    std::lock_guard<std::mutex> lk(_mutex);
    uint32_t deleted = 0, j;

    for (j = 0; j < CLUSTER_SLOTS; j++) {
        if (node->getSlotBit(j)) {
            clusterDelSlotNoLock(j);
            deleted++;
        }
    }
    return deleted;
}

/* Clear the migrating / importing state for all the slots.
* This is useful at initialization and when turning a master into slave. */
void ClusterState::clusterCloseAllSlots() {
    //memset(server.cluster->migrating_slots_to, 0,
    //    sizeof(server.cluster->migrating_slots_to));
    // memset(server.cluster->importing_slots_from, 0,
    //    sizeof(server.cluster->importing_slots_from));
}

uint64_t ClusterState::clusterGetOrUpdateMaxEpoch(bool update) {
    std::lock_guard<std::mutex> lk(_mutex);
    uint64_t max = 0;
    for (auto& node : _nodes) {
        if (node.second->getConfigEpoch() > max) {
            max = node.second->getConfigEpoch();
        }
    }

    if (max < _currentEpoch) {
        max = _currentEpoch;
    }

    if (update) {
        _currentEpoch = max;
    }

    return max;
}

ClusterMsg::ClusterMsg(const ClusterMsg::Type type,
    const std::shared_ptr<ClusterState> cstate,
    const std::shared_ptr<ServerEntry> svr) 
    : _header(std::make_shared<ClusterMsgHeader>(type, cstate, svr)),
      _msgData(nullptr) {
    switch (type) {
    case Type::MEET:
    case Type::PING:
    case Type::PONG:
        _msgData = std::move(std::make_shared<ClusterMsgDataGossip>());
        break;
    default:
        INVARIANT(0);
        break;
    }
}

ClusterMsg::ClusterMsg(const std::shared_ptr<ClusterMsgHeader>& header,
                                        const std::shared_ptr<ClusterMsgData>& data)
    :_header(header),
     _msgData(data) {
}

std::string ClusterMsg::clusterGetMessageTypeString(Type type) {
    switch (type) {
    case Type::MEET:
        return "meet";
    case Type::PING:
        return "ping";
    case Type::PONG:
        return "pong";
    case Type::FAIL:
        return "fail";
    case Type::PUBLISH:
        return "publish";
    case Type::FAILOVER_AUTH_ACK:
        return "auth-req";
    case Type::FAILOVER_AUTH_REQUEST:
        return "auth-ack";
    case Type::UPDATE:
        return "update";
    case Type::MFSTART:
        return "mfstart";
    default:
        INVARIANT_D(0);
        return "unknown";
    }


}

bool ClusterMsg::clusterNodeIsInGossipSection(const CNodePtr& node) const {
    INVARIANT_D(_msgData != nullptr);
    return _msgData->clusterNodeIsInGossipSection(node);
}

void ClusterMsg::clusterAddGossipEntry(const CNodePtr& node) {
    INVARIANT_D(_msgData != nullptr);
    _msgData->addGossipEntry(node);
    _header->_count++;
}

void ClusterMsg::setEntryCount(uint32_t count) { 
    _header->_count = count;
}

uint32_t ClusterMsg::getEntryCount() const {
    INVARIANT_D(_header != nullptr);
    return _header->_count;
}

bool ClusterMsg::isMaster() const {
    INVARIANT_D(_header != nullptr);
    return _header->_slaveOf == ClusterMsgHeader::CLUSTER_NODE_NULL_NAME;
}

std::string ClusterMsg::msgEncode() const {
    std::vector<uint8_t> key;

    std::string data = _msgData->dataEncode();

    // TODO(vinchen): FIXME: update _totlen here, it is very tricky.
    auto hdrSize = _header->getHeaderSize();
    _header->_totlen = hdrSize + data.size();

    std::string head = _header->headEncode();
    key.insert(key.end(), head.begin(), head.end());

    key.insert(key.end(), data.begin(), data.end());

    INVARIANT_D(_header->_totlen == key.size());

    return std::string(reinterpret_cast<const char *>(
                key.data()), key.size());
}

Expected<ClusterMsg> ClusterMsg::msgDecode(const std::string& key) {
    auto headLength = ClusterMsgHeader::getHeaderSize();
    auto headerStr = key.substr(0, headLength);

    Expected<ClusterMsgHeader> header = ClusterMsgHeader::headDecode(headerStr);

    if (!header.ok()) {
        return header.status();
    }
    auto headerPtr = std::make_shared<ClusterMsgHeader>
            (std::move(header.value()));

    auto msgStr = key.substr(headLength, key.size());
    auto type  = headerPtr->_type;
    std::shared_ptr<ClusterMsgData> msgDataPtr;

    if (type == Type::PING|| type == Type::PONG||
          type == Type::MEET) {
        auto count = headerPtr->_count;
        auto msgGData = ClusterMsgDataGossip::dataDecode(msgStr, count);
        if (!msgGData.ok()) {
            return msgGData.status();
        }
        msgDataPtr = std::make_shared<ClusterMsgDataGossip>
            (std::move(msgGData.value()));

    } else if (type == Type::UPDATE) {
        auto msgUData = ClusterMsgDataUpdate::dataDecode(msgStr);
        if (!msgUData.ok()) {
            return msgUData.status();
        }
        msgDataPtr = std::make_shared<ClusterMsgDataUpdate>
            (std::move(msgUData.value()));
    } else {
        return {ErrorCodes::ERR_OK, "decode error type"};
    }

    return ClusterMsg(headerPtr, msgDataPtr);
}


ClusterMsgHeader::ClusterMsgHeader(const ClusterMsg::Type type,
                const std::shared_ptr<ClusterState> cstate,
                const std::shared_ptr<ServerEntry> svr)
    :_sig("RCmb"),
     _ver(ClusterMsg::CLUSTER_PROTO_VER),
     _type(type),
     _count(0),
     _currentEpoch(cstate->getCurrentEpoch()),
     _slaveOf(CLUSTER_NODE_NULL_NAME),
     _state(cstate->_state)  {
    auto myself = cstate->getMyselfNode();
    /* If this node is a master, we send its slots bitmap and configEpoch.
    * If this node is a slave we send the master's information instead (the
    * node is flagged as slave so the receiver knows that it is NOT really
    * in charge for this slots. */
    auto master = (myself->nodeIsSlave() && myself->_slaveOf) ?
                        myself->_slaveOf : myself;

    _sender = myself->getNodeName();

    std::shared_ptr<ServerParams> params = svr->getParams();
    _myIp = params->bindIp;
    _port = params->port;
    _cport = _port + CLUSTER_PORT_INCR;

    // TODO(wayenchen)
    /* Handle cluster-announce-port/cluster-announce-ip as well. */

    _slots = master->_mySlots;
    if (myself->_slaveOf) {
        _slaveOf = myself->_slaveOf->getNodeName();
    }
    INVARIANT_D(_slaveOf.size() == 40);

    _flags = myself->getFlags();
    _configEpoch = master->getConfigEpoch();

    // TODO(wayenchen)
    /* Set the replication offset. */
    _offset = 0;
    if (myself->nodeIsMaster() && cstate->_mfEnd) {
        _mflags[0] |= CLUSTERMSG_FLAG0_PAUSED;
    }

    if (type == ClusterMsg::Type::FAIL) {
        

    } else if (type == ClusterMsg::Type::UPDATE) {
        _totlen = sizeof(ClusterMsg) - sizeof(ClusterMsgData);
        _totlen += sizeof(ClusterMsgDataUpdate);
    } else {
        _totlen = 0;
    }
}


ClusterMsgHeader::ClusterMsgHeader(const std::string& sig,
                const uint32_t totlen,
                const uint16_t port , const ClusterMsg::Type type,
                const uint16_t count, const uint64_t currentEpoch,
                const uint64_t configEpoch, const uint64_t offset,
                const std::string& sender,
                const std::bitset<CLUSTER_SLOTS>& slots,
                const std::string& slaveOf, const std::string& myIp,
                const uint16_t cport, const uint16_t flags, ClusterHealth state)
    :_sig(sig),
     _totlen(totlen),
     _ver(ClusterMsg::CLUSTER_PROTO_VER),
     _port(port),
     _type(type),
     _count(count),
     _currentEpoch(currentEpoch),
     _configEpoch(configEpoch),
     _offset(offset),
     _sender(sender),
     _slots(slots),
     _slaveOf(slaveOf),
     _myIp(myIp),
     _cport(cport),
     _flags(flags),
     _state(state) {
}

ClusterMsgHeader::ClusterMsgHeader(ClusterMsgHeader&& o)
    :_sig(std::move(o._sig)),
     _totlen(std::move(o._totlen)),
     _ver(std::move(o._ver)),
     _port(std::move(o._port)),
     _type(std::move(o._type)),
     _count(std::move(o._count)),
     _currentEpoch(std::move(o._currentEpoch)),
     _configEpoch(std::move(o._configEpoch)),
     _offset(std::move(o._offset)),
     _sender(std::move(o._sender)),
     _slots(std::move(o._slots)),
     _slaveOf(std::move(o._slaveOf)),
     _myIp(std::move(o._myIp)),
     _cport(std::move(o._cport)),
     _flags(std::move(o._flags)),
     _state(std::move(o._state)) {
}

size_t ClusterMsgHeader::getHeaderSize()  {
    size_t strLen = 4 + CLUSTER_NAME_LENGTH*2 + CLUSTER_IP_LENGTH;
    size_t intLen = sizeof(_totlen) + sizeof(_ver) + sizeof(_port)
            + sizeof(_type) + sizeof(_count) + sizeof(_currentEpoch)
            + sizeof(_configEpoch) + sizeof(_offset) + sizeof(_cport)
            + sizeof(_flags) + 1;
    return strLen + intLen + sizeof(_slots);
}


std::string ClusterMsgHeader::headEncode() const {
    std::vector<uint8_t> key;
    key.reserve(ClusterMsgHeader::getHeaderSize());

    key.insert(key.end(), _sig.begin(), _sig.end());
    CopyUint(&key, _totlen);
    CopyUint(&key, _ver);
    CopyUint(&key, _port);
    CopyUint(&key, (uint16_t)_type);
    CopyUint(&key, _count);
    CopyUint(&key, _currentEpoch);
    CopyUint(&key, _configEpoch);
    CopyUint(&key, _offset);

    key.insert(key.end(), _sender.begin(), _sender.end());
    //  _slots
    auto* ptr = reinterpret_cast<const uint8_t*>(&_slots);
    const size_t len = sizeof(_slots);
    std::copy(ptr, ptr + len, std::back_inserter(key));
    //  slaveOf
    key.insert(key.end(), _slaveOf.begin(), _slaveOf.end());

    key.insert(key.end(), _myIp.begin(), _myIp.end());

    uint16_t ipLen = CLUSTER_IP_LENGTH - _myIp.size();
    std::vector<uint8_t> zeroVec(ipLen, '\0');
    key.insert(key.end(), zeroVec.begin(), zeroVec.end());

    CopyUint(&key, _cport);
    CopyUint(&key, _flags);

    uint8_t state = (_state == ClusterHealth::CLUSTER_FAIL) ? 0 : 1;

    CopyUint(&key, state);

    INVARIANT_D(key.size() == getHeaderSize());

    return std::string(reinterpret_cast<const char *>(
                key.data()), key.size());
}

Expected<ClusterMsgHeader> ClusterMsgHeader::headDecode(const std::string& key) {
    if (key.size() != ClusterMsgHeader::getHeaderSize()) {
        return {ErrorCodes::ERR_DECODE, "invalid keylen"};
    }
    size_t offset = 0;

    std::string sig(key.c_str()+offset, 4);
    if (sig != "RCmb") {
        return {ErrorCodes::ERR_DECODE, "invalid cluster message header"};
    }

    offset +=4;

    auto decode = [&] (auto func) { auto n = func(key.c_str()+offset); offset+=sizeof(n); return n; };

    auto totlen = decode(int32Decode);
    auto ver = decode(int16Decode);
    if (ver != ClusterMsg::CLUSTER_PROTO_VER) {
        return {ErrorCodes::ERR_DECODE, "Can't handle messages of different versions."};
    }
    auto port = decode(int16Decode);
    auto ptype = decode(int16Decode);
    if (ptype >= ClusterMsg::CLUSTERMSG_TYPE_COUNT) {
        return{ ErrorCodes::ERR_DECODE, "invalid cluster message type" + std::to_string(ptype) };
    }
    auto type = (ClusterMsg::Type)(ptype);
    auto count = decode(int16Decode);
    auto currentEpoch = decode(int64Decode);
    auto configEpoch = decode(int64Decode);
    auto headOffset = decode(int64Decode);
    std::string sender(key.c_str()+offset, CLUSTER_NAME_LENGTH);
    offset += CLUSTER_NAME_LENGTH;

    std::bitset<CLUSTER_SLOTS> slots;
    constexpr size_t kSlotsBytes = sizeof(slots);
    std::copy(key.begin()+offset, key.begin()+offset+kSlotsBytes,
            reinterpret_cast<uint8_t*>(&slots));
    offset += kSlotsBytes;


    std::string slaveOf(key.c_str()+offset, CLUSTER_NAME_LENGTH);
    offset += CLUSTER_NAME_LENGTH;

    std::string myIp(key.c_str()+offset, CLUSTER_IP_LENGTH);
    auto  pos = myIp.find('\0');
    if (pos > 0) {
        myIp.erase(pos, CLUSTER_IP_LENGTH);
    }
    offset += CLUSTER_IP_LENGTH;
    uint16_t cport = decode(int16Decode);
    uint16_t flags = decode(int16Decode);


    uint8_t  s = static_cast<uint8_t>(key.back());
    return  ClusterMsgHeader(sig, totlen,
                port, type, count, currentEpoch, configEpoch, headOffset,
                sender, slots, slaveOf, myIp, cport, flags, int2ClusterHealth(s));
}

ClusterMsgDataUpdate::ClusterMsgDataUpdate(const std::shared_ptr<ClusterNode> cnode)
    : ClusterMsgData(ClusterMsgData::Type::Update),
     _configEpoch(cnode->getConfigEpoch()),
     _nodeName(cnode->getNodeName()),
     _slots(cnode->_mySlots) {
}

ClusterMsgDataUpdate::ClusterMsgDataUpdate()
    : ClusterMsgData(ClusterMsgData::Type::Update),
     _configEpoch(0),
     _nodeName() {
}

ClusterMsgDataUpdate::ClusterMsgDataUpdate(const uint64_t configEpoch,
                const std::string &nodeName,
                const std::bitset<CLUSTER_SLOTS>& slots)
    : ClusterMsgData(ClusterMsgData::Type::Update),
     _configEpoch(configEpoch),
     _nodeName(nodeName),
     _slots(slots) {
}

size_t ClusterMsgDataUpdate::getDataSize() {
    size_t updateLen = sizeof(_configEpoch) + CLUSTER_NAME_LENGTH;
    return updateLen  + CLUSTER_SLOTS/8;
}

std::string ClusterMsgDataUpdate::dataEncode() const {
    std::vector<uint8_t> key;
    key.reserve(ClusterMsgDataUpdate::getDataSize());

    //  _configEpoch
    CopyUint(&key, _configEpoch);

    //  _nodeName
    key.insert(key.end(), _nodeName.begin(), _nodeName.end());
    //  _slots  std::bitset<CLUSTER_SLOTS>
    auto* ptr = reinterpret_cast<const uint8_t*>(&_slots);
    const size_t len = sizeof(_slots);
    std::copy(ptr, ptr + len, std::back_inserter(key));

    return std::string(reinterpret_cast<const char *>(
                key.data()), key.size());
}

Expected<ClusterMsgDataUpdate> ClusterMsgDataUpdate::dataDecode(const std::string& key) {
    if (key.size() != ClusterMsgDataUpdate::getDataSize()) {
        return {ErrorCodes::ERR_DECODE, "invalid keylen"};
    }
    size_t offset = 0;
    auto decode = [&] (auto func, size_t size) { auto n = func(key.c_str()+offset); offset+=size; return n;};

    auto configEpoch = decode(int64Decode, sizeof(uint64_t));

    std::string nodeName(key.c_str()+offset, CLUSTER_NAME_LENGTH);
    offset += CLUSTER_NAME_LENGTH;

    std::bitset<CLUSTER_SLOTS> slots;
    constexpr size_t kSlotsBytes = sizeof(slots);
    std::copy(key.begin()+offset, key.begin()+offset+kSlotsBytes,
            reinterpret_cast<uint8_t*>(&slots));
    return ClusterMsgDataUpdate(configEpoch, nodeName, slots);
}

ClusterMsgDataGossip::ClusterMsgDataGossip()
    : ClusterMsgData(ClusterMsgData::Type::Gossip) {}

ClusterMsgDataGossip::ClusterMsgDataGossip(std::vector<ClusterGossip>&& gossipMsg)
    : ClusterMsgData(ClusterMsgData::Type::Gossip), 
     _gossipMsg(std::move(gossipMsg)) {
}

bool ClusterMsgDataGossip::clusterNodeIsInGossipSection(const CNodePtr& node) const {
    for (auto& n : _gossipMsg) {
        if (n._gossipName == node->getNodeName())
            return true;
    }
    return false;
}
void ClusterMsgDataGossip::addGossipEntry(const CNodePtr& node) {
    ClusterGossip gossip(node->getNodeName(), node->_pingSent / 1000,
        node->_pongReceived / 1000, node->getNodeIp(), node->getPort(),
        node->getCport(), node->getFlags());

    _gossipMsg.emplace_back(std::move(gossip));
}

std::string ClusterMsgDataGossip::dataEncode() const {
    const size_t gossipSize = ClusterGossip::getGossipSize();
    std::vector<uint8_t> key;

    uint16_t keySize = gossipSize*(_gossipMsg.size());
    key.reserve(keySize);

    for (auto& ax : _gossipMsg) {
        std::string content = ax.gossipEncode();
        key.insert(key.end(), content.begin(), content.end());
    }
    return std::string(reinterpret_cast<const char *>(
                key.data()), key.size());
}

Expected<ClusterMsgDataGossip> ClusterMsgDataGossip::dataDecode(const std::string& key, uint16_t count) {
    const size_t gossipSize = ClusterGossip::getGossipSize();
    if (key.size() != count*gossipSize)  {
        return {ErrorCodes::ERR_DECODE, "invalid gossip data keylen"};
    }
    std::vector<ClusterGossip> gossipMsg;
    std::vector<string> res;

    auto it = key.begin();
    for (; it < key.end(); it+=gossipSize) {
        std::string temp(it, it+gossipSize);
        res.push_back(temp);
        auto gMsg = ClusterGossip::gossipDecode(temp);
        if (!gMsg.ok()) {
            return gMsg.status();
        }
        gossipMsg.emplace_back(std::move(gMsg.value()));
    }

    return ClusterMsgDataGossip(std::move(gossipMsg));
}

ClusterGossip::ClusterGossip(const std::shared_ptr<ClusterNode> node)
    :_gossipName(node->getNodeName()),
     _pingSent(node->_pingSent/1000),
     _pongReceived(node->_pongReceived/1000),
     _gossipIp(node->getNodeIp()),
     _gossipPort(node->getPort()),
     _gossipCport(node->getCport()),
     _gossipFlags(node->_flags) {
}

ClusterGossip::ClusterGossip(const std::string& gossipName,
                const uint32_t pingSent, const uint32_t pongReceived,
                const std::string& gossipIp, const uint16_t gossipPort,
                const uint16_t gossipCport, uint16_t gossipFlags)
    :_gossipName(gossipName),
     _pingSent(pingSent),
     _pongReceived(pongReceived),
     _gossipIp(gossipIp),
     _gossipPort(gossipPort),
     _gossipCport(gossipCport),
     _gossipFlags(gossipFlags) {
}

size_t ClusterGossip::getGossipSize()  {
    size_t strLen = CLUSTER_NAME_LENGTH + CLUSTER_IP_LENGTH;
    size_t intLen = sizeof(_pingSent) + sizeof(_pongReceived)
            + sizeof(_gossipPort) + sizeof(_gossipCport) + sizeof(_gossipFlags);
    return strLen + intLen;
}


std::string ClusterGossip::gossipEncode() const {
    std::vector<uint8_t> key;
    key.reserve(ClusterGossip::getGossipSize());

    //  _gossipNodeName
    key.insert(key.end(), _gossipName.begin(), _gossipName.end());

    //  _pingSent  uint64_t
    CopyUint(&key, _pingSent);
    CopyUint(&key, _pongReceived);

    key.insert(key.end(), _gossipIp.begin(), _gossipIp.end());

    uint8_t ipLen = CLUSTER_IP_LENGTH - _gossipIp.size();
    std::vector<uint8_t> zeroVec(ipLen, '\0');
    key.insert(key.end(), zeroVec.begin(), zeroVec.end());

    CopyUint(&key, _gossipPort);
    CopyUint(&key, _gossipCport);
    CopyUint(&key, _gossipFlags);

    return std::string(reinterpret_cast<const char *>(
                key.data()), key.size());
}

Expected<ClusterGossip> ClusterGossip::gossipDecode(const std::string& key) {
    const size_t gossipSize = ClusterGossip::getGossipSize();
    if (key.size() != gossipSize) {
        return {ErrorCodes::ERR_DECODE, "invalid gossip keylen"};
    }
    size_t offset = 0;
    auto decode = [&] (auto func) { auto n = func(key.c_str()+offset); offset+=sizeof(n); return n;};

    std::string gossipName(key.c_str()+offset, CLUSTER_NAME_LENGTH);
    offset += CLUSTER_NAME_LENGTH;

    auto pingSent = decode(int32Decode);
    auto pongReceived = decode(int32Decode);

    std::string gossipIp(key.c_str()+offset, CLUSTER_NAME_LENGTH);
    auto  pos = gossipIp.find('\0');
    if (pos > 0) {
        gossipIp.erase(pos, CLUSTER_IP_LENGTH);
    }

    offset +=  CLUSTER_IP_LENGTH;
    auto gossipPort = decode(int16Decode);
    auto gossipCport = decode(int16Decode);
    auto gossipFlags = decode(int16Decode);

    return ClusterGossip(gossipName, pingSent, pongReceived, gossipIp,
            gossipPort, gossipCport, gossipFlags);
}

ClusterManager::ClusterManager(const std::shared_ptr<ServerEntry>& svr,
                const std::shared_ptr<ClusterNode>& node,
                const std::shared_ptr<ClusterState>& state)
    :_svr(svr),
     _isRunning(false),
     _clusterNode(node),
     _clusterState(state),
     _clusterNetwork(nullptr),
     _netMatrix(std::make_shared<NetworkMatrix>()),
     _reqMatrix(std::make_shared<RequestMatrix>()) {
}

ClusterManager::ClusterManager(const std::shared_ptr<ServerEntry>& svr)
    :_svr(svr),
     _isRunning(false),
     _clusterNode(nullptr),
     _clusterState(nullptr),
     _clusterNetwork(nullptr),
     _netMatrix(std::make_shared<NetworkMatrix>()),
     _reqMatrix(std::make_shared<RequestMatrix>()) {
}

void ClusterManager::installClusterState(std::shared_ptr<ClusterState> o) {
    _clusterState = std::move(o);
}

void ClusterManager::installClusterNode(std::shared_ptr<ClusterNode> o) {
    _clusterNode = std::move(o);
}


std::shared_ptr<ClusterState> ClusterManager::getClusterState() const {
    return _clusterState;
}

NetworkAsio* ClusterManager::getClusterNetwork() const {
    return _clusterNetwork.get();
}


Status ClusterManager::initNetWork() {
    shared_ptr<ServerParams> cfg = _svr->getParams();
    _clusterNetwork = std::make_unique<NetworkAsio>(_svr, _netMatrix,
                                                _reqMatrix, cfg);

    // TODO(vinchen): cfg->netIoThreadNum
    Status s = _clusterNetwork->prepare(cfg->bindIp, cfg->port+CLUSTER_PORT_INCR, 1);
    if (!s.ok()) {
        return s;
    }
    // listener
    s = _clusterNetwork->run(true);
    if (!s.ok()) {
        return s;
    } else {
        LOG(INFO) << "cluster network ready to accept connections at "
            << cfg->bindIp << ":" << cfg->port;
    }
    return {ErrorCodes::ERR_OK, "init network ok"};
}

Status ClusterManager::initMetaData() {
    Catalog *catalog = _svr->getCatalog();
    INVARIANT(catalog != nullptr);

    // TODO(vinchen): cluster_announce_port/cluster_announce_bus_port
    auto params = _svr->getParams();
    std::string nodeIp = params->bindIp;
    uint16_t nodePort = params->port;
    uint16_t nodeCport = nodePort + CLUSTER_PORT_INCR;

    std::shared_ptr<ClusterState> gState = std::make_shared<tendisplus::ClusterState>(_svr);
    installClusterState(gState);

    auto vs = catalog->getAllClusterMeta();
    if (!vs.ok()) {
        return vs.status();
    } else if (vs.value().size() > 0) {
        int vssize = vs.value().size();
        INVARIANT(vssize > 0);
        LOG(INFO) << "catalog nodeName is" <<vs.value()[0]->nodeName;

        // create node
        for (auto& nodeMeta : vs.value()) {
            auto node = _clusterState->clusterLookupNode(nodeMeta->nodeName);
            if (!node) {
                node = std::make_shared<ClusterNode>(
                    nodeMeta->nodeName, nodeMeta->nodeFlag,
                    _clusterState,
                    nodeMeta->ip, nodeMeta->port, nodeMeta->cport,
                    nodeMeta->pingTime, nodeMeta->pongTime,
                    nodeMeta->configEpoch, nodeMeta->slots);
            } else {
                INVARIANT_D(0);
                LOG(WARNING) << "more than one node exists" << nodeMeta->nodeName;
            }

            if (node->nodeIsMyself()) {
                LOG(INFO) << "Node configuration loaded, I'm " << node->getNodeName();
                INVARIANT_D(!_clusterState->getMyselfNode());

                node->setNodeIp(nodeIp);
                node->setNodePort(nodePort);
                node->setNodeCport(nodeCport);

                _clusterState->setMyselfNode(node);
                // TODO(vinchen): what for?
                installClusterNode(node);
            }
        }

        if (!_clusterState->getMyselfNode()) {
            LOG(FATAL) << "Myself node for cluster is missing, please check it!";
            return{ ErrorCodes::ERR_INTERNAL, "" };
        }

        // master-slave info
        for (auto& nodeMeta : vs.value()) {
            if (nodeMeta->masterName != "-") {
                // TODO(vinchen): master maybe null?
                auto master = _clusterState->clusterLookupNode(nodeMeta->masterName);
                auto node = _clusterState->clusterLookupNode(nodeMeta->nodeName);
                INVARIANT(master != nullptr && node != nullptr);

                node->setMaster(master);
                master->addSlave(node);
            }
        }

        /* Something that should never happen: currentEpoch smaller than
        * the max epoch found in the nodes configuration. However we handle this
        * as some form of protection against manual editing of critical files. */
        _clusterState->clusterGetOrUpdateMaxEpoch(true);
        return {ErrorCodes::ERR_OK, "init cluster from catalog "};

     } else {
            const uint8_t flagName = CLUSTER_NODE_MYSELF|CLUSTER_NODE_MASTER;
            LOG(INFO) << "start init clusterNode with flag:" << flagName;
            std::shared_ptr<ClusterNode>  gNode = std::make_shared<ClusterNode>(
                getUUid(20), flagName, _clusterState, nodeIp, nodePort, nodeCport);
            installClusterNode(gNode);
            _clusterState->clusterAddNode(gNode);
            _clusterState->setMyselfNode(gNode);
            auto nodename = _clusterNode->getNodeName();
            LOG(INFO) << "No cluster configuration found, I'm " << nodename;
            // store clusterMeta
            auto pVs = std::make_unique<ClusterMeta>(nodename);

            Status s = catalog->setClusterMeta(*pVs);
            if (!s.ok()) {
                LOG(FATAL) << "catalog setClusterMeta error:"<< s.toString();
                return s;
            } else {
                LOG(INFO) << "cluster metadata set finish "<< "store ClusterMeta Node name is" << pVs->nodeName << "ip address is " << pVs->ip << "node Flag is" << pVs->nodeFlag;
            }
    }

    return  {ErrorCodes::ERR_OK, "init metadata ok"};
}

Status ClusterManager::startup() {
    std::lock_guard<std::mutex> lk(_mutex);
    Status s_meta = initMetaData();
    Status s_Net = initNetWork();

    if (!s_meta.ok() || !s_Net.ok()) {
        if (!s_meta.ok()) {
            LOG(INFO) << "init metadata fail"<< s_meta.toString();
            return s_meta;
        } else {
            LOG(INFO) << "init network fail"<< s_Net.toString();
            return s_Net;
        }
    } else {
        auto name = _clusterNode->getNodeName();
        auto state = _clusterState->getClusterState();
        std::string clusterState = (unsigned(state) > 0) ? "OK": "FAIL";
        LOG(INFO) << "cluster init success:"
            << " myself node name " << name << "cluster state is" << clusterState;
    }

    std::shared_ptr<ServerParams> params = _svr->getParams();
    /* Port sanity check II
    * The other handshake port check is triggered too late to stop
    * us from trying to use a too-high cluster port number. */
    if (params->port > (65535 - CLUSTER_PORT_INCR)) {
        LOG(ERROR) << "Redis port number too high. "
            "Cluster communication port is 10,000 port "
            "numbers higher than your Redis port. "
            "Your Redis port number must be "
            "lower than 55535.";
        exit(1);
    }

    _isRunning.store(true, std::memory_order_relaxed);
    _controller = std::make_unique<std::thread>(std::move([this]() {
        controlRoutine();
    }));

    return {ErrorCodes::ERR_OK, "init cluster finish"};
}

void ClusterManager::controlRoutine() {
    uint64_t iteration = 0;
    while (_isRunning.load(std::memory_order_relaxed)) {
        auto now = msSinceEpoch();
        uint64_t handshake_timeout = 0;

        /* Number of times this function was called so far. */
        iteration++;

        /* The handshake timeout is the time after which a handshake node that was
        * not turned into a normal node is removed from the nodes. Usually it is
        * just the NODE_TIMEOUT value, but when NODE_TIMEOUT is too small we use
        * the value of 1 second. */
        handshake_timeout = _svr->getParams()->clusterNodeTimeout;
        if (handshake_timeout < 1000)
            handshake_timeout = 1000;

        /* Update myself flags. */
        // clusterUpdateMyselfFlags();

        auto nodelist = _clusterState->getNodesList();
        _clusterState->_statsPfailNodes = 0;
        // TODO(wayenchen) : do something like clusterCron()
        // check whether _clusterState->getNodesList() is thread safe.
        // like clusterDelNode() while iterator
        for (auto& nodePair : nodelist) {
            auto node = nodePair.second;

            /* Not interested in reconnecting the link with myself or nodes
            * for which we have no address. */
            if (node->_flags & (CLUSTER_NODE_MYSELF | CLUSTER_NODE_NOADDR)) continue;

            if (node->nodeTimedOut()) {
                _clusterState->_statsPfailNodes++;
            }

            /* A Node in HANDSHAKE state has a limited lifespan equal to the
            * configured node timeout. */
            if (node->nodeInHandshake() && now - node->_ctime > handshake_timeout) {
                _clusterState->clusterDelNode(node);
                continue;
            }

            if (!node->getSession().get()) {
                auto esess = clusterCreateSession(node);
                if (!esess.status().ok()) {
                    /* We got a synchronous error from connect before
                    * clusterSendPing() had a chance to be called.
                    * If node->ping_sent is zero, failure detection can't work,
                    * so we claim we actually sent a ping now (that will
                    * be really sent as soon as the link is obtained). */
                    if (node->_pingSent == 0) node->_pingSent = msSinceEpoch();

                    LOG(WARNING) << "Unable to connect to Cluster Node:"
                        << node->getNodeIp()
                        << ", Port: " << node->getCport()
                        << ", Error:" << esess.status().toString();
                    continue;
                }

                auto sess = esess.value();
                node->setSession(sess);

                /* Queue a PING in the new connection ASAP: this is crucial
                * to avoid false positives in failure detection.
                *
                * If the node is flagged as MEET, we send a MEET message instead
                * of a PING one, to force the receiver to add us in its node
                * table. */
                auto old_ping_sent = node->_pingSent;
                sess->clusterSendPing(node->_flags & CLUSTER_NODE_MEET ?
                    ClusterMsg::Type::MEET : ClusterMsg::Type::PING);
                if (old_ping_sent) {
                    /* If there was an active ping before the link was
                    * disconnected, we want to restore the ping time, otherwise
                    * replaced by the clusterSendPing() call. */
                    node->_pingSent = old_ping_sent;
                }
                /* We can clear the flag after the first packet is sent.
                * If we'll never receive a PONG, we'll never send new packets
                * to this node. Instead after the PONG is received and we
                * are no longer in meet/handshake status, we want to send
                * normal PING packets. */
                node->_flags &= ~CLUSTER_NODE_MEET;

                LOG(INFO) << "Connecting with Node " << node->getNodeName() << "at " <<
                    node->getNodeIp() << ":" << node->getCport();
            }
        }
        // done
        std::this_thread::sleep_for(100ms);
    }

    LOG(INFO) << "cluster controller exits";
}


Expected<std::shared_ptr<ClusterSession>> ClusterManager::clusterCreateSession(const std::shared_ptr<ClusterNode>& node) {
    std::shared_ptr<BlockingTcpClient> client =
        std::move(_clusterNetwork->createBlockingClient(64 * 1024 * 1024));
    Status s = client->connect(node->getNodeIp(), node->getCport(), std::chrono::seconds(3));
    if (!s.ok()) {
        return s;
    }

    auto sess = _clusterNetwork->client2ClusterSession(std::move(client));
    if (!sess.ok()) {
        LOG(WARNING) << "client2ClusterSession failed: ";
        return{ ErrorCodes::ERR_NETWORK, "clent2ClusterSession failed" };
    }
    INVARIANT_D(sess.value()->getType() == Session::Type::CLUSTER);
    sess.value()->setNode(node);

    return sess.value();
}

ClusterSession::ClusterSession(std::shared_ptr<ServerEntry> server,
    asio::ip::tcp::socket sock,
    uint64_t connid,
    bool initSock,
    std::shared_ptr<NetworkMatrix> netMatrix,
    std::shared_ptr<RequestMatrix> reqMatrix)
    : NetSession(server, std::move(sock), connid, initSock,
                    netMatrix, reqMatrix, Session::Type::CLUSTER),
      _pkgSize(-1) {
    DLOG(INFO) << "cluster session, id:" << id() << " created";
}

// copy from clusterReadHandler
void ClusterSession::drainReqNet() {
    unsigned int readlen, rcvbuflen;

    rcvbuflen = _queryBufPos;
    if (rcvbuflen < 8) {
        /* First, obtain the first 8 bytes to get the full message
        * length. */
        readlen = 8 - rcvbuflen;
    } else {
        /* Finally read the full message. */
        if (rcvbuflen == 8) {
            // TODO(wayenchen)
            _pkgSize = int32Decode(_queryBuf.data() + 4);

            /* Perform some sanity check on the message signature
            * and length. */
            if (memcmp(_queryBuf.data(), "RCmb", 4) != 0 ||
                _pkgSize < CLUSTERMSG_MIN_LEN) {
                LOG(WARNING)
                    << "Bad message length or signature received "
                    << "from Cluster bus.";
                endSession();
                return;
            }
        }
        readlen = _pkgSize - rcvbuflen;
    }
    // here we use >= than >, so the last element will always be 0,
    // it's convinent for c-style string search
    if (readlen + (size_t)_queryBufPos >= _queryBuf.size()) {
        // the fill should be as fast as memset in 02 mode, refer to here
        // NOLINT(whitespace/line_length) https://stackoverflow.com/questions/8848575/fastest-way-to-reset-every-value-of-stdvectorint-to-0)
        _queryBuf.resize((readlen + _queryBufPos) * 2, 0);
    }

    auto self(shared_from_this());
    uint64_t curr = nsSinceEpoch();
    _sock.
        async_read_some(asio::buffer(_queryBuf.data() + _queryBufPos, readlen),
            [this, self, curr](const std::error_code& ec, size_t actualLen) {
        drainReqCallback(ec, actualLen);
    });
}


void ClusterSession::drainReqCallback(const std::error_code& ec, size_t actualLen) {
    if (ec) {
        /* I/O error... */
        LOG(WARNING) << "I/O error reading from node link: " << ec.message();
        return;
    }

    INVARIANT_D(_pkgSize != (size_t)-1 || (size_t)_queryBufPos + actualLen <= 8);

#ifdef TENDIS_DEBUG
    State curr = _state.load(std::memory_order_relaxed);
    INVARIANT(curr == State::DrainReqNet);
#endif

    _queryBufPos += actualLen;
    _queryBuf[_queryBufPos] = 0;

    /* Total length obtained? Process this packet. */
    if (_queryBufPos >= 8 && (size_t)_queryBufPos == _pkgSize) {
        setState(State::Process);
        schedule();
    } else {
        setState(State::DrainReqNet);
        schedule();
    }
}

void ClusterSession::processReq() {
    _ctx->setProcessPacketStart(nsSinceEpoch());
    // TODO(wayenchen)
    auto status = clusterProcessPacket();
    _reqMatrix->processed += 1;
    _reqMatrix->processCost += nsSinceEpoch() - _ctx->getProcessPacketStart();
    _ctx->setProcessPacketStart(0);

    _queryBufPos = 0;
    if (!status.ok()) {
        endSession();
    } else {
        setState(State::DrainReqNet);
        schedule();
    }
}

Status ClusterSession::clusterProcessGossipSection(const ClusterMsg& msg) {
    auto hdr = msg.getHeader();
    auto count = hdr->_count;
    auto data = msg.getData();
    auto cstate = _server->getClusterMgr()->getClusterState();
    auto myself = cstate->getMyselfNode();
    auto mstime = msSinceEpoch();
    INVARIANT_D(data->getType() == ClusterMsgData::Type::Gossip);
    if (data->getType() != ClusterMsgData::Type::Gossip) {
        return{ ErrorCodes::ERR_CLUSTER, 
            "invalid gossip message: type is " + std::to_string(uint32_t(data->getType())) };
    }

    std::shared_ptr<ClusterMsgDataGossip> gossip =
                std::dynamic_pointer_cast<ClusterMsgDataGossip>(data);
    auto sender = _node ? _node : cstate->clusterLookupNode(hdr->_sender);

    if (count != gossip->getGossipList().size()) {
        // invalid message
        return{ ErrorCodes::ERR_CLUSTER, "invalid gossip message size" };
    }

    for (const auto& g: gossip->getGossipList()) {
        auto flags = g._gossipFlags;


        /*       if (server.verbosity == LL_DEBUG) {
                   ci = representClusterNodeFlags(sdsempty(), flags);
                   serverLog(LL_DEBUG, "GOSSIP %.40s %s:%d@%d %s",
                       g->nodename,
                       g->ip,
                       ntohs(g->port),
                       ntohs(g->cport),
                       ci);
                   sdsfree(ci);
               }*/

        /* Update our state accordingly to the gossip sections */
        auto node = cstate->clusterLookupNode(g._gossipName);
        if (node) {
            /* We already know this node.
            Handle failure reports, only when the sender is a master. */
            if (sender && sender->nodeIsMaster() && node != myself) {
                if (flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)) {
                    if (node->addFailureReport(sender)) {
                        redis_port::serverLog(LL_VERBOSE,
                            "Node %.40s reported node %.40s as not reachable.",
                            sender->getNodeName().c_str(), node->getNodeName().c_str());
                    }

                    node->markAsFailingIfNeeded();
                } else {
                    if (node->delFailureReport(sender)) {
                        redis_port::serverLog(LL_VERBOSE,
                            "Node %.40s reported node %.40s is back online.",
                            sender->getNodeName().c_str(), node->getNodeName().c_str());
                    }
                }
            }

            /* If from our POV the node is up (no failure flags are set),
            * we have no pending ping for the node, nor we have failure
            * reports for this node, update the last pong time with the
            * one we see from the other nodes. */
            if (!(flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)) &&
                node->_pingSent == 0 &&
                node->failureReportsCount() == 0) {
                mstime_t pongtime = g._pongReceived;
                pongtime *= 1000; /* Convert back to milliseconds. */

                /* Replace the pong time with the received one only if
                * it's greater than our view but is not in the future
                * (with 500 milliseconds tolerance) from the POV of our
                * clock. */
                if (pongtime <= (mstime + 500) &&
                    pongtime > node->_pongReceived) {
                    node->_pongReceived = pongtime;
                }
            }

            /* If we already know this node, but it is not reachable, and
            * we see a different address in the gossip section of a node that
            * can talk with this other node, update the address, disconnect
            * the old link if any, so that we'll attempt to connect with the
            * new address. */
            if (node->_flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL) &&
                !(flags & CLUSTER_NODE_NOADDR) &&
                !(flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)) &&
                (node->getNodeIp() != g._gossipIp ||
                    node->getPort() != g._gossipCport ||
                    node->getCport() != g._gossipCport)) {
                if (node->getSession()) {
                    // TODO(vinchen): how to close the session safe
                    //node->getSession()->setCloseAfterRsp();
                    // if (node->link) freeClusterLink(node->link);
                }
                node->setNodeIp(g._gossipIp);
                node->setNodePort(g._gossipCport);
                node->setNodeCport(g._gossipCport);
                node->_flags &= ~CLUSTER_NODE_NOADDR;
            }
        } else {
            /* If it's not in NOADDR state and we don't have it, we
            * start a handshake process against this IP/PORT pairs.
            *
            * Note that we require that the sender of this gossip message
            * is a well known node in our cluster, otherwise we risk
            * joining another cluster. */
            if (sender &&
                !(flags & CLUSTER_NODE_NOADDR) &&
                !cstate->clusterBlacklistExists(g._gossipName)) {

                cstate->clusterStartHandshake(g._gossipIp,
                    g._gossipPort, g._gossipCport);
            }
        }
    }

    return{ ErrorCodes::ERR_OK, "" };
}

Status ClusterSession::clusterProcessPacket() {
    INVARIANT_D(_queryBuf.size() >= _pkgSize);
    auto emsg = ClusterMsg::msgDecode(std::string(_queryBuf.data(), _pkgSize));
    if (!emsg.ok()) {
        return emsg.status();
    }
    auto msg = emsg.value();
    auto hdr = msg.getHeader();

    uint32_t totlen = hdr->_totlen;
    auto type = hdr->_type;

    redis_port::serverLog(LL_VERBOSE, 
        "--- Processing packet of type %d, %lu bytes",
        (uint16_t)type, (uint32_t)totlen);

    if (totlen < 16 || totlen > _pkgSize) {
        return{ ErrorCodes::ERR_DECODE, "invalid message len" };
    }

    uint16_t flags = hdr->_flags;
    uint64_t senderCurrentEpoch = 0, senderConfigEpoch = 0;
    auto cstate = _server->getClusterMgr()->getClusterState();
    auto myself = cstate->getMyselfNode();

    /* Check if the sender is a known node. */
    auto sender = cstate->clusterLookupNode(hdr->_sender);
    if (sender && !sender->nodeInHandshake()) {
        /* Update our curretEpoch if we see a newer epoch in the cluster. */
        senderCurrentEpoch = hdr->_currentEpoch;
        senderConfigEpoch = hdr->_configEpoch;
        if (senderCurrentEpoch > cstate->getCurrentEpoch())
            cstate->setCurrentEpoch(senderCurrentEpoch);
        /* Update the sender configEpoch if it is publishing a newer one. */
        if (senderConfigEpoch > sender->getConfigEpoch()) {
            sender->setConfigEpoch(senderConfigEpoch);
            // TODO(vinchen): save
            cstate->clusterSaveNodes();
            //clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG |
            //    CLUSTER_TODO_FSYNC_CONFIG);
        }
        /* Update the replication offset info for this node. */
        sender->_replOffset = hdr->_offset;
        sender->_replOffsetTime = msSinceEpoch();
        // FIXME: 
        /* If we are a slave performing a manual failover and our master
        * sent its offset while already paused, populate the MF state. */
        if (hdr->_mflags[0] & CLUSTERMSG_FLAG0_PAUSED &&
            cstate->setMfMasterOffsetIfNecessary(sender)) {
            redis_port::serverLog(LL_WARNING,
                "Received replication offset for paused "
                "master manual failover: %lld",
                sender->_replOffset);
        }
    }

    if (type == ClusterMsg::Type::PING || 
        type == ClusterMsg::Type::MEET) {
        DLOG(INFO) << "Ping/MEET package received: "
            << hdr->_sender;

        /* We use incoming MEET messages in order to set the address
        * for 'myself', since only other cluster nodes will send us
        * MEET messages on handshakes, when the cluster joins, or
        * later if we changed address, and those nodes will use our
        * official address to connect to us. So by obtaining this address
        * from the socket is a simple way to discover / update our own
        * address in the cluster without it being hardcoded in the config.
        *
        * However if we don't have an address at all, we update the address
        * even with a normal PING packet. If it's wrong it will be fixed
        * by MEET later. */
        // TODO(wayenchen)
        if ((type == ClusterMsg::Type::MEET || myself->getNodeIp() == "")/* &&
            server.cluster_announce_ip == NULL*/) {
            auto eip = getLocalIp();
            if (eip.ok() && eip.value() != myself->getNodeIp()) {
                redis_port::serverLog(LL_WARNING, "IP address for this node updated to %s",
                        eip.value().c_str());
                myself->setNodeIp(eip.value());
                cstate->clusterSaveNodes();
            }
        }

        /* Add this node if it is new for us and the msg type is MEET.
        * In this stage we don't try to add the node with the right
        * flags, slaveof pointer, and so forth, as this details will be
        * resolved when we'll receive PONGs from the node. */
        if (!sender && type == ClusterMsg::Type::MEET) {
            bool save = true;
            auto ip = nodeIp2String(hdr->_myIp);
            auto node = std::make_shared<ClusterNode>(getUUid(20), CLUSTER_NODE_HANDSHAKE,
                cstate, ip, hdr->_port, hdr->_cport);

            cstate->clusterAddNode(node, save);
            // clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
        }

        /* If this is a MEET packet from an unknown node, we still process
        * the gossip section here since we have to trust the sender because
        * of the message type. */
        if (!sender && type == ClusterMsg::Type::MEET) {
            clusterProcessGossipSection(msg);
        }

        /* Anyway reply with a PONG */
        clusterSendPing(ClusterMsg::Type::PONG);
    }

    if (type == ClusterMsg::Type::PING || type == ClusterMsg::Type::PONG ||
        type == ClusterMsg::Type::MEET) {
        redis_port::serverLog(LL_DEBUG, "%s packet received: %s",
            type == ClusterMsg::Type::PING ? "ping" : "pong",
            hdr->_sender.c_str());

        if (_node) {
            if (_node->nodeInHandshake()) {
                /* If we already have this node, try to change the
                * IP/port of the node with the new one. */
                if (sender) {
                    redis_port::serverLog(LL_VERBOSE,
                        "Handshake: we already know node %.40s, "
                        "updating the address if needed.", sender->getNodeName());
                    if (sender->updateAddressIfNeeded(this, msg)) {
                    //    clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG |
                    //        CLUSTER_TODO_UPDATE_STATE);
                    }
                    ///* Free this node as we already have it. This will
                    //* cause the link to be freed as well. */
                    cstate->clusterDelNode(_node);
                    return { ErrorCodes::ERR_OK, "" };
                }

                /* First thing to do is replacing the random name with the
                * right node name if this was a handshake stage. */
                redis_port::serverLog(LL_DEBUG, "Handshake with node %.40s completed.",
                    _node->getNodeName());
                _node->_flags &= ~CLUSTER_NODE_HANDSHAKE;
                _node->_flags |= flags & (CLUSTER_NODE_MASTER | CLUSTER_NODE_SLAVE);
                cstate->clusterRenameNode(_node, hdr->_sender, true);
                // save config
                //clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
            } else if (_node->getNodeName() == hdr->_sender) {
                /* If the reply has a non matching node ID we
                * disconnect this node and set it as not having an associated
                * address. */
                redis_port::serverLog(LL_DEBUG, "PONG contains mismatching sender ID. "
                    "About node %.40s added %d ms ago, having flags %d",
                    _node->getNodeName().c_str(),
                    (uint32_t)(msSinceEpoch() - _node->_ctime),
                    _node->_flags);
                _node->_flags |= CLUSTER_NODE_NOADDR;
                _node->setNodeIp("");
                _node->setNodePort(0);
                _node->setNodeCport(0);
                _node->setSession(nullptr);
                // TODO(vinchen)
                setCloseAfterRsp();
                //_node->se
                //freeClusterLink(link);
                //clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
                cstate->clusterSaveNodes();
                return { ErrorCodes::ERR_OK, "" };
            }
        }

        /* Copy the CLUSTER_NODE_NOFAILOVER flag from what the sender
        * announced. This is a dynamic flag that we receive from the
        * sender, and the latest status must be trusted. We need it to
        * be propagated because the slave ranking used to understand the
        * delay of each slave in the voting process, needs to know
        * what are the instances really competing. */
        if (sender) {
            int nofailover = flags & CLUSTER_NODE_NOFAILOVER;
            sender->_flags &= ~CLUSTER_NODE_NOFAILOVER;
            sender->_flags |= nofailover;
        }

        /* Update the node address if it changed. */
        if (sender && type == ClusterMsg::Type::PING &&
            sender->nodeInHandshake() &&
            sender->updateAddressIfNeeded(this, msg)) {
            //clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG |
            //    CLUSTER_TODO_UPDATE_STATE);
        }

        /* Update our info about the node */
        if (_node && type == ClusterMsg::Type::PONG) {
            _node->_pongReceived = msSinceEpoch();
            _node->_pingSent = 0;

            /* The PFAIL condition can be reversed without external
            * help if it is momentary (that is, if it does not
            * turn into a FAIL state).
            *
            * The FAIL condition is also reversible under specific
            * conditions detected by clearNodeFailureIfNeeded(). */
            if (_node->nodeTimedOut()) {
                _node->_flags &= ~CLUSTER_NODE_PFAIL;
                //clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG |
                //    CLUSTER_TODO_UPDATE_STATE);
            } else if (_node->nodeFailed()) {
                //clearNodeFailureIfNeeded(link->node);
            }
        }

        /* Check for role switch: slave -> master or master -> slave. */
        if (sender) {
            if (msg.isMaster()) {
                /* Node is a master. */
                sender->setAsMaster();
            } else {
                /* Node is a slave. */
                auto master = cstate->clusterLookupNode(hdr->_slaveOf);

                /* Master turned into a slave! Reconfigure the node. */
                sender->changeToSlaveIfNeeded();
                //if (sender->nodeIsMaster()) {

                    //clusterDelNodeSlots(sender);
                    ///* Update config and state. */
                    //clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG |
                    //    CLUSTER_TODO_UPDATE_STATE);
                //}

                /* Master node changed for this slave? */
                if (master && sender->getMaster() != master) {
                    auto orgMaster = sender->getMaster();
                    if (orgMaster)
                        orgMaster->removeSlave(sender);

                    master->addSlave(sender);
                    sender->setMaster(master);

                    /* Update config. */
                    //clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
                }
            }
        }

        /* Update our info about served slots.
        *
        * Note: this MUST happen after we update the master/slave state
        * so that CLUSTER_NODE_MASTER flag will be set. */

        /* Many checks are only needed if the set of served slots this
        * instance claims is different compared to the set of slots we have
        * for it. Check this ASAP to avoid other computational expansive
        * checks later. */
        CNodePtr sender_master = nullptr; /* Sender or its master if slave. */
        bool dirty_slots = false; /* Sender claimed slots don't match my view? */

        if (sender) {
            sender_master = sender->nodeIsMaster() ? sender : sender->getMaster();
            if (sender_master) {
                if (sender_master->_mySlots != hdr->_slots) {
                    dirty_slots = true;
                }
            }
        }

        /* 1) If the sender of the message is a master, and we detected that
        *    the set of slots it claims changed, scan the slots to see if we
        *    need to update our configuration. */
        if (sender && sender->nodeIsMaster() && dirty_slots) {
            cstate->clusterUpdateSlotsConfigWith(sender, senderConfigEpoch, hdr->_slots);
        }

        /* 2) We also check for the reverse condition, that is, the sender
        *    claims to serve slots we know are served by a master with a
        *    greater configEpoch. If this happens we inform the sender.
        *
        * This is useful because sometimes after a partition heals, a
        * reappearing master may be the last one to claim a given set of
        * hash slots, but with a configuration that other instances know to
        * be deprecated. Example:
        *
        * A and B are master and slave for slots 1,2,3.
        * A is partitioned away, B gets promoted.
        * B is partitioned away, and A returns available.
        *
        * Usually B would PING A publishing its set of served slots and its
        * configEpoch, but because of the partition B can't inform A of the
        * new configuration, so other nodes that have an updated table must
        * do it. In this way A will stop to act as a master (or can try to
        * failover if there are the conditions to win the election). */
        if (sender && dirty_slots) {
            int j;

            for (j = 0; j < CLUSTER_SLOTS; j++) {
                if (hdr->_slots.test(j)) {
                    auto nodej = cstate->getNodeBySlot(j);
                    if (nodej == sender ||
                        // TODO(vinchen) : why? Because all nodej update need UPDATE message
                        nodej == nullptr) {
                        continue;
                    }

                    if (nodej->getConfigEpoch() > senderConfigEpoch) {
                        redis_port::serverLog(LL_VERBOSE,
                            "Node %.40s has old slots configuration, sending "
                            "an UPDATE message about %.40s",
                            sender->getNodeName(), nodej->getNodeName());
                        clusterSendUpdate(nodej);

                        /* TODO(wayenchen): instead of exiting the loop send every other
                        * UPDATE packet for other nodes that are the new owner
                        * of sender's slots. */
                        break;
                    }
                }
            }
        }

        /* If our config epoch collides with the sender's try to fix
        * the problem. */
        if (sender &&
            myself->nodeIsMaster() && sender->nodeIsMaster() &&
            senderConfigEpoch == myself->getConfigEpoch()) {
            cstate->clusterHandleConfigEpochCollision(sender);
        }

        /* Get info from the gossip section */
        if (sender)
            clusterProcessGossipSection(msg);
    } else {
        // TODO(wayenchen): other message
        INVARIANT_D(0);
    }
    return{ ErrorCodes::ERR_OK, "" };
}

Status ClusterSession::clusterReadHandler() {
    drainReqNet();
    return{ ErrorCodes::ERR_OK, "" };
}

Status ClusterSession::clusterSendMessage(const ClusterMsg& msg) {
    setResponse(msg.msgEncode());
    return{ ErrorCodes::ERR_OK, "" };
}

Status ClusterSession::clusterSendUpdate(CNodePtr node) {
    INVARIANT_D(0);
    return{ ErrorCodes::ERR_OK, "" };
}

void ClusterSession::setNode(const CNodePtr& node) {
    _node = node;
}

std::string ClusterSession::nodeIp2String(const std::string& announcedIp) const {
    if (announcedIp != "") {
        return announcedIp;
    } else {
        auto eip = getRemoteIp();
        if (!eip.ok()) {
            return "?";
        }
        return eip.value();
    }
}

/* Send a PING or PONG packet to the specified node, making sure to add enough
* gossip informations. */
Status ClusterSession::clusterSendPing(ClusterMsg::Type type) {
    auto clusterState = _server->getClusterMgr()->getClusterState();

    uint32_t gossipcount = 0; /* Number of gossip sections added so far. */
    uint32_t wanted; /* Number of gossip sections we want to append if possible. */
                /* freshnodes is the max number of nodes we can hope to append at all:
                * nodes available minus two (ourself and the node we are sending the
                * message to). However practically there may be less valid nodes since
                * nodes in handshake state, disconnected, are not considered. */
    uint32_t nodeCount = clusterState->getNodeCount();
    uint32_t freshnodes = nodeCount - 2;

    /* How many gossip sections we want to add? 1/10 of the number of nodes
    * and anyway at least 3. Why 1/10?
    *
    * If we have N masters, with N/10 entries, and we consider that in
    * node_timeout we exchange with each other node at least 4 packets
    * (we ping in the worst case in node_timeout/2 time, and we also
    * receive two pings from the host), we have a total of 8 packets
    * in the node_timeout*2 falure reports validity time. So we have
    * that, for a single PFAIL node, we can expect to receive the following
    * number of failure reports (in the specified window of time):
    *
    * PROB * GOSSIP_ENTRIES_PER_PACKET * TOTAL_PACKETS:
    *
    * PROB = probability of being featured in a single gossip entry,
    *        which is 1 / NUM_OF_NODES.
    * ENTRIES = 10.
    * TOTAL_PACKETS = 2 * 4 * NUM_OF_MASTERS.
    *
    * If we assume we have just masters (so num of nodes and num of masters
    * is the same), with 1/10 we always get over the majority, and specifically
    * 80% of the number of nodes, to account for many masters failing at the
    * same time.
    *
    * Since we have non-voting slaves that lower the probability of an entry
    * to feature our node, we set the number of entires per packet as
    * 10% of the total nodes we have. */
    wanted = floor(nodeCount / 10);
    if (wanted < 3) wanted = 3;
    if (wanted > freshnodes) wanted = freshnodes;

    /* Include all the nodes in PFAIL state, so that failure reports are
    * faster to propagate to go from PFAIL to FAIL state. */
    uint32_t pfail_wanted = clusterState->_statsPfailNodes;

    /* Populate the header. */
    if (_node && type == ClusterMsg::Type::PING) {
        _node->_pingSent = msSinceEpoch();
    }
    //clusterBuildMessageHdr(hdr, type);
    ClusterMsg msg(type, clusterState, _server);

    /* Populate the gossip fields */
    uint32_t maxiterations = wanted * 3;
    while (freshnodes > 0 && gossipcount < wanted && maxiterations--) {
        auto node = clusterState->getRandomNode();

        /* Don't include this node: the whole packet header is about us
        * already, so we just gossip about other nodes. */
        if (node == clusterState->getMyselfNode()) continue;

        /* PFAIL nodes will be added later. */
        if (node->_flags & CLUSTER_NODE_PFAIL) continue;

        /* In the gossip section don't include:
        * 1) Nodes in HANDSHAKE state.
        * 3) Nodes with the NOADDR flag set.
        * 4) Disconnected nodes if they don't have configured slots.
        */
        if (node->_flags & (CLUSTER_NODE_HANDSHAKE | CLUSTER_NODE_NOADDR) ||
            (node->getSession() == nullptr && node->_numSlots == 0)) {
            freshnodes--; /* Tecnically not correct, but saves CPU. */
            continue;
        }

        /* Do not add a node we already have. */
        if (msg.clusterNodeIsInGossipSection(node))
            continue;

        /* Add it */
        msg.clusterAddGossipEntry(node);
        freshnodes--;
        gossipcount++;
    }

    // TODO(wayenchen):
    /* If there are PFAIL nodes, add them at the end. */
    if (pfail_wanted) {
        //dictIterator *di;
        //dictEntry *de;

        //di = dictGetSafeIterator(server.cluster->nodes);
        //while ((de = dictNext(di)) != NULL && pfail_wanted > 0) {
        //    clusterNode *node = dictGetVal(de);
        //    if (node->flags & CLUSTER_NODE_HANDSHAKE) continue;
        //    if (node->flags & CLUSTER_NODE_NOADDR) continue;
        //    if (!(node->flags & CLUSTER_NODE_PFAIL)) continue;
        //    clusterSetGossipEntry(hdr, gossipcount, node);
        //    freshnodes--;
        //    gossipcount++;
        //    /* We take the count of the slots we allocated, since the
        //    * PFAIL stats may not match perfectly with the current number
        //    * of PFAIL nodes. */
        //    pfail_wanted--;
        //}
        //dictReleaseIterator(di);
    }

    INVARIANT_D(gossipcount == msg.getEntryCount());

    return clusterSendMessage(msg);
}

}  // namespace tendisplus
