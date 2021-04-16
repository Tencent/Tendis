// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <math.h>
#include <bitset>
#include <set>
#include <sstream>
#include "tendisplus/cluster/cluster_manager.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/storage/varint.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/time.h"

namespace tendisplus {

inline ClusterHealth int2ClusterHealth(const uint8_t t) {
  if (t == 1) {
    return ClusterHealth::CLUSTER_OK;
  } else {
    return ClusterHealth::CLUSTER_FAIL;
  }
}

const std::string clusterMsgTypeString(ClusterMsg::Type c) {
  switch (c) {
    case ClusterMsg::Type::PING:
      return "ping";
    case ClusterMsg::Type::PONG:
      return "pong";
    case ClusterMsg::Type::MEET:
      return "meet";
    case ClusterMsg::Type::FAIL:
      return "fail";
    case ClusterMsg::Type::PUBLISH:
      return "publish";
    case ClusterMsg::Type::FAILOVER_AUTH_REQUEST:
      return "auth-req";
    case ClusterMsg::Type::FAILOVER_AUTH_ACK:
      return "auth-ack";
    case ClusterMsg::Type::UPDATE:
      return "update";
    case ClusterMsg::Type::MFSTART:
      return "mfstart";
  }
  return "unkown";
}

ClusterNode::ClusterNode(const std::string& nodeName,
                         const uint16_t flags,
                         std::shared_ptr<ClusterState> cstate,
                         const std::string& host,
                         uint32_t port,
                         uint32_t cport,
                         uint64_t pingSend,
                         uint64_t pongReceived,
                         uint64_t epoch)
  : _nodeName(nodeName),
    _configEpoch(epoch),
    _nodeIp(host),
    _nodePort(port),
    _nodeCport(cport),
    _nodeSession(nullptr),
    _nodeClient(nullptr),
    _numSlaves(0),
    _numSlots(0),
    _ctime(msSinceEpoch()),
    _flags(flags),
    _slaveOf(nullptr),
    _pingSent(0),
    _pongReceived(0),
    _failTime(0),
    _votedTime(0),
    _replOffsetTime(0),
    _orphanedTime(0),
    _replOffset(0) {}

bool ClusterNode::operator==(const ClusterNode& other) const {
  bool ret = _nodeName == other._nodeName &&
    _configEpoch == other._configEpoch && _nodeIp == other._nodeIp &&
    _nodePort == other._nodePort && _nodeCport == other._nodeCport &&
    (_flags & ~CLUSTER_NODE_MYSELF) == (other._flags & ~CLUSTER_NODE_MYSELF) &&
    _numSlots == other._numSlots && _mySlots == other._mySlots &&
    _replOffset == other._replOffset;

  if (!ret) {
    return ret;
  }

  if (_slaveOf || other._slaveOf) {
    if (!_slaveOf || !other._slaveOf) {
      return false;
    }
    if (!(*(_slaveOf.get()) == *(other._slaveOf.get()))) {
      return false;
    }
  }

  if (_slaves.size() != other._slaves.size()) {
    return false;
  }

  for (auto slave : _slaves) {
    bool exist = false;
    for (auto otherslave : other._slaves) {
      if (*slave.get() == *otherslave.get()) {
        exist = true;
      }
    }

    if (!exist) {
      return false;
    }
  }

  return true;
}

struct redisNodeFlags {
  uint16_t flag;
  const std::string name;
};

static struct redisNodeFlags redisNodeFlagsTable[] = {
  {CLUSTER_NODE_MYSELF, "myself"},
  {CLUSTER_NODE_MASTER, "master"},
  {CLUSTER_NODE_SLAVE, "slave"},
  {CLUSTER_NODE_PFAIL, "fail?"},
  {CLUSTER_NODE_FAIL, "fail"},
  {CLUSTER_NODE_HANDSHAKE, "handshake"},
  {CLUSTER_NODE_NOADDR, "noaddr"},
  {CLUSTER_NODE_NOFAILOVER, "nofailover"},
  {CLUSTER_NODE_ARBITER, "arbiter"}};

std::string representClusterNodeFlags(uint16_t flags) {
  std::stringstream ss;
  bool empty = true;
  uint16_t firstFlag = 0;
  auto size = sizeof(redisNodeFlagsTable) / sizeof(struct redisNodeFlags);
  for (uint32_t i = 0; i < size; i++) {
    struct redisNodeFlags* nodeflag = redisNodeFlagsTable + i;
    if (flags & nodeflag->flag) {
      if (firstFlag > 0) {
        ss << "," << nodeflag->name;
      } else {
        ss << nodeflag->name;
        firstFlag++;
      }
      empty = false;
    }
  }
  if (empty) {
    return "noflags";
  }

  return ss.str();
}


ConnectState ClusterNode::getConnectState() {
  auto state = (getSession() || (_flags & CLUSTER_NODE_MYSELF))
    ? ConnectState::CONNECTED
    : ConnectState::DISCONNECTED;
  return state;
}

std::string ClusterNode::getNodeName() const {
  std::lock_guard<myMutex> lk(_mutex);
  return _nodeName;
}

std::string ClusterNode::getNodeNameNolock() const {
  return _nodeName;
}


void ClusterNode::setNodeName(const std::string& name) {
  std::lock_guard<myMutex> lk(_mutex);
  _nodeName = name;
}

void ClusterNode::setNodeIp(const std::string& ip) {
  std::lock_guard<myMutex> lk(_mutex);
  _nodeIp = ip;
}

std::string ClusterNode::getNodeIp() const {
  std::lock_guard<myMutex> lk(_mutex);
  return _nodeIp;
}

void ClusterNode::setNodePort(uint64_t port) {
  std::lock_guard<myMutex> lk(_mutex);
  _nodePort = port;
}

uint64_t ClusterNode::getPort() const {
  std::lock_guard<myMutex> lk(_mutex);
  return _nodePort;
}

void ClusterNode::setNodeCport(uint64_t cport) {
  std::lock_guard<myMutex> lk(_mutex);
  _nodeCport = cport;
}

uint64_t ClusterNode::getConfigEpoch() const {
  std::lock_guard<myMutex> lk(_mutex);
  return _configEpoch;
}

void ClusterNode::setConfigEpoch(uint64_t epoch) {
  std::lock_guard<myMutex> lk(_mutex);
  _configEpoch = epoch;
}

uint64_t ClusterNode::getVoteTime() const {
  std::lock_guard<myMutex> lk(_mutex);
  return _votedTime;
}

void ClusterNode::setVoteTime(uint64_t time) {
  std::lock_guard<myMutex> lk(_mutex);
  _votedTime = time;
}

void ClusterNode::setNodePfail() {
  std::lock_guard<myMutex> lk(_mutex);
  _flags |= CLUSTER_NODE_PFAIL;
}

Status ClusterNode::addSlot(uint32_t slot, uint32_t masterSlavesCount) {
  std::lock_guard<myMutex> lk(_mutex);
  if (slot >= CLUSTER_SLOTS) {
    return {ErrorCodes::ERR_CLUSTER, "slot should be less than 16383"};
  }
  if (_mySlots.test(slot)) {
    return {ErrorCodes::ERR_INTERNAL, ""};
  }
  _mySlots.set(slot);
  _numSlots++;
  if (_numSlots == 1 && masterSlavesCount) {
    _flags |= CLUSTER_NODE_MIGRATE_TO;
  }
  return {ErrorCodes::ERR_OK, ""};
}

bool ClusterNode::setSlotBit(uint32_t slot, uint32_t masterSlavesCount) {
  std::lock_guard<myMutex> lk(_mutex);
  bool old = _mySlots.test(slot);
  _mySlots.set(slot);
  if (!old) {
    _numSlots++;
    //  When a master gets its first slot, even if it has no slaves,
    //  it gets flagged with MIGRATE_TO, that is, the master is a valid
    //  target for replicas migration, if and only if at least one of
    //  the other masters has slaves right now.
    //
    //  Normally masters are valid targets of replica migration if:
    //  1. The used to have slaves (but no longer have).
    //  2. They are slaves failing over a master that used to have slaves.
    //
    //  However new masters with slots assigned are considered valid
    //  migration targets if the rest of the cluster is not a slave-less.
    //
    //  See https://github.com/antirez/redis/issues/3043 for more info. */
    if (_numSlots == 1 && masterSlavesCount)
      _flags |= CLUSTER_NODE_MIGRATE_TO;
  }

  return old;
}

// Clear the slot bit and return the old value.
bool ClusterNode::clearSlotBit(uint32_t slot) {
  std::lock_guard<myMutex> lk(_mutex);
  bool old = _mySlots.test(slot);
  _mySlots.reset(slot);
  if (old)
    _numSlots--;

  return old;
}

bool ClusterNode::getSlotBit(uint32_t slot) const {
  std::lock_guard<myMutex> lk(_mutex);
  return _mySlots.test(slot);
}

uint64_t ClusterNode::getCtime() const {
  std::lock_guard<myMutex> lk(_mutex);
  return _ctime;
}

uint32_t ClusterNode::delAllSlotsNoLock() {
  INVARIANT_D(0);
  return 0;
}

// Delete all the slots associated with the specified node.
// The number of deleted slots is returned.
uint32_t ClusterNode::delAllSlots() {
  std::lock_guard<myMutex> lk(_mutex);
  return delAllSlotsNoLock();
}

bool ClusterNode::addSlave(std::shared_ptr<ClusterNode> slave) {
  std::lock_guard<myMutex> lk(_mutex);
  for (auto v : _slaves) {
    if (v == slave)
      return false;
  }

  _slaves.emplace_back(std::move(slave));
  _flags |= CLUSTER_NODE_MIGRATE_TO;
  _numSlaves++;
  return true;
}

bool ClusterNode::removeSlave(std::shared_ptr<ClusterNode> slave) {
  std::lock_guard<myMutex> lk(_mutex);

  for (auto iter = _slaves.begin(); iter != _slaves.end();) {
    if (*iter == slave) {
      iter = _slaves.erase(iter);
      _numSlaves--;

      if (_slaves.size() == 0) {
        _flags &= ~CLUSTER_NODE_MIGRATE_TO;
      }
      return true;
    } else {
      ++iter;
    }
  }

  return false;
}

Expected<std::vector<std::shared_ptr<ClusterNode>>> ClusterNode::getSlaves()
  const {
  std::lock_guard<myMutex> lk(_mutex);
  if (!nodeIsMaster()) {
    return {ErrorCodes::ERR_CLUSTER, "not a master"};
  }
  return _slaves;
}

uint32_t ClusterNode::getNonFailingSlavesCount() const {
  std::lock_guard<myMutex> lk(_mutex);
  uint32_t okslaves = 0;

  for (const auto& slave : _slaves) {
    if (!slave->nodeFailed())
      okslaves++;
  }

  return okslaves;
}

std::list<std::shared_ptr<ClusterNodeFailReport>> ClusterNode::getFailReport()
  const {
  std::lock_guard<myMutex> lk(_mutex);
  return _failReport;
}

void ClusterNode::addFailureReport(std::shared_ptr<ClusterNodeFailReport> n) {
  std::lock_guard<myMutex> lk(_mutex);
  _failReport.push_back(n);
}

void ClusterNode::delFailureReport(std::string nodename) {
  std::lock_guard<myMutex> lk(_mutex);
  for (auto it = _failReport.begin(); it != _failReport.end();) {
    if ((*it)->getFailNode() == nodename) {
      it = _failReport.erase(it);
    } else {
      it++;
    }
  }
}
void ClusterNode::cleanupFailureReports(mstime_t lifeTime) {
  std::lock_guard<myMutex> lk(_mutex);
  for (auto it = _failReport.begin(); it != _failReport.end();) {
    CReportPtr n = *it;
    mstime_t live = msSinceEpoch() - n->getFailTime();
    if (live > lifeTime) {
      it = _failReport.erase(it);
    } else {
      it++;
    }
  }
}

std::string ClusterNode::getFlagStr() {
  std::lock_guard<myMutex> lk(_mutex);
  std::string flagname = representClusterNodeFlags(_flags);
  return flagname;
}

CNodePtr ClusterNode::getMaster() const {
  std::lock_guard<myMutex> lk(_mutex);
  return _slaveOf;
}

uint16_t ClusterNode::getFlags() {
  std::lock_guard<myMutex> lk(_mutex);
  return _flags;
}

uint32_t ClusterNode::getSlotNum() {
  std::lock_guard<myMutex> lk(_mutex);
  return _numSlots;
}

uint64_t ClusterNode::getSentTime() {
  std::lock_guard<myMutex> lk(_mutex);
  return _pingSent;
}

uint64_t ClusterNode::getReceivedTime() {
  std::lock_guard<myMutex> lk(_mutex);
  return _pongReceived;
}

void ClusterNode::setSentTime(uint64_t t) {
  std::lock_guard<myMutex> lk(_mutex);
  _pingSent = t;
}

void ClusterNode::setReceivedTime(uint64_t t) {
  std::lock_guard<myMutex> lk(_mutex);
  _pongReceived = t;
}

std::bitset<CLUSTER_SLOTS> ClusterNode::getSlots() const {
  std::lock_guard<myMutex> lk(_mutex);
  return _mySlots;
}

uint32_t ClusterNode::getSlavesCount() const {
  std::lock_guard<myMutex> lk(_mutex);
  return _slaves.size();
}

uint16_t ClusterNode::getSlaveNum() const {
  std::lock_guard<myMutex> lk(_mutex);
  return _numSlaves;
}

void ClusterNode::markAsFailing() {
  std::lock_guard<myMutex> lk(_mutex);
  _flags &= ~CLUSTER_NODE_PFAIL;
  _flags |= CLUSTER_NODE_FAIL;
  _failTime = msSinceEpoch();
}

void ClusterNode::setAsMaster() {
  std::lock_guard<myMutex> lk(_mutex);

  _flags &= ~CLUSTER_NODE_SLAVE;
  _flags |= CLUSTER_NODE_MASTER;
  _slaveOf = nullptr;
}

bool ClusterNode::clearNodeFailureIfNeeded(uint32_t timeout) {
  std::lock_guard<myMutex> lk(_mutex);
  auto now = msSinceEpoch();

  INVARIANT_D(nodeFailed());

  // For slaves we always clear the FAIL flag if we can contact the
  // node again.
  if (nodeIsSlave() || _numSlots == 0 || nodeIsArbiter()) {
    serverLog(LL_NOTICE,
              "Clear FAIL state for node %.40s: %s is reachable again.",
              _nodeName.c_str(),
              nodeIsSlave() ? "slave" : "master without slots");
    _flags &= ~CLUSTER_NODE_FAIL;

    return true;
  }

  // If it is a master and...
  // 1) The FAIL state is old enough.
  // 2) It is yet serving slots from our point of view (not failed over).
  // Apparently no one is going to fix these slots, clear the FAIL flag.
  if (nodeIsMaster() && _numSlots > 0 &&
      (now - _failTime) > (timeout * CLUSTER_FAIL_UNDO_TIME_MULT)) {
    serverLog(LL_NOTICE,
              "Clear FAIL state for node %.40s: is reachable again and "
              "nobody is serving its slots after some time.",
              _nodeName.c_str());
    _flags &= ~CLUSTER_NODE_FAIL;
    return true;
  }

  return false;
}

void ClusterNode::setMaster(std::shared_ptr<ClusterNode> master) {
  std::lock_guard<myMutex> lk(_mutex);
  _slaveOf = master;
}

bool ClusterNode::nodeIsMaster() const {
  std::lock_guard<myMutex> lk(_mutex);
  return (_flags & CLUSTER_NODE_MASTER) ? true : false;
}

bool ClusterNode::nodeIsArbiter() const {
  std::lock_guard<myMutex> lk(_mutex);
  return (_flags & CLUSTER_NODE_ARBITER) ? true : false;
}

bool ClusterNode::nodeIsSlave() const {
  std::lock_guard<myMutex> lk(_mutex);
  return (_flags & CLUSTER_NODE_SLAVE) ? true : false;
}

bool ClusterNode::nodeHasAddr() const {
  std::lock_guard<myMutex> lk(_mutex);
  return (_flags & CLUSTER_NODE_NOADDR) ? false : true;
}

bool ClusterNode::nodeWithoutAddr() const {
  std::lock_guard<myMutex> lk(_mutex);
  return (_flags & CLUSTER_NODE_NOADDR) ? true : false;
}

bool ClusterNode::nodeIsMyself() const {
  std::lock_guard<myMutex> lk(_mutex);
  return (_flags & CLUSTER_NODE_MYSELF) ? true : false;
}

bool ClusterNode::nodeInHandshake() const {
  std::lock_guard<myMutex> lk(_mutex);
  return (_flags & CLUSTER_NODE_HANDSHAKE) ? true : false;
}

bool ClusterNode::nodeTimedOut() const {
  std::lock_guard<myMutex> lk(_mutex);
  return (_flags & CLUSTER_NODE_PFAIL) ? true : false;
}

bool ClusterNode::nodeFailed() const {
  std::lock_guard<myMutex> lk(_mutex);
  return (_flags & CLUSTER_NODE_FAIL) ? true : false;
}

bool ClusterNode::nodeCantFailover() const {
  std::lock_guard<myMutex> lk(_mutex);
  return (_flags & CLUSTER_NODE_NOFAILOVER) ? true : false;
}


ClusterNodeFailReport::ClusterNodeFailReport(const std::string& node,
                                             mstime_t time)
  : _nodeName(node), _time(time) {}


void ClusterNodeFailReport::setFailNode(const std::string& node) {
  _nodeName = node;
}

void ClusterNodeFailReport::setFailTime(mstime_t time) {
  _time = time;
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
 * The function returns false if the node address is still the same,
 * otherwise true is returned. */
bool ClusterState::updateAddressIfNeeded(CNodePtr node,
                                         std::shared_ptr<ClusterSession> sess,
                                         const ClusterMsg& msg) {
  std::string ip = msg.getHeader()->_myIp;
  uint32_t port = msg.getHeader()->_port;
  uint32_t cport = msg.getHeader()->_cport;

  /* We don't proceed if the link is the same as the sender link, as this
   * function is designed to see if the node link is consistent with the
   * symmetric link that is used to receive PINGs from the node.
   *
   * As a side effect this function never frees the passed 'link', so
   * it is safe to call during packet processing. */
  if (sess == node->getSession())
    return false;

  /* if domain name is used in gossip instead of ip address,
   * no need to change the domain name in k8s */
  if (_server->getParams()->domainEnabled)
    return false;

  ip = sess->nodeIp2String(ip);
  if (node->getPort() == port && node->getCport() == cport &&
      ip == node->getNodeIp()) {
    return false;
  }

  std::lock_guard<myMutex> lock(_mutex);
  {
    /* IP / port is different, update it. */
    node->setNodeIp(ip);
    node->setNodePort(port);
    node->setNodeCport(cport);
    node->freeClusterSession();
    node->_flags &= ~CLUSTER_NODE_NOADDR;
  }

  serverLog(LL_WARNING,
            "Address updated for node %.40s, now %s:%d",
            node->getNodeName().c_str(),
            ip.c_str(),
            port);


  return true;
}

void ClusterNode::setClient(std::shared_ptr<BlockingTcpClient> client) {
  std::lock_guard<myMutex> lk(_mutex);
  _nodeClient = client;
}

std::shared_ptr<BlockingTcpClient> ClusterNode::getClient() const {
  std::lock_guard<myMutex> lk(_mutex);
  return _nodeClient;
}

void ClusterNode::setSession(std::shared_ptr<ClusterSession> sess) {
  std::lock_guard<myMutex> lk(_mutex);
  INVARIANT_D(!_nodeSession);
  _nodeSession = sess;
}

std::shared_ptr<ClusterSession> ClusterNode::getSession() const {
  std::lock_guard<myMutex> lk(_mutex);
  return _nodeSession;
}


void ClusterNode::freeClusterSession() {
  std::shared_ptr<ClusterSession> tmpSession = nullptr;
  {
    std::lock_guard<myMutex> lk(_mutex);
    if (!_nodeSession) {
      return;
    }
    LOG(INFO) << "free session:" << _nodeSession->getRemoteRepr();
    tmpSession = std::move(_nodeSession);
    _nodeSession = nullptr;
  }

  tmpSession->setCloseAfterRsp();
  tmpSession->setNode(nullptr);
  // TODO(vinchen): check it carefully
  tmpSession->endSession();
  tmpSession = nullptr;
}

ClusterState::ClusterState(std::shared_ptr<ServerEntry> server)
  : _myself(nullptr),
    _currentEpoch(0),
    _lastVoteEpoch(0),
    _server(server),
    _blockState(false),
    _blockTime(0),
    _mfEnd(0),
    _mfSlave(nullptr),
    _mfMasterOffset(0),
    _isMfOffsetReceived(false),
    _mfCanStart(0),
    _statsPfailNodes(0),
    _isCliBlocked(false),
    _failoverAuthTime(0),
    _failoverAuthCount(0),
    _failoverAuthSent(0),
    _failoverAuthRank(0),
    _failoverAuthEpoch(0),
    _isVoteFailByDataAge(false),
    _state(ClusterHealth::CLUSTER_FAIL),
    _size(1),
    _allSlots(),
    _cantFailoverReason(CLUSTER_CANT_FAILOVER_NONE),
    _lastLogTime(0),
    _updateStateCallTime(0),
    _amongMinorityTime(0),
    _todoBeforeSleep(0) {
  _nodesBlackList.clear();
  _slotsKeysCount.fill(0);
  _statsMessagesReceived.fill(0);
  _statsMessagesSent.fill(0);
}

bool ClusterState::clusterHandshakeInProgress(const std::string& host,
                                              uint32_t port,
                                              uint32_t cport) {
  std::lock_guard<myMutex> lk(_mutex);
  for (auto& pnode : _nodes) {
    auto node = pnode.second;
    if (!node->nodeInHandshake())
      continue;

    if (node->getNodeIp() == host && node->getPort() == port &&
        node->getCport() == cport)
      return true;
  }

  return false;
}

bool ClusterState::clusterStartHandshake(const std::string& host,
                                         uint32_t port,
                                         uint32_t cport) {
  // TODO(wayenchen)
  /* IP sanity check */

  /* Port sanity check */
  if (port <= 0 || port > 65535 || cport <= 0 || cport > 65535) {
    return false;
  }

  if (clusterHandshakeInProgress(host, port, cport)) {
    // errno = EAGAIN;
    // return 0;
    // NODE(vinchen): diff from redis, for test
    return true;
  }

  auto name = getUUid(20);
  auto node =
    std::make_shared<ClusterNode>(name,
                                  CLUSTER_NODE_HANDSHAKE | CLUSTER_NODE_MEET,
                                  shared_from_this(),
                                  host,
                                  port,
                                  cport);
  clusterAddNode(node);

  return true;
}

Status ClusterState::clusterBumpConfigEpochWithoutConsensus() {
  std::lock_guard<myMutex> lk(_mutex);
  uint64_t maxEpoch = clusterGetOrUpdateMaxEpoch(false);
  uint64_t configEpoch = _myself->getConfigEpoch();
  if (configEpoch == 0 || configEpoch != maxEpoch) {
    setCurrentEpoch(_currentEpoch + 1);
    _myself->setConfigEpoch(_currentEpoch);
  }
  Status s = clusterSaveNodesNoLock();
  if (!s.ok()) {
    return s;
  }
  return {ErrorCodes::ERR_OK, "bump config epoch ok"};
}

/* Send a PONG packet to every connected node that's not in handshake state
 * and for which we have a valid link.
 *
 * In Redis Cluster pongs are not used just for failure detection, but also
 * to carry important configuration information. So broadcasting a pong is
 * useful when something changes in the configuration and we want to make
 * the cluster aware ASAP (for instance after a slave promotion).
 *
 * The 'target' argument specifies the receiving instances using the
 * defines below:
 *
 * CLUSTER_BROADCAST_ALL -> All known instances.
 * CLUSTER_BROADCAST_LOCAL_SLAVES -> All slaves in my master-slaves ring.
 */
#define CLUSTER_BROADCAST_ALL 0
#define CLUSTER_BROADCAST_LOCAL_SLAVES 1
void ClusterState::clusterBroadcastPong(int target, uint64_t offset) {
  std::lock_guard<myMutex> lk(_mutex);
  for (const auto& nodep : _nodes) {
    const auto& node = nodep.second;

    if (!node->getSession())
      continue;
    if (node == _myself || node->nodeInHandshake())
      continue;
    if (target == CLUSTER_BROADCAST_LOCAL_SLAVES) {
      int local_slave = node->nodeIsSlave() && node->getMaster() &&
        (node->getMaster() == _myself ||
         node->getMaster() == _myself->getMaster());
      if (!local_slave)
        continue;
    }
    clusterSendPingNoLock(node->getSession(), ClusterMsg::Type::PONG, offset);
  }
}

void ClusterState::clusterSendFail(CNodePtr node, uint64_t offset) {
  ClusterMsg msg(
    ClusterMsg::Type::FAIL, shared_from_this(), _server, offset, node);
  clusterBroadcastMessage(msg);
}

void ClusterState::clusterBroadcastMessage(ClusterMsg& msg) {
  for (const auto& nodep : _nodes) {
    const auto& node = nodep.second;

    if (!node->getSession())
      continue;
    if (node->getFlags() & (CLUSTER_NODE_MYSELF | CLUSTER_NODE_HANDSHAKE))
      continue;
    node->getSession()->clusterSendMessage(msg);
  }
}

void ClusterState::clusterUpdateSlotsConfigWith(
  CNodePtr sender,
  uint64_t senderConfigEpoch,
  const std::bitset<CLUSTER_SLOTS>& slots) {
  std::array<uint16_t, CLUSTER_SLOTS> dirty_slots;
  CNodePtr newmaster = nullptr;
  uint32_t dirty_slots_count = 0;

  /* Here we set curmaster to this node or the node this node
   * replicates to if it's a slave. In the for loop we are
   * interested to check if slots are taken away from curmaster. */

  bool needReconfigure;
  bool masterNotFail;
  bool slaveIsNotFullSync;
  {
    std::lock_guard<myMutex> lk(_mutex);
    CNodePtr myself = getMyselfNode();
    auto curmaster = myself->nodeIsMaster() ? myself : myself->_slaveOf;
    if (sender == myself) {
      LOG(INFO) << "Discarding UPDATE message about myself.";
      return;
    }
    for (size_t j = 0; j < CLUSTER_SLOTS; j++) {
      if (slots.test(j)) {
        if (_allSlots[j] == sender)
          continue;

        /* We rebind the slot to the new node claiming it if:
         * 1) The slot was unassigned or the new node claims it with a
         *    greater configEpoch.
         * 2) We are not currently importing the slot. */

        if (_allSlots[j] == nullptr ||
            _allSlots[j]->getConfigEpoch() < senderConfigEpoch) {
          if (_allSlots[j] == myself && sender != myself) {
            dirty_slots[dirty_slots_count] = j;
            dirty_slots_count++;
          }
          if (_allSlots[j] == curmaster) {
            newmaster = sender;
          }
          clusterDelSlotNoLock(j);
          clusterAddSlotNoLock(sender, j);
        }
      }
    }
    needReconfigure =
      (newmaster && curmaster->getSlotNum() == 0) ? true : false;

    masterNotFail =
      (!curmaster->nodeFailed()) && myself->nodeIsSlave() ? true : false;
    /* NOTE(wayenchen) judge if the slave is on fullsync */
    slaveIsNotFullSync = (myself->nodeIsSlave() && masterNotFail &&
                          _server->getReplManager()->isSlaveFullSyncDone())
      ? true
      : false;
  }
  // NOTE(takenliu) save and update once at the last.
  clusterUpdateState();
  bool inMigrateTask = _server->getMigrateManager()->slotsInTask(slots);
  /* If at least one slot was reassigned from a node to another node
   * with a greater configEpoch, it is possible that:
   * 1) We are a master left without slots. This means that we were
   *    failed over and we should turn into a replica of the new
   *    master.
   * 2) We are a slave and our master is left without slots. We need
   *    to replicate to the new slots owner. */
  if (needReconfigure) {
    serverLog(LL_WARNING,
              "Configuration change detected "
              "Reconfiguring myself as a replica of %.40s",
              sender->getNodeName().c_str());
    Status s;
    if (masterNotFail) {
      s = _server->getReplManager()->replicationUnSetMaster();
      /* if error, fix it in cron*/
      if (!s.ok()) {
        LOG(ERROR) << "set myself master fail" << s.toString();
      }
    }
    /* NOTE(wayenchen) slave should not full sync when set new master*/
    if (!inMigrateTask &&
        ((slaveIsNotFullSync || !masterNotFail) || isMyselfMaster())) {
      s = clusterSetMaster(newmaster);
      if (!s.ok()) {
        LOG(ERROR) << "set newmaster :" << newmaster->getNodeName()
                   << "fail:" << s.toString();
      }
    }
  } else if (dirty_slots_count && !inMigrateTask) {
    /* NOTE(wayenchen): use gc to delete slots */
    /*
    auto s = _server->getGcMgr()->delGarbage();
    if (!s.ok()) {
      LOG(ERROR) << "delete dirty slos fail" << s.toString();
    }
    LOG(INFO) << "finish del key in slot, num is:" << dirty_slots_count;
    */
  }
}

void ClusterState::clusterHandleConfigEpochCollision(CNodePtr sender) {
  std::lock_guard<myMutex> lk(_mutex);

  /* Prerequisites: nodes have the same configEpoch and are both masters. */
  if (sender->getConfigEpoch() != _myself->getConfigEpoch() ||
      !sender->nodeIsMaster() || !_myself->nodeIsMaster())
    return;

  /* Don't act if the colliding node has a smaller Node ID. */
  if (sender->getNodeName() <= _myself->getNodeName()) {
    return;
  }

  /* Get the next ID available at the best of this node knowledge. */
  _currentEpoch++;

  _myself->setConfigEpoch(_currentEpoch);
  serverLog(LL_WARNING,
            "WARNING: configEpoch collision with node %.40s."
            " configEpoch set to %lu",
            sender->getNodeName().c_str(),
            (uint64_t)_currentEpoch);
}

uint64_t ClusterState::getCurrentEpoch() const {
  std::lock_guard<myMutex> lk(_mutex);
  return _currentEpoch;
}

uint64_t ClusterState::getLastVoteEpoch() const {
  std::lock_guard<myMutex> lk(_mutex);
  return _lastVoteEpoch;
}
uint64_t ClusterState::getFailAuthEpoch() const {
  std::lock_guard<myMutex> lk(_mutex);
  return _failoverAuthEpoch;
}

void ClusterState::setCurrentEpoch(uint64_t epoch) {
  std::lock_guard<myMutex> lk(_mutex);
  _currentEpoch = epoch;
}

void ClusterState::incrCurrentEpoch() {
  std::lock_guard<myMutex> lk(_mutex);
  _currentEpoch++;
}

void ClusterState::setFailAuthEpoch(uint64_t epoch) {
  _failoverAuthEpoch.store(epoch, std::memory_order_relaxed);
}

CNodePtr ClusterState::getMyselfNode() const {
  std::lock_guard<myMutex> lk(_mutex);
  return _myself;
}

std::string ClusterState::getMyselfName() const {
  std::lock_guard<myMutex> lk(_mutex);
  return _myself->getNodeName();
}

void ClusterState::setLastVoteEpoch(uint64_t epoch) {
  std::lock_guard<myMutex> lk(_mutex);
  _lastVoteEpoch = epoch;
}

Status ClusterState::setSlot(CNodePtr n, const uint32_t slot) {
  std::lock_guard<myMutex> lk(_mutex);
  bool s = clusterDelSlot(slot);
  if (s) {
    bool result = clusterAddSlot(n, slot);
    if (!result) {
      return {ErrorCodes::ERR_CLUSTER, "setslot add new slot fail"};
    }
  } else {
    return {ErrorCodes::ERR_CLUSTER, "setslot delete old slot fail!"};
  }

  if (n == _myself) {
    Status s = clusterBumpConfigEpochWithoutConsensus();
    if (!s.ok()) {
      return s;
    }
  }

  return {ErrorCodes::ERR_OK, "finish setslot"};
}

Status ClusterState::setSlots(CNodePtr n,
                              const std::bitset<CLUSTER_SLOTS>& slots) {
  {
    std::lock_guard<myMutex> lk(_mutex);
    serverLog(LL_VERBOSE,
              "setSlots node:%s slots:%s",
              n->getNodeName().c_str(),
              bitsetStrEncode(slots).c_str());
    size_t idx = 0;
    while (idx < slots.size()) {
      if (slots.test(idx)) {
        // already set
        if (_allSlots[idx] == n) {
          ++idx;
          continue;
        }
        bool s = clusterDelSlot(idx);
        if (s) {
          bool result = clusterAddSlot(n, idx);
          if (!result) {
            LOG(ERROR) << "setSlots addslot fail on slot:" << idx;
            return {ErrorCodes::ERR_CLUSTER, "setslot add new slot fail"};
          }
        } else {
          LOG(ERROR) << "setSlots delslot fail on slot:" << idx;
          return {ErrorCodes::ERR_CLUSTER, "setslot delete old slot fail!"};
        }
      }
      ++idx;
    }
  }

  if (n == getMyselfNode()) {
    Status s = clusterBumpConfigEpochWithoutConsensus();
    if (!s.ok()) {
      LOG(ERROR) << "setSlots BumpConfigEpoch fail";
      return s;
    }
    // NOTE(wayenchen) broadcast gossip message if meta change finished
    uint64_t offset = _server->getReplManager()->replicationGetOffset();
    clusterBroadcastPong(CLUSTER_BROADCAST_ALL, offset);
  }

  return {ErrorCodes::ERR_OK, "set slots ok"};
}

Status ClusterState::setSlotMyself(const uint32_t slot) {
  bool flag = clusterDelSlot(slot);
  if (flag) {
    bool result = clusterAddSlot(_myself, slot);
    if (!result) {
      return {ErrorCodes::ERR_CLUSTER, "setslot add new slot fail"};
    }
  } else {
    return {ErrorCodes::ERR_CLUSTER, "setslot delete old slot fail!"};
  }
  Status s = clusterBumpConfigEpochWithoutConsensus();
  if (!s.ok()) {
    return s;
  }
  return {ErrorCodes::ERR_OK, "finish setslot"};
}

void ClusterState::setSlotBelongMyself(const uint32_t slot) {
  Status s = setSlotMyself(slot);

  if (!s.ok()) {
    LOG(FATAL) << "set slot error:" << s.toString();
  } else {
    LOG(INFO) << "set slot: " << slot << "belong to" << _myself->getNodeName()
              << "finish!";
  }
}

CNodePtr ClusterState::getNodeBySlot(uint32_t slot) const {
  std::lock_guard<myMutex> lk(_mutex);
  return _allSlots[slot];
}

bool ClusterState::isSlotBelongToMe(uint32_t slot) {
  std::lock_guard<myMutex> lk(_mutex);
  auto owner = getNodeBySlot(slot);
  if (!owner) {
    return false;
  }
  return owner == _myself;
}

Expected<CNodePtr> ClusterState::clusterHandleRedirect(uint32_t slot,
                                                       Session* sess) const {
  std::lock_guard<myMutex> lk(_mutex);
  if (_state == ClusterHealth::CLUSTER_FAIL) {
    return {ErrorCodes::ERR_CLUSTER_REDIR_DOWN_STATE, ""};
  }

  auto node = getNodeBySlot(slot);
  if (!node) {
    return {ErrorCodes::ERR_CLUSTER_REDIR_DOWN_UNBOUND, ""};
  }

  if ((sess->getCtx()->getFlags() & CLIENT_READONLY) && isMyselfSlave() &&
      _myself->_slaveOf == node) {
    auto cmd = Command::getCommand(sess);
    if (cmd != nullptr && (cmd->getFlags() & CMD_READONLY)) {
      // cmd == evalCom || cmd == evalShaCommand
      return _myself;
    }
  }

  if (node != _myself) {
    std::stringstream ss;
    ss << "-"
       << "MOVED"
       << " " << slot << " " << node->getNodeIp() << ":" << node->getPort()
       << "\r\n";
    return {ErrorCodes::ERR_MOVED, ss.str()};
  }

  return node;
}

bool ClusterState::isContainSlot(uint32_t slotId) {
  std::lock_guard<myMutex> lk(_mutex);
  return getNodeBySlot(slotId) == _myself;
}

bool ClusterState::clusterIsOK() const {
  std::lock_guard<myMutex> lk(_mutex);
  return getClusterState() == ClusterHealth::CLUSTER_OK;
}

void ClusterState::setMyselfNode(CNodePtr node) {
  std::lock_guard<myMutex> lk(_mutex);
  INVARIANT(node != nullptr);
  if (!_myself) {
    _myself = node;
  }
}

bool ClusterState::isMyselfMaster() {
  std::lock_guard<myMutex> lk(_mutex);
  return _myself->nodeIsMaster();
}

bool ClusterState::isMyselfSlave() const {
  std::lock_guard<myMutex> lk(_mutex);
  return _myself->nodeIsSlave();
}

CNodePtr ClusterState::getMyMaster() {
  std::lock_guard<myMutex> lk(_mutex);
  if (_myself->nodeIsMaster())
    return nullptr;
  return _myself->getMaster();
}

uint64_t ClusterState::getPfailNodeNum() const {
  std::lock_guard<myMutex> lk(_mutex);
  return _statsPfailNodes;
}

void ClusterState::incrPfailNodeNum() {
  std::lock_guard<myMutex> lk(_mutex);
  _statsPfailNodes++;
}

void ClusterState::setPfailNodeNum(uint64_t t) {
  std::lock_guard<myMutex> lk(_mutex);
  _statsPfailNodes = t;
}

mstime_t ClusterState::getFailAuthTime() const {
  std::lock_guard<myMutex> lk(_mutex);
  return _failoverAuthTime;
}

void ClusterState::setFailAuthTime(mstime_t t) {
  std::lock_guard<myMutex> lk(_mutex);
  _failoverAuthTime = t;
}

void ClusterState::addFailAuthTime(mstime_t t) {
  std::lock_guard<myMutex> lk(_mutex);
  _failoverAuthTime += t;
}

void ClusterState::setFailAuthRank(uint32_t t) {
  _failoverAuthRank.store(t, std::memory_order_relaxed);
}

void ClusterState::setFailAuthCount(uint32_t t) {
  _failoverAuthCount.store(t, std::memory_order_relaxed);
}

void ClusterState::setFailAuthSent(uint32_t t) {
  _failoverAuthSent.store(t, std::memory_order_relaxed);
}

bool ClusterState::clusterNodeFailed(const std::string& nodeid) {
  std::lock_guard<myMutex> lk(_mutex);
  auto node = clusterLookupNodeNoLock(nodeid);
  return node->nodeFailed();
}

bool ClusterState::isDataAgeTooLarge() {
  std::lock_guard<myMutex> lk(_mutex);
  if (!_myself->nodeIsSlave()) {
    return false;
  }
  if (!_myself->getMaster()->nodeFailed()) {
    return false;
  }
  return _isVoteFailByDataAge.load(std::memory_order_relaxed);
}

Status ClusterState::forgetNodes() {
  std::lock_guard<myMutex> lk(_mutex);
  std::vector<CNodePtr> nodesList;
  for (auto& v : _nodes) {
    if (v.second == _myself) {
      continue;
    }
    nodesList.push_back(v.second);
  }
  for (auto it = nodesList.begin(); it != nodesList.end(); it++) {
    clusterDelNodeNoLock(*it);
    DLOG(INFO) << "delete node " << (*it)->getNodeName()
               << "nodeNum:" << _nodes.size();
  }

  return {ErrorCodes::ERR_OK, ""};
}

Status ClusterState::clusterSaveNodesNoLock() {
  Catalog* cataLog = _server->getCatalog();
  EpochMeta epoch(_currentEpoch, _lastVoteEpoch);
  Status s = cataLog->setEpochMeta(epoch);
  if (!s.ok()) {
    LOG(FATAL) << "save epoch meta error:" << s.toString();
    return s;
  }

  std::unordered_map<std::string, CNodePtr>::iterator iter;
  for (iter = _nodes.begin(); iter != _nodes.end(); iter++) {
    CNodePtr node = iter->second;

    uint16_t nodeFlags = node->getFlags();
    if (nodeFlags & CLUSTER_NODE_HANDSHAKE)
      continue;

    std::string masterName =
      (node->_slaveOf) ? node->_slaveOf->getNodeName() : "-";

    std::bitset<CLUSTER_SLOTS> slots = node->getSlots();

    auto slotBuff = std::move(bitsetEncodeVec(slots));

    ClusterMeta meta(node->getNodeName(),
                     node->getNodeIp(),
                     node->getPort(),
                     node->getCport(),
                     nodeFlags,
                     masterName,
                     node->_pingSent,
                     node->_pongReceived,
                     node->getConfigEpoch(),
                     slotBuff);

    Status sMeta = cataLog->setClusterMeta(std::move(meta));

    if (!sMeta.ok()) {
      LOG(FATAL) << "save Node error:" << s.toString();
      return s;
    }
  }
  return {ErrorCodes::ERR_OK, "save node config finish"};
}

void ClusterState::setGossipBlock(uint64_t time) {
  _blockTime.store(time, std::memory_order_relaxed);
  _blockState.store(true, std::memory_order_relaxed);
}

void ClusterState::setGossipUnBlock() {
  _blockState.store(false, std::memory_order_relaxed);
}

Expected<std::string> ClusterState::getNodeInfo(CNodePtr n) {
  if (!n->nodeIsMaster()) {
    return {ErrorCodes::ERR_WRONG_TYPE, "node is not master!"};
  }

  // if node fail do what? mark fail flag
  std::string flags = representClusterNodeFlags(n->getFlags());
  std::bitset<CLUSTER_SLOTS> slots = n->getSlots();
  std::string slotStr;
  if (slots.count() == 0) {
    slotStr = "contain no slots";
  } else {
    slotStr = bitsetStrEncode(slots);
    slotStr.erase(slotStr.begin());
    slotStr.erase(slotStr.end() - 1);
  }

  std::stringstream stream;
  stream << "slot:" << slotStr << "\n";
  // todo (how to get master migrate info)
  //  get nothing if it is a slave
  std::string nodeName = n->getNodeName();

  if (n->nodeIsMyself()) {
    auto emigrStr =
      _server->getMigrateManager()->getMigrateInfoStrSimple(slots);
    if (emigrStr.ok()) {
      stream << "migrateSlots:" << emigrStr.value() << "\n";
    } else {
      stream << "migrateSlots:null"
             << "\n";
    }
  }
  stream << "flag:" << flags << "\n"
         << "configEpoch:" << n->getConfigEpoch() << "\n"
         << "currentEpoch:" << _currentEpoch << "\n"
         << "lastVoteEpoch:" << _lastVoteEpoch;

  return stream.str();
}

Expected<std::string> ClusterState::getBackupInfo() {
  auto myself = getMyselfNode();
  CNodePtr targetMaster = myself->nodeIsMaster() ? myself : myself->getMaster();

  auto eptInfo = getNodeInfo(targetMaster);
  if (!eptInfo.ok()) {
    return eptInfo;
  }
  return eptInfo.value();
}

Expected<std::string> ClusterState::clusterReplyMultiBulkSlots() {
  std::stringstream ss;
  uint32_t nodeNum = 0;
  std::stringstream ssTemp;

  std::lock_guard<myMutex> lk(_mutex);
  for (const auto& v : _nodes) {
    CNodePtr node = v.second;
    if (!node->nodeIsMaster() || node->getSlotNum() == 0) {
      continue;
    }
    int32_t start = -1;

    auto exptSlaveList = node->getSlaves();
    if (!exptSlaveList.ok()) {
      return exptSlaveList.status();
    }
    auto slaveList = exptSlaveList.value();
    uint16_t slaveNum = slaveList.size();
    for (int32_t j = 0; j < CLUSTER_SLOTS; j++) {
      auto bit = node->getSlots().test(j);
      if (bit) {
        if (start == -1)
          start = j;
      }
      if (start != -1 && (!bit || j == CLUSTER_SLOTS - 1)) {
        if (bit && j == CLUSTER_SLOTS - 1)
          j++;
        nodeNum++;

        Command::fmtMultiBulkLen(ssTemp, slaveNum + 3);

        if (start == j - 1) {
          Command::fmtLongLong(ssTemp, start);
          Command::fmtLongLong(ssTemp, start);
        } else {
          Command::fmtLongLong(ssTemp, start);
          Command::fmtLongLong(ssTemp, j - 1);
        }

        Command::fmtMultiBulkLen(ssTemp, 3);
        Command::fmtBulk(ssTemp, node->getNodeIp());
        Command::fmtLongLong(ssTemp, node->getPort());
        Command::fmtBulk(ssTemp, node->getNodeName());
        if (slaveNum > 0) {
          for (uint16_t i = 0; i < slaveNum; i++) {
            Command::fmtMultiBulkLen(ssTemp, 3);
            CNodePtr slave = slaveList[i];
            Command::fmtBulk(ssTemp, slave->getNodeIp());
            Command::fmtLongLong(ssTemp, slave->getPort());
            Command::fmtBulk(ssTemp, slave->getNodeName());
          }
        }
        start = -1;
      }
    }
  }
  Command::fmtMultiBulkLen(ss, nodeNum);
  ss << ssTemp.str();
  return ss.str();
}

void ClusterState::clusterSaveNodes() {
  std::lock_guard<myMutex> lk(_mutex);
  Status s = clusterSaveNodesNoLock();

  if (!s.ok()) {
    LOG(FATAL) << "save Node confg error:" << s.toString();
  }
}

Status ClusterState::clusterSaveConfig() {
  std::lock_guard<myMutex> lk(_mutex);
  Status s = clusterSaveNodesNoLock();
  return s;
}

void ClusterState::setClientUnBlock() {
  _cv.notify_one();
  _isCliBlocked.store(false, std::memory_order_relaxed);
}

bool ClusterState::isRightReplicate() {
  if (!getMyMaster()) {
    return false;
  }
  auto masterIp = getMyMaster()->getNodeIp();
  auto masterPort = getMyMaster()->getPort();
  auto replMgr = _server->getReplManager();

  Status s;
  std::vector<uint32_t> errList =
    replMgr->checkMasterHost(masterIp, masterPort);

  if (errList.size() > 0) {
    return false;
  }
  return true;
}

Status ClusterState::forceFailover(bool force, bool takeover) {
  resetManualFailoverNoLock();
  setMfEnd(msSinceEpoch() + CLUSTER_MF_TIMEOUT);
  Status s;
  if (takeover) {
    /* A takeover does not perform any initial check. It just
     * generates a new configuration epoch for this node without
     * consensus, claims the master's slots, and broadcast the new
     * configuration. */
    serverLog(LL_WARNING, "Taking over the master (user request).");
    s = clusterBumpConfigEpochWithoutConsensus();
    if (!s.ok()) {
      LOG(ERROR) << "bump config fail when force failover";
      return s;
    }
    auto s = clusterFailoverReplaceYourMaster();
    if (!s.ok()) {
      LOG(ERROR) << "replace master fail when force failover";
      return s;
    }

  } else if (force) {
    if (!isRightReplicate()) {
      serverLog(LL_WARNING, "slave replication is not right");
      return {ErrorCodes::ERR_CLUSTER, "slave replication not right"};
    }
    /* If this is a forced failover, we don't need to talk with our
     * master to agree about the offset. We just failover taking over
     * it without coordination. */
    serverLog(LL_WARNING, "Forced failover user request accepted.");
    setMfStart();
  } else {
    if (!isRightReplicate()) {
      serverLog(LL_WARNING, "slave replication is not right");
      return {ErrorCodes::ERR_CLUSTER, "slave replication not right"};
    }
    serverLog(LL_WARNING, "Manual failover user request accepted.");
    uint64_t offset = _server->getReplManager()->replicationGetOffset();
    clusterSendMFStart(_myself->getMaster(), offset);
  }

  return {ErrorCodes::ERR_OK, "finish force failover"};
}

bool ClusterState::clusterSetNodeAsMaster(CNodePtr node) {
  std::lock_guard<myMutex> lk(_mutex);
  bool s = clusterSetNodeAsMasterNoLock(node);
  return s;
}

bool ClusterState::clusterSetNodeAsMasterNoLock(CNodePtr node) {
  if (node->nodeIsMaster()) {
    return false;
  }

  if (node->getMaster()) {
    // NODE(vinchen): There is no deadlock between
    // node->_mutex and node->getMaster()->_mutex.
    // Because if we want to lock more than one node,
    // you must lock ClusterState::_mutex first.
    clusterNodeRemoveSlave(node->getMaster(), node);
    if (node != _myself) {
      node->_flags |= CLUSTER_NODE_MIGRATE_TO;
    }
  }

  node->setAsMaster();
  return true;
}

/* NOTE(wayenchen) if set ignoreRepl as true, setMaster will not replicate
 * data*/
Status ClusterState::clusterSetMaster(CNodePtr node, bool ignoreRepl) {
  Status s;
  {
    std::lock_guard<myMutex> lk(_mutex);
    if (_myself->getMaster() != node) {
      auto s = clusterSetMasterNoLock(node);
      if (!s.ok()) {
        LOG(ERROR) << "set master " << node->getNodeName()
                   << "as my master failed";
        return s;
      }
      LOG(INFO) << "set node meta:" << node->getNodeName() << "as my master";
    }
    if (isClientBlock()) {
      setClientUnBlock();
      INVARIANT_D(!_isCliBlocked.load(std::memory_order_relaxed));
      LOG(INFO) << "unlock finish when set new master:" << node->getNodeName();
    }
  }
  if (!ignoreRepl) {
    bool incrSync = false;
    {
      std::lock_guard<myMutex> lk(_mutex);
      if (_mfEnd != 0 && _mfSlave == node) {
        incrSync = true;
      }
    }
    LOG(INFO) << "clusterSetMaster incrSync:" << incrSync;

    s = _server->getReplManager()->replicationSetMaster(
      node->getNodeIp(), node->getPort(), false, incrSync);
    if (!s.ok()) {
      LOG(ERROR) << "relication set master fail:" << s.toString();
      return s;
    }
  }
  LOG(INFO) << "replication set node:" << node->getNodeName()
            << " as my master,"
            << "ip:" << node->getNodeIp() << " port:" << node->getPort()
            << ",ignoreRepl:" << ignoreRepl;

  resetManualFailover();

  return {ErrorCodes::ERR_OK, ""};
}

Status ClusterState::clusterSetMasterNoLock(CNodePtr node) {
  INVARIANT(node != _myself);
  INVARIANT(_myself->getSlotNum() == 0);
  INVARIANT(!_myself->nodeIsArbiter());

  if (_myself->nodeIsMaster()) {
    _myself->_flags &= ~(CLUSTER_NODE_MASTER | CLUSTER_NODE_MIGRATE_TO);
    _myself->_flags |= CLUSTER_NODE_SLAVE;
    clusterCloseAllSlots();
  } else {
    if (_myself->getMaster()) {
      clusterNodeRemoveSlaveNolock(_myself->getMaster(), _myself);
    }
  }
  bool s = clusterNodeAddSlaveNolock(node, _myself);
  if (!s) {
    return {ErrorCodes::ERR_CLUSTER, "add slave fail"};
  }
  return {ErrorCodes::ERR_OK, ""};
}


bool ClusterState::clusterNodeRemoveSlave(CNodePtr master, CNodePtr slave) {
  std::lock_guard<myMutex> lk(_mutex);
  return master->removeSlave(slave);
}

bool ClusterState::clusterNodeRemoveSlaveNolock(CNodePtr master,
                                                CNodePtr slave) {
  return master->removeSlave(slave);
}

bool ClusterState::clusterNodeAddSlave(CNodePtr master, CNodePtr slave) {
  std::lock_guard<myMutex> lk(_mutex);
  slave->setMaster(master);
  return master->addSlave(slave);
}

bool ClusterState::clusterNodeAddSlaveNolock(CNodePtr master, CNodePtr slave) {
  slave->setMaster(master);
  return master->addSlave(slave);
}

void ClusterState::clusterBlacklistAddNode(CNodePtr node) {
  std::lock_guard<myMutex> lk(_mutex);
  std::string id = node->getNodeName();
  clusterBlacklistCleanupNoLock();

  auto iter = _nodesBlackList.find(id);
  if (iter != _nodesBlackList.end()) {
    _nodesBlackList[id] = sinceEpoch() + CLUSTER_BLACKLIST_TTL;
  } else {
    _nodesBlackList.insert(
      std::make_pair(id, sinceEpoch() + CLUSTER_BLACKLIST_TTL));
  }
}

void ClusterState::clusterBlacklistCleanupNoLock() {
  for (auto it = _nodesBlackList.begin(); it != _nodesBlackList.end();) {
    std::string id = it->first;
    uint64_t expire = it->second;
    if (expire < sinceEpoch()) {
      it = _nodesBlackList.erase(it);
    } else {
      ++it;
    }
  }
}

bool ClusterState::clusterBlacklistExists(const std::string& nodeid) {
  std::lock_guard<myMutex> lk(_mutex);
  clusterBlacklistCleanupNoLock();
  auto iter = _nodesBlackList.find(nodeid);
  return iter != _nodesBlackList.end();
}

void ClusterState::clusterAddNodeNoLock(CNodePtr node) {
  std::string nodeName = node->getNodeName();
  std::unordered_map<std::string, CNodePtr>::iterator it;
  if ((it = _nodes.find(nodeName)) != _nodes.end()) {
    _nodes[nodeName] = node;
  } else {
    _nodes.insert(std::make_pair(nodeName, node));
    serverLog(LL_VERBOSE,
              "cluster add node:%s ip:%s port:%lu ",
              node->getNodeName().c_str(),
              node->getNodeIp().c_str(),
              node->getPort());
  }
}

std::string ClusterState::clusterGenNodesDescription(uint16_t filter,
                                                     bool simple) {
  std::stringstream ss;
  for (const auto& v : _nodes) {
    CNodePtr node = v.second;
    if (node->getFlags() & filter) {
      continue;
    }
    std::string nodeDescription = clusterGenNodeDescription(node, simple);
    ss << nodeDescription << "\n";
  }
  return Command::fmtBulk(ss.str());
}

std::string ClusterState::clusterGenNodeDescription(CNodePtr n, bool simple) {
  std::stringstream stream;
  std::string masterName =
    n->_slaveOf ? " " + n->_slaveOf->getNodeName() + " " : " - ";
  std::string flags = representClusterNodeFlags(n->_flags);

  ConnectState connectState = n->getConnectState();
  std::string stateStr =
    (connectState == ConnectState::CONNECTED) ? "connected" : "disconnected";

  stream << n->getNodeName() << " " << n->getNodeIp() << ":" << n->getPort()
         << "@" << n->getCport() << " " << flags << masterName << n->_pingSent
         << " " << n->_pongReceived << " " << n->getConfigEpoch() << " "
         << stateStr;

  if (n->nodeIsMaster()) {
    auto slots = n->getSlots();
    std::string slotStr = bitsetStrEncode(slots);
    slotStr.erase(slotStr.end() - 1);
    stream << slotStr;

    auto migrateMgr = _server->getMigrateManager();
    if (n->nodeIsMyself()) {
      std::string migrateStr;
      Expected<std::string> eMigrStr("");
      if (simple) {
        eMigrStr = migrateMgr->getMigrateInfoStrSimple(slots);
      } else {
        eMigrStr = migrateMgr->getMigrateInfoStr(slots);
      }
      if (eMigrStr.ok()) {
        migrateStr = eMigrStr.value();
        stream << " " << migrateStr;
      }
    }
  }

  return stream.str();
}

std::string ClusterState::clusterGenStateDescription() {
  std::vector<std::string> states = {"ok", "fail", "needhelp"};
  uint32_t slots_assigned = 0, slots_ok = 0;
  uint32_t slots_pfail = 0, slots_fail = 0;

  for (size_t j = 0; j < CLUSTER_SLOTS; j++) {
    CNodePtr node = _allSlots[j];
    if (node == nullptr)
      continue;
    slots_assigned++;
    if (node->nodeFailed()) {
      slots_fail++;
    } else if (node->nodeTimedOut()) {
      slots_pfail++;
    } else {
      slots_ok++;
    }
  }
  uint64_t myepoch = (_myself->nodeIsSlave() && _myself->_slaveOf)
    ? _myself->_slaveOf->getConfigEpoch()
    : _myself->getConfigEpoch();

  std::stringstream clusterInfo;

  uint8_t x = (_state == ClusterHealth::CLUSTER_OK) ? 0 : 1;

  clusterInfo << "cluster_state:" << states[x] << "\r\n"
              << "cluster_slots_assigend:" << slots_assigned << "\r\n"
              << "cluster_slots_ok:" << slots_ok << "\r\n"
              << "cluster_slots_pfail:" << slots_pfail << "\r\n"
              << "cluster_known_nodes:" << _nodes.size() << "\r\n"
              << "cluster_size:" << _size << "\r\n"
              << "cluster_current_epoch:" << _currentEpoch << "\r\n"
              << "cluster_my_epoch:" << myepoch << "\r\n";

  uint64_t totMsgSent = 0;
  uint64_t totMsgReceived = 0;

  for (uint16_t i = 0; i < CLUSTERMSG_TYPE_COUNT; i++) {
    if (_statsMessagesSent[i] == 0)
      continue;
    totMsgSent += _statsMessagesSent[i];
    clusterInfo << "cluster_stats_messages_"
                << clusterMsgTypeString(ClusterMsg::Type(i))
                << "_sent:" << _statsMessagesSent[i] << "\r\n";
  }
  clusterInfo << "cluster_stats_messages_sent:" << totMsgSent << "\r\n";

  for (uint16_t i = 0; i < CLUSTERMSG_TYPE_COUNT; i++) {
    if (_statsMessagesReceived[i] == 0)
      continue;
    totMsgReceived += _statsMessagesReceived[i];
    clusterInfo << "cluster_stats_messages_"
                << clusterMsgTypeString(ClusterMsg::Type(i))
                << "_received:" << _statsMessagesReceived[i] << "\r\n";
  }
  clusterInfo << "cluster_stats_messages_received:" << totMsgSent << "\r\n";

  return Command::fmtBulk(clusterInfo.str());
}

void ClusterState::clusterAddNode(CNodePtr node, bool save) {
  std::lock_guard<myMutex> lk(_mutex);

  clusterAddNodeNoLock(node);
  if (save) {
    clusterSaveNodesNoLock();
  }
}

void ClusterState::clusterDelNodeNoLock(CNodePtr delnode) {
  /* 1) Mark slots as unassigned. */
  auto nodeName = delnode->getNodeName();
  if (_nodes.find(nodeName) == _nodes.end()) {
    LOG(WARNING) << "can not find delete node" << nodeName;
    return;
  }
  for (uint32_t j = 0; j < CLUSTER_SLOTS; j++) {
    if (_allSlots[j] == delnode) {
      clusterDelSlot(j);
    }
  }

  /* 2) Remove failure reports. */
  for (const auto& nodep : _nodes) {
    const auto& node = nodep.second;
    if (node == delnode)
      continue;

    clusterNodeDelFailureReport(node, delnode);
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

    n_slaves += node->getSlavesCount();
  }

  return n_slaves;
}

Status ClusterState::freeClusterNode(CNodePtr delnode) {
  std::lock_guard<myMutex> lk(_mutex);
  /* If the node has associated slaves, we have to set
   * all the slaves->slaveof fields to NULL (unknown). */
  for (auto& vs : delnode->_slaves) {
    vs->_slaveOf = nullptr;
  }
  /* Remove this node from the list of slaves of its master. */
  if (delnode->nodeIsSlave() && delnode->_slaveOf) {
    bool s = clusterNodeRemoveSlaveNolock(delnode->_slaveOf, delnode);
    if (!s) {
      LOG(ERROR) << "remove this node from the list of slaves of its "
                    "master FAIL";
    }
  }

  int delNum = _nodes.erase(delnode->getNodeNameNolock());
  if (delNum < 1) {
    LOG(ERROR) << "delete this node from nodelist fail ";
  }
  delnode->freeClusterSession();

  return {ErrorCodes::ERR_OK, ""};
}

void ClusterState::clusterDelNode(CNodePtr node, bool save) {
  std::lock_guard<myMutex> lk(_mutex);
  clusterDelNodeNoLock(node);

  if (save) {
    clusterSaveNodesNoLock();
  }
}

void ClusterState::clusterRenameNode(CNodePtr node,
                                     const std::string& newname,
                                     bool save) {
  std::lock_guard<myMutex> lk(_mutex);
  std::string oldname = node->getNodeName();
  serverLog(LL_DEBUG,
            "Renaming node %.40s into %.40s",
            oldname.c_str(),
            newname.c_str());

  clusterDelNodeNoLock(node);
  node->setNodeName(newname);
  clusterAddNodeNoLock(node);

  if (save) {
    clusterSaveNodesNoLock();
  }
}

CNodePtr ClusterState::getRandomNode() const {
  std::lock_guard<myMutex> lk(_mutex);
  auto rand = std::rand() % _nodes.size();
  auto random_it = std::next(std::begin(_nodes), rand);
  return random_it->second;
}

/* find if node in cluster, */
CNodePtr ClusterState::clusterLookupNodeNoLock(const std::string& name) {
  std::unordered_map<std::string, CNodePtr>::iterator it;

  if ((it = _nodes.find(name)) != _nodes.end()) {
    return it->second;
  } else {
    return nullptr;
  }
}

CNodePtr ClusterState::clusterLookupNode(const std::string& name) {
  std::lock_guard<myMutex> lk(_mutex);
  auto it = clusterLookupNodeNoLock(name);
  return it;
}

const std::unordered_map<std::string, CNodePtr> ClusterState::getNodesList()
  const {
  std::lock_guard<myMutex> lk(_mutex);
  return _nodes;
}

uint32_t ClusterState::getNodeCount() const {
  std::lock_guard<myMutex> lk(_mutex);

  return _nodes.size();
}

bool ClusterState::setMfMasterOffsetIfNecessary(const CNodePtr& node) {
  if (_mfEnd && _myself->nodeIsSlave() && _myself->getMaster() == node &&
      _mfMasterOffset == 0) {
    return true;
  }
  return false;
}

/* Add the specified slot to the list of slots that node 'n' will
 * serve. Return true if the operation ended with success.
 * If the slot is already assigned to another instance this is considered
 * an error and false is returned. */

bool ClusterState::clusterAddSlot(CNodePtr node, const uint32_t slot) {
  std::lock_guard<myMutex> lk(_mutex);
  auto s = clusterAddSlotNoLock(node, slot);
  return s;
}

bool ClusterState::clusterAddSlotNoLock(CNodePtr node, const uint32_t slot) {
  if (_allSlots[slot] != nullptr /* || _allSlots[slot] != node*/) {
    return false;
  } else {
    auto slavesInCluster = clusterMastersHaveSlavesNoLock();
    Status s = node->addSlot(slot, slavesInCluster);
    if (!s.ok()) {
      return false;
    }
    _allSlots[slot] = node;
    DLOG(INFO) << "node:" << node->getNodeName() << "add slot:" << slot
               << "finish";
    return true;
  }
}

// cluster_state add much Zslots
bool ClusterState::clusterSetSlot(CNodePtr n,
                                  const std::bitset<CLUSTER_SLOTS>& bitmap) {
  std::lock_guard<myMutex> lk(_mutex);
  bool result = true;
  size_t idx = 0;
  while (idx < bitmap.size()) {
    if (bitmap.test(idx)) {
      result = clusterAddSlot(n, idx);
      if (result == false)
        return false;
    }
    idx++;
  }
  return result;
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
  std::lock_guard<myMutex> lk(_mutex);

  return clusterDelSlotNoLock(slot);
}

/* Delete all the slots associated with the specified node.
 * The number of deleted slots is returned. */
uint32_t ClusterState::clusterDelNodeSlots(CNodePtr node) {
  std::lock_guard<myMutex> lk(_mutex);
  uint32_t deleted = 0, j;

  for (j = 0; j < CLUSTER_SLOTS; j++) {
    if (node->getSlotBit(j)) {
      clusterDelSlotNoLock(j);
      deleted++;
    }
  }
  return deleted;
}

void ClusterState::addFailVoteNum() {
  std::lock_guard<myMutex> lk(_mutex);
  _failoverAuthCount.fetch_add(1, std::memory_order_relaxed);
}

bool ClusterState::clusterNodeAddFailureReport(CNodePtr failing,
                                               CNodePtr sender) {
  std::lock_guard<myMutex> lk(_mutex);
  for (auto& record : failing->getFailReport()) {
    std::string name = record->getFailNode();
    if (name == sender->getNodeName()) {
      record->setFailTime(msSinceEpoch());
      return false;
    }
  }
  CReportPtr newRecord = std::make_shared<ClusterNodeFailReport>(
    sender->getNodeName(), msSinceEpoch());
  failing->addFailureReport(newRecord);
  return true;
}

void ClusterState::clusterNodeCleanupFailureReports(CNodePtr node) {
  auto nodeTimeout = _server->getParams()->clusterNodeTimeout;
  mstime_t lifeTime = nodeTimeout * CLUSTER_FAIL_REPORT_VALIDITY_MULT;

  node->cleanupFailureReports(lifeTime);
}

bool ClusterState::clusterNodeDelFailureReport(CNodePtr node, CNodePtr sender) {
  std::lock_guard<myMutex> lk(_mutex);
  bool find = false;
  auto list = node->getFailReport();
  for (auto it = list.begin(); it != list.end();) {
    if ((*it)->getFailNode() == sender->getNodeName()) {
      find = true;
      break;
    } else {
      ++it;
    }
  }
  if (!find)
    return false;
  node->delFailureReport(sender->getNodeName());
  clusterNodeCleanupFailureReports(node);
  return find;
}

uint32_t ClusterState::clusterNodeFailureReportsCount(CNodePtr node) {
  std::lock_guard<myMutex> lk(_mutex);
  INVARIANT(node != nullptr);
  clusterNodeCleanupFailureReports(node);
  return node->getFailReport().size();
}

// return true, mean node mark as faling. It need to save nodes;
bool ClusterState::markAsFailingIfNeeded(CNodePtr node) {
  std::lock_guard<myMutex> lk(_mutex);
  {
    uint32_t failures;
    uint32_t needed_quorum = (_size / 2) + 1;

    if (!node->nodeTimedOut())
      return false; /* We can reach it. */
    if (node->nodeFailed())
      return false; /* Already FAILing. */

    failures = clusterNodeFailureReportsCount(node);
    /* Also count myself as a voter if I'm a master. */
    if (_myself->nodeIsMaster())
      failures++;

    if (failures < needed_quorum)
      return false; /* No weak agreement from masters. */

    serverLog(LL_NOTICE,
              "Marking node %.40s as failing (quorum reached). ",
              node->getNodeName().c_str());
    /* Mark the node as failing. */
    node->markAsFailing();
  }
  /* Broadcast the failing node name to everybody, forcing all the other
   * reachable nodes to flag the node as FAIL. */
  clusterSendFail(node, _server->getReplManager()->replicationGetOffset());

  clusterUpdateState();
  clusterSaveNodes();
  return true;
}

void ClusterState::manualFailoverCheckTimeout() {
  bool delFakeTask = false;
  string masterIp = "";
  uint64_t masterPort = 0;
  {
    std::lock_guard<myMutex> lk(_mutex);
    if (_mfEnd && _mfEnd < msSinceEpoch()) {
      serverLog(LL_WARNING, "Manual failover timed out.");
      resetManualFailoverNoLock();

      if (_mfCanStart && _myself->getMaster()) {
        delFakeTask = true;
        masterIp = _myself->getMaster()->getNodeIp();
        masterPort = _myself->getMaster()->getPort();
      }
    }
  }
  if (delFakeTask) {
    _server->getReplManager()->DelFakeFullPushStatus(
            masterIp, masterPort);
  }
}

void ClusterState::resetManualFailover() {
  std::lock_guard<myMutex> lk(_mutex);
  resetManualFailoverNoLock();
}

void ClusterState::resetManualFailoverNoLock() {
  if (isClientBlock()) {
    LOG(INFO) << "reset manual failover, lock should release";
    setClientUnBlock();
    INVARIANT_D(!_isCliBlocked.load(std::memory_order_relaxed));
  }
  _mfEnd = 0;
  _mfCanStart = 0;
  _mfSlave = nullptr;
  _mfMasterOffset = 0;
  _isMfOffsetReceived.store(false, std::memory_order_relaxed);
}

void ClusterState::clusterHandleManualFailover() {
  bool addFakeTask = false;
  uint64_t slaveOffset = 0;
  string masterIp = "";
  uint64_t masterPort = 0;
  {
    std::lock_guard<myMutex> lk(_mutex);
    /* Return ASAP if no manual failover is in progress. */
    if (_mfEnd == 0)
      return;

    /* If mf_can_start is non-zero, the failover was already triggered so the
     * next steps are performed by clusterHandleSlaveFailover(). */
    if (_mfCanStart) {
      LOG(ERROR) << "mf_can_start is non-zero";
      return;
    }

    if (_mfMasterOffset == 0 && !hasReciveOffset()) {
      LOG(ERROR) << "_mfMasterOffset is zero";
      return;
    }

    slaveOffset = _server->getReplManager()->replicationGetOffset();
    LOG(INFO) << "mfMasterOffset:" << _mfMasterOffset
              << " slaveOffset: " << slaveOffset;
    if (_mfMasterOffset <= slaveOffset) {
      /* Our replication offset matches the master replication offset
       * announced after clients were paused. We can start the failover. */
      setMfStart();
      _isMfOffsetReceived.store(false, std::memory_order_relaxed);
      addFakeTask = true;
      masterIp = _myself->getMaster()->getNodeIp();
      masterPort = _myself->getMaster()->getPort();
      serverLog(LL_WARNING,
                "All master replication stream processed, "
                "manual failover can start.");
    }
  }
  if (addFakeTask) {
    // NOTE(takenliu): add fake FullPushStatus,
    //   to protect binlog not be recycled.
    _server->getReplManager()->AddFakeFullPushStatus(
            slaveOffset, masterIp, masterPort);
  }
}

/* This function is responsible to decide if this replica should be migrated
 * to a different (orphaned) master. It is called by the clusterCron() function
 * only if:
 *
 * 1) We are a slave node.
 * 2) It was detected that there is at least one orphaned master in
 *    the cluster.
 * 3) We are a slave of one of the masters with the greatest number of
 *    slaves.
 *
 * This checks are performed by the caller since it requires to iterate
 * the nodes anyway, so we spend time into clusterHandleSlaveMigration()
 * if definitely needed.
 *
 * The fuction is called with a pre-computed max_slaves, that is the max
 * number of working (not in FAIL state) slaves for a single master.
 *
 * Additional conditions for migration are examined inside the function.
 */
void ClusterState::clusterHandleSlaveMigration(uint32_t max_slaves) {
  CNodePtr target = nullptr, candidate = nullptr;
  bool readyMigrate = false;
  bool isMasterFail;
  {
    std::lock_guard<myMutex> lk(_mutex);

    uint32_t j, okslaves = 0;
    auto mymaster = _myself->getMaster();

    /* Step 1: Don't migrate if the cluster state is not ok. */
    if (_state != ClusterHealth::CLUSTER_OK)
      return;

    /* Step 2: Don't migrate if my master will not be left with at least
     *         'migration-barrier' slaves after my migration. */
    if (mymaster == nullptr)
      return;
    for (j = 0; j < mymaster->getSlavesCount(); j++) {
      if (!mymaster->_slaves[j]->nodeFailed() &&
          !mymaster->_slaves[j]->nodeTimedOut()) {
        okslaves++;
      }
    }
    if (okslaves <= _server->getParams()->clusterMigrationBarrier)
      return;

    /* Step 3: Identify a candidate for migration, and check if among the
     * masters with the greatest number of ok slaves, I'm the one with the
     * smallest node ID (the "candidate slave").
     *
     * Note: this means that eventually a replica migration will occurr
     * since slaves that are reachable again always have their FAIL flag
     * cleared, so eventually there must be a candidate. At the same time
     * this does not mean that there are no race conditions possible (two
     * slaves migrating at the same time), but this is unlikely to
     * happen, and harmless when happens. */
    candidate = _myself;
    for (const auto& nodep : _nodes) {
      const auto& node = nodep.second;

      uint32_t okslaves = 0;
      bool is_orphaned = true;

      /* We want to migrate only if this master is working, orphaned, and
       * used to have slaves or if failed over a master that had slaves
       * (MIGRATE_TO flag). This way we only migrate to instances that
       * were supposed to have replicas. */
      if (node->nodeIsSlave() || node->nodeFailed())
        is_orphaned = false;
      if (!(node->getFlags() & CLUSTER_NODE_MIGRATE_TO))
        is_orphaned = false;

      /* Check number of working slaves. */
      if (node->nodeIsMaster())
        okslaves = clusterCountNonFailingSlaves(node);
      if (okslaves > 0)
        is_orphaned = false;

      if (is_orphaned) {
        if (!target && node->getSlotNum() > 0)
          target = node;

        /* Track the starting time of the orphaned condition for this
         * master. */
        if (!node->_orphanedTime)
          node->_orphanedTime = msSinceEpoch();
      } else {
        node->_orphanedTime = 0;
      }

      /* Check if I'm the slave candidate for the migration: attached
       * to a master with the maximum number of slaves and with the
       * smallest node ID. */
      if (okslaves == max_slaves) {
        for (j = 0; j < node->getSlavesCount(); j++) {
          if (node->_slaves[j]->getNodeName() < candidate->getNodeName()) {
            candidate = node->_slaves[j];
          }
        }
      }
    }
    if (target && candidate == _myself &&
        (msSinceEpoch() - target->_orphanedTime) >
          CLUSTER_SLAVE_MIGRATION_DELAY) {
      readyMigrate = true;
    }
    isMasterFail = _myself->getMaster()->nodeFailed() ? true : false;
  }
  /* Step 4: perform the migration if there is a target, and if I'm the
   * candidate, but only if the master is continuously orphaned for a
   * couple of seconds, so that during failovers, we give some time to
   * the natural slaves of this instance to advertise their switch from
   * the old master to the new one. */
  /* NOTE(wayenchen) slave should not full sync when set new master*/
  if (readyMigrate && _server->getReplManager()->isSlaveFullSyncDone()) {
    serverLog(LL_WARNING,
              "Migrating to orphaned master begin %.40s",
              target->getNodeName().c_str());
    Status s;
    if (!isMasterFail) {
      s = _server->getReplManager()->replicationUnSetMaster();
      if (!s.ok()) {
        LOG(ERROR) << "set myself master fail in slave migration";
      }
    }

    LOG(INFO) << "lock OK, begin set new master" << target->getNodeName();
    s = clusterSetMaster(target);
    if (!s.ok()) {
      LOG(ERROR) << "set my master fail in slave migration";
    }
  }
}

uint32_t ClusterState::clusterCountNonFailingSlaves(CNodePtr node) {
  return node->getNonFailingSlavesCount();
}

/* -----------------------------------------------------------------------------
 * SLAVE node specific functions
 * --------------------------------------------------------------------------
 */

/* This function sends a FAILOVE_AUTH_REQUEST message to every node in order
 * to see if there is the quorum for this slave instance to failover its
 * failing master.
 *
 * Note that we send the failover request to everybody, master and slave
 * nodes, but only the masters are supposed to reply to our query. */
void ClusterState::clusterRequestFailoverAuth(void) {
  uint64_t offset = _server->getReplManager()->replicationGetOffset();
  ClusterMsg msg(ClusterMsg::Type::FAILOVER_AUTH_REQUEST,
                 shared_from_this(),
                 _server,
                 offset);
  clusterBroadcastMessage(msg);
}

/* Send a FAILOVER_AUTH_ACK message to the specified node. */
void ClusterState::clusterSendFailoverAuth(CNodePtr node) {
  if (!node->getSession()) {
    return;
  }
  uint64_t offset = _server->getReplManager()->replicationGetOffset();
  ClusterMsg msg(ClusterMsg::Type::FAILOVER_AUTH_ACK,
                 shared_from_this(),
                 _server,
                 offset,
                 node);
  node->getSession()->clusterSendMessage(msg);
}

/* Send a MFSTART message to the specified node. */
void ClusterState::clusterSendMFStart(CNodePtr node, uint64_t offset) {
  if (!node->getSession()) {
    return;
  }
  ClusterMsg msg(
    ClusterMsg::Type::MFSTART, shared_from_this(), _server, offset, node);
  node->getSession()->clusterSendMessage(msg);
}

/* Vote for the node asking for our vote if there are the conditions. */
void ClusterState::clusterSendFailoverAuthIfNeeded(CNodePtr node,
                                                   const ClusterMsg& request) {
  std::lock_guard<myMutex> lk(_mutex);
  auto master = node->getMaster();

  uint64_t requestCurrentEpoch = request.getHeader()->_currentEpoch;
  uint64_t requestConfigEpoch = request.getHeader()->_configEpoch;
  const auto& claimed_slots = request.getHeader()->_slots;
  int force_ack = request.getMflags() & CLUSTERMSG_FLAG0_FORCEACK;
  int j;

  // TODO(vinchen): if there is a slave, we should be able to vote!
  /* IF we are not a master serving at least 1 slot or an arbiter, we don't
   * have the right to vote, as the cluster size in Redis Cluster is the
   * number of masters serving at least one slot, and quorum is the cluster
   * size + 1 */
  if ((_myself->nodeIsSlave() || _myself->getSlotNum() == 0) &&
      !_myself->nodeIsArbiter())
    return;

  uint64_t curretEpoch = getCurrentEpoch();
  /* Request epoch must be >= our currentEpoch.
   * Note that it is impossible for it to actually be greater since
   * our currentEpoch was updated as a side effect of receiving this
   * request, if the request epoch was greater. */
  if (requestCurrentEpoch < curretEpoch) {
    serverLog(LL_WARNING,
              "Failover auth denied to %.40s: reqEpoch (%" PRIu64
              ") < curEpoch(%" PRIu64 ")",
              node->getNodeName().c_str(),
              requestCurrentEpoch,
              curretEpoch);
    return;
  }

  /* I already voted for this epoch? Return ASAP. */
  uint64_t lastVoteEpoch = getLastVoteEpoch();
  if (lastVoteEpoch == curretEpoch) {
    serverLog(LL_WARNING,
              "Failover auth denied to %.40s: already voted for epoch %" PRIu64,
              node->getNodeName().c_str(),
              curretEpoch);
    return;
  }

  /* Node must be a slave and its master down.
   * The master can be non failing if the request is flagged
   * with CLUSTERMSG_FLAG0_FORCEACK (manual failover). */
  if (request.isMaster() || master == nullptr ||
      (!master->nodeFailed() && !force_ack)) {
    if (request.isMaster()) {
      serverLog(LL_WARNING,
                "Failover auth denied to %.40s: it is a master node",
                node->getNodeName().c_str());

    } else if (master == nullptr) {
      serverLog(LL_WARNING,
                "Failover auth denied to %.40s: I don't know its master",
                node->getNodeName().c_str());
    } else if (!master->nodeFailed()) {
      serverLog(LL_WARNING,
                "Failover auth denied to %.40s: its master is up",
                node->getNodeName().c_str());
    }
    return;
  }

  auto nodeTimeout = _server->getParams()->clusterNodeTimeout;
  /* We did not voted for a slave about this master for two
   * times the node timeout. This is not strictly needed for correctness
   * of the algorithm but makes the base case more linear. */

  if (msSinceEpoch() - master->getVoteTime() < nodeTimeout * 2) {
    serverLog(LL_WARNING,
              "Failover auth denied to %.40s: "
              "can't vote about this master before %" PRIu64 " milliseconds",
              node->getNodeName().c_str(),
              ((nodeTimeout * 2) - (msSinceEpoch() - master->getVoteTime())));
    return;
  }

  /* The slave requesting the vote must have a configEpoch for the claimed
   * slots that is >= the one of the masters currently serving the same
   * slots in the current configuration. */
  for (j = 0; j < CLUSTER_SLOTS; j++) {
    if (claimed_slots.test(j) == 0)
      continue;

    if (_allSlots[j] == nullptr ||
        _allSlots[j]->getConfigEpoch() <= requestCurrentEpoch) {
      continue;
    }

    /* If we reached this point we found a slot that in our current slots
     * is served by a master with a greater configEpoch than the one claimed
     * by the slave requesting our vote. Refuse to vote for this slave. */
    serverLog(LL_WARNING,
              "Failover auth denied to %.40s: "
              "slot %d epoch (%" PRIu64 ") > reqEpoch (%" PRIu64 ")",
              node->getNodeName().c_str(),
              j,
              _allSlots[j]->getConfigEpoch(),
              requestConfigEpoch);
    return;
  }

  /* We can vote for this slave. */
  setLastVoteEpoch(curretEpoch);
  master->setVoteTime(msSinceEpoch());
  clusterSaveNodes();

  clusterSendFailoverAuth(node);
  serverLog(LL_WARNING,
            "Failover auth granted to %.40s for epoch %" PRIu64,
            node->getNodeName().c_str(),
            curretEpoch);
}

// This function returns the "rank" of this instance, a slave, in the context
// of its master-slaves ring. The rank of the slave is given by the number of
// other slaves for the same master that have a better replication offset
// compared to the local one (better means, greater, so they claim more data).
//
// A slave with rank 0 is the one with the greatest (most up to date)
// replication offset, and so forth. Note that because how the rank is
// computed multiple slaves may have the same rank, in case they have the same
// offset.
//
// The slave rank is used to add a delay to start an election in order to
// get voted and replace a failing master. Slaves with better replication
// offsets are more likely to win. */
uint32_t ClusterState::clusterGetSlaveRank(void) {
  uint32_t rank = 0;
  auto myoffset = _server->getReplManager()->replicationGetOffset();
  INVARIANT_D(_myself->nodeIsSlave());
  std::lock_guard<myMutex> lk(_mutex);

  auto master = _myself->getMaster();
  if (master == nullptr)
    return 0; /* Never called by slaves without master. */

  auto exptSlaveList = master->getSlaves();
  INVARIANT_D(exptSlaveList.ok());
  auto slaveList = exptSlaveList.value();
  for (uint32_t j = 0; j < slaveList.size(); j++)
    if (slaveList[j] != _myself && !slaveList[j]->nodeCantFailover() &&
        slaveList[j]->_replOffset > myoffset)
      rank++;
  return rank;
}

// This function is called by clusterHandleSlaveFailover() in order to
// let the slave log why it is not able to failover. Sometimes there are
// not the conditions, but since the failover function is called again and
// again, we can't log the same things continuously.
//
// This function works by logging only if a given set of conditions are
// true:
//
// 1) The reason for which the failover can't be initiated changed.
//    The reasons also include a NONE reason we reset the state to
//    when the slave finds that its master is fine (no FAIL flag).
// 2) Also, the log is emitted again if the master is still down and
//    the reason for not failing over is still the same, but more than
//    CLUSTER_CANT_FAILOVER_RELOG_PERIOD seconds elapsed.
// 3) Finally, the function only logs if the slave is down for more than
//    five seconds + NODE_TIMEOUT. This way nothing is logged when a
//    failover starts in a reasonable time.
//
// The function is called with the reason why the slave can't failover
// which is one of the integer macros CLUSTER_CANT_FAILOVER_*.
//
// The function is guaranteed to be called only if 'myself' is a slave. */
void ClusterState::clusterLogCantFailover(int reason) {
  std::lock_guard<myMutex> lk(_mutex);
  std::string msg;

  mstime_t nolog_fail_time = _server->getParams()->clusterNodeTimeout + 5000;

  /* Don't log if we have the same reason for some time. */
  if (reason == _cantFailoverReason &&
      msSinceEpoch() - _lastLogTime <
        CLUSTER_CANT_FAILOVER_RELOG_PERIOD * 10000)
    return;

  _cantFailoverReason = reason;

  /* We also don't emit any log if the master failed no long ago, the
   * goal of this function is to log slaves in a stalled condition for
   * a long time. */
  if (_myself->getMaster() && _myself->getMaster()->nodeFailed() &&
      (msSinceEpoch() - _myself->getMaster()->_failTime) < nolog_fail_time)
    return;

  switch (reason) {
    case CLUSTER_CANT_FAILOVER_DATA_AGE:
      msg =
        "Disconnected from master for longer than allowed. "
        "Please check the 'cluster-slave-validity-factor' "
        "configuration "
        "option.";
      break;
    case CLUSTER_CANT_FAILOVER_WAITING_DELAY:
      msg = "Waiting the delay before I can start a new failover.";
      break;
    case CLUSTER_CANT_FAILOVER_EXPIRED:
      msg = "Failover attempt expired.";
      break;
    case CLUSTER_CANT_FAILOVER_WAITING_VOTES:
      msg = "Waiting for votes, but majority still not reached.";
      break;
    default:
      msg = "Unknown reason code.";
      break;
  }
  _lastLogTime = msSinceEpoch();
  //  serverLog(LL_WARNING, "Currently unable to failover: %s",
  //  (char*)msg.c_str());
}

/* This function implements the final part of automatic and manual failovers,
 * where the slave grabs its master's hash slots, and propagates the new
 * configuration.
 *
 * Note that it's up to the caller to be sure that the node got a new
 * configuration epoch already. */
Status ClusterState::clusterFailoverReplaceYourMasterMeta(void) {
  std::lock_guard<myMutex> lk(_mutex);
  CNodePtr oldmaster = _myself->getMaster();
  if (_myself->nodeIsMaster() || oldmaster == nullptr) {
    return {ErrorCodes::ERR_CLUSTER, "no condition to replace master"};
  }
  /* 1) Turn this node into a master. */
  bool s = clusterSetNodeAsMasterNoLock(_myself);
  if (!s) {
    return {ErrorCodes::ERR_CLUSTER, "set myself as master fail"};
  }

  for (uint32_t j = 0; j < CLUSTER_SLOTS; j++) {
    if (_allSlots[j] == oldmaster) {
      bool s = clusterDelSlot(j);
      if (s) {
        bool result = clusterAddSlotNoLock(_myself, j);
        if (!result) {
          LOG(ERROR) << "setSlots addslot fail on slot:" << j;
          return {ErrorCodes::ERR_CLUSTER, "setslot add new slot fail"};
        }
      } else {
        LOG(ERROR) << "setSlots delslot fail on slot:" << j;
        return {ErrorCodes::ERR_CLUSTER, "setslot delete old slot fail!"};
      }
    }
  }
  return {ErrorCodes::ERR_OK, "replace metadata"};
}

Status ClusterState::clusterFailoverReplaceYourMaster(void) {
  Status s;
  s = _server->getReplManager()->replicationUnSetMaster();
  if (!s.ok()) {
    LOG(ERROR) << "replication unset master fail on node" << s.toString();
    return s;
  }
  LOG(INFO) << "replication unsetMaster success";
  s = clusterFailoverReplaceYourMasterMeta();
  if (!s.ok()) {
    LOG(ERROR) << "replace master meta update fail" << s.toString();
    return s;
  }
  /* Pong all the other nodes so that they can update the state
   *    accordingly and detect that we switched to master role. */
  /* Set the replication offset. */
  uint64_t offset = _server->getReplManager()->replicationGetOffset();
  clusterBroadcastPong(CLUSTER_BROADCAST_ALL, offset);
  /* If there was a manual failover in progress, clear the state. */
  resetManualFailover();


  /* 5) Update state and save config. */
  clusterUpdateState();
  clusterSaveNodes();
  return {ErrorCodes::ERR_OK, "finish replace master"};
}
/* This function is called if we are a slave node and our master serving
 * a non-zero amount of hash slots is in FAIL state.
 *
 * The gaol of this function is:
 * 1) To check if we are able to perform a failover, is our data updated?
 * 2) Try to get elected by masters.
 * 3) Perform the failover informing all the other nodes.
 */
Status ClusterState::clusterHandleSlaveFailover() {
  mstime_t data_age;
  mstime_t now = msSinceEpoch();
  mstime_t auth_age;
  int needed_quorum;
  int manual_failover;
  {
    std::lock_guard<myMutex> lock(_mutex);
    if (now < _failoverAuthTime) {
      auth_age = 0;
    } else {
      auth_age = now - _failoverAuthTime;
    }
    needed_quorum = (_size / 2) + 1;
    manual_failover = _mfEnd != 0 && _mfCanStart;
  }
  mstime_t auth_timeout, auth_retry_time;

  /* Compute the failover timeout (the max time we have to send votes
   * and wait for replies), and the failover retry time (the time to wait
   * before trying to get voted again).
   *
   * Timeout is MAX(NODE_TIMEOUT*2,2000) milliseconds.
   * Retry is two times the Timeout.
   */
  auto nodeTimeout = _server->getParams()->clusterNodeTimeout;
  auth_timeout = nodeTimeout * 2;
  if (auth_timeout < 2000)
    auth_timeout = 2000;
  auth_retry_time = auth_timeout * 2;
  /* Pre conditions to run the function, that must be met both in case
   * of an automatic or manual failover:
   * 1) We are a slave.
   * 2) Our master is flagged as FAIL, or this is a manual failover.
   * 3) We don't have the no failover configuration set, and this is
   *    not a manual failover.
   * 4) It is serving slots. */
  if (isMyselfMaster() || getMyMaster() == nullptr ||
      (!getMyMaster()->nodeFailed() && !manual_failover) ||
      (_server->getParams()->clusterSlaveNoFailover) ||
      getMyMaster()->getSlotNum() == 0) {
    /* There are no reasons to failover, so we set the reason why we
     * are returning without failing over to NONE. */
    _cantFailoverReason = CLUSTER_CANT_FAILOVER_NONE;
    return {ErrorCodes::ERR_CLUSTER, "err pre condition"};
  }

  /* Set data_age to the number of seconds we are disconnected from
   * the master. */
  auto replMgr = _server->getReplManager();
  uint64_t syncTime = replMgr->getLastSyncTime();
  data_age = msSinceEpoch() - syncTime;
  INVARIANT_D(data_age > 0);

  /* Remove the node timeout from the data age as it is fine that we are
   * disconnected from our master at least for the time it was down to be
   * flagged as FAIL, that's the baseline. */
  if (data_age > nodeTimeout)
    data_age -= nodeTimeout;

  /* Check if our data is recent enough according to the slave validity
   * factor configured by the user.
   *
   * Check bypassed for manual failovers. */
  auto slavefactor = _server->getParams()->clusterSlaveValidityFactor;
  mstime_t limitTime =
    ((mstime_t)gBinlogHeartbeatSecs * 1000) + nodeTimeout * slavefactor;
  if (slavefactor && data_age > limitTime) {
    if (!manual_failover) {
      _isVoteFailByDataAge.store(true, std::memory_order_relaxed);
      clusterLogCantFailover(CLUSTER_CANT_FAILOVER_DATA_AGE);
      LOG(ERROR) << "vote fail, data age to large:" << data_age
                 << " limtTime is:" << limitTime;
      return {ErrorCodes::ERR_CLUSTER, "data age to big"};
    }
  }

  /* If the previous failover attempt timedout and the retry time has
   * elapsed, we can setup a new one. */
  if (auth_age > auth_retry_time) {
    // TODO(wayenchen) add API to get and set _failoverAuth** params
    auto delayTime = msSinceEpoch() +
      500 + /* Fixed delay of 500 milliseconds, let FAIL msg propagate. */
      redis_port::random() %
        500; /* Random delay between 0 and 500 milliseconds. */
    setFailAuthTime(delayTime);
    setFailAuthCount(0);
    setFailAuthSent(0);
    setFailAuthRank(clusterGetSlaveRank());

    /* We add another delay that is proportional to the slave rank.
     * Specifically 1 second * rank. This way slaves that have a probably
     * less updated replication offset, are penalized. */
    addFailAuthTime(static_cast<uint64_t>(getFailAuthRank()) * 1000ULL);

    /* However if this is a manual failover, no delay is needed. */
    if (getMfEnd()) {
      setFailAuthTime(msSinceEpoch());
      setFailAuthRank(0);
    }
    serverLog(LL_WARNING,
              "Start of election delayed for %" PRId64
              " milliseconds "
              "(rank #%d, offset %" PRIu64 ").",
              (_failoverAuthTime - msSinceEpoch()),
              getFailAuthRank(),
              _server->getReplManager()->replicationGetOffset());

    /* Now that we have a scheduled election, broadcast our offset
     * to all the other slaves so that they'll updated their offsets
     * if our offset is better. */
    uint64_t offset = _server->getReplManager()->replicationGetOffset();
    clusterBroadcastPong(CLUSTER_BROADCAST_LOCAL_SLAVES, offset);
    return {ErrorCodes::ERR_CLUSTER, "delat election"};
  }

  /* It is possible that we received more updated offsets from other
   * slaves for the same master since we computed our election delay.
   * Update the delay if our rank changed.
   *
   * Not performed if this is a manual failover. */
  if (getFailAuthSent() == 0 && getMfEnd() == 0) {
    auto newrank = clusterGetSlaveRank();
    if (newrank > getFailAuthRank()) {
      int64_t added_delay = (newrank - getFailAuthRank()) * 1000ULL;
      addFailAuthTime(added_delay);
      setFailAuthRank(newrank);
      serverLog(LL_WARNING,
                "Slave rank updated to #%d, added %" PRId64
                " milliseconds of delay.",
                newrank,
                added_delay);
    }
  }

  /* Return ASAP if we can't still start the election. */
  if (msSinceEpoch() < getFailAuthTime()) {
    clusterLogCantFailover(CLUSTER_CANT_FAILOVER_WAITING_DELAY);
    return {ErrorCodes::ERR_CLUSTER, ""};
  }

  /* Return ASAP if the election is too old to be valid. */
  if (auth_age > auth_timeout) {
    clusterLogCantFailover(CLUSTER_CANT_FAILOVER_EXPIRED);
    return {ErrorCodes::ERR_CLUSTER, ""};
  }

  /* Ask for votes if needed. */
  if (getFailAuthSent() == 0) {
    incrCurrentEpoch();
    auto epoch = getCurrentEpoch();
    setFailAuthEpoch(epoch);

    serverLog(LL_WARNING,
              "Starting a failover election for epoch %" PRIu64 ".",
              _currentEpoch);

    clusterRequestFailoverAuth();
    setFailAuthSent(1);
    clusterSaveNodes();
    clusterUpdateState();

    return {ErrorCodes::ERR_CLUSTER, "wait for replies"};
  }

  /* Check if we reached the quorum. */
  if (getFailAuthCount() >= needed_quorum) {
    /* We have the quorum, we can finally failover the master. */
    serverLog(LL_WARNING, "Failover election won: I'm the new master.");
    /* Update my configEpoch to the epoch of the election. */
    if (_myself->getConfigEpoch() < getFailAuthEpoch()) {
      _myself->setConfigEpoch(getFailAuthEpoch());
      serverLog(LL_WARNING,
                "configEpoch set to %" PRIu64 " after successful failover",
                _myself->getConfigEpoch());
    }

    return {ErrorCodes::ERR_OK, ""};
  } else {
    clusterLogCantFailover(CLUSTER_CANT_FAILOVER_WAITING_VOTES);
    return {ErrorCodes::ERR_CLUSTER, ""};
  }
}


/* Clear the migrating / importing state for all the slots.
 * This is useful at initialization and when turning a master into slave. */
void ClusterState::clusterCloseAllSlots() {
  std::lock_guard<myMutex> lk(_mutex);
  /* NOTE(wayenchen) clear migrate state by stop all migrate task here,
   * do not use cluster reset command when migrating */
  auto migrateMgr = _server->getMigrateManager();
  if (migrateMgr->existMigrateTask()) {
    migrateMgr->stopAllTasks(false);
  }
}


uint64_t ClusterState::clusterGetOrUpdateMaxEpoch(bool update) {
  std::lock_guard<myMutex> lk(_mutex);
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
                       const std::shared_ptr<ServerEntry> svr,
                       uint64_t offset,
                       CNodePtr node)
  : _sig("RCmb"),
    _totlen(-1),
    _type(type),
    _mflags(0),
    _header(std::make_shared<ClusterMsgHeader>(cstate, svr, offset)),
    _msgData(nullptr) {
  if (cstate->getMyselfNode()->nodeIsMaster() && cstate->getMfEnd()) {
    _mflags |= CLUSTERMSG_FLAG0_PAUSED;
  }

  switch (type) {
    case Type::MEET:
    case Type::PING:
    case Type::PONG:
      _msgData = std::move(std::make_shared<ClusterMsgDataGossip>());
      break;
    case Type::FAIL:
      INVARIANT_D(node != nullptr);
      _msgData =
        std::move(std::make_shared<ClusterMsgDataFail>(node->getNodeName()));
      break;
    case Type::UPDATE:
      INVARIANT_D(node != nullptr);
      _msgData = std::move(std::make_shared<ClusterMsgDataUpdate>(
        node->getConfigEpoch(), node->getNodeName(), node->getSlots()));
      break;
    case Type::FAILOVER_AUTH_ACK:
    case Type::MFSTART:
      if (cstate->getMyselfNode()->nodeIsMaster() && cstate->getMfEnd()) {
        _mflags |= CLUSTERMSG_FLAG0_PAUSED;
      }
      break;
    case Type::FAILOVER_AUTH_REQUEST:
      // If this is a manual failover, set the CLUSTERMSG_FLAG0_FORCEACK
      // bit in the header to communicate the nodes receiving the message
      // that they should authorized the failover even if the master is
      // working.
      if (cstate->getMfEnd()) {
        _mflags |= CLUSTERMSG_FLAG0_FORCEACK;
      }
      break;
    default:
      INVARIANT(0);
      break;
  }
}

ClusterMsg::ClusterMsg(const std::string& sig,
                       const uint32_t totlen,
                       const ClusterMsg::Type type,
                       const uint32_t mflags,
                       const std::shared_ptr<ClusterMsgHeader>& header,
                       const std::shared_ptr<ClusterMsgData>& data)
  : _sig(sig),
    _totlen(totlen),
    _type(type),
    _mflags(mflags),
    _header(header),
    _msgData(data) {}

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

/**
 *
 * @param info
 * @return
 */
auto ClusterNode::parseClusterNodesInfo(const std::string& info)
  -> Expected<std::bitset<CLUSTER_SLOTS>> {
  std::bitset<CLUSTER_SLOTS> slots;
  auto slotsMeta = tendisplus::stringSplit(info, ",");

  for (const auto& v : slotsMeta) {
    auto slotPair = tendisplus::stringSplit(v, "-");
    if (slotPair.size() == 1) {
      auto slot = tendisplus::stol(slotPair[0]);
      RET_IF_ERR_EXPECTED(slot);
      if (slot.value() < 0) {
        return {ErrorCodes::ERR_PARSEOPT, "Wrong Slots"};
      }
      // set single bit as true
      slots.set(slot.value());
    } else {
      auto slotStart = tendisplus::stol(slotPair[0]);
      auto slotEnd = tendisplus::stol(slotPair[1]);
      RET_IF_ERR_EXPECTED(slotStart);
      if (slotStart.value() < 0 || slotStart.value() >= CLUSTER_SLOTS) {
        return {ErrorCodes::ERR_PARSEOPT, "Wrong Slots"};
      }
      RET_IF_ERR_EXPECTED(slotEnd);
      if (slotEnd.value() < 0 || slotEnd.value() >= CLUSTER_SLOTS) {
        return {ErrorCodes::ERR_PARSEOPT, "Wrong Slots"};
      }

      // set bit as true from start to end
      for (int i = slotStart.value(); i <= slotEnd.value(); ++i) {
        slots.set(i);
      }
    }
  }

  return slots;
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

void ClusterMsg::setEntryCount(uint16_t count) {
  _header->_count = count;
}

uint16_t ClusterMsg::getEntryCount() const {
  INVARIANT_D(_header != nullptr);
  return _header->_count;
}

bool ClusterMsg::isMaster() const {
  INVARIANT_D(_header != nullptr);
  return _header->_slaveOf == ClusterMsgHeader::CLUSTER_NODE_NULL_NAME;
}

void ClusterMsg::setTotlen(uint32_t totlen) {
  _totlen = totlen;
}

std::string ClusterMsg::msgEncode() {  // NOLINT
  std::vector<uint8_t> key;

  std::string data = "";
  if (_msgData != nullptr) {
    data = _msgData->dataEncode();
  } else {
    INVARIANT_D(_type == ClusterMsg::Type::FAILOVER_AUTH_ACK ||
                _type == ClusterMsg::Type::FAILOVER_AUTH_REQUEST ||
                _type == ClusterMsg::Type::MFSTART);
  }

  std::string head = _header->headEncode();

  key.insert(key.end(), _sig.begin(), _sig.end());

  size_t sigLen =
    _sig.length() + sizeof(_totlen) + sizeof(_type) + sizeof(_mflags);
  setTotlen(static_cast<uint32_t>(head.size() + data.size() + sigLen));

  CopyUint(&key, _totlen);
  CopyUint(&key, (uint16_t)_type);
  CopyUint(&key, (uint32_t)_mflags);

  key.insert(key.end(), head.begin(), head.end());

  key.insert(key.end(), data.begin(), data.end());
  INVARIANT_D(_totlen == key.size());

  return std::string(reinterpret_cast<const char*>(key.data()), key.size());
}

Expected<ClusterMsg> ClusterMsg::msgDecode(const std::string& key) {
  std::size_t offset = 0;
  std::string sig(key.c_str() + offset, 4);
  if (sig != "RCmb") {
    return {ErrorCodes::ERR_DECODE, "invalid cluster message header"};
  }
  offset += 4;
  auto decode = [&](auto func) {
    auto n = func(key.c_str() + offset);
    offset += sizeof(n);
    return n;
  };
  auto totlen = decode(int32Decode);
  auto ptype = decode(int16Decode);
  auto mflags = decode(int32Decode);

  if (ptype >= CLUSTERMSG_TYPE_COUNT) {
    return {ErrorCodes::ERR_DECODE,
            "invalid cluster message type" + std::to_string(ptype)};
  }
  auto type = (ClusterMsg::Type)(ptype);

  auto keyStr = key.substr(offset, key.size() - offset);
  auto headDecode = ClusterMsgHeader::headDecode(keyStr);
  if (!headDecode.ok()) {
    return headDecode.status();
  }
  auto header = headDecode.value();
  auto headLen = header.getHeaderSize();

  auto headerPtr = std::make_shared<ClusterMsgHeader>(std::move(header));

  auto msgStr = key.substr(headLen + offset, key.size() - headLen - offset);

  std::shared_ptr<ClusterMsgData> msgDataPtr = nullptr;

  if (type == Type::PING || type == Type::PONG || type == Type::MEET) {
    auto count = headerPtr->_count;
    auto msgGData = ClusterMsgDataGossip::dataDecode(msgStr, count);
    if (!msgGData.ok()) {
      return msgGData.status();
    }
    msgDataPtr =
      std::make_shared<ClusterMsgDataGossip>(std::move(msgGData.value()));

  } else if (type == Type::UPDATE) {
    auto msgUData = ClusterMsgDataUpdate::dataDecode(msgStr);
    if (!msgUData.ok()) {
      return msgUData.status();
    }
    msgDataPtr =
      std::make_shared<ClusterMsgDataUpdate>(std::move(msgUData.value()));
  } else if (type == Type::FAIL) {
    auto msgFdata = ClusterMsgDataFail::dataDecode(msgStr);
    if (!msgFdata.ok()) {
      return msgFdata.status();
    }
    msgDataPtr =
      std::make_shared<ClusterMsgDataFail>(std::move(msgFdata.value()));
  } else if (type == Type::FAILOVER_AUTH_ACK ||
             type == Type::FAILOVER_AUTH_REQUEST || type == Type::MFSTART) {
    msgDataPtr = nullptr;
  } else {
    // TODO(wayenchen)
    // server.cluster->stats_bus_messages_received[type]++;
    return {ErrorCodes::ERR_CLUSTER, "decode error type"};
  }

  return ClusterMsg(sig, totlen, type, mflags, headerPtr, msgDataPtr);
}


ClusterMsgHeader::ClusterMsgHeader(const std::shared_ptr<ClusterState> cstate,
                                   const std::shared_ptr<ServerEntry> svr,
                                   uint64_t offset)
  : _ver(ClusterMsg::CLUSTER_PROTO_VER),
    _count(0),
    _currentEpoch(cstate->getCurrentEpoch()),
    _offset(offset),
    _slaveOf(CLUSTER_NODE_NULL_NAME),
    _state(cstate->_state) {
  auto myself = cstate->getMyselfNode();
  /* If this node is a master, we send its slots bitmap and configEpoch.
   * If this node is a slave we send the master's information instead (the
   * node is flagged as slave so the receiver knows that it is NOT really
   * in charge for this slots. */
  auto master =
    (myself->nodeIsSlave() && myself->_slaveOf) ? myself->_slaveOf : myself;

  _sender = myself->getNodeName();

  std::shared_ptr<ServerParams> params = svr->getParams();
  /* Node ip should not use bind ip if not use domain */
  if (svr->getParams()->domainEnabled) {
    _myIp = params->bindIp;
  } else {
    _myIp = "";
  }

  _port = params->port;
  _cport = _port + CLUSTER_PORT_INCR;

  // TODO(wayenchen)
  /* Handle cluster-announce-port/cluster-announce-ip as well. */

  _slots = master->getSlots();
  if (myself->getMaster()) {
    _slaveOf = myself->_slaveOf->getNodeName();
  }
  INVARIANT_D(_slaveOf.size() == 40);

  _flags = myself->getFlags();
  _configEpoch = master->getConfigEpoch();
}

ClusterMsgHeader::ClusterMsgHeader(const uint16_t port,
                                   const uint16_t count,
                                   const uint64_t currentEpoch,
                                   const uint64_t configEpoch,
                                   const uint64_t offset,
                                   const std::string& sender,
                                   const std::bitset<CLUSTER_SLOTS>& slots,
                                   const std::string& slaveOf,
                                   const std::string& myIp,
                                   const uint16_t cport,
                                   const uint16_t flags,
                                   ClusterHealth state)
  : _ver(ClusterMsg::CLUSTER_PROTO_VER),
    _port(port),
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
    _state(state) {}


ClusterMsgHeader::ClusterMsgHeader(ClusterMsgHeader&& o)
  : _ver(std::move(o._ver)),
    _port(std::move(o._port)),
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
    _state(std::move(o._state)) {}

size_t ClusterMsgHeader::getHeaderSize() const {
  return fixedSize() + lenStrEncodeSize(_myIp) + bitsetEncodeSize(_slots);
}

size_t ClusterMsgHeader::fixedSize() {
  size_t strLen = CLUSTER_NAME_LENGTH * 2;

  size_t intLen = sizeof(_ver) + sizeof(_port) + sizeof(_count) +
    sizeof(_currentEpoch) + sizeof(_configEpoch) + sizeof(_offset) +
    sizeof(_cport) + sizeof(_flags) + 1;

  return strLen + intLen;
}

std::string ClusterMsgHeader::headEncode() const {
  std::vector<uint8_t> key;
  key.reserve(ClusterMsgHeader::fixedSize());

  CopyUint(&key, _ver);
  CopyUint(&key, _port);
  CopyUint(&key, _count);
  CopyUint(&key, _currentEpoch);
  CopyUint(&key, _configEpoch);
  CopyUint(&key, _offset);

  INVARIANT_D(_sender.size() == CLUSTER_NAME_LENGTH);
  key.insert(key.end(), _sender.begin(), _sender.end());
  //  slaveOf
  INVARIANT_D(_slaveOf.size() == CLUSTER_NAME_LENGTH);
  key.insert(key.end(), _slaveOf.begin(), _slaveOf.end());

  CopyUint(&key, _cport);
  CopyUint(&key, _flags);

  uint8_t state = (_state == ClusterHealth::CLUSTER_FAIL) ? 0 : 1;
  CopyUint(&key, state);

  auto ipEnc = lenStrEncode(_myIp);
  key.insert(key.end(), ipEnc.begin(), ipEnc.end());

  auto slotStr = bitsetEncode(_slots);
  key.insert(key.end(), slotStr.begin(), slotStr.end());

  return std::string(reinterpret_cast<const char*>(key.data()), key.size());
}

Expected<ClusterMsgHeader> ClusterMsgHeader::headDecode(
  const std::string& key) {
  size_t offset = 0;
  auto decode = [&](auto func) {
    auto n = func(key.c_str() + offset);
    offset += sizeof(n);
    INVARIANT_D(offset <= key.size());
    return n;
  };

  size_t minHeaderSize = ClusterMsgHeader::fixedSize();
  if (key.size() < minHeaderSize) {
    return {ErrorCodes::ERR_DECODE, "decode head length less than minsize"};
  }
  auto ver = decode(int16Decode);
  if (ver != ClusterMsg::CLUSTER_PROTO_VER) {
    return {ErrorCodes::ERR_DECODE,
            "Can't handle messages of different versions."};
  }
  auto port = decode(int16Decode);

  auto count = decode(int16Decode);
  auto currentEpoch = decode(int64Decode);
  auto configEpoch = decode(int64Decode);
  auto headOffset = decode(int64Decode);

  std::string sender(key.c_str() + offset, CLUSTER_NAME_LENGTH);
  offset += CLUSTER_NAME_LENGTH;

  std::string slaveOf(key.c_str() + offset, CLUSTER_NAME_LENGTH);
  offset += CLUSTER_NAME_LENGTH;

  uint16_t cport = decode(int16Decode);
  uint16_t flags = decode(int16Decode);

  uint8_t state = static_cast<uint8_t>(*(key.begin() + offset));
  offset += 1;

  auto eIpLen = lenStrDecode(key.c_str() + offset, key.size() - offset);
  if (!eIpLen.ok()) {
    return {ErrorCodes::ERR_DECODE, "Invalid Ip"};
  }
  auto myIp = eIpLen.value().first;
  offset += eIpLen.value().second;

  auto st =
    bitsetDecode<CLUSTER_SLOTS>(key.c_str() + offset, key.size() - offset);
  if (!st.ok()) {
    LOG(ERROR) << "header bitset decode error";
    return st.status();
  }
  std::bitset<CLUSTER_SLOTS> slots = std::move(st.value());

  return ClusterMsgHeader(port,
                          count,
                          currentEpoch,
                          configEpoch,
                          headOffset,
                          sender,
                          slots,
                          slaveOf,
                          myIp,
                          cport,
                          flags,
                          int2ClusterHealth(state));
}


ClusterMsgDataUpdate::ClusterMsgDataUpdate(
  const std::shared_ptr<ClusterNode> cnode)
  : ClusterMsgData(ClusterMsgData::Type::Update),
    _configEpoch(cnode->getConfigEpoch()),
    _nodeName(cnode->getNodeName()),
    _slots(cnode->getSlots()) {}

ClusterMsgDataUpdate::ClusterMsgDataUpdate()
  : ClusterMsgData(ClusterMsgData::Type::Update),
    _configEpoch(0),
    _nodeName() {}

ClusterMsgDataUpdate::ClusterMsgDataUpdate(
  const uint64_t configEpoch,
  const std::string& nodeName,
  const std::bitset<CLUSTER_SLOTS>& slots)
  : ClusterMsgData(ClusterMsgData::Type::Update),
    _configEpoch(configEpoch),
    _nodeName(nodeName),
    _slots(slots) {}

size_t ClusterMsgDataUpdate::fixedSize() {
  size_t updateLen = sizeof(_configEpoch) + CLUSTER_NAME_LENGTH;
  return updateLen;
}
std::string ClusterMsgDataUpdate::dataEncode() const {
  std::vector<uint8_t> key;
  auto slotStr = bitsetEncode(_slots);
  key.reserve(fixedSize() + slotStr.size());
  //  _configEpoch
  CopyUint(&key, _configEpoch);
  //  _nodeName
  INVARIANT_D(_nodeName.size() == CLUSTER_NAME_LENGTH);
  key.insert(key.end(), _nodeName.begin(), _nodeName.end());
  // _slots
  key.insert(key.end(), slotStr.begin(), slotStr.end());

  return std::string(reinterpret_cast<const char*>(key.data()), key.size());
}

Expected<ClusterMsgDataUpdate> ClusterMsgDataUpdate::dataDecode(
  const std::string& key) {
  size_t minUpdateSize = ClusterMsgDataUpdate::fixedSize();
  if (key.size() < minUpdateSize) {
    return {ErrorCodes::ERR_DECODE, "decode update length less than minsize"};
  }
  size_t offset = 0;
  auto decode = [&](auto func) {
    auto n = func(key.c_str() + offset);
    offset += sizeof(n);
    return n;
  };

  auto configEpoch = decode(int64Decode);

  std::string nodeName(key.c_str() + offset, CLUSTER_NAME_LENGTH);
  offset += CLUSTER_NAME_LENGTH;

  auto st =
    bitsetDecode<CLUSTER_SLOTS>(key.c_str() + offset, key.size() - offset);
  if (!st.ok()) {
    LOG(INFO) << "update message bitset decode error ";
    return st.status();
  }
  auto slots = std::move(st.value());
  return ClusterMsgDataUpdate(configEpoch, nodeName, slots);
}

ClusterMsgDataGossip::ClusterMsgDataGossip()
  : ClusterMsgData(ClusterMsgData::Type::Gossip) {}

ClusterMsgDataGossip::ClusterMsgDataGossip(
  std::vector<ClusterGossip>&& gossipMsg)
  : ClusterMsgData(ClusterMsgData::Type::Gossip),
    _gossipMsg(std::move(gossipMsg)) {}

bool ClusterMsgDataGossip::clusterNodeIsInGossipSection(
  const CNodePtr& node) const {
  for (auto& n : _gossipMsg) {
    if (n._gossipName == node->getNodeName())
      return true;
  }
  return false;
}
void ClusterMsgDataGossip::addGossipEntry(const CNodePtr& node) {
  ClusterGossip gossip(node->getNodeName(),
                       node->_pingSent / 1000,
                       node->_pongReceived / 1000,
                       node->getNodeIp(),
                       node->getPort(),
                       node->getCport(),
                       node->getFlags());

  _gossipMsg.emplace_back(std::move(gossip));
}

std::string ClusterMsgDataGossip::dataEncode() const {
  std::vector<uint8_t> key;

  for (auto& ax : _gossipMsg) {
    std::string content = ax.gossipEncode();
    key.insert(key.end(), content.begin(), content.end());
  }
  return std::string(reinterpret_cast<const char*>(key.data()), key.size());
}

Expected<ClusterMsgDataGossip> ClusterMsgDataGossip::dataDecode(
  const std::string& key, uint16_t count) {
  const size_t minSize = ClusterGossip::fixedSize();
  if (key.size() < count * minSize) {
    return {ErrorCodes::ERR_DECODE, "too small gossip data keylen"};
  }
  std::vector<ClusterGossip> gossipMsg;

  auto it = key.begin();
  for (; it < key.end();) {
    auto gMsg = ClusterGossip::gossipDecode(&(*it), key.end() - it);
    if (!gMsg.ok()) {
      INVARIANT_D(0);
      return gMsg.status();
    }
    auto gossipSize = gMsg.value().getGossipSize();
    gossipMsg.emplace_back(std::move(gMsg.value()));
    it += gossipSize;
  }
  INVARIANT_D(it == key.end());
  if (it != key.end()) {
    return {ErrorCodes::ERR_DECODE, "invalid gossip data keylen"};
  }

  return ClusterMsgDataGossip(std::move(gossipMsg));
}

Status ClusterMsgDataGossip::addGossipMsg(const ClusterGossip& msg) {
  _gossipMsg.push_back(std::move(msg));
  INVARIANT(_gossipMsg.size() > 0);
  return {ErrorCodes::ERR_OK, ""};
}

std::string ClusterMsgDataFail::dataEncode() const {
  return _nodeName;
}

Expected<ClusterMsgDataFail> ClusterMsgDataFail::dataDecode(
  const std::string& msg) {
  // TODO(vinchen): do some check here
  return ClusterMsgDataFail(msg);
}

ClusterGossip::ClusterGossip(const std::shared_ptr<ClusterNode> node)
  : _gossipName(node->getNodeName()),
    _pingSent(node->_pingSent / 1000),
    _pongReceived(node->_pongReceived / 1000),
    _gossipIp(node->getNodeIp()),
    _gossipPort(node->getPort()),
    _gossipCport(node->getCport()),
    _gossipFlags(node->_flags) {}

ClusterGossip::ClusterGossip(const std::string& gossipName,
                             const uint64_t pingSent,
                             const uint64_t pongReceived,
                             const std::string& gossipIp,
                             const uint16_t gossipPort,
                             const uint16_t gossipCport,
                             uint16_t gossipFlags)
  : _gossipName(gossipName),
    _pingSent(pingSent),
    _pongReceived(pongReceived),
    _gossipIp(gossipIp),
    _gossipPort(gossipPort),
    _gossipCport(gossipCport),
    _gossipFlags(gossipFlags) {}

size_t ClusterGossip::getGossipSize() const {
  return fixedSize() + lenStrEncodeSize(_gossipIp);
}

size_t ClusterGossip::fixedSize() {
  size_t strLen = CLUSTER_NAME_LENGTH;
  size_t intLen = sizeof(_pingSent) + sizeof(_pongReceived) +
    sizeof(_gossipPort) + sizeof(_gossipCport) + sizeof(_gossipFlags);
  return strLen + intLen;
}

std::string ClusterGossip::gossipEncode() const {
  std::vector<uint8_t> key;
  key.reserve(ClusterGossip::getGossipSize());

  //  _gossipNodeName
  INVARIANT_D(_gossipName.size() == CLUSTER_NAME_LENGTH);
  key.insert(key.end(), _gossipName.begin(), _gossipName.end());

  CopyUint(&key, _pingSent);
  CopyUint(&key, _pongReceived);
  CopyUint(&key, _gossipPort);
  CopyUint(&key, _gossipCport);
  CopyUint(&key, _gossipFlags);

  auto ipLen = lenStrEncode(_gossipIp);
  key.insert(key.end(), ipLen.begin(), ipLen.end());

  return std::string(reinterpret_cast<const char*>(key.data()), key.size());
}

Expected<ClusterGossip> ClusterGossip::gossipDecode(const char* key,
                                                    size_t size) {
  const size_t minSize = ClusterGossip::fixedSize();
  if (size < minSize) {
    return {ErrorCodes::ERR_DECODE, "invalid gossip keylen"};
  }
  size_t offset = 0;
  auto decode = [&](auto func) {
    auto n = func(key + offset);
    offset += sizeof(n);
    INVARIANT_D(offset <= size);
    return n;
  };

  std::string gossipName(key + offset, CLUSTER_NAME_LENGTH);
  offset += CLUSTER_NAME_LENGTH;

  auto pingSent = decode(int64Decode);
  auto pongReceived = decode(int64Decode);
  auto gossipPort = decode(int16Decode);
  auto gossipCport = decode(int16Decode);
  auto gossipFlags = decode(int16Decode);

  auto eIpLen = lenStrDecode(key + offset, size - offset);
  if (!eIpLen.ok()) {
    return {ErrorCodes::ERR_DECODE, "invalid gossip ip"};
  }
  auto gossipIp = eIpLen.value().first;
  offset += eIpLen.value().second;

  return ClusterGossip(gossipName,
                       pingSent,
                       pongReceived,
                       gossipIp,
                       gossipPort,
                       gossipCport,
                       gossipFlags);
}

ClusterManager::ClusterManager(const std::shared_ptr<ServerEntry>& svr,
                               const std::shared_ptr<ClusterNode>& node,
                               const std::shared_ptr<ClusterState>& state)
  : _svr(svr),
    _isRunning(false),
    _clusterNode(node),
    _clusterState(state),
    _clusterNetwork(nullptr),
    _megPoolSize(0),
    _netMatrix(std::make_shared<NetworkMatrix>()),
    _reqMatrix(std::make_shared<RequestMatrix>()) {}

ClusterManager::ClusterManager(const std::shared_ptr<ServerEntry>& svr)
  : _svr(svr),
    _isRunning(false),
    _clusterNode(nullptr),
    _clusterState(nullptr),
    _clusterNetwork(nullptr),
    _megPoolSize(0),
    _netMatrix(std::make_shared<NetworkMatrix>()),
    _reqMatrix(std::make_shared<RequestMatrix>()) {}

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

bool ClusterManager::isRunning() const {
  return _isRunning.load(std::memory_order_relaxed);
}

Status ClusterManager::clusterDelNodeMeta(const std::string& nodeName) {
  Catalog* catalog = _svr->getCatalog();
  INVARIANT(catalog != nullptr);
  Status s = catalog->delClusterMeta(nodeName);
  if (!s.ok()) {
    LOG(ERROR) << "delete node:" << nodeName << "meta data fail";
  }
  return s;
}

Status ClusterManager::clusterDelNodesMeta() {
  Status s;
  Catalog* catalog = _svr->getCatalog();
  INVARIANT(catalog != nullptr);
  auto vs = catalog->getAllClusterMeta();
  if (!vs.ok()) {
    LOG(ERROR) << "getAllClusterMeta error";
    return vs.status();
  } else if (vs.value().size() > 0) {
    int vssize = vs.value().size();
    INVARIANT(vssize > 0);
    for (auto& nodeMeta : vs.value()) {
      auto flag = nodeMeta->nodeFlag;
      if (flag & CLUSTER_NODE_MYSELF) {
        continue;
      }
      s = catalog->delClusterMeta(nodeMeta->nodeName);
      if (!s.ok()) {
        return s;
      }
    }
  }
  return {ErrorCodes::ERR_OK, ""};
}


Status ClusterManager::clusterReset(uint16_t hard) {
  /* Turn into master. */
  Status s;
  CNodePtr myself;
  std::shared_ptr<Catalog> catalog;
  bool isSlave = false;
  {
    std::lock_guard<mutex> lk(_mutex);
    myself = _clusterState->getMyselfNode();
    if (myself->nodeIsSlave()) {
      _clusterState->clusterSetNodeAsMaster(myself);
      isSlave = true;
    }
    myself->_flags &= ~CLUSTER_NODE_ARBITER;
  }
  if (isSlave) {
    s = _svr->getReplManager()->replicationUnSetMaster();
    if (!s.ok()) {
      LOG(ERROR) << "set myself master fail when cluster reset";
    }
    return s;
  }
  std::lock_guard<mutex> lk(_mutex);
  /* Close slots, reset manual failover state. */
  _clusterState->clusterCloseAllSlots();
  _clusterState->resetManualFailover();
  _clusterState->clusterDelNodeSlots(myself);

  /* Forget all the nodes, but myself. */
  s = _clusterState->forgetNodes();
  if (!s.ok()) {
    LOG(ERROR) << "forget nodes fail" << s.toString();
    return s;
  }
  /* Delete all the nodes meta, but myself. */
  s = clusterDelNodesMeta();
  if (!s.ok()) {
    LOG(ERROR) << "delete nodes meta fail when reset";
    return s;
  }

  if (hard) {
    _clusterState->setCurrentEpoch(0);
    _clusterState->setLastVoteEpoch(0);
    myself->setConfigEpoch(0);
    serverLog(LL_WARNING, "configEpoch set to 0 via CLUSTER RESET HARD");

    std::string oldname = myself->getNodeName();
    std::string newName = getUUid(20);

    s = clusterDelNodeMeta(oldname);
    if (!s.ok()) {
      LOG(ERROR) << "delete meta:" << oldname << "fail when renameNode";
      return s;
    }
    DLOG(INFO) << "Rename node " << oldname << "into:" << newName;
    _clusterState->clusterRenameNode(myself, newName);
    serverLog(LL_NOTICE, "Node hard reset, now I'm %.40s", newName.c_str());
  }
  _clusterState->clusterSaveNodes();
  _clusterState->clusterUpdateState();
  return {ErrorCodes::ERR_OK, "finish reset"};
}

void ClusterManager::stop() {
  LOG(WARNING) << "cluster manager begins stops...";
  _isRunning.store(false, std::memory_order_relaxed);
  _controller->join();

  _clusterNetwork->stop();
  _clusterNetwork.reset();
  _clusterState.reset();

  LOG(WARNING) << "cluster manager stops success";
}

Status ClusterManager::initNetWork() {
  shared_ptr<ServerParams> cfg = _svr->getParams();
  _clusterNetwork =
    std::make_unique<NetworkAsio>(_svr, _netMatrix, _reqMatrix, cfg, "cluster");

  Status s =
    _clusterNetwork->prepare(cfg->bindIp, cfg->port + CLUSTER_PORT_INCR, 1);
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
  Catalog* catalog = _svr->getCatalog();
  INVARIANT(catalog != nullptr);

  // TODO(wayenchen): cluster_announce_port/cluster_announce_bus_port
  auto params = _svr->getParams();
  uint16_t nodePort = params->port;
  std::string nodeIp = "";
  /* Node ip init as empty string if not use domain */
  bool useDomain = params->domainEnabled;
  if (useDomain) {
    nodeIp = params->bindIp;
  }
  uint16_t nodeCport = nodePort + CLUSTER_PORT_INCR;

  std::shared_ptr<ClusterState> gState =
    std::make_shared<tendisplus::ClusterState>(_svr);
  installClusterState(gState);

  auto vs = catalog->getAllClusterMeta();
  if (!vs.ok()) {
    LOG(ERROR) << "getAllClusterMeta error";
    return vs.status();
  } else if (vs.value().size() > 0) {
    int vssize = vs.value().size();
    INVARIANT(vssize > 0);
    // create node
    uint16_t myselfNum = 0;
    for (auto& nodeMeta : vs.value()) {
      auto node = _clusterState->clusterLookupNode(nodeMeta->nodeName);
      if (!node) {
        uint64_t pingTime = msSinceEpoch();
        uint64_t pongTime = msSinceEpoch();
        node = std::make_shared<ClusterNode>(nodeMeta->nodeName,
                                             nodeMeta->nodeFlag,
                                             _clusterState,
                                             nodeMeta->ip,
                                             nodeMeta->port,
                                             nodeMeta->cport,
                                             pingTime,
                                             pongTime,
                                             nodeMeta->configEpoch);
        _clusterState->clusterAddNode(node);

        Expected<std::bitset<CLUSTER_SLOTS>> st =
          bitsetDecodeVec<CLUSTER_SLOTS>(nodeMeta->slots);

        if (!st.ok()) {
          LOG(ERROR) << "bitset decode init error :";
          return st.status();
        }

        auto eSlot = _clusterState->clusterSetSlot(node, st.value());

        if (eSlot == false) {
          LOG(ERROR) << "SLOT assgin failed, please check it!";
        }
      } else {
        INVARIANT_D(0);
        LOG(WARNING) << "more than one node exists" << nodeMeta->nodeName;
      }

      if (node->nodeIsMyself()) {
        LOG(INFO) << "Node configuration loaded, I'm " << node->getNodeName();
        if (++myselfNum > 1) {
          LOG(ERROR) << "contain two myself nodes";
        }
        node->setNodeIp(nodeIp);
        node->setNodePort(nodePort);
        node->setNodeCport(nodeCport);

        _clusterState->setMyselfNode(node);
        installClusterNode(node);
      }
    }

    if (!_clusterState->getMyselfNode()) {
      LOG(FATAL) << "Myself node for cluster is missing, please check it!";
      return {ErrorCodes::ERR_INTERNAL, ""};
    }

    // master-slave info
    for (auto& nodeMeta : vs.value()) {
      if (nodeMeta->masterName != "-") {
        auto master = _clusterState->clusterLookupNode(nodeMeta->masterName);
        auto node = _clusterState->clusterLookupNode(nodeMeta->nodeName);
        INVARIANT(master != nullptr && node != nullptr);

        _clusterState->clusterNodeAddSlave(master, node);
      }
    }

    /* Something that should never happen: currentEpoch smaller than
     * the max epoch found in the nodes configuration. However we handle
     * this as some form of protection against manual editing of critical
     * files. */
    _clusterState->clusterGetOrUpdateMaxEpoch(true);

    return {ErrorCodes::ERR_OK, "init cluster from catalog "};

  } else {
    const uint16_t flagName = CLUSTER_NODE_MYSELF | CLUSTER_NODE_MASTER;

    std::shared_ptr<ClusterNode> gNode = std::make_shared<ClusterNode>(
      getUUid(20), flagName, _clusterState, nodeIp, nodePort, nodeCport);

    installClusterNode(gNode);
    _clusterState->clusterAddNode(gNode);
    _clusterState->setMyselfNode(gNode);

    auto nodename = _clusterNode->getNodeName();
    LOG(INFO) << "No cluster configuration found, I'm " << nodename;
    // store clusterMeta
    std::vector<uint16_t> slotInfo = {};

    auto pVs = std::make_unique<ClusterMeta>(nodename,
                                             nodeIp,
                                             nodePort,
                                             nodeCport,
                                             flagName,
                                             "-",
                                             0,
                                             0,
                                             gNode->getConfigEpoch(),
                                             slotInfo);

    Status s = catalog->setClusterMeta(*pVs);
    EpochMeta epoch(_clusterState->getCurrentEpoch(), 0);
    Status sEpoch = catalog->setEpochMeta(epoch);
    if (!s.ok() || !sEpoch.ok()) {
      LOG(FATAL) << "catalog setClusterMeta error:" << s.toString();
      return s;
    } else {
      LOG(INFO) << "cluster metadata set finish "
                << "store ClusterMeta Node name is" << pVs->nodeName
                << ", ip address is " << pVs->ip << ", node Flag is "
                << pVs->nodeFlag;
    }
  }

  auto s_epoch = catalog->getEpochMeta();
  if (!s_epoch.ok()) {
    LOG(ERROR) << "catalog epoch meta get error";
    return s_epoch.status();
  } else {
    uint64_t currentEpoch = s_epoch.value()->currentEpoch;
    uint64_t voteEpoch = s_epoch.value()->lastVoteEpoch;
    _clusterState->setCurrentEpoch(currentEpoch);
    _clusterState->setLastVoteEpoch(voteEpoch);
  }

  _clusterState->setMfEnd(0);
  _clusterState->resetManualFailover();

  return {ErrorCodes::ERR_OK, "init metadata ok"};
}

Status ClusterManager::startup() {
  std::lock_guard<std::mutex> lk(_mutex);

  Status s_meta = initMetaData();
  Status s_Net = initNetWork();

  if (!s_meta.ok() || !s_Net.ok()) {
    if (!s_meta.ok()) {
      LOG(ERROR) << "init metadata fail" << s_meta.toString();
      return s_meta;
    } else {
      LOG(ERROR) << "init network fail" << s_Net.toString();
      return s_Net;
    }
  } else {
    auto name = _clusterNode->getNodeName();
    auto state = _clusterState->getClusterState();
    std::string clusterState = (unsigned(state) > 0) ? "OK" : "FAIL";
    LOG(INFO) << "cluster init success:"
              << " myself node name " << name << " cluster state is "
              << clusterState;
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
  _controller =
    std::make_unique<std::thread>(std::move([this]() { controlRoutine(); }));

  return {ErrorCodes::ERR_OK, "init cluster finish"};
}

void ClusterState::clusterUpdateMyselfFlags() {
  std::lock_guard<myMutex> lock(_mutex);
  auto oldflags = _myself->getFlags();

  uint16_t nofailover =
    _server->getParams()->clusterSlaveNoFailover ? CLUSTER_NODE_NOFAILOVER : 0;
  _myself->_flags &= ~CLUSTER_NODE_NOFAILOVER;
  _myself->_flags |= nofailover;

  if (_myself->getFlags() != oldflags) {
    LOG(INFO) << "change flag to nofailover on:" << _myself->getNodeName()
              << "flag name is:" << _myself->getFlagStr();
    clusterSaveNodes();
  }
}
void ClusterState::cronRestoreSessionIfNeeded() {
  auto now = msSinceEpoch();
  uint64_t handshake_timeout = 0;

  /* The handshake timeout is the time after which a handshake node that was
   * not turned into a normal node is removed from the nodes. Usually it is
   * just the NODE_TIMEOUT value, but when NODE_TIMEOUT is too small we use
   * the value of 1 second. */
  handshake_timeout = _server->getParams()->clusterNodeTimeout;
  if (handshake_timeout < 1000)
    handshake_timeout = 1000;

  setPfailNodeNum(0);
  std::list<CNodePtr> pingNodeList;
  {
    std::lock_guard<myMutex> lock(_mutex);
    auto iter = _nodes.begin();
    for (; iter != _nodes.end(); iter++) {
      auto node = (*iter).second;
      /* Not interested in reconnecting the link with myself or nodes
       * for which we have no address. */
      if (node->_flags & (CLUSTER_NODE_MYSELF | CLUSTER_NODE_NOADDR))
        continue;

      if (node->nodeTimedOut()) {
        incrPfailNodeNum();
      }
      /* A Node in HANDSHAKE state has a limited lifespan equal to the
       * configured node timeout. */
      if (node->nodeInHandshake() &&
          now - node->getCtime() > handshake_timeout) {
        LOG(WARNING) << "deleting handshake node:" << node->getNodeName()
                     << ", IP:" << node->getNodeIp()
                     << ", PORT:" << node->getPort();

        clusterDelNodeNoLock(node);
        iter = _nodes.begin();
        continue;
      }

      if (!node->getSession()) {
        bool error = false;
        std::string errmsg;

        auto client = node->getClient();
        if (!client) {
          auto eclient = _server->getClusterMgr()->clusterCreateClient(node);
          if (!eclient.ok()) {
            error = true;
            errmsg = eclient.status().toString();
          } else {
            client = std::move(eclient.value());
            node->setClient(client);
          }
        }

        std::shared_ptr<ClusterSession> sess = nullptr;
        if (!error) {
          INVARIANT_D(client != nullptr);
          auto s = client->tryWaitConnect();
          if (s.ok()) {
            // connected
            auto esess =
              _server->getClusterMgr()->clusterCreateSession(client, node);
            if (!esess.ok()) {
              error = true;
              errmsg = esess.status().toString();
            } else {
              sess = std::move(esess.value());
            }
          } else if (s.code() == ErrorCodes::ERR_CONNECT_TRY) {
            // async connect
            serverLog(LL_VERBOSE,
                      "Async Connecting with Node %.40s at %s:%lu",
                      node->getNodeName().c_str(),
                      node->getNodeIp().c_str(),
                      node->getCport());
            continue;
          } else {
            error = true;
            errmsg = s.toString();
          }
        }

        node->setClient(nullptr);
        if (error) {
          /* We got a synchronous error from connect before
           * clusterSendPing() had a chance to be called.
           * If node->ping_sent is zero, failure detection can't work,
           * so we claim we actually sent a ping now (that will
           * be really sent as soon as the link is obtained). */
          if (node->getSentTime() == 0)
            node->setSentTime(msSinceEpoch());

          LOG(WARNING) << "Unable to connect to Cluster Node:"
                       << node->getNodeName() << ", IP: " << node->getNodeIp()
                       << ", Port: " << node->getCport()
                       << ", Error:" << errmsg;
          continue;
        }
        node->setSession(sess);
        /* Queue a PING in the new connection ASAP: this is crucial
         * to avoid false positives in failure detection.
         *
         * If the node is flagged as MEET, we send a MEET message
         * instead of a PING one, to force the receiver to add us in its
         * node table. */
        pingNodeList.push_back(node);
      }
    }
  }

  if (pingNodeList.empty()) {
    return;
  }
  uint64_t offset = _server->getReplManager()->replicationGetOffset();
  for (auto iter = pingNodeList.begin(); iter != pingNodeList.end(); iter++) {
    CNodePtr node = *iter;
    auto old_ping_sent = node->getSentTime();
    clusterSendPing(node->getSession(),
                    node->_flags & CLUSTER_NODE_MEET ? ClusterMsg::Type::MEET
                                                     : ClusterMsg::Type::PING,
                    offset);
    if (old_ping_sent) {
      /* If there was an active ping before the link was
       * disconnected, we want to restore the ping time, otherwise
       * replaced by the clusterSendPing() call. */
      node->setSentTime(old_ping_sent);
    }
    /* We can clear the flag after the first packet is sent.
     * If we'll never receive a PONG, we'll never send new packets
     * to this node. Instead after the PONG is received and we
     * are no longer in meet/handshake status, we want to send
     * normal PING packets. */
    node->_flags &= ~CLUSTER_NODE_MEET;

    LOG(INFO) << "Connecting with Node " << node->getNodeName() << " at "
              << node->getNodeIp() << ":" << node->getCport();
  }
}

void ClusterState::cronPingSomeNodes() {
  CNodePtr minPongNode = nullptr;
  uint64_t minPong = 0;

  /* Check a few random nodes and ping the one with the oldest
   * pong_received time. */
  for (uint32_t j = 0; j < 5; j++) {
    auto node = getRandomNode();

    /* Don't ping nodes disconnected or with a ping currently active. */
    if (!node->getSession() || node->getSentTime() != 0)
      continue;

    if (node->getFlags() & (CLUSTER_NODE_MYSELF | CLUSTER_NODE_HANDSHAKE))
      continue;

    if (!minPongNode || minPong > node->getReceivedTime()) {
      minPongNode = node;
      minPong = node->getReceivedTime();
    }
  }
  if (minPongNode) {
    uint64_t offset = _server->getReplManager()->replicationGetOffset();
    serverLog(
      LL_DEBUG, "Pinging node %.40s", minPongNode->getNodeName().c_str());
    clusterSendPing(minPongNode->getSession(), ClusterMsg::Type::PING, offset);
  }
}

void ClusterState::cronCheckFailState() {
  uint32_t orphaned_masters = 0;
  uint32_t max_slaves = 0;
  uint32_t this_slaves = 0;
  bool updateState = false;
  auto nodeTimeout = _server->getParams()->clusterNodeTimeout;
  /* Iterate nodes to check if we need to flag something as failing.
   * This loop is also responsible to:
   * 1) Check if there are orphaned masters (masters without non failing
   *    slaves).
   * 2) Count the max number of non failing slaves for a single master.
   * 3) Count the number of slaves for our master, if we are a slave. */
  std::list<CNodePtr> pingNodeList;
  std::list<CNodePtr> mfNodeList;
  {
    std::lock_guard<myMutex> lock(_mutex);
    auto iter = _nodes.begin();
    for (; iter != _nodes.end(); iter++) {
      auto node = (*iter).second;

      if (node->getFlags() &
          (CLUSTER_NODE_MYSELF | CLUSTER_NODE_NOADDR | CLUSTER_NODE_HANDSHAKE))
        continue;

      /* Orphaned master check, useful only if the current instance
       * is a slave that may migrate to another master. */
      if (isMyselfSlave() && node->nodeIsMaster() && !node->nodeFailed()) {
        uint32_t okslaves = clusterCountNonFailingSlaves(node);
        /* A master is orphaned if it is serving a non-zero number of
         * slots, have no working slaves, but used to have at least one
         * slave, or failed over a master that used to have slaves. */
        if (okslaves == 0 && node->getSlotNum() > 0 &&
            node->getFlags() & CLUSTER_NODE_MIGRATE_TO) {
          orphaned_masters++;
        }
        if (okslaves > max_slaves)
          max_slaves = okslaves;

        if (isMyselfSlave() && getMyMaster() == node)
          this_slaves = okslaves;
      }

      mstime_t now =
        msSinceEpoch(); /* Use an updated time at every iteration. */
      mstime_t delay;

      /* If we are waiting for the PONG more than half the cluster
       * timeout, reconnect the link: maybe there is a connection
       * issue even if the node is alive. */
      if (node->getSession() && /* is connected */
          now - node->getSession()->getCtime() >
            nodeTimeout &&       /* was not already reconnected */
          node->getSentTime() && /* we already sent a ping */
          node->getReceivedTime() <
            node->getSentTime() && /* still waiting pong */
          /* and we are waiting for the pong more than timeout/2 */
          now - node->getSentTime() > nodeTimeout / 2) {
        /* Disconnect the link, it will be reconnected automatically. */
        node->freeClusterSession();
      }


      /* If we have currently no active ping in this instance, and the
       * received PONG is older than half the cluster timeout, send
       * a new ping now, to ensure all the nodes are pinged without
       * a too big delay. */
      if (node->getSession() && node->getSentTime() == 0 &&
          (now - node->getReceivedTime()) > nodeTimeout / 2) {
        pingNodeList.push_back(node);
        continue;
      }

      /* If we are a master and one of the slaves requested a manual
       * failover, ping it continuously. */
      if (getMfEnd() && isMyselfMaster() && getMfSlave() == node &&
          node->getSession()) {
        mfNodeList.push_back(node);
        continue;
      }

      /* Check only if we have an active ping for this instance. */
      if (node->getSentTime() == 0)
        continue;

      /* Compute the delay of the PONG. Note that if we already received
       * the PONG, then node->ping_sent is zero, so can't reach this
       * code at all. */
      delay = now - node->getSentTime();
      if (delay > nodeTimeout) {
        /* Timeout reached. Set the node as possibly failing if it is
         * not already in this state. */
        if (!(node->getFlags() & (CLUSTER_NODE_PFAIL | CLUSTER_NODE_FAIL))) {
          serverLog(LL_NOTICE,
                    "*** NODE %.40s possibly failing",
                    node->getNodeName().c_str());
          node->setNodePfail();
          updateState = true;
        }
      }
    }
  }

  if (!pingNodeList.empty()) {
    uint64_t offset = _server->getReplManager()->replicationGetOffset();
    for (auto iter = pingNodeList.begin(); iter != pingNodeList.end(); iter++) {
      clusterSendPing((*iter)->getSession(), ClusterMsg::Type::PING, offset);
    }
  }
  if (!mfNodeList.empty()) {
    uint64_t offset = _server->getReplManager()->replicationGetMaxBinlogId();
    for (auto iter = mfNodeList.begin(); iter != mfNodeList.end(); iter++) {
      clusterSendPing((*iter)->getSession(), ClusterMsg::Type::PING, offset);
    }
  }

  /* Abort a manual failover if the timeout is reached. */
  manualFailoverCheckTimeout();

  if (isMyselfSlave()) {
    clusterHandleManualFailover();

    auto s1 = clusterHandleSlaveFailover();
    if (s1.ok()) {
      /* Take responsability for the cluster slots. */
      LOG(INFO) << "node" << getMyselfName() << "vote success:";
      _isVoteFailByDataAge.store(false, std::memory_order_relaxed);
      auto s2 = clusterFailoverReplaceYourMaster();
      if (!s2.ok()) {
        LOG(ERROR) << "replace master fail" << s2.toString();
      }
    }
    /* If there are orphaned slaves, and we are a slave among the masters
     * with the max number of non-failing slaves, consider migrating to
     * the orphaned masters. Note that it does not make sense to try
     * a migration if there is no master with at least *two* working
     * slaves. */
    bool needSlaveChange = _server->getParams()->slaveMigarateEnabled;
    if (orphaned_masters && max_slaves >= 2 && this_slaves == max_slaves &&
        needSlaveChange)
      clusterHandleSlaveMigration(max_slaves);
  }

  if (updateState || _state == ClusterHealth::CLUSTER_FAIL)
    clusterUpdateState();
}  // namespace tendisplus


void ClusterState::cronCheckReplicate() {
  if (!getMyMaster()) {
    return;
  }

  if (!getMyMaster()->nodeHasAddr()) {
    LOG(ERROR) << "my master has no addr:" << getMyMaster()->getNodeName();
    return;
  }

  auto masterIp = getMyMaster()->getNodeIp();
  auto masterPort = getMyMaster()->getPort();
  auto replMgr = _server->getReplManager();

  Status s;
  std::vector<uint32_t> errList =
    replMgr->checkMasterHost(masterIp, masterPort);

  if (errList.size() == 0)
    return;
  /* If we are a slave node but the replication meta is error on some
   * storeid, fix the metadata */
  for (auto& id : errList) {
    LOG(WARNING) << "storeid:" << id << "replicate meta is not right";
    if (replMgr->getMasterHost(id) != "") {
      LOG(WARNING) << "change storeid:" << id
                   << "replicate metedata target to:" << masterIp << ":"
                   << masterPort;
      s = replMgr->replicationUnSetMaster(id);
      if (!s.ok()) {
        LOG(ERROR) << "unset old master fail" << s.toString();
        return;
      }
    }
    s = replMgr->replicationSetMaster(masterIp, masterPort, id, false);
    if (!s.ok()) {
      LOG(ERROR) << "storeid :" << id << "set master fail:" << s.toString();
    }
    LOG(INFO) << "cron check replicate fix on store:" << id;
  }
}

void ClusterState::clusterUpdateState() {
  std::lock_guard<myMutex> lock(_mutex);

  uint32_t reachable_masters = 0;
  ClusterHealth new_state;

  /* If this is a master node, wait some time before turning the state
   * into OK, since it is not a good idea to rejoin the cluster as a writable
   * master, after a reboot, without giving the cluster a chance to
   * reconfigure this node. Note that the delay is calculated starting from
   * the first call to this function and not since the server start, in order
   * to don't count the DB loading time. */
  if (_updateStateCallTime == 0) {
    _updateStateCallTime = msSinceEpoch();
  }

  if (_myself->nodeIsMaster() && _state == ClusterHealth::CLUSTER_FAIL &&
      msSinceEpoch() - _updateStateCallTime < CLUSTER_WRITABLE_DELAY) {
    return;
  }
  /* Start assuming the state is OK. We'll turn it into FAIL if there
   * are the right conditions. */
  new_state = ClusterHealth::CLUSTER_OK;

  if (_server->getParams()->clusterRequireFullCoverage) {
    for (size_t j = 0; j < CLUSTER_SLOTS; j++) {
      auto owner = getNodeBySlot(j);
      if (owner == nullptr || owner->nodeFailed()) {
        if (owner == nullptr) {
          DLOG(ERROR) << "clusterstate turn to fail: slot " << j
                      << " belong to no node";
        } else {
          DLOG(ERROR) << "state turn fail: node" << owner->getNodeName()
                      << "is marked as fail on slot:" << j;
        }
        new_state = ClusterHealth::CLUSTER_FAIL;
        break;
      }
    }
  }

  /* Compute the cluster size, that is the sum of the number of master nodes
   * serving at least a single slot  and the number of arbiter.
   *
   * At the same time count the number of reachable masters having
   * at least one slot. */
  _size = 0;
  for (const auto& nodep : _nodes) {
    const auto& node = nodep.second;
    if (node->nodeIsMaster() && (node->getSlotNum() || node->nodeIsArbiter())) {
      _size++;
      if ((node->getFlags() & (CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)) == 0) {
        reachable_masters++;
      }
    }
  }
  uint16_t needed_quorum = (_size / 2) + 1;
  /* If we are in a minority partition, change the cluster state
   * to FAIL. */
  if (reachable_masters < needed_quorum) {
    new_state = ClusterHealth::CLUSTER_FAIL;
    _amongMinorityTime = msSinceEpoch();
  }
  /* Log a state change */
  if (new_state != _state) {
    mstime_t rejoin_delay = _server->getParams()->clusterNodeTimeout;

    /* If the instance is a master and was partitioned away with the
     * minority, don't let it accept queries for some time after the
     * partition heals, to make sure there is enough time to receive
     * a configuration update. */

    if (rejoin_delay > CLUSTER_MAX_REJOIN_DELAY)
      rejoin_delay = CLUSTER_MAX_REJOIN_DELAY;
    if (rejoin_delay < CLUSTER_MIN_REJOIN_DELAY)
      rejoin_delay = CLUSTER_MIN_REJOIN_DELAY;

    if (new_state == ClusterHealth::CLUSTER_OK && _myself->nodeIsMaster() &&
        msSinceEpoch() - _amongMinorityTime < rejoin_delay) {
      return;
    }

    /* Change the state and log the event. */
    serverLog(LL_WARNING,
              "Cluster state changed: %s",
              new_state == ClusterHealth::CLUSTER_OK ? "ok" : "fail");
    _state = new_state;
  }
}
uint64_t ClusterState::getMfEnd() const {
  std::lock_guard<myMutex> lock(_mutex);
  return _mfEnd;
}

CNodePtr ClusterState::getMfSlave() const {
  std::lock_guard<myMutex> lock(_mutex);
  return _mfSlave;
}

void ClusterState::setMfEnd(uint64_t x) {
  std::lock_guard<myMutex> lock(_mutex);
  _mfEnd = x;
}

void ClusterState::setMfSlave(CNodePtr n) {
  std::lock_guard<myMutex> lock(_mutex);
  _mfSlave = n;
}

void ClusterState::setMfStart() {
  std::lock_guard<myMutex> lock(_mutex);
  _mfCanStart = 1;
}


Status ClusterState::clusterBlockMyself(uint64_t locktime) {
  if (isClientBlock()) {
    LOG(WARNING) << "already block!";
    setClientUnBlock();
    INVARIANT_D(!_isCliBlocked.load(std::memory_order_relaxed));
  }

  auto exptLockList = clusterLockMySlots();
  if (!exptLockList.ok()) {
    LOG(ERROR) << "fail lock slots on:" << getMyselfName()
               << exptLockList.status().toString();
    return exptLockList.status();
  }

  _manualLockThead = std::make_unique<std::thread>(
    [this](uint64_t time, std::list<std::unique_ptr<ChunkLock>>&& locklist) {
      std::unique_lock<std::mutex> lk(_failMutex);
      _cv.wait_for(lk, std::chrono::milliseconds(time));
      LOG(INFO) << "mflock end:" << msSinceEpoch();
    },
    locktime,
    std::move(exptLockList.value()));

  /*NOTE(wayenchen) thread should be detach */
  _manualLockThead->detach();
  _manualLockThead.reset();
  return {ErrorCodes::ERR_OK, "finish lock chunk on node"};
}

Expected<std::list<std::unique_ptr<ChunkLock>>>
ClusterState::clusterLockMySlots() {
  mstime_t locktime = msSinceEpoch();
  std::list<std::unique_ptr<ChunkLock>> lockList;
  std::unordered_map<uint32_t, std::set<uint32_t>> slotsMap;

  auto slots = _myself->getSlots();
  size_t idx = 0;
  while (idx < CLUSTER_SLOTS) {
    if (slots.test(idx)) {
      uint32_t storeid = _server->getSegmentMgr()->getStoreid(idx);
      slotsMap[storeid].insert(idx);
    }
    idx++;
  }

  uint64_t lockNum = 0;
  std::bitset<CLUSTER_SLOTS> slotsDone;
  uint32_t storeNum = _server->getKVStoreCount();

  for (uint32_t i = 0; i < storeNum; i++) {
    std::unordered_map<uint32_t, std::set<uint32_t>>::iterator it;
    if ((it = slotsMap.find(i)) != slotsMap.end()) {
      auto slotSet = it->second;
      for (auto iter = slotSet.begin(); iter != slotSet.end(); ++iter) {
        auto lock = ChunkLock::AquireChunkLock(i,
                                               *iter,
                                               mgl::LockMode::LOCK_S,
                                               nullptr,
                                               _server->getMGLockMgr(),
                                               1000);
        if (!lock.ok()) {
          LOG(ERROR) << "lock chunk fail on :" << *iter;
        } else {
          lockList.push_back(std::move(lock.value()));
          lockNum++;
          slotsDone.set(*iter);
        }
        // NOTE(wayenchen) LOCK TIME should be limited
        mstime_t delay = msSinceEpoch() - locktime;
        if (delay > CLUSTER_MF_TIMEOUT / 2) {
          return {ErrorCodes::ERR_TIMEOUT, "lock timeout"};
        }
      }
    }
  }
  _isCliBlocked.store(true, std::memory_order_relaxed);
  LOG(INFO) << "finish lock on:" << bitsetStrEncode(slotsDone)
            << "Lock num:" << lockNum << "time:" << msSinceEpoch();
  if (lockNum == 0) {
    return {ErrorCodes::ERR_INTERNAL, "no lock finish"};
  }
  return lockList;
}


void ClusterManager::controlRoutine() {
  uint64_t iteration = 0;
  while (_isRunning.load(std::memory_order_relaxed)) {
    /* Update myself flags. */
    _clusterState->clusterUpdateMyselfFlags();
    /* Check if we have disconnected nodes and re-establish the connection.
     * Also update a few stats while we are here, that can be used to make
     * better decisions in other part of the code. */
    _clusterState->cronRestoreSessionIfNeeded();
    /* Ping some random node 1 time every 10 iterations, so that we usually
     * ping one random node every second. */
    if (!(iteration++ % 10)) {
      _clusterState->cronPingSomeNodes();
      _clusterState->cronCheckReplicate();
    }

    _clusterState->cronCheckFailState();

    std::this_thread::sleep_for(100ms);
  }
}

Expected<std::shared_ptr<BlockingTcpClient>>
ClusterManager::clusterCreateClient(const std::shared_ptr<ClusterNode>& node) {
  std::shared_ptr<BlockingTcpClient> client =
    std::move(_clusterNetwork->createBlockingClient(64 * 1024 * 1024));
  Status s = client->connect(
    node->getNodeIp(), node->getCport(), std::chrono::seconds(3), false);
  if (!s.ok()) {
    LOG(ERROR) << "create client fail";
    return s;
  }
  return client;
}

Expected<std::shared_ptr<ClusterSession>> ClusterManager::clusterCreateSession(
  std::shared_ptr<BlockingTcpClient> client,
  const std::shared_ptr<ClusterNode>& node) {
  auto sess = _clusterNetwork->client2ClusterSession(std::move(client));
  if (!sess.ok()) {
    LOG(WARNING) << "client2ClusterSession failed: ";
    return {ErrorCodes::ERR_NETWORK, "clent2ClusterSession failed"};
  }
  INVARIANT_D(sess.value()->getType() == Session::Type::CLUSTER);
  sess.value()->setNode(node);
  return sess.value();
}

bool ClusterManager::hasDirtyKey(uint32_t storeid) {
  auto node = _clusterState->getMyselfNode();
  auto curmaster = node->nodeIsMaster() ? node : node->getMaster();
  for (uint32_t chunkid = 0; chunkid < CLUSTER_SLOTS; chunkid++) {
    if (_svr->getSegmentMgr()->getStoreid(chunkid) == storeid &&
        _clusterState->getNodeBySlot(chunkid) != curmaster &&
        !emptySlot(chunkid)) {
      LOG(WARNING) << "hasDirtyKey storeid:" << storeid
                   << " chunkid:" << chunkid;
      return true;
    }
  }
  return false;
}

uint64_t ClusterManager::countKeysInSlot(uint32_t slot) {
  uint32_t storeId = _svr->getSegmentMgr()->getStoreid(slot);
  LocalSessionGuard g(_svr.get());
  auto expdb = _svr->getSegmentMgr()->getDb(
    g.getSession(), storeId, mgl::LockMode::LOCK_IS);
  if (!expdb.ok()) {
    return 0;
  }
  auto kvstore = std::move(expdb.value().store);
  auto ptxn = kvstore->createTransaction(nullptr);
  INVARIANT_D(ptxn.ok());
  auto slotCursor = std::move(ptxn.value()->createSlotCursor(slot));

  uint64_t keyNum = 0;
  while (true) {
    Expected<Record> expRcd = slotCursor->next();
    if (expRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
      break;
    }
    if (!expRcd.ok()) {
      LOG(ERROR) << "get slot cursor error:" << expRcd.status().toString();
      return keyNum;
    }
    keyNum++;
  }
  return keyNum;
}

std::vector<std::string> ClusterManager::getKeyBySlot(uint32_t slot,
                                                      uint32_t count) {
  uint32_t storeId = _svr->getSegmentMgr()->getStoreid(slot);
  LocalSessionGuard g(_svr.get());
  auto expdb = _svr->getSegmentMgr()->getDb(
    g.getSession(), storeId, mgl::LockMode::LOCK_IS);
  std::vector<std::string> keysList;

  if (!expdb.ok()) {
    return keysList;
  }
  auto kvstore = std::move(expdb.value().store);
  auto ptxn = kvstore->createTransaction(nullptr);
  if (!ptxn.ok()) {
    LOG_STATUS(ptxn.status());
    return keysList;
  }
  auto slotCursor = std::move(ptxn.value()->createSlotCursor(slot));

  uint32_t n = 0;
  while (true) {
    Expected<Record> expRcd = slotCursor->next();
    if (expRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
      break;
    }

    if (!expRcd.ok()) {
      LOG(ERROR) << "get slot cursor error:" << expRcd.status().toString();
      return {};
    }
    std::string keyname = expRcd.value().getRecordKey().getPrimaryKey();
    keysList.push_back(keyname);
    n++;
    if (n >= count)
      break;
  }
  return keysList;
}

bool ClusterManager::emptySlot(uint32_t slot) {
  uint32_t storeId = _svr->getSegmentMgr()->getStoreid(slot);
  LocalSessionGuard g(_svr.get());
  auto expdb = _svr->getSegmentMgr()->getDb(
    g.getSession(), storeId, mgl::LockMode::LOCK_IS);
  if (!expdb.ok()) {
    return false;
  }
  auto kvstore = std::move(expdb.value().store);
  auto ptxn = kvstore->createTransaction(nullptr);
  INVARIANT_D(ptxn.ok());
  auto slotCursor = std::move(ptxn.value()->createSlotCursor(slot));
  auto v = slotCursor->next();

  if (!v.ok()) {
    if (v.status().code() == ErrorCodes::ERR_EXHAUST) {
      return true;
    } else {
      LOG(ERROR) << "slot not empty beacause get slot:" << slot
                 << "cusror fail";
      return false;
    }
  }
  return false;
}

ClusterSession::ClusterSession(std::shared_ptr<ServerEntry> server,
                               asio::ip::tcp::socket sock,
                               uint64_t connid,
                               bool initSock,
                               std::shared_ptr<NetworkMatrix> netMatrix,
                               std::shared_ptr<RequestMatrix> reqMatrix)
  : NetSession(server,
               std::move(sock),
               connid,
               initSock,
               netMatrix,
               reqMatrix,
               Session::Type::CLUSTER),
    _pkgSize(-1) {
  DLOG(INFO) << "cluster session, id:" << id() << " created";
}

void ClusterSession::schedule() {
  stepState();
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
      _pkgSize = int32Decode(_queryBuf.data() + 4);
      /* Perform some sanity check on the message signature
       * and length. */
      if (memcmp(_queryBuf.data(), "RCmb", 4) != 0 ||
          _pkgSize < CLUSTERMSG_MIN_LEN) {
        LOG(WARNING) << "Bad message length or signature received "
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
    // NOLINT(whitespace/line_length)
    // https://stackoverflow.com/questions/8848575/fastest-way-to-reset-every-value-of-stdvectorint-to-0)
    _queryBuf.resize((readlen + _queryBufPos) * 2, 0);
  }

  auto self(shared_from_this());
  uint64_t curr = nsSinceEpoch();
  _sock.async_read_some(
    asio::buffer(_queryBuf.data() + _queryBufPos, readlen),
    [this, self, curr](const std::error_code& ec, size_t actualLen) {
      drainReqCallback(ec, actualLen);
    });
}


void ClusterSession::drainReqCallback(const std::error_code& ec,
                                      size_t actualLen) {
  if (ec) {
    /* I/O error... */
    LOG(WARNING) << "I/O error reading from node link(" << getRemoteRepr()
                 << "): " << ec.message();
    endSession();
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
  auto status = clusterProcessPacket();
  _reqMatrix->processed += 1;
  _reqMatrix->processCost += nsSinceEpoch() - _ctx->getProcessPacketStart();
  _ctx->setProcessPacketStart(0);

  _queryBufPos = 0;
  if (!status.ok()) {
    if (_node) {
      LOG(ERROR) << "clusterProcessPacket failed, freeClusterSession ip:"
                 << _node->getNodeIp() << " Cport:" << _node->getCport();
      _node->freeClusterSession();
    } else {
      setCloseAfterRsp();
      endSession();
    }
  } else {
    setState(State::DrainReqNet);
    schedule();
  }
}

bool ClusterState::clusterProcessGossipSection(
  std::shared_ptr<ClusterSession> sess, const ClusterMsg& msg) {
  std::lock_guard<myMutex> lock(_mutex);
  auto hdr = msg.getHeader();
  auto count = hdr->_count;
  auto data = msg.getData();
  auto mstime = msSinceEpoch();
  INVARIANT(data->getType() == ClusterMsgData::Type::Gossip);

  bool save = false;
  std::shared_ptr<ClusterMsgDataGossip> gossip =
    std::dynamic_pointer_cast<ClusterMsgDataGossip>(data);
  auto sender =
    sess->getNode() ? sess->getNode() : clusterLookupNodeNoLock(hdr->_sender);

  INVARIANT(count == gossip->getGossipList().size());

  for (const auto& g : gossip->getGossipList()) {
    auto flags = g._gossipFlags;

#ifdef TENDIS_DEBUG
    auto flagsStr = representClusterNodeFlags(flags);
    serverLog(LL_DEBUG,
              "GOSSIP %.40s %s:%d@%d %s",
              g._gossipName.c_str(),
              g._gossipIp.c_str(),
              g._gossipPort,
              g._gossipCport,
              flagsStr.c_str());
#endif

    /* Update our state accordingly to the gossip sections */
    auto node = clusterLookupNodeNoLock(g._gossipName);
    if (node) {
      /* We already know this node.
      Handle failure reports, only when the sender is a master. */
      if (sender && sender->nodeIsMaster() && node != _myself) {
        if (flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)) {
          if (clusterNodeAddFailureReport(node, sender)) {
            serverLog(LL_VERBOSE,
                      "Node %.40s reported node %.40s as not reachable.",
                      sender->getNodeName().c_str(),
                      node->getNodeName().c_str());
          }
          if (markAsFailingIfNeeded(node)) {
            LOG(INFO) << "mark node " << node->getNodeName() << ":fail";
            save = true;
          }
        } else {
          if (clusterNodeDelFailureReport(node, sender)) {
            serverLog(LL_VERBOSE,
                      "Node %.40s reported node %.40s is back online.",
                      sender->getNodeName().c_str(),
                      node->getNodeName().c_str());
          }
        }
      }

      /* If from our POV the node is up (no failure flags are set),
       * we have no pending ping for the node, nor we have failure
       * reports for this node, update the last pong time with the
       * one we see from the other nodes. */
      if (!(flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)) &&
          node->_pingSent == 0 && clusterNodeFailureReportsCount(node) == 0) {
        mstime_t pongtime = g._pongReceived;
        pongtime *= 1000; /* Convert back to milliseconds. */

        /* Replace the pong time with the received one only if
         * it's greater than our view but is not in the future
         * (with 500 milliseconds tolerance) from the POV of our
         * clock. */
        if (pongtime <= (mstime + 500) && pongtime > node->_pongReceived) {
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
           node->getPort() != g._gossipPort ||
           node->getCport() != g._gossipCport)) {
        // is it possiable that node is _myself?
        INVARIANT(node != _myself);
        LOG(WARNING) << "clusterProcessGossipSection node info update,ip:"
                     << g._gossipIp << " port:" << g._gossipPort
                     << " Cport:" << g._gossipCport;
        node->freeClusterSession();
        node->setNodeIp(g._gossipIp);
        node->setNodePort(g._gossipPort);
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
      if (sender && !(flags & CLUSTER_NODE_NOADDR) &&
          !clusterBlacklistExists(g._gossipName)) {
        clusterStartHandshake(g._gossipIp, g._gossipPort, g._gossipCport);
      }
    }
  }

  return save;
}

Status ClusterState::clusterProcessPacket(std::shared_ptr<ClusterSession> sess,
                                          const ClusterMsg& msg) {
  bool save = false;
  bool update = false;

  if (getBlockState()) {
    // sleep or return OK?
    DLOG(INFO) << "packet begin block at:" << msSinceEpoch();
    std::this_thread::sleep_for(chrono::milliseconds(getBlockTime()));
    DLOG(INFO) << "packet begin receive at:" << msSinceEpoch();
  }

  const auto guard = MakeGuard([&] {
    if (save) {
      clusterSaveNodes();
    }
    if (update) {
      clusterUpdateState();
    }
  });

  auto hdr = msg.getHeader();
  auto type = msg.getType();

  _statsMessagesReceived[(uint16_t)type]++;

  uint16_t flags = hdr->_flags;
  uint64_t senderCurrentEpoch = 0, senderConfigEpoch = 0;
  auto timeout = _server->getParams()->clusterNodeTimeout;
  bool useDomain = _server->getParams()->domainEnabled;

  /* Check if the sender is a known node. */
  auto sender = clusterLookupNode(hdr->_sender);
  if (sender && !sender->nodeInHandshake()) {
    /* Update our curretEpoch if we see a newer epoch in the cluster. */
    senderCurrentEpoch = hdr->_currentEpoch;
    senderConfigEpoch = hdr->_configEpoch;
    if (senderCurrentEpoch > getCurrentEpoch())
      setCurrentEpoch(senderCurrentEpoch);
    /* Update the sender configEpoch if it is publishing a newer one. */
    if (senderConfigEpoch > sender->getConfigEpoch()) {
      sender->setConfigEpoch(senderConfigEpoch);
      save = true;
    }
    /* Update the replication offset info for this node. */
    sender->_replOffset = hdr->_offset;
    sender->_replOffsetTime = msSinceEpoch();
    // FIXME:
    /* If we are a slave performing a manual failover and our master
     * sent its offset while already paused, populate the MF state. */
    if (msg.getMflags() & CLUSTERMSG_FLAG0_PAUSED &&
        setMfMasterOffsetIfNecessary(sender)) {
      _mfMasterOffset = sender->_replOffset;
      _isMfOffsetReceived.store(true, std::memory_order_relaxed);
      serverLog(LL_WARNING,
                "Received replication offset for paused "
                "master manual failover: %lu %lu",
                sender->_replOffset,
                _server->getReplManager()->replicationGetOffset());
    }
  }

  auto typeStr = ClusterMsg::clusterGetMessageTypeString(type);
  if (type == ClusterMsg::Type::PING || type == ClusterMsg::Type::MEET) {
    serverLog(LL_DEBUG,
              "%s packet received: %s, id:%lu, (%s)",
              typeStr.c_str(),
              hdr->_sender.c_str(),
              sess->id(),
              sess->getNode() ? "I'm sender" : "I'm receiver");

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
    /* NOTE(wayenchen) domain named will not be changed so not  need to
     * update*/
    if ((type == ClusterMsg::Type::MEET || _myself->getNodeIp() == "") &&
        !useDomain) {
      auto eip = sess->getLocalIp();
      if (eip.ok() && eip.value() != _myself->getNodeIp()) {
        serverLog(LL_WARNING,
                  "IP address for this node updated to %s",
                  eip.value().c_str());
        _myself->setNodeIp(eip.value());
        save = true;
      }
    }

    /* Add this node if it is new for us and the msg type is MEET.
     * In this stage we don't try to add the node with the right
     * flags, slaveof pointer, and so forth, as this details will be
     * resolved when we'll receive PONGs from the node. */
    if (!sender && type == ClusterMsg::Type::MEET) {
      auto ip = sess->nodeIp2String(hdr->_myIp);
      auto node = std::make_shared<ClusterNode>(getUUid(20),
                                                CLUSTER_NODE_HANDSHAKE,
                                                shared_from_this(),
                                                ip,
                                                hdr->_port,
                                                hdr->_cport);

      clusterAddNode(node);
      save = true;
    }

    /* If this is a MEET packet from an unknown node, we still process
     * the gossip section here since we have to trust the sender because
     * of the message type. */
    if (!sender && type == ClusterMsg::Type::MEET) {
      if (clusterProcessGossipSection(sess, msg))
        save = true;
    }

    /* Anyway reply with a PONG */
    uint64_t offset = _server->getReplManager()->replicationGetOffset();
    clusterSendPing(sess, ClusterMsg::Type::PONG, offset);
  }

  if (type == ClusterMsg::Type::PING || type == ClusterMsg::Type::PONG ||
      type == ClusterMsg::Type::MEET) {
    serverLog(LL_DEBUG,
              "%s packet received: %s, id:%lu, (%s)",
              typeStr.c_str(),
              hdr->_sender.c_str(),
              sess->id(),
              sess->getNode() ? const_cast<char*>("I'm sender")
                              : const_cast<char*>("I'm receiver"));


    auto sessNode = sess->getNode();
    if (sessNode) {
      if (sessNode->nodeInHandshake()) {
        /* TODO(wayenchen): Test, node1 meet node2 more than one time
            node1, node2
            node1: cluster meet node2ip node2port
                   wait for node1 and node2 ping/pong
                   cluster meet node2ip node2port (node1 meet node2 one
           more time)
        */

        /* If we already have this node, try to change the
         * IP/port of the node with the new one. */
        if (sender) {
          serverLog(LL_VERBOSE,
                    "Handshake: we already know node %.40s, "
                    "updating the address if needed.",
                    sender->getNodeName().c_str());
          if (updateAddressIfNeeded(sender, sess, msg)) {
            update = true;
            save = true;
          }
          /* Free this node as we already have it. This will
           * cause the link to be freed as well. */
          clusterDelNode(sessNode);
          save = true;  // needed?

          return {ErrorCodes::ERR_CLUSTER,
                  "Handshake: we already know node" + sender->getNodeName()};
        }

        /* First thing to do is replacing the random name with the
         * right node name if this was a handshake stage. */
        serverLog(LL_DEBUG,
                  "Handshake with node %.40s completed.",
                  sessNode->getNodeName().c_str());
        sessNode->_flags &= ~CLUSTER_NODE_HANDSHAKE;
        sessNode->_flags |= flags &
          (CLUSTER_NODE_MASTER | CLUSTER_NODE_SLAVE | CLUSTER_NODE_ARBITER);
        clusterRenameNode(sessNode, hdr->_sender);
        save = true;
      } else if (sessNode->getNodeName() != hdr->_sender) {
        /* TODO(vinchen): How to repeat?
           The _sender change the ID dynamically?
        */

        /* If the reply has a non matching node ID we
         * disconnect this node and set it as not having an associated
         * address. */
        serverLog(LL_VERBOSE,
                  "PONG contains mismatching sender ID. "
                  "About node %.40s added %d ms ago, having flags %d",
                  sessNode->getNodeName().c_str(),
                  (uint32_t)(msSinceEpoch() - sessNode->getCtime()),
                  sessNode->getFlags());
        sessNode->_flags |= CLUSTER_NODE_NOADDR;
        sessNode->setNodeIp("");
        sessNode->setNodePort(0);
        sessNode->setNodeCport(0);
        sessNode->freeClusterSession();
        save = true;
        //  std::string nodeName = hdr->_sender;
        return {ErrorCodes::ERR_CLUSTER,
                "PONG contains mismatching sender " + hdr->_sender};
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

    /* Update the node arbiter flag */
    if (sender &&
        (type == ClusterMsg::Type::PING || type == ClusterMsg::Type::PONG)) {
      if ((sender->_flags & CLUSTER_NODE_ARBITER) !=
          (flags & CLUSTER_NODE_ARBITER)) {
        serverLog(LL_WARNING,
                  "%s arbiter flags change %d",
                  sender->getNodeName().c_str(),
                  (flags & CLUSTER_NODE_ARBITER));
      }
      sender->_flags &= ~CLUSTER_NODE_ARBITER;
      sender->_flags |= flags & CLUSTER_NODE_ARBITER;
      save = true;
    }

    /* Update the node address if it changed. */
    if (sender && type == ClusterMsg::Type::PING &&
        !sender->nodeInHandshake() &&
        updateAddressIfNeeded(sender, sess, msg)) {
      clusterUpdateState();
      save = true;
    }

    /* TODO(vinchen): why only sessNode is not null,
     * the _pongReceived can be updated?
     */
    /* Update our info about the node */
    if (sessNode && type == ClusterMsg::Type::PONG) {
      sessNode->setReceivedTime(msSinceEpoch());
      sessNode->setSentTime(0);

      /* The PFAIL condition can be reversed without external
       * help if it is momentary (that is, if it does not
       * turn into a FAIL state).6
       *
       * The FAIL condition is also reversible under specific
       * conditions detected by clearNodeFailureIfNeeded(). */
      if (sessNode->nodeTimedOut()) {
        sessNode->_flags &= ~CLUSTER_NODE_PFAIL;
        save = true;
      } else if (sessNode->nodeFailed()) {
        if (sessNode->clearNodeFailureIfNeeded(timeout)) {
          save = true;
          clusterUpdateState();
        }
      }
    }

    /* Check for role switch: slave -> master or master -> slave. */
    if (sender) {
      if (msg.isMaster()) {
        /* Node is a master. */
        if (clusterSetNodeAsMaster(sender))
          save = true;
      } else {
        /* Node is a slave. */
        auto master = clusterLookupNode(hdr->_slaveOf);

        /* Master turned into a slave! Reconfigure the node. */
        if (sender->nodeIsMaster()) {
          clusterDelNodeSlots(sender);
          sender->_flags &= ~(CLUSTER_NODE_MASTER | CLUSTER_NODE_MIGRATE_TO);
          sender->_flags |= CLUSTER_NODE_SLAVE;
          save = true;
        }

        /* Master node changed for this slave? */
        if (master && sender->getMaster() != master) {
          auto orgMaster = sender->getMaster();
          if (orgMaster)
            clusterNodeRemoveSlave(orgMaster, sender);

          clusterNodeAddSlave(master, sender);
          /* Update config. */
          clusterUpdateState();
          save = true;
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
    /* Sender claimed slots don't match my view? */
    bool dirty_slots = false;

    if (sender) {
      sender_master = sender->nodeIsMaster() ? sender : sender->getMaster();
      if (sender_master) {
        if (sender_master->getSlots() != hdr->_slots) {
          dirty_slots = true;
        }
      }
    }

    /* 1) If the sender of the message is a master, and we detected that
     *    the set of slots it claims changed, scan the slots to see if we
     *    need to update our configuration. */
    if (sender && sender->nodeIsMaster() && dirty_slots) {
      clusterUpdateSlotsConfigWith(sender, senderConfigEpoch, hdr->_slots);
      save = true;
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
          auto nodej = getNodeBySlot(j);
          if (nodej == sender ||
              // why? Because all nodej update need UPDATE message
              nodej == nullptr) {
            continue;
          }

          if (nodej->getConfigEpoch() > senderConfigEpoch) {
            serverLog(LL_VERBOSE,
                      "Node %.40s has old slots configuration, sending "
                      "an UPDATE message about %.40s",
                      sender->getNodeName().c_str(),
                      nodej->getNodeName().c_str());
            uint64_t offset = _server->getReplManager()->replicationGetOffset();
            clusterSendUpdate(sess, nodej, offset);

            /* TODO(vinchen): instead of exiting the loop send every
             * other UPDATE packet for other nodes that are the new
             * owner of sender's slots. */
            break;
          }
        }
      }
    }

    /* If our config epoch collides with the sender's try to fix
     * the problem. */
    if (sender && _myself->nodeIsMaster() && sender->nodeIsMaster() &&
        senderConfigEpoch == _myself->getConfigEpoch()) {
      clusterHandleConfigEpochCollision(sender);
      save = true;
    }

    /* Get info from the gossip section */
    if (sender) {
      if (clusterProcessGossipSection(sess, msg))
        save = true;
    }
  } else if (type == ClusterMsg::Type::FAIL) {
    std::shared_ptr<ClusterMsgDataFail> failMsg =
      std::dynamic_pointer_cast<ClusterMsgDataFail>(msg.getData());
    std::string failName = failMsg->getNodeName();
    if (sender) {
      auto failing = clusterLookupNode(failName);
      if (failing &&
          !(failing->getFlags() & (CLUSTER_NODE_FAIL | CLUSTER_NODE_MYSELF))) {
        failing->_flags |= CLUSTER_NODE_FAIL;
        failing->_failTime = msSinceEpoch();
        failing->_flags &= ~CLUSTER_NODE_PFAIL;
        std::string flagname = representClusterNodeFlags(failing->_flags);

        serverLog(LL_NOTICE,
                  "FAIL message received from %.40s about %.40s",
                  hdr->_sender.c_str(),
                  failName.c_str());
        save = true;
        clusterUpdateState();
      }
    } else {
      serverLog(LL_NOTICE,
                "Ignoring FAIL message from unknown node %.40s about %.40s",
                hdr->_sender.c_str(),
                failName.c_str());
    }

  } else if (type == ClusterMsg::Type::PUBLISH) {
    INVARIANT_D(0);
  } else if (type == ClusterMsg::Type::FAILOVER_AUTH_REQUEST) {
    if (!sender)
      return {ErrorCodes::ERR_OK, ""}; /* We don't know that node. */
    clusterSendFailoverAuthIfNeeded(sender, msg);
  } else if (type == ClusterMsg::Type::FAILOVER_AUTH_ACK) {
    if (!sender)
      return {ErrorCodes::ERR_OK, ""}; /* We don't know that node. */
    /* We consider this vote only if the sender is a master serving
     * a non zero number of slots or an arbiter, and its currentEpoch is
     * greater or equal to epoch where this node started the election. */
    if (sender->nodeIsMaster() &&
        (sender->getSlotNum() > 0 || sender->nodeIsArbiter()) &&
        senderCurrentEpoch >= getFailAuthEpoch()) {
      addFailVoteNum();
      /* Maybe we reached a quorum here, set a flag to make sure
       * we check ASAP. */
      auto s = clusterHandleSlaveFailover();
      if (s.ok()) {
        /* Take responsability for the cluster slots. */
        auto result = clusterFailoverReplaceYourMaster();
        if (!s.ok()) {
          LOG(ERROR) << "replace mater:" << getMyMaster()->getNodeName()
                     << "fail";
        }
      }
    }
  } else if (type == ClusterMsg::Type::MFSTART) {
    /* This message is acceptable only if I'm a master and the sender
     * is one of my slaves. */
    if (!sender || sender->getMaster() != _myself)
      return {ErrorCodes::ERR_OK, ""};
    /* Manual failover requested from slaves. Initialize the state
     * accordingly. */
    resetManualFailover();

    auto s = clusterBlockMyself(2 * CLUSTER_MF_TIMEOUT);
    if (s.ok()) {
      setMfSlave(sender);
      setMfEnd(msSinceEpoch() + CLUSTER_MF_TIMEOUT);
      LOG(INFO) << "set mf_end:" << getMfEnd();
    } else {
      LOG(ERROR) << "manual failover lock fail" << s.toString();
    }
    serverLog(LL_WARNING,
              "Manual failover requested by slave %.40s.",
              sender->getNodeName().c_str());
  } else if (type == ClusterMsg::Type::UPDATE) {
    std::shared_ptr<ClusterMsgDataUpdate> updateMsg =
      std::dynamic_pointer_cast<ClusterMsgDataUpdate>(msg.getData());

    uint64_t reportedConfigEpoch = updateMsg->getConfigEpoch();

    if (!sender) {
      /* We don't know the sender. */
      return {ErrorCodes::ERR_OK, ""};
    }

    auto n = clusterLookupNode(updateMsg->getNodeName());
    if (!n) {
      /* We don't know the reported node. */
      return {ErrorCodes::ERR_OK, ""};
    }

    /* Nothing new. */
    if (n->getConfigEpoch() >= reportedConfigEpoch)
      return {ErrorCodes::ERR_OK, ""};

    /* If in our current config the node is a slave, set it as a master. */
    if (n->nodeIsSlave()) {
      if (clusterSetNodeAsMaster(n))
        save = true;
    }

    /* Update the node's configEpoch. */
    n->setConfigEpoch(reportedConfigEpoch);

    /* Check the bitmap of served slots and update our
     * config accordingly. */
    clusterUpdateSlotsConfigWith(n, reportedConfigEpoch, updateMsg->getSlots());

    save = true;
  } else {
    // TODO(wayenchen): other message
    INVARIANT_D(0);
    serverLog(
      LL_WARNING, "Received unknown packet type: %d", static_cast<int>(type));
  }
  return {ErrorCodes::ERR_OK, ""};
}

Status ClusterSession::clusterProcessPacket() {
  INVARIANT_D(_queryBuf.size() >= _pkgSize);
  auto emsg = ClusterMsg::msgDecode(std::string(_queryBuf.data(), _pkgSize));
  if (!emsg.ok()) {
    return emsg.status();
  }

  auto msg = emsg.value();
  auto hdr = msg.getHeader();

  uint32_t totlen = msg.getTotlen();
  auto type = msg.getType();

  serverLog(LL_DEBUG,
            "--- Processing packet of type %s, %u bytes",
            ClusterMsg::clusterGetMessageTypeString(type).c_str(),
            (uint32_t)totlen);

  if (totlen < 16 || totlen > _pkgSize) {
    return {ErrorCodes::ERR_DECODE, "invalid message len"};
  }

  return _server->getClusterMgr()->getClusterState()->clusterProcessPacket(
    shared_from_this(), msg);
}

Status ClusterSession::clusterReadHandler() {
  drainReqNet();
  return {ErrorCodes::ERR_OK, ""};
}

Status ClusterSession::clusterSendMessage(ClusterMsg& msg) {  // NOLINT
  setResponse(msg.msgEncode());
  return {ErrorCodes::ERR_OK, ""};
}

Status ClusterState::clusterSendUpdate(std::shared_ptr<ClusterSession> sess,
                                       CNodePtr node,
                                       uint64_t offset) {
  ClusterMsg msg(
    ClusterMsg::Type::UPDATE, shared_from_this(), _server, offset, node);
  _statsMessagesSent[uint16_t(ClusterMsg::Type::UPDATE)]++;
  return sess->clusterSendMessage(msg);
}

void ClusterSession::setNode(const CNodePtr& node) {
  _node = node;
}


std::string ClusterSession::nodeIp2String(
  const std::string& announcedIp) const {
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

Status ClusterState::clusterSendPing(std::shared_ptr<ClusterSession> sess,
                                     ClusterMsg::Type type,
                                     uint64_t offset) {
  std::lock_guard<myMutex> lock(_mutex);
  auto s = clusterSendPingNoLock(sess, type, offset);
  return s;
}

/* Send a PING or PONG packet to the specified node, making sure to add enough
 * gossip informations. */
Status ClusterState::clusterSendPingNoLock(std::shared_ptr<ClusterSession> sess,
                                           ClusterMsg::Type type,
                                           uint64_t offset) {
  if (!sess) {
    return {ErrorCodes::ERR_OK, ""};
  }
  if (sess->getNode()) {
    DLOG(INFO) << "send message:" << sess->getNode()->getNodeName()
               << "type:" << clusterMsgTypeString(type) << "ip"
               << sess->getNode()->getNodeIp();
  }
  uint32_t gossipcount = 0; /* Number of gossip sections added so far. */
  uint32_t
    wanted; /* Number of gossip sections we want to append if possible. */
  /* freshnodes is the max number of nodes we can hope to append at all:
   * nodes available minus two (ourself and the node we are sending the
   * message to). However practically there may be less valid nodes since
   * nodes in handshake state, disconnected, are not considered. */
  uint32_t nodeCount = getNodeCount();
  uint32_t freshnodes = nodeCount - 2;

  /* How many gossip sections we want to add? 1/10 of the
   * number of nodes
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
  if (wanted < 3)
    wanted = 3;
  if (wanted > freshnodes)
    wanted = freshnodes;

  /* Include all the nodes in PFAIL state, so that failure reports are
   * faster to propagate to go from PFAIL to FAIL state. */
  uint32_t pfail_wanted = getPfailNodeNum();

  /* Populate the header. */
  auto sessNode = sess->getNode();
  if (sessNode && type == ClusterMsg::Type::PING) {
    sessNode->setSentTime(msSinceEpoch());
  }
  ClusterMsg msg(type, shared_from_this(), _server, offset);
  /* Populate the gossip fields */
  uint32_t maxiterations = wanted * 3;
  while (freshnodes > 0 && gossipcount < wanted && maxiterations--) {
    auto node = getRandomNode();

    /* Don't include this node: the whole packet header is about us
     * already, so we just gossip about other nodes. */
    if (node == _myself)
      continue;

    /* PFAIL nodes will be added later. */
    if (node->_flags & CLUSTER_NODE_PFAIL)
      continue;

    /* In the gossip section don't include:
     * 1) Nodes in HANDSHAKE state.
     * 3) Nodes with the NOADDR flag set.
     * 4) Disconnected nodes if they don't have configured slots.
     */
    if (node->_flags & (CLUSTER_NODE_HANDSHAKE | CLUSTER_NODE_NOADDR) ||
        (node->getSession() == nullptr && node->getSlotNum() == 0)) {
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

  if (pfail_wanted) {
    auto nodeList = getNodesList();
    for (const auto& v : nodeList) {
      CNodePtr node = v.second;
      if (node->_flags & CLUSTER_NODE_HANDSHAKE)
        continue;
      if (node->_flags & CLUSTER_NODE_NOADDR)
        continue;
      if (!(node->_flags & CLUSTER_NODE_PFAIL))
        continue;
      msg.clusterAddGossipEntry(node);
      freshnodes--;
      gossipcount++;
    }
  }
  INVARIANT_D(gossipcount == msg.getEntryCount());

  _statsMessagesSent[uint16_t(type)]++;
  return sess->clusterSendMessage(msg);
}

}  // namespace tendisplus
