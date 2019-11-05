#include "tendisplus/utils/time.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/server/cluster_manager.h"
#include "tendisplus/storage/catalog.h"
#include "tendisplus/storage/varint.h"

namespace tendisplus {

template <typename T>
void CopyUint(std::vector<uint8_t> *buf, T element) {
   for (size_t i = 0; i < sizeof(element); ++i) {
        buf->emplace_back((element>>((sizeof(element)-i-1)*8))&0xff);
    }
}


inline ClusterHealth int2State(const uint8_t t) {
    if(t == 1) { return ClusterHealth::CLUSTER_OK; }
    else {
        return ClusterHealth::CLUSTER_FAIL;
    }
}


ClusterNode::ClusterNode(const std::string& nodeName,
                    const uint16_t flagName)
    :_nodeName(nodeName),
     _configEpoch(0),
     _nodeIp(""),
     _nodePort(0),
     _nodeCport(0),
     _ctime(msSinceEpoch()),
     _flag(flagName),
     _numSlots(0),
     _numSlaves(0),
     _slaveOf(nullptr),
     _pingSent(0),
     _pongReceived(0),
     _votedTime(0),
     _replOffsetTime(0),
     _orphanedTime(0),
     _replOffset(0) {        
}

ClusterNode::ClusterNode(const uint16_t flagName)
    :_nodeName("TendisNode"+getUUid(40)),
     _configEpoch(0),
     _nodeIp(""),
     _nodePort(0),
     _nodeCport(0),
     _ctime(msSinceEpoch()),
     _flag(flagName),
     _numSlots(0),
     _numSlaves(0),
     _slaveOf(nullptr),
     _pingSent(0),
     _pongReceived(0),
     _votedTime(0),
     _replOffsetTime(0),
     _orphanedTime(0),
     _replOffset(0) {        
}


void ClusterNode::setNodeName(const std::string& name) {
    _nodeName = name;
}

void ClusterNode::setNodeIp(const std::string& ip) {
    _nodeIp = ip;
}
void ClusterNode::setNodePort(uint64_t port) {
    _nodePort = port;
}

void ClusterNode::setNodeCport(uint64_t cport){
    _nodeCport = cport;
}

void ClusterNode::setConfigEpoch(uint64_t epoch) {
    _configEpoch = epoch;
}

Status ClusterNode::addSlot(uint64_t slot) {
    _mySlots.set(slot);
    _numSlots ++;
    return  {ErrorCodes::ERR_OK,""};
}

Status ClusterNode::addSlave(std::shared_ptr<ClusterNode> slave){
    _slaves.push_back(std::move(slave));
    _numSlaves++;
    _flag |= CLUSTER_NODE_MIGRATE_TO; 
    return  {ErrorCodes::ERR_OK,""};
}

bool  ClusterNode::nodeIsMaster() {
    if(_flag & CLUSTER_NODE_MASTER) { return true; }
    else{
        return false;
    }
}

bool  ClusterNode::nodeIsSlave() {
    if(_flag & CLUSTER_NODE_SLAVE) { return true; }
    else{
        return false;
    }
}

ClusterState::ClusterState()
    :_myself(nullptr),
     _currentEpoch(0),
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
     _lastVoteEpoch(0),
     _todoBeforeSleep(0),
     _statsMessagesSent({}),
     _statsMessagesReceived({}),
     _statsPfailNodes(0) {        
}


void ClusterState::setCurrentEpoch(uint64_t epoch) {
    _currentEpoch = epoch;
}

void ClusterState::setSlotBelong(CNodePtr n, const uint64_t slot) {
    _allSlots[slot] = n;
}

void ClusterState::setMyselfNode(CNodePtr node) {
    INVARIANT(node != nullptr);
    if(!_myself) {
        _myself = node;
    }
}

void ClusterState::addClusterNode(CNodePtr node) {
    std::string nodeName = node->getNodeName();
    std::unordered_map<std::string,CNodePtr>::iterator it;
    if ((it = _nodes.find(nodeName)) != _nodes.end()) {
        _nodes[nodeName] = it->second;
    }else{
        _nodes.insert(std::make_pair(nodeName,node));
    }
}

Status ClusterState::setSlot(CNodePtr n, uint64_t slot) { 
    std::bitset<CLUSTER_SLOTS> arr = n->getSlots();
    if(!arr.test(slot)) {     
        Status s = n->addSlot(slot);
        if (!s.ok()) {
           return s;
        }
    } else{
        return {ErrorCodes::ERR_NOTFOUND,""};
    }
    return {ErrorCodes::ERR_OK,""};
}

Status ClusterState::clusterNodeAddSlave(CNodePtr master, CNodePtr slave) {
    for(auto v: master->getSlaves()){
        if(v==slave)  return {ErrorCodes::ERR_NOTFOUND,"already is slave"};
    }
    Status s = master->addSlave(slave);

    if (!s.ok()) {
        return s;
    }
    return {ErrorCodes::ERR_OK,""};
}

/* find if node in cluster, */

Expected<CNodePtr> ClusterState::clusterLookupNode(const std::string& name) {
    std::unordered_map<std::string,CNodePtr>::iterator it;

    if ((it = _nodes.find(name)) != _nodes.end()) {
        return it->second;
    }else {
        return {ErrorCodes::ERR_NOTFOUND,"look up fail"};
    }

}

Expected<std::unordered_map<std::string,CNodePtr>> ClusterState::getNodesList() {
    if(_nodes.empty()) {
        return {ErrorCodes::ERR_NOTFOUND,"nodes list empty"};
    }else {
        return _nodes;
    }
}

/* Add the specified slot to the list of slots that node 'n' will
 * serve. Return C_OK if the operation ended with success.
 * If the slot is already assigned to another instance this is considered
 * an error and C_ERR is returned. */

Status ClusterState::clusterAddSlot(CNodePtr node, const uint64_t slot) {

    if(_allSlots[slot] != nullptr || _allSlots[slot] != node) {     
        return {ErrorCodes::ERR_NOTFOUND,"add slot fail"};
    }
    else {
         setSlot(node,slot);
         setSlotBelong(node,slot);
         return {ErrorCodes::ERR_OK,""};;
    }
}

ClusterMsg::ClusterMsg(const std::shared_ptr<ClusterMsgDataHeader>& header, const std::shared_ptr<ClusterMsgData>& data)
    :_header(header),
     _msgData(data) {
}

std::string ClusterMsg::msgEncode() const{
    std::vector<uint8_t> key;

    std::string head = _header->headEncode();
    key.insert(key.end(), head.begin(), head.end());
    
    std::string data = _msgData->dataEncode();
    key.insert(key.end(), data.begin(), data.end());

    return std::string(reinterpret_cast<const char *>(
                key.data()), key.size());

}


ClusterMsgDataHeader::ClusterMsgDataHeader(const uint16_t type, const std::shared_ptr<ClusterNode> cnode,
            const std::shared_ptr<ClusterState> cstate, const std::shared_ptr<ServerEntry> svr)
    :_sig("RCmb"),
     _ver(0),
     _type(type),
     _count(0),
     _currentEpoch(cstate->getCurrentEpoch()),
     _sender(cnode->getNodeName()),
     _slaveOf(cnode->_slaveOf->getNodeName()),
     _flags(cnode->_flag),
     _clusterState(cstate->_state)  {
        std::shared_ptr<ClusterNode> master = ( cnode->nodeIsSlave() && cnode->_slaveOf) ? cnode->_slaveOf : cnode; 
        _configEpoch = master->getConfigEpoch();
        _slots = master->_mySlots;
        std::shared_ptr<ServerParams> params = svr->getParams();
        _myIp = params->bindIp;
        _port = params->port;
        _cport = _port + CLUSTER_PORT_INCR;
        if (type == CLUSTERMSG_TYPE_UPDATE) {
            _totlen = sizeof(ClusterMsg)-sizeof(ClusterMsgData);
            _totlen += sizeof(ClusterMsgDataUpdate);
        } else {
            _totlen = 0;
        }
     //  _offset = params->master_repl_offset;  
}


ClusterMsgDataHeader::ClusterMsgDataHeader(const std::string& sig, const uint64_t totlen,
                const uint16_t ver, const uint16_t port ,
                const uint16_t type, const uint16_t count,
                const uint64_t currentEpoch, const uint64_t configEpoch,
                const uint64_t offset , const std::string& sender,
                const std::bitset<CLUSTER_SLOTS>& slots, const std::string& slaveOf,
                const std::string& myIp,  const uint16_t cport,
                const uint16_t flags , ClusterHealth state)
    :_sig(sig),
     _totlen(totlen),
     _ver(ver),
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
     _clusterState(state) {
}

ClusterMsgDataHeader::ClusterMsgDataHeader(ClusterMsgDataHeader&& o)
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
     _clusterState(std::move(o._clusterState)) {
}

std::string ClusterMsgDataHeader::headEncode() const{
    std::vector<uint8_t> key;
    key.reserve(2230);

    key.insert(key.end(), _sig.begin(), _sig.end());   

    CopyUint(&key,_totlen);
    CopyUint(&key,_ver);
    CopyUint(&key,_port);
    CopyUint(&key,_type);
    CopyUint(&key,_count);
    CopyUint(&key,_currentEpoch);
    CopyUint(&key,_configEpoch);
    CopyUint(&key,_offset);

    key.insert(key.end(), _sender.begin(), _sender.end());
    //_slots
    std::string slotString = _slots.to_string();
    key.insert(key.end(), slotString.begin(), slotString.end());

    key.insert(key.end(), _slaveOf.begin(), _slaveOf.end());
    key.insert(key.end(), _myIp.begin(), _myIp.end());

    CopyUint(&key,_cport);
    CopyUint(&key,_flags);

    const uint8_t *p = reinterpret_cast<const uint8_t*>(&_clusterState);
    static_assert(sizeof(_clusterState) == 1, "invalid clusterState size");
    key.insert(key.end(), p, p + (sizeof(_clusterState)));

    return std::string(reinterpret_cast<const char *>(
                key.data()), key.size());
}

Expected<ClusterMsgDataHeader> ClusterMsgDataHeader::headDecode(const std::string& key) {
    const size_t nameLen= 46;
    size_t offset = 0;

    std::string sig(key.c_str()+offset,4);
    offset +=4;

    auto decode = [&] (auto func) { auto n = func(key.c_str()+offset); offset+=sizeof(n); return n;};

    auto totlen = decode(int32Decode);
    auto ver = decode(int16Decode);
    auto port = decode(int16Decode);
    auto type = decode(int16Decode);
    auto count = decode(int16Decode);
    auto currentEpoch = decode(int64Decode);
    auto  configEpoch = decode(int64Decode);
    auto headOffset = decode(int64Decode);
 
    std::string sender(key.c_str()+offset,nameLen);
    offset += nameLen;

    std::string slotString(key.c_str()+offset,CLUSTER_SLOTS);
    std::bitset<CLUSTER_SLOTS> slots(slotString);
    offset += CLUSTER_SLOTS;

    std::string slaveOf(key.c_str()+offset,nameLen);
    offset += nameLen;

    //myIp = std::move(std::string(key.c_str()+offset,nameLen));
    std::string myIp(key.c_str()+offset,nameLen);
    offset += nameLen;

    uint16_t cport = decodeUint16(key,offset);
    uint16_t flags = decodeUint16(key,offset);

    ClusterHealth state = int2State(static_cast<uint8_t>(key.back()));

    return  ClusterMsgDataHeader(sig, totlen, ver,
                port, type, count, currentEpoch, configEpoch, headOffset, sender, slots, slaveOf, myIp, cport, flags, state);
    
}

ClusterMsgDataUpdate::ClusterMsgDataUpdate(const std::shared_ptr<ClusterNode> cnode)
    :_configEpoch(cnode->getConfigEpoch()),
     _nodeName(cnode->getNodeName()),
     _slots(cnode->_mySlots) {
}
 

ClusterMsgDataUpdate::ClusterMsgDataUpdate(const uint64_t configEpoch, const std::string &nodeName,
                const std::bitset<CLUSTER_SLOTS>& slots) 
    :_configEpoch(configEpoch),
     _nodeName(nodeName),
     _slots(slots) {
}

std::string ClusterMsgDataUpdate::dataEncode() const {
    std::vector<uint8_t> key;
    key.reserve(2110);

    //_configEpoch
    CopyUint(&key,_configEpoch);
    
    //_nodeName
    key.insert(key.end(), _nodeName.begin(), _nodeName.end());
    //_slots  std::bitset<CLUSTER_SLOTS>  
    //to do std::copy
    std::string slotString = _slots.to_string();
    key.insert(key.end(), slotString.begin(), slotString.end());

    return std::string(reinterpret_cast<const char *>(
                key.data()), key.size());

}

std::shared_ptr<ClusterMsgData> ClusterMsgDataUpdate::dataDecode(const std::string& key) {
    const size_t nameLen= 46;
    size_t offset = 0;
    auto decode = [&] (auto func, size_t size) { auto n = func(key.c_str()+offset); offset+=size; return n;};

    auto  configEpoch = decode(int64Decode,sizeof(uint64_t));

    std::string nodeName(key.c_str()+offset,nameLen);
    offset += nameLen;

    std::string slotString(key.c_str()+offset,CLUSTER_SLOTS);
    std::bitset<CLUSTER_SLOTS> slots(slotString);
    offset += CLUSTER_SLOTS;

    return std::make_shared<ClusterMsgDataUpdate>(configEpoch,nodeName,slots);
}


ClusterMsgDataGossip::ClusterMsgDataGossip(vector<ClusterGossip>&& gossipMsg)
    :_gossipMsg(std::move(gossipMsg)) {
}

std::string ClusterMsgDataGossip::dataEncode() const{
    const size_t gossipSize = 106;
    std::vector<uint8_t> key;

    uint16_t keySize= gossipSize*_gossipMsg.size();      
    key.reserve(keySize);

    for(auto& ax : _gossipMsg) {
        std::string content = ax.gossipEncode();
        key.insert(key.end(), content.begin(), content.end());    
    }
    return std::string(reinterpret_cast<const char *>(
                key.data()), key.size());
}

std::shared_ptr<ClusterMsgData> ClusterMsgDataGossip::dataDecode(const std::string& key) {

    const size_t gossipSize = 106;
    vector<ClusterGossip> gossipMsg;

    vector<string> res;
    auto it = key.begin();
  	for (; it < key.end(); it+=gossipSize){
	  	std::string temp(it,it+gossipSize);
        res.push_back(temp);
        auto gMsg = ClusterGossip::gossipDecode(temp);
        if (!gMsg.ok()) {
            return nullptr;
        }
        gossipMsg.push_back(gMsg.value());
	}

    return std::make_shared<ClusterMsgDataGossip>(std::move(gossipMsg));
}

ClusterGossip::ClusterGossip(const std::shared_ptr<ClusterNode> node)
    :_gossipName(node->getNodeName()),
     _pingSent(node->_pingSent/1000),
     _pongReceived(node->_pongReceived/1000),
     _gossipIp(node->getNodeIp()),
     _gossipPort(node->getPort()),              
     _gossipCport(node->getCport()),             
     _gossipFlags(node->_flag) {
}

ClusterGossip::ClusterGossip(const std::string& gossipName, const uint32_t pingSent,
                const uint32_t pongReceived, const std::string& gossipIp,
                const uint16_t gossipPort, const uint16_t gossipCport,
                uint16_t gossipFlags) 
    :_gossipName(gossipName),
     _pingSent(pingSent),
     _pongReceived(pongReceived),
     _gossipIp(gossipIp),
     _gossipPort(gossipPort),
     _gossipCport(gossipCport),
     _gossipFlags(gossipFlags) {
}

std::string ClusterGossip::gossipEncode() const{
    std::vector<uint8_t> key;
    key.reserve(126);
    //TendisNode-57731f1f4f95c376b22f59bb3728a413216573c01e3329d7a2a4357e0e5baaf81a89e476073fe2a6
    //_gossipNodeName
    key.insert(key.end(), _gossipName.begin(), _gossipName.end());

    //_pingSent  uint64_t
    CopyUint(&key,_pingSent);
    CopyUint(&key,_pongReceived);

    key.insert(key.end(), _gossipIp.begin(), _gossipIp.end());

    uint8_t ipLen = 46 - _gossipIp.size();
    std::vector<uint8_t> zeroVec(ipLen,'\0');
    
    key.insert(key.end(),zeroVec.begin(),zeroVec.end());
    //_gossipPort  uint64_t

    CopyUint(&key,_gossipPort);
    CopyUint(&key,_gossipCport);
    CopyUint(&key,_gossipFlags);

    return std::string(reinterpret_cast<const char *>(
                key.data()), key.size());
}

Expected<ClusterGossip> ClusterGossip::gossipDecode(const std::string& key) {
    const size_t nameLen= 46;
    size_t offset = 0;

    auto decode = [&] (auto func) { auto n = func(key.c_str()+offset); offset+=sizeof(n); return n;};

    std::string gossipName(key.c_str()+offset,nameLen);
    offset += nameLen;

    auto pingSent = decode(int32Decode);
    auto pongReceived = decode(int32Decode);

    std::string gossipIp(key.c_str()+offset,nameLen);
    offset += nameLen;
 
    auto gossipPort = decode(int16Decode);
    auto gossipCport = decode(int16Decode);
    auto gossipFlags = decode(int16Decode);

    return ClusterGossip(gossipName, pingSent, pongReceived, gossipIp, gossipPort, gossipCport, gossipFlags);
}

ClusterManager::ClusterManager(const std::shared_ptr<ServerEntry>& svr, const std::shared_ptr<ClusterNode>& node,
                const std::shared_ptr<ClusterState>& state)
    :_svr(svr),
     _clusterNode(node),
     _clusterState(state),
     _clusterNetwork(nullptr),
     _netMatrix(std::make_shared<NetworkMatrix>()),
     _reqMatrix(std::make_shared<RequestMatrix>()){
 }  

ClusterManager::ClusterManager(const std::shared_ptr<ServerEntry>& svr)
    :_svr(svr),
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


NetworkAsio* ClusterManager::getClusterNetwork() {
    return _clusterNetwork.get();
}

Status ClusterManager::initNetWork() {
    shared_ptr<ServerParams> cfg = _svr->getParams();
    _clusterNetwork = std::make_unique<NetworkAsio>(_svr,_netMatrix,
                                                _reqMatrix, cfg);

    Status s = _clusterNetwork->prepare(cfg->bindIp, cfg->port+CLUSTER_PORT_INCR, cfg->netIoThreadNum);
    if (!s.ok()) {
        return s;
    }
    // listener 
    s = _clusterNetwork->run();
    if (!s.ok()) {
        return s;
    } else {
        LOG(INFO) << "cluster network ready to accept connections at "
            << cfg->bindIp << ":" << cfg->port;
    }
    return {ErrorCodes::ERR_OK,"init network ok"};
}

Status ClusterManager::initMetaData() {

    Catalog *catalog = _svr->getCatalog();
    INVARIANT(catalog != nullptr);

    std::shared_ptr<ClusterState> gState = std::make_shared<tendisplus::ClusterState>();
    installClusterState(gState);

    auto vs = catalog->getClusterMeta();   
    if (vs.ok()) {
        //  LOG(INFO)<<"catalog nodeName is" <<vs.value()[0]->nodeName;
        int vssize = vs.value().size();
        INVARIANT(vssize >0);     
        LOG(INFO)<<"catalog nodeName is" <<vs.value()[0]->nodeName;
        return {ErrorCodes::ERR_OK,"init cluster from catalog "};

     } else if (vs.status().code() == ErrorCodes::ERR_NOTFOUND) {      
            
            const uint8_t flagName = CLUSTER_NODE_MYSELF|CLUSTER_NODE_MASTER;
            LOG(INFO)<<"start init clusterNode with flag:" << flagName;
            std::shared_ptr<ClusterNode>  gNode = std::make_shared<ClusterNode>(flagName);
            installClusterNode(gNode);
        
            _clusterState->addClusterNode(gNode);
            _clusterState->setMyselfNode(gNode);
            auto nodename = _clusterNode->getNodeName();
            LOG(INFO)<<"No cluster configuration found, I'm "<<nodename;
   
            //store clusterMeta 
            auto pVs = std::make_unique<ClusterMeta>(nodename);

            Status s = catalog->setClusterMeta(*pVs);
            if (!s.ok()) {
                LOG(FATAL) << "catalog setClusterMeta error:"<< s.toString();
                return s;
            } else {
                LOG(INFO) << "cluster metadata set finish "<< "store ClusterMeta Node name is" << pVs->nodeName << "ip address is " << pVs->ip << "node Flag is" << pVs->nodeFlag;
            }
    } else {
        return vs.status();
    }

    
    return  {ErrorCodes::ERR_OK,"init metadata ok"};
}

Status ClusterManager::startup() {
 
    std::lock_guard<std::mutex> lk(_mutex);
    Status s_meta = initMetaData();
    Status s_net = initNetWork();

    if (!s_meta.ok() || !s_net.ok()) {
        if(!s_meta.ok()) {   
            LOG(INFO)<<"init metadata fail"<< s_meta.toString();
            return s_meta;
        }else{
            LOG(INFO)<<"init network fail"<< s_net.toString();
            return s_net;
        }
    } else {  
        auto name = _clusterNode->getNodeName();
        auto state = _clusterState->getClusterState();
        std::string clusterState = (unsigned(state) > 0) ? "OK": "FAIL";
        LOG(INFO) << "cluster init sucess:"
            << " myself node name " << name << "cluster state is" << clusterState; 
    }

    _gossipMessgae = std::make_unique<WorkerPool>("gossip-data",
                                                   _gossipMatrix);
    Status s = _gossipMessgae->startup(_megPoolSize);
    if (!s.ok()) {
          return s;
    }

    std::shared_ptr<ServerParams> params = _svr->getParams();

    std::string nodeIp = params->bindIp;
    uint16_t nodePort = params->port;
    uint16_t nodeCport = nodePort+ CLUSTER_PORT_INCR;

    _clusterNode->setNodeIp(nodeIp);
    _clusterNode->setNodePort(nodePort);
    _clusterNode->setNodeCport(nodeCport);
   
    return {ErrorCodes::ERR_OK,"init cluster finish"};
}

}  // namespace tendisplus
