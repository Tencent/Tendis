#ifndef SRC_TENDISPLUS_SERVER_GOSSIP_MANAGER_H_
#define SRC_TENDISPLUS_SERVER_GOSSIP_MANAGER_H_

#include <string>
#include <vector>
#include <memory>
#include <array>
#include <utility>
#include <list>
#include <unordered_map>
#include <bitset>
#include "tendisplus/server/server_entry.h"
#include "tendisplus/network/network.h"

namespace tendisplus {

enum class ClusterHealth: std::uint8_t {
    CLUSTER_FAIL = 0,
    CLUSTER_OK = 1,  
};


#define CLUSTER_SLOTS 16384

#define CLUSTER_NAMELEN 40    /* sha1 hex length */
#define CLUSTER_PORT_INCR 10000 /* Cluster port = baseport + PORT_INCR */

/* Cluster node flags and macros. */
#define CLUSTER_NODE_MASTER 1     /* The node is a master */
#define CLUSTER_NODE_SLAVE 2      /* The node is a slave */
#define CLUSTER_NODE_PFAIL 4      /* Failure? Need acknowledge */
#define CLUSTER_NODE_FAIL 8       /* The node is believed to be malfunctioning */
#define CLUSTER_NODE_MYSELF 16    /* This node is myself */
#define CLUSTER_NODE_HANDSHAKE 32 /* We have still to exchange the first ping */
#define CLUSTER_NODE_NOADDR   64  /* We don't know the address of this node */
#define CLUSTER_NODE_MEET 128     /* Send a MEET message to this node */
#define CLUSTER_NODE_MIGRATE_TO 256 /* Master elegible for replica migration. */
#define CLUSTER_NODE_NOFAILOVER 512 /* Slave will not try to failver. */
#define CLUSTER_NODE_NULL_NAME "\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000"

/* Redirection errors returned by getNodeByQuery(). */
#define CLUSTER_REDIR_NONE 0          /* Node can serve the request. */
#define CLUSTER_REDIR_CROSS_SLOT 1    /* -CROSSSLOT request. */
#define CLUSTER_REDIR_UNSTABLE 2      /* -TRYAGAIN redirection required */
#define CLUSTER_REDIR_ASK 3           /* -ASK redirection required. */
#define CLUSTER_REDIR_MOVED 4         /* -MOVED redirection required. */
#define CLUSTER_REDIR_DOWN_STATE 5    /* -CLUSTERDOWN, global state. */
#define CLUSTER_REDIR_DOWN_UNBOUND 6  /* -CLUSTERDOWN, unbound slot. */


/* Reasons why a slave is not able to failover. */
#define CLUSTER_CANT_FAILOVER_NONE 0
#define CLUSTER_CANT_FAILOVER_DATA_AGE 1
#define CLUSTER_CANT_FAILOVER_WAITING_DELAY 2
#define CLUSTER_CANT_FAILOVER_EXPIRED 3
#define CLUSTER_CANT_FAILOVER_WAITING_VOTES 4
#define CLUSTER_CANT_FAILOVER_RELOG_PERIOD (60*5) /* seconds. */

/* clusterState todo_before_sleep flags. */
#define CLUSTER_TODO_HANDLE_FAILOVER (1<<0)
#define CLUSTER_TODO_UPDATE_STATE (1<<1)
#define CLUSTER_TODO_SAVE_CONFIG (1<<2)
#define CLUSTER_TODO_FSYNC_CONFIG (1<<3)

/* Message types.
 *
 * Note that the PING, PONG and MEET messages are actually the same exact
 * kind of packet. PONG is the reply to ping, in the exact format as a PING,
 * while MEET is a special PING that forces the receiver to add the sender
 * as a node (if it is not already in the list). */
#define CLUSTERMSG_TYPE_PING 0          /* Ping */
#define CLUSTERMSG_TYPE_PONG 1          /* Pong (reply to Ping) */
#define CLUSTERMSG_TYPE_MEET 2          /* Meet "let's join" message */
#define CLUSTERMSG_TYPE_FAIL 3          /* Mark node xxx as failing */
#define CLUSTERMSG_TYPE_PUBLISH 4       /* Pub/Sub Publish propagation */
#define CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST 5 /* May I failover? */
#define CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK 6     /* Yes, you have my vote */
#define CLUSTERMSG_TYPE_UPDATE 7        /* Another node slots configuration */
#define CLUSTERMSG_TYPE_MFSTART 8       /* Pause clients for manual failover */
#define CLUSTERMSG_TYPE_COUNT 9         /* Total number of message types. */

/*
#define nodeIsMaster(n) ((n)->_flag & CLUSTER_NODE_MASTER)
#define nodeIsSlave(n) ((n)->_flag & CLUSTER_NODE_SLAVE)
#define nodeInHandshake(n) ((n)->_flag & CLUSTER_NODE_HANDSHAKE)
#define nodeHasAddr(n) (!((n)->_flag & CLUSTER_NODE_NOADDR))
#define nodeWithoutAddr(n) ((n)->_flag & CLUSTER_NODE_NOADDR)
#define nodeTimedOut(n) ((n)->_flag & CLUSTER_NODE_PFAIL)
#define nodeFailed(n) ((n)->_flag & CLUSTER_NODE_FAIL)
#define nodeCantFailover(n) ((n)->_flag & CLUSTER_NODE_NOFAILOVER)
*/
using  mstime_t = uint64_t;


class ClusterNode {
 public:
    explicit ClusterNode(const uint16_t flag);
    ClusterNode(const std::string& name, const uint16_t flag);
 //   ClusterNode();
    ClusterNode(const ClusterNode&) = delete;
    ClusterNode(ClusterNode&&) = delete;

    //get node name
    std::string getNodeName() const { return _nodeName; }    
   
    void setNodeName(const std::string& name) ;
    
    uint64_t getPort() const { return _nodePort; }
    void setNodePort(uint64_t port);
    
    uint64_t getCport() const { return _nodeCport; }
    void setNodeCport(uint64_t cport);
    
    std::vector<std::shared_ptr<ClusterNode>> getSlaves() const { return _slaves; }
    std::bitset<CLUSTER_SLOTS> getSlots() const { return _mySlots; }

    std::string getNodeIp() const {return _nodeIp;}
    void setNodeIp(const std::string& name);

    uint64_t getConfigEpoch() const { return _configEpoch; }
    void setConfigEpoch(uint64_t epoch) ;

    Status addSlot(uint64_t slot);
    Status addSlave(std::shared_ptr<ClusterNode> slave);
    
    bool nodeIsMaster();
    bool nodeIsSlave();
 private: 

    std::string _nodeName;
    uint64_t _configEpoch;
    std::string _nodeIp;  /* Latest known IP address of this node */
    uint64_t _nodePort;  /* Latest known clients port of this node */
    uint64_t _nodeCport;  /* Latest known cluster port of this node. */
 public:
 
    mstime_t _ctime;
    uint16_t _flag;
    std::bitset<CLUSTER_SLOTS> _mySlots;  /* slots handled by this node */
    uint16_t _numSlots;
    uint16_t _numSlaves;

    std::vector<std::shared_ptr<ClusterNode>> _slaves;
    std::shared_ptr<ClusterNode> _slaveOf;
    
    mstime_t _pingSent;
    mstime_t _pongReceived;
    mstime_t _failTime;
    mstime_t _votedTime;
    mstime_t _replOffsetTime;
    mstime_t _orphanedTime;
    
    uint64_t _replOffset;  /* Last known repl offset for this node. */
    std::list<std::shared_ptr<ClusterNode>> _failReport; 
};

using CNodePtr = std::shared_ptr<ClusterNode>;

class ClusterState: public std::enable_shared_from_this<ClusterState> {

 public:
    ClusterState();
    ClusterState(const ClusterState&) = delete;
    ClusterState(ClusterState&&) = delete;
    
    //get epoch
    uint64_t getCurrentEpoch() const { return _currentEpoch;}
     // set epoch
    void setCurrentEpoch(uint64_t epoch);   
    //get myself
    CNodePtr  getMyselfNode() const { return _myself;}
    std::string getMyselfName() const { return _myself->getNodeName();}
    //set myself
    void setMyselfNode(CNodePtr node);
    //addNode
    void addClusterNode(CNodePtr node);
   

    Expected<CNodePtr> clusterLookupNode(const std::string& name);
    Status  clusterNodeAddSlave(CNodePtr master, CNodePtr slave);
    //Status ClusterState::clusterMaster(CNodePtr n);
    Status clusterAddSlot(CNodePtr n, const uint64_t slot);
    Status setSlot(CNodePtr n, uint64_t slot);
    void setSlotBelong(CNodePtr n, const uint64_t slot);
    
    Expected<std::unordered_map<std::string,CNodePtr>> getNodesList();
    ClusterHealth getClusterState() const { return _state; }


   //to do(wayenchen)
   // Status clusterReadMeta();
   // Status clusterDelSlot(uint64_t slot);
   // Status clusterDelNodeSlots(CNodePtr n);
   // Status clusterDelNode(CNodePtr delnode);
   //Status setStateFail();
   //void clusterUpdateState();
 private:
    CNodePtr _myself; /* This node */
    uint64_t _currentEpoch;

 public:
    ClusterHealth _state;
    uint16_t _size;
    
    std::unordered_map<std::string,CNodePtr> _nodes;
    std::unordered_map<std::string,CNodePtr> _nodesBackList;
    std::array<CNodePtr, CLUSTER_SLOTS> _migratingSlots;
    std::array<CNodePtr, CLUSTER_SLOTS> _importingSlots;
    std::array<CNodePtr, CLUSTER_SLOTS> _allSlots;
    
    std::array<uint64_t,CLUSTER_SLOTS> _slotsKeysCount;
    //rax *slots_to_keys;
    mstime_t _failoverAuthTime;
    uint16_t _failoverAuthCount;    /* Number of votes received so far. */
    uint16_t _failoverAuthSent;     /* True if we already asked for votes. */
    uint16_t _failoverAuthRank;     /* This slave rank for current auth request. */
    uint64_t _failoverAuthEpoch; /* Epoch of the current election. */
    uint8_t _cantFailoverReason;   /* Why a slave is currently not able to failover*/
    uint64_t _lastVoteEpoch;     /* Epoch of the last vote granted. */
   
   
    uint8_t _todoBeforeSleep; /* Things to do in clusterBeforeSleep(). */
    /* Messages received and sent by type. */
    std::array<uint64_t,CLUSTERMSG_TYPE_COUNT> _statsMessagesSent;
    std::array<uint64_t,CLUSTERMSG_TYPE_COUNT> _statsMessagesReceived;
    long long _statsPfailNodes;    /* Number of nodes in PFAIL status */  

};

class ClusterMsgDataHeader;
class ClusterMsgData;
class ClusterMsg {
 public:
    ClusterMsg(const std::shared_ptr<ClusterMsgDataHeader>& header, const std::shared_ptr<ClusterMsgData>& data);
    ClusterMsg(const ClusterMsg&) = delete;
    ClusterMsg(ClusterMsg&&) = delete;

//   void clusterSetGossipEntry(int i, clusterNode *n) 
    std::string msgEncode() const;
    
//string encode  
 private:
    std::shared_ptr<ClusterMsgDataHeader> _header;
    std::shared_ptr<ClusterMsgData> _msgData;
};

class ClusterMsgDataHeader{
 public:
    ClusterMsgDataHeader(const uint16_t type, const std::shared_ptr<ClusterNode> cnode,
            const std::shared_ptr<ClusterState> cstate, const std::shared_ptr<ServerEntry> svr);
 
    ClusterMsgDataHeader(const std::string& sig, const uint64_t totlen,
                const uint16_t ver, const uint16_t port ,
                const uint16_t type, const uint16_t count,
                const uint64_t currentEpoch, const uint64_t configEpoch,
                const uint64_t offset , const std::string& sender,
                const std::bitset<CLUSTER_SLOTS>& slots, const std::string& slaveOf,
                const std::string& myIp,  const uint16_t cport,
                const uint16_t flags , ClusterHealth state);

    ClusterMsgDataHeader(const ClusterMsgDataHeader&) = delete;
    ClusterMsgDataHeader(ClusterMsgDataHeader&&);

  // std::string _mflags; /* Message flags: CLUSTERMSG_FLAG[012]_... */

    std::string headEncode() const; 
    Expected<ClusterMsgDataHeader> headDecode(const std::string& key);

    std::string _sig;
    uint64_t _totlen;
    uint16_t _ver;
    uint16_t _port;    /* TCP base port number.*/
    uint16_t _type;    /* Message type */ 
    uint16_t _count;    
    uint64_t _currentEpoch;
    uint64_t _configEpoch;
    uint64_t _offset;

    std::string _sender; /* Name of the sender node */
    std::bitset<CLUSTER_SLOTS> _slots;
    std::string _slaveOf;
    std::string _myIp; /* Sender IP, if not all zeroed. */
    uint16_t _cport;      /* Sender TCP cluster bus port */
    uint16_t _flags;      /* Sender node flags */
    ClusterHealth _clusterState; /* Cluster state from the POV of the sender */
};


class ClusterMsgData{
 public:
 //  ClusterMsgData();
   virtual std::string dataEncode() const = 0;
   virtual  std::shared_ptr<ClusterMsgData> dataDecode(const std::string& key) = 0;
   virtual ~ClusterMsgData() {};
};

class ClusterMsgDataUpdate: public ClusterMsgData {
 public:
    ClusterMsgDataUpdate(const uint64_t configEpoch, const std::string &nodeName,
            const std::bitset<CLUSTER_SLOTS> &_slots);
    explicit ClusterMsgDataUpdate(std::shared_ptr<ClusterNode> node);                     
    ClusterMsgDataUpdate(const ClusterMsgDataUpdate&) = delete;
    ClusterMsgDataUpdate(ClusterMsgDataUpdate&&) = default;
    virtual ~ClusterMsgDataUpdate() {};
    
    virtual std::string dataEncode() const override;
    virtual std::shared_ptr<ClusterMsgData> dataDecode(const std::string& key) override;
 
    uint64_t _configEpoch; 
    std::string _nodeName;
    std::bitset<CLUSTER_SLOTS> _slots;
};

class ClusterGossip ;
class ClusterMsgDataGossip: public ClusterMsgData {
 public:
    explicit ClusterMsgDataGossip(vector<ClusterGossip>&& gossipMsg);
    ClusterMsgDataGossip(const ClusterMsgDataGossip&) = delete;
    ClusterMsgDataGossip(ClusterMsgDataGossip&&) = delete;
    virtual ~ClusterMsgDataGossip() { _gossipMsg.clear();};
    virtual std::string dataEncode() const override;
    virtual std::shared_ptr<ClusterMsgData> dataDecode(const std::string& key) override;

    std::vector<ClusterGossip> _gossipMsg;
};


class ClusterGossip {
 public:
   explicit ClusterGossip(const std::shared_ptr<ClusterNode> node);

   ClusterGossip(const std::string& gossipName, const uint32_t pingSent,
                const uint32_t pongReceived, const std::string& gossipIp,
                const uint16_t gossipPort, const uint16_t gossipCport,
                uint16_t gossipFlags);

   ClusterGossip(const ClusterGossip&) = default;
   ClusterGossip(ClusterGossip&&) = default;
 
   std::string gossipEncode() const;
   static Expected<ClusterGossip> gossipDecode(const std::string& key);
   std::string _gossipName;
   uint32_t _pingSent;
   uint32_t _pongReceived;
   std::string _gossipIp;  
   uint16_t _gossipPort;              //base port last time it was seen
   uint16_t _gossipCport;             // cluster port last time it was seen 
   uint16_t _gossipFlags;             //node->flags copy 
};

class ClusterNode;
class NetworkAsio;
class NetworkMatrix;
class RequestMatrix;
class ClusterMeta;
class ClusterManager {
 public:
    
    ClusterManager(const std::shared_ptr<ServerEntry>& svr, const std::shared_ptr<ClusterNode>& node,
                 const std::shared_ptr<ClusterState>& state);
    
    explicit ClusterManager(const std::shared_ptr<ServerEntry>& svr);
    ClusterManager(const ClusterManager&) = delete;
    ClusterManager(ClusterManager&&) = delete;
    
    Status startup();   
    Status initNetWork();
    Status initMetaData();
    void installClusterNode(std::shared_ptr<ClusterNode>);
    void installClusterState(std::shared_ptr<ClusterState>);

    NetworkAsio* getClusterNetwork();
    //void stop();
   // Status run(); 
   // bool isRunning();
   // Status clusterSendPing(uint16_t type);
   //  Status clusterSendUpdate();
 private:
    std::mutex _mutex;
    std::shared_ptr<ServerEntry> _svr;
    std::shared_ptr<ClusterNode> _clusterNode;
    std::shared_ptr<ClusterState> _clusterState;
    std::unique_ptr<NetworkAsio> _clusterNetwork;
    
    //work pool
    std::unique_ptr<WorkerPool> _gossipMessgae;
    std::shared_ptr<PoolMatrix> _gossipMatrix;
    uint16_t _megPoolSize;
    //encode模块 

    //decode 模块

    //定时任务模块

    std::shared_ptr<NetworkMatrix> _netMatrix;
    std::shared_ptr<RequestMatrix> _reqMatrix;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_SERVER_GOSSIP_MANAGER_H_
