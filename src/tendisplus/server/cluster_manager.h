#ifndef SRC_TENDISPLUS_SERVER_CLUSTER_MANAGER_H_
#define SRC_TENDISPLUS_SERVER_CLUSTER_MANAGER_H_

#include <string>
#include <vector>
#include <memory>
#include <array>
#include <utility>
#include <list>
#include <unordered_map>
#include <algorithm>
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

// TODO(wayenchen)
#define CLUSTERMSG_MIN_LEN 100

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

///* Message types.
// *
// * Note that the PING, PONG and MEET messages are actually the same exact
// * kind of packet. PONG is the reply to ping, in the exact format as a PING,
// * while MEET is a special PING that forces the receiver to add the sender
// * as a node (if it is not already in the list). */
//#define CLUSTERMSG_TYPE_PING 0          /* Ping */
//#define CLUSTERMSG_TYPE_PONG 1          /* Pong (reply to Ping) */
//#define CLUSTERMSG_TYPE_MEET 2          /* Meet "let's join" message */
//#define CLUSTERMSG_TYPE_FAIL 3          /* Mark node xxx as failing */
//#define CLUSTERMSG_TYPE_PUBLISH 4       /* Pub/Sub Publish propagation */
//#define CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST 5 /* May I failover? */
//#define CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK 6     /* Yes, you have my vote */
//#define CLUSTERMSG_TYPE_UPDATE 7        /* Another node slots configuration */
//#define CLUSTERMSG_TYPE_MFSTART 8       /* Pause clients for manual failover */
//#define CLUSTERMSG_TYPE_COUNT 9         /* Total number of message types. */

#define CLUSTER_IP_LENGTH  46
#define CLUSTER_NAME_LENGTH  40

using  mstime_t = uint64_t;

class ClusterSession;
class ClusterState;
class ClusterMsg;

class ClusterNode : public std::enable_shared_from_this<ClusterNode> {
    friend class ClusterState;
 public:
    ClusterNode(const std::string& name, const uint16_t flags,
                        std::shared_ptr<ClusterState> cstate,
                        const std::string& host = "",
                        uint32_t port = 0, uint32_t cport = 0,
                        uint64_t pingSend = 0, uint64_t pongReceived = 0,
                        uint64_t epoch = 0, const std::vector<std::string>& slots_ = {""});
 
    ClusterNode(const ClusterNode&) = delete;
    ClusterNode(ClusterNode&&) = delete;

    void prepareToFree();

    // get node name
    std::string getNodeName() const { return _nodeName; }
    void setNodeName(const std::string& name);

    uint64_t getPort() const { return _nodePort; }
    void setNodePort(uint64_t port);

    uint64_t getCport() const { return _nodeCport; }
    void setNodeCport(uint64_t cport);

    //std::vector<std::shared_ptr<ClusterNode>> getSlaves() const { return _slaves; }
    //std::bitset<CLUSTER_SLOTS> getSlots() const { return _mySlots; }

    std::string getNodeIp() const {return _nodeIp;}
    void setNodeIp(const std::string& name);

    uint64_t getConfigEpoch() const { return _configEpoch; }
    void setConfigEpoch(uint64_t epoch);

    uint16_t getFlags() const { return _flags; }



    bool addFailureReport(std::shared_ptr<ClusterNode> sender);
    bool delFailureReport(std::shared_ptr<ClusterNode> sender);
    uint32_t getNonFailingSlavesCount() const;
    uint32_t failureReportsCount();

    void markAsFailingIfNeeded();

    std::shared_ptr<ClusterNode> getMaster() const { return _slaveOf; }
    void setMaster(const std::shared_ptr<ClusterNode>& master);
    bool updateAddressIfNeeded(ClusterSession* sess, const ClusterMsg& msg);
    void setAsMaster();

    bool changeToSlaveIfNeeded();

    bool nodeIsMaster() const;
    bool nodeIsSlave() const;
    bool nodeInHandshake() const;
    bool nodeHasAddr() const;
    bool nodeWithoutAddr() const;
    bool nodeTimedOut() const;
    bool nodeFailed() const;
    bool nodeCantFailover() const;

    bool nodeIsMyself() const;

    const std::shared_ptr<ClusterSession> getSession() const {
        return _nodeSession;
    }

    void setSession(const std::shared_ptr<ClusterSession>& sess);

    bool getSlotBit(uint32_t slot) const;

 protected:
    Status addSlot(uint32_t slot);
    bool setSlotBit(uint32_t slot, uint32_t masterSlavesCount);
    bool clearSlotBit(uint32_t slot);
    uint32_t delAllSlots();
    uint32_t delAllSlotsNoLock();

 public:
    Status addSlave(std::shared_ptr<ClusterNode> slave);
    Status removeSlave(std::shared_ptr<ClusterNode> slave);

 private:
    mutable std::mutex _mutex;
    std::string _nodeName;
    uint64_t _configEpoch;
    std::string _nodeIp;  /* Latest known IP address of this node */
    uint64_t _nodePort;  /* Latest known clients port of this node */
    uint64_t _nodeCport;  /* Latest known cluster port of this node. */
    std::shared_ptr<ClusterState> _clusterState;
    std::shared_ptr<ClusterSession> _nodeSession; /* TCP/IP session with this node */
    void cleanupFailureReportsNoLock();

// TODO(wayenchen): make it private
 public:
    mstime_t _ctime;
    uint16_t _flags;
    std::bitset<CLUSTER_SLOTS> _mySlots;  /* slots handled by this node */
    uint16_t _numSlots;
    uint16_t _numSlaves;

    std::vector<std::shared_ptr<ClusterNode>> _slaves;
    std::shared_ptr<ClusterNode> _slaveOf;
    uint64_t _pingSent;
    uint64_t _pongReceived;
    mstime_t _failTime;
    mstime_t _votedTime;
    mstime_t _replOffsetTime;
    mstime_t _orphanedTime;
    // FIXME: there is no offset in tendis
    uint64_t _replOffset;  /* Last known repl offset for this node. */
    std::list<std::shared_ptr<ClusterNode>> _failReport;
};

using CNodePtr = std::shared_ptr<ClusterNode>;

class ClusterMsgHeader;
class ClusterMsgData;
class ClusterMsg{
 public:
    /* Message types.
     *
     * Note that the PING, PONG and MEET messages are actually the same exact
     * kind of packet. PONG is the reply to ping, in the exact format as a PING,
     * while MEET is a special PING that forces the receiver to add the sender
     * as a node (if it is not already in the list). */
    enum class Type : uint16_t {
           PING = 0,          /* Ping */
           PONG = 1,          /* Pong (reply to Ping) */
           MEET = 2,         /* Meet "let's join" message */
           FAIL = 3,          /* Mark node xxx as failing */
           PUBLISH = 4,       /* Pub/Sub Publish propagation */
           FAILOVER_AUTH_REQUEST = 5, /* May I failover? */
           FAILOVER_AUTH_ACK = 6,     /* Yes, you have my vote */
           UPDATE = 7,        /* Another node slots configuration */
           MFSTART = 8,       /* Pause clients for manual failover */
    };
    static constexpr uint32_t CLUSTERMSG_TYPE_COUNT = 9;
    static constexpr uint32_t CLUSTER_PROTO_VER = 1;

    static std::string clusterGetMessageTypeString(Type type);

    ClusterMsg(const ClusterMsg::Type type,
        const std::shared_ptr<ClusterState> cstate,
        const std::shared_ptr<ServerEntry> svr);

    ClusterMsg(const std::shared_ptr<ClusterMsgHeader>& header,
            const std::shared_ptr<ClusterMsgData>& data);

    ClusterMsg(const ClusterMsg&) = default;
    ClusterMsg(ClusterMsg&&) = default;

    bool clusterNodeIsInGossipSection(const CNodePtr& node) const;
    void clusterAddGossipEntry(const CNodePtr& node);

    void setEntryCount(uint32_t count);
    uint32_t getEntryCount() const;

    bool isMaster() const;

    // void clusterSetGossipEntry(int i, clusterNode *n)
    std::string msgEncode() const;
    static Expected<ClusterMsg> msgDecode(const std::string& key);

    std::shared_ptr<ClusterMsgHeader> getHeader() const { return _header; }
    std::shared_ptr<ClusterMsgData> getData() const  { return _msgData; }

 private:
    std::shared_ptr<ClusterMsgHeader> _header;
    std::shared_ptr<ClusterMsgData> _msgData;
};

/* Message flags better specify the packet content or are used to
* provide some information about the node state. */
#define CLUSTERMSG_FLAG0_PAUSED (1<<0) /* Master paused for manual failover. */
#define CLUSTERMSG_FLAG0_FORCEACK (1<<1) /* Give ACK to AUTH_REQUEST even if
master is up. */

class ClusterMsgHeader{
 public:
    ClusterMsgHeader(const ClusterMsg::Type type,
            const std::shared_ptr<ClusterState> cstate,
            const std::shared_ptr<ServerEntry> svr);
    ClusterMsgHeader(const std::string& sig, const uint32_t totlen,
                const uint16_t port ,
                const ClusterMsg::Type type, const uint16_t count,
                const uint64_t currentEpoch, const uint64_t configEpoch,
                const uint64_t offset , const std::string& sender,
                const std::bitset<CLUSTER_SLOTS>& slots,
                const std::string& slaveOf, const std::string& myIp,
                const uint16_t cport, const uint16_t flags, 
                const ClusterHealth state);

    ClusterMsgHeader(const ClusterMsgHeader&) = delete;
    ClusterMsgHeader(ClusterMsgHeader&&);

    static constexpr const char* CLUSTER_NODE_NULL_NAME = "0000000000000000000000000000000000000000";

  // std::string _mflags; /* Message flags: CLUSTERMSG_FLAG[012]_... */
    static size_t getHeaderSize();
    std::string headEncode() const;
    static Expected<ClusterMsgHeader> headDecode(const std::string& key);

    std::string _sig;
    uint32_t _totlen;
    uint16_t _ver;
    uint16_t _port;    /* TCP base port number.*/
    ClusterMsg::Type _type;    /* Message type */
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
    ClusterHealth _state; /* Cluster state from the POV of the sender */
    unsigned char _mflags[3]; /* Message flags: CLUSTERMSG_FLAG[012]_... */
};


class ClusterMsgData{
 public:
    enum class Type {
        Gossip = 0,
        Update = 1,
    };
    ClusterMsgData(Type type) : _type(type) {}
    virtual std::string dataEncode() const = 0;
    virtual bool clusterNodeIsInGossipSection(const CNodePtr& node) const { return 0; }
    virtual void addGossipEntry(const CNodePtr& node) {}
    virtual ~ClusterMsgData() {} 
    virtual Type getType() const { return _type; }

 private:
    Type _type;
};

class ClusterMsgDataUpdate: public ClusterMsgData {
 public:
    ClusterMsgDataUpdate();
    ClusterMsgDataUpdate(const uint64_t configEpoch,
            const std::string &nodeName,
            const std::bitset<CLUSTER_SLOTS> &_slots);
    explicit ClusterMsgDataUpdate(std::shared_ptr<ClusterNode> node);
    ClusterMsgDataUpdate(const ClusterMsgDataUpdate&) = delete;
    ClusterMsgDataUpdate(ClusterMsgDataUpdate&&) = default;
    virtual ~ClusterMsgDataUpdate() {}

    static size_t getDataSize();
    std::string dataEncode() const override;
    static Expected<ClusterMsgDataUpdate>  dataDecode(const std::string& key);

    uint64_t getConfigEpoch() const { return _configEpoch; }
    std::string getNodeName() const { return  _nodeName; }
    std::bitset<CLUSTER_SLOTS> getSlots()  const { return  _slots; }

private:
    uint64_t _configEpoch;
    std::string _nodeName;
    std::bitset<CLUSTER_SLOTS> _slots;
};

class NetSession;
class NetworkMatrix;
class RequestMatrix;

class ClusterSession : public NetSession {
public:
    ClusterSession(std::shared_ptr<ServerEntry> server,
        asio::ip::tcp::socket sock,
        uint64_t connid,
        bool initSock,
        std::shared_ptr<NetworkMatrix> netMatrix,
        std::shared_ptr<RequestMatrix> reqMatrix);
    ClusterSession(const ClusterSession&) = delete;
    ClusterSession(ClusterSession&&) = delete;
    virtual ~ClusterSession() = default;

    Status clusterProcessPacket();
    Status clusterProcessGossipSection(const ClusterMsg& msg);
    Status clusterReadHandler();
    Status clusterSendMessage(const ClusterMsg& msg);
    Status clusterSendUpdate(CNodePtr node);

    Status clusterSendPing(ClusterMsg::Type type);

    void setNode(const CNodePtr& node);

private:
    // read data from socket
    virtual void drainReqNet();
    virtual void drainReqCallback(const std::error_code& ec, size_t actualLen);

    // handle msg parsed from drainReqCallback
    virtual void processReq();
    std::string nodeIp2String(const std::string& announcedIp) const;

    uint64_t _pkgSize;
    CNodePtr _node;
};

class ClusterState: public std::enable_shared_from_this<ClusterState> {
    friend class ClusterNode;
 public:
    ClusterState(std::shared_ptr<ServerEntry> server);
    ClusterState(const ClusterState&) = delete;
    ClusterState(ClusterState&&) = delete;
    // get epoch
    uint64_t getCurrentEpoch() const { return _currentEpoch;}
    // set epoch
    void setCurrentEpoch(uint64_t epoch);
    // get myself
    CNodePtr  getMyselfNode() const { return _myself;}
    std::string getMyselfName() const { return _myself->getNodeName();}
    // set myself
    void setMyselfNode(CNodePtr node);
    // addNode
    void clusterAddNode(CNodePtr node, bool save = false);
    void clusterDelNode(CNodePtr node, bool save = false);
    void clusterRenameNode(CNodePtr node, const std::string& newname, bool save = false);
    void clusterSaveNodes();

    void clusterBlacklistAddNode(CNodePtr node);
    bool clusterBlacklistExists(const std::string& nodeid) const;

    CNodePtr getRandomNode() const;
    CNodePtr clusterLookupNode(const std::string& name);
    // Status ClusterState::clusterMaster(CNodePtr n);
    bool clusterAddSlot(CNodePtr n, const uint32_t slot);
    bool clusterDelSlot(const uint32_t slot);
    uint32_t clusterDelNodeSlots(CNodePtr node);
    void clusterCloseAllSlots();


    //Status setSlot(CNodePtr n, uint32_t slot);
    //void setSlotBelong(CNodePtr n, const uint32_t slot);
    CNodePtr getNodeBySlot(uint32_t slot) const;
    bool clusterHandshakeInProgress(const std::string& host, uint32_t port, uint32_t cport);
    bool clusterStartHandshake(const std::string& host, uint32_t port, uint32_t cport);
    Status clusterUpdateSlotsConfigWith(CNodePtr sender,
        uint64_t senderConfigEpoch, const std::bitset<CLUSTER_SLOTS>& slots);
    Status clusterHandleConfigEpochCollision(CNodePtr sender);

    void clusterSendFail(const std::string& nodename);
    void clusterBroadcastMessage(const ClusterMsg& msg);

    // if update == true, _currentEpoch should be updated
    uint64_t clusterGetOrUpdateMaxEpoch(bool update = false);

    const std::unordered_map<std::string, CNodePtr> getNodesList() const;
    uint32_t getNodeCount() const;
    bool setMfMasterOffsetIfNecessary(const CNodePtr& node);
    ClusterHealth getClusterState() const { return _state; }

    // TODO(wayenchen)
    // Status clusterReadMeta();
    // Status clusterDelSlot(uint64_t slot);
    // Status clusterDelNodeSlots(CNodePtr n);
    // Status clusterDelNode(CNodePtr delnode);
    // Status setStateFail();
    // void clusterUpdateState();
 private:
    mutable std::mutex _mutex;
    CNodePtr _myself; /* This node */
    uint64_t _currentEpoch;
    std::shared_ptr<ServerEntry> _server;
    void clusterSaveNodesNoLock();
    void clusterAddNodeNoLock(CNodePtr node);
    void clusterDelNodeNoLock(CNodePtr node);
    bool clusterDelSlotNoLock(const uint32_t slot);
    void freeClusterNode(CNodePtr node);
    void clusterBlacklistCleanupNoLock();

    uint32_t clusterMastersHaveSlavesNoLock();

 public:
    ClusterHealth _state;
    uint16_t _size;
    std::unordered_map<std::string, CNodePtr> _nodes;
    std::unordered_map<std::string, CNodePtr> _nodesBackList;
    std::array<CNodePtr, CLUSTER_SLOTS> _migratingSlots;
    std::array<CNodePtr, CLUSTER_SLOTS> _importingSlots;
    std::array<CNodePtr, CLUSTER_SLOTS> _allSlots;
    std::array<uint64_t, CLUSTER_SLOTS> _slotsKeysCount;
    // rax *slots_to_keys;
    // TODO(wayenchen): should we make them as atomic?
    mstime_t _failoverAuthTime;
    uint16_t _failoverAuthCount;    /* Number of votes received so far. */
    uint16_t _failoverAuthSent;     /* True if we already asked for votes. */
    uint16_t _failoverAuthRank;     /* This slave rank for current auth request. */
    uint64_t _failoverAuthEpoch; /* Epoch of the current election. */
    uint8_t _cantFailoverReason;   /* Why a slave is currently not able to failover*/

                                /* Manual failover state in common. */
    mstime_t _mfEnd;            /* Manual failover time limit (ms unixtime).
                                It is zero if there is no MF in progress. */
                                /* Manual failover state of master. */
    CNodePtr _mfSlave;          /* Slave performing the manual failover. */
                                /* Manual failover state of slave. */
    uint64_t _mfMasterOffset;   /* Master offset the slave needs to start MF
                                or zero if stil not received. */
    uint32_t _mfCanStart;       /* If non-zero signal that the manual failover
                                can start requesting masters vote. */
                                /* The followign fields are used by masters to take state on elections. */
    uint64_t _lastVoteEpoch;     /* Epoch of the last vote granted. */
    uint8_t _todoBeforeSleep; /* Things to do in clusterBeforeSleep(). */
    /* Messages received and sent by type. */
    std::array<uint64_t, ClusterMsg::CLUSTERMSG_TYPE_COUNT> _statsMessagesSent;
    std::array<uint64_t, ClusterMsg::CLUSTERMSG_TYPE_COUNT> _statsMessagesReceived;
    uint64_t _statsPfailNodes;    /* Number of nodes in PFAIL status */
};



class ClusterGossip;
class ClusterMsgDataGossip: public ClusterMsgData {
 public:
    ClusterMsgDataGossip();
    ClusterMsgDataGossip(std::vector<ClusterGossip>&& gossipMsg);
    ClusterMsgDataGossip(const ClusterMsgDataGossip&) = delete;
    ClusterMsgDataGossip(ClusterMsgDataGossip&&) = default;

    virtual ~ClusterMsgDataGossip() = default;
    virtual std::string dataEncode() const override;
  //  std::shared_ptr<ClusterMsgData>
  //          dataDecode(const std::string& key) override;
    static Expected<ClusterMsgDataGossip> dataDecode(const std::string& key,
            uint16_t count);

    bool clusterNodeIsInGossipSection(const CNodePtr& node) const override;
    void addGossipEntry(const CNodePtr& node) override;

    const std::vector<ClusterGossip>& getGossipList() const  { return _gossipMsg; }

private:
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
    virtual ~ClusterGossip() = default;

    static size_t getGossipSize();
    virtual  std::string gossipEncode() const;
    static Expected<ClusterGossip> gossipDecode(const std::string& key);

    std::string _gossipName;
    uint32_t _pingSent;
    uint32_t _pongReceived;
    std::string _gossipIp;
    uint16_t _gossipPort;              // base port last time it was seen
    uint16_t _gossipCport;             // cluster port last time it was seen
    uint16_t _gossipFlags;             // node->flags copy
};

class ClusterNode;
class NetworkAsio;
class NetworkMatrix;
class RequestMatrix;
class ClusterMeta;
class ClusterManager {
 public:
    ClusterManager(const std::shared_ptr<ServerEntry>& svr,
                const std::shared_ptr<ClusterNode>& node,
                const std::shared_ptr<ClusterState>& state);
    explicit ClusterManager(const std::shared_ptr<ServerEntry>& svr);
    ClusterManager(const ClusterManager&) = delete;
    ClusterManager(ClusterManager&&) = delete;

    Status startup();
    Status initNetWork();
    Status initMetaData();
    void installClusterNode(std::shared_ptr<ClusterNode>);
    void installClusterState(std::shared_ptr<ClusterState>);

    Expected<std::shared_ptr<ClusterSession>> clusterCreateSession(const std::shared_ptr<ClusterNode>& node);

    NetworkAsio* getClusterNetwork() const;
    std::shared_ptr<ClusterState> getClusterState() const;
    // void stop();
    // Status run();
    // bool isRunning();
    // Status clusterSendPing(uint16_t type);
    // Status clusterSendUpdate();
 protected:
    void controlRoutine();

 private:
    std::mutex _mutex;
    std::shared_ptr<ServerEntry> _svr;
    std::atomic<bool> _isRunning;
    std::shared_ptr<ClusterNode> _clusterNode;
    std::shared_ptr<ClusterState> _clusterState;
    std::unique_ptr<NetworkAsio> _clusterNetwork;

    uint16_t _megPoolSize;
    // encode

    // decode

    // controller
    std::unique_ptr<std::thread> _controller;

    std::shared_ptr<NetworkMatrix> _netMatrix;
    std::shared_ptr<RequestMatrix> _reqMatrix;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_SERVER_CLUSTER_MANAGER_H_
