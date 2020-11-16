// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_STORAGE_CATALOG_H_
#define SRC_TENDISPLUS_STORAGE_CATALOG_H_

#include <memory>
#include <string>
#include <vector>

#include "tendisplus/replication/repl_manager.h"
#include "tendisplus/cluster/migrate_manager.h"

namespace tendisplus {

enum class ReplState : std::uint8_t;
enum class MigrateReceiveState;

enum class ConnectState : std::uint8_t {
  CONNECTED = 0,
  DISCONNECTED = 1,  // initialsync, transfer whole db
};

// for MSVC, it must be class , but not struct
// repl meta
class StoreMeta {
 public:
  StoreMeta();
  StoreMeta(const StoreMeta&) = default;
  StoreMeta(StoreMeta&&) = delete;
  StoreMeta(uint32_t id_,
            const std::string& syncFromHost_,
            uint16_t syncFromPort_,
            int32_t syncFromId_,
            uint64_t binlogId_,
            ReplState replState_);
  std::unique_ptr<StoreMeta> copy() const;

  uint32_t id;
  std::string syncFromHost;
  uint16_t syncFromPort;
  int32_t syncFromId;  // the storeid of master
  // the binlog has been applied, but it's unreliable in the
  // catalog. KVStore->getHighestBinlogId() should been more reliable.
  uint64_t binlogId;
  ReplState replState;
  string replErr;
};

class ChunkMeta {
 public:
  ChunkMeta();
  ChunkMeta(const ChunkMeta&) = default;
  ChunkMeta(ChunkMeta&&) = delete;
  ChunkMeta(uint32_t id_,
            const std::string& syncFromHost_,
            uint16_t syncFromPort_,
            int32_t syncFromId_,
            uint64_t binlogId_,
            MigrateReceiveState migrateState_);
  std::unique_ptr<ChunkMeta> copy() const;

  uint32_t id;
  std::string syncFromHost;
  uint16_t syncFromPort;
  int32_t syncFromId;  // the storeid of master
  // the binlog has been applied, but it's unreliable in the
  // catalog. KVStore->getHighestBinlogId() should been more reliable.
  uint64_t binlogId;
  MigrateReceiveState migrateState;
};

// store meta
class StoreMainMeta {
 public:
  StoreMainMeta() : StoreMainMeta(-1, KVStore::StoreMode::STORE_NONE) {}
  StoreMainMeta(const StoreMainMeta&) = default;
  StoreMainMeta(StoreMainMeta&&) = delete;
  StoreMainMeta(uint32_t id_, KVStore::StoreMode mode_)
    : id(id_), storeMode(mode_) {}
  std::unique_ptr<StoreMainMeta> copy() const;

  uint32_t id;
  KVStore::StoreMode storeMode;
};

// store meta
class MainMeta {
 public:
  MainMeta() : MainMeta(0, 0, BinlogVersion::BINLOG_VERSION_1) {}
  MainMeta(const MainMeta&) = default;
  MainMeta(MainMeta&&) = delete;
  MainMeta(uint32_t instCount,
           uint32_t hashSpace_,
           BinlogVersion binlogVersion_)
    : kvStoreCount(instCount),
      chunkSize(hashSpace_),
      binlogVersion(binlogVersion_) {}
  std::unique_ptr<MainMeta> copy() const;

  uint32_t kvStoreCount;
  uint32_t chunkSize;
  BinlogVersion binlogVersion;
};

// cluster meta
/* Regular config lines have at least eight fields */
// master node: 66478bda726ae6ba4e8fb55034d8e5e5804223ff 127.0.0.1 6381 master -
// 0 1496130037660 2 connected 10923-16383 0-5999 slave node
// ：6fb7dfdb6188a9fe53c48ea32d541724f36434e9 127.0.0.1 6383 slave
// 8f285670923d4f1c599ecc93367c95a30fb8bf34 0 1496130040668 4 connected
class ClusterMeta {
 public:
  ClusterMeta();
  explicit ClusterMeta(const std::string& nodeName_);
  ClusterMeta(const std::string& nodeName_,
              const std::string& ip_,
              uint64_t port_,
              uint64_t cport_,
              uint16_t nodeFlag_,
              const std::string& masterName_,
              uint64_t pingTime_,
              uint64_t pongTime_,
              uint64_t configEpoch_,
              const std::vector<uint16_t>& slots_);

  ClusterMeta(const ClusterMeta&) = default;
  ClusterMeta(ClusterMeta&&) = delete;
  // [name]  [ip] [port]   [flag(master、myself、salve)]    [(-or master id)]
  // [ping send UNIX time]    [pong receive unix time]   [-config epoch]
  // [connectState]    [slot]


  std::unique_ptr<ClusterMeta> copy() const;

  std::string nodeName;
  std::string ip;
  uint64_t port;
  uint64_t cport;
  uint16_t nodeFlag;
  std::string masterName;
  uint64_t pingTime;
  uint64_t pongTime;
  uint64_t configEpoch;
  std::vector<uint16_t> slots;

  static std::string& getClusterPrefix();
  static constexpr const char* CLUSTER_PREFIX = "store_cluster_";
};

// epoch meta , just use current epoch
class EpochMeta {
 public:
  EpochMeta() : EpochMeta(0, 0) {}
  EpochMeta(const EpochMeta&) = default;
  EpochMeta(EpochMeta&&) = delete;

  EpochMeta(uint64_t bigEpoch, uint64_t voteEpoch_)
    : currentEpoch(bigEpoch), lastVoteEpoch(voteEpoch_) {}
  std::unique_ptr<EpochMeta> copy() const;

  uint64_t currentEpoch;
  uint64_t lastVoteEpoch;
};

// store meta
class Catalog {
 public:
  Catalog(std::unique_ptr<KVStore> store,
          uint32_t kvStoreCount,
          uint32_t chunkSize,
          bool binlogUsingDefaultCF);
  virtual ~Catalog() = default;
  // repl meta for each store
  Expected<std::unique_ptr<StoreMeta>> getStoreMeta(uint32_t idx);
  Status setStoreMeta(const StoreMeta& meta);

  Status stop();
  // main meta for each store
  Expected<std::unique_ptr<StoreMainMeta>> getStoreMainMeta(uint32_t idx);
  Status setStoreMainMeta(const StoreMainMeta& meta);
  // cluster meta
  bool clusterMetaExist(Transaction* txn, const std::string& nodeName);
  Status delClusterMeta(const std::string& nodeName);
  Expected<std::unique_ptr<ClusterMeta>> getClusterMeta(const string& nodeName);

  Expected<std::vector<std::unique_ptr<ClusterMeta>>> getAllClusterMeta();
  Status setClusterMeta(const ClusterMeta& meta);
  // epoch meta
  Expected<std::unique_ptr<EpochMeta>> getEpochMeta();
  Status setEpochMeta(const EpochMeta& meta);
  // main meta
  Expected<std::unique_ptr<MainMeta>> getMainMeta();
  Status setMainMeta(const MainMeta& meta);

  uint32_t getKVStoreCount() const {
    return _kvStoreCount;
  }
  uint32_t getChunkSize() const {
    return _chunkSize;
  }
  BinlogVersion getBinlogVersion() const {
    return _binlogVersion;
  }

 private:
  // Status setMainMeta(const MainMeta& meta);
  std::unique_ptr<KVStore> _store;
  uint32_t _kvStoreCount;
  uint32_t _chunkSize;
  BinlogVersion _binlogVersion;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_STORAGE_CATALOG_H_
