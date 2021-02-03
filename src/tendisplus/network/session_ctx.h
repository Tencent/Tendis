// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_NETWORK_SESSION_CTX_H_
#define SRC_TENDISPLUS_NETWORK_SESSION_CTX_H_

#include <stdint.h>
#include <utility>
#include <string>
#include <algorithm>
#include <list>
#include <vector>
#include <tuple>
#include <unordered_map>
#include <memory>

#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"

#include "tendisplus/lock/lock.h"
#include "tendisplus/lock/mgl/lock_defines.h"
#include "tendisplus/storage/kvstore.h"
#include "tendisplus/server/session.h"
#include "tendisplus/utils/string.h"

namespace tendisplus {

// storeLock state pair
using SLSP = std::tuple<uint32_t, uint32_t, std::string, mgl::LockMode>;

class ILock;
class SessionCtx {
  enum class PerfLevel : unsigned char {
    kUninitialized = 0,             // unknown setting
    kDisable = 1,                   // disable perf stats
    kEnableCount = 2,               // enable only count stats
    kEnableTimeExceptForMutex = 3,  // Other than count stats, also enable
                                    // time stats except for mutexes
    // Other than time, also measure CPU time counters. Still don't measure
    // time (neither wall time nor CPU time) for mutexes.
    kEnableTimeAndCPUTimeExceptForMutex = 4,
    kEnableTime = 5,  // enable count and time stats
    kOutOfBounds = 6  // N.B. Must always be the last value!
  };

 public:
  explicit SessionCtx(Session* sess);
  SessionCtx(const SessionCtx&) = delete;
  SessionCtx(SessionCtx&&) = delete;
  bool authed() const;
  void setAuthed();
  uint32_t getDbId() const;
  void setDbId(uint32_t);

  void setProcessPacketStart(uint64_t);
  uint64_t getProcessPacketStart() const;

  void setWaitLock(uint32_t storeId,
                   uint32_t chunkId,
                   const std::string& key,
                   mgl::LockMode mode);
  SLSP getWaitlock() const;
  std::list<SLSP> getLockStates() const;

  void addLock(ILock* lock);
  void removeLock(ILock* lock);

  // return by value, only for stats
  std::vector<std::string> getArgsBrief() const;
  void setArgsBrief(const std::vector<std::string>& v);
  void clearRequestCtx();
  Status commitAll(const std::string& cmd);
  Status rollbackAll();
  Expected<Transaction*> createTransaction(const PStore& kvstore);
  Expected<uint64_t> commitTransaction(Transaction*);
  void setExtendProtocol(bool v);
  void setExtendProtocolValue(uint64_t ts, uint64_t version);
  bool setPerfLevel(const std::string& level);
  uint64_t getTsEP() const {
    return _timestamp;
  }
  uint64_t getVersionEP() const {
    return _version;
  }
  PerfLevel getPerfLevel() const {
    return _perfLevel;
  }
  bool needResetPerLevel();
  std::string getPerfContextStr() const;
  std::string getIOstatsContextStr() const;
  bool isEp() const {
    return _extendProtocol;
  }
  bool isReplOnly() const {
    return _replOnly;
  }
  void setReplOnly(bool v) {
    _replOnly = v;
  }

  void setKeylock(const std::string& key, mgl::LockMode mode);
  void unsetKeylock(const std::string& key);

  bool isLockedByMe(const std::string& key, mgl::LockMode mode);

  uint32_t getIsMonitor() const;
  void setIsMonitor(bool in);

  inline bool isInMulti() const {
    return (_flags & CLIENT_MULTI);
  }
  inline void setMulti() {
    _flags |= CLIENT_MULTI;
    _txnVersion = _version;
  }
  inline void resetMulti() {
    _flags &= ~CLIENT_MULTI;
    _txnVersion = -1;
  }

  inline bool isMaster() {
    return (_flags & CLIENT_MASTER);
  }

  inline bool isSlave() {
    return (_flags & CLIENT_SLAVE);
  }

  int64_t getFlags() {
    return _flags;
  }
  inline void setFlags(int64_t flag) {
    _flags |= flag;
  }
  inline void resetFlags(int64_t flag) {
    _flags &= ~flag;
  }
  bool verifyVersion(uint64_t keyVersion);

  static constexpr uint64_t VERSIONEP_UNINITED = -1;
  static constexpr uint64_t TSEP_UNINITED = -1;

 private:
  // not protected by mutex
  bool _authed;
  uint32_t _dbId;
  uint32_t _waitlockStore;
  uint32_t _waitlockChunk;
  mgl::LockMode _waitlockMode;
  std::string _waitlockKey;
  uint64_t _processPacketStart;
  uint64_t _timestamp;
  uint64_t _version;
  PerfLevel _perfLevel;
  bool _perfLevelFlag;
  uint64_t _txnVersion;
  bool _extendProtocol;
  bool _replOnly;
  Session* _session;
  std::unordered_map<std::string, mgl::LockMode> _keylockmap;
  bool _isMonitor;
  int64_t _flags;

  mutable std::mutex _mutex;

  // protected by mutex
  std::vector<ILock*> _locks;
  // multi key
  std::unordered_map<std::string, std::unique_ptr<Transaction>> _txnMap;
  std::vector<std::string> _argsBrief;
  rocksdb::PerfContext _perfContext;
  rocksdb::IOStatsContext _ioContext;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_NETWORK_SESSION_CTX_H_
