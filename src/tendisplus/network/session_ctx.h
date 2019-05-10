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

#include "tendisplus/lock/lock.h"
#include "tendisplus/lock/mgl/lock_defines.h"

namespace tendisplus {

// storeLock state pair
using SLSP = std::tuple<uint32_t, std::string, mgl::LockMode>;

class ILock;
class SessionCtx {
 public:
    SessionCtx();
    SessionCtx(const SessionCtx&) = delete;
    SessionCtx(SessionCtx&&) = delete;
    bool authed() const;
    void setAuthed();
    uint32_t getDbId() const;
    void setDbId(uint32_t);

    void setProcessPacketStart(uint64_t);
    uint64_t getProcessPacketStart() const;

    void setWaitLock(uint32_t storeId, const std::string& key, mgl::LockMode mode);
    SLSP getWaitlock() const;
    std::list<SLSP> getLockStates() const;

    void addLock(ILock *lock);
    void removeLock(ILock *lock);

    // return by value, only for stats
    std::vector<std::string> getArgsBrief() const;
    void setArgsBrief(const std::vector<std::string>& v);
    void clearRequestCtx();
    void setExtendProtocol(bool v);
    void setExtendProtocolValue(uint64_t ts, uint64_t version);
    uint64_t getEpTs() const { return _timestamp; }
    uint64_t getEpVersion() const { return _version; }
    bool isEp() const { return _extendProtocol; }

    void setKeylock(const std::string& key, mgl::LockMode mode);
    void unsetKeylock(const std::string& key);

    bool isLockedByMe(const std::string& key, mgl::LockMode mode);
 private:
    // not protected by mutex
    bool _authed;
    uint32_t _dbId;
    uint32_t _waitlockStore;
    mgl::LockMode _waitlockMode;
    std::string _waitlockKey;
    uint64_t _processPacketStart;
    uint64_t _timestamp;
    uint64_t _version;
    bool _extendProtocol;
    std::unordered_map<std::string, mgl::LockMode> _keylockmap;

    mutable std::mutex _mutex;

    // protected by mutex
    std::vector<ILock*> _locks;
    std::vector<std::string> _argsBrief;
    std::list<std::string> _delBigKeys;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_NETWORK_SESSION_CTX_H_

