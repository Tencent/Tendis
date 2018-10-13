#ifndef SRC_TENDISPLUS_NETWORK_SESSION_CTX_H_
#define SRC_TENDISPLUS_NETWORK_SESSION_CTX_H_

#include <stdint.h>
#include <utility>
#include <string>
#include <algorithm>
#include <list>
#include <vector>

#include "tendisplus/lock/lock.h"
#include "tendisplus/lock/mgl/lock_defines.h"

namespace tendisplus {

// storeLock state pair
using SLSP = std::pair<uint32_t, mgl::LockMode>;

class StoreLock;
class SessionCtx {
 public:
    SessionCtx();
    SessionCtx(const SessionCtx&) = delete;
    SessionCtx(SessionCtx&&) = delete;
    bool authed() const;
    void setAuthed();
    uint32_t getDbId() const;

    void resetSingleReqCtx();

    void addReadPacketCost(uint64_t);
    uint64_t getReadPacketCost() const;

    void setProcessPacketStart(uint64_t);
    uint64_t getProcessPacketStart() const;

    void setProcessPacketEnd(uint64_t);
    uint64_t getProcessPacketEnd() const;

    void setSendPacketStart(uint64_t);
    uint64_t getSendPacketStart() const;

    void setSendPacketEnd(uint64_t);
    uint64_t getSendPacketEnd() const;

    void setWaitLock(uint32_t storeId, mgl::LockMode mode);
    SLSP getWaitlock() const;
    std::list<SLSP> getLockStates() const;

    void addLock(StoreLock *lock);
    void removeLock(StoreLock *lock);

    // return by value, only for stats
    std::vector<std::string> getArgsBrief() const;
    void setArgsBrief(const std::vector<std::string>& v);
    void clearRequestCtx();

 private:
    // not protected by mutex
    bool _authed;
    uint32_t _dbId;
    uint32_t _waitlockStore;
    mgl::LockMode _waitlockMode;

    // single packet perf, not protected by mutex
    uint64_t _readPacketCost;
    uint64_t _processPacketStart;
    uint64_t _processPacketEnd;
    uint64_t _sendPacketStart;
    uint64_t _sendPacketEnd;

    mutable std::mutex _mutex;
    // currently, we only use StoreLock, if more lock
    // types are used, this list should be refined as
    // std::list<Lock*>

    // protected by mutex
    std::vector<StoreLock*> _locks;
    std::vector<std::string> _argsBrief;
    std::list<std::string> _delBigKeys;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_NETWORK_SESSION_CTX_H_

