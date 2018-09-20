#ifndef SRC_TENDISPLUS_NETWORK_SESSION_CTX_H_
#define SRC_TENDISPLUS_NETWORK_SESSION_CTX_H_

#include <stdint.h>

namespace tendisplus {

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

 private:
    bool _authed;
    uint32_t _dbId;

    // single packet perf
    uint64_t _readPacketCost;
    uint64_t _processPacketStart;
    uint64_t _processPacketEnd;
    uint64_t _sendPacketStart;
    uint64_t _sendPacketEnd;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_NETWORK_SESSION_CTX_H_

