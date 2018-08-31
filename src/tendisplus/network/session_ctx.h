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

 private:
    bool _authed;
    uint32_t _dbId;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_NETWORK_SESSION_CTX_H_

