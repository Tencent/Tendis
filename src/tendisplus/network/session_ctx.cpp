#include "tendisplus/network/session_ctx.h"

namespace tendisplus {

SessionCtx::SessionCtx()
    :_authed(false),
     _dbId(0) {
}

bool SessionCtx::authed() const {
    return _authed;
}

uint32_t SessionCtx::getDbId() const {
    return _dbId;
}

}  // namespace tendisplus
