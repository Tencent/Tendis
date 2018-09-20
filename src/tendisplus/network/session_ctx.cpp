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

void SessionCtx::setAuthed() {
    _authed = true;
}

void SessionCtx::resetSingleReqCtx() {
    _readPacketCost = 0;
    _processPacketStart = 0;
    _processPacketEnd = 0;
    _sendPacketStart = 0;
    _sendPacketEnd = 0;
}

void SessionCtx::addReadPacketCost(uint64_t cost) {
    _readPacketCost += cost;
}

uint64_t SessionCtx::getReadPacketCost() const {
    return _readPacketCost;
}

void SessionCtx::setProcessPacketStart(uint64_t start) {
    _processPacketStart = start;
}

uint64_t SessionCtx::getProcessPacketStart() const {
    return _processPacketStart;
}

void SessionCtx::setProcessPacketEnd(uint64_t end) {
    _processPacketEnd = end;
}

uint64_t SessionCtx::getProcessPacketEnd() const {
    return _processPacketEnd;
}

void SessionCtx::setSendPacketStart(uint64_t start) {
    _sendPacketStart = start;
}

uint64_t SessionCtx::getSendPacketStart() const {
    return _sendPacketStart;
}

void SessionCtx::setSendPacketEnd(uint64_t end) {
    _sendPacketEnd = end;
}

uint64_t SessionCtx::getSendPacketEnd() const {
    return _sendPacketEnd;
}
}  // namespace tendisplus
