// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include "tendisplus/utils/cursor_map.h"

#include "tendisplus/utils/time.h"

namespace tendisplus {

/**
 * @brief CursorMap c-tor
 * @param maxCursorCount used as max cursor-mapping count
 *       default is MAX_MAPPING_COUNT
 */
CursorMap::CursorMap(size_t maxCursorCount, size_t maxSessionLimit)
          : _maxCursorCount(maxCursorCount),
            _maxSessionLimit(maxSessionLimit) {}

/**
 * @brief add mapping into cursorMap
 * @param cursor cursor in tendisplus, means k-v's sequence among all kv-stores
 * @param mapping meta data in tendisplus,
 *       include {kv-storeid, lastScanKey, sessionId}
 * @note mapping evict policy as follows:
 *      1. check whether _cursorMap.isFull ? => evict mapping NOT belong to
 *         current session id(mapping.sessionId) by LRU
 *      2. check whether _sessionTs[id].isFull ? => evict mapping belong to
 *         current session by LRU
 *      thus, other client's fast scan commands may not overwrite others' slow
 *      scan commands
 */
void CursorMap::addMapping(uint64_t cursor, const CursorMapping &mapping) {
  // make lock guard
  std::lock_guard<std::mutex> lk(_mutex);
  auto currentSession = mapping.sessionId;

  /**
   * firstly, check whether cursorMap_ is full. if so, evict mapping by LRU
   * NOTE(pecochen): when cursorMap_ is full, evict mapping not belong to
   *        current session (mapping.sessionId)
   */
  if (_cursorMap.size() == _maxCursorCount) {
    auto iter = _cursorTs.cbegin();
    for (; iter != _cursorTs.cend(); iter++) {
      if (_cursorMap[iter->second].sessionId != currentSession) {
        break;
      }
    }
    auto sessionId = _cursorMap[iter->second].sessionId;
    evictMappingbyLRU(sessionId, iter);
  }

  /**
   * secondly, check whether session-level cursorMap is full.
   * if so, evict mapping by LRU belong to this session.
   * NOTE(pecochen): use std::set::find to adapt interface arguments,
   *   std::set::find has the same algorithm complexity with std::set::operator[]
   *   they're O(log(std::Container::size())
   */
  if (_sessionTs.count(currentSession)
      && (_sessionTs[currentSession].size() == _maxSessionLimit)) {
    uint64_t ts = *_sessionTs[currentSession].cbegin();
    auto iter = _cursorTs.find(ts);
    evictMappingbyLRU(currentSession, iter);
  }

  /**
   * evict old {timestamp, cursor} from _cursorTs;
   * search timeStamp by record in _cursorReverseTs;
   */
  if (_cursorReverseTs.count(cursor)) {
    uint64_t ts = _cursorReverseTs[cursor];
    auto iter = _cursorTs.find(ts);
    evictMappingbyLRU(currentSession, iter);
  }

  auto time = getCurrentTime();
  _cursorMap[cursor] = mapping;
  _cursorTs[time] = cursor;
  _cursorReverseTs[cursor] = time;
  _sessionTs[currentSession].emplace(time);
}

/**
 * @brief check and get mapping from cursorMap
 * @param cursor cursor in tendisplus, means k-v's sequence among all kv-stores
 * @return Expected represent mapping or status when error occurs
 */
Expected<CursorMap::CursorMapping> CursorMap::getMapping(uint64_t cursor) {
  // make lock guard
  std::lock_guard<std::mutex> lk(_mutex);

  // check and get mapping
  if (_cursorMap.count(cursor) > 0) {      // means mapping in _cursorMap
    return _cursorMap[cursor];
  } else {
    return {ErrorCodes::ERR_NOTFOUND, ""};
  }
}

/**
 * @ get cursorMap ref, only for debug
 * @return _cursorMap
 */
auto CursorMap::getMap() const
      -> const std::unordered_map<uint64_t, CursorMapping> & {
  return _cursorMap;
}

/**
 * @brief get current time by ns, especially check whether the same record
 *      in collection. If so, while (collection.exist(record)) { record++; }
 * @return current time as ns level
 * @note use getCurrentTime() when get mutex lock, keep "get-set" operation in
 *      lock guard scope.
 */
uint64_t CursorMap::getCurrentTime() {
  auto time = nsSinceEpoch();
  while (_cursorTs.count(time)) {
    time++;
  }

  return time;
}

/**
 * @brief get specific session mapping count
 * @param sessionId
 * @return std::set::size()
 */
inline size_t CursorMap::getSessionMappingCount(uint64_t sessionId) {
  return _sessionTs.count(sessionId) ? _sessionTs[sessionId].size() : 0;
}

/**
 * @brief evict mapping by LRU algorithm, used mostly at the following cases:
 *      1. when _cursorMap.size() == MAX_MAPPING_COUNT
 *          => evict mapping by LRU NOT belong to current session
 *      2. when _sessionTs[id].size() == MAX_SESSION_LIMIT
 *          => evict mapping by LRU belong to current session
 *      all above cases evict mapping by info => {ts, cursor}
 * @param id sessionId
 * @param lru mapping info const iterator => {ts, cursor}
 * @note this function only can be called in lock guard scope
 */
void CursorMap::evictMappingbyLRU(
        uint64_t id,
        std::map<uint64_t, uint64_t>::const_iterator lru) {
  auto ts = lru->first;
  auto cursor = lru->second;

  _cursorMap.erase(cursor);
  _cursorTs.erase(ts);
  _cursorReverseTs.erase(cursor);
  _sessionTs[id].erase(ts);
}

}  // namespace tendisplus
