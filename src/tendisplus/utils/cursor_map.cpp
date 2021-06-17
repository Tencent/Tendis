// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.
#include "tendisplus/utils/cursor_map.h"
#include "tendisplus/utils/time.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/redis_port.h"

namespace tendisplus {

/**
 * @brief CursorMap c-tor
 * @param maxCursorCount used as max cursor-mapping count
 *       default is MAX_MAPPING_COUNT
 * @param maxSessionLimit used as max session-level mapping count
 *       default is MAX_SESSION_LIMIT
 */
CursorMap::CursorMap(size_t maxCursorCount,
                     size_t maxSessionLimit,
                     size_t maxExpireTimeSec)
  : _maxCursorCount(maxCursorCount),
    _maxSessionLimit(maxSessionLimit),
    _maxExpireTimeNs((uint64_t)maxExpireTimeSec * 1000000000) {}

/**
 * @brief add mapping into cursorMap
 * @param cursor cursor in tendisplus, means k-v's sequence among all kv-stores
 * @param kvstoreId kv-store id
 * @param key last scan key, will used in next scan operation
 * @param sessionId this scan operation session id
 * @note mapping evict policy as follows:
 *      1. check whether _cursorMap.isFull ? => evict mapping NOT belong to
 *         current session id(mapping.sessionId) by LRU
 *      2. check whether _sessionTs[id].isFull ? => evict mapping belong to
 *         current session by LRU
 *      thus, other client's fast scan commands may not overwrite others' slow
 *      scan commands
 */
void CursorMap::addMapping(const std::string& cursor,
                           size_t kvstoreId,
                           const std::string& lastScanPos,
                           uint64_t sessionId) {
  // make lock guard
  std::lock_guard<std::recursive_mutex> lk(_mutex);

  /**
   * NOTE(pecochen): evict session-level mapping -> evict global-level mapping
   * when session number < (_maxCursorCount / _maxSessionLimit), the operation
   * sequence is meaningless.
   */

  /**
   * firstly, check whether session-level cursorMap is full.
   * if so, evict mapping by LRU belong to this session.
   */
  if (_sessionTs.count(sessionId) &&
      (_sessionTs[sessionId].size() >= _maxSessionLimit)) {
    uint64_t ts = *_sessionTs[sessionId].cbegin();
    evictMapping(_cursorTs[ts]);
  }

  /**
   * secondly, check whether cursorMap_ is full. if so, evict mapping by LRU
   * NOTE(pecochen): when cursorMap_ is full, evict mapping not belong to
   *        current session (mapping.sessionId)
   */
  if (_cursorMap.size() >= _maxCursorCount) {
    auto iter = _cursorTs.cbegin();
    for (; iter != _cursorTs.cend(); iter++) {
      if (_cursorMap[iter->second].sessionId != sessionId) {
        break;
      }
    }
    evictMapping(iter->second);  // cursorTs::iterator => {ts, cursor}
  }

  /**
   * evict old {timestamp, cursor} from _cursorTs;
   * search timeStamp by record in _cursorReverseTs;
   */
  if (_cursorMap.count(cursor)) {
    evictMapping(cursor);
  }

  auto time = getCurrentTime();
  _cursorMap[cursor] = {kvstoreId, lastScanPos, sessionId, time};
  _cursorTs[time] = cursor;
  _sessionTs[sessionId].emplace(time);
}

/**
 * @brief check and get mapping from cursorMap
 * @param cursor cursor in tendisplus, means k-v's sequence among all kv-stores
 * @return Expected represent mapping or status when error occurs
 */
Expected<CursorMap::CursorMapping> CursorMap::getMapping(
  const std::string& cursor) {
  std::lock_guard<std::recursive_mutex> lk(_mutex);

  // check and get mapping
  if (_cursorMap.count(cursor)) {  // means mapping in _cursorMap
    auto map = _cursorMap[cursor];
    // if CursorMapping is expired(default 1 day), ignore it!
    if (nsSinceEpoch() - map.timeStamp > _maxExpireTimeNs) {
      evictTooOldCursor(nsSinceEpoch() - _maxExpireTimeNs);
      return {ErrorCodes::ERR_NOTFOUND, "Mapping expired"};
    }
    return map;
  } else {
    return {ErrorCodes::ERR_NOTFOUND, "Mapping NOT FOUND"};
  }
}

/**
 * @brief get _cursorMap ref, only for debug
 * @return _cursorMap
 */
auto CursorMap::getMap() const
  -> const std::unordered_map<std::string, CursorMapping>& {
  return _cursorMap;
}

/**
 * @brief get _cursorTs ref, only for debug
 * @return _cursorTs
 */
auto CursorMap::getTs() const -> const std::map<uint64_t, std::string>& {
  return _cursorTs;
}

/**
 * @brief get _sessionTs ref, only for debug
 * @return _sessionTs
 */
auto CursorMap::getSessionTs() const
  -> const std::unordered_map<uint64_t, std::set<uint64_t>>& {
  return _sessionTs;
}

size_t CursorMap::maxCursorCount() const {
  return _maxCursorCount;
}
size_t CursorMap::maxSessionLimit() const {
  return _maxSessionLimit;
}

/**
 * @brief get current time by ns, especially check whether the same record
 *      in collection. If so, while (collection.exist(record)) { record++; }
 * @return current time as ns level
 * @note use getCurrentTime() when get mutex lock, keep "get-set" operation in
 *      lock guard scope. By std::recursive_mutex, can use lock_guard more times
 */
uint64_t CursorMap::getCurrentTime() {
  std::lock_guard<std::recursive_mutex> lk(_mutex);

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
 * @note use std::recursive_mutex to make lock guard.
 */
inline size_t CursorMap::getSessionMappingCount(uint64_t sessionId) {
  std::lock_guard<std::recursive_mutex> lk(_mutex);
  return _sessionTs.count(sessionId) ? _sessionTs[sessionId].size() : 0;
}

/**
 * @brief evict mapping, used mostly at the following cases:
 *      1. when _cursorMap.size() >= MAX_MAPPING_COUNT
 *          => evict mapping by LRU NOT belong to current session
 *      2. when _sessionTs[id].size() >= MAX_SESSION_LIMIT
 *          => evict mapping by LRU belong to current session
 *      all above cases evict mapping by info => {ts, cursor}
 * @param cursor cursor as unique key in _cursorMap
 * @note this function only can be called in lock guard scope,
 *      by std::recursive_mutex, can lock_guard recursively.
 */
void CursorMap::evictMapping(const std::string& cursor) {
  std::lock_guard<std::recursive_mutex> lk(_mutex);
  INVARIANT_D(_cursorMap.count(cursor));

  auto ts = _cursorMap[cursor].timeStamp;
  auto id = _cursorMap[cursor].sessionId;

  INVARIANT_D(_cursorMap.erase(cursor));
  INVARIANT_D(_cursorTs.erase(ts));
  INVARIANT_D(_sessionTs[id].erase(ts));

  if (_sessionTs[id].empty()) {
    _sessionTs.erase(id);
  }
}

/**
 * @brief evict mapping which created before specified timestamp
 * @param ts ts as the timestamp in nanoseconds
 * @note this function only can be called in lock guard scope,
 *      by std::recursive_mutex, can lock_guard recursively.
 */
void CursorMap::evictTooOldCursor(uint64_t ts) {
  std::lock_guard<std::recursive_mutex> lk(_mutex);
  /**
   *  _cursorTs is sorted by timestamp, it evicts all the elements
   *  when the timestamp is small than the input `ts`
   */
  while (!_cursorTs.empty() && _cursorTs.cbegin()->first < ts) {
    // evict the element which is too old
    evictMapping(_cursorTs.cbegin()->second);
  }
}

KeyCursorMap::KeyCursorMap(size_t maxCursorCount,
                           size_t maxSessionLimit,
                           size_t cursorMapSize) {
  for (size_t i = 0; i < cursorMapSize; i++) {
    _cursorMap.emplace_back(
      std::make_unique<CursorMap>(maxCursorCount, maxSessionLimit));
  }
}

void KeyCursorMap::addMapping(const std::string& key,
                              uint64_t cursor,
                              size_t kvstoreId,
                              const std::string& lastScanKey,
                              uint64_t sessionId) {
  auto slot = redis_port::keyHashSlot(key.c_str(), key.size());
  auto realCursor = key + "_" + std::to_string(cursor);

  _cursorMap[slot % _cursorMap.size()]->addMapping(
    realCursor, kvstoreId, lastScanKey, sessionId);
}

Expected<CursorMap::CursorMapping> KeyCursorMap::getMapping(
  const std::string& key, uint64_t cursor) {
  INVARIANT_D(cursor != 0);
  auto slot = redis_port::keyHashSlot(key.c_str(), key.size());
  auto real_cursor = key + "_" + std::to_string(cursor);

  return _cursorMap[slot % _cursorMap.size()]->getMapping(real_cursor);
}

std::string KeyCursorMap::getLastScanPos(const std::string& key,
                                         uint64_t cursor) {
  if (cursor == 0) {
    return "0";
  }

  auto emapping = getMapping(key, cursor);
  if (!emapping.ok()) {
    LOG(ERROR) << "Can't find cursor " << std::to_string(cursor) << " of key "
               << key;
    return "0";
  }

  return emapping.value().lastScanPos;
}

}  // namespace tendisplus
