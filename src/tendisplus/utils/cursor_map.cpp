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
CursorMap::CursorMap(size_t maxCursorCount) : _maxCursorCount(maxCursorCount) {}

/**
 * @brief add mapping into cursorMap
 * @param cursor cursor in tendisplus, means k-v's sequence among all kv-stores
 * @param mapping meta data in tendisplus, include {kv-storeid, lastScanKey}
 * @note when cursorMap_ is full, remove cursorMapping as LFU by _cursorTs
 */
void CursorMap::addMapping(uint64_t cursor, const CursorMapping &mapping) {
  // make lock guard
  std::lock_guard<std::mutex> lk(_mutex);

  // check whether cursorMap_ is full, if so, remove mapping
  // NOTE(pecochen): there needn't to check whether it's add or modify
  //           if it's modify, the next time it may evict element.
  //           But import another check in CursorMap::addMapping is expensive
  if (_cursorMap.size() == _maxCursorCount) {
    auto cursorToRemove = _cursorTs.begin()->second;      // LFU {ts, cursor}
    _cursorMap.erase(cursorToRemove);
    _cursorTs.erase(_cursorTs.begin());
    _cursorReverseTs.erase(cursorToRemove);
  }

  /**
   * remove old {timestamp, cursor} from _cursorTs;
   * search timeStamp by record in _cursorReverseTs;
   */
  if (_cursorReverseTs.count(cursor)) {
     _cursorTs.erase(_cursorReverseTs[cursor]);
     _cursorReverseTs.erase(cursor);
  }

  auto time = nsSinceEpoch();
  _cursorMap[cursor] = mapping;
  _cursorTs[time] = cursor;
  _cursorReverseTs[cursor] = time;
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

}  // namespace tendisplus
