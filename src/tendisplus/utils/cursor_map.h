// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_UTILS_CURSOR_MAP_H_
#define SRC_TENDISPLUS_UTILS_CURSOR_MAP_H_

#include <set>
#include <map>
#include <unordered_map>
#include <atomic>
#include <string>
#include <mutex> // NOLINT

#include "tendisplus/utils/status.h"

namespace tendisplus {

class CursorMap {
 public:
  static constexpr size_t MAX_MAPPING_COUNT = 10000;
  static constexpr size_t MAX_SESSION_LIMIT = 100;
  struct CursorMapping {
    size_t kvstoreId;
    std::string lastScanKey;
    uint64_t sessionId;
    uint64_t timeStamp;
  };

  explicit CursorMap(size_t maxCursorCount = MAX_MAPPING_COUNT,
                     size_t maxSessionLimit = MAX_SESSION_LIMIT);
  ~CursorMap() = default;
  CursorMap(const CursorMap &) = delete;
  CursorMap &operator=(const CursorMap &) = delete;
  CursorMap(CursorMap &&) = delete;
  CursorMap &operator=(CursorMap &&) = delete;

  void addMapping(uint64_t cursor, size_t kvstoreId,
                  const std::string &key, uint64_t sessionId);
  Expected<CursorMapping> getMapping(uint64_t cursor);

#ifdef TENDIS_DEBUG
  auto getMap() const -> const std::unordered_map<uint64_t, CursorMapping> &;
  auto getTs() const -> const std::map<uint64_t, uint64_t> &;
  auto getSessionTs() const
  -> const std::unordered_map<uint64_t, std::set<uint64_t>> &;
#endif

 private:
  uint64_t getCurrentTime();
  size_t getSessionMappingCount(uint64_t sessionId);
  void evictMapping(uint64_t cursor);

  size_t _maxCursorCount;
  size_t _maxSessionLimit;
  std::recursive_mutex _mutex;
  std::unordered_map<uint64_t, CursorMapping> _cursorMap;   // {cursor, mapping}
  std::map<uint64_t, uint64_t> _cursorTs;                   // {ts, cursor}
  std::unordered_map<uint64_t, std::set<uint64_t>> _sessionTs;  // {id, Ts(es)}
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_UTILS_CURSOR_MAP_H_
