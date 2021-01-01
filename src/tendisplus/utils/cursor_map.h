// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_UTILS_CURSOR_MAP_H_
#define SRC_TENDISPLUS_UTILS_CURSOR_MAP_H_

#include <unordered_map>
#include <atomic>
#include <string>
#include <mutex> // NOLINT

#include "tendisplus/utils/status.h"

namespace tendisplus {

class CursorMap {
 public:
  static constexpr size_t MAX_MAPPING_COUNT = 10000;
  struct CursorMapping {
    int kvstoreId;
    std::string lastScanKey;
  };

  explicit CursorMap(size_t maxCursorCount = MAX_MAPPING_COUNT);
  ~CursorMap() = default;
  CursorMap(const CursorMap &) = delete;
  CursorMap &operator=(const CursorMap &) = delete;
  CursorMap(CursorMap &&) = delete;
  CursorMap &operator=(CursorMap &&) = delete;

  void addMapping(uint64_t cursor, const CursorMapping &mapping);
  Expected<CursorMapping> getMapping(uint64_t cursor);
  auto getMap() const -> const std::unordered_map<uint64_t, CursorMapping> &;

 private:
  size_t _maxCursorCount;
  std::mutex _mutex;
  std::unordered_map<uint64_t, CursorMapping> _cursorMap;   // {cursor, mapping}
  std::unordered_map<uint64_t, uint64_t> _cursorTs;         // {ts, cursor}
  std::unordered_map<uint64_t, uint64_t> _cursorReverseTs;  // {cursor, ts}
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_UTILS_CURSOR_MAP_H_
