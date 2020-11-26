// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_STORAGE_PESSIMISTIC_H_
#define SRC_TENDISPLUS_STORAGE_PESSIMISTIC_H_

#include <string>
#include <unordered_set>
#include <memory>
#include <vector>
#include <mutex>  // NOLINT

namespace tendisplus {

class PessimisticShard {
 public:
  PessimisticShard() = default;
  bool isLocked(const std::string&) const;
  void lock(const std::string&);
  void unlock(const std::string&);

 private:
  mutable std::mutex _mutex;
  std::unordered_set<std::string> _set;
};

// hardware_destructive_interference_size requires quite high version
// gcc. 128 should work for most cases
class PessimisticMgr {
 public:
  explicit PessimisticMgr(uint32_t num);
  ~PessimisticMgr() = default;
  PessimisticShard* getShard(uint32_t id);

 private:
  std::vector<std::unique_ptr<PessimisticShard>> _data;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_STORAGE_PESSIMISTIC_H_
