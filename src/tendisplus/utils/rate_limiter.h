// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_UTILS_RATE_LIMITER_H_
#define SRC_TENDISPLUS_UTILS_RATE_LIMITER_H_

#include <utility>
#include <memory>
#include <algorithm>
#include "rocksdb/env.h"
#include "rocksdb/rate_limiter.h"

namespace tendisplus {
class RateLimiter {
 public:
  explicit RateLimiter(uint64_t bytesPerSecond)
    : _bytesPerSecond(bytesPerSecond) {
    _rateLimiter = std::unique_ptr<rocksdb::RateLimiter>(
      rocksdb::NewGenericRateLimiter(bytesPerSecond));
  }

  void SetBytesPerSecond(uint64_t bytesPerSecond) {
    if (bytesPerSecond != 0 && bytesPerSecond != _bytesPerSecond) {
      _rateLimiter->SetBytesPerSecond(bytesPerSecond);
      _bytesPerSecond = bytesPerSecond;
    }
  }

  void Request(uint64_t bytes) {
    /* *
     * request size for GenericRateLimiter::Request() must be less than
     * _rateLimiter->GetSingleBurstBytes().
     * More detail at GenericRateLimiter::Request()
     */
    uint64_t singleBurst = _rateLimiter->GetSingleBurstBytes();
    uint64_t left = bytes;
    while (left > 0) {
      auto rsize = std::min(left, singleBurst);
      _rateLimiter->Request(rsize, rocksdb::Env::IOPriority::IO_HIGH, nullptr);
      left -= rsize;
    }
  }

 private:
  uint64_t _bytesPerSecond;
  std::unique_ptr<rocksdb::RateLimiter> _rateLimiter;
};
}  // namespace tendisplus
#endif  // SRC_TENDISPLUS_UTILS_RATE_LIMITER_H_
