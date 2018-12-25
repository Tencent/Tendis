#ifndef SRC_TENDISPLUS_UTILS_RATE_LIMITER_H_
#define SRC_TENDISPLUS_UTILS_RATE_LIMITER_H_

#include <utility>
#include <memory>
#include "rocksdb/env.h"
#include "rocksdb/rate_limiter.h"

namespace tendisplus {
class RateLimiter  {
 public:
    explicit RateLimiter(uint64_t bytesPerSecond) {
        _rateLimiter = std::unique_ptr<rocksdb::RateLimiter>(
            rocksdb::NewGenericRateLimiter(bytesPerSecond));
    }

    void Request(uint64_t bytes) {
        _rateLimiter->Request(
                bytes,
                rocksdb::Env::IOPriority::IO_HIGH,
                nullptr);
    }

 private:
    std::unique_ptr<rocksdb::RateLimiter> _rateLimiter;
};
}  // namespace tendisplus
#endif  // SRC_TENDISPLUS_UTILS_RATE_LIMITER_H_
