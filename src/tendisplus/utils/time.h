#ifndef SRC_TENDISPLUS_UTILS_TIME_H_
#define SRC_TENDISPLUS_UTILS_TIME_H_

#include <chrono>  // NOLINT
#include <string>

namespace tendisplus {

uint64_t nsSinceEpoch();
uint64_t msSinceEpoch();
uint32_t sinceEpoch();

using SCLOCK = std::chrono::steady_clock;
std::string timePointRepr(const SCLOCK::time_point&);
uint64_t nsSinceEpoch(const SCLOCK::time_point&);
uint32_t sinceEpoch(const SCLOCK::time_point&);
std::string epochToDatetime(const time_t epoch);

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_UTILS_TIME_H_
