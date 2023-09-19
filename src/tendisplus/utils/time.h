// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_UTILS_TIME_H_
#define SRC_TENDISPLUS_UTILS_TIME_H_

#include <chrono>
#include <string>

namespace tendisplus {

uint64_t nsSinceEpoch();
uint64_t usSinceEpoch();
uint64_t msSinceEpoch();
uint32_t sinceEpoch();

using SCLOCK = std::chrono::steady_clock;
using TCLOCK = std::chrono::system_clock;

std::string timePointRepr(const SCLOCK::time_point&);
uint64_t nsSinceEpoch(const SCLOCK::time_point&);
uint32_t sinceEpoch(const SCLOCK::time_point&);
std::string epochToDatetime(uint64_t epoch);
// in one string, example: 2022/1/1-00:00:00
std::string epochToDatetimeInOneStr(uint64_t epoch);
std::string msEpochToDatetime(uint64_t msEpoch);
std::string nsEpochToDatetime(uint64_t nsEpoch);

uint64_t msToNow(uint64_t ms);
}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_UTILS_TIME_H_
