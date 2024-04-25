// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_UTILS_TIME_H_
#define SRC_TENDISPLUS_UTILS_TIME_H_

#include <chrono>
#include <filesystem>
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

// ref https://en.cppreference.com/w/cpp/chrono/file_clock/to_from_sys
// NOTE(raffertyyu) std::chrono::file_clock::to_sys/from_sys is available
// since c++20. Remove to_sys function when tendis using c++20
using Tsys_time_point =
  std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>;
constexpr Tsys_time_point to_sys(
  const std::filesystem::file_time_type& tp) noexcept {
  constexpr std::chrono::seconds epochDiff{6437664000};
  return Tsys_time_point(tp.time_since_epoch()) + epochDiff;
}
}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_UTILS_TIME_H_
