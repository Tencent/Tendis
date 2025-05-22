// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.


#include "tendisplus/server/latency_monitor.h"

#include <stddef.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "tendisplus/commands/command.h"
#include "tendisplus/utils/string.h"

void tendisplus::LatencyMonitor::getInfoString(std::stringstream& ss) {
  uint64_t tc = mLast.count;
  for (auto& s : mTimeSpan) {
    tc += s.count;
  }
  if (tc == 0) {
    return;
  }
  uint64_t te = mLast.total;
  uint64_t t = 0;
  double p = 0;
  for (auto& s : mTimeSpan) {
    if (s.count == 0) {
      continue;
    }
    te += s.total;
    t += s.count;
    p = t * 100. / tc;
    ss << "<= " << std::setw(12) << s.span                          // %12ld
       << " " << std::setw(20) << s.total                           // %20ld
       << " " << std::setw(16) << s.count                           // %16ld
       << " " << std::fixed << std::setprecision(2) << p << "%\n";  // %.2f%%
  }
  if (mLast.count > 0) {
    ss << ">  " << std::setw(12) << mLast.span << " " << std::setw(20)
       << mLast.total << " " << std::setw(16) << mLast.count << " 100.00%\n";
  }
  ss << "T  " << std::setw(12) << te / tc << " " << std::setw(20) << te << " "
     << std::setw(16) << tc << "\n";
}

std::string tendisplus::LatencyMonitorSet::getLatencyInfo() {
  std::stringstream ss;
  for (auto& monitor : mPool) {
    ss << "LatencyMonitorName:" << monitor.name() << "\n";
    monitor.getInfoString(ss);
    ss << "\n";
  }
  return ss.str();
}

void tendisplus::LatencyMonitorSet::init(
  const std::vector<LatencyMonitorConf>& confs) {
  mPool = std::vector<LatencyMonitor>(confs.size());
  int i = 0;
  for (auto& c : confs) {
    mPool[i].init(c);
    ++i;
  }
}

void tendisplus::LatencyMonitorSet::init() {
  LatencyMonitorConf allCommandsCfg = {"all", {}, {}};
  allCommandsCfg.cmds = Command::listCommands();
  auto& spin = allCommandsCfg.timeSpan;
  uint64_t minSpan = LATENCY_THRESHOLD_MIN, maxSpan = LATENCY_THRESHOLD_MAX;
  for (uint64_t u = minSpan; u <= maxSpan; u *= 2) {
    spin.emplace_back(u);
  }
  std::vector<LatencyMonitorConf> confs{allCommandsCfg};
  init(confs);
}
