// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_SERVER_LATENCY_MONITOR_H_
#define SRC_TENDISPLUS_SERVER_LATENCY_MONITOR_H_

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <list>
#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#define LATENCY_THRESHOLD_MIN 2'000
#define LATENCY_THRESHOLD_MAX 1'000'000

namespace tendisplus {
struct LatencyMonitorConf {
  std::string name;
  std::vector<std::string> cmds;
  std::vector<int> timeSpan;
};

class LatencyMonitor {
 public:
  struct TimeSpan {
    uint64_t span;
    std::atomic<uint64_t> total;
    std::atomic<uint64_t> count;

    bool operator<(const TimeSpan& oth) const {
      return span < oth.span;
    }
    TimeSpan& operator+=(const TimeSpan& s) {
      total += s.total;
      count += s.count;
      return *this;
    }
  };

 public:
  LatencyMonitor() : mLast{0, 0, 0} {}
  LatencyMonitor& operator+=(const LatencyMonitor& m) {
    for (size_t i = 0; i < mTimeSpan.size(); ++i) {
      mTimeSpan[i] += m.mTimeSpan[i];
    }
    mLast += m.mLast;
    return *this;
  }
  bool find(const std::string& cmd) const {
    return mCmds.count(cmd) > 0;
  }
  void init(const LatencyMonitorConf& c) {
    mName = c.name;
    for (auto& commandName : c.cmds) {
      mCmds.insert(commandName);
    }
    int n = c.timeSpan.size();
    mTimeSpan = std::vector<TimeSpan>(n);
    for (int i = 0; i < n; ++i) {
      mTimeSpan[i].span = c.timeSpan[i];
      mTimeSpan[i].total = 0;
      mTimeSpan[i].count = 0;
    }
    if (!mTimeSpan.empty()) {
      mLast.span = mTimeSpan.back().span;
    }
  }
  void reset() {
    for (auto& s : mTimeSpan) {
      s.total = 0;
      s.count = 0;
    }
    mLast.total = 0;
    mLast.count = 0;
  }
  const std::string& name() const {
    return mName;
  }
  int add(uint64_t v) {
    TimeSpan span{v};
    auto it = std::lower_bound(mTimeSpan.begin(), mTimeSpan.end(), span);
    if (it == mTimeSpan.end()) {
      mLast.total += v;
      ++mLast.count;
      return mTimeSpan.size();
    } else {
      it->total += v;
      ++it->count;
      return it - mTimeSpan.begin();
    }
  }
  void add(uint64_t v, int idx) {
    TimeSpan& s(idx < static_cast<int>(mTimeSpan.size()) ? mTimeSpan[idx]
                                                         : mLast);
    s.total += v;
    ++s.count;
  }
  void getInfoString(std::stringstream& ss);

 private:
  std::string mName;
  std::unordered_set<std::string> mCmds;
  std::vector<TimeSpan> mTimeSpan;
  TimeSpan mLast;
};

class LatencyMonitorSet {
 public:
  LatencyMonitorSet() {}
  void init(const std::vector<LatencyMonitorConf>& conf);
  void init();
  std::string getLatencyInfo();
  void reset() {
    for (auto& monitor : mPool) {
      monitor.reset();
    }
  }
  void addLatencyInfo(const std::string& commandName, uint64_t duration) {
    for (auto& monitor : mPool) {
      if (monitor.find(commandName)) {
        monitor.add(duration);
      }
    }
  }
  std::vector<LatencyMonitor>& getLatencyMonitors() {
    return mPool;
  }

 private:
  std::vector<LatencyMonitor> mPool;
};
}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_SERVER_LATENCY_MONITOR_H_
