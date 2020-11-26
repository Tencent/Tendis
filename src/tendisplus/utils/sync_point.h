// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_TENDISPLUS_UTILS_SYNC_POINT_H_
#define SRC_TENDISPLUS_UTILS_SYNC_POINT_H_

#include <assert.h>

#include <condition_variable>  // NOLINT
#include <mutex>               // NOLINT
#include <string>
#include <unordered_set>
#include <unordered_map>
#include <functional>
#include <vector>

#ifndef WITH_SYNC_POINT
#define TEST_SYNC_POINT(x)
#define TEST_SYNC_POINT_CALLBACK(x, y)
#else

namespace tendisplus {

// This class provides facility to reproduce race conditions deterministically
// in unit tests.
// Developer could specify sync points in the codebase via TEST_SYNC_POINT.
// Each sync point represents a position in the execution stream of a thread.
// In the unit test, 'Happens After' relationship among sync points could be
// setup via SyncPoint::LoadDependency, to reproduce a desired interleave of
// threads execution.
// Refer to (DBTest,TransactionLogIteratorRace), for an example use case.

class SyncPoint {
 public:
  static SyncPoint* GetInstance();

  struct Dependency {
    std::string predecessor;
    std::string successor;
  };
  // call once at the beginning of a test to setup the dependency between
  // sync points
  void LoadDependency(const std::vector<Dependency>& dependencies);

  // Set up a call back function in sync point.
  void SetCallBack(const std::string point,
                   std::function<void(void*)> callback);
  // Clear all call back functions.
  void ClearAllCallBacks();

  // enable sync point processing (disabled on startup)
  void EnableProcessing();

  // disable sync point processing
  void DisableProcessing();

  // remove the execution trace of all sync points
  void ClearTrace();

  // triggered by TEST_SYNC_POINT, blocking execution until all predecessors
  // are executed.
  // And/or call registered callback functionn, with argument `cb_arg`
  void Process(const std::string& point, void* cb_arg = nullptr);

  // TODO(xxx): it might be useful to provide a function that blocks until all
  // sync points are cleared.

 private:
  bool PredecessorsAllCleared(const std::string& point);

  // successor/predecessor map loaded from LoadDependency
  std::unordered_map<std::string, std::vector<std::string>> successors_;
  std::unordered_map<std::string, std::vector<std::string>> predecessors_;
  std::unordered_map<std::string, std::function<void(void*)>> callbacks_;

  std::mutex mutex_;
  std::condition_variable cv_;
  // sync points that have been passed through
  std::unordered_set<std::string> cleared_points_;
  bool enabled_ = false;
  int num_callbacks_running_ = 0;
};

}  // namespace tendisplus

// Use TEST_SYNC_POINT to specify sync points inside code base.
// Sync points can have happens-after depedency on other sync points,
// configured at runtime via SyncPoint::LoadDependency. This could be
// utilized to re-produce race conditions between threads.
// See TransactionLogIteratorRace in db_test.cc for an example use case.
// TEST_SYNC_POINT is no op in release build.
#define TEST_SYNC_POINT(x) tendisplus::SyncPoint::GetInstance()->Process(x)
#define TEST_SYNC_POINT_CALLBACK(x, y) \
  tendisplus::SyncPoint::GetInstance()->Process(x, y)
#endif  // WITH_SYNC_POINT

#endif  // SRC_TENDISPLUS_UTILS_SYNC_POINT_H_
