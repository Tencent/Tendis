// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_NETWORK_WORKER_POOL_H_
#define SRC_TENDISPLUS_NETWORK_WORKER_POOL_H_

#include <functional>
#include <utility>
#include <atomic>
#include <memory>
#include <vector>
#include <string>
#include <map>

#include "asio.hpp"
#include "tendisplus/server/server_params.h"
#include "tendisplus/utils/status.h"
#include "tendisplus/utils/time.h"
#include "tendisplus/utils/atomic_utility.h"
#include "tendisplus/utils/invariant.h"

namespace tendisplus {

class IOCtxException : public std::exception {
 public:
  IOCtxException() : std::exception() {}
};

class PoolMatrix {
 public:
  PoolMatrix operator-(const PoolMatrix& right);
  Atom<uint64_t> inQueue{0};
  Atom<uint64_t> executing{0};
  Atom<uint64_t> executed{0};
  Atom<uint64_t> queueTime{0};
  Atom<uint64_t> executeTime{0};
  std::string toString() const;
  std::string getInfoString() const;
  void reset();
};

// TODO(pecochen): currently only support static thread-num
// It's better to adaptively resize thread-pool by pressure
class WorkerPool {
 public:
  explicit WorkerPool(const std::string& name,
                      std::shared_ptr<PoolMatrix> poolMatrix);
  WorkerPool(const WorkerPool&) = delete;
  WorkerPool(WorkerPool&&) = delete;
  Status startup(size_t poolSize);
  bool isFull() const;
  template <typename fn>
  void schedule(fn&& task) {
    int64_t enQueueTs = nsSinceEpoch();
    ++_matrix->inQueue;
    auto taskWrap = [this, mytask = std::move(task), enQueueTs]() mutable {
      int64_t outQueueTs = nsSinceEpoch();
      _matrix->queueTime += outQueueTs - enQueueTs;
      ++_matrix->executing;
      try {
        mytask();
      } catch (const std::exception& ex) {
        std::stringstream ss;
        ss << "schedule task error:" << ex.what();
        LOG(ERROR) << ss.str();
        INVARIANT_D(0);
      }
      --_matrix->inQueue;
      --_matrix->executing;
      int64_t endExeTs = nsSinceEpoch();
      _matrix->executeTime += endExeTs - outQueueTs;
      ++_matrix->executed;
    };
    asio::post(*_ioCtx, std::move(taskWrap));
    // NOTE(deyukong): use asio::post rather than ctx.post, the latter one
    // only support copyable callbacks. which means you cannot use a lambda
    // which captures unique_Ptr as params
    // refers here: https://github.com/boostorg/asio/issues/61
    // _ioCtx->post(std::move(taskWrap));
  }
  void stop();
  size_t size() const;
  void resize(size_t poolSize);

 private:
  void consumeTasks(size_t idx);
  void resizeIncrease(size_t size);
  void resizeDecrease(size_t size);
  mutable std::mutex _mutex;
  std::atomic<bool> _isRuning;
  std::unique_ptr<asio::io_context> _ioCtx;
  const std::string _name;
  std::shared_ptr<PoolMatrix> _matrix;
  std::atomic<uint64_t> _idGenerator;
  std::map<std::thread::id, std::thread> _threads;
};

}  // namespace tendisplus
#endif  // SRC_TENDISPLUS_NETWORK_WORKER_POOL_H_
