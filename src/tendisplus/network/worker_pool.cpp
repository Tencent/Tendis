// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <stdlib.h>
#include <functional>
#include <string>
#include "glog/logging.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/network/worker_pool.h"
#include "tendisplus/utils/invariant.h"


namespace tendisplus {

std::string PoolMatrix::toString() const {
  std::stringstream ss;
  ss << "\ninQueue\t" << inQueue << "\nexecuting\t" << executing
     << "\nexecuted\t" << executed << "\nqueueTime\t" << queueTime << "ns"
     << "\nexecuteTime\t" << executeTime << "ns";
  return ss.str();
}

std::string PoolMatrix::getInfoString() const {
  std::stringstream ss;
  ss << "inQueue " << inQueue << ",executing " << executing
     << ",executed " << executed << ",queueTime " << queueTime << "ns"
     << ",executeTime " << executeTime << "ns";
  return ss.str();
}

void PoolMatrix::reset() {
  inQueue = 0;
  executed = 0;
  queueTime = 0;
  executeTime = 0;
}

PoolMatrix PoolMatrix::operator-(const PoolMatrix& right) {
  PoolMatrix result;
  // inQueue is a state, donot handle it
  result.inQueue = inQueue;
  result.executing = executing;
  result.executed = executed - right.executed;
  result.queueTime = queueTime - right.queueTime;
  result.executeTime = executeTime - right.executeTime;
  return result;
}

WorkerPool::WorkerPool(const std::string& name,
                       std::shared_ptr<PoolMatrix> poolMatrix)
  : _isRuning(false),
    _ioCtx(std::make_unique<asio::io_context>()),
    _name(name),
    _matrix(poolMatrix),
    _idGenerator(0) {}

bool WorkerPool::isFull() const {
  std::lock_guard<std::mutex> lk(_mutex);
  return _matrix->inQueue.get() >= _threads.size();
}

void WorkerPool::consumeTasks(size_t idx) {
  bool detachFlag{false};
  char threadName[64];
  memset(threadName, 0, sizeof threadName);
  pthread_getname_np(pthread_self(), threadName, sizeof threadName);
  char* pname = &threadName[0];
  const auto guard = MakeGuard([this, pname, &detachFlag] {
    std::lock_guard<std::mutex> lk(_mutex);
    const auto& thd_id = std::this_thread::get_id();
    if (_threads.count(thd_id)) {
      // erase thread in scope guard, only here.
      // NOTE: after detach thread itself, std::this_thread::get_id()
      //       can't get the thread id.Because of this, we don't erase
      //       other thread. It's really correct.
      if (detachFlag) {
        // NOTE: make sure std::thread::detach() and
        // std::container::erase() execute together, or
        // serverEntry::shutdown() may get the lock, then occurs error.
        _threads[thd_id].detach();
        _threads.erase(thd_id);
        LOG(INFO) << "thd: " << thd_id << ",name:" << pname << ", detach()";
        LOG(INFO) << "thd: " << thd_id << ",name:" << pname
                  << ", clean and exit";
        return;
      }
      if (_threads[thd_id].get_id() == thd_id) {
        LOG(INFO) << "thd: " << thd_id << ",name:" << pname
                  << ", clean and exit";
        return;
      }
    }
    LOG(FATAL) << "thd:" << thd_id << ",name:" << pname
               << "not found in threads_list";
  });

  while (_isRuning.load(std::memory_order_relaxed)) {
    DLOG(INFO) << "WorkerPool consumeTasks work:" << idx;
    asio::io_context::work work(*_ioCtx);
    try {
      _ioCtx->run();
    } catch (const IOCtxException&) {
      auto id = std::this_thread::get_id();
      auto iter = _threads.find(id);
      if (iter != _threads.cend()) {
        // NOTE: if in need, we can log exiting thread info with its
        // name from pthread_getname_np();
        detachFlag = true;
        return;
      } else {
        LOG(ERROR) << "This thread isn't in collection";
      }
    } catch (const std::exception& ex) {
      LOG(ERROR) << "Workerpool:" << pname << " occurs error:" << ex.what();
      INVARIANT_D(0);
    }
  }
}

void WorkerPool::stop() {
  LOG(INFO) << "workerPool begins to stop...";
  _isRuning.store(false, std::memory_order_relaxed);
  _ioCtx->stop();
  for (auto& t : _threads) {
    t.second.join();
  }
  LOG(INFO) << "workerPool stops complete...";
}

Status WorkerPool::startup(size_t poolsize) {
  std::lock_guard<std::mutex> lk(_mutex);

  // worker threads rely on _isRunning flag
  _isRuning.store(true, std::memory_order_relaxed);

  for (size_t i = 0; i < poolsize; ++i) {
    std::thread thd = std::thread([this, i]() {
      std::string threadName = _name + "_" + std::to_string(i);
      threadName.resize(15);  // pthread_setname_np allows a maximum thread
                              // name of 16 bytes including the trailing '\0'
      INVARIANT(!pthread_setname_np(pthread_self(), threadName.c_str()));
      consumeTasks(i);
    });
    auto tid = thd.get_id();
    _threads.emplace(tid, std::move(thd));
  }
  _idGenerator.store(poolsize, std::memory_order_relaxed);
  return {ErrorCodes::ERR_OK, ""};
}

size_t WorkerPool::size() const {
  std::lock_guard<std::mutex> lk(_mutex);
  return _threads.size();
}

/**
 * @brief resize, impl in resizeIncrease() and resizeDecrease()
 * @param poolSize, the pool size number
 */
void WorkerPool::resize(size_t poolSize) {
  std::lock_guard<std::mutex> lk(_mutex);

  // poolSize == 0 means stop the worker pool
  // if (poolSize == 0) {
  //     stop();
  //     return;
  // }
  // executor list need to resize to 0,
  // stop pool by workerPool::stop() directly will lost complete events

  auto size = _threads.size();
  if (poolSize > size) {
    resizeIncrease(poolSize - size);
  } else if (size > poolSize) {
    resizeDecrease(size - poolSize);
  } else {
    return;  // no need to resize
  }
}

/**
 * @brief increase the pool thread size
 * @param size result to increase by calc (poolSize - container.size())
 * @note  logic like WorkerPool::startup(), but should increase _idGenerator.
 */
void WorkerPool::resizeIncrease(size_t size) {
  for (size_t i = 0; i < size; ++i) {
    std::thread thd = std::thread([this]() {
      std::string threadName = _name + "_" +
        std::to_string(_idGenerator.load(std::memory_order_relaxed));
      threadName.resize(15);
      INVARIANT(!pthread_setname_np(pthread_self(), threadName.c_str()));
      consumeTasks(_idGenerator.load(std::memory_order_relaxed));
    });
    auto tid = thd.get_id();
    _threads.emplace(tid, std::move(thd));
    _idGenerator.fetch_add(1, std::memory_order::memory_order_relaxed);
  }
}

/**
 * @brief decrease the pool thread size
 * @param size size result to decrease by calc (container.size() - poolsize)
 * @note logic core is that use exception to exit asio::io_context::run().
 *      1. detach the thread in try-catch scope,
 *      2. erase thread from list in scopeguard created by makeGuard(), or may
 * occur bugs.
 */
void WorkerPool::resizeDecrease(size_t size) {
  for (size_t i = 0; i < size; ++i) {
    auto exceptionTask = []() { throw IOCtxException(); };
    this->schedule(std::move(exceptionTask));
  }
}

}  // namespace tendisplus
