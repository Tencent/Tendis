#include <stdlib.h>
#include <functional>
#include <string>
#include "glog/logging.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/network/worker_pool.h"


namespace tendisplus {

std::string PoolMatrix::toString() const {
    std::stringstream ss;
    ss << "\ninQueue\t" << inQueue
        << "\nexecuted\t" << executed
        << "\nqueueTime\t" << queueTime << "ns"
        << "\nexecuteTime\t" << executeTime << "ns";
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
    result.executed = executed - right.executed;
    result.queueTime = queueTime - right.queueTime;
    result.executeTime = executeTime - right.executeTime;
    return result;
}

WorkerPool::WorkerPool(const std::string& name,
                       std::shared_ptr<PoolMatrix> poolMatrix)
    :_isRuning(false),
     _ioCtx(std::make_unique<asio::io_context>()),
     _name(name),
     _matrix(poolMatrix) {
}

bool WorkerPool::isFull() const {
    std::lock_guard<std::mutex> lk(_mutex);
    return _matrix->inQueue.get() >= _threads.size();
}

void WorkerPool::consumeTasks(size_t idx) {
    char threadName[64];
    memset(threadName, 0, sizeof threadName);
    pthread_getname_np(pthread_self(), threadName, sizeof threadName);
    LOG(INFO) << "workerthread:" << threadName << " starts";
    char* pname = &threadName[0];
    const auto guard = MakeGuard([this, pname]{
        std::lock_guard<std::mutex> lk(_mutex);
        const auto& thd_id = std::this_thread::get_id();
        for (auto v = _threads.begin(); v != _threads.end(); v++) {
            if (v->get_id() == thd_id) {
                LOG(INFO) << "thd:" << thd_id
                          << ",name:" << pname
                          << ", clean and exit";
                return;
            }
        }
        LOG(FATAL) << "thd:" << thd_id
                   << ",name:" << pname
                   << "not found in threads_list";
    });

    while (_isRuning.load(std::memory_order_relaxed)) {
        LOG(INFO) << "WorkerPool consumeTasks work:" << idx;
        // TODO(deyukong): use run_for to make threads more adaptive
        asio::io_context::work work(*_ioCtx);
        _ioCtx->run();
    }
}

void WorkerPool::stop() {
    LOG(INFO) << "workerPool begins to stop...";
    _isRuning.store(false, std::memory_order_relaxed);
    _ioCtx->stop();
    for (auto& t : _threads) {
        t.join();
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
            // NOTE(deyukong): pthread_setname_np requires the
            // name not longer than 15 chars.
            pthread_setname_np(pthread_self(), threadName.c_str());
            consumeTasks(i);
        });
        _threads.emplace_back(std::move(thd));
    }
    return {ErrorCodes::ERR_OK, ""};
}

}  // namespace tendisplus
