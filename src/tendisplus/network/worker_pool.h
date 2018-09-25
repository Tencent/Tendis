#ifndef SRC_TENDISPLUS_NETWORK_WORKER_POOL_H_
#define SRC_TENDISPLUS_NETWORK_WORKER_POOL_H_

#include <functional>
#include <utility>
#include <atomic>
#include <memory>
#include <vector>
#include <string>
#include "asio.hpp"
#include "tendisplus/utils/status.h"
#include "tendisplus/utils/time.h"
#include "tendisplus/utils/atomic_utility.h"

namespace tendisplus {

struct PoolMatrix {
    PoolMatrix operator-(const PoolMatrix& right);
    Atom<uint64_t> inQueue{0};
    Atom<uint64_t> executed{0};
    Atom<uint64_t> queueTime{0};
    Atom<uint64_t> executeTime{0};
    std::string toString() const;
};

// TODO(deyukong): currently only support static thread-num
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
        auto taskWrap = [this, mytask = std::move(task), enQueueTs] () mutable {
            int64_t outQueueTs = nsSinceEpoch();
            _matrix->queueTime += outQueueTs - enQueueTs;
            --_matrix->inQueue;
            mytask();
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

 private:
    void consumeTasks(size_t idx);
    mutable std::mutex _mutex;
    std::atomic<bool> _isRuning;
    // TODO(deyukong): single or multiple _ioCtx, which is better?
    std::unique_ptr<asio::io_context> _ioCtx;
    const std::string _name;
    std::shared_ptr<PoolMatrix> _matrix;
    std::vector<std::thread> _threads;
};

}  // namespace tendisplus
#endif  // SRC_TENDISPLUS_NETWORK_WORKER_POOL_H_
