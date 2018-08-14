#ifndef SRC_TENDISPLUS_NETWORK_WORKER_POOL_H_
#define SRC_TENDISPLUS_NETWORK_WORKER_POOL_H_

#include <functional>
#include <utility>
#include <atomic>
#include <memory>
#include <vector>
#include "asio.hpp"
#include "tendisplus/utils/status.h"

namespace tendisplus {

// TODO(deyukong): currently only support static thread-num
// It's better to adaptively resize thread-pool by pressure
class WorkerPool {
 public:
    WorkerPool() = default;
    WorkerPool(const WorkerPool&) = delete;
    WorkerPool(WorkerPool&&) = delete;
    Status startup(size_t poolSize);
    template <typename fn>
    void schedule(fn&& task) {
        auto taskWrap = [this, mytask = std::move(task)] {
            mytask();
        };
        _ioCtx->post(std::move(taskWrap));
    }
    void stop();

 private:
    std::mutex _mutex;
    std::atomic<bool> _isRuning;
    void consumeTasks(size_t idx);
    // TODO(deyukong): single or multiple _ioCtx, which is better?
    std::unique_ptr<asio::io_context> _ioCtx;
    std::vector<std::thread::id> _threads;
};

}  // namespace tendisplus
#endif  // SRC_TENDISPLUS_NETWORK_WORKER_POOL_H_
