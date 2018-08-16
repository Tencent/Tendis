#include <functional>
#include "glog/logging.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/network/worker_pool.h"


namespace tendisplus {


void WorkerPool::consumeTasks(size_t idx) {
    LOG(INFO) << "net workerthread:" << idx << " starts";
    const auto guard = MakeGuard([this, idx] {
        std::lock_guard<std::mutex> lk(_mutex);
        const auto& thd_id = std::this_thread::get_id();
        for (auto v = _threads.begin(); v != _threads.end(); v++) {
            if (v->get_id() == thd_id) {
                _threads.erase(v);
                LOG(INFO) << "thd:" << thd_id << ",idx:"
                    << idx << "clean and exit";
                return;
            }
        }
        LOG(FATAL) << "thd:" << thd_id << ",idx:"
            << idx << "not found in threads_list";
    });

    while (_isRuning.load(std::memory_order_relaxed)) {
        // TODO(deyukong): use run_for to make threads more adaptive
        asio::io_context::work work(*_ioCtx);
        try {
            _ioCtx->run();
        } catch (const std::exception& ex) {
            LOG(FATAL) << "_ioCtx->run failed:" << ex.what();
        } catch (...) {
            LOG(FATAL) << "unknown exception";
        }
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
    for (size_t i = 0; i < poolsize; ++i) {
        std::thread thd = std::thread([this](size_t idx) {
            return [this, idx]() {
                consumeTasks(idx);
            };
        } (i));
        _threads.emplace_back(std::move(thd));
    }
}

}  // namespace tendisplus
