#include <utility>
#include <memory>
#include "glog/logging.h"
#include "tendisplus/server/server_entry.h"
#include "tendisplus/server/server_params.h"

namespace tendisplus {

ServerEntry::ServerEntry()
        :_isRunning(false),
         _isStopped(true),
         _network(nullptr),
         _executor(nullptr) {
}

Status ServerEntry::startup(std::shared_ptr<ServerParams> cfg) {
    std::lock_guard<std::mutex> lk(_mutex);
    _network = std::make_unique<NetworkAsio>(shared_from_this());
    auto s = _network->prepare(cfg->bindIp, cfg->port);
    if (!s.ok()) {
        return s;
    }
    s = _network->run();
    if (!s.ok()) {
        return s;
    }
    _executor = std::make_unique<WorkerPool>();
    size_t cpuNum = std::thread::hardware_concurrency();
    if (cpuNum == 0) {
        return {ErrorCodes::ERR_INTERNAL, "cpu num cannot be detected"};
    }
    s = _executor->startup(std::max(size_t(4), cpuNum/2));
    if (!s.ok()) {
        return s;
    }
    return {ErrorCodes::ERR_OK, ""};
}

void ServerEntry::addSession(std::unique_ptr<NetSession> sess) {
    std::lock_guard<std::mutex> lk(_mutex);
    if (!_isRunning.load(std::memory_order_relaxed)) {
        LOG(WARNING) << "session:" << sess->getRemoteRepr()
            << "comes when stopping, ignore it";
        return;
    }
    // TODO(deyukong): max conns


    // NOTE(deyukong): god's first driving force
    sess->start();
    uint64_t id = sess->getConnId();
    if (_sessions.find(id) != _sessions.end()) {
        LOG(FATAL) << "add conn:" << id << ",id already exists";
    }
    _sessions[id] = std::move(sess);
}

void ServerEntry::endSession(uint64_t connId) {
    std::lock_guard<std::mutex> lk(_mutex);
    if (!_isRunning.load(std::memory_order_relaxed)) {
        return;
    }
    auto it = _sessions.find(connId);
    if (it == _sessions.end()) {
        LOG(FATAL) << "destroy conn:" << connId << ",not exists";
    }
    _sessions.erase(it);
}

void ServerEntry::processReq(uint64_t connId) {
    NetSession *sess = nullptr;
    {
        std::lock_guard<std::mutex> lk(_mutex);
        if (!_isRunning.load(std::memory_order_relaxed)) {
            return;
        }
        auto it = _sessions.find(connId);
        if (it == _sessions.end()) {
            LOG(FATAL) << "conn:" << connId << ",invalid state";
        }
        sess = it->second.get();
        if (sess == nullptr) {
            LOG(FATAL) << "conn:" << connId << ",null in servermap";
        }
    }
    sess->setOkRsp();
}

void ServerEntry::waitStopComplete() {
    std::unique_lock<std::mutex> lk(_mutex);
    _eventCV.wait(lk, [this] {
        return _isRunning.load(std::memory_order_relaxed) == false
            && _isStopped.load(std::memory_order_relaxed) == true;
    });
}

void ServerEntry::stop() {
    LOG(INFO) << "server begins to stop...";
    std::lock_guard<std::mutex> lk(_mutex);
    _isRunning.store(false, std::memory_order_relaxed);
    _network->stop();
    _executor->stop();
    _sessions.clear();
    LOG(INFO) << "server stops complete...";
    _eventCV.notify_all();
}

}  // namespace tendisplus
