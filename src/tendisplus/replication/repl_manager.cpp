#include <chrono>
#include <algorithm>

#include "glog/logging.h"
#include "tendisplus/replication/repl_manager.h"
#include "tendisplus/utils/scopeguard.h"

namespace tendisplus {

ReplManager::ReplManager(std::shared_ptr<ServerEntry> svr)
        :_isRunning(false),
         _svr(svr),
         _fetcherMatrix(std::make_shared<PoolMatrix>()),
         _supplierMatrix(std::make_shared<PoolMatrix>()) {
}

Status ReplManager::startup() {
    std::lock_guard<std::mutex> lk(_mutex);
    Catalog *catalog = _svr->getCatalog();
    if (!catalog) {
        LOG(FATAL) << "ReplManager::startup catalog not inited!";
    }

    for (uint32_t i = 0; i < KVStore::INSTANCE_NUM; i++) {
        Expected<std::unique_ptr<StoreMeta>> meta = catalog->getStoreMeta(i);
        if (meta.ok()) {
            _fetchMeta.emplace_back(std::move(meta.value()));
        } else if (meta.status().code() == ErrorCodes::ERR_NOTFOUND) {
            auto pMeta = std::unique_ptr<StoreMeta>(
                new StoreMeta(i, "", 0, -1,
                    Transaction::MAX_VALID_TXNID+1, ReplState::REPL_NONE));
            Status s = catalog->setStoreMeta(*pMeta);
            if (!s.ok()) {
                return s;
            }
            _fetchMeta.emplace_back(std::move(pMeta));
        } else {
            return meta.status();
        }
    }

    assert(_fetchMeta.size() == KVStore::INSTANCE_NUM);

    for (size_t i = 0; i < _fetchMeta.size(); ++i) {
        if (i != _fetchMeta[i]->id) {
            std::stringstream ss;
            ss << "meta:" << i << " has id:" << _fetchMeta[i]->id;
            return {ErrorCodes::ERR_INTERNAL, ss.str()};
        }
    }

    for (uint32_t i = 0; i < KVStore::INSTANCE_NUM; i++) {
        _fetchStatus.emplace_back(std::move(
            std::unique_ptr<FetchStatus>(
                new FetchStatus {
                    isRunning: false,
                    nextSchedTime: SCLOCK::now(),
                })));
    }

    for (uint32_t i = 0; i < KVStore::INSTANCE_NUM; i++) {
        _fetchClients.emplace_back(nullptr);
    }

    // TODO(deyukong): configure
    size_t cpuNum = std::thread::hardware_concurrency();
    if (cpuNum == 0) {
        return {ErrorCodes::ERR_INTERNAL, "cpu num cannot be detected"};
    }

    _fetcher = std::make_unique<WorkerPool>(_fetcherMatrix);
    Status s = _fetcher->startup(std::max(POOL_SIZE, cpuNum/2));
    if (!s.ok()) {
        return s;
    }

    _supplier = std::make_unique<WorkerPool>(_supplierMatrix);
    s = _supplier->startup(std::max(POOL_SIZE, cpuNum/2));
    if (!s.ok()) {
        return s;
    }

    _isRunning.store(true, std::memory_order_relaxed);
    _controller = std::thread([this]() {
        controlRoutine();
    });

    return {ErrorCodes::ERR_OK, ""};
}

BlockingTcpClient *ReplManager::ensureClient(uint32_t idx) {
    auto source = [this, idx]() {
        std::lock_guard<std::mutex> lk(_mutex);
        return std::make_pair(
            _fetchMeta[idx]->syncFromHost,
            _fetchMeta[idx]->syncFromPort);
    }();

    {
        std::lock_guard<std::mutex> lk(_mutex);
        if (_fetchClients[idx] != nullptr) {
            return _fetchClients[idx].get();
        }
    }

    auto client = std::make_unique<BlockingTcpClient>(64*1024*1024);
    Status s = client->connect(
        source.first,
        source.second,
        std::chrono::seconds(3));
    if (!s.ok()) {
        LOG(WARNING) << "connect " << source.first
            << ":" << source.second << " failed:"
            << s.toString();
        return nullptr;
    }

    {
        std::lock_guard<std::mutex> lk(_mutex);
        _fetchClients[idx] = std::move(client);
        return _fetchClients[idx].get();
    }
}

void ReplManager::startFullSync(std::unique_ptr<StoreMeta> metaSnapshot) {
    auto client = ensureClient(metaSnapshot->id);
    if (!client) {
        LOG(WARNING) << "startFullSync with: "
                    << metaSnapshot->syncFromHost << ":"
                    << metaSnapshot->syncFromPort
                    << " failed, no valid client";
        return;
    }

    // std::shared_ptr<std::string> masterauth = _svr->masterauth();
}

void ReplManager::fetchRoutine(uint32_t i) {
    SCLOCK::time_point nextSched = SCLOCK::now();
    auto guard = MakeGuard([this, &nextSched, i] {
        std::lock_guard<std::mutex> lk(_mutex);
        _fetchStatus[i]->isRunning = false;
        _fetchStatus[i]->nextSchedTime = nextSched;
    });

    std::unique_ptr<StoreMeta> metaSnapshot = [this, i]() {
        std::lock_guard<std::mutex> lk(_mutex);
        return std::move(_fetchMeta[i]->copy());
    }();
    assert(!metaSnapshot->isRunning);

    if (metaSnapshot->syncFromHost == "") {
        // if master is nil, try sched after 1 second
        nextSched = nextSched + std::chrono::seconds(1);
        return;
    }

    if (metaSnapshot->replState == ReplState::REPL_CONNECT) {
        startFullSync(std::move(metaSnapshot));
        return;
    }
}

void ReplManager::controlRoutine() {
    using namespace std::chrono_literals;  // (NOLINT)
    while (_isRunning.load(std::memory_order_relaxed)) {
        {
            std::lock_guard<std::mutex> lk(_mutex);
            for (size_t i = 0; i < _fetchStatus.size(); i++) {
                if (!_fetchStatus[i]->isRunning
                        && SCLOCK::now() >= _fetchStatus[i]->nextSchedTime) {
                    _fetchStatus[i]->isRunning = true;
                    _fetcher->schedule([this, i]() {
                        fetchRoutine(i);
                    });
                }
            }
        }

        std::this_thread::sleep_for(1s);
    }
}

void ReplManager::stop() {
    std::lock_guard<std::mutex> lk(_mutex);
    LOG(WARNING) << "repl manager begins stops...";
    _isRunning.store(false, std::memory_order_relaxed);
    _controller.join();
    _fetcher->stop();
    _supplier->stop();
    LOG(WARNING) << "repl manager stops succ";
}

}  // namespace tendisplus
