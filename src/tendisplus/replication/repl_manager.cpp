#include <list>
#include <chrono>
#include <algorithm>
#include <fstream>
#include <string>
#include <set>
#include <map>

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/error/en.h"
#include "glog/logging.h"
#include "tendisplus/replication/repl_manager.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/redis_port.h"

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
    {
        std::lock_guard<std::mutex> lk(_mutex);
        if (_fetchClients[idx] != nullptr) {
            return _fetchClients[idx].get();
        }
    }

    auto source = [this, idx]() {
        std::lock_guard<std::mutex> lk(_mutex);
        return std::make_pair(
            _fetchMeta[idx]->syncFromHost,
            _fetchMeta[idx]->syncFromPort);
    }();

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

    std::shared_ptr<std::string> masterauth = _svr->masterauth();
    if (*masterauth != "") {
        std::stringstream ss;
        ss << "AUTH " << *masterauth;
        client->writeLine(ss.str(), std::chrono::seconds(1));
        Expected<std::string> s = client->readLine(std::chrono::seconds(1));
        if (!s.ok()) {
            LOG(WARNING) << "fullSync auth error:" << s.status().toString();
            return nullptr;
        }
        if (s.value().size() == 0 || s.value()[0] == '-') {
            LOG(INFO) << "fullSync auth failed:" << s.value();
            return nullptr;
        }
    }

    {
        std::lock_guard<std::mutex> lk(_mutex);
        _fetchClients[idx] = std::move(client);
        return _fetchClients[idx].get();
    }
}

void ReplManager::changeReplStateInLock(uint32_t idx, ReplState state,
        uint64_t binlogId, bool persist) {
    // TODO(deyukong): mechanism to assert mutex held
    auto nowState = _fetchMeta[idx]->copy();
    nowState->replState = state;
    nowState->binlogId = binlogId;
    if (persist) {
        Catalog *catalog = _svr->getCatalog();
        Status s = catalog->setStoreMeta(*nowState);
        if (!s.ok()) {
            LOG(FATAL) << "setStoreMeta failed:" << s.toString();
        }
    }
}

void ReplManager::changeReplState(uint32_t idx, ReplState state,
        uint64_t binlogId, bool persist) {
    std::lock_guard<std::mutex> lk(_mutex);
    changeReplStateInLock(idx, state, binlogId, persist);
}

void ReplManager::startFullSync(const StoreMeta& metaSnapshot) {
    LOG(INFO) << "store:" << metaSnapshot.id << " fullsync start";

    auto client = ensureClient(metaSnapshot.id);
    if (client == nullptr) {
        LOG(WARNING) << "startFullSync with: "
                    << metaSnapshot.syncFromHost << ":"
                    << metaSnapshot.syncFromPort
                    << " failed, no valid client";
        return;
    }

    bool rollback = true;
    auto guard = MakeGuard([this, &rollback, &metaSnapshot]{
        std::lock_guard<std::mutex> lk(_mutex);
        if (rollback) {
            _fetchClients[metaSnapshot.id].reset();
            changeReplStateInLock(metaSnapshot.id, ReplState::REPL_CONNECT,
                Transaction::MAX_VALID_TXNID+1, false);
        }
    });

    std::stringstream ss;
    ss << "FULLSYNC " << metaSnapshot.syncFromId;
    client->writeLine(ss.str(), std::chrono::seconds(1));
    Expected<std::string> s = client->readLine(std::chrono::seconds(3));
    if (!s.ok()) {
        LOG(WARNING) << "fullSync req master error:" << s.status().toString();
        return;
    }

    if (s.value().size() == 0 || s.value()[0] == '-') {
        LOG(INFO) << "fullSync req master failed:" << s.value();
        return;
    }

    changeReplState(metaSnapshot.id, ReplState::REPL_TRANSFER,
        Transaction::MAX_VALID_TXNID+1, false);

    auto expFlist = [&s]() -> Expected<std::map<std::string, size_t>> {
        rapidjson::Document doc;
        doc.Parse(s.value());
        if (doc.HasParseError()) {
            return {ErrorCodes::ERR_NETWORK,
                        rapidjson::GetParseError_En(doc.GetParseError())};
        }
        if (!doc.IsObject()) {
            return {ErrorCodes::ERR_NOTFOUND, "flist not json obj"};
        }
        std::map<std::string, size_t> result;
        for (auto& o : doc.GetObject()) {
            if (!o.value.IsUint64()) {
                return {ErrorCodes::ERR_NOTFOUND, "json value not uint64"};
            }
            result[o.name.GetString()] = o.value.GetUint64();
        }
        return result;
    }();

    // TODO(deyukong): split the transfering-physical-task into many
    // small schedule-unit, each processes one file, or a fixed-size block.
    if (!expFlist.ok()) {
        return;
    }

    PStore store = _svr->getSegmentMgr()->getInstanceById(metaSnapshot.id);
    assert(store != nullptr);
    if (store->isRunning()) {
        LOG(FATAL) << "BUG: store:" << metaSnapshot.id << " shouldnt be"
            << " running when logic comes to here";
    }
    Status clearStatus =  store->clear();
    if (!clearStatus.ok()) {
        LOG(FATAL) << "Unexpected store:" << metaSnapshot.id << " clear"
            << " failed:" << clearStatus.toString();
    }

    std::string fPrefix =  store->dbPath() + "/" + store->dbId();
    const std::map<std::string, size_t>& flist = expFlist.value();
    std::set<std::string> finished_files;
    while (true) {
        if (finished_files.size() == flist.size()) {
            break;
        }
        Expected<std::string> s = client->readLine(std::chrono::seconds(1));
        if (!s.ok()) {
            return;
        }
        if (finished_files.find(s.value()) != finished_files.end()) {
            LOG(FATAL) << "BUG: fullsync " << s.value() << " retransfer";
        }
        if (flist.find(s.value()) == flist.end()) {
            LOG(FATAL) << "BUG: fullsync " << s.value() << " invalid file";
        }
        std::string fullFileName = fPrefix + "/" + s.value();
        auto myfile = std::fstream(fullFileName,
                std::ios::out|std::ios::binary);
        if (!myfile.is_open()) {
            LOG(ERROR) << "open file:" << fullFileName << " for write failed";
            return;
        }
        size_t remain = flist.at(s.value());
        while (remain) {
            size_t batchSize = std::min(remain, size_t(20ULL*1024*1024));
            remain -= batchSize;
            Expected<std::string> exptData =
                client->read(batchSize, std::chrono::seconds(1));
            if (!exptData.ok()) {
                LOG(ERROR) << "fullsync read bulk data failed:"
                            << exptData.status().toString();
                return;
            }
            myfile.write(exptData.value().c_str(), exptData.value().size());
            if (myfile.bad()) {
                LOG(ERROR) << "write file:" << fullFileName
                            << " failed:" << strerror(errno);
                return;
            }
        }
        LOG(INFO) << "fullsync file:" << fullFileName << " transfer done";
        finished_files.insert(s.value());
    }

    Expected<std::string> expDone = client->readLine(std::chrono::seconds(1));
    if (!expDone.ok() || expDone.value().size() == 0) {
        LOG(ERROR) << "read FULLSYNCDONE from master failed:"
                    << expDone.status().toString();
        return;
    }
    std::string fullsyncDone;
    uint64_t binlogId = Transaction::MAX_VALID_TXNID+1;
    std::stringstream fullSyncDoneSs(expDone.value());
    fullSyncDoneSs >> fullsyncDone >> binlogId;
    if (fullsyncDone != "FULLSYNCDONE" ||
            binlogId == Transaction::MAX_VALID_TXNID+1) {
        LOG(ERROR) << "invalid FULLSYNCDONE command:" << expDone.value();
        return;
    }
    Status expDoneReplyStatus =
        client->writeLine("+OK", std::chrono::seconds(1));
    if (!expDoneReplyStatus.ok()) {
        LOG(ERROR) << "reply fullsync done failed:"
            << expDoneReplyStatus.toString();
        return;
    }

    changeReplState(metaSnapshot.id, ReplState::REPL_CONNECTED, binlogId, true);
    rollback = false;

    LOG(INFO) << "store:" << metaSnapshot.id << " fullsync Done";
}

// TODO(deyukong): fixme, remove the long long int
Expected<uint64_t> ReplManager::fetchBinlog(const StoreMeta& metaSnapshot) {
    constexpr size_t suggestBatch = 1024;
    auto client = ensureClient(metaSnapshot.id);
    if (client == nullptr) {
        return {ErrorCodes::ERR_NETWORK, "cant get client"};
    }
    bool resetClient = true;
    auto guard = MakeGuard([this, &resetClient, &metaSnapshot]{
        if (resetClient) {
            std::lock_guard<std::mutex> lk(_mutex);
            _fetchClients[metaSnapshot.id].reset();
        }
    });

    std::stringstream ss;
    ss << "FETCHBINLOG " << metaSnapshot.binlogId << " " << suggestBatch;
    Status s = client->writeLine(ss.str(), std::chrono::seconds(1));
    if (!s.ok()) {
        return s;
    }

    Expected<std::string> exptNum = client->readLine(std::chrono::seconds(1));
    if (!exptNum.ok()) {
        return exptNum.status();
    }
    if (exptNum.value().size() < 2 || exptNum.value()[0] != '*') {
        return {ErrorCodes::ERR_NETWORK, "marshaled FETCHBINLOG result"};
    }

    const std::string& numStr = exptNum.value();
    long long int arrayNum = -1;  // NOLINT
    bool ok = redis_port::string2ll(
        numStr.c_str() + 1,
        numStr.size() - 1,
        &arrayNum);
    if (!ok) {
        std::stringstream ss;
        ss << "decode numstr:" << numStr << " failed";
        return {ErrorCodes::ERR_NETWORK, ss.str()};
    }
    if (arrayNum % 2 != 0) {
        return {ErrorCodes::ERR_NETWORK, "binlog array odd size!"};
    }
    std::vector<std::string> rawBinlogs;
    for (long long int i = 0; i < arrayNum; i++) {  // NOLINT
        Expected<std::string> exptSize =
            client->readLine(std::chrono::seconds(1));
        if (!exptSize.ok()) {
            return exptSize.status();
        }
        if (exptSize.value().size() < 2 || exptSize.value()[0] != '$') {
            return {ErrorCodes::ERR_NETWORK, "marshaled FETCHBINLOG size"};
        }
        const std::string& sizeStr = exptSize.value();
        long long int rcdSize = -1;  // NOLINT
        ok = redis_port::string2ll(sizeStr.c_str() + 1,
            sizeStr.size() - 1, &rcdSize);
        if (!ok) {
            std::stringstream ss;
            ss << "decode binlogSize:" << sizeStr << " failed";
            return {ErrorCodes::ERR_NETWORK, ss.str()};
        }
        if (rcdSize <= 0) {
            LOG(FATAL) << "binlog record size:" << rcdSize << " invalid";
        }
        Expected<std::string> exptRcd =
            client->read(rcdSize + 2, std::chrono::seconds(1));
        if (!exptSize.ok()) {
            return exptSize.status();
        }
        rawBinlogs.emplace_back(
            std::string(exptRcd.value().c_str(), exptRcd.value().size()-2));
    }
    assert(rawBinlogs.size() == arrayNum);

    // from this point, the resp data from master are parsed succ,
    // so this client can be reused.
    resetClient = false;

    std::map<uint64_t, std::list<ReplLog>> binlogGroup;
    for (size_t i = 0; i < rawBinlogs.size(); i+=2) {
        Expected<ReplLog> logkv =
            ReplLog::decode(rawBinlogs[i], rawBinlogs[i+1]);
        if (!logkv.ok()) {
            return logkv.status();
        }
        const ReplLogKey& logKey = logkv.value().getReplLogKey();
        if (binlogGroup.find(logKey.getTxnId()) == binlogGroup.end()) {
            binlogGroup[logKey.getTxnId()] = std::list<ReplLog>();
        }
        binlogGroup[logKey.getTxnId()].emplace_back(std::move(logkv.value()));
    }
    for (const auto& logList : binlogGroup) {
        assert(logList.second.size() >= 1);
        const ReplLogKey& firstLogKey = logList.second.begin()->getReplLogKey();
        const ReplLogKey& lastLogKey = logList.second.rbegin()->getReplLogKey();
        if (!(static_cast<uint16_t>(firstLogKey.getFlag()) &
                static_cast<uint16_t>(ReplFlag::REPL_GROUP_START))) {
            LOG(FATAL) << "txnId:" << firstLogKey.getTxnId()
                << " first record not marked begin";
        }
        if (!(static_cast<uint16_t>(lastLogKey.getFlag()) &
                static_cast<uint16_t>(ReplFlag::REPL_GROUP_END))) {
            LOG(FATAL) << "txnId:" << lastLogKey.getTxnId()
                << " last record not marked begin";
        }
    }

    for (const auto& logList : binlogGroup) {
        Status s = applySingleTxn(metaSnapshot.id,
            logList.first, logList.second);
        if (!s.ok()) {
            return s;
        }
    }

    if (arrayNum > 0) {
        changeReplState(metaSnapshot.id, ReplState::REPL_CONNECTED,
                binlogGroup.rbegin()->first+1, true);
    }
    return {ErrorCodes::ERR_OK, ""};
}

Status ReplManager::applySingleTxn(uint32_t storeId, uint64_t txnId,
        const std::list<ReplLog>& ops) {
    PStore store = _svr->getSegmentMgr()->getInstanceById(storeId);
    assert(store);

    auto ptxn = store->createTransaction();
    if (!ptxn.ok()) {
        return ptxn.status();
    }

    std::unique_ptr<Transaction> txn = std::move(ptxn.value());
    // TODO(deyukong): insert oplogs
    for (const auto& log : ops) {
        const ReplLogValue& logVal = log.getReplLogValue();

        Expected<RecordKey> expRk = RecordKey::decode(logVal.getOpKey());
        if (!expRk.ok()) {
            return expRk.status();
        }

        switch (logVal.getOp()) {
            case (ReplOp::REPL_OP_SET): {
                Expected<RecordValue> expRv =
                    RecordValue::decode(logVal.getOpValue());
                if (!expRv.ok()) {
                    return expRv.status();
                }
                auto s = store->setKV(expRk.value(), expRv.value(), txn.get());
                if (!s.ok()) {
                    return s;
                }
            }
            case (ReplOp::REPL_OP_DEL): {
                auto s = store->delKV(expRk.value(), txn.get());
                if (!s.ok()) {
                    return s;
                }
            }
            default: {
                LOG(FATAL) << "invalid binlogOp:"
                            << static_cast<uint8_t>(logVal.getOp());
            }
        }
    }
    Expected<Transaction::CommitId> expCmit = txn->commit();
    if (!expCmit.ok()) {
        return expCmit.status();
    }
    return {ErrorCodes::ERR_OK, ""};
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
        startFullSync(*metaSnapshot);
        nextSched = nextSched + std::chrono::seconds(1);
        return;
    } else if (metaSnapshot->replState == ReplState::REPL_CONNECTED) {
        Expected<uint64_t> syncId = fetchBinlog(*metaSnapshot);
        if (!syncId.ok()) {
            nextSched = nextSched + std::chrono::seconds(10);
        } else if (syncId.value() == metaSnapshot->binlogId) {
            // nothing fetched
            nextSched = nextSched + std::chrono::seconds(1);
        } else {
            nextSched = SCLOCK::now();
        }
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
