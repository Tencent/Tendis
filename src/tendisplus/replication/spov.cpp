#include <list>
#include <chrono>
#include <algorithm>
#include <fstream>
#include <string>
#include <set>
#include <map>
#include <limits>
#include <memory>
#include <utility>

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/error/en.h"
#include "glog/logging.h"
#include "tendisplus/replication/repl_manager.h"
#include "tendisplus/storage/record.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/lock/lock.h"
#include <endian.h>

namespace tendisplus {

Expected<BackupInfo> getBackupInfo(BlockingTcpClient* client,
                               const StoreMeta& metaSnapshot) {
    std::stringstream ss;
    ss << "FULLSYNC " << metaSnapshot.syncFromId;
    Status s = client->writeLine(ss.str(), std::chrono::seconds(1));
    if (!s.ok()) {
        LOG(WARNING) << "fullSync master failed:" << s.toString();
        return s;
    }

    BackupInfo bkInfo;
    auto expPos = client->readLine(std::chrono::seconds(1000));
    if (!expPos.ok()) {
        LOG(WARNING) << "fullSync req master error:"
                     << expPos.status().toString();
        return expPos.status();
    }
    if (expPos.value().size() == 0 || expPos.value()[0] == '-') {
        LOG(WARNING) << "fullSync req master failed:" << expPos.value();
        return {ErrorCodes::ERR_INTERNAL, "fullSync master not ok"};
    }
    Expected<uint64_t> pos = ::tendisplus::stoul(expPos.value());
    if (!pos.ok()) {
        LOG(WARNING) << "fullSync binlogpos parse fail:"
                     << pos.status().toString();
        return pos.status();
    }
    bkInfo.setBinlogPos(pos.value());

    auto expFlist = client->readLine(std::chrono::seconds(100));
    if (!expFlist.ok()) {
        LOG(WARNING) << "fullSync req flist error:"
                     << expFlist.status().toString();
        return expFlist.status();
    }
    rapidjson::Document doc;
    doc.Parse(expFlist.value());
    if (doc.HasParseError()) {
        return {ErrorCodes::ERR_NETWORK,
                    rapidjson::GetParseError_En(doc.GetParseError())};
    }
    if (!doc.IsObject()) {
        return {ErrorCodes::ERR_NOTFOUND, "flist not json obj"};
    }
    std::map<std::string, uint64_t> result;
#ifdef _WIN32
#undef GetObject
#endif
    for (auto& o : doc.GetObject()) {
        if (!o.value.IsUint64()) {
            return {ErrorCodes::ERR_NOTFOUND, "json value not uint64"};
        }
        result[o.name.GetString()] = o.value.GetUint64();
    }
    bkInfo.setFileList(result);
    return bkInfo;
}

// spov's network communicate procedure
// read binlogpos low watermark
// read filelist={filename->filesize}
// foreach file
//     read filename
//     read content
//     send +OK
// send +OK
void ReplManager::slaveStartFullsync(const StoreMeta& metaSnapshot) {
    LOG(INFO) << "store:" << metaSnapshot.id << " fullsync start";

    LocalSessionGuard sg(_svr);
    // NOTE(deyukong): there is no need to setup a guard to clean the temp ctx
    // since it's on stack
    sg.getSession()->setArgs(
        {"slavefullsync", std::to_string(metaSnapshot.id)});

    // 1) stop store and clean it's directory
    auto expdb = _svr->getSegmentMgr()->getDb(sg.getSession(), metaSnapshot.id,
        mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
        LOG(ERROR) << "get store:" << metaSnapshot.id
            << " failed: " << expdb.status().toString();
        return;
    }
    auto store = std::move(expdb.value().store);
    INVARIANT(store != nullptr);

    Status stopStatus = store->stop();
    if (!stopStatus.ok()) {
        // there may be uncanceled transactions binding with the store
        LOG(WARNING) << "stop store:" << metaSnapshot.id
                    << " failed:" << stopStatus.toString();
        return;
    }
    INVARIANT(!store->isRunning());
    Status clearStatus =  store->clear();
    if (!clearStatus.ok()) {
        LOG(FATAL) << "Unexpected store:" << metaSnapshot.id << " clear"
            << " failed:" << clearStatus.toString();
    }

    // 2) require a blocking-client
    std::shared_ptr<BlockingTcpClient> client =
        std::move(createClient(metaSnapshot));
    if (client == nullptr) {
        LOG(WARNING) << "startFullSync with: "
                    << metaSnapshot.syncFromHost << ":"
                    << metaSnapshot.syncFromPort
                    << " failed, no valid client";
        return;
    }

    // 3) necessary pre-conditions all ok, startup a guard to rollback
    // state if failed
    bool rollback = true;
    auto guard = MakeGuard([this, &rollback, &metaSnapshot]{
        std::lock_guard<std::mutex> lk(_mutex);
        if (rollback) {
            auto newMeta = metaSnapshot.copy();
            newMeta->replState = ReplState::REPL_CONNECT;
            newMeta->binlogId = Transaction::TXNID_UNINITED;
            changeReplStateInLock(*newMeta, false);
        }
    });

    auto newMeta = metaSnapshot.copy();
    newMeta->replState = ReplState::REPL_TRANSFER;
    newMeta->binlogId = Transaction::TXNID_UNINITED;
    changeReplState(*newMeta, false);

    // 4) read backupinfo from master
    auto bkInfo = getBackupInfo(client.get(), metaSnapshot);
    if (!bkInfo.ok()) {
        LOG(WARNING) << "storeId:" << metaSnapshot.id
                     << ",syncMaster:" << metaSnapshot.syncFromHost
                     << ":" << metaSnapshot.syncFromPort
                     << ":" << metaSnapshot.syncFromId
                     << " failed:" << bkInfo.status().toString();
        return;
    }

    // TODO(deyukong): split the transfering-physical-task into many
    // small schedule-unit, each processes one file, or a fixed-size block.
    auto backupExists = [store]() -> Expected<bool> {
        std::error_code ec;
        bool exists = filesystem::exists(
                        filesystem::path(store->dftBackupDir()), ec);
        if (ec) {
            return {ErrorCodes::ERR_INTERNAL, ec.message()};
        }
        return exists;
    }();
    if (!backupExists.ok() || backupExists.value()) {
        LOG(ERROR) << "store:" << metaSnapshot.id
            << " backupDir exists:" << store->dftBackupDir();
        return;
    }

    auto flist = bkInfo.value().getFileList();

    std::set<std::string> finishedFiles;
    while (true) {
        if (finishedFiles.size() == flist.size()) {
            break;
        }
        Expected<std::string> s = client->readLine(std::chrono::seconds(10));
        if (!s.ok()) {
            return;
        }
        if (finishedFiles.find(s.value()) != finishedFiles.end()) {
            LOG(FATAL) << "BUG: fullsync " << s.value() << " retransfer";
        }
        if (flist.find(s.value()) == flist.end()) {
            LOG(FATAL) << "BUG: fullsync " << s.value() << " invalid file";
        }
        std::string fullFileName = store->dftBackupDir() + "/" + s.value();
        filesystem::path fileDir =
                filesystem::path(fullFileName).remove_filename();
        if (!filesystem::exists(fileDir)) {
            filesystem::create_directories(fileDir);
        }
        auto myfile = std::fstream(fullFileName,
                    std::ios::out|std::ios::binary);
        if (!myfile.is_open()) {
            LOG(ERROR) << "open file:" << fullFileName << " for write failed";
            return;
        }
        size_t remain = flist.at(s.value());
        while (remain) {
            size_t batchSize = std::min(remain, FILEBATCH);
            remain -= batchSize;
            Expected<std::string> exptData =
                client->read(batchSize, std::chrono::seconds(100));
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
            Status s = client->writeLine("+OK", std::chrono::seconds(1));
            if (!s.ok()) {
                LOG(ERROR) << "write file:" << fullFileName
                           << " reply failed:" << s.toString();
                return;
            }
        }
        LOG(INFO) << "fullsync file:" << fullFileName << " transfer done";
        finishedFiles.insert(s.value());
    }

    client->writeLine("+OK", std::chrono::seconds(1));

    // 5) restart store, change to stready-syncing mode
    Expected<uint64_t> restartStatus = store->restart(true);
    if (!restartStatus.ok()) {
        LOG(FATAL) << "fullSync restart store:" << metaSnapshot.id
                   << ",failed:" << restartStatus.status().toString();
    }

    newMeta = metaSnapshot.copy();
    newMeta->replState = ReplState::REPL_CONNECTED;
    newMeta->binlogId = bkInfo.value().getBinlogPos();

    // NOTE(deyukong): the line below is commented, because it can not
    // hold true all times. since readonly-txns also increases binlogPos
    // INVARIANT(bkInfo.value().getBinlogPos() <= restartStatus.value());

    changeReplState(*newMeta, true);

    rollback = false;

    LOG(INFO) << "store:" << metaSnapshot.id
              << ",fullsync Done, files:" << finishedFiles.size()
              << ",binlogId:" << newMeta->binlogId
              << ",restart binlogId:" << restartStatus.value();
}

void ReplManager::slaveChkSyncStatus(const StoreMeta& metaSnapshot) {
    bool reconn = [this, &metaSnapshot] {
        std::lock_guard<std::mutex> lk(_mutex);
        auto sessionId = _syncStatus[metaSnapshot.id]->sessionId;
        auto lastSyncTime = _syncStatus[metaSnapshot.id]->lastSyncTime;
        if (sessionId == std::numeric_limits<uint64_t>::max()) {
            return true;
        }
        if (lastSyncTime + std::chrono::seconds(BINLOGHEARTBEATSECS)
            <= SCLOCK::now()) {
            return true;
        }
        return false;
    }();

    if (!reconn) {
        return;
    }
    LOG(INFO) << "store:" << metaSnapshot.id
                << " reconn with:" << metaSnapshot.syncFromHost
                << "," << metaSnapshot.syncFromPort
                << "," << metaSnapshot.syncFromId;

    std::shared_ptr<BlockingTcpClient> client =
        std::move(createClient(metaSnapshot));
    if (client == nullptr) {
        LOG(WARNING) << "store:" << metaSnapshot.id << " reconn master failed";
        return;
    }

    std::stringstream ss;
    ss << "INCRSYNC " << metaSnapshot.syncFromId
        << ' ' << metaSnapshot.id
        << ' ' << metaSnapshot.binlogId;
    client->writeLine(ss.str(), std::chrono::seconds(1));
    Expected<std::string> s = client->readLine(std::chrono::seconds(10));
    if (!s.ok()) {
        LOG(WARNING) << "store:" << metaSnapshot.id
                << " psync master failed with error:" << s.status().toString();
        return;
    }
    if (s.value().size() == 0 || s.value()[0] != '+') {
        LOG(WARNING) << "store:" << metaSnapshot.id
                << " incrsync master bad return:" << s.value();
        return;
    }

    Status pongStatus  = client->writeLine("+PONG", std::chrono::seconds(1));
    if (!pongStatus.ok()) {
        LOG(WARNING) << "store:" << metaSnapshot.id
                << " write pong failed:" << pongStatus.toString();
        return;
    }

    NetworkAsio *network = _svr->getNetwork();
    INVARIANT(network != nullptr);

    // why dare we transfer a client to a session ?
    // 1) the logic gets here, so there wont be any
    // async handlers in the event queue.
    // 2) every handler is triggered by calling client's
    // some read/write/connect functions.
    // 3) master side will read +PONG before sending
    // new data, so there wont be any sticky packets.
    Expected<uint64_t> expSessionId =
            network->client2Session(std::move(client));
    if (!expSessionId.ok()) {
        LOG(WARNING) << "client2Session failed:"
                    << expSessionId.status().toString();
        return;
    }
    uint64_t sessionId = expSessionId.value();

    {
        std::lock_guard<std::mutex> lk(_mutex);
        uint64_t currSessId = _syncStatus[metaSnapshot.id]->sessionId;
        if (currSessId != std::numeric_limits<uint64_t>::max()) {
            Status s = _svr->cancelSession(currSessId);
            LOG(INFO) << "sess:" << currSessId
                    << ",discard status:"
                    << (s.ok() ? "ok" : s.toString());
        }
        _syncStatus[metaSnapshot.id]->sessionId = sessionId;
        _syncStatus[metaSnapshot.id]->lastSyncTime = SCLOCK::now();
    }
    LOG(INFO) << "store:" << metaSnapshot.id
        << ",binlogId:" << metaSnapshot.binlogId
        << " psync master succ."
        << "session id: " << sessionId << ";";
}

void ReplManager::slaveSyncRoutine(uint32_t storeId) {
    SCLOCK::time_point nextSched = SCLOCK::now();
    auto guard = MakeGuard([this, &nextSched, storeId] {
        std::lock_guard<std::mutex> lk(_mutex);
        INVARIANT(_syncStatus[storeId]->isRunning);
        _syncStatus[storeId]->isRunning = false;
        if (nextSched > _syncStatus[storeId]->nextSchedTime) {
            _syncStatus[storeId]->nextSchedTime = nextSched;
        }
        _cv.notify_all();
    });

    std::unique_ptr<StoreMeta> metaSnapshot = [this, storeId]() {
        std::lock_guard<std::mutex> lk(_mutex);
        return std::move(_syncMeta[storeId]->copy());
    }();

    if (metaSnapshot->syncFromHost == "") {
        // if master is nil, try sched after 1 second
        LOG(WARNING) << "metaSnapshot->syncFromHost is nil, sleep 10 seconds";
        nextSched = nextSched + std::chrono::seconds(10);
        return;
    }

    INVARIANT(metaSnapshot->replState == ReplState::REPL_CONNECT ||
        metaSnapshot->replState == ReplState::REPL_CONNECTED);

    if (metaSnapshot->replState == ReplState::REPL_CONNECT) {
        slaveStartFullsync(*metaSnapshot);
        nextSched = nextSched + std::chrono::seconds(3);
        return;
    } else if (metaSnapshot->replState == ReplState::REPL_CONNECTED) {
        slaveChkSyncStatus(*metaSnapshot);
        nextSched = nextSched + std::chrono::seconds(10);
        return;
    } else {
        INVARIANT(false);
    }
}

#ifdef BINLOG_V1
Status ReplManager::applyBinlogs(uint32_t storeId, uint64_t sessionId,
            const std::map<uint64_t, std::list<ReplLog>>& binlogs) {
    // NOTE(deyukong): donot lock store in IX mode again
    // the caller has duty to do this thing.
    [this, storeId]() {
        std::unique_lock<std::mutex> lk(_mutex);
        _cv.wait(lk,
                [this, storeId]
                {return !_syncStatus[storeId]->isRunning;});
        _syncStatus[storeId]->isRunning = true;
    }();

    bool idMatch = [this, storeId, sessionId]() {
        std::unique_lock<std::mutex> lk(_mutex);
        return (sessionId == _syncStatus[storeId]->sessionId);
    }();
    auto guard = MakeGuard([this, storeId, &idMatch] {
        std::unique_lock<std::mutex> lk(_mutex);
        INVARIANT(_syncStatus[storeId]->isRunning);
        _syncStatus[storeId]->isRunning = false;
        if (idMatch) {
            _syncStatus[storeId]->lastSyncTime = SCLOCK::now();
        }
    });

    if (!idMatch) {
        return {ErrorCodes::ERR_NOTFOUND, "sessionId not match"};
    }

    for (const auto& logList : binlogs) {
        Status s = applySingleTxn(storeId, logList.first, logList.second);
        if (!s.ok()) {
            return s;
        }
    }

    // TODO(deyukong): perf and maybe periodly save binlogpos
    if (binlogs.size() > 0) {
        std::lock_guard<std::mutex> lk(_mutex);
        auto newMeta = _syncMeta[storeId]->copy();
        newMeta->binlogId = binlogs.rbegin()->first;
        INVARIANT(newMeta->replState == ReplState::REPL_CONNECTED);
        changeReplStateInLock(*newMeta, true);
    }
    return {ErrorCodes::ERR_OK, ""};
}

// NOTE(deyukong): should be called with lock held
Status ReplManager::applySingleTxn(uint32_t storeId, uint64_t txnId,
                                   const std::list<ReplLog>& ops) {
    auto expdb = _svr->getSegmentMgr()->getDb(nullptr, storeId,
        mgl::LockMode::LOCK_NONE);
    if (!expdb.ok()) {
        return expdb.status();
    }
    auto store = std::move(expdb.value().store);
    INVARIANT(store != nullptr);
    auto ptxn = store->createTransaction(nullptr);
    if (!ptxn.ok()) {
        return ptxn.status();
    }

    std::unique_ptr<Transaction> txn = std::move(ptxn.value());
    Status s = store->applyBinlog(ops, txn.get());
    if (!s.ok()) {
        return s;
    }
    Expected<uint64_t> expCmit = txn->commit();
    if (!expCmit.ok()) {
        return expCmit.status();
    }

    // NOTE(vinchen): store the binlog time spov when txn commited.
    store->setBinlogTime(txn->getBinlogTime());
    return {ErrorCodes::ERR_OK, ""};
}
#else
// if logKey == "", it means binlog_heartbeat
Status ReplManager::applyRepllogV2(Session* sess, uint32_t storeId,
        const std::string& logKey, const std::string& logValue) {
    [this, storeId]() {
        std::unique_lock<std::mutex> lk(_mutex);
        _cv.wait(lk,
            [this, storeId]
        {return !_syncStatus[storeId]->isRunning; });
        _syncStatus[storeId]->isRunning = true;
    }();

    uint64_t sessionId = sess->id();
    bool idMatch = [this, storeId, sessionId]() {
        std::unique_lock<std::mutex> lk(_mutex);
        return (sessionId == _syncStatus[storeId]->sessionId);
    }();
    auto guard = MakeGuard([this, storeId, &idMatch] {
        std::unique_lock<std::mutex> lk(_mutex);
        INVARIANT(_syncStatus[storeId]->isRunning);
        _syncStatus[storeId]->isRunning = false;
        if (idMatch) {
            _syncStatus[storeId]->lastSyncTime = SCLOCK::now();
        }
    });

    if (!idMatch) {
        return{ ErrorCodes::ERR_NOTFOUND, "sessionId not match" };
    }

    if (logKey == "") {
        // binlog_heartbeat
        // do nothing
    } else {
        auto binlog = applySingleTxnV2(sess, storeId, logKey, logValue);
        if (!binlog.ok()) {
            return binlog.status();
        } else {
            std::lock_guard<std::mutex> lk(_mutex);
            // NOTE(vinchen): store the binlogId without changeReplState()
            // If it's shutdown, we can get the largest binlogId from rocksdb.
            _syncMeta[storeId]->binlogId = binlog.value();
        }
    }
    return{ ErrorCodes::ERR_OK, "" };
}


Expected<uint64_t> ReplManager::applySingleTxnV2(Session* sess, uint32_t storeId,
    const std::string& logKey, const std::string& logValue) {
    // TODO(takenliu) should use LOCK_X ?
    auto expdb = _svr->getSegmentMgr()->getDb(sess, storeId,
        mgl::LockMode::LOCK_IX);
    if (!expdb.ok()) {
        return expdb.status();
    }
    auto store = std::move(expdb.value().store);
    INVARIANT(store != nullptr);
    auto ptxn = store->createTransaction(sess);
    if (!ptxn.ok()) {
        return ptxn.status();
    }

    std::unique_ptr<Transaction> txn = std::move(ptxn.value());

    auto key = ReplLogKeyV2::decode(logKey);
    if (!key.ok()) {
        return key.status();
    }
    auto binlogId = key.value().getBinlogId();

    auto value = ReplLogValueV2::decode(logValue);
    if (!value.ok()) {
        return value.status();
    }

    uint64_t timestamp = 0;
    size_t offset = value.value().getHdrSize();
    auto data = value.value().getData();
    size_t dataSize = value.value().getDataSize();
    while (offset < dataSize) {
        size_t size = 0;
        auto entry = ReplLogValueEntryV2::decode((const char*)data + offset,
                        dataSize - offset, &size);
        if (!entry.ok()) {
            return entry.status();
        }
        offset += size;

        timestamp = entry.value().getTimestamp();

        auto s = txn->applyBinlog(entry.value());
        if (!s.ok()) {
            return s;
        }
    }

    if (offset != dataSize) {
        return { ErrorCodes::ERR_INTERNAL, "bad binlog" };
    }

    // store the binlog directly, same as master
    auto s = txn->setBinlogKV(binlogId, logKey, logValue);
    if (!s.ok()) {
        return s;
    }

    Expected<uint64_t> expCmit = txn->commit();
    if (!expCmit.ok()) {
        return expCmit.status();
    }

    // NOTE(vinchen): store the binlog time spov when txn commited.
    // only need to set the last timestamp
    store->setBinlogTime(timestamp);

    return binlogId;
}
#endif

#ifdef BINLOG_V1
Status ReplManager::saveBinlogs(uint32_t storeId,
    const std::list<ReplLog>& logs) {
    if (logs.size() == 0) {
        return {ErrorCodes::ERR_OK, ""};
    }
    std::ofstream *fs = nullptr;
    uint32_t currentId = 0;
    {
        std::unique_lock<std::mutex> lk(_mutex);
        fs = _logRecycStatus[storeId]->fs.get();
        currentId = _logRecycStatus[storeId]->fileSeq;
    }
    if (fs == nullptr) {
        char fname[128], tbuf[128];
        memset(fname, 0, 128);
        memset(tbuf, 0, 128);

        // ms to second
        time_t time = (time_t)(uint32_t)(logs.front().getReplLogKey().getTimestamp()/1000);     // NOLINT
        struct tm lt;
        (void) localtime_r(&time, &lt);
        strftime(tbuf, sizeof(tbuf), "%Y%m%d%H%M%S", &lt);

        snprintf(fname, sizeof(fname), "%s/%d/binlog-%d-%07d-%s.log",
            _dumpPath.c_str(), storeId, storeId, currentId+1, tbuf);
        fs = new std::ofstream(fname,
            std::ios::out|std::ios::app|std::ios::binary);
        if (!fs->is_open()) {
            std::stringstream ss;
            ss << "open:" << fname << " failed";
            return {ErrorCodes::ERR_INTERNAL, ss.str()};
        }
        std::unique_lock<std::mutex> lk(_mutex);
        auto& v = _logRecycStatus[storeId];
        v->fs.reset(fs);
        v->fileSeq = currentId + 1;
        v->fileCreateTime = SCLOCK::now();
        v->fileSize = 0;
    }

    uint64_t written = 0;
    // NOTE(deyukong):
    // |txnId 8byte be|keyLen 4byte be|key|valLen 4byte be|val|
    for (const auto& v : logs) {
        auto kv = v.encode();
        uint64_t txnId = v.getReplLogKey().getTxnId();
        uint64_t txnIdTrans = htobe64(txnId);
        fs->write(reinterpret_cast<char*>(&txnIdTrans), sizeof(txnIdTrans));

        uint32_t keyLen = kv.first.size();
        uint32_t keyLenTrans = htobe32(keyLen);
        fs->write(reinterpret_cast<char*>(&keyLenTrans), sizeof(keyLenTrans));
        fs->write(kv.first.c_str(), keyLen);

        uint32_t valLen = kv.second.size();
        uint32_t valLenTrans = htobe32(valLen);
        fs->write(reinterpret_cast<char*>(&valLenTrans), sizeof(valLenTrans));
        fs->write(kv.second.c_str(), valLen);
        written += keyLen + valLen + sizeof(keyLen) + sizeof(valLen) +
            sizeof(txnIdTrans);
    }
    INVARIANT(fs->good());
    {
        std::unique_lock<std::mutex> lk(_mutex);
        auto& v = _logRecycStatus[storeId];
        v->fileSize += written;
        if (v->fileSize >= ReplManager::BINLOGSIZE
         || v->fileCreateTime +
             std::chrono::seconds(ReplManager::BINLOGSYNCSECS)
                            <= SCLOCK::now()) {
            v->fs->close();
            v->fs.reset();
        }
    }
    return {ErrorCodes::ERR_OK, ""};
}
#else
std::ofstream* ReplManager::getCurBinlogFs(uint32_t storeId) {
    std::ofstream *fs = nullptr;
    uint32_t currentId = 0;
    uint64_t ts = 0;
    {
        std::unique_lock<std::mutex> lk(_mutex);
        fs = _logRecycStatus[storeId]->fs.get();
        currentId = _logRecycStatus[storeId]->fileSeq;
        ts = _logRecycStatus[storeId]->timestamp;
    }
    if (fs == nullptr) {
        char fname[256], tbuf[256];
        memset(fname, 0, 128);
        memset(tbuf, 0, 128);

        // ms to second
        time_t time = (time_t)(uint32_t)(ts / 1000);
        struct tm lt;
        (void)localtime_r(&time, &lt);
        strftime(tbuf, sizeof(tbuf), "%Y%m%d%H%M%S", &lt);

        snprintf(fname, sizeof(fname), "%s/%d/binlog-%d-%07d-%s.log",
            _dumpPath.c_str(), storeId, storeId, currentId + 1, tbuf);

        fs = KVStore::createBinlogFile(fname, storeId);
        if (!fs) {
            return fs;
        }

        std::unique_lock<std::mutex> lk(_mutex);
        auto& v = _logRecycStatus[storeId];
        v->fs.reset(fs);
        v->fileSeq = currentId + 1;
        v->fileCreateTime = SCLOCK::now();
        v->fileSize = BINLOG_HEADER_V2_LEN;
    }
    return fs;
}

void ReplManager::updateCurBinlogFs(uint32_t storeId, uint64_t written,
                uint64_t ts, bool flushFile) {
    std::unique_lock<std::mutex> lk(_mutex);
    auto& v = _logRecycStatus[storeId];
    v->fileSize += written;
    if (v->fileSize >= ReplManager::BINLOGSIZE
        || v->fileCreateTime +
        std::chrono::seconds(ReplManager::BINLOGSYNCSECS)
        <= SCLOCK::now()
        || flushFile) {
        if (v->fs) {
            v->fs->close();
            v->fs.reset();
        }
        if (ts) {
            v->timestamp = ts;
        }
    }
}


#endif

}  // namespace tendisplus
