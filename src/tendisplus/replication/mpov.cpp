#include <list>
#include <chrono>
#include <algorithm>
#include <fstream>
#include <string>
#include <set>
#include <map>
#include <limits>
#include <utility>
#include <memory>
#include <vector>

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/error/en.h"
#include "glog/logging.h"
#include "tendisplus/replication/repl_manager.h"
#include "tendisplus/storage/record.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/time.h"
#include "tendisplus/lock/lock.h"

namespace tendisplus {

void ReplManager::supplyFullSync(asio::ip::tcp::socket sock,
                        const std::string& storeIdArg) {
    std::shared_ptr<BlockingTcpClient> client =
        std::move(_svr->getNetwork()->createBlockingClient(
            std::move(sock), 64*1024*1024));

    // NOTE(deyukong): this judge is not precise
    // even it's not full at this time, it can be full during schedule.
    if (isFullSupplierFull()) {
        client->writeLine("-ERR workerpool full", std::chrono::seconds(1));
        return;
    }

    Expected<int64_t> expStoreId = stoul(storeIdArg);
    if (!expStoreId.ok() || expStoreId.value() < 0) {
        client->writeLine("-ERR invalid storeId", std::chrono::seconds(1));
        return;
    }
    uint32_t storeId = static_cast<uint32_t>(expStoreId.value());
    _fullPusher->schedule([this, storeId, client(std::move(client))]() mutable {
        supplyFullSyncRoutine(std::move(client), storeId);
    });
}

bool ReplManager::isFullSupplierFull() const {
    return _fullPusher->isFull();
}

void ReplManager::masterPushRoutine(uint32_t storeId, uint64_t clientId) {
    SCLOCK::time_point nextSched = SCLOCK::now();
    SCLOCK::time_point lastSend = SCLOCK::time_point::min();
    auto guard = MakeGuard([this, &nextSched, &lastSend, storeId, clientId] {
        std::lock_guard<std::mutex> lk(_mutex);
        auto& mpov = _pushStatus[storeId];
        if (mpov.find(clientId) == mpov.end()) {
            return;
        }
        INVARIANT(mpov[clientId]->isRunning);
        mpov[clientId]->isRunning = false;
        if (nextSched > mpov[clientId]->nextSchedTime) {
            mpov[clientId]->nextSchedTime = nextSched;
        }
        if (lastSend > mpov[clientId]->lastSendBinlogTime) {
            mpov[clientId]->lastSendBinlogTime = lastSend;
        }
        // currently nothing waits for master's push process
        // _cv.notify_all();
    });

    uint64_t binlogPos = 0;
    BlockingTcpClient *client = nullptr;
    uint32_t dstStoreId = 0;
    bool needHeartbeat = false;
    {
        std::lock_guard<std::mutex> lk(_mutex);
        if (_incrPaused ||
                _pushStatus[storeId].find(clientId) ==
                            _pushStatus[storeId].end()) {
            nextSched = nextSched + std::chrono::seconds(1);
            lastSend = _pushStatus[storeId][clientId]->lastSendBinlogTime;
            return;
        }
        binlogPos = _pushStatus[storeId][clientId]->binlogPos;
        client = _pushStatus[storeId][clientId]->client.get();
        dstStoreId = _pushStatus[storeId][clientId]->dstStoreId;
        lastSend = _pushStatus[storeId][clientId]->lastSendBinlogTime;
    }
#ifdef BINLOG_V1
    Expected<uint64_t> newPos =
            masterSendBinlog(client, storeId, dstStoreId, binlogPos);
#else
    // for safe : -5
    if (lastSend + std::chrono::seconds(BINLOGHEARTBEATSECS - 5) < SCLOCK::now()) {
        needHeartbeat = true;
    }

    Expected<uint64_t> newPos =
            masterSendBinlogV2(client, storeId, dstStoreId, binlogPos, needHeartbeat);
#endif
    if (!newPos.ok()) {
        LOG(WARNING) << "masterSendBinlog to client:"
                << client->getRemoteRepr() << " failed:"
                << newPos.status().toString();
        std::lock_guard<std::mutex> lk(_mutex);
        // it is safe to remove a non-exist key
        _pushStatus[storeId].erase(clientId);
        return;
    } else {
        if (newPos.value() > binlogPos) {
            nextSched = SCLOCK::now();
            lastSend = nextSched;
        } else {
            nextSched = SCLOCK::now() + std::chrono::seconds(1);
            if (needHeartbeat) {
                lastSend = SCLOCK::now();
            }
        }
        std::lock_guard<std::mutex> lk(_mutex);
        _pushStatus[storeId][clientId]->binlogPos = newPos.value();
    }
}

#ifdef BINLOG_V1
Expected<uint64_t> ReplManager::masterSendBinlog(BlockingTcpClient* client,
                uint32_t storeId, uint32_t dstStoreId, uint64_t binlogPos) {
    constexpr uint32_t suggestBatch = 64;
    constexpr size_t suggestBytes = 16*1024*1024;

    LocalSessionGuard sg(_svr);
    sg.getSession()->getCtx()->setArgsBrief(
        {"mastersendlog",
         std::to_string(storeId),
         client->getRemoteRepr(),
         std::to_string(dstStoreId),
         std::to_string(binlogPos)});

    auto expdb = _svr->getSegmentMgr()->getDb(sg.getSession(),
                    storeId, mgl::LockMode::LOCK_IS);
    if (!expdb.ok()) {
        return expdb.status();
    }
    auto store = std::move(expdb.value().store);
    INVARIANT(store != nullptr);

    auto ptxn = store->createTransaction();
    if (!ptxn.ok()) {
        return ptxn.status();
    }

    std::unique_ptr<Transaction> txn = std::move(ptxn.value());
    std::unique_ptr<BinlogCursor> cursor =
        txn->createBinlogCursor(binlogPos+1);

    std::vector<ReplLog> binlogs;
    uint32_t cnt = 0;
    uint64_t nowId = 0;
    size_t estimateSize = 0;

    while (true) {
        Expected<ReplLog> explog = cursor->next();
        if (explog.ok()) {
            cnt += 1;
            const ReplLogKey& rlk = explog.value().getReplLogKey();
            const ReplLogValue& rlv = explog.value().getReplLogValue();
            estimateSize += rlv.getOpValue().size();
            if (nowId == 0 || nowId != rlk.getTxnId()) {
                nowId = rlk.getTxnId();
                if (cnt >= suggestBatch || estimateSize >= suggestBytes) {
                    break;
                } else {
                    binlogs.emplace_back(std::move(explog.value()));
                }
            } else {
                binlogs.emplace_back(std::move(explog.value()));
            }
        } else if (explog.status().code() == ErrorCodes::ERR_EXHAUST) {
            // no more data
            break;
        } else {
            LOG(ERROR) << "iter binlog failed:"
                        << explog.status().toString();
            return explog.status();
        }
    }

    std::stringstream ss;
    Command::fmtMultiBulkLen(ss, binlogs.size()*2 + 2);
    Command::fmtBulk(ss, "applybinlogs");
    Command::fmtBulk(ss, std::to_string(dstStoreId));
    for (auto& v : binlogs) {
        ReplLog::KV kv = v.encode();
        Command::fmtBulk(ss, kv.first);
        Command::fmtBulk(ss, kv.second);
    }
    std::string stringtoWrite = ss.str();
    uint32_t secs = 1;
    if (stringtoWrite.size() > 1024*1024) {
        secs = 2;
    } else if (stringtoWrite.size() > 1024*1024*10) {
        secs = 4;
    }
    Status s = client->writeData(stringtoWrite, std::chrono::seconds(secs));
    if (!s.ok()) {
        return s;
    }

    // TODO(vinchen): NO NEED TO READ OK
    Expected<std::string> exptOK = client->readLine(std::chrono::seconds(secs));
    if (!exptOK.ok()) {
        return exptOK.status();
    } else if (exptOK.value() != "+OK") {
        LOG(WARNING) << "store:" << storeId << " dst Store:" << dstStoreId
                     << " apply binlogs failed:" << exptOK.value();
        return {ErrorCodes::ERR_NETWORK, "bad return string"};
    }

    if (binlogs.size() == 0) {
        return binlogPos;
    } else {
        return binlogs[binlogs.size()-1].getReplLogKey().getTxnId();
    }
}
#else
Expected<uint64_t> ReplManager::masterSendBinlogV2(BlockingTcpClient* client,
    uint32_t storeId, uint32_t dstStoreId, uint64_t binlogPos, bool needHeartBeart) {
    constexpr uint32_t suggestBatch = 64;
    constexpr size_t suggestBytes = 16 * 1024 * 1024;

    LocalSessionGuard sg(_svr);
    sg.getSession()->getCtx()->setArgsBrief(
    { "mastersendlog",
        std::to_string(storeId),
        client->getRemoteRepr(),
        std::to_string(dstStoreId),
        std::to_string(binlogPos) });

    auto expdb = _svr->getSegmentMgr()->getDb(sg.getSession(),
        storeId, mgl::LockMode::LOCK_IS);
    if (!expdb.ok()) {
        return expdb.status();
    }
    auto store = std::move(expdb.value().store);
    INVARIANT(store != nullptr);

    auto ptxn = store->createTransaction();
    if (!ptxn.ok()) {
        return ptxn.status();
    }

    std::unique_ptr<Transaction> txn = std::move(ptxn.value());
    std::unique_ptr<BinlogCursorV2> cursor =
        txn->createBinlogCursorV2(binlogPos + 1);

    uint32_t cnt = 0;
    size_t estimateSize = 0;
    uint64_t binlogId = binlogPos;

    std::stringstream ss;
    Command::fmtBulk(ss, "applybinlogsv2");
    Command::fmtBulk(ss, std::to_string(dstStoreId));
    while (true) {
        Expected<ReplLogRawV2> explog = cursor->next();
        if (explog.ok()) {
            cnt += 1;

            estimateSize += explog.value().getReplLogKey().size();
            estimateSize += explog.value().getReplLogValue().size();

            Command::fmtBulk(ss, explog.value().getReplLogKey());
            Command::fmtBulk(ss, explog.value().getReplLogValue());

            binlogId = explog.value().getBinlogId();

            if (estimateSize > suggestBytes || cnt >= suggestBatch) {
                break;
            }
        } else if (explog.status().code() == ErrorCodes::ERR_EXHAUST) {
            // no more data
            break;
        } else {
            LOG(ERROR) << "iter binlog failed:"
                << explog.status().toString();
            return explog.status();
        }
    }

    std::stringstream ss2;
    if (cnt == 0) {
        if (!needHeartBeart) {
            return binlogPos;
        }
        // keep the client alive
        Command::fmtMultiBulkLen(ss2, 2);
        Command::fmtBulk(ss2, "binlog_heartbeat");
        Command::fmtBulk(ss2, std::to_string(dstStoreId));
    } else {
        // TODO(vinchen): too more copy
        Command::fmtMultiBulkLen(ss2, cnt * 2 + 2);
        // ss2 << ss.rdbuf();
        ss2 << ss.str();
    }

    std::string stringtoWrite = ss2.str();
    uint32_t secs = 1;
    if (stringtoWrite.size() > 1024 * 1024) {
        secs = 2;
    } else if (stringtoWrite.size() > 1024 * 1024 * 10) {
        secs = 4;
    } else {
        // TODO(vinchen):
        secs = 100;
    }
    Status s = client->writeData(stringtoWrite, std::chrono::seconds(secs));
    if (!s.ok()) {
        LOG(WARNING) << "store:" << storeId << " dst Store:" << dstStoreId
            << " writeData failed:" << s.toString() << "; Size:" << stringtoWrite.size();
        return s;
    }

    // TODO(vinchen): NO NEED TO READ OK?
    Expected<std::string> exptOK = client->readLine(std::chrono::seconds(secs));
    if (!exptOK.ok()) {
        LOG(WARNING) << "store:" << storeId << " dst Store:" << dstStoreId
            << " readLine failed:" << exptOK.status().toString() << "; Size:" << stringtoWrite.size();
        return exptOK.status();
    } else if (exptOK.value() != "+OK") {
        LOG(WARNING) << "store:" << storeId << " dst Store:" << dstStoreId
            << " apply binlogs failed:" << exptOK.value();
        return{ ErrorCodes::ERR_NETWORK, "bad return string" };
    }

    if (cnt == 0) {
        return binlogPos;
    } else {
        return binlogId;
    }
}
#endif

//  1) s->m INCRSYNC (m side: session2Client)
//  2) m->s +OK
//  3) s->m +PONG (s side: client2Session)
//  4) m->s periodly send binlogs
//  the 3) step is necessary, if ignored, the +OK in step 2) and binlogs
//  in step 4) may sticky together. and redis-resp protocal is not fixed-size
//  That makes client2Session complicated.

// NOTE(deyukong): we define binlogPos the greatest id that has been applied.
// "NOT" the smallest id that has not been applied. keep the same with
// BackupInfo's setCommitId
void ReplManager::registerIncrSync(asio::ip::tcp::socket sock,
            const std::string& storeIdArg,
            const std::string& dstStoreIdArg,
            const std::string& binlogPosArg) {
    std::shared_ptr<BlockingTcpClient> client =
        std::move(_svr->getNetwork()->createBlockingClient(
            std::move(sock), 64*1024*1024));

    uint64_t storeId;
    uint64_t  dstStoreId;
    uint64_t binlogPos;
    try {
        storeId = stoul(storeIdArg);
        dstStoreId = stoul(dstStoreIdArg);
        binlogPos = stoul(binlogPosArg);
    } catch (const std::exception& ex) {
        std::stringstream ss;
        ss << "-ERR parse opts failed:" << ex.what();
        client->writeLine(ss.str(), std::chrono::seconds(1));
        return;
    }

    if (storeId >= _svr->getKVStoreCount() ||
            dstStoreId >= _svr->getKVStoreCount()) {
        client->writeLine("-ERR invalid storeId", std::chrono::seconds(1));
        return;
    }

    // NOTE(vinchen): In the cluster view, storeID of source and dest must be
    // same.
    if (storeId != dstStoreId) {
        client->writeLine("-ERR source storeId is different from dstStoreId ",
                    std::chrono::seconds(1));
        return;
    }

    LocalSessionGuard sg(_svr);
    auto expdb = _svr->getSegmentMgr()->getDb(sg.getSession(),
        storeId, mgl::LockMode::LOCK_IS);
    if (!expdb.ok()) {
        std::stringstream ss;
        ss << "-ERR store " << storeId << " error: "
            << expdb.status().toString();
        client->writeLine(ss.str(), std::chrono::seconds(1));
        return;
    }

    uint64_t firstPos = [this, storeId]() {
        std::lock_guard<std::mutex> lk(_mutex);
        return _logRecycStatus[storeId]->firstBinlogId;
    }();

    // NOTE(deyukong): this check is not precise
    // (not in the same critical area with the modification to _pushStatus),
    // but it does not harm correctness.
    // A strict check may be too complicated to read.
    if (firstPos > binlogPos) {
        std::stringstream ss;
        ss << "-ERR invalid binlogPos, firstPos:" << firstPos
            << ",binlogPos:" << binlogPos;
        client->writeLine(ss.str(), std::chrono::seconds(1));
        return;
    }
    client->writeLine("+OK", std::chrono::seconds(1));
    Expected<std::string> exptPong = client->readLine(std::chrono::seconds(1));
    if (!exptPong.ok()) {
        LOG(WARNING) << "slave incrsync handshake failed:"
                << exptPong.status().toString();
        return;
    } else if (exptPong.value() != "+PONG") {
        LOG(WARNING) << "slave incrsync handshake not +PONG:"
                << exptPong.value();
        return;
    }

    std::string remoteHost = client->getRemoteRepr();
    bool registPosOk =
            [this,
             storeId,
             dstStoreId,
             binlogPos,
             client = std::move(client)]() mutable {
        std::lock_guard<std::mutex> lk(_mutex);
        if (_logRecycStatus[storeId]->firstBinlogId > binlogPos) {
            return false;
        }
        uint64_t clientId = _clientIdGen.fetch_add(1);
        _pushStatus[storeId][clientId] =
            std::move(std::unique_ptr<MPovStatus>(
                new MPovStatus {
                     false,
                     static_cast<uint32_t>(dstStoreId),
                     binlogPos,
                     SCLOCK::now(),
                     SCLOCK::time_point::min(),
                     std::move(client),
                     clientId}));
        return true;
    }();
    LOG(INFO) << "slave:" << remoteHost
            << " registerIncrSync " << (registPosOk ? "ok" : "failed");
}

// mpov's network communicate procedure
// send binlogpos low watermark
// send filelist={filename->filesize}
// foreach file
//     send filename
//     send content
//     read +OK
// read +OK
void ReplManager::supplyFullSyncRoutine(
            std::shared_ptr<BlockingTcpClient> client, uint32_t storeId) {
    LocalSessionGuard sg(_svr);
    sg.getSession()->getCtx()->setArgsBrief(
        {"masterfullsync",
         client->getRemoteRepr(),
         std::to_string(storeId)});
    LOG(INFO) << "client:" << client->getRemoteRepr()
              << ",storeId:" << storeId
              << ",begins fullsync";
    auto expdb = _svr->getSegmentMgr()->getDb(sg.getSession(),
                storeId, mgl::LockMode::LOCK_IS);
    if (!expdb.ok()) {
        std::stringstream ss;
        ss << "-ERR store " << storeId << " error: "
            << expdb.status().toString();
        client->writeLine(ss.str(), std::chrono::seconds(1));
        return;
    }
    auto store = std::move(expdb.value().store);
    INVARIANT(store != nullptr);

    if (!store->isRunning()) {
        client->writeLine("-ERR store is not running", std::chrono::seconds(1));
        return;
    }

    uint64_t currTime = nsSinceEpoch();
    Expected<BackupInfo> bkInfo = store->backup(
        store->dftBackupDir(),
        KVStore::BackupMode::BACKUP_CKPT);
    if (!bkInfo.ok()) {
        std::stringstream ss;
        ss << "-ERR backup failed:" << bkInfo.status().toString();
        client->writeLine(ss.str(), std::chrono::seconds(1));
        return;
    } else {
        LOG(INFO) << "storeId:" << storeId
                  << ",backup cost:" << (nsSinceEpoch() - currTime) << "ns";
    }

    auto guard = MakeGuard([this, store, storeId]() {
        Status s = store->releaseBackup();
        if (!s.ok()) {
            LOG(ERROR) << "supplyFullSync end clean store:"
                    << storeId << " error:" << s.toString();
        }
    });

    // send binlogPos
    Status s = client->writeLine(
            std::to_string(bkInfo.value().getBinlogPos()),
            std::chrono::seconds(10));
    if (!s.ok()) {
        LOG(ERROR) << "store:" << storeId
                   << " fullsync send binlogpos failed:" << s.toString();
        return;
    }

    // send fileList
    rapidjson::StringBuffer sb;
    rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
    writer.StartObject();
    for (const auto& kv : bkInfo.value().getFileList()) {
        writer.Key(kv.first.c_str());
        writer.Uint64(kv.second);
    }
    writer.EndObject();
    s = client->writeLine(sb.GetString(), std::chrono::seconds(1000));
    if (!s.ok()) {
        LOG(ERROR) << "store:" << storeId
                   << " fullsync send filelist failed:" << s.toString();
        return;
    }

    std::string readBuf;
    readBuf.reserve(FILEBATCH);  // 20MB
    for (auto& fileInfo : bkInfo.value().getFileList()) {
        s = client->writeLine(fileInfo.first, std::chrono::seconds(10));
        if (!s.ok()) {
            LOG(ERROR) << "write fname:" << fileInfo.first
                        << " to client failed:" << s.toString();
            return;
        }
        std::string fname = store->dftBackupDir() + "/" + fileInfo.first;
        auto myfile = std::ifstream(fname, std::ios::binary);
        if (!myfile.is_open()) {
            LOG(ERROR) << "open file:" << fname << " for read failed";
            return;
        }
        size_t remain = fileInfo.second;
        while (remain) {
            size_t batchSize = std::min(remain, FILEBATCH);
            _rateLimiter->Request(batchSize);
            readBuf.resize(batchSize);
            remain -= batchSize;
            myfile.read(&readBuf[0], batchSize);
            if (!myfile) {
                LOG(ERROR) << "read file:" << fname
                            << " failed with err:" << strerror(errno);
                return;
            }
            s = client->writeData(readBuf, std::chrono::seconds(100));
            if (!s.ok()) {
                LOG(ERROR) << "write bulk to client failed:" << s.toString();
                return;
            }
            auto rpl = client->readLine(std::chrono::seconds(10));
            if (!rpl.ok() || rpl.value() != "+OK") {
                LOG(ERROR) << "send client:" << client->getRemoteRepr()
                           << "file:" << fileInfo.first
                           << ",size:" << fileInfo.second
                           << " failed:"
                           << (rpl.ok() ? rpl.value() : rpl.status().toString());      // NOLINT
                return;
            }
        }
    }
    Expected<std::string> reply = client->readLine(std::chrono::seconds(1));
    if (!reply.ok()) {
        LOG(ERROR) << "fullsync done read "
                   << client->getRemoteRepr() << " reply failed:"
                   << reply.status().toString();
    } else {
        LOG(INFO) << "fullsync done read "
                  << client->getRemoteRepr() << " reply:" << reply.value();
    }
}

}  // namespace tendisplus
