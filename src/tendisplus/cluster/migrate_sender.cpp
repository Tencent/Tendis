#include <utility>
#include "glog/logging.h"
#include "tendisplus/cluster/migrate_sender.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/replication/repl_util.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/invariant.h"
namespace tendisplus {

ChunkMigrateSender::ChunkMigrateSender(const std::bitset<CLUSTER_SLOTS>& slots,
    std::shared_ptr<ServerEntry> svr,
    std::shared_ptr<ServerParams> cfg,
    bool is_fake) :
    _slots(slots),
    _svr(svr),
    _cfg(cfg),
    _isFake(is_fake),
    _taskid(""),
    _clusterState(svr->getClusterMgr()->getClusterState()),
    _sendstate(MigrateSenderStatus::SNAPSHOT_BEGIN),
    _storeid(0),
    _snapshotKeyNum(0),
    _binlogNum(0),
    _delSlot(0),
    _consistency(false),
    _nodeid(""),
    _curBinlogid(UINT64_MAX),
    _dstIp(""),
    _dstPort(0),
    _dstStoreid(0),
    _dstNode(nullptr) {
}

Status ChunkMigrateSender::sendChunk() {
    LOG(INFO) <<"sendChunk begin on store:" << _storeid
              << " slots:" << bitsetStrEncode(_slots);
    uint64_t start = msSinceEpoch();
    /* send Snapshot of bitmap data */
    Status s = sendSnapshot();
    if (!s.ok()) {
        return s;
    }
    auto snapshot_binlog = _curBinlogid;
    _sendstate = MigrateSenderStatus::SNAPSHOT_DONE;
    uint64_t sendSnapTimeEnd = msSinceEpoch();
    /* send binlog of the task slots in iteration of 10(default),
     * make sure the diff offset of srcNode and DstNode is small enough*/
    s = sendBinlog();
    if (!s.ok()) {
        auto s2 = sendOver();
        if (!s2.ok()) {
            return s2;
        }
        return s;
    }
    auto send_binlog = _curBinlogid;
    _sendstate = MigrateSenderStatus::BINLOG_DONE;
    /* lock chunks to block the client for while */
    auto lockStart = msSinceEpoch();
    s = lockChunks();
    if (!s.ok()) {
        return s;
    }
    /* finish sending the diff offset of maxbinlog */
    s = sendLastBinlog();
    if (!s.ok()) {
        /* if error, it need to unlock chunks */
        unlockChunks();
        auto s2 = sendOver();
        if (!s2.ok()) {
            return s2;
        }
        return s;
    }
    /* in chunk lock, send syncversion meta */
    s = sendVersionMeta();
    if (!s.ok()) {
        unlockChunks();
        auto s2 = sendOver();
        if (!s2.ok()) {
            return s2;
        }
        LOG(ERROR) << "send version meta Fail:" << s.toString();
        return s;
    }
    auto last_binlog = _curBinlogid;
    _sendstate = MigrateSenderStatus::LASTBINLOG_DONE;
    /* send migrateend command and wait for return*/
    s = sendOver();
    if (!s.ok()) {
        /* if error, it can't unlock chunks. It should
         * wait the gossip message. */
        return s;
    }
    _sendstate = MigrateSenderStatus::SENDOVER_DONE;
    /* check the meta data of source node */
    if (!checkSlotsBlongDst()) {
        s = _clusterState->setSlots(_dstNode, _slots);
        if (!s.ok()) {
            LOG(ERROR) << "set myself meta data fail on slots:" << s.toString();
            return s;
        }
        _clusterState->clusterSaveNodes();
    }
    /* unlock after receive package */
    unlockChunks();
    auto end = msSinceEpoch();
    _sendstate = MigrateSenderStatus::METACHANGE_DONE;

    serverLog(LL_NOTICE, "ChunkMigrateSender::sendChunk success"
        " [%s] [%s] [total used time:%lu] [snapshot time:%lu]"
        " [send binlog time:%lu] [locked time:%lu]"
        " [snapshot count:%lu] [binlog count:%lu] [binlog:%lu %lu %lu]",
        _client->getRemoteRepr().c_str(),
        bitsetStrEncode(_slots).c_str(),
        end - start,
        sendSnapTimeEnd - start,
        lockStart - sendSnapTimeEnd,
        end - lockStart,
        _snapshotKeyNum,
        _binlogNum,
        snapshot_binlog, send_binlog, last_binlog
        );
    return{ ErrorCodes::ERR_OK, "" };
}

void ChunkMigrateSender::setDstNode(const std::string nodeid) {
    _nodeid = nodeid;
    _dstNode = _clusterState->clusterLookupNode(_nodeid);
}

void ChunkMigrateSender::setSenderStatus(MigrateSenderStatus s) {
    _sendstate  = s;
}

// check if bitmap all belong to dst node
bool ChunkMigrateSender::checkSlotsBlongDst() {
    std::lock_guard<myMutex> lk(_mutex);
    for (size_t id =0; id < _slots.size(); id++) {
        if (_slots.test(id)) {
            if (_clusterState->getNodeBySlot(id) != _dstNode) {
                return false;
             }
        }
    }
    return true;
}

Expected<std::unique_ptr<Transaction>> ChunkMigrateSender::initTxn() {
    auto kvstore = _dbWithLock->store;
    auto ptxn = kvstore->createTransaction(nullptr);
    if (!ptxn.ok()) {
        return ptxn.status();
    }
    ptxn.value()->SetSnapshot();
    return  ptxn;
}

Expected<uint64_t> ChunkMigrateSender::sendRange(Transaction* txn,
                                uint32_t begin, uint32_t end) {
    // LOG(INFO) << "snapshot sendRange send begin, store:"
    //          << _storeid << " beginSlot:" << begin
    //          << " endSlot:" << end;
    // need add IS lock for chunks ???
    auto cursor = std::move(txn->createSlotsCursor(begin, end));
    uint32_t totalWriteNum = 0;
    uint32_t curWriteLen = 0;
    uint32_t curWriteNum = 0;
    uint32_t timeoutSec = 100;
    Status s;
    while (true) {
        Expected<Record> expRcd = cursor->next();
        if (expRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
            break;
        }

        if (!expRcd.ok()) {
            LOG(ERROR) << "snapshot sendRange failed storeid:" << _storeid
                << " err:" << expRcd.status().toString();
            return expRcd.status();
        }
        Record &rcd = expRcd.value();
        const RecordKey &rcdKey = rcd.getRecordKey();

        std::string key = rcdKey.encode();
        const RecordValue& rcdValue = rcd.getRecordValue();
        std::string value = rcdValue.encode();

        SyncWriteData("0");

        uint32_t keylen = key.size();
        SyncWriteData(string(reinterpret_cast<char*>(&keylen),
                                sizeof(uint32_t)));

        SyncWriteData(key);

        uint32_t valuelen = value.size();
        SyncWriteData(string(reinterpret_cast<char*>(&valuelen),
                                sizeof(uint32_t)));
        SyncWriteData(value);

        curWriteNum++;
        totalWriteNum++;
        uint64_t sendBytes = 1 + sizeof(uint32_t) + keylen
                               + sizeof(uint32_t) + valuelen;
        curWriteNum += sendBytes;

        /* *
         * rate limit for migration
         */
        _svr->getMigrateManager()->requestRateLimit(sendBytes);

        if (curWriteNum >= 10000 || curWriteLen > 10*1024*1024) {
            SyncWriteData("1");
            SyncReadData(exptData, _OKSTR.length(), timeoutSec)
            if (exptData.value() != _OKSTR) {
                LOG(ERROR) << "read data is not +OK."
                           << "totalWriteNum:" << totalWriteNum
                           << " curWriteNum:" << curWriteNum
                           << " data:" << exptData.value();
                return { ErrorCodes::ERR_INTERNAL, "read +OK failed"};
            }
            curWriteNum = 0;
            curWriteLen = 0;
        }
    }
    // send over of one slot
    SyncWriteData("2");
    SyncReadData(exptData, _OKSTR.length(), timeoutSec)

    if (exptData.value() != _OKSTR) {
        LOG(ERROR) << "read receiver data is not +OK on slot:" << begin;
        return { ErrorCodes::ERR_INTERNAL, "read +OK failed"};
    }
    // LOG(INFO) << "snapshot sendRange end, storeid:" << _storeid
    //    << " beginSlot:" << begin
    //    << " endSlot:" << end
    //    << " totalKeynum:" << totalWriteNum;

    return totalWriteNum;
}

// deal with slots that is not continuous
Status ChunkMigrateSender::sendSnapshot() {
    Status s;
    auto expdb = _svr->getSegmentMgr()->getDb(NULL, _storeid,
                                              mgl::LockMode::LOCK_IS);
    if (!expdb.ok()) {
        return expdb.status();
    }
    _dbWithLock = std::make_unique<DbWithLock>(std::move(expdb.value()));
    auto kvstore = _dbWithLock->store;

    {
        std::lock_guard<myMutex> lk(_mutex);
        _curBinlogid = kvstore->getHighestBinlogId();
    }
    LOG(INFO) << "sendSnapshot begin, storeid:" << _storeid
              <<" _curBinlogid:" << _curBinlogid
              <<" slots:" << bitsetStrEncode(_slots);
    uint32_t startTime = sinceEpoch();
    auto eTxn = initTxn();
    if (!eTxn.ok()) {
        return eTxn.status();
    }
    uint32_t timeoutSec = 10;
    uint32_t sendSlotNum = 0;
    for (size_t  i = 0; i < CLUSTER_SLOTS; i++) {
        if (_slots.test(i)) {
            sendSlotNum++;
            auto ret = sendRange(eTxn.value().get(), i , i+1);
            if (!ret.ok()) {
                LOG(ERROR) << "sendRange failed, slot:" << i << "-" << i + 1;
                return ret.status();
            }
            _snapshotKeyNum += ret.value();
        }
    }
    SyncWriteData("3");  // send over of all
    SyncReadData(exptData, _OKSTR.length(), timeoutSec)
    if (exptData.value() != _OKSTR) {
        LOG(ERROR) << "read receiver data is not +OK, data:"
                   << exptData.value();
        return { ErrorCodes::ERR_INTERNAL, "read +OK failed"};
    }
    uint32_t endTime = sinceEpoch();
    LOG(INFO) << "sendSnapshot finished, storeid:" << _storeid
              << " sendSlotNum:" << sendSlotNum
              << " totalWriteNum:" << _snapshotKeyNum
              << " useTime:" << endTime - startTime
              << " slots:" << bitsetStrEncode(_slots);
    return  {ErrorCodes::ERR_OK, ""};
}

uint64_t ChunkMigrateSender::getMaxBinLog(Transaction* ptxn) const {
    uint64_t maxBinlogId = 0;
    auto expBinlogidMax = RepllogCursorV2::getMaxBinlogId(ptxn);
    if (!expBinlogidMax.ok()) {
        if (expBinlogidMax.status().code() != ErrorCodes::ERR_EXHAUST) {
            LOG(ERROR) << "slave offset getMaxBinlogId error:"
                    << expBinlogidMax.status().toString();
        }
    } else {
        maxBinlogId = expBinlogidMax.value();
    }
    return maxBinlogId;
}

Status ChunkMigrateSender::resetClient() {
    std::lock_guard<myMutex> lk(_mutex);
    //  bool needHeartbeat = false;
    //   bool needRetry = false;
    setClient(nullptr);
    std::shared_ptr<BlockingTcpClient> client =
            std::move(_svr->getNetwork()->createBlockingClient(64 * 1024 * 1024));

    Status s = client->connect(
            _dstIp,
            _dstPort,
            std::chrono::seconds(3));
    if (!s.ok()) {
        LOG(WARNING) << "send binlog connect " << _dstIp
                     << ":" << _dstPort << " failed:"
                     << s.toString();
        return {ErrorCodes::ERR_NETWORK, ""};
    }
    setClient(client);
    return  {ErrorCodes::ERR_OK, ""};
}
// catch up binlog from _curBinlogid to end
Status ChunkMigrateSender::catchupBinlog(uint64_t end) {
    bool needHeartbeat = false;
    bool needRetry = false;
    uint64_t binlogNum = 0;
    uint64_t newBinlogId = 0;
    if (_curBinlogid >= end) {
        return { ErrorCodes::ERR_OK, "" };
    }
    bool done = false;
    Status s;

    for (uint32_t i = 0; i < RETRY_CNT; ++i) {
        s = SendSlotsBinlog(_client.get(), _storeid, _dstStoreid,
                            _curBinlogid, end, needHeartbeat, _slots, _taskid,
                            _svr, &binlogNum, &newBinlogId, &needRetry);
        /* NOTE(wayenchen) may have already sended half binlog but fail, 
            so update the _curbinlog first*/
        {
            std::lock_guard<myMutex> lk(_mutex);
            _binlogNum += binlogNum;
            if (newBinlogId != 0) {
                _curBinlogid = newBinlogId;
            }
        }
        if (s.ok()) {
            done = true;
            break;
        }

        if (!s.ok() && needRetry) {
            if (newBinlogId > 0) {
                _curBinlogid = newBinlogId;
            }
            needRetry = false;
            s = resetClient();
            if (!s.ok()) {
                LOG(ERROR) << "reset client fail on slots:"
                           << bitsetStrEncode(_slots);
                continue;
            }
            LOG(INFO) << "reconn receiver on slots:"
                      << bitsetStrEncode(_slots);
        }
    }

    if (!done) {
        serverLog(LL_NOTICE, "ChunkMigrateSender::catchupBinlog from"
                                     " %lu to %lu failed: [%s] [%s] [%s]",
            _curBinlogid,
            end,
            _client->getRemoteRepr().c_str(),
            bitsetStrEncode(_slots).c_str(),
            s.toString().c_str());

        return s;
    } else {
        serverLog(LL_VERBOSE, "ChunkMigrateSender::catchupBinlog from"
            " %lu to %lu ok: [%s] [%s]",
            _curBinlogid,
            end,
            _client->getRemoteRepr().c_str(),
            bitsetStrEncode(_slots).c_str());
    }

    return s;
}

Status ChunkMigrateSender::sendBinlog() {
    PStore kvstore = _dbWithLock->store;
    uint32_t distance = _svr->getParams()->migrateDistance;
    uint16_t iterNum = _svr->getParams()->migrateBinlogIter;
    bool finishCatchup = false;
    uint32_t catchupTimes = 0;
    uint64_t binlogHigh = kvstore->getHighestBinlogId();
    uint64_t diffOffset = 0;
    auto start = _curBinlogid;

    serverLog(LL_NOTICE, "ChunkMigrateSender::sendBinlog from"
        " %lu begin: [%s] [%s]",
        start,
        _client->getRemoteRepr().c_str(),
        bitsetStrEncode(_slots).c_str());

    while (catchupTimes < iterNum) {
        auto s = catchupBinlog(binlogHigh);
        if (!s.ok()) {
            // retry
            return s;
        }
        catchupTimes++;

        binlogHigh = kvstore->getHighestBinlogId();
        // judge if reach for distance
        diffOffset = binlogHigh - _curBinlogid;
        if (diffOffset <= distance) {
            serverLog(LL_VERBOSE,
                "ChunkMigrateSender::sendBinlog in"
                " kvstore(%u) finish: [%s] [%s] [%lu]",
                _storeid,
                _client->getRemoteRepr().c_str(),
                bitsetStrEncode(_slots).c_str(),
                diffOffset);

            finishCatchup = true;
            break;
        }
    }
    if (!finishCatchup) {
        serverLog(LL_NOTICE,
            "ChunkMigrateSender::sendBinlog in kvstore(%u) failed:"
            " [%s] [%s] [Can't catchup binlog(%lu), distance:%lu]",
            _storeid,
            _client->getRemoteRepr().c_str(),
            bitsetStrEncode(_slots).c_str(),
            binlogHigh,
            diffOffset);
        return { ErrorCodes::ERR_INTERNAL, "send binlog not finish" };
    }

    serverLog(LL_NOTICE, "ChunkMigrateSender::sendBinlog from"
        " %lu to %lu end: [%s] [%s]",
        start,
        _curBinlogid,
        _client->getRemoteRepr().c_str(),
        bitsetStrEncode(_slots).c_str());
    return  { ErrorCodes::ERR_OK, "" };
}

Status ChunkMigrateSender::sendLastBinlog() {
    PStore kvstore = _dbWithLock->store;
    auto ptxn = kvstore->createTransaction(nullptr);
    if (!ptxn.ok()) {
        LOG(ERROR) << "send binlog create transaction fail:"
                   << "on slots:" << bitsetStrEncode(_slots);
        return ptxn.status();
    }
    auto maxBinlogId = getMaxBinLog(ptxn.value().get());
    auto s = catchupBinlog(maxBinlogId);
    if (!s.ok()) {
        return s;
    }

    serverLog(LL_VERBOSE, "ChunkMigrateSender::sendLastBinlog"
        " in kvstore(%u) ok: [%s] [%s] [send binlog total num:%lu]",
        _storeid,
        _client->getRemoteRepr().c_str(),
        bitsetStrEncode(_slots).c_str(),
        _binlogNum);

    return {ErrorCodes::ERR_OK, ""};
}


// versionmeta send every migrate task, so in migrating versionmeta may send
// many times
Status ChunkMigrateSender::sendVersionMeta() {
    PStore kvstore = _dbWithLock->store;

    auto ptxn = kvstore->createTransaction(nullptr);
    auto txn = std::move(ptxn.value());
    std::unique_ptr<VersionMetaCursor> cursor = txn->createVersionMetaCursor();
    std::vector<VersionMeta> versionMeta;
    while (true) {
        auto record = cursor->next();
        if (record.ok()) {
            versionMeta.emplace_back(record.value());
        } else {
            if (record.status().code() != ErrorCodes::ERR_EXHAUST) {
                LOG(WARNING) << record.status().toString();
            }
            break;
        }
    }

    // no versionmeta just skip send versionmeta
    if (versionMeta.size() == 0) {
        return {ErrorCodes::ERR_OK, ""};
    }

    std::stringstream ss;
    Command::fmtMultiBulkLen(ss, versionMeta.size() * 3 + 1);
    Command::fmtBulk(ss, "migrateversionmeta");
    for (auto& meta : versionMeta) {
        Command::fmtBulk(ss, meta.getName());
        Command::fmtBulk(ss, std::to_string(meta.getTimeStamp()));
        Command::fmtBulk(ss, std::to_string(meta.getVersion()));
    }

    std::string stringtoWrite = ss.str();

    // LocalSessionGuard sg(_svr.get());
    // sg.getSession()->setArgs({stringtoWrite});

    Status s = _client->writeData(stringtoWrite);

    if (!s.ok()) {
        LOG(ERROR) << " writeData failed:" << s.toString() << ",data:" << ss.str();
        return s;
    }

    uint32_t secs = _cfg->timeoutSecBinlogWaitRsp;
    Expected<std::string> expOK = _client->readLine(std::chrono::seconds(secs));

    if (!expOK.ok()) {
        LOG(ERROR) << " src Store:" << _dstStoreid
                   << " readLine failed:" << expOK.status().toString()
                   << "; Size:" << stringtoWrite.size() << "; Seconds:" << secs;
        // maybe miss message in network
        return {ErrorCodes::ERR_CLUSTER, "missing package"};
    } else if (expOK.value() != "+OK") {
        LOG(ERROR) << "get response of migratesyncversion failed "
                   << "dstStoreid:" << _dstStoreid << " rsp:" << expOK.value();
        return {ErrorCodes::ERR_NETWORK, "bad return string"};
    }

    return {ErrorCodes::ERR_OK, ""};
}

Status ChunkMigrateSender::sendOver() {
    std::string binlogInfo = needToSendFail() ? "-ERR" : "+OK";
    std::stringstream ss;
    Command::fmtMultiBulkLen(ss, 3);
    Command::fmtBulk(ss, "migrateend");
    Command::fmtBulk(ss, _taskid);
    Command::fmtBulk(ss, binlogInfo);

    std::string stringtoWrite = ss.str();
    Status s = _client->writeData(stringtoWrite);
    if (!s.ok()) {
        serverLog(LL_NOTICE, "ChunkMigrateSender::sendOver"
            " in kvstore(%u) fail: [%s] [%s] [network error:%s]",
            _storeid,
            _client->getRemoteRepr().c_str(),
            bitsetStrEncode(_slots).c_str(),
            s.toString().c_str());
        return s;
    }

    // wait 3 sec to get the response
    uint32_t secs = 3;
    Expected<std::string> exptOK =
            _client->readLine(std::chrono::seconds(secs));
    if (!exptOK.ok()) {
        serverLog(LL_NOTICE, "ChunkMigrateSender::sendOver"
            " in kvstore(%u) fail: [%s] [%s] [bad response:%s]",
            _storeid,
            _client->getRemoteRepr().c_str(),
            bitsetStrEncode(_slots).c_str(),
            exptOK.status().toString().c_str());
        return exptOK.status();
    } else if (exptOK.value() != "+OK") {
        serverLog(LL_NOTICE, "ChunkMigrateSender::sendOver"
            " in kvstore(%u) fail: [%s] [%s] [bad response:%s]",
            _storeid,
            _client->getRemoteRepr().c_str(),
            bitsetStrEncode(_slots).c_str(),
            exptOK.value().c_str());
        return {ErrorCodes::ERR_NETWORK, "bad return string"};
    }

    serverLog(LL_VERBOSE, "ChunkMigrateSender::sendOver"
        " in kvstore(%u) success: [%s] [%s]",
        _storeid,
        _client->getRemoteRepr().c_str(),
        bitsetStrEncode(_slots).c_str());

    return {ErrorCodes::ERR_OK, ""};
}

Status ChunkMigrateSender::deleteChunkRange(
            uint32_t  chunkidStart, uint32_t chunkidEnd) {
    // LOG(INFO) << "deleteChunk begin on chunkid:" << chunkid;
    INVARIANT_D(chunkidStart <= chunkidEnd);
    INVARIANT_D(chunkidStart < CLUSTER_SLOTS);
    INVARIANT_D(chunkidEnd < CLUSTER_SLOTS);

    auto expdb = _svr->getSegmentMgr()->getDb(NULL, _storeid,
            mgl::LockMode::LOCK_IS);
    if (!expdb.ok()) {
        LOG(ERROR) << "getDb failed:" << _storeid;
        return expdb.status();
    }
    PStore kvstore = expdb.value().store;
    RecordKey rkStart(chunkidStart, 0, RecordType::RT_INVALID, "", "");
    RecordKey rkEnd(chunkidEnd + 1, 0, RecordType::RT_INVALID, "", "");
    string start = rkStart.prefixChunkid();
    string end = rkEnd.prefixChunkid();
    auto s = kvstore->deleteRange(start, end);

    if (!s.ok()) {
        LOG(ERROR) << "kvstore->deleteRange failed, chunkidStart:" << chunkidStart
            << " chunkidEnd:" << chunkidEnd << " err:" << s.toString();
        return s;
    }
    // NOTE(takenliu) after deleteRange, cursor seek will scan all the keys in delete range,
    //     so we call compactRange to real delete the keys.
    s = kvstore->compactRange(
        ColumnFamilyNumber::ColumnFamily_Default, &start, &end);
    if (!s.ok()) {
        LOG(ERROR) << "kvstore->compactRange failed, chunkidStart:" << chunkidStart
                   << " chunkidEnd:" << chunkidEnd << " err:" << s.toString();
        return s;
    }
    LOG(INFO) << "deleteChunk and compactRange end, storeid:" <<_storeid
        << " chunkidStart:" << chunkidStart << " chunkidEnd:" << chunkidEnd;
    return {ErrorCodes::ERR_OK, ""};
}


Status ChunkMigrateSender::deleteChunks(const std::bitset<CLUSTER_SLOTS>& slots) {
    /* NOTE(wayenchen) check if chunk not belong to me，
        make sure MOVE work well before delete */
    if (!checkSlotsBlongDst()){
        return  {ErrorCodes::ERR_CLUSTER,
                "slots not belongs to dstNodes"};
    }

    LOG(INFO) << "deleteChunks beigin slots: " << bitsetStrEncode(_slots);
    size_t idx = 0;
    uint32_t startChunkid = UINT32_MAX;
    uint32_t endChunkid = UINT32_MAX;
    while (idx < slots.size()) {
        if (_svr->getSegmentMgr()->getStoreid(idx) == _storeid) {
            if (slots.test(idx)) {
                if (startChunkid == UINT32_MAX) {
                    startChunkid = idx;
                }
                endChunkid = idx;
                _delSlot++;
            } else {
                if (startChunkid != UINT32_MAX) {
                    /* just throw it into gc job, no waiting for deleting result */
                    auto s = _svr->getGcMgr()->deleteChunks(_storeid, startChunkid, endChunkid);
                    if (!s.ok()) {
                        LOG(ERROR) << "deleteChunk fail, startChunkid:"
                                   << startChunkid
                                   << " endChunkid:" << endChunkid
                                   << " err:" << s.toString();
                        return s;
                    }
                    LOG(INFO) << "deleteChunk ok, startChunkid:" << startChunkid
                              << " endChunkid:" << endChunkid;
                    startChunkid = UINT32_MAX;
                    endChunkid = UINT32_MAX;
                }
            }
        }
        idx++;
    }
    if (startChunkid != UINT32_MAX) {
        auto s = _svr->getGcMgr()->deleteChunks(_storeid, startChunkid, endChunkid);
        if (!s.ok()) {
            LOG(ERROR) << "deleteChunk fail, startChunkid:"
                       << startChunkid
                       << " endChunkid:" << endChunkid
                       << " err:" << s.toString();
            return s;
        }
        LOG(INFO) << "deleteChunk ok, startChunkid:" << startChunkid
                  << " endChunkid:" << endChunkid;
    }

    _consistency = true;
    LOG(INFO) << "deleteChunks OK, isFake:" << _isFake
        << " snapshotKeyNum: " << _snapshotKeyNum
        << " binlogNum: " << _binlogNum
        << " , storeid: " << _storeid << " slots:" << bitsetStrEncode(_slots);

    return {ErrorCodes::ERR_OK, ""};
}

Status ChunkMigrateSender::lockChunks() {
    size_t chunkid = 0;
    Status s = { ErrorCodes::ERR_OK, "" };
    mstime_t locktime = msSinceEpoch();
    std::string remote;
    if (_client) {
        remote = _client->getRemoteRepr();
    }

    const auto guard = MakeGuard([&] {
        if (!s.ok()) {
            _slotsLockList.clear();
        }

        serverLog(LL_NOTICE, "ChunkMigrateSender::lockChunks "
            "%s: [%s] [%s] [used time:%lu]",
            s.ok() ? "success" : s.toString().c_str(),
            remote.c_str(),
            bitsetStrEncode(_slots).c_str(),
            msSinceEpoch() - locktime);
    });


    while (chunkid < _slots.size()) {
        if (_slots.test(chunkid)) {
            uint32_t storeId = _svr->getSegmentMgr()->getStoreid(chunkid);

            auto lock = ChunkLock::AquireChunkLock(storeId, chunkid,
                        mgl::LockMode::LOCK_X,
                        nullptr, _svr->getMGLockMgr(), 1000);
            if (!lock.ok()) {
                s = lock.status();
                return s;
            }

            mstime_t delay = msSinceEpoch() - locktime;
            if (delay > CLUSTER_MF_TIMEOUT / 2) {
                s = { ErrorCodes::ERR_TIMEOUT, "lock timeout" };
                return s;
            }

            _slotsLockList.push_back(std::move(lock.value()));
        }
        chunkid++;
    }
    return s;
}

void ChunkMigrateSender::unlockChunks() {
    std::lock_guard<myMutex> lk(_mutex);
    _slotsLockList.clear();
}

bool ChunkMigrateSender::needToWaitMetaChanged() const {
    return _sendstate == MigrateSenderStatus::LASTBINLOG_DONE ||
        _sendstate == MigrateSenderStatus::SENDOVER_DONE;
}


bool ChunkMigrateSender::needToSendFail() const {
    return _sendstate == MigrateSenderStatus::SNAPSHOT_DONE ||
        _sendstate == MigrateSenderStatus::BINLOG_DONE;
}

}  // namespace tendisplus
