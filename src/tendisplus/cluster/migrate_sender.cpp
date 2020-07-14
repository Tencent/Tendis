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
    Status s;

    const auto guard = MakeGuard([&] {
        s = unlockChunks();
        if (!s.ok()) {
            LOG(ERROR) << "unlock sltos fail" << s.toString();
        }
    });
    /* send Snapshot of bitmap data */
    s = sendSnapshot();
    if (!s.ok()) {
        return s;
    }
    _sendstate = MigrateSenderStatus::SNAPSHOT_DONE;
    /* send binlog of the task slots in iteration of 10(default),
     * make sure the diff offset of srcNode and DstNode is small enough*/
    s = sendBinlog();
    if (!s.ok()) {
        LOG(ERROR) << "catch up binlog fail on slots:" << bitsetStrEncode(_slots)
                   << s.toString();
        return  s;
    }
    _sendstate = MigrateSenderStatus::BINLOG_DONE;
    /* lock chunks to block the client for while */
    s = lockChunks();
    if (!s.ok()) {
        LOG(ERROR) << "lockChunks fail:" << s.toString();
        return s;
    }
    /* finish sending the diff offset of maxbinlog */
    s = finishLastBinlog();
    if (!s.ok()) {
        LOG(ERROR) << "send last binlog Fail:" << s.toString();
        return s;
    }
    /* send migrateend command and wait for return*/
    s = sendOver();
    if (!s.ok()) {
        LOG(ERROR) << "sendover fail:" << s.toString();
        if (s.code() == ErrorCodes::ERR_CLUSTER) {
            LOG(ERROR) << "sendover error cluster";
            return {ErrorCodes::ERR_CLUSTER, "send over fail on"};
        }
        return s;
    }
    /* check the meta data of source node */
    if (!checkSlotsBlongDst()) {
        s = _clusterState->setSlots(_dstNode, _slots);
        if (!s.ok()) {
            LOG(ERROR) << "set myself meta data fail on slots:" << s.toString();
            return {ErrorCodes::ERR_CLUSTER, "set slot dstnode fail"};
        }
    }
    /* unlock after receive package */
    s = unlockChunks();
    if (!s.ok()) {
        LOG(ERROR) << "unlock fail on slots:"+ _slots.to_string();
        return  {ErrorCodes::ERR_CLUSTER, "unlock fail on slots"};
    }
    _sendstate = MigrateSenderStatus::METACHANGE_DONE;
    LOG(INFO) <<"sendChunk end on store:" << _storeid
              << " slots:" << bitsetStrEncode(_slots);

    // NOTE(wayenchen) send end should be marked here
    // TODO(takenliu) if one of the follow steps failed, do what ???
    auto ret = addMigrateBinlog(MigrateBinlogType::SEND_END,
                                _slots.to_string(), _storeid, _svr.get(),
                                _dstNode->getNodeName());
    if (!ret.ok()) {
        LOG(ERROR) << "addMigrateBinlog failed:" << ret.status().toString()
                   << " slots:" << bitsetStrEncode(_slots);
        // return ret.status();  // TODO(takenliu) what should to do?
    }

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
    std::lock_guard<std::mutex> lk(_mutex);
    for (size_t id =0; id < _slots.size(); id++) {
        if (_slots.test(id)) {
            if (_clusterState->getNodeBySlot(id) != _dstNode) {
                LOG(WARNING) << "slot:" << id << "not belong to:" << _nodeid;
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
    // snapShot open
    // TODO(takenliu) add a gtest for SI-level
    ptxn.value()->SetSnapshot();
    return  ptxn;
}

Expected<uint64_t> ChunkMigrateSender::sendRange(Transaction* txn,
                                uint32_t begin, uint32_t end) {
    LOG(INFO) << "snapshot sendRange send begin, store:"
              << _storeid << " beginSlot:" << begin
              << " endSlot:" << end;
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
                LOG(INFO) << "snapshot sendRange Record is over, totalWriteNum:"
                          << totalWriteNum
                          << " storeid:" << _storeid;
            break;
        }

        if (!expRcd.ok()) {
            return expRcd.status();
        }
        Record &rcd = expRcd.value();
        const RecordKey &rcdKey = rcd.getRecordKey();

        std::string key = rcdKey.encode();
        const RecordValue& rcdValue = rcd.getRecordValue();
        std::string value = rcdValue.encode();

        SyncWriteData("0");

        uint32_t keylen = key.size();
        SyncWriteData(string(reinterpret_cast<char*>(&keylen), sizeof(uint32_t)));

        SyncWriteData(key);

        uint32_t valuelen = value.size();
        SyncWriteData(string(reinterpret_cast<char*>(&valuelen), sizeof(uint32_t)));
        SyncWriteData(value);

        curWriteNum++;
        totalWriteNum++;
        curWriteLen += 1 + sizeof(uint32_t) + keylen
                        + sizeof(uint32_t) + valuelen;

        if (curWriteNum >= 1000) {
            SyncWriteData("1");
            SyncReadData(exptData, 3, timeoutSec)
            if (exptData.value() != "+OK") {
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
    SyncReadData(exptData, 3, timeoutSec)

    if (exptData.value() != "+OK") {
        LOG(ERROR) << "read receiver data is not +OK on slot:" << begin;
        return { ErrorCodes::ERR_INTERNAL, "read +OK failed"};
    }
    LOG(INFO) << "snapshot sendRange end, storeid:" << _storeid
        << " beginSlot:" << begin
        << " endSlot:" << end
        << " totalKeynum:" << totalWriteNum;

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
        std::lock_guard<std::mutex> lk(_mutex);
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
    uint32_t timeoutSec = 160;
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
    SyncReadData(exptData, 3, timeoutSec)
    if (exptData.value() != "+OK") {
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
    return  {ErrorCodes::ERR_OK, "finish snapshot of bitmap"};
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

// catch up binlog from start to end
Expected<uint64_t> ChunkMigrateSender::catchupBinlog(uint64_t start,
                                    uint64_t end,
                                    const std::bitset<CLUSTER_SLOTS> &slots) {
    bool needHeartbeat = false;
    auto s = SendSlotsBinlog(_client.get(), _storeid, _dstStoreid,
            start, end , needHeartbeat, slots,  _svr, _cfg);
    if (!s.ok()) {
        LOG(ERROR) << "ChunkMigrateSender::sendBinlog fail to client"
                   << _client->getRemoteRepr()
                   << s.status().toString();
        return s.status();
    }
    return s.value();
}

// catch up for tryNum
Status ChunkMigrateSender::pursueBinLog(uint64_t *startBinLog) {
    uint32_t distance =  _svr->getParams()->migrateDistance;
    uint16_t iterNum = _svr->getParams()->migrateBinlogIter;
    bool finishCatchup = false;
    PStore kvstore = _dbWithLock->store;
    uint32_t  catchupTimes = 0;
    uint64_t  binlogHigh = kvstore->getHighestBinlogId();
    while (catchupTimes < iterNum) {
        auto expectNum = catchupBinlog(*startBinLog, binlogHigh , _slots);
        if (!expectNum.ok()) {
            return expectNum.status();
        }
        catchupTimes++;
        auto sendNum = expectNum.value();
        _binlogNum += sendNum;
        *startBinLog = binlogHigh;

        binlogHigh = kvstore->getHighestBinlogId();
        // judge if reach for distance
        auto diffOffset = binlogHigh - *startBinLog;
        if (diffOffset <= distance) {
            LOG(INFO) << "last distance:" << diffOffset << "on" << bitsetStrEncode(_slots);
            finishCatchup = true;
            break;
        }
        DLOG(INFO) << "pursueBinLog finish iteration time:" << catchupTimes
                  << "on slots:" << bitsetStrEncode(_slots)
                  << "from:" << *startBinLog  << "to" << binlogHigh;
    }
    if (!finishCatchup) {
        return  {ErrorCodes::ERR_INTERNAL, "pursue binlog not finish"};
    }
    return  {ErrorCodes::ERR_OK, "finish binlog iteration"};
}

Status ChunkMigrateSender::sendBinlog() {
    LOG(INFO) << "sendBinlog begin, storeid:" << _storeid
        << " dstip:" << _dstIp << " dstport:" << _dstPort
        << " slots:" << bitsetStrEncode(_slots);
    PStore kvstore = _dbWithLock->store;
    auto ptxn = kvstore->createTransaction(nullptr);
    if (!ptxn.ok()) {
        LOG(ERROR) << "send binlog create transaction fail:"
                   << "on slots:" << bitsetStrEncode(_slots);
    }

    auto highBinlogId = kvstore->getHighestBinlogId();
    auto lowBinLogId = _curBinlogid;
    // if no data come when migrating, no need to send end log
    if (lowBinLogId < highBinlogId) {
        auto s = pursueBinLog(&lowBinLogId);
        if (!s.ok()) {
            // TODO(wayenchen) delete dirty data on dstNode,
           LOG(ERROR) << "fail catch up slots:" << bitsetStrEncode(_slots);
           return s;
        }
    }
    {
        std::lock_guard<std::mutex> lk(_mutex);
        _curBinlogid = lowBinLogId;
    }

    return { ErrorCodes::ERR_OK, ""};
}

Status ChunkMigrateSender::finishLastBinlog() {
    PStore kvstore = _dbWithLock->store;
    auto ptxn = kvstore->createTransaction(nullptr);
    if (!ptxn.ok()) {
        LOG(ERROR) << "send binlog create transaction fail:"
                   << "on slots:" << bitsetStrEncode(_slots);
    }
    auto maxBinlogId = getMaxBinLog(ptxn.value().get());
    // last binlog send
    if (_curBinlogid < maxBinlogId) {
        auto sLogNum = catchupBinlog(_curBinlogid, maxBinlogId , _slots);
        if (!sLogNum.ok()) {
            LOG(ERROR) << "last catchup fail on task:"
                       << bitsetStrEncode(_slots)
                       << sLogNum.status().toString();
            auto s = unlockChunks();
            if (!s.ok()) {
                LOG(ERROR) << s.toString();
                return  s;
            }
            return sLogNum.status();
        }
        _binlogNum += sLogNum.value();
    }
    LOG(INFO) << "ChunkMigrateSender::sendBinlog over"
             << " dstip:" << _dstIp << " dstport:" << _dstPort
             << " slots:" << bitsetStrEncode(_slots)
             << " send binlog total num:" << _binlogNum;

    return  {ErrorCodes::ERR_OK, "finish sendbinlog"};
}

Status ChunkMigrateSender::sendOver() {
    std::stringstream ss2;
    Command::fmtMultiBulkLen(ss2, 3);
    Command::fmtBulk(ss2, "migrateend");
    Command::fmtBulk(ss2, _slots.to_string());
    Command::fmtBulk(ss2, std::to_string(_dstStoreid));

    std::string stringtoWrite = ss2.str();
    Status s = _client->writeData(stringtoWrite);
    if (!s.ok()) {
        LOG(ERROR) << " writeData failed:" << s.toString()
                   << ",data:" << ss2.str();
        return s;
    }

    // if check meta data change successfully
    if (checkSlotsBlongDst()) {
        return {ErrorCodes::ERR_OK, ""};
    }

    uint32_t secs = _cfg->timeoutSecBinlogWaitRsp;
    Expected<std::string> exptOK = _client->readLine(std::chrono::seconds(secs));

    if (!exptOK.ok()) {
        LOG(ERROR) << " dst Store:" << _dstStoreid
                   << " readLine failed:" << exptOK.status().toString()
                   << "; Size:" << stringtoWrite.size()
                   << "; Seconds:" << secs;
        // maybe miss message in network
        return {ErrorCodes::ERR_CLUSTER, "missing package"};

    } else if (exptOK.value() != "+OK") {  // TODO(takenliu): two phase commit protocol
        LOG(ERROR) << "get response of migrateend failed "
                   << "dstStoreid:" << _dstStoreid
                   << " rsp:" << exptOK.value();
        return {ErrorCodes::ERR_NETWORK, "bad return string"};
    }

    return { ErrorCodes::ERR_OK, ""};
}

Status ChunkMigrateSender::deleteChunkRange(uint32_t  chunkidStart, uint32_t chunkidEnd) {
    // LOG(INFO) << "deleteChunk begin on chunkid:" << chunkid;
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
        LOG(ERROR) << "deleteChunk commit failed, chunkidStart:" << chunkidStart
            << " chunkidEnd:" << chunkidEnd << " err:" << s.toString();
        return s;
    }
    LOG(INFO) << "deleteChunk end, chunkidStart:" << chunkidStart
              << " chunkidEnd:" << chunkidEnd;
    return {ErrorCodes::ERR_OK, ""};
}

Status ChunkMigrateSender::deleteChunks(const std::bitset<CLUSTER_SLOTS>& slots) {
    /* NOTE(wayenchen) check if chunk not belong to me,
     * make sure MOVE work well before delete */
    if (!_isFake && !checkSlotsBlongDst()) {
        return  {ErrorCodes::ERR_CLUSTER, "slots not belongs to dstNodes"};
    }
    lockChunks();

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
                    auto s = deleteChunkRange(startChunkid, endChunkid);
                    if (!s.ok()) {
                        LOG(ERROR) << "deleteChunk fail, startChunkid:" << startChunkid
                                   << " endChunkid:" << endChunkid << " err:" << s.toString();
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
        auto s = deleteChunkRange(startChunkid, endChunkid);
        if (!s.ok()) {
            LOG(ERROR) << "deleteChunk fail, startChunkid:" << startChunkid
                       << " endChunkid:" << endChunkid << " err:" << s.toString();
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

    unlockChunks();
    return {ErrorCodes::ERR_OK, ""};
}

Status ChunkMigrateSender::lockChunks() {
    // NOTE(takenliu) if need multi thread in the future, _slotsLockList need be protected by mutex.
    LOG(INFO) << "lockChunks begin, slots:" << bitsetStrEncode(_slots);
    size_t chunkid = 0;
    Status s;
    while (chunkid < _slots.size()) {
        if (_slots.test(chunkid)) {
            uint32_t storeId = _svr->getSegmentMgr()->getStoreid(chunkid);

            auto lock = ChunkLock::AquireChunkLock(storeId, chunkid,
                                                   mgl::LockMode::LOCK_X,
                                                   nullptr, _svr->getMGLockMgr());
            if (!lock.ok()) {
                LOG(ERROR) << "lock chunk fail on :" << chunkid;
                return  lock.status();
            }
            _slotsLockList.push_back(std::move(lock.value()));
            LOG(INFO) << "lockChunks one sucess, chunkid:" << chunkid << " storeid:" << storeId;
        }
        chunkid++;
    }
    LOG(INFO) << "lockChunks end, slots:" << bitsetStrEncode(_slots);
    return  {ErrorCodes::ERR_OK, ""};
}

Status ChunkMigrateSender::unlockChunks() {
    LOG(INFO) << "unlock slots:" << bitsetStrEncode(_slots);
    _slotsLockList.clear();
    /*size should be zero after clear*/
    if(_slotsLockList.size() > 0) {
        return  {ErrorCodes::ERR_INTERNAL, "unlock slots:"+bitsetStrEncode(_slots)+ "failed"};
    }
    return  {ErrorCodes::ERR_OK, ""};
}

}  // namespace tendisplus
