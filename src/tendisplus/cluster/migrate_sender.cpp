#include <utility>
#include "glog/logging.h"
#include "tendisplus/cluster/migrate_sender.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/replication/repl_util.h"
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

    _sendstate(MigrateSenderStatus::SNAPSHOT_BEGIN),
    _storeid(0),
    _snapshotKeyNum(0),
    _binlogNum(0),
    _delNum(0),
    _delSlot(0),
    _consistency(false),
    _nodeid(""),
    _curBinlogid(UINT64_MAX),
    _dstIp(""),
    _dstPort(0),
    _dstStoreid(0),
    _dstNode(nullptr) {
        _clusterState = _svr->getClusterMgr()->getClusterState();
}


Status ChunkMigrateSender::sendChunk() {
    LOG(INFO) <<"sendChunk begin on store:" << _storeid << " slots:" << bitsetStrEncode(_slots);
    Status s = sendSnapshot(_slots);
    if (!s.ok()) {
        return s;
    }
    // LOG(INFO) <<"sendSnapshot finish on store:"
    //     << _storeid << " slots:" << bitsetStrEncode(_slots);
    // define max time to catch up binlog
    _sendstate = MigrateSenderStatus::SNAPSHOT_DONE;

    uint16_t  maxTime = 10;

    s = sendBinlog(maxTime);
    if (!s.ok()) {
        LOG(ERROR) << "catch up binlog fail on storeid:" << _storeid;
        return  s;
    }
    _sendstate = MigrateSenderStatus::BINLOG_DONE;
    // LOG(INFO) << "send binlog finish on store:" << _storeid;
    s = sendOver();

    if (!s.ok()) {
        LOG(ERROR) << "sendover error";
        if (s.code() == ErrorCodes::ERR_CLUSTER) {
            LOG(ERROR) << "sendover error cluster";
            return {ErrorCodes::ERR_CLUSTER, "send over fail on"};
        }
        return s;
    }

    /* unlock after receive package */
    // TODO(wayenchen)  takenliu add, use ChunkMigrateSender::unlockChunks
    s = _svr->getMigrateManager()->unlockChunks(_slots);
    if (!s.ok()) {
        LOG(ERROR) << "unlock fail on slots:"+ _slots.to_string();
        return  {ErrorCodes::ERR_CLUSTER, "unlock fail on slots"};
    }
    // LOG(INFO) <<"sendChunk unlockChunks store:" << _storeid
    //    << " slots:" << bitsetStrEncode(_slots);

    _sendstate = MigrateSenderStatus::METACHANGE_DONE;
    LOG(INFO) <<"sendChunk end and unlockChunks on store:" << _storeid << " slots:" << bitsetStrEncode(_slots);

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
bool ChunkMigrateSender::checkSlotsBlongDst(const std::bitset<CLUSTER_SLOTS>& slots) {
    for (size_t id =0; id < slots.size(); id++) {
        if (slots.test(id)) {
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
    auto ptxn = kvstore->createTransaction(NULL);

    if (!ptxn.ok()) {
        return ptxn.status();
    }
    // snapShot open
    // TODO(takenliu) add a gtest for SI-level
    ptxn.value()->SetSnapshot();
    LOG(INFO) << "initTxn SetSnapshot";
    // TODO(wayenchen)  takenliu add, use std::unique_ptr
    return std::move(ptxn);
}

Expected<uint64_t> ChunkMigrateSender::sendRange(Transaction* txn,
                                uint32_t begin, uint32_t end) {
    LOG(INFO) << "snapshot sendRange begin, store:" << _storeid << " beginSlot:" << begin
              << " endSlot:" << end;
    auto cursor = std::move(txn->createSlotsCursor(begin, end));
    uint32_t totalWriteNum = 0;
    uint32_t curWriteNum = 0;
    uint32_t curWriteLen = 0;
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
        curWriteLen+= 1 + sizeof(uint32_t) + keylen + sizeof(uint32_t) + valuelen;

        if (curWriteNum >= 1000) {
            SyncWriteData("1");
            SyncReadData(exptData, 3, timeoutSec)
            if (exptData.value() != "+OK") {
                LOG(ERROR) << "read data is not +OK. totalWriteNum:" << totalWriteNum
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
Status ChunkMigrateSender::sendSnapshot(const SlotsBitmap& slots) {
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
              <<" slots:" << bitsetStrEncode(slots);
    uint32_t startTime = sinceEpoch();
    auto eTxn = initTxn();
    if (!eTxn.ok()) {
        return eTxn.status();
    }
    Transaction * txn = eTxn.value().get();
    uint32_t timeoutSec = 160;
    uint32_t sendSlotNum = 0;
    for (size_t  i = 0; i < CLUSTER_SLOTS; i++) {
        if (slots.test(i)) {
            sendSlotNum++;
            auto ret = sendRange(txn, i , i+1);
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
              <<" sendSlotNum:" << sendSlotNum
              <<" totalWriteNum:" << _snapshotKeyNum
              <<" useTime:" << endTime - startTime
              <<" slots:" << bitsetStrEncode(slots);
    return  {ErrorCodes::ERR_OK, "finish snapshot of bitmap"};
}


uint64_t ChunkMigrateSender::getMaxBinLog(Transaction* ptxn) {
    uint64_t maxBinlogId;
    auto expBinlogidMax = RepllogCursorV2::getMaxBinlogId(ptxn);
    if (expBinlogidMax.status().code() == ErrorCodes::ERR_EXHAUST) {
        maxBinlogId = 0;
    } else {
        maxBinlogId = expBinlogidMax.value();
    }
    return  maxBinlogId;
}

// catch up binlog from start to end
Expected<uint64_t> ChunkMigrateSender::catchupBinlog(uint64_t start, uint64_t end,
                                            const std::bitset<CLUSTER_SLOTS> &slots) {
    bool needHeartbeat = false;
    auto s =  SendSlotsBinlog(_client.get(), _storeid, _dstStoreid,
            start, end , needHeartbeat, slots,  _svr, _cfg);
    if (!s.ok()) {
        LOG(ERROR) << "ChunkMigrateSender::sendBinlog to client:"
                   << _client->getRemoteRepr() << " failed:"
                   << s.status().toString();
    }
    return  s.value();
}

// catch up for maxTime
bool ChunkMigrateSender::pursueBinLog(uint16_t maxTime , uint64_t &startBinLog,
        uint64_t &binlogHigh, Transaction *txn) {
    uint32_t  distance =  _svr->getParams()->migrateDistance;
    uint64_t maxBinlogId = 0;
    bool finishCatchup = false;
    PStore kvstore = _dbWithLock->store;
    uint32_t  catchupTimes = 0;
    while (catchupTimes < maxTime) {
        auto expectNum = catchupBinlog(startBinLog, binlogHigh , _slots);
        if (!expectNum.ok()) {
            return false;
        }
        auto sendNum = expectNum.value();
        _binlogNum += sendNum;

        catchupTimes++;
        LOG(INFO) << "pursueBinLog " << catchupTimes << " times finish from:"
                << startBinLog <<" to:" <<binlogHigh
                << " storeid:" << _storeid << " slots:" << bitsetStrEncode(_slots);
        {
            // TODO(wayenchen)  takenliu add, binlogHigh maybe not send over
            std::lock_guard<std::mutex> lk(_mutex);
            startBinLog = binlogHigh;
            binlogHigh = kvstore->getHighestBinlogId();
        }
        maxBinlogId = getMaxBinLog(txn);
        auto diffOffset = maxBinlogId - startBinLog;

        //  reach for distance
        if (diffOffset < distance) {
            LOG(INFO) << "pursueBinLog lag is small enough, distance:" << diffOffset
                << " startBinLog:" << startBinLog << " maxBinlogId:" << maxBinlogId
                << " storeid:" << _storeid;
            finishCatchup = true;
            break;
        }
    }
    return  finishCatchup;
}


Status ChunkMigrateSender::sendBinlog(uint16_t maxTime) {
    LOG(INFO) << "sendBinlog begin, storeid:" << _storeid
        << " dstip:" << _dstIp << " dstport:" << _dstPort
        << " slots:" << bitsetStrEncode(_slots);
    PStore kvstore = _dbWithLock->store;

    uint32_t startTime = sinceEpoch();
    auto ptxn = kvstore->createTransaction(NULL);
    auto heighBinlog = kvstore->getHighestBinlogId();

    // if no data come when migrating, no need to send end log
    if (_curBinlogid < heighBinlog) {
        // TODO(wayenchen)  takenliu add, use _curBinlogid as reference is not clear
        bool catchUp = pursueBinLog(maxTime, _curBinlogid ,
                                heighBinlog, ptxn.value().get());
        if (!catchUp) {
            // delete dirty data on dstNode, to do wayen
           return {ErrorCodes::ERR_TIMEOUT, "catch up fail"};
        }
    }
    // TODO(wayenchen)  takenliu add, lockChunks shouldn't be processed by MigrateManager,
    //    only processed in ChunkMigrateSender use ChunkMigrateSender::lockChunks
    LOG(INFO) << "sendBinlog lockChunks begin, storeid:" << _storeid
        << " slots:" << bitsetStrEncode(_slots);
    Status s = _svr->getMigrateManager()->lockChunks(_slots);
    if (!s.ok()) {
        return {ErrorCodes::ERR_CLUSTER, "fail lock slots"};
    }
    LOG(INFO) << "sendBinlog lockChunks sucess, storeid:" << _storeid
        << " slots:" << bitsetStrEncode(_slots);

    // LOCKs need time, so need recompute max binlog
    heighBinlog = getMaxBinLog(ptxn.value().get());
    // last binlog send
    if (_curBinlogid <  heighBinlog) {
        LOG(INFO) << "sendBinlog last catchupBinlog on store:" << _storeid << " _curBinglogid:"
                   << _curBinlogid << " heighBinglog:" << heighBinlog
                   << " slots:" << bitsetStrEncode(_slots);
        auto sLogNum = catchupBinlog(_curBinlogid, heighBinlog , _slots);
        if (!sLogNum.ok()) {
            LOG(ERROR) << "last catchup fail on store:" << _storeid;
            s = _svr->getMigrateManager()->unlockChunks(_slots);
            if (!s.ok()) {
                LOG(ERROR) << "unlock fail on slots in sendBinlog";
            }
            return {ErrorCodes::ERR_NETWORK, "send last binglog fail"};
        }
        _binlogNum += sLogNum.value();
    }
    uint32_t endTime = sinceEpoch();

    LOG(INFO) << "ChunkMigrateSender::sendBinlog over, remote_addr "
              << _client->getRemoteRepr() << ":" <<_client->getRemotePort()
              << " curbinlog:" << _curBinlogid << " endbinlog:" << heighBinlog
              << " send binlog total num:" << _binlogNum
              << " useTime:" << endTime - startTime
              << " storeid:" << _storeid << " slots:" << bitsetStrEncode(_slots);

    return { ErrorCodes::ERR_OK, ""};
}


Status ChunkMigrateSender::sendOver() {
    // TODO(takenliu) if one of the follow steps failed, do what ???

    auto ret = addMigrateBinlog(MigrateBinlogType::SEND_END,
        _slots.to_string(), _storeid, _svr.get(),
        _dstNode->getNodeName());
    if (!ret.ok()) {
        LOG(ERROR) << "addMigrateBinlog failed:" << ret.status().toString()
                   << " slots:" << bitsetStrEncode(_slots);
        // return ret.status();  // TODO(takenliu) what should to do?
    }

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
    if (checkSlotsBlongDst(_slots)) {
        return { ErrorCodes::ERR_OK, ""};
    }

    uint32_t secs = _cfg->timeoutSecBinlogWaitRsp;
    Expected<std::string> exptOK = _client->readLine(std::chrono::seconds(secs));

    if (!exptOK.ok()) {
        LOG(ERROR) <<  " dst Store:" << _dstStoreid
                     << " readLine failed:" << exptOK.status().toString()
                     << "; Size:" << stringtoWrite.size()
                     << "; Seconds:" << secs;
        // maybe miss message in network
        return { ErrorCodes::ERR_CLUSTER, "missing package" };

    } else if (exptOK.value() != "+OK") {  // TODO(takenliu): two phase commit protocol
        LOG(ERROR) << "get response of migrateend failed "
            << "dstStoreid:" << _dstStoreid
            << " rsp:" << exptOK.value();
        return{ ErrorCodes::ERR_NETWORK, "bad return string" };
    }

    // set  meta data of source node
    s = _clusterState->setSlots(_dstNode, _slots);
    if (!s.ok()) {
        LOG(ERROR) << "set myself meta data fail on slots";
        return { ErrorCodes::ERR_CLUSTER, "set slot dstnode fail" };
    }

    return { ErrorCodes::ERR_OK, ""};
}


Expected<uint64_t> ChunkMigrateSender::deleteChunk(uint32_t  chunkid) {
    // LOG(INFO) << "deleteChunk begin on chunkid:" << chunkid;
    // NOTE(takenliu) for fake task, maybe only call deleteChunk(), so _dbWithLock maybe null.
    if (_isFake) {
        auto expdb = _svr->getSegmentMgr()->getDb(NULL, _storeid,
                                                  mgl::LockMode::LOCK_IS);
        if (!expdb.ok()) {
            LOG(ERROR) << "getDb failed:" << _storeid;
            return expdb.status();
        }
        _dbWithLock = std::make_unique<DbWithLock>(std::move(expdb.value()));
    }
    PStore kvstore = _dbWithLock->store;
    auto ptxn = kvstore->createTransaction(NULL);
    if (!ptxn.ok()) {
        return ptxn.status();
    }
    auto cursor = std::move(ptxn.value()->createSlotsCursor(chunkid, chunkid+1));
    if (cursor == nullptr) {
        LOG(ERROR) << "createSlotsCursor failed";
        return {ErrorCodes::ERR_INTERGER, "createSlotsCursor failed"};
    }
    bool over = false;
    uint32_t deleteNum = 0;

    while (true) {
        Expected<Record> expRcd = cursor->next();
        if (expRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
            over = true;
            break;
        }
        if (!expRcd.ok()) {
            LOG(ERROR) << "delete cursor error on chunkid:" << chunkid;
            return false;
        }

        Record &rcd = expRcd.value();
        const RecordKey &rcdKey = rcd.getRecordKey();
        auto s = ptxn.value()->delKV(rcdKey.encode());
        if (!s.ok()) {
            LOG(ERROR) << "delete key fail";
            continue;
        }
        deleteNum++;
    }
    auto s = ptxn.value()->commit();
    // todo: retry_times
    if (!s.ok()) {
        return s.status();
    }
    LOG(INFO) << "deleteChunk chunkid:" << chunkid
        << " num:" << deleteNum << " is_over:" << over;
    return deleteNum;
}

bool ChunkMigrateSender::deleteChunks(const std::bitset<CLUSTER_SLOTS>& slots) {
    lockChunks();

    LOG(INFO) << "deleteChunks beigin slots: " << bitsetStrEncode(_slots);
    size_t idx = 0;
    while (idx < slots.size()) {
        if (slots.test(idx)) {
            auto v = deleteChunk(idx);
            if (!v.ok()) {
                LOG(ERROR) << "delete slot:" << idx << "fail";
                return false;
            }
            _delNum += v.value();
            _delSlot++;
        }
        idx++;
    }
    LOG(INFO) << "finish del key num: " << _delNum << " del slots num: " << _delSlot;
    if (!_isFake && _delNum != _snapshotKeyNum + _binlogNum) {
        LOG(ERROR) << "deleteChunks delNum: " << _delNum
                   << "is not equal to (snaphotKey+binlog) "
                   << "snapshotKey num: " << _snapshotKeyNum
                   << " binlog num: " << _binlogNum
                   << " storeid: " << _storeid << " slots:" << bitsetStrEncode(_slots);
    } else {
        _consistency = true;
        LOG(INFO) << "deleteChunks OK, isFake:" << _isFake
            << " delNum: " << _delNum
            << " snapshotKeyNum: " << _snapshotKeyNum
            << " binlogNum: " << _binlogNum
            << " , storeid: " << _storeid << " slots:" << bitsetStrEncode(_slots);
    }

    unlockChunks();
    return true;
}

Status ChunkMigrateSender::lockChunks() {
    // NOTE(takenliu) if need multi thread in the future, _slotsLockList need be protected by mutex.
    LOG(INFO) << "lockChunks begin, slots:" << bitsetStrEncode(_slots);
    size_t chunkid = 0;
    Status s;
    while (chunkid < _slots.size()) {
        if (_slots.test(chunkid)) {
            uint32_t storeId = _svr->getSegmentMgr()->getStoreid(chunkid);

            auto lock = std::make_unique<ChunkLock>
                    (storeId, chunkid, mgl::LockMode::LOCK_X, nullptr, _svr->getMGLockMgr());

            _slotsLockList.push_back(std::move(lock));

            //LOG(INFO) << "lockChunks one sucess, chunkid:" << chunkid << " storeid:" << storeId;
        }
        chunkid++;
    }
    LOG(INFO) << "lockChunks end, slots:" << bitsetStrEncode(_slots);
    return  {ErrorCodes::ERR_OK, ""};
}

Status ChunkMigrateSender::unlockChunks() {
    LOG(INFO) << "unlockChunks slots:" << bitsetStrEncode(_slots);
    _slotsLockList.clear();
    return  {ErrorCodes::ERR_OK, ""};
}

} // end namespace
