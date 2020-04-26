#include "glog/logging.h"
#include "tendisplus/replication/repl_util.h"
#include "tendisplus/cluster/migrate_sender.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/utils/invariant.h"
namespace tendisplus {

ChunkMigrateSender::ChunkMigrateSender(const std::bitset<CLUSTER_SLOTS>& slots,
    std::shared_ptr<ServerEntry> svr,
    std::shared_ptr<ServerParams> cfg) :
    _slots(slots),
    _svr(svr),
    _cfg(cfg),
    _curBinlogid(UINT64_MAX) {
        size_t idx = 0;
        while (idx < slots.size() && !slots.test(idx)) {
            ++idx;
        }
        _start  = idx;
        idx  = slots.size()-1;
        while (idx > 0 && !slots.test(idx)) {
            --idx;
        }
        _end  = idx;
        _clusterState = _svr->getClusterMgr()->getClusterState();
}


Status ChunkMigrateSender::sendChunk() {
    Status s = sendSnapshot(_slots);
   // Status s = sendSnapshot(_start, _end+1);  // work on continous slots
    if (!s.ok()) {
        return s;
    }
    // define max time to catch up binlog
    uint16_t  maxTime = 10;
    s = sendBinlog(maxTime);
    if (!s.ok()) {
        LOG(ERROR) << "catch up binlog fail on task:" << _start;
        return  s;
    }

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
    s = _svr->getMigrateManager()->unlockChunks(_slots);
    if (!s.ok()) {
        LOG(ERROR) << "unlock fail on slots:"+ _slots.to_string();
        return  {ErrorCodes::ERR_CLUSTER, "unlock fail on slots"};
    }

    return{ ErrorCodes::ERR_OK, "" };
}



void ChunkMigrateSender::setDstNode(const std::string nodeid) {
    _nodeid = nodeid;
    _dstNode = _clusterState->clusterLookupNode(_nodeid);
}


// check if bitmap all belong to dst node
bool ChunkMigrateSender::checkSlotsBlongDst(const std::bitset<CLUSTER_SLOTS>& slots) {
//    auto state = _svr->getClusterMgr()->getClusterState();
//    CNodePtr receiverNode = state->clusterLookupNode(_nodeid);

    for (size_t id =0; id < slots.size(); id++) {
        if (slots.test(id)) {
            if (_clusterState->getNodeBySlot(id) != _dstNode) {
                LOG(ERROR) << "slot:" << id << "not belong to:" << _nodeid;
                return false;
             }
        }
    }
    return true;
}


Expected<Transaction*> ChunkMigrateSender:: initTxn() {
    auto expdb = _svr->getSegmentMgr()->getDb(NULL, _storeid,
                                              mgl::LockMode::LOCK_IS);
    if (!expdb.ok()) {
        return expdb.status();
    }

    _dbWithLock = std::make_unique<DbWithLock>(std::move(expdb.value()));
    auto kvstore = _dbWithLock->store;

    auto ptxn = kvstore->createTransaction(NULL);

    if (!ptxn.ok()) {
        return ptxn.status();
    }
    _curBinlogid = kvstore->getHighestBinlogId();

    Transaction* txn =  ptxn.value().release();
    return  txn;
}

Status ChunkMigrateSender::sendRange(Transaction* txn,
                                uint32_t begin, uint32_t end) {
    LOG(INFO) << "snapshot sendRange begin, beginSlot:" << begin
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
                LOG(INFO) << "sendSnapshot Record is over, totalWriteNum:" << totalWriteNum
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
        SyncWriteData(string((char*)&keylen, sizeof(uint32_t)));

        SyncWriteData(key);

        uint32_t valuelen = value.size();
        SyncWriteData(string((char*)&valuelen, sizeof(uint32_t)));
        SyncWriteData(value);

        curWriteNum++;
        totalWriteNum++;
        // LOG(INFO) << "sendSnapshot Record is running, totalWriteNum:" << totalWriteNum << "slot:" << begin;
        curWriteLen+= 1 + sizeof(uint32_t) + keylen + sizeof(uint32_t) + valuelen;

        if (curWriteNum >= 1000) {
            SyncWriteData("1");
            SyncReadData(exptData, 3, timeoutSec)
            if (exptData.value() != "+OK") {
                LOG(ERROR) << "read data is not +OK. totalWriteNum:" << totalWriteNum
                           << " curWriteNum:" << curWriteNum << " data:" << exptData.value();
                return { ErrorCodes::ERR_INTERNAL, "read +OK failed"};
            }
            curWriteNum = 0;
            curWriteLen = 0;
        }
    }
    LOG(INFO) << "snapshot sendRange end, beginSlot:" << begin
        << " endSlot:" << end
        << " totalKeynum:" << totalWriteNum;

    return { ErrorCodes::ERR_OK, ""};
}
// send snapshot from begin to end
// more effective than bitmap  when slots is continuous
Status ChunkMigrateSender::sendSnapshot(uint32_t begin, uint32_t end) {
    auto eTxn = initTxn();
    if (!eTxn.ok()) {
        return eTxn.status();
    }
    Transaction * txn = eTxn.value();
    auto s = sendRange(txn, begin, end);

    if (!s.ok()) {
        LOG(ERROR) << "fail send snapshot from:" << begin << " to:"<< end;
        return  {ErrorCodes::ERR_NETWORK, "fail send snapshot"};
    }

    uint32_t timeoutSec = 100;
    SyncWriteData("2"); //  send over
    SyncReadData(exptData, 3, timeoutSec)
    if (exptData.value() != "+OK") {
        LOG(ERROR) << "read receiver data is not +OK. totalWriteNum:" << " data:" << exptData.value();
        return { ErrorCodes::ERR_INTERNAL, "read +OK failed"};
    }

    return  {ErrorCodes::ERR_OK, "finish snapshot"};
}

//deal with slots that is not continuous
Status ChunkMigrateSender::sendSnapshot(const SlotsBitmap& slots) {
    Status s;
    auto eTxn = initTxn();
    if (!eTxn.ok()) {
        return eTxn.status();
    }
    Transaction * txn = eTxn.value();
    uint32_t timeoutSec = 100;

    uint32_t sendSlotNum = 0;
    for (size_t  i = 0; i < CLUSTER_SLOTS; i++) {
        if (slots.test(i)) {
            sendSlotNum++;
            s = sendRange(txn, i , i+1);
            if (!s.ok()) {
                LOG(ERROR) << "fail send snapshot of bitmap";
                return  {ErrorCodes::ERR_NETWORK, "fail send snapshot"};
            }
        }
    }
    SyncWriteData("2"); // send over
    SyncReadData(exptData, 3, timeoutSec)
    if (exptData.value() != "+OK") {
        LOG(ERROR) << "read receiver data is not +OK. totalWriteNum:" << " data:" << exptData.value();
        return { ErrorCodes::ERR_INTERNAL, "read +OK failed"};
    }
    LOG(INFO) << "sendSnapshot finished, sendSlotNum:" << sendSlotNum;
    return  {ErrorCodes::ERR_OK, "finish snapshot of bitmap"};
}


uint64_t ChunkMigrateSender::getMaxBinLog(Transaction* ptxn) {
    uint64_t maxBinlogId ;
    auto expBinlogidMax = RepllogCursorV2::getMaxBinlogId(ptxn);
    if (expBinlogidMax.status().code() == ErrorCodes::ERR_EXHAUST) {
        maxBinlogId = 0;
    } else {
        maxBinlogId = expBinlogidMax.value();
    }
    return  maxBinlogId;
}

// catch up binlog from start to end
Status ChunkMigrateSender::catchupBinlog(uint64_t start, uint64_t end, const std::bitset<CLUSTER_SLOTS> &slots) {
    bool needHeartbeat = false;
    auto s =  SendSlotsBinlog(_client.get(), _storeid, _dstStoreid,
            start, end , needHeartbeat, slots,  _svr, _cfg);
    if (!s.ok()) {
        LOG(ERROR) << "ChunkMigrateSender::sendBinlog to client:"
                   << _client->getRemoteRepr() << " failed:"
                   << s.toString();
    }
    return  s;
}

// catch up for maxTime
bool ChunkMigrateSender::pursueBinLog(uint16_t  maxTime , uint64_t  &startBinLog ,
                                        uint64_t &binlogHigh, Transaction *txn) {
    uint32_t  distance =  _svr->getParams()->migrateDistance;
    uint64_t maxBinlogId = 0;
    bool finishCatchup = true;
    PStore kvstore = _dbWithLock->store;
    uint32_t  catchupTimes = 0;
    while (catchupTimes < maxTime) {
        auto expectLog = catchupBinlog(startBinLog, binlogHigh , _slots);
        if (!expectLog.ok()) {
            return false;
        }
        LOG(INFO) << "catchupBinlog time:" << catchupTimes << " finish, binlogid from:" << startBinLog << " to:" << binlogHigh;
        catchupTimes++;
        startBinLog = binlogHigh;
        binlogHigh = kvstore->getHighestBinlogId();

        maxBinlogId = getMaxBinLog(txn);
        auto diffOffset = maxBinlogId - startBinLog;
        //reach for distance
        if (diffOffset < distance) {
            _endBinlogid = maxBinlogId;
            finishCatchup = true;
            break;
        }
    }
    return  finishCatchup;
}


Status ChunkMigrateSender::sendBinlog(uint16_t maxTime) {
    LOG(INFO) << "sendBinlog begin, storeid:" << _storeid
        << " dstip:" << _dstIp << " dstport:" << _dstPort;
    PStore kvstore = _dbWithLock->store;

    auto ptxn = kvstore->createTransaction(NULL);
    auto heighBinlog = kvstore->getHighestBinlogId();

    // TODO(wayenchen) user one cursor to catchup.
    /*
    // if no data come when migrating, no need to send end log
    if (_curBinlogid < heighBinlog) {
        LOG(INFO) << "pursueBinLog begin, _curBinlogid:" << _curBinlogid
            << " heighBinlog:" << heighBinlog;
        bool catchUp = pursueBinLog(maxTime, _curBinlogid , heighBinlog, ptxn.value().get());
        if (!catchUp) {
           return {ErrorCodes::ERR_TIMEOUT, "catch up fail"};
        }
    }
    */
    Status s = _svr->getMigrateManager()->lockChunks(_slots);
    if (!s.ok()) {
        return {ErrorCodes::ERR_CLUSTER, "fail lock slots"};
    }

    // LOCKs need time, so need recompute max binlog
    heighBinlog = getMaxBinLog(ptxn.value().get());
    LOG(INFO) << "sendBinlog lockChunks and catchupBinlog, _curBinlogid:" << _curBinlogid
              << " heighBinlog:" << heighBinlog;
    // last binlog send
    if (_curBinlogid <  heighBinlog) {
        auto sLog = catchupBinlog(_curBinlogid, heighBinlog , _slots);
        if (!sLog.ok()) {
            return {ErrorCodes::ERR_NETWORK, "send last binglog fail"};
        }
    }

    LOG(INFO) << "ChunkMigrateSender::sendBinlog over, remote_addr "
              << _client->getRemoteRepr() << ":" <<_client->getRemotePort()
              << " curbinlog:" << _curBinlogid << " endbinlog:" << heighBinlog;

    return { ErrorCodes::ERR_OK, ""};
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

    //if check meta data change successfully (gossip msessgae faster than command return message)
    if (checkSlotsBlongDst(_slots)) {
        LOG(INFO) << "check meta data change successfully , just return";
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

    } else if (exptOK.value() != "+OK") { // TODO: two phase commit protocol
        LOG(ERROR) << "get response of migrateend failed "
            << "dstStoreid:" << _dstStoreid
            << " rsp:" << exptOK.value();
        return{ ErrorCodes::ERR_NETWORK, "bad return string" };
    }


 //   auto state = _svr->getClusterMgr()->getClusterState();
 //   CNodePtr receiverNode = state->clusterLookupNode(_nodeid);

    // set  meta data of source node
    s = _clusterState->setSlots(_dstNode, _slots);
    if (!s.ok()) {
        LOG(ERROR) << "set myself meta data fail on slots";
        return { ErrorCodes::ERR_CLUSTER, "set slot dstnode fail" };
    }

    LOG(INFO) << "sendOver end ，storeid:" << _storeid;

    return { ErrorCodes::ERR_OK, ""};
}


Expected<bool> ChunkMigrateSender::deleteChunk(uint32_t  chunkid) {
    PStore kvstore = _dbWithLock->store;
    auto ptxn = kvstore->createTransaction(NULL);
    if (!ptxn.ok()) {
        return ptxn.status();
    }
    auto cursor = std::move(ptxn.value()->createSlotsCursor(chunkid, chunkid+1));
    bool over = false;
    uint32_t deleteNum = 0;

    while (true) {
        Expected<Record> expRcd = cursor->next();
        if (expRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
            over = true;
            break;
        }

        if (!expRcd.ok()) {
            return expRcd.status();
        }

        Record &rcd = expRcd.value();
        const RecordKey &rcdKey = rcd.getRecordKey();
        ptxn.value()->delKV(rcdKey.encode());
        deleteNum++;
    }

    auto s = ptxn.value()->commit();
    // todo: retry_times
    if (!s.ok()) {
        return s.status();
    }
    LOG(INFO) << "deleteChunk chunkid:" << chunkid
        << " num:" << deleteNum << " is_over:" << over;
    return over;
}

bool ChunkMigrateSender::deleteChunks(const std::bitset<CLUSTER_SLOTS>& slots) {
    size_t idx = 0;
    while (idx < slots.size()) {
        if (slots.test(idx)) {
            auto v = deleteChunk(idx);
            if (!v.value()) {
                LOG(ERROR) << "delete slot:" << idx << "fail";
                return false;
            }
        }
        idx++;
    }
    return true;
}


} // end namespace
