#include "glog/logging.h"
#include "tendisplus/replication/repl_util.h"
#include "tendisplus/cluster/migrate_sender.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

ChunkMigrateSender::ChunkMigrateSender(uint32_t chunkid,
    std::shared_ptr<ServerEntry> svr,
    std::shared_ptr<ServerParams> cfg) :
    _svr(svr),
    _cfg(cfg),
    _chunkid(chunkid) {
}

Status ChunkMigrateSender::sendChunk() {
    Status s= sendSnapshot();
    if (!s.ok()) {
        return s;
    }
    s = sendBinlog();
    if (!s.ok()) {
        return s;
    }
    s = sendOver();
    if (!s.ok()) {
        return s;
    }
    _svr->updateChunkInfo(_chunkid, "not_mine");
    _svr->gossipBroadcast(_chunkid, _dstIp, _dstPort);
    _svr->unlockChunk(_chunkid, "X");
    return{ ErrorCodes::ERR_OK, "" };
}

std::string ChunkMigrateSender::encodePrefixPk(uint32_t chunkid) {
    std::string result;
    for (size_t i = 0; i < sizeof(chunkid); ++i) {
        result.push_back((chunkid>>((sizeof(chunkid)-i-1)*8))&0xff);
    }
    return result;
}

Status ChunkMigrateSender::sendSnapshot() {
    std::unique_ptr<Cursor> cursor;
    {
        //LocalSessionGuard sg(_svr);
        auto expdb = _svr->getSegmentMgr()->getDb(NULL, _storeid,
            mgl::LockMode::LOCK_IS); // lock IS
        if (!expdb.ok()) {
            return expdb.status();
        }
        _dbWithLock = std::make_unique<DbWithLock>(std::move(expdb.value()));

        auto kvstore = _dbWithLock->store;
        auto ptxn = kvstore->createTransaction(NULL);
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        // TODO(takenliu):truncatebinlog need contain binlogs after _curBinlogid.
        // _curBinlogid = kvstore->getHighestBinlogId(); // takenliutest,TODO
        _curBinlogid = 0;
        cursor = std::move(ptxn.value()->createCursor()); // it's snapshot cursor
    }
    LOG(INFO) << "sendSnapshot begin, chunkid:" << _chunkid << " storeid:" << _storeid;

    cursor->seek(encodePrefixPk(_chunkid));
    uint32_t totalWriteNum = 0;
    uint32_t curWriteNum = 0;
    uint32_t curWriteLen = 0;
    uint32_t timeoutSec = 100;
    Status s;
    // size_t sendBatch = (_cfg->binlogRateLimitMB * 1024 * 1024) / 10;
    // todo rate limit
    while (true) {
        Expected<Record> expRcd = cursor->next();
        if (expRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
            LOG(INFO) << "sendSnapshot Record is over, totalWriteNum:" << totalWriteNum
                << " chunkid:" << _chunkid << " storeid:" << _storeid;
            break;
        }
        if (!expRcd.ok()) {
            return expRcd.status();
        }
        Record &rcd = expRcd.value();
        const RecordKey &rcdKey = rcd.getRecordKey();
        /*LOG(INFO) << "takenliutest type:" << (int)rcdKey.getRecordType()
            << " pk:" << rcdKey.getPrimaryKey()
            << " chunkid:" << rcdKey.getChunkId() << " dbid:" << rcdKey.getDbId();*/

        if (rcdKey.getChunkId() != _chunkid) {
            LOG(INFO) << "sendSnapshot Record is over, totalWriteNum:" << totalWriteNum
                      << " chunkid:" << _chunkid << " storeid:" << _storeid;
            break;
        }
        std::string key = rcdKey.encode();
        const RecordValue& rcdValue = rcd.getRecordValue();
        std::string value = rcdValue.encode();

        SyncWriteData("0");
        //LOG(INFO) << "takenliutest write 0.";
        uint32_t keylen = key.size();
        SyncWriteData(string((char*)&keylen, sizeof(uint32_t)));
        //LOG(INFO) << "takenliutest write keylen:" << keylen;
        SyncWriteData(key);
        //LOG(INFO) << "takenliutest write key.";

        uint32_t valuelen = value.size();
        SyncWriteData(string((char*)&valuelen, sizeof(uint32_t)));
        SyncWriteData(value);

        curWriteNum++;
        totalWriteNum++;
        curWriteLen+= 1 + sizeof(uint32_t) + keylen + sizeof(uint32_t) + valuelen;

        if (curWriteNum >= 1000) {
            DLOG(INFO) << "ChunkMigrateSender::sendSnapshot chunk " << _chunkid
                << " send one batch,totalWriteNum:" << totalWriteNum;
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
    SyncWriteData("2"); // send over
    SyncReadData(exptData, 3, timeoutSec)
    if (exptData.value() != "+OK") {
        LOG(ERROR) << "read data is not +OK. totalWriteNum:" << totalWriteNum
                   << " curWriteNum:" << curWriteNum << " data:" << exptData.value();
        return { ErrorCodes::ERR_INTERNAL, "read +OK failed"};
    }
    LOG(INFO) << "sendSnapshot over, totalWriteNum:" << totalWriteNum
              << " chunkid:" << _chunkid << " storeid:" << _storeid;
    return { ErrorCodes::ERR_OK, ""};
}

Status ChunkMigrateSender::sendBinlog(){
    PStore kvstore = _dbWithLock->store;
    //_endBinlogid = kvstore->getHighestBinlogId(); // idempotent
    // takenliutest,TODO
    _endBinlogid = std::numeric_limits<uint64_t >::max();
    LOG(INFO) << "sendBinlog begin, chunkid:" << _chunkid << " storeid:" << _storeid
        << " beginBinlogid:" << _curBinlogid << " endBinlogid:" << _endBinlogid;
    bool beginCatchup = false;
    while (true) {
        bool needHeartbeat = false;
        // todo rate limit.
        // todo chunkid match only
        Expected<uint64_t> newPos =
            masterSendBinlogV2(_client.get(), _storeid, _dstStoreid,
                    _curBinlogid, needHeartbeat, _svr, _cfg, _chunkid);
        if (!newPos.ok()) {
            LOG(ERROR) << "ChunkMigrateSender::sendBinlog to client:"
                << _client->getRemoteRepr() << " failed:"
                << newPos.status().toString();
            return newPos.status();
        }

        if (newPos.value() >= _endBinlogid || newPos.value() == _curBinlogid) { // lock and catchup
            if (!beginCatchup) {
                _svr->lockChunk(_chunkid, "X");
                _endBinlogid = kvstore->getHighestBinlogId();
                beginCatchup = true;
            } else { // over
                LOG(INFO) << "ChunkMigrateSender::sendBinlog over, remote_addr "
                    << _client->getRemoteRepr() << ":" <<_client->getRemotePort()
                    << ",curbinlog:" << _curBinlogid
                    << " endbinlog:" << _endBinlogid
                    << " newbinlog:" << newPos.value();
                break;
            }
        }
        _curBinlogid = newPos.value();
    }
    return { ErrorCodes::ERR_OK, ""};
}

Status ChunkMigrateSender::sendOver() {
    LOG(INFO) << "sendOver begin, chunkid:" << _chunkid << " storeid:" << _storeid;
    std::stringstream ss2;
    Command::fmtMultiBulkLen(ss2, 3);
    Command::fmtBulk(ss2, "migrateend");
    Command::fmtBulk(ss2, std::to_string(_chunkid));
    Command::fmtBulk(ss2, std::to_string(_dstStoreid));

    std::string stringtoWrite = ss2.str();
    Status s = _client->writeData(stringtoWrite);
    if (!s.ok()) {
        LOG(ERROR) << " writeData failed:" << s.toString()
                     << ",data:" << ss2.str();
        return s;
    }

    uint32_t secs = _cfg->timeoutSecBinlogWaitRsp;
    Expected<std::string> exptOK = _client->readLine(std::chrono::seconds(secs));
    if (!exptOK.ok()) {
        LOG(ERROR) << "chunkid:" << _chunkid << " dst Store:" << _dstStoreid
                     << " readLine failed:" << exptOK.status().toString()
                     << "; Size:" << stringtoWrite.size()
                     << "; Seconds:" << secs;
        return exptOK.status();
    } else if (exptOK.value() != "+OK") { // TODO: two phase commit protocol
        LOG(ERROR) << "get response of migrateend failed, chunkid:"
            << _chunkid << " dstStoreid:" << _dstStoreid
            << " rsp:" << exptOK.value();
        return{ ErrorCodes::ERR_NETWORK, "bad return string" };
    }
    LOG(INFO) << "sendOver end, chunkid:" << _chunkid << " storeid:" << _storeid;
    return { ErrorCodes::ERR_OK, ""};
}

Expected<bool> ChunkMigrateSender::deleteChunk(uint32_t maxDeletenum) {
    std::unique_ptr<Cursor> cursor;
    PStore kvstore = _dbWithLock->store;
    auto ptxn = kvstore->createTransaction(NULL);
    if (!ptxn.ok()) {
        return ptxn.status();
    }
    cursor = std::move(ptxn.value()->createCursor());
 
    cursor->seek(encodePrefixPk(_chunkid));
    bool over = false;
    const uint32_t maxDeleteNum = 1000;
    uint32_t deleteNum = 0;
    while (true) {
        Expected<Record> expRcd = cursor->next();
        if (!expRcd.ok()) {
            return expRcd.status();
        }

        if (expRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
            over = true;
            LOG(INFO) << "deleteChunk over, chunkid:" << _chunkid
                      << " deleteNum:" << deleteNum;
            break;
        }
        Record &rcd = expRcd.value();
        const RecordKey &rcdKey = rcd.getRecordKey();
        if (rcdKey.getChunkId() != _chunkid) {
            over = true;
            LOG(INFO) << "deleteChunk over, chunkid:" << _chunkid
               << " deleteNum:" << deleteNum;
            break;
        }

        //LOG(INFO) << "takenliutest, deleteKey :" << rcdKey.getPrimaryKey();
        ptxn.value()->delKV(rcdKey.encode()); // need congurate binlog
        deleteNum++;
        if (deleteNum >= maxDeleteNum) {
            break;
        }
    }
    auto s = ptxn.value()->commit();
    if (!s.ok()) { // todo: retry_times
        return s.status();
    }
    DLOG(INFO) << "deleteChunk chunkid:" << _chunkid
        << " num:" << deleteNum << " is_over:" << over;
    return over;
}

} // end namespace
