#include "glog/logging.h"
#include "tendisplus/cluster/migrate_receiver.h"
#include "tendisplus/replication/repl_util.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

ChunkMigrateReceiver::ChunkMigrateReceiver(
    const std::bitset<CLUSTER_SLOTS>& slots,
    uint32_t storeid,
    std::shared_ptr<ServerEntry> svr,
    std::shared_ptr<ServerParams> cfg) :
    _svr(svr),
    _cfg(cfg),
    _storeid(storeid),
    _slots(slots) {
}

Status ChunkMigrateReceiver::receiveSnapshot() {
    std::stringstream ss;
    const std::string nodename = _svr->getClusterMgr()->getClusterState()->getMyselfName();
    std::string bitmapStr = _slots.to_string();
    ss << "readymigrate " << bitmapStr << " " << _storeid << " " << nodename;
    Status s = _client->writeLine(ss.str());
    if (!s.ok()) {
        LOG(ERROR) << "readymigrate srcDb failed:" << s.toString();
        return s;
    }

    auto expRsp = _client->readLine(std::chrono::seconds(50));
    if (!expRsp.ok()) {
        LOG(ERROR) << "readymigrate req srcDb error:"
                     << expRsp.status().toString();
        return expRsp.status();
    }
    std::string value = expRsp.value();
    if (expRsp.value() != "+OK") {
        LOG(WARNING) << "readymigrate req srcDb failed:" << expRsp.value();
        return {ErrorCodes::ERR_INTERNAL, "readymigrate req srcDb failed"};
    }

    LOG(INFO) << "receiveSnapshot, get response of readymigrate ok";

    bool over = false;
    uint32_t timeoutSec = 60;
    uint32_t readNum = 0;
    while (true) {
        SyncReadData(exptData, 1, timeoutSec)
        if (exptData.value()[0] == '0') {
            SyncReadData(keylenData, 4, timeoutSec)
            uint32_t keylen = *(uint32_t*)keylenData.value().c_str();
            SyncReadData(keyData, keylen, timeoutSec)

            SyncReadData(valuelenData, 4, timeoutSec)
            uint32_t valuelen = *(uint32_t*)valuelenData.value().c_str();
            SyncReadData(valueData, valuelen, timeoutSec)

            supplySetKV(keyData.value(), valueData.value());
            readNum++;
        } else if (exptData.value()[0] == '1') {
            SyncWriteData("+OK")
        } else if (exptData.value()[0] == '2') {
            over = true;
            SyncWriteData("+OK")
            break;
        }
    }
    LOG(INFO) << "migrate snapshot  transfer done,readnum:" << readNum;
    return { ErrorCodes::ERR_OK, "" };
}

Status ChunkMigrateReceiver::supplySetKV(const string& key, const string& value) {
    Expected<RecordKey> expRk = RecordKey::decode(key);
    if (!expRk.ok()) {
        return expRk.status();
    }
    Expected<RecordValue> expRv = RecordValue::decode(value);
    if (!expRv.ok()) {
        return expRv.status();
    }

    uint32_t  slotid = expRk.value().getChunkId();
    if (!_slots.test(slotid)) {
        LOG(ERROR) << "slotid:"<< expRk.value().getPrimaryKey()
                   << "is not a member in bitmap";
        return { ErrorCodes::ERR_INTERNAL, "slotid not match" };
    }


    PStore kvstore = _dbWithLock->store;
    auto eTxn = kvstore->createTransaction(nullptr);
    EXPECT_EQ(eTxn.ok(), true);
    std::unique_ptr<Transaction> txn = std::move(eTxn.value());

    Status s = kvstore->setKV(expRk.value(), expRv.value(), txn.get());
    EXPECT_EQ(s.ok(), true);

    // add TTL, what type need ttl ?
    if (expRv.value().getRecordType() == RecordType::RT_DATA_META) { // kv no expire???
        if (!Command::noExpire()) {
            // add new index entry
            TTLIndex n_ictx(expRk.value().getPrimaryKey(), expRv.value().getRecordType(), _storeid, expRv.value().getTtl());
            s = txn->setKV(n_ictx.encode(),
                RecordValue(RecordType::RT_TTL_INDEX).encode());
            if (!s.ok()) {
                return s;
            }
        }
    }

    auto commitStatus = txn->commit(); // todo, all commit need retry???
    s = commitStatus.status();
    if (s.ok()) {
        return { ErrorCodes::ERR_OK, "" };
    } else if (s.code() != ErrorCodes::ERR_COMMIT_RETRY) {
        return s;
    }
    return { ErrorCodes::ERR_OK, "" };
}

Status ChunkMigrateReceiver::applyBinlog(Session* sess, uint32_t storeid,
                            uint32_t chunkid, const std::string& logKey,
                            const std::string& logValue) {
    auto binlog = applySingleTxnV2(sess, storeid,
        logKey, logValue, chunkid);
    if (!binlog.ok()) {
        return binlog.status();
    }
    return { ErrorCodes::ERR_OK, "" };
}



} // end namespace
