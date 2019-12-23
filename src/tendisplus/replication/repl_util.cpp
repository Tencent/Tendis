#include "glog/logging.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/replication/repl_util.h"

namespace tendisplus {

std::shared_ptr<BlockingTcpClient> createClient(
    const string& ip, uint16_t port, std::shared_ptr<ServerEntry> svr) {
    std::shared_ptr<BlockingTcpClient> client =
        std::move(svr->getNetwork()->createBlockingClient(64*1024*1024));
    Status s = client->connect(
        ip,
        port,
        std::chrono::seconds(3));
    if (!s.ok()) {
        LOG(WARNING) << "connect " << ip
            << ":" << port << " failed:"
            << s.toString();
        return nullptr;
    }

    std::string masterauth = svr->masterauth();
    if (masterauth != "") {
        std::stringstream ss;
        ss << "AUTH " << masterauth;
        client->writeLine(ss.str());
        Expected<std::string> s = client->readLine(std::chrono::seconds(10));
        if (!s.ok()) {
            LOG(WARNING) << "fullSync auth error:" << s.status().toString();
            return nullptr;
        }
        if (s.value().size() == 0 || s.value()[0] == '-') {
            LOG(INFO) << "fullSync auth failed:" << s.value();
            return nullptr;
        }
    }
    return std::move(client);
}

Expected<uint64_t> masterSendBinlogV2(BlockingTcpClient* client,
    uint32_t storeId, uint32_t dstStoreId,
    uint64_t binlogPos, bool needHeartBeart,
    std::shared_ptr<ServerEntry> svr,
    const std::shared_ptr<ServerParams> cfg,
    uint32_t chunkid) {
    // TODO(vinchen): to be options
    constexpr uint32_t suggestBatch = 256;
    constexpr size_t suggestBytes = 16 * 1024 * 1024;

    LocalSessionGuard sg(svr.get());
    sg.getSession()->setArgs(
            { "mastersendlog",
              std::to_string(storeId),
              client->getRemoteRepr(),
              std::to_string(dstStoreId),
              std::to_string(binlogPos) });

    auto expdb = svr->getSegmentMgr()->getDb(sg.getSession(),
                                             storeId, mgl::LockMode::LOCK_IS);
    if (!expdb.ok()) {
        return expdb.status();
    }
    auto store = std::move(expdb.value().store);
    INVARIANT(store != nullptr);

    auto ptxn = store->createTransaction(sg.getSession());
    if (!ptxn.ok()) {
        return ptxn.status();
    }

    std::unique_ptr<Transaction> txn = std::move(ptxn.value());
    std::unique_ptr<RepllogCursorV2> cursor =
            txn->createRepllogCursorV2(binlogPos + 1);

    uint64_t binlogId = binlogPos;
    BinlogWriter writer(suggestBytes, suggestBatch);
    while (true) {
        Expected<ReplLogRawV2> explog = cursor->next();
        if (explog.ok()) {
            // migrate chunk, need filter by chunkid, and flush command should not be done.
            if (chunkid != Transaction::CHUNKID_UNINITED
                && explog.value().getChunkId() != chunkid) {
                continue;
            }
            if (explog.value().getChunkId() == Transaction::CHUNKID_FLUSH) {
                // flush binlog should be alone
                if (writer.getCount() > 0)
                    break;

                writer.setFlag(BinlogFlag::FLUSH);
                LOG(INFO) << "send flush binlog to slave, store:" << storeId;
            }

            binlogId = explog.value().getBinlogId();
            if (writer.writeRepllogRaw(explog.value()) ||
                writer.getFlag() == BinlogFlag::FLUSH) {
                // full  or flush
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
    if (writer.getCount() == 0) {
        if (!needHeartBeart) {
            return binlogPos;
        }
        // keep the client alive
        Command::fmtMultiBulkLen(ss2, 2);
        Command::fmtBulk(ss2, "binlog_heartbeat");
        Command::fmtBulk(ss2, std::to_string(dstStoreId));
    } else {
        // TODO(vinchen): too more copy
        Command::fmtMultiBulkLen(ss2, 6);
        Command::fmtBulk(ss2, "applybinlogsv2");
        Command::fmtBulk(ss2, std::to_string(dstStoreId));
        Command::fmtBulk(ss2, writer.getBinlogStr());
        Command::fmtBulk(ss2, std::to_string(writer.getCount()));
        Command::fmtBulk(ss2, std::to_string((uint32_t)writer.getFlag()));
        Command::fmtBulk(ss2, std::to_string(chunkid));
    }

    std::string stringtoWrite = ss2.str();

    Status s = client->writeData(stringtoWrite);
    if (!s.ok()) {
        LOG(WARNING) << "store:" << storeId << " dst Store:" << dstStoreId
                     << " writeData failed:" << s.toString()
                     << "; Size:" << stringtoWrite.size();
        return s;
    }

    uint32_t secs = cfg->timeoutSecBinlogWaitRsp;
    // TODO(vinchen): NO NEED TO READ OK?
    Expected<std::string> exptOK = client->readLine(std::chrono::seconds(secs));
    if (!exptOK.ok()) {
        LOG(WARNING) << "store:" << storeId << " dst Store:" << dstStoreId
                     << " readLine failed:" << exptOK.status().toString()
                     << "; Size:" << stringtoWrite.size()
                     << "; Seconds:" << secs;
        return exptOK.status();
    } else if (exptOK.value() != "+OK") {
        LOG(WARNING) << "store:" << storeId << " dst Store:" << dstStoreId
                     << " apply binlogs failed:" << exptOK.value();
        return{ ErrorCodes::ERR_NETWORK, "bad return string" };
    }

    if (writer.getCount() == 0) {
        return binlogPos;
    } else {
        INVARIANT_D(binlogPos + writer.getCount() <= binlogId);
        return binlogId;
    }
}

Expected<uint64_t> applySingleTxnV2(Session* sess, uint32_t storeId,
    const std::string& logKey, const std::string& logValue,
    uint32_t chunkid) {
    auto svr = sess->getServerEntry();
    auto expdb = svr->getSegmentMgr()->getDb(sess, storeId,
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

    uint64_t binlogId = 0;
    if (chunkid == Transaction::CHUNKID_UNINITED) {
        binlogId = key.value().getBinlogId();
        if (binlogId <= store->getHighestBinlogId()) {
            string err = "binlogId:" + to_string(binlogId)
                + " can't be smaller than highestBinlogId:" + to_string(store->getHighestBinlogId());
            LOG(ERROR) << err;
            return { ErrorCodes::ERR_MANUAL, err };
        }

        // store the binlog directly, same as master
        auto s = txn->setBinlogKV(binlogId, logKey, logValue);
        if (!s.ok()) {
            return s;
        }
    } else { // migrating chunk.
        //LOG(INFO) << "takenliutest: applyBinlog";
        auto s = txn->setBinlogKV(logKey, logValue);
        if (!s.ok()) {
            return s;
        }
        binlogId = txn->getBinlogId();
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

}  // namespace tendisplus
