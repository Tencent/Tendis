#include <memory>
#include <string>
#include <utility>
//#include "tendisplus/replication/repl_util.h"
#include "glog/logging.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

std::shared_ptr<BlockingTcpClient> createClient(
    const string& ip, uint16_t port, std::shared_ptr<ServerEntry> svr) {
    ServerEntry* svrPtr = svr.get();
    auto client = createClient(ip, port, svrPtr);
    return  client;
}

std::shared_ptr<BlockingTcpClient> createClient(
        const string& ip, uint16_t port, ServerEntry* svr) {
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
    const std::shared_ptr<ServerParams> cfg) {

    uint32_t suggestBatch = svr->getParams()->bingLogSendBatch;
    size_t suggestBytes = svr->getParams()->bingLogSendBytes;


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
            if (explog.value().getChunkId() == Transaction::CHUNKID_FLUSH) {
                // flush binlog should be alone
                LOG(INFO) << "masterSendBinlogV2 deal with chunk flush: "
                    <<explog.value().getChunkId();
                if (writer.getCount() > 0)
                    break;

                writer.setFlag(BinlogFlag::FLUSH);
                LOG(INFO) << "masterSendBinlogV2 send flush binlog to slave, store:" << storeId;
            } else if (explog.value().getChunkId() == Transaction::CHUNKID_MIGRATE) {
                // migrate binlog should be alone
                LOG(INFO) << "masterSendBinlogV2 deal with chunk migrate: "
                    <<explog.value().getChunkId();
                if (writer.getCount() > 0)
                    break;

                writer.setFlag(BinlogFlag::MIGRATE);
                LOG(INFO) << "masterSendBinlogV2 send migrate binlog to slave, store:" << storeId;
            }


            binlogId = explog.value().getBinlogId();
            if (writer.writeRepllogRaw(explog.value()) ||
                writer.getFlag() == BinlogFlag::FLUSH ||
                writer.getFlag() == BinlogFlag::MIGRATE) {
                // full or flush
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

    // TODO(wayenchen)  takenliu add, call sendWriter(), avoid code copy.
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
        Command::fmtMultiBulkLen(ss2, 5);
        Command::fmtBulk(ss2, "applybinlogsv2");
        Command::fmtBulk(ss2, std::to_string(dstStoreId));
        Command::fmtBulk(ss2, writer.getBinlogStr());
        Command::fmtBulk(ss2, std::to_string(writer.getCount()));
        Command::fmtBulk(ss2, std::to_string((uint32_t)writer.getFlag()));
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
    BinlogApplyMode mode) {
    auto svr = sess->getServerEntry();
    auto expdb = svr->getSegmentMgr()->getDb(sess, storeId,
                                             mgl::LockMode::LOCK_IX);
    if (!expdb.ok()) {
        LOG(ERROR) << "getDb failed:" << expdb.status().toString();
        return expdb.status();
    }
    auto store = std::move(expdb.value().store);
    INVARIANT(store != nullptr);
    auto ptxn = store->createTransaction(sess);
    if (!ptxn.ok()) {
        LOG(ERROR) << "createTransaction failed:" << ptxn.status().toString();
        return ptxn.status();
    }

    std::unique_ptr<Transaction> txn = std::move(ptxn.value());

    auto key = ReplLogKeyV2::decode(logKey);
    if (!key.ok()) {
        LOG(ERROR) << "ReplLogKeyV2::decode failed:" << key.status().toString();
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
    if (mode == BinlogApplyMode::KEEP_BINLOG_ID) {
        binlogId = key.value().getBinlogId();
        if (binlogId <= store->getHighestBinlogId()) {
            string err = "binlogId:" + to_string(binlogId)
                + " can't be smaller than highestBinlogId:"
                + to_string(store->getHighestBinlogId());
            LOG(ERROR) << err;
            return { ErrorCodes::ERR_MANUAL, err };
        }

        // store the binlog directly, same as master
        auto s = txn->setBinlogKV(binlogId, logKey, logValue);
        if (!s.ok()) {
            return s;
        }
    } else {  // migrating chunk.
        // use self binlogId to replace sender's binlogId
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

Status sendWriter(BinlogWriter* writer, BlockingTcpClient*client,
                  uint32_t dstStoreId, bool needHeartBeart,
                  uint32_t secs) {
    std::stringstream ss2;

    if (writer && writer->getCount() >0) {
        Command::fmtMultiBulkLen(ss2, 5);
        Command::fmtBulk(ss2, "migratebinlogs");
        Command::fmtBulk(ss2, std::to_string(dstStoreId));
        Command::fmtBulk(ss2, writer->getBinlogStr());
        Command::fmtBulk(ss2, std::to_string(writer->getCount()));
        Command::fmtBulk(ss2, std::to_string((uint32_t)writer->getFlag()));
    } else {
        if (!needHeartBeart) {
            return {ErrorCodes::ERR_OK, "finish send bulk"};
        }

        Command::fmtMultiBulkLen(ss2, 2);
        Command::fmtBulk(ss2, "binlog_heartbeat");
        Command::fmtBulk(ss2, std::to_string(dstStoreId));
    }

    std::string stringtoWrite = ss2.str();
    Status s = client->writeData(stringtoWrite);
    if (!s.ok()) {
        LOG(WARNING) << " dst Store:" << dstStoreId
                     << " writeData failed:" << s.toString()
                     << "; Size:" << stringtoWrite.size();
        return s;
    }
    Expected<std::string> exptOK = client->readLine(std::chrono::seconds(secs));
    if (!exptOK.ok()) {
        LOG(WARNING) <<  " dst Store:" << dstStoreId
                     << " readLine failed:" << exptOK.status().toString()
                     << "; Size:" << stringtoWrite.size()
                     << "; Seconds:" << secs;
        return exptOK.status();
    } else if (exptOK.value() != "+OK") {
        LOG(WARNING) << " dst Store:" << dstStoreId
                     << " apply binlogs failed:" << exptOK.value();
        return{ ErrorCodes::ERR_NETWORK, "bad return string" };
    }

    return  {ErrorCodes::ERR_OK, "finish send bulk"};
}

Status SendSlotsBinlog(BlockingTcpClient* client,
                       uint32_t storeId, uint32_t dstStoreId,
                       uint64_t binlogPos, uint64_t binlogEnd,
                       bool needHeartBeart,
                       const std::bitset<CLUSTER_SLOTS>& slotsMap,
                       std::shared_ptr<ServerEntry> svr,
                       uint64_t* sendBinlogNum,
                       uint64_t* newBinlogId) {
    uint32_t suggestBatch = svr->getParams()->bingLogSendBatch;
    size_t suggestBytes = svr->getParams()->bingLogSendBytes;
    uint32_t timeoutSecs = svr->getParams()->timeoutSecBinlogWaitRsp;

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

    std::unique_ptr<BinlogWriter> writer =
                std::make_unique<BinlogWriter>(suggestBytes, suggestBatch);

    DLOG(INFO) << "begin catch up from:" << binlogPos << " to:" << binlogEnd
            << " storeid:" << storeId
            << " slots:" << bitsetStrEncode(slotsMap);
    *sendBinlogNum = 0;
    uint64_t binlogId = binlogPos;
    uint64_t binlogNum = 0;
    while (true) {
        Expected<ReplLogRawV2> explog = cursor->next();
        if (!explog.ok()) {
            if (explog.status().code() == ErrorCodes::ERR_EXHAUST) {
                DLOG(INFO) << "catch up binlog exhaust,binlogPos:" << binlogPos
                          << " binlogEnd:" << binlogEnd << " Current:" << binlogId
                          << " logNum:" << *sendBinlogNum << " storeid:" << storeId
                          << " slots:" << bitsetStrEncode(slotsMap);
                break;
            }
            LOG(ERROR) << "iter binlog failed:"
                       << explog.status().toString();
            return explog.status();
        }

        auto slot = explog.value().getChunkId();

        if (slot > CLUSTER_SLOTS - 1) {
            continue;
        }

        if (explog.value().getBinlogId() > binlogEnd) {
            DLOG(INFO) << "catch up binlog success, binlogPos:" << binlogPos
                       << " binlogEnd:" << binlogEnd << " logNum:" << *sendBinlogNum
                       << " storeid:" << storeId
                       << " slots:" << bitsetStrEncode(slotsMap);
            break;
        }

        // write slot binlog
        if (slotsMap.test(slot)) {
            bool writeFull = writer->writeRepllogRaw(explog.value());
            binlogNum++;
            binlogId = explog.value().getBinlogId();

            if (writeFull || writer->getFlag() == BinlogFlag::FLUSH) {
                auto s = sendWriter(writer.get(), client, dstStoreId, needHeartBeart, timeoutSecs);
                if (!s.ok()) {
                    LOG(ERROR) << "send writer bulk fail on slot:"
                        << slot << " " << s.toString();
                    return s;
                }
                *newBinlogId = binlogId;
                *sendBinlogNum += binlogNum;
                binlogNum = 0;

                /* *
                 * Rate limit for migration
                 */
                svr->getMigrateManager()->requestRateLimit(writer->getSize());
                writer->resetWriter();
            }
        }
    }
    if (writer->getCount() != 0) {
        auto s = sendWriter(writer.get(), client, dstStoreId, needHeartBeart, timeoutSecs);
        if (!s.ok()) {
            LOG(ERROR) << "send writer bulk fail, cout:" << writer->getCount();
            return s;
        }
        *newBinlogId = binlogId;
        *sendBinlogNum += binlogNum;
        binlogNum = 0;
		
        /* *
         * Rate limit for migration
         */
        svr->getMigrateManager()->requestRateLimit(writer->getSize());
        writer->resetWriter();
    }
    return { ErrorCodes::ERR_OK, "" };
}

}  // namespace tendisplus
