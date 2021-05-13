// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include "tendisplus/replication/repl_util.h"

#include <memory>
#include <string>
#include <utility>
#include "glog/logging.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

std::shared_ptr<BlockingTcpClient> createClient(
  const string& ip, uint16_t port, std::shared_ptr<ServerEntry> svr) {
  ServerEntry* svrPtr = svr.get();
  auto client = createClient(ip, port, svrPtr);
  return client;
}

std::shared_ptr<BlockingTcpClient> createClient(const string& ip,
                                                uint16_t port,
                                                ServerEntry* svr) {
  std::shared_ptr<BlockingTcpClient> client =
    std::move(svr->getNetwork()->createBlockingClient(64 * 1024 * 1024));
  Status s = client->connect(ip, port, std::chrono::seconds(3));
  if (!s.ok()) {
    LOG(WARNING) << "connect " << ip << ":" << port
                 << " failed:" << s.toString();
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

Expected<BinlogResult> masterSendBinlogV2(
  BlockingTcpClient* client,
  uint32_t storeId,
  uint32_t dstStoreId,
  uint64_t binlogPos,
  bool needHeartBeart,
  std::shared_ptr<ServerEntry> svr,
  const std::shared_ptr<ServerParams> cfg) {
  uint32_t suggestBatch = svr->getParams()->binlogSendBatch;
  size_t suggestBytes = svr->getParams()->binlogSendBytes;

  LocalSessionGuard sg(svr.get());
  sg.getSession()->setArgs({"mastersendlog",
                            std::to_string(storeId),
                            client->getRemoteRepr(),
                            std::to_string(dstStoreId),
                            std::to_string(binlogPos)});

  auto expdb = svr->getSegmentMgr()->getDb(
    sg.getSession(), storeId, mgl::LockMode::LOCK_IS);
  if (!expdb.ok()) {
    return expdb.status();
  }
  auto store = std::move(expdb.value().store);
  INVARIANT(store != nullptr);

  auto ptxn = store->createTransaction(sg.getSession());
  if (!ptxn.ok()) {
    return ptxn.status();
  }

  BinlogResult br;

  std::unique_ptr<Transaction> txn = std::move(ptxn.value());
  std::unique_ptr<RepllogCursorV2> cursor =
    txn->createRepllogCursorV2(binlogPos + 1);

  BinlogWriter writer(suggestBytes, suggestBatch);
  while (true) {
    Expected<ReplLogRawV2> explog = cursor->next();
    if (explog.ok()) {
      if (explog.value().getChunkId() == Transaction::CHUNKID_FLUSH) {
        // flush binlog should be alone
        LOG(INFO) << "masterSendBinlogV2 deal with chunk flush: "
                  << explog.value().getChunkId();
        if (writer.getCount() > 0)
          break;

        writer.setFlag(BinlogFlag::FLUSH);
        LOG(INFO) << "masterSendBinlogV2 send flush binlog to slave, store:"
                  << storeId;
      } else if (explog.value().getChunkId() == Transaction::CHUNKID_MIGRATE) {
        // migrate binlog should be alone
        LOG(INFO) << "masterSendBinlogV2 deal with chunk migrate: "
                  << explog.value().getChunkId();
        if (writer.getCount() > 0)
          break;

        writer.setFlag(BinlogFlag::MIGRATE);
        LOG(INFO) << "masterSendBinlogV2 send migrate binlog to slave, store:"
                  << storeId;
      }

      br.binlogId = explog.value().getBinlogId();
      br.binlogTs = explog.value().getTimestamp();

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
      LOG(ERROR) << "iter binlog failed:" << explog.status().toString();
      return explog.status();
    }
  }

  std::stringstream ss2;
  if (writer.getCount() == 0) {
    br.binlogId = binlogPos;
    br.binlogTs = msSinceEpoch();

    if (!needHeartBeart) {
      return br;
    }
    // keep the client alive
    Command::fmtMultiBulkLen(ss2, 3);
    Command::fmtBulk(ss2, "binlog_heartbeat");
    Command::fmtBulk(ss2, std::to_string(dstStoreId));
    /* add timestamp which binlog_heartbeat created */
    Command::fmtBulk(ss2, std::to_string(br.binlogTs));
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
  Expected<std::string> exptOK = client->readLine(std::chrono::seconds(secs));
  if (!exptOK.ok()) {
    LOG(WARNING) << "store:" << storeId << " dst Store:" << dstStoreId
                 << " readLine failed:" << exptOK.status().toString()
                 << "; Size:" << stringtoWrite.size() << "; Seconds:" << secs;
    return exptOK.status();
  } else if (exptOK.value() != "+OK") {
    LOG(WARNING) << "store:" << storeId << " dst Store:" << dstStoreId
                 << " apply binlogs failed:" << exptOK.value();
    return {ErrorCodes::ERR_NETWORK, "bad return string"};
  }

  if (writer.getCount() == 0) {
    return br;
  } else {
    INVARIANT_D(binlogPos + writer.getCount() <= br.binlogId);
    return br;
  }
}

Expected<BinlogResult> masterSendAof(BlockingTcpClient* client,
                                     uint32_t storeId,
                                     uint32_t dstStoreId,
                                     uint64_t binlogPos,
                                     bool needHeartBeart,
                                     std::shared_ptr<ServerEntry> svr,
                                     const std::shared_ptr<ServerParams> cfg) {
  LocalSessionGuard sg(svr.get());

  sg.getSession()->setArgs({"mastersendlog",
                            std::to_string(storeId),
                            client->getRemoteRepr(),
                            std::to_string(dstStoreId),
                            std::to_string(binlogPos)});

  auto expdb = svr->getSegmentMgr()->getDb(
    sg.getSession(), storeId, mgl::LockMode::LOCK_IS);
  if (!expdb.ok()) {
    return expdb.status();
  }
  auto store = std::move(expdb.value().store);
  INVARIANT(store != nullptr);

  auto ptxn = store->createTransaction(sg.getSession());
  if (!ptxn.ok()) {
    return ptxn.status();
  }

  BinlogResult br;
  std::unique_ptr<Transaction> txn = std::move(ptxn.value());
  std::unique_ptr<RepllogCursorV2> cursor =
    txn->createRepllogCursorV2(binlogPos + 1);

  std::stringstream ss;
  std::string cmdStr = "";
  uint16_t cmdNum = 0;
  uint32_t aofPsyncNum = svr->getParams()->aofPsyncNum;
  while (true) {
    Expected<ReplLogRawV2> explog = cursor->next();
    if (explog.ok()) {
      br.binlogId = explog.value().getBinlogId();
      br.binlogTs = explog.value().getTimestamp();

      std::string value = explog.value().getReplLogValue();
      Expected<ReplLogValueV2> logValue = ReplLogValueV2::decode(value);

      if (!logValue.ok()) {
        LOG(ERROR) << "decode logvalue failed." << logValue.status().toString();
        return logValue.status();
      }
      cmdStr = logValue.value().getCmd();

      ss << cmdStr;
      if ((++cmdNum) > aofPsyncNum) {
        break;
      }

    } else if (explog.status().code() == ErrorCodes::ERR_EXHAUST) {
      // no more data
      break;
    } else {
      LOG(ERROR) << "iter binlog failed:" << explog.status().toString();
      return explog.status();
    }
  }
  if (cmdNum == 0) {
    br.binlogId = binlogPos;
    br.binlogTs = 0;
    if (!needHeartBeart) {
      return br;
    }
    // keep the client alive
    // TODO(wayenchen) if send PING or binglog_heart, need read data to avoid
    // client error
    if (svr->getParams()->psyncEnabled) {
      Command::fmtMultiBulkLen(ss, 3);
      Command::fmtBulk(ss, "binlog_heartbeat");
      Command::fmtBulk(ss, std::to_string(dstStoreId));
      Command::fmtBulk(ss, std::to_string(br.binlogTs));
    } else {
      Command::fmtMultiBulkLen(ss, 1);
      Command::fmtBulk(ss, "PING");
    }
  }
  Status s = client->writeData(ss.str());
  if (!s.ok()) {
    LOG(ERROR) << "store:" << storeId << " dst Store:" << dstStoreId
               << " writeData failed:" << s.toString()
               << "remote:" << client->getRemoteRepr()
               << "; Size:" << cmdStr.size();
    return s;
  }
  return br;
}

Expected<BinlogResult> applySingleTxnV2(Session* sess,
                                        uint32_t storeId,
                                        const std::string& logKey,
                                        const std::string& logValue,
                                        BinlogApplyMode mode) {
  auto svr = sess->getServerEntry();
  auto expdb =
    svr->getSegmentMgr()->getDb(sess, storeId, mgl::LockMode::LOCK_IX);
  if (!expdb.ok()) {
    LOG(ERROR) << "getDb failed:" << expdb.status().toString();
    return expdb.status();
  }

  if (mode == BinlogApplyMode::KEEP_BINLOG_ID) {
    if (!sess->getCtx()->isReplOnly()) {
      INVARIANT_D(0);
      return {ErrorCodes::ERR_INTERNAL, "It is not a slave"};
    }
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
    auto entry = ReplLogValueEntryV2::decode(
      (const char*)data + offset, dataSize - offset, &size);
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
    return {ErrorCodes::ERR_INTERNAL, "bad binlog"};
  }

  uint64_t binlogId = 0;
  if (mode == BinlogApplyMode::KEEP_BINLOG_ID) {
    binlogId = key.value().getBinlogId();
    if (binlogId <= store->getHighestBinlogId()) {
      string err = "binlogId:" + to_string(binlogId) +
        " can't be smaller than highestBinlogId:" +
        to_string(store->getHighestBinlogId());
      LOG(ERROR) << err << " storeid:" << storeId;
      return {ErrorCodes::ERR_MANUAL, err};
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

  BinlogResult br;
  br.binlogTs = timestamp;
  br.binlogId = binlogId;
  return br;
}

Status sendWriter(BinlogWriter* writer,
                  BlockingTcpClient* client,
                  uint32_t dstStoreId,
                  const std::string& taskId,
                  bool needHeartBeart,
                  bool* needRetry,
                  uint32_t secs) {
  std::stringstream ss2;

  if (writer && writer->getCount() > 0) {
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
    // keep the client alive
    Command::fmtMultiBulkLen(ss2, 3);
    Command::fmtBulk(ss2, "migrate_heartbeat");
    Command::fmtBulk(ss2, std::to_string(dstStoreId));
    Command::fmtBulk(ss2, taskId);
  }

  std::string stringtoWrite = ss2.str();
  Status s = client->writeData(stringtoWrite);
  if (!s.ok()) {
    LOG(WARNING) << " dst Store:" << dstStoreId
                 << " writeData failed:" << s.toString()
                 << "; Size:" << stringtoWrite.size();
    *needRetry = true;
    return s;
  }

  Expected<std::string> exptOK = client->readLine(std::chrono::seconds(secs));
  if (!exptOK.ok()) {
    LOG(WARNING) << " dst Store:" << dstStoreId
                 << " readLine failed:" << exptOK.status().toString()
                 << "; Size:" << stringtoWrite.size() << "; Seconds:" << secs;
    *needRetry = true;
    return exptOK.status();
  } else if (exptOK.value() != "+OK") {
    LOG(WARNING) << " dst Store:" << dstStoreId
                 << " apply binlogs failed:" << exptOK.value();
    return {ErrorCodes::ERR_NETWORK, "bad return string"};
  }

  return {ErrorCodes::ERR_OK, "finish send bulk"};
}

Status SendSlotsBinlog(BlockingTcpClient* client,
                       uint32_t storeId,
                       uint32_t dstStoreId,
                       uint64_t binlogPos,
                       uint64_t binlogEnd,
                       bool needHeartBeart,
                       const std::bitset<CLUSTER_SLOTS>& slotsMap,
                       const std::string& taskid,
                       std::shared_ptr<ServerEntry> svr,
                       uint64_t* sendBinlogNum,
                       uint64_t* newBinlogId,
                       bool* needRetry,
                       uint64_t* binlogTimeStamp) {
  uint32_t suggestBatch = svr->getParams()->binlogSendBatch;
  size_t suggestBytes = svr->getParams()->binlogSendBytes;
  uint32_t timeoutSecs = svr->getParams()->timeoutSecBinlogWaitRsp;

  LocalSessionGuard sg(svr.get());
  sg.getSession()->setArgs({"mastersendlog",
                            std::to_string(storeId),
                            client->getRemoteRepr(),
                            std::to_string(dstStoreId),
                            std::to_string(binlogPos)});

  auto expdb = svr->getSegmentMgr()->getDb(
    sg.getSession(), storeId, mgl::LockMode::LOCK_IS);
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
  // cursor need ignore readbarrier
  std::unique_ptr<RepllogCursorV2> cursor =
    txn->createRepllogCursorV2(binlogPos + 1, true);

  std::unique_ptr<BinlogWriter> writer =
    std::make_unique<BinlogWriter>(suggestBytes, suggestBatch);

  DLOG(INFO) << "begin catch up from:" << binlogPos << " to:" << binlogEnd
             << " storeid:" << storeId
             << " slots:" << bitsetStrEncode(slotsMap);
  *sendBinlogNum = 0;
  uint64_t binlogId = binlogPos;
  uint64_t binlogNum = 0;
  uint64_t totalLogNum = 0;
  uint64_t heartBeatTime = sinceEpoch();
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
      LOG(ERROR) << "iter binlog failed:" << explog.status().toString();
      return explog.status();
    }

    if (!(++totalLogNum % 10000)) {
      auto delay = sinceEpoch() - heartBeatTime;
      /*NOTE(wayenchen) send a heartbeat every 6s*/
      if (delay > 6) {
        // send migrate heartheat
        auto s = sendWriter(
          nullptr, client, dstStoreId, taskid, true, needRetry, timeoutSecs);
        if (!s.ok()) {
          LOG(ERROR) << "send migrate heartbeat fail on task:" << taskid
                     << s.toString();
          return s;
        }
        heartBeatTime = sinceEpoch();
      }
    }

    auto slot = explog.value().getChunkId();
    *binlogTimeStamp = explog.value().getTimestamp();
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
        auto s = sendWriter(writer.get(),
                            client,
                            dstStoreId,
                            taskid,
                            needHeartBeart,
                            needRetry,
                            timeoutSecs);
        if (!s.ok()) {
          LOG(ERROR) << "send writer bulk fail on slot:" << slot << " "
                     << s.toString();
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
    auto s = sendWriter(writer.get(),
                        client,
                        dstStoreId,
                        taskid,
                        needHeartBeart,
                        needRetry,
                        timeoutSecs);
    if (!s.ok()) {
      LOG(ERROR) << "send writer bulk fail, cout:" << writer->getCount();
      return s;
    }
    *newBinlogId = binlogId;
    *sendBinlogNum += binlogNum;

    /* *
     * Rate limit for migration
     */
    svr->getMigrateManager()->requestRateLimit(writer->getSize());
    writer->resetWriter();
  }
  return {ErrorCodes::ERR_OK, ""};
}

}  // namespace tendisplus
