// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <string>
#include <utility>
#include <memory>
#include <algorithm>
#include <cctype>
#include <vector>
#include <clocale>
#include <map>
#include <list>
#include "glog/logging.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/base64.h"
#include "tendisplus/storage/varint.h"
#include "tendisplus/storage/rocks/rocks_kvstore.h"

namespace tendisplus {

class BackupCommand : public Command {
 public:
  BackupCommand() : Command("backup", "as") {}

  ssize_t arity() const {
    return -2;
  }

  int32_t firstkey() const {
    return 0;
  }

  int32_t lastkey() const {
    return 0;
  }

  int32_t keystep() const {
    return 0;
  }

  Expected<std::string> run(Session* sess) final {
    const std::string& dir = sess->getArgs()[1];
    auto mode = KVStore::BackupMode::BACKUP_COPY;
    if (sess->getArgs().size() >= 3) {
      const std::string& str_mode = toLower(sess->getArgs()[2]);
      if (str_mode == "ckpt") {
        mode = KVStore::BackupMode::BACKUP_CKPT;
      } else if (str_mode == "copy") {
        mode = KVStore::BackupMode::BACKUP_COPY;
      } else {
        return {ErrorCodes::ERR_MANUAL, "mode error, should be ckpt or copy"};
      }
    }
    auto svr = sess->getServerEntry();
    INVARIANT(svr != nullptr);

    // check whether current user has write permission on dir argument
    try {
      auto tmpDirPath = dir + "/tmpDir";
      filesystem::create_directory(tmpDirPath);
      filesystem::remove(tmpDirPath);
    } catch (const filesystem::filesystem_error& e) {
      return {ErrorCodes::ERR_MANUAL, e.what()};
    }

    if (!filesystem::exists(dir)) {
      return {ErrorCodes::ERR_MANUAL, "dir not exist:" + dir};
    }
    if (filesystem::equivalent(dir, svr->getParams()->dbPath)) {
      return {ErrorCodes::ERR_MANUAL, "dir cant be dbPath:" + dir};
    }

    if (svr->isClusterEnabled()) {
      auto state = svr->getClusterMgr()->getClusterState();
      Expected<std::string> eptNodeInfo = state->getBackupInfo();
      if (!eptNodeInfo.ok()) {
        LOG(ERROR) << "fail get info of my master:"
                   << state->getMyselfNode()->getMaster()->getNodeName();
        return {ErrorCodes::ERR_CLUSTER, "get info fail"};
      }

      std::string myNodeInfo = eptNodeInfo.value();
      LOG(INFO) << "myself Node info is:" << myNodeInfo;
      ofstream outfile(dir + "/" + "clustermeta.txt", fstream::out);
      if (outfile.is_open()) {
        outfile << myNodeInfo << std::endl;
        if (!outfile.good()) {
          LOG(ERROR) << "write node backup info fail";
          return {ErrorCodes::ERR_MANUAL, "write fail"};
        }
      } else {
        LOG(ERROR) << "can't open file: clustermeta.txt";
        return {ErrorCodes::ERR_INTERNAL, "can't open file: clustermeta.txt"};
      }
      // TODO(wayenchen) find path to write to file
    }

    // TODO(wayenchen): use make guard to unset backupruning when backup
    // failed!
    svr->setBackupRunning();

    for (uint32_t i = 0; i < svr->getKVStoreCount(); ++i) {
      // NOTE(deyukong): here we acquire IS lock
      auto expdb =
        svr->getSegmentMgr()->getDb(sess, i, mgl::LockMode::LOCK_IS, true);
      if (!expdb.ok()) {
        return expdb.status();
      }

      auto store = std::move(expdb.value().store);
      // if store is not open, skip it
      if (!store->isOpen()) {
        continue;
      }
      std::string dbdir = dir + "/" + std::to_string(i) + "/";
      Expected<BackupInfo> bkInfo =
        store->backup(dbdir, mode, svr->getCatalog()->getBinlogVersion());
      if (!bkInfo.ok()) {
        svr->onBackupEndFailed(i, bkInfo.status().toString());
        return bkInfo.status();
      }
    }
    svr->onBackupEnd();
    return Command::fmtOK();
  }
} bkupCmd;

class RestoreBackupCommand : public Command {
 public:
  RestoreBackupCommand() : Command("restorebackup", "aws") {}

  ssize_t arity() const {
    return -3;
  }

  int32_t firstkey() const {
    return 0;
  }

  int32_t lastkey() const {
    return 0;
  }

  int32_t keystep() const {
    return 0;
  }

  // restorebackup "all"|storeId dir
  // restorebackup "all"|storeId dir force
  Expected<std::string> run(Session* sess) final {
    auto svr = sess->getServerEntry();
    INVARIANT(svr != nullptr);
    const std::string& kvstore = sess->getArgs()[1];
    const std::string& dir = sess->getArgs()[2];

    bool isForce = false;
    if (sess->getArgs().size() >= 4) {
      isForce = sess->getArgs()[3] == "force";
    }
    // TODO(takenliu): should lock db first, then check it, and then
    // restore. otherwise, the state maybe changed when do restore.
    if (kvstore == "all") {
      for (uint32_t i = 0; i < svr->getKVStoreCount(); ++i) {
        if (!isForce && !isEmpty(svr, sess, i)) {
          return {ErrorCodes::ERR_INTERNAL, "not empty. use force please"};
        }
        if (svr->getReplManager()->isSlaveOfSomeone(i)) {
          return {ErrorCodes::ERR_INTERNAL, "has master, slaveof no one first"};
        }
        if (svr->getReplManager()->hasSomeSlave(i)) {
          return {ErrorCodes::ERR_INTERNAL, "has slave, rm slave first"};
        }
      }
      for (uint32_t i = 0; i < svr->getKVStoreCount(); ++i) {
        LOG(INFO) << "restorebackup store:" << i << " isForce:" << isForce
                  << " isEmpty:" << isEmpty(svr, sess, i);
        std::string storeDir = dir + "/" + std::to_string(i) + "/";
        auto ret = restoreBackup(svr, sess, i, storeDir);
        if (!ret.ok()) {
          return ret.status();
        }
      }
    } else {
      Expected<uint64_t> exptStoreId = ::tendisplus::stoul(kvstore.c_str());
      if (!exptStoreId.ok()) {
        return exptStoreId.status();
      }
      uint32_t storeId = (uint32_t)exptStoreId.value();
      if (!isForce && !isEmpty(svr, sess, storeId)) {
        return {ErrorCodes::ERR_INTERNAL, "not empty. use force please"};
      }
      if (svr->getReplManager()->isSlaveOfSomeone(storeId)) {
        return {ErrorCodes::ERR_INTERNAL, "has master, slaveof no one first"};
      }
      if (svr->getReplManager()->hasSomeSlave(storeId)) {
        return {ErrorCodes::ERR_INTERNAL, "has slave, rm slave first"};
      }
      LOG(INFO) << "restorebackup store:" << storeId << " isForce:" << isForce
                << " isEmpty:" << isEmpty(svr, sess, storeId);
      auto ret = restoreBackup(svr, sess, storeId, dir);
      if (!ret.ok()) {
        return ret.status();
      }
    }
    return Command::fmtOK();
  }

 private:
  bool isEmpty(ServerEntry* svr, Session* sess, uint32_t storeId) {
    // IS lock
    auto expdb =
      svr->getSegmentMgr()->getDb(sess, storeId, mgl::LockMode::LOCK_IS, true);
    if (!expdb.ok()) {
      return false;
    }
    auto store = std::move(expdb.value().store);
    return store->isEmpty();
  }

  Expected<std::string> restoreBackup(ServerEntry* svr,
                                      Session* sess,
                                      uint32_t storeId,
                                      const std::string& dir) {
    // X lock
    auto expdb =
      svr->getSegmentMgr()->getDb(sess, storeId, mgl::LockMode::LOCK_X, true);
    if (!expdb.ok()) {
      return expdb.status();
    }

    auto store = std::move(expdb.value().store);
    // if store is not open, skip it
    if (!store->isOpen()) {
      return {ErrorCodes::ERR_INTERNAL, "store not open"};
    }

    Status stopStatus = store->stop();
    if (!stopStatus.ok()) {
      // there may be uncanceled transactions binding with the store
      LOG(WARNING) << "restoreBackup stop store:" << storeId
                   << " failed:" << stopStatus.toString();
      return {ErrorCodes::ERR_INTERNAL, "stop failed."};
    }

    // clear dir
    INVARIANT(!store->isRunning());
    Status clearStatus = store->clear();
    if (!clearStatus.ok()) {
      INVARIANT_D(0);
      LOG(ERROR) << "Unexpected store:" << storeId << " clear"
                 << " failed:" << clearStatus.toString();
      return clearStatus;
    }

    Expected<std::string> ret = store->restoreBackup(dir);
    if (!ret.ok()) {
      return ret.status();
    }

    auto backup_meta = store->getBackupMeta(dir);
    if (!backup_meta.ok()) {
      return backup_meta.status();
    }
    uint64_t binlogpos = backup_meta.value().getBinlogPos();
    BinlogVersion binlogVersion = backup_meta.value().getBinlogVersion();

    uint32_t flags = 0;
    BinlogVersion mybversion = svr->getCatalog()->getBinlogVersion();
    LOG(INFO) << "store: " << storeId
              << " binlogVersion:" << static_cast<int>(binlogVersion)
              << " mybversion:" << static_cast<int>(mybversion);
    if (binlogVersion == BinlogVersion::BINLOG_VERSION_1) {
      if (mybversion == BinlogVersion::BINLOG_VERSION_2) {
        flags |= ROCKS_FLAGS_BINLOGVERSION_CHANGED;
      }
    } else if (binlogVersion == BinlogVersion::BINLOG_VERSION_2) {
      if (mybversion == BinlogVersion::BINLOG_VERSION_1) {
        LOG(ERROR) << "invalid binlog version";
        return {ErrorCodes::ERR_INTERNAL, "invalid binlog version"};
      }
    } else {
      INVARIANT_D(0);
    }

    Expected<uint64_t> restartStatus =
      store->restart(false, Transaction::MIN_VALID_TXNID, binlogpos, flags);
    if (!restartStatus.ok()) {
      INVARIANT_D(0);
      LOG(ERROR) << "restoreBackup restart store:" << storeId
                 << ",failed:" << restartStatus.status().toString();
      return {ErrorCodes::ERR_INTERNAL, "restart failed."};
    }
    Status s = svr->getReplManager()->resetRecycleState(storeId);
    if (!s.ok()) {
      INVARIANT_D(0);
      LOG(ERROR) << "restoreBackup ReplManager resetRecycleState store:"
                 << storeId << ",failed:" << s.toString();
      return {ErrorCodes::ERR_INTERNAL, "restart failed."};
    }

    return Command::fmtOK();
  }
} restoreBackupCommand;

// fullSync storeId dstStoreId ip port
class FullSyncCommand : public Command {
 public:
  FullSyncCommand() : Command("fullsync", "as") {}

  ssize_t arity() const {
    return 4;
  }

  int32_t firstkey() const {
    return 0;
  }

  int32_t lastkey() const {
    return 0;
  }

  int32_t keystep() const {
    return 0;
  }

  bool isBgCmd() const {
    return true;
  }

  Expected<std::string> run(Session* sess) final {
    LOG(FATAL) << "fullsync should not be called";
    // void compiler complain
    return {ErrorCodes::ERR_INTERNAL, "shouldn't be called"};
  }
} fullSyncCommand;

class QuitCommand : public Command {
 public:
  QuitCommand() : Command("quit", "as") {}

  ssize_t arity() const {
    return 0;
  }

  int32_t firstkey() const {
    return 0;
  }

  int32_t lastkey() const {
    return 0;
  }

  int32_t keystep() const {
    return 0;
  }

  bool isBgCmd() const {
    return true;
  }

  Expected<std::string> run(Session* sess) final {
    LOG(FATAL) << "quit should not be called";
    // void compiler complain
    return {ErrorCodes::ERR_INTERNAL, "shouldn't be called"};
  }
} quitCmd;

class ToggleIncrSyncCommand : public Command {
 public:
  ToggleIncrSyncCommand() : Command("toggleincrsync", "as") {}

  ssize_t arity() const {
    return 2;
  }

  int32_t firstkey() const {
    return 0;
  }

  int32_t lastkey() const {
    return 0;
  }

  int32_t keystep() const {
    return 0;
  }

  Expected<std::string> run(Session* sess) final {
    Expected<uint64_t> state = ::tendisplus::stoul(sess->getArgs()[1]);
    if (!state.ok()) {
      return state.status();
    }
    LOG(INFO) << "toggle incrsync state to:" << state.value();
    auto svr = sess->getServerEntry();
    INVARIANT(svr != nullptr);
    auto replMgr = svr->getReplManager();
    INVARIANT(replMgr != nullptr);
    replMgr->togglePauseState(state.value() ? false : true);
    return Command::fmtOK();
  }
} toggleIncrSyncCmd;

class IncrSyncCommand : public Command {
 public:
  IncrSyncCommand() : Command("incrsync", "as") {}

  ssize_t arity() const {
    return 6;
  }

  int32_t firstkey() const {
    return 0;
  }

  int32_t lastkey() const {
    return 0;
  }

  int32_t keystep() const {
    return 0;
  }

  bool isBgCmd() const {
    return true;
  }

  // incrSync storeId dstStoreId binlogId ip port
  // binlogId: the last binlog that has been applied
  Expected<std::string> run(Session* sess) final {
    LOG(FATAL) << "incrsync should not be called";

    // void compiler complain
    return {ErrorCodes::ERR_INTERNAL, "shouldn't be called"};
  }
} incrSyncCommand;

// TODO(takenliu) binlog_tool need send every applybinlogsv2 command and wait
// response, then send the next one,
//     because applybinlogsv2 will be processed by multi thread, so the squence
//     will be disorder
class ApplyBinlogsGeneric : public Command {
 private:
  BinlogApplyMode _mode;

 public:
  ApplyBinlogsGeneric(const std::string& name,
                      const char* sflags,
                      BinlogApplyMode mode)
    : Command(name, sflags), _mode(mode) {}

  static Status runNormal(Session* sess,
                          uint32_t storeId,
                          const std::string& binlogs,
                          size_t binlogCnt,
                          BinlogApplyMode mode) {
    auto svr = sess->getServerEntry();
    auto replMgr = svr->getReplManager();
    INVARIANT(replMgr != nullptr);

    size_t cnt = 0;
    BinlogReader reader(binlogs);
    while (true) {
      auto eLog = reader.next();
      if (eLog.status().code() == ErrorCodes::ERR_EXHAUST) {
        break;
      } else if (!eLog.ok()) {
        LOG(ERROR) << "reader.next() failed:" << eLog.status().toString();
        return eLog.status();
      }
      Status s;
      if (mode == BinlogApplyMode::KEEP_BINLOG_ID) {
        s = replMgr->applyRepllogV2(sess,
                                    storeId,
                                    eLog.value().getReplLogKey(),
                                    eLog.value().getReplLogValue());
      } else {
        if (!svr->isClusterEnabled()) {
          LOG(ERROR) << "not ClusterEnabled.";
          return {ErrorCodes::ERR_INTERNAL, "not ClusterEnabled"};
        }
        auto migrateMgr = svr->getMigrateManager();
        s = migrateMgr->applyRepllog(sess,
                                     storeId,
                                     mode,
                                     eLog.value().getReplLogKey(),
                                     eLog.value().getReplLogValue());
      }
      if (!s.ok()) {
        LOG(ERROR) << "applyRepllog failed,mode:" << (uint32_t)mode
                   << " err:" << eLog.status().toString();
        return s;
      }
      cnt++;
    }

    if (cnt != binlogCnt) {
      return {ErrorCodes::ERR_PARSEOPT, "invalid binlog size of binlog count"};
    }

    return {ErrorCodes::ERR_OK, ""};
  }

  static Status runFlush(Session* sess,
                         uint32_t storeId,
                         const std::string& binlogs,
                         size_t binlogCnt) {
    auto svr = sess->getServerEntry();
    auto replMgr = svr->getReplManager();
    INVARIANT(replMgr != nullptr);

    if (binlogCnt != 1) {
      return {ErrorCodes::ERR_PARSEOPT, "invalid binlog count"};
    }

    BinlogReader reader(binlogs);
    auto eLog = reader.nextV2();
    if (!eLog.ok()) {
      return eLog.status();
    }

    if (reader.nextV2().status().code() != ErrorCodes::ERR_EXHAUST) {
      return {ErrorCodes::ERR_PARSEOPT, "too big flush binlog"};
    }

    LOG(INFO) << "doing flush " << eLog.value().getReplLogValue().getCmd();

    LocalSessionGuard sg(svr);
    sg.getSession()->setArgs({eLog.value().getReplLogValue().getCmd()});

    auto expdb = svr->getSegmentMgr()->getDb(
      sg.getSession(), storeId, mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
      return expdb.status();
    }
    // fake the session to be not replonly!
    sg.getSession()->getCtx()->setReplOnly(false);

    // set binlog time before flush,
    // because the flush binlog is logical, not binary
    expdb.value().store->setBinlogTime(
      eLog.value().getReplLogValue().getTimestamp());
    auto eflush = expdb.value().store->flush(
      sg.getSession(), eLog.value().getReplLogKey().getBinlogId());
    if (!eflush.ok()) {
      return eflush.status();
    }
    INVARIANT_D(eflush.value() == eLog.value().getReplLogKey().getBinlogId());

    replMgr->onFlush(storeId, eflush.value());
    return {ErrorCodes::ERR_OK, ""};
  }

  static Status runMigrate(Session* sess,
                           uint32_t storeId,
                           const std::string& binlogs,
                           size_t binlogCnt) {
    auto svr = sess->getServerEntry();
    if (!svr->isClusterEnabled()) {
      LOG(ERROR) << "not ClusterEnabled.";
      return {ErrorCodes::ERR_INTERNAL, "not ClusterEnabled"};
    }
    auto migrateMgr = svr->getMigrateManager();
    INVARIANT(migrateMgr != nullptr);

    if (binlogCnt != 1) {
      LOG(ERROR) << "runMigrate binlogCnt != 1:";
      return {ErrorCodes::ERR_PARSEOPT, "invalid binlog count"};
    }

    BinlogReader reader(binlogs);
    auto eLog = reader.next();
    if (!eLog.ok()) {
      return eLog.status();
    }

    if (reader.nextV2().status().code() != ErrorCodes::ERR_EXHAUST) {
      return {ErrorCodes::ERR_PARSEOPT, "too big migrate binlog"};
    }
    const string& logKey = eLog.value().getReplLogKey();
    const string& logValue = eLog.value().getReplLogValue();
    auto key = ReplLogKeyV2::decode(logKey);
    if (!key.ok()) {
      LOG(ERROR) << "ReplLogKeyV2::decode failed:" << key.status().toString();
      return key.status();
    }
    auto value = ReplLogValueV2::decode(logValue);
    if (!value.ok()) {
      return value.status();
    }

    // LOG(INFO) << "runMigrate: " <<
    // eLog.value().getReplLogValue().getCmd();

    auto splits = stringSplit(value.value().getCmd(), "_");

    // args: type storeid slots nodename
    if (splits.size() != 4) {
      LOG(ERROR) << "runMigrate args err:" << value.value().getCmd();
      return {ErrorCodes::ERR_PARSEOPT, "args error"};
    }

    auto expdb =
      svr->getSegmentMgr()->getDb(sess, storeId, mgl::LockMode::LOCK_IX);
    if (!expdb.ok()) {
      return expdb.status();
    }
    auto store = std::move(expdb.value().store);
    INVARIANT(store != nullptr);

    Expected<int64_t> etype = ::tendisplus::stoll(splits[0]);
    if (!etype.ok() || etype.value() < MigrateBinlogType::RECEIVE_START ||
        etype.value() > MigrateBinlogType::SEND_END ||
        splits[2].size() != CLUSTER_SLOTS) {
      LOG(ERROR) << "runMigrate args err:" << value.value().getCmd();
      return etype.status();
    }
    MigrateBinlogType type = static_cast<MigrateBinlogType>(etype.value());

    auto ret =
      migrateMgr->applyMigrateBinlog(svr, store, type, splits[2], splits[3]);
    if (!ret.ok()) {
      LOG(ERROR) << "applyMigrateBinlog failed:" << ret.status().toString();
      return {ErrorCodes::ERR_INTERGER, "applyMigrateBinlog failed"};
    }

    // add binlog
    auto ptxn = sess->getCtx()->createTransaction(store);
    if (!ptxn.ok()) {
      LOG(ERROR) << "createTransaction failed:" << ptxn.status().toString();
      return ptxn.status();
    }

    uint64_t binlogId = key.value().getBinlogId();
    // store the binlog directly, same as master
    auto s = ptxn.value()->setBinlogKV(binlogId, logKey, logValue);
    if (!s.ok()) {
      return s;
    }
    Expected<uint64_t> expCmit =
      sess->getCtx()->commitTransaction(ptxn.value());
    if (!expCmit.ok()) {
      return expCmit.status();
    }

    store->setBinlogTime(value.value().getTimestamp());

    return {ErrorCodes::ERR_OK, ""};
  }

  // applybinlogsv2 storeId binlogs cnt flag
  // why is there no storeId ? storeId is contained in this
  // session in fact.
  // please refer to comments of ReplManager::registerIncrSync
  Expected<std::string> run(Session* sess) final {
    const std::vector<std::string>& args = sess->getArgs();
    uint32_t storeId;
    Expected<uint64_t> exptStoreId = ::tendisplus::stoul(args[1]);
    if (!exptStoreId.ok()) {
      return exptStoreId.status();
    }
    storeId = (uint32_t)exptStoreId.value();

    auto svr = sess->getServerEntry();
    INVARIANT(svr != nullptr);
    if (storeId >= svr->getKVStoreCount()) {
      return {ErrorCodes::ERR_PARSEOPT, "invalid storeId"};
    }

    uint64_t binlogCnt;
    auto exptCnt = ::tendisplus::stoul(args[3]);
    if (!exptCnt.ok()) {
      return exptCnt.status();
    }
    binlogCnt = exptCnt.value();

    auto eflag = ::tendisplus::stoul(args[4]);
    if (!eflag.ok()) {
      return {ErrorCodes::ERR_PARSEOPT, "invalid binlog flags"};
    }
    switch ((BinlogFlag)eflag.value()) {
      case BinlogFlag::NORMAL: {
        auto s = runNormal(sess, storeId, args[2], binlogCnt, _mode);
        if (!s.ok()) {
          return s;
        }
        break;
      }
      case BinlogFlag::FLUSH: {
        auto s = runFlush(sess, storeId, args[2], binlogCnt);
        if (!s.ok()) {
          return s;
        }
        break;
      }
      case BinlogFlag::MIGRATE: {
        auto s = runMigrate(sess, storeId, args[2], binlogCnt);
        if (!s.ok()) {
          return s;
        }
        break;
      }
    }
    return Command::fmtOK();
  }
};

class ApplyBinlogsCommandV2 : public ApplyBinlogsGeneric {
 public:
  ApplyBinlogsCommandV2()
    : ApplyBinlogsGeneric(
        "applybinlogsv2", "aws", BinlogApplyMode::KEEP_BINLOG_ID) {}

  ssize_t arity() const {
    return 5;
  }

  int32_t firstkey() const {
    return 0;
  }

  int32_t lastkey() const {
    return 0;
  }

  int32_t keystep() const {
    return 0;
  }
} applyBinlogsV2Command;

class MigrateBinlogsCommand : public ApplyBinlogsGeneric {
 public:
  MigrateBinlogsCommand()
    : ApplyBinlogsGeneric(
        "migratebinlogs", "aws", BinlogApplyMode::NEW_BINLOG_ID) {}

  ssize_t arity() const {
    return 5;
  }

  int32_t firstkey() const {
    return 0;
  }

  int32_t lastkey() const {
    return 0;
  }

  int32_t keystep() const {
    return 0;
  }
} migrateBinlogsCommand;

class RestoreBinlogCommandV2 : public Command {
 public:
  RestoreBinlogCommandV2() : Command("restorebinlogv2", "aws") {}

  ssize_t arity() const {
    return 4;
  }

  int32_t firstkey() const {
    return 0;
  }

  int32_t lastkey() const {
    return 0;
  }

  int32_t keystep() const {
    return 0;
  }

  Status runNormal(Session* sess,
                   uint32_t storeId,
                   const std::string& key,
                   const std::string& value) {
    bool oldReplOnly = sess->getCtx()->isReplOnly();
    sess->getCtx()->setReplOnly(true);
    auto ret = applySingleTxnV2(
      sess, storeId, key, value, BinlogApplyMode::KEEP_BINLOG_ID);
    sess->getCtx()->setReplOnly(oldReplOnly);
    if (!ret.ok()) {
      return ret.status();
    }
    return {ErrorCodes::ERR_OK, ""};
  }

  static Status runFlush(Session* sess,
                         uint32_t storeId,
                         const std::string& key,
                         const std::string& value) {
    auto svr = sess->getServerEntry();
    auto replMgr = svr->getReplManager();
    INVARIANT(replMgr != nullptr);

    auto logKey = ReplLogKeyV2::decode(key);
    if (!logKey.ok()) {
      return logKey.status();
    }
    auto logValue = ReplLogValueV2::decode(value);
    if (!logValue.ok()) {
      return logValue.status();
    }

    LOG(INFO) << "doing flush " << logValue.value().getCmd();

    LocalSessionGuard sg(svr);
    sg.getSession()->setArgs({logValue.value().getCmd()});

    auto expdb = svr->getSegmentMgr()->getDb(
      sg.getSession(), storeId, mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
      return expdb.status();
    }
    // fake the session to be not replonly!
    sg.getSession()->getCtx()->setReplOnly(false);

    // set binlog time before flush,
    // because the flush binlog is logical, not binary
    expdb.value().store->setBinlogTime(logValue.value().getTimestamp());
    auto eflush =
      expdb.value().store->flush(sg.getSession(), logKey.value().getBinlogId());
    if (!eflush.ok()) {
      return eflush.status();
    }
    INVARIANT_D(eflush.value() == logKey.value().getBinlogId());

    replMgr->onFlush(storeId, eflush.value());
    return {ErrorCodes::ERR_OK, ""};
  }

  Status runMigrate(Session* sess,
                    uint32_t storeId,
                    const std::string& logKey,
                    const std::string& logValue) {
    auto svr = sess->getServerEntry();
    if (!svr->isClusterEnabled()) {
      LOG(ERROR) << "not ClusterEnabled.";
      return {ErrorCodes::ERR_INTERNAL, "not ClusterEnabled"};
    }
    auto migrateMgr = svr->getMigrateManager();
    INVARIANT(migrateMgr != nullptr);

    auto key = ReplLogKeyV2::decode(logKey);
    if (!key.ok()) {
      LOG(ERROR) << "ReplLogKeyV2::decode failed:" << key.status().toString();
      return key.status();
    }

    auto value = ReplLogValueV2::decode(logValue);
    if (!value.ok()) {
      return value.status();
    }

    auto splits = stringSplit(value.value().getCmd(), "_");

    // args: type storeid slots nodename
    if (splits.size() != 4) {
      LOG(ERROR) << "runMigrate args err:" << value.value().getCmd();
      return {ErrorCodes::ERR_PARSEOPT, "args error"};
    }

    LocalSessionGuard sg(svr);
    auto expdb = svr->getSegmentMgr()->getDb(
      sg.getSession(), storeId, mgl::LockMode::LOCK_IX);
    if (!expdb.ok()) {
      return expdb.status();
    }

    Expected<int64_t> etype = ::tendisplus::stoll(splits[0]);
    if (!etype.ok() || etype.value() < MigrateBinlogType::RECEIVE_START ||
        etype.value() > MigrateBinlogType::SEND_END ||
        splits[2].size() != CLUSTER_SLOTS) {
      LOG(ERROR) << "runMigrate args err:" << value.value().getCmd();
      return etype.status();
    }
    MigrateBinlogType type = static_cast<MigrateBinlogType>(etype.value());

    Expected<uint64_t> cmdStoreId = ::tendisplus::stoul(splits[1]);
    if (!cmdStoreId.ok()) {
      return cmdStoreId.status();
    }
    if (storeId != cmdStoreId.value()) {
      LOG(ERROR) << "runMigrate storeid err, storeId:" << storeId
                 << " cmdStoreId:" << cmdStoreId.value();
      return {ErrorCodes::ERR_INTERGER, "storeid not match"};
    }

    expdb.value().store->setBinlogTime(value.value().getTimestamp());

    return migrateMgr->restoreMigrateBinlog(type, storeId, splits[2]);
  }

  // restorebinlogv2 storeId key(binlogid) value([op key value]*) checksum
  Expected<std::string> run(Session* sess) final {
    const std::vector<std::string>& args = sess->getArgs();

    uint32_t storeId;
    Expected<uint64_t> exptStoreId = ::tendisplus::stoul(args[1]);
    if (!exptStoreId.ok()) {
      return exptStoreId.status();
    }
    storeId = (uint32_t)exptStoreId.value();

    auto svr = sess->getServerEntry();
    INVARIANT(svr != nullptr);
    if (storeId >= svr->getKVStoreCount()) {
      return {ErrorCodes::ERR_PARSEOPT, "invalid storeId"};
    }

    std::string key = Base64::Decode(args[2].c_str(), args[2].size());
    std::string value = Base64::Decode(args[3].c_str(), args[3].size());

    auto logValue = ReplLogValueV2::decode(value);
    if (!logValue.ok()) {
      return logValue.status();
    }
    if (logValue.value().getChunkId() == Transaction::CHUNKID_FLUSH) {
      // TODO(takenliu) finish logical and add gtest for restorebinlog
      // flush
      auto s = runFlush(sess, storeId, key, value);
      if (!s.ok()) {
        return s;
      }
    } else if (logValue.value().getChunkId() == Transaction::CHUNKID_MIGRATE) {
      auto s = runMigrate(sess, storeId, key, value);
      if (!s.ok()) {
        return s;
      }
    } else {
      auto s = runNormal(sess, storeId, key, value);
      if (!s.ok()) {
        return s;
      }
    }
    return Command::fmtOK();
  }
} restoreBinlogV2Command;

class restoreEndCommand : public Command {
 public:
  restoreEndCommand() : Command("restoreend", "aws") {}

  ssize_t arity() const {
    return 2;
  }

  int32_t firstkey() const {
    return 0;
  }

  int32_t lastkey() const {
    return 0;
  }

  int32_t keystep() const {
    return 0;
  }

  // restoreend storeId
  Expected<std::string> run(Session* sess) final {
    const std::vector<std::string>& args = sess->getArgs();

    uint32_t storeId;
    Expected<uint64_t> exptStoreId = ::tendisplus::stoul(args[1]);
    if (!exptStoreId.ok()) {
      return exptStoreId.status();
    }
    storeId = (uint32_t)exptStoreId.value();

    auto svr = sess->getServerEntry();
    INVARIANT(svr != nullptr);
    if (storeId >= svr->getKVStoreCount()) {
      return {ErrorCodes::ERR_PARSEOPT, "invalid storeId"};
    }
    if (svr->isClusterEnabled()) {
      auto migrateMgr = svr->getMigrateManager();
      INVARIANT(migrateMgr != nullptr);

      auto ret = migrateMgr->onRestoreEnd(storeId);
      if (!ret.ok()) {
        return ret;
      }
    }
    return Command::fmtOK();
  }
} restoreEndCmd;


class BinlogHeartbeatCommand : public Command {
 public:
  BinlogHeartbeatCommand() : Command("binlog_heartbeat", "as") {}

  ssize_t arity() const {
    return -2;
  }

  int32_t firstkey() const {
    return 0;
  }

  int32_t lastkey() const {
    return 0;
  }

  int32_t keystep() const {
    return 0;
  }

  // binlog_heartbeat storeId [binlogts]
  Expected<std::string> run(Session* sess) final {
    const std::vector<std::string>& args = sess->getArgs();

    uint32_t storeId;
    uint64_t binlogTs = 0;

    if (sess->getArgs().size() != 2 && sess->getArgs().size() != 3) {
      // Forward compatible: before 2.0.6, there is no `binlogts` in
      // binlog_heartbeat
      return {ErrorCodes::ERR_PARSEOPT, "invalid parameter count"};
    }

    Expected<uint64_t> exptStoreId = ::tendisplus::stoul(args[1]);
    RET_IF_ERR_EXPECTED(exptStoreId);

    if (sess->getArgs().size() > 2) {
      auto eBinlogts = ::tendisplus::stoul(args[2]);
      RET_IF_ERR_EXPECTED(eBinlogts);

      binlogTs = eBinlogts.value();
    }

    auto svr = sess->getServerEntry();
    INVARIANT(svr != nullptr);
    if (exptStoreId.value() >= svr->getKVStoreCount()) {
      return {ErrorCodes::ERR_PARSEOPT, "invalid storeId"};
    }
    storeId = (uint32_t)exptStoreId.value();

    auto replMgr = svr->getReplManager();
    INVARIANT(replMgr != nullptr);

    Status s =
      replMgr->applyRepllogV2(sess, storeId, "", ::tendisplus::ultos(binlogTs));
    if (!s.ok()) {
      return s;
    }
    return Command::fmtOK();
  }
} binlogHeartbeatCmd;

class MigateHeartbeatCommand : public Command {
 public:
  MigateHeartbeatCommand() : Command("migrate_heartbeat", "as") {}

  ssize_t arity() const {
    return 3;
  }

  int32_t firstkey() const {
    return 0;
  }

  int32_t lastkey() const {
    return 0;
  }

  int32_t keystep() const {
    return 0;
  }

  // migate_heartbeat storeId
  Expected<std::string> run(Session* sess) final {
    const std::vector<std::string>& args = sess->getArgs();

    uint32_t storeId;
    Expected<uint64_t> exptStoreId = ::tendisplus::stoul(args[1]);
    if (!exptStoreId.ok()) {
      return exptStoreId.status();
    }

    auto svr = sess->getServerEntry();
    INVARIANT(svr != nullptr);
    if (exptStoreId.value() >= svr->getKVStoreCount()) {
      return {ErrorCodes::ERR_PARSEOPT, "invalid storeId"};
    }
    storeId = (uint32_t)exptStoreId.value();

    auto migMgr = svr->getMigrateManager();
    INVARIANT(migMgr != nullptr);

    // LOCK_IS first
    auto expdb =
      svr->getSegmentMgr()->getDb(sess, storeId, mgl::LockMode::LOCK_IS);
    if (!expdb.ok()) {
      return expdb.status();
    }
    std::string taskId = args[2];
    Status s = migMgr->applyRepllog(
      sess, storeId, BinlogApplyMode::NEW_BINLOG_ID, "", taskId);

    if (!s.ok()) {
      return s;
    }
    return Command::fmtOK();
  }
} migrateHeartbeatCmd;

class SlaveofCommand : public Command {
 public:
  SlaveofCommand() : Command("slaveof", "ast") {}

  Expected<std::string> runSlaveofSomeOne(Session* sess) {
    auto svr = sess->getServerEntry();
    INVARIANT(svr != nullptr);
    const auto& args = sess->getArgs();
    auto replMgr = svr->getReplManager();
    INVARIANT(replMgr != nullptr);

    std::string ip = args[1];
    uint32_t port;
    try {
      port = std::stoul(args[2]);
    } catch (std::exception& ex) {
      return {ErrorCodes::ERR_PARSEPKT, ex.what()};
    }
    if (args.size() == 3) {
      // NOTE(takenliu): ensure automic operation for all store
      std::list<Expected<DbWithLock>> expdbList;
      for (uint32_t i = 0; i < svr->getKVStoreCount(); ++i) {
        auto expdb =
          svr->getSegmentMgr()->getDb(sess, i, mgl::LockMode::LOCK_X, true);
        if (!expdb.ok()) {
          return expdb.status();
        }
        if (!expdb.value().store->isOpen()) {
          // NOTE(takenliu): only DestroyStoreCommand will set isOpen
          // be false, and it's unuse.
          return {ErrorCodes::ERR_INTERNAL, "store not open"};
        }
        if (ip != "" && !expdb.value().store->isEmpty(true)) {
          return {ErrorCodes::ERR_MANUAL, "store not empty"};
        }
        expdbList.push_back(std::move(expdb));
      }
      for (uint32_t i = 0; i < svr->getKVStoreCount(); ++i) {
        Status s = replMgr->changeReplSourceInLock(i, ip, port, i);
        if (!s.ok()) {
          return s;
        }
      }
      return Command::fmtOK();
    } else if (args.size() == 5) {
      uint32_t storeId;
      uint32_t sourceStoreId;
      try {
        storeId = std::stoul(args[3]);
        sourceStoreId = std::stoul(args[4]);
      } catch (std::exception& ex) {
        return {ErrorCodes::ERR_PARSEPKT, ex.what()};
      }
      if (storeId >= svr->getKVStoreCount() ||
          sourceStoreId >= svr->getKVStoreCount()) {
        return {ErrorCodes::ERR_PARSEPKT, "invalid storeId"};
      }
      Status s =
        replMgr->changeReplSource(sess, storeId, ip, port, sourceStoreId);
      if (!s.ok()) {
        return s;
      }
      return Command::fmtOK();
    } else {
      return {ErrorCodes::ERR_PARSEPKT, "bad argument num"};
    }
  }

  Expected<std::string> runSlaveofNoOne(Session* sess) {
    auto svr = sess->getServerEntry();
    INVARIANT(svr != nullptr);
    const auto& args = sess->getArgs();
    auto replMgr = svr->getReplManager();
    INVARIANT(replMgr != nullptr);

    if (args.size() == 4) {
      uint32_t storeId = 0;
      try {
        storeId = std::stoul(args[3]);
      } catch (std::exception& ex) {
        return {ErrorCodes::ERR_PARSEPKT, ex.what()};
      }
      if (storeId >= svr->getKVStoreCount()) {
        return {ErrorCodes::ERR_PARSEPKT, "invalid storeId"};
      }

      Status s = replMgr->changeReplSource(sess, storeId, "", 0, 0);
      if (!s.ok()) {
        return s;
      }
      return Command::fmtOK();
    } else {
      for (uint32_t i = 0; i < svr->getKVStoreCount(); ++i) {
        Status s = replMgr->changeReplSource(sess, i, "", 0, 0);
        if (!s.ok()) {
          return s;
        }
      }
      return Command::fmtOK();
    }
  }

  // slaveof no one
  // slaveof no one myStoreId
  // slaveof ip port
  // slaveof ip port myStoreId sourceStoreId
  Expected<std::string> run(Session* sess) final {
    const auto& args = sess->getArgs();
    INVARIANT(args.size() >= size_t(3));
    LOG(INFO) << "SlaveofCommand, " << sess->getCmdStr();
    if (toLower(args[1]) == "no" && toLower(args[2]) == "one") {
      return runSlaveofNoOne(sess);
    } else {
      return runSlaveofSomeOne(sess);
    }
  }

  ssize_t arity() const {
    return -3;
  }

  int32_t firstkey() const {
    return 0;
  }

  int32_t lastkey() const {
    return 0;
  }

  int32_t keystep() const {
    return 0;
  }
} slaveofCommand;

class ReplStatusCommand : public Command {
 public:
  ReplStatusCommand() : Command("replstatus", "as") {}

  ssize_t arity() const {
    return 2;
  }

  int32_t firstkey() const {
    return 0;
  }

  int32_t lastkey() const {
    return 0;
  }

  int32_t keystep() const {
    return 0;
  }

  Expected<std::string> run(Session* sess) final {
    const std::vector<std::string>& args = sess->getArgs();

    uint32_t storeId;
    Expected<uint64_t> exptStoreId = ::tendisplus::stoul(args[1]);
    if (!exptStoreId.ok()) {
      return exptStoreId.status();
    }

    auto svr = sess->getServerEntry();
    INVARIANT(svr != nullptr);
    if (exptStoreId.value() >= svr->getKVStoreCount()) {
      return {ErrorCodes::ERR_PARSEOPT, "invalid storeId"};
    }
    storeId = (uint32_t)exptStoreId.value();

    auto catalog = svr->getCatalog();
    INVARIANT(catalog != nullptr);
    Expected<std::unique_ptr<StoreMeta>> meta = catalog->getStoreMeta(storeId);
    if (!meta.ok()) {
      return meta.status();
    }
    return Command::fmtLongLong((uint8_t)meta.value().get()->replState);
  }
} replStatusCommand;

class PsyncCommand : public Command {
 public:
  PsyncCommand() : Command("psync", "ars") {}

  ssize_t arity() const {
    return 4;
  }

  int32_t firstkey() const {
    return 0;
  }

  int32_t lastkey() const {
    return 0;
  }

  int32_t keystep() const {
    return 0;
  }

  bool isBgCmd() const {
    return true;
  }

  Expected<std::string> run(Session* sess) final {
    LOG(FATAL) << "psync should not be called";
    // void compiler complain
    return {ErrorCodes::ERR_INTERNAL, "shouldn't be called"};
  }
} PsyncCommand;

}  // namespace tendisplus
