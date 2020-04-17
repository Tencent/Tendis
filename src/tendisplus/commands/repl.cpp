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

namespace tendisplus {

class BackupCommand: public Command {
 public:
    BackupCommand()
        :Command("backup", "a") {
    }

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

    Expected<std::string> run(Session *sess) final {
        const std::string& dir = sess->getArgs()[1];
        auto mode = KVStore::BackupMode::BACKUP_COPY;
        if (sess->getArgs().size() >= 3) {
            const std::string &str_mode = toLower(sess->getArgs()[2]);
            if (str_mode == "ckpt") {
                mode = KVStore::BackupMode::BACKUP_CKPT;
            } else if (str_mode == "copy") {
                mode = KVStore::BackupMode::BACKUP_COPY;
            } else {
                return {ErrorCodes::ERR_MANUAL, "mode error, should be ckpt or copy"};
            }
        }
        std::shared_ptr<ServerEntry> svr = sess->getServerEntry();
        INVARIANT(svr != nullptr);
        if (!filesystem::exists(dir)) {
            return {ErrorCodes::ERR_MANUAL, "dir not exist:" + dir};
        }
        if (filesystem::equivalent(dir, svr->getParams()->dbPath)) {
            return {ErrorCodes::ERR_MANUAL, "dir cant be dbPath:" + dir};
        }

        for (uint32_t i = 0; i < svr->getKVStoreCount(); ++i) {
            // NOTE(deyukong): here we acquire IS lock
            auto expdb = svr->getSegmentMgr()->getDb(sess, i,
                mgl::LockMode::LOCK_IS, true);
            if (!expdb.ok()) {
                return expdb.status();
            }

            auto store = std::move(expdb.value().store);
            // if store is not open, skip it
            if (!store->isOpen()) {
                continue;
            }
            std::string dbdir = dir + "/" + std::to_string(i) + "/";
            Expected<BackupInfo> bkInfo = store->backup(
                dbdir, mode);
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
    RestoreBackupCommand()
        :Command("restorebackup", "aw") {
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

    // restorebackup "all"|storeId dir
    // restorebackup "all"|storeId dir force
    Expected<std::string> run(Session *sess) final {
        std::shared_ptr<ServerEntry> svr = sess->getServerEntry();
        INVARIANT(svr != nullptr);
        const std::string& kvstore = sess->getArgs()[1];
        const std::string& dir = sess->getArgs()[2];

        bool isForce = false;
        if (sess->getArgs().size() >= 4) {
            isForce = sess->getArgs()[3] == "force";
        }
        if (kvstore == "all") {
            for (uint32_t i = 0; i < svr->getKVStoreCount(); ++i) {
                if (!isForce && !isEmpty(svr, sess, i)) {
                    return {ErrorCodes::ERR_INTERNAL, "not empty. use force please"};
                }
            }
            for (uint32_t i = 0; i < svr->getKVStoreCount(); ++i) {
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
            auto ret = restoreBackup(svr, sess, storeId, dir);
            if (!ret.ok()) {
                return ret.status();
            }
        }
        return Command::fmtOK();
    }

 private:
    bool isEmpty(std::shared_ptr<ServerEntry> svr, Session *sess, uint32_t storeId){
         // IS lock
        auto expdb = svr->getSegmentMgr()->getDb(sess, storeId,
            mgl::LockMode::LOCK_IS, true);
        if (!expdb.ok()) {
            return false;
        }
        auto store = std::move(expdb.value().store);
        return store->isEmpty();
    }

    Expected<std::string> restoreBackup(std::shared_ptr<ServerEntry> svr,
        Session *sess, uint32_t storeId, const std::string& dir) {
        // X lock
        auto expdb = svr->getSegmentMgr()->getDb(sess, storeId,
            mgl::LockMode::LOCK_X, true);
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
        Status clearStatus =  store->clear();
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
        uint64_t binlogpos = Transaction::TXNID_UNINITED;
        for (auto& o : backup_meta.value().GetObject()) {
            if (o.name == "binlogpos" && o.value.IsUint()) {
                binlogpos = o.value.GetUint64();
            }
        }
        if (binlogpos == Transaction::TXNID_UNINITED) {
            LOG(ERROR) << "binlogpos is invalid " << dir;
        }

        Expected<uint64_t> restartStatus = store->restart(false, Transaction::MIN_VALID_TXNID, binlogpos);
        if (!restartStatus.ok()) {
            INVARIANT_D(0);
            LOG(ERROR) << "restoreBackup restart store:" << storeId
                   << ",failed:" << restartStatus.status().toString();
            return {ErrorCodes::ERR_INTERNAL, "restart failed."};
        }

        return Command::fmtOK();
    }
} restoreBackupCommand;

// fullSync storeId dstStoreId ip port
class FullSyncCommand: public Command {
 public:
    FullSyncCommand()
        :Command("fullsync", "a") {
    }

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

    Expected<std::string> run(Session *sess) final {
        LOG(FATAL) << "fullsync should not be called";
        // void compiler complain
        return {ErrorCodes::ERR_INTERNAL, "shouldn't be called"};
    }
} fullSyncCommand;

class ToggleIncrSyncCommand: public Command {
 public:
    ToggleIncrSyncCommand()
        :Command("toggleincrsync", "a") {
    }

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

    Expected<std::string> run(Session *sess) final {
        Expected<uint64_t> state = ::tendisplus::stoul(sess->getArgs()[1]);
        if (!state.ok()) {
            return state.status();
        }
        LOG(INFO) << "toggle incrsync state to:" << state.value();
        std::shared_ptr<ServerEntry> svr = sess->getServerEntry();
        INVARIANT(svr != nullptr);
        auto replMgr = svr->getReplManager();
        INVARIANT(replMgr != nullptr);
        replMgr->togglePauseState(state.value() ? false : true);
        return Command::fmtOK();
    }
} toggleIncrSyncCmd;

class IncrSyncCommand: public Command {
 public:
    IncrSyncCommand()
        :Command("incrsync", "a") {
    }

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

    // incrSync storeId dstStoreId binlogId ip port
    // binlogId: the last binlog that has been applied
    Expected<std::string> run(Session *sess) final {
        LOG(FATAL) << "incrsync should not be called";

        // void compiler complain
        return {ErrorCodes::ERR_INTERNAL, "shouldn't be called"};
    }
} incrSyncCommand;

#ifdef BINLOG_V1
// @input pullbinlogs storeId startBinlogId
// @output nextBinlogId [[k,v], [k,v]...]
class PullBinlogsCommand: public Command {
 public:
    PullBinlogsCommand()
        :Command("pullbinlogs", "a") {
    }

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

    Expected<std::string> run(Session *sess) final {
        uint64_t storeId;
        uint64_t binlogPos;
        Expected<uint64_t> exptStoreId =
            ::tendisplus::stoul(sess->getArgs()[1]);
        if (!exptStoreId.ok()) {
            return exptStoreId.status();
        }
        Expected<uint64_t> exptBinlogId =
            ::tendisplus::stoul(sess->getArgs()[2]);
        if (!exptBinlogId.ok()) {
            return exptBinlogId.status();
        }
        binlogPos = exptBinlogId.value();

        auto server = sess->getServerEntry();
        if (exptStoreId.value() >= server->getKVStoreCount()) {
            return {ErrorCodes::ERR_PARSEOPT, "invalid storeId"};
        }
        storeId = exptStoreId.value();
        auto expdb = server->getSegmentMgr()->getDb(sess, storeId,
            mgl::LockMode::LOCK_IS);
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
        std::unique_ptr<BinlogCursor> cursor =
            txn->createBinlogCursor(binlogPos);

        std::vector<ReplLog> binlogs;
        uint64_t currId = Transaction::TXNID_UNINITED;
        while (true) {
            Expected<ReplLog> explog = cursor->next();
            if (!explog.ok()) {
                if (explog.status().code() != ErrorCodes::ERR_EXHAUST) {
                    return explog.status();
                }
                break;
            }
            uint64_t tmpId = explog.value().getReplLogKey().getTxnId();
            if (currId == Transaction::TXNID_UNINITED) {
                currId = tmpId;
            }
            if (binlogs.size() >= 1000 && tmpId != currId) {
                break;
            }
            binlogs.push_back(std::move(explog.value()));
            currId = tmpId;
        }
        std::stringstream ss;
        if (binlogs.size() == 0) {
            Command::fmtMultiBulkLen(ss, 2);
            Command::fmtLongLong(ss, binlogPos);
            Command::fmtMultiBulkLen(ss, 0);
        } else {
            Command::fmtMultiBulkLen(ss, 2*binlogs.size()+1);
            Command::fmtLongLong(ss,
                binlogs.back().getReplLogKey().getTxnId()+1);
            for (const auto& v : binlogs) {
                ReplLog::KV kv = v.encode();
                Command::fmtBulk(ss, kv.first);
                Command::fmtBulk(ss, kv.second);
            }
        }
        return ss.str();
    }
} pullBinlogsCommand;

class RestoreBinlogCommand: public Command {
 public:
    RestoreBinlogCommand()
        :Command("restorebinlog", "a") {
    }

    ssize_t arity() const {
        return -4;
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

    // restorebinlog storeId k1 v1 k2 v2 ...
    Expected<std::string> run(Session *sess) final {
        const std::vector<std::string>& args = sess->getArgs();
        if (args.size() % 2 != 0) {
            return {ErrorCodes::ERR_PARSEOPT, "invalid param len"};
        }
        uint64_t storeId;
        Expected<uint64_t> exptStoreId = ::tendisplus::stoul(args[1]);
        if (!exptStoreId.ok()) {
            return exptStoreId.status();
        }
        storeId = exptStoreId.value();
        auto server = sess->getServerEntry();
        if (storeId >= server->getKVStoreCount()) {
            return {ErrorCodes::ERR_PARSEOPT, "invalid storeid"};
        }
        std::vector<ReplLog> logs;
        for (size_t i = 2; i < args.size(); i+=2) {
            auto kv = ReplLog::decode(args[i], args[i+1]);
            if (!kv.ok()) {
                return kv.status();
            }
            logs.push_back(std::move(kv.value()));
        }
        uint64_t txnId = logs.front().getReplLogKey().getTxnId();
        for (const auto& kv : logs) {
            if (kv.getReplLogKey().getTxnId() != txnId) {
                return {ErrorCodes::ERR_PARSEOPT, "txn id not all the same"};
            }
        }

        auto expdb = server->getSegmentMgr()->getDb(sess, storeId,
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
        for (const auto& kv : logs) {
            // NOTE(vinchen): It don't need to get the timestamp of binlog
            // for restorebinlog, because it isn't under the mode of
            // REPLICATE_ONLY
            uint64_t timestamp = 0;

            Expected<RecordKey> expRk =
                RecordKey::decode(kv.getReplLogValue().getOpKey());
            if (!expRk.ok()) {
                return expRk.status();
            }

            switch (kv.getReplLogValue().getOp()) {
                case (ReplOp::REPL_OP_SET): {
                    Expected<RecordValue> expRv =
                        RecordValue::decode(kv.getReplLogValue().getOpValue());
                    if (!expRv.ok()) {
                        return expRv.status();
                    }
                    auto s = txn->setKV(expRk.value().encode(),
                        expRv.value().encode(), timestamp);
                    if (!s.ok()) {
                        return {ErrorCodes::ERR_INTERNAL, s.toString()};
                    } else {
                        break;
                    }
                }
                case (ReplOp::REPL_OP_DEL): {
                    auto s = txn->delKV(expRk.value().encode(), timestamp);
                    if (!s.ok()) {
                        return {ErrorCodes::ERR_INTERNAL, s.toString()};
                    } else {
                        break;
                    }
                }
                default: {
                    return {ErrorCodes::ERR_PARSEOPT, "invalid replop"};
                }
            }
        }
        auto commitId = txn->commit();
        if (commitId.ok()) {
            return Command::fmtOK();
        }
        return commitId.status();
    }
} restoreBinlogCmd;

class ApplyBinlogsCommand: public Command {
 public:
    ApplyBinlogsCommand()
        :Command("applybinlogs", "a") {
    }

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

    // applybinlogs storeId [k0 v0] [k1 v1] ...
    // why is there no storeId ? storeId is contained in this
    // session in fact.
    // please refer to comments of ReplManager::registerIncrSync
    Expected<std::string> run(Session *sess) final {
        const std::vector<std::string>& args = sess->getArgs();
        std::map<uint64_t, std::list<ReplLog>> binlogGroup;

        uint64_t storeId;
        Expected<uint64_t> exptStoreId = ::tendisplus::stoul(args[1]);
        if (!exptStoreId.ok()) {
            return exptStoreId.status();
        }

        auto svr = sess->getServerEntry();
        INVARIANT(svr != nullptr);
        if (exptStoreId.value() >= svr->getKVStoreCount()) {
            return {ErrorCodes::ERR_PARSEOPT, "invalid storeId"};
        }
        storeId = exptStoreId.value();

        for (size_t i = 2; i < args.size(); i+=2) {
            Expected<ReplLog> logkv = ReplLog::decode(args[i], args[i+1]);
            if (!logkv.ok()) {
                return logkv.status();
            }
            const ReplLogKey& logKey = logkv.value().getReplLogKey();
            uint64_t txnId  = logKey.getTxnId();
            if (binlogGroup.find(txnId) == binlogGroup.end()) {
                binlogGroup[txnId] = std::list<ReplLog>();
            }
            binlogGroup[txnId].emplace_back(std::move(logkv.value()));
        }
        for (const auto& logList : binlogGroup) {
            INVARIANT(logList.second.size() >= 1);
            const ReplLogKey& firstLogKey =
                              logList.second.begin()->getReplLogKey();
            const ReplLogKey& lastLogKey =
                              logList.second.rbegin()->getReplLogKey();
            if (!(static_cast<uint16_t>(firstLogKey.getFlag()) &
                    static_cast<uint16_t>(ReplFlag::REPL_GROUP_START))) {
                LOG(FATAL) << "txnId:" << firstLogKey.getTxnId()
                    << " first record not marked begin";
            }
            if (!(static_cast<uint16_t>(lastLogKey.getFlag()) &
                    static_cast<uint16_t>(ReplFlag::REPL_GROUP_END))) {
                LOG(FATAL) << "txnId:" << lastLogKey.getTxnId()
                    << " last record not marked end";
            }
        }

        auto replMgr = svr->getReplManager();
        INVARIANT(replMgr != nullptr);

        auto expdb = svr->getSegmentMgr()->getDb(sess, storeId,
            mgl::LockMode::LOCK_IX);
        if (!expdb.ok()) {
            return expdb.status();
        }
        Status s = replMgr->applyBinlogs(storeId,
                                         sess->id(),
                                         binlogGroup);
        if (s.ok()) {
            return Command::fmtOK();
        } else {
            return s;
        }
    }
} applyBinlogsCommand;
#else
class ApplyBinlogsCommandV2 : public Command {
 public:
    ApplyBinlogsCommandV2()
        :Command("applybinlogsv2", "aw") {
    }

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

    static Status runNormal(Session *sess, uint32_t storeId,
                const std::string& binlogs, size_t binlogCnt) {
        auto svr = sess->getServerEntry();
        auto replMgr = svr->getReplManager();
        INVARIANT(replMgr != nullptr);

        //  ReplManager::applySingleTxnV2() should lock db with LOCK_IX
        auto expdb = svr->getSegmentMgr()->getDb(sess, storeId,
            mgl::LockMode::LOCK_IX);
        if (!expdb.ok()) {
            return expdb.status();
        }
        INVARIANT_D(sess->getCtx()->isReplOnly());

        size_t cnt = 0;
        BinlogReader reader(binlogs);
        while (true) {
            auto eLog = reader.next();
            if (eLog.status().code() == ErrorCodes::ERR_EXHAUST) {
                break;
            } else if (!eLog.ok()) {
                return eLog.status();
            }
            Status s = replMgr->applyRepllogV2(sess, storeId,
                eLog.value().getReplLogKey(), eLog.value().getReplLogValue());
            if (!s.ok()) {
                return s;
            }
            cnt++;
        }

        if (cnt != binlogCnt) {
            return{ ErrorCodes::ERR_PARSEOPT,
                    "invalid binlog size of binlog count" };
        }

        return{ ErrorCodes::ERR_OK, "" };
    }

    static Status runFlush(Session *sess, uint32_t storeId,
        const std::string& binlogs, size_t binlogCnt) {
        auto svr = sess->getServerEntry();
        auto replMgr = svr->getReplManager();
        INVARIANT(replMgr != nullptr);

        if (binlogCnt != 1) {
            return{ ErrorCodes::ERR_PARSEOPT, "invalid binlog count" };
        }

        BinlogReader reader(binlogs);
        auto eLog = reader.nextV2();
        if (!eLog.ok()) {
            return eLog.status();
        }

        if (reader.nextV2().status().code() != ErrorCodes::ERR_EXHAUST) {
            return{ ErrorCodes::ERR_PARSEOPT, "too big flush binlog" };
        }

        LOG(INFO) << "doing flush " << eLog.value().getReplLogValue().getCmd();

        LocalSessionGuard sg(svr);
        sg.getSession()->setArgs({ eLog.value().getReplLogValue().getCmd() });

        auto expdb = svr->getSegmentMgr()->getDb(sg.getSession(), storeId,
            mgl::LockMode::LOCK_X);
        if (!expdb.ok()) {
            return expdb.status();
        }
        // fake the session to be not replonly!
        sg.getSession()->getCtx()->setReplOnly(false);

        // set binlog time before flush,
        // because the flush binlog is logical, not binary
        expdb.value().store->setBinlogTime(
                            eLog.value().getReplLogValue().getTimestamp());
        auto eflush = expdb.value().store->flush(sg.getSession(),
                                    eLog.value().getReplLogKey().getBinlogId());
        if (!eflush.ok()) {
            return eflush.status();
        }
        INVARIANT_D(eflush.value() ==
                    eLog.value().getReplLogKey().getBinlogId());

        replMgr->onFlush(storeId, eflush.value());
        return{ ErrorCodes::ERR_OK, "" };
    }
    // applybinlogsv2 storeId binlogs cnt flag
    // why is there no storeId ? storeId is contained in this
    // session in fact.
    // please refer to comments of ReplManager::registerIncrSync
    Expected<std::string> run(Session *sess) final {
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
            return{ ErrorCodes::ERR_PARSEOPT, "invalid storeId" };
        }

        uint64_t binlogCnt;
        auto exptCnt = ::tendisplus::stoul(args[3]);
        if (!exptCnt.ok()) {
            return exptCnt.status();
        }
        binlogCnt = exptCnt.value();

        auto eflag = ::tendisplus::stoul(args[4]);
        if (!eflag.ok()) {
            return{ ErrorCodes::ERR_PARSEOPT, "invalid binlog flags" };
        }
        switch ((BinlogFlag)eflag.value()) {
        case BinlogFlag::NORMAL: {
            auto s = runNormal(sess, storeId, args[2], binlogCnt);
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
        }
        return Command::fmtOK();
    }
} applyBinlogsV2Command;

class RestoreBinlogCommandV2 : public Command {
 public:
    RestoreBinlogCommandV2()
        :Command("restorebinlogv2", "aw") {
    }

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

    // restorebinlogv2 storeId key(binlogid) value([op key value]*) checksum
    Expected<std::string> run(Session *sess) final {
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
            return{ ErrorCodes::ERR_PARSEOPT, "invalid storeId" };
        }

        std::string key = Base64::Decode(args[2].c_str(), args[2].size());
        std::string value = Base64::Decode(args[3].c_str(), args[3].size());

        auto replMgr = svr->getReplManager();
        INVARIANT(replMgr != nullptr);

        // LOCK_IX first
        auto expdb = svr->getSegmentMgr()->getDb(sess, storeId,
            mgl::LockMode::LOCK_IX);
        if (!expdb.ok()) {
            return expdb.status();
        }

        sess->getCtx()->setReplOnly(true);
        Expected<uint64_t> ret =
            replMgr->applySingleTxnV2(sess, storeId, key, value);
        if (!ret.ok()) {
            return ret.status();
        }

        return Command::fmtOK();
    }
} restoreBinlogV2Command;


class BinlogHeartbeatCommand : public Command {
 public:
    BinlogHeartbeatCommand()
        :Command("binlog_heartbeat", "a") {
    }

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

    // binlog_heartbeat storeId
    Expected<std::string> run(Session *sess) final {
        const std::vector<std::string>& args = sess->getArgs();

        uint32_t storeId;
        Expected<uint64_t> exptStoreId = ::tendisplus::stoul(args[1]);
        if (!exptStoreId.ok()) {
            return exptStoreId.status();
        }

        auto svr = sess->getServerEntry();
        INVARIANT(svr != nullptr);
        if (exptStoreId.value() >= svr->getKVStoreCount()) {
            return{ ErrorCodes::ERR_PARSEOPT, "invalid storeId" };
        }
        storeId = (uint32_t)exptStoreId.value();

        auto replMgr = svr->getReplManager();
        INVARIANT(replMgr != nullptr);

        // LOCK_IS first
        auto expdb = svr->getSegmentMgr()->getDb(sess, storeId,
            mgl::LockMode::LOCK_IS);
        if (!expdb.ok()) {
            return expdb.status();
        }

        Status s = replMgr->applyRepllogV2(sess, storeId, "", "");
        if (!s.ok()) {
            return s;
        }
        return Command::fmtOK();
    }
} binlogHeartbeatCmd;
#endif

class SlaveofCommand: public Command {
 public:
    SlaveofCommand()
        :Command("slaveof", "ast") {
    }

    Expected<std::string> runSlaveofSomeOne(Session* sess) {
        std::shared_ptr<ServerEntry> svr = sess->getServerEntry();
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
                auto expdb = svr->getSegmentMgr()->getDb(sess, i,
                    mgl::LockMode::LOCK_X, true);
                if (!expdb.ok()) {
                    return expdb.status();
                }
                if (!expdb.value().store->isOpen()) {
                    // NOTE(takenliu): only DestroyStoreCommand will set isOpen be false, and it's unuse.
                    //return {ErrorCodes::ERR_OK, ""};
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
            Status s = replMgr->changeReplSource(sess,
                    storeId, ip, port, sourceStoreId);
            if (!s.ok()) {
                return s;
            }
            return Command::fmtOK();
        } else {
            return {ErrorCodes::ERR_PARSEPKT, "bad argument num"};
        }
    }

    Expected<std::string> runSlaveofNoOne(Session* sess) {
        std::shared_ptr<ServerEntry> svr = sess->getServerEntry();
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
    Expected<std::string> run(Session *sess) final {
        const auto& args = sess->getArgs();
        INVARIANT(args.size() >= size_t(3));
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

class ReplStatusCommand: public Command {
 public:
    ReplStatusCommand()
        :Command("replstatus", "a") {
    }

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

    Expected<std::string> run(Session *sess) final {
        const std::vector<std::string>& args = sess->getArgs();

        uint32_t storeId;
        Expected<uint64_t> exptStoreId = ::tendisplus::stoul(args[1]);
        if (!exptStoreId.ok()) {
            return exptStoreId.status();
        }

        auto svr = sess->getServerEntry();
        INVARIANT(svr != nullptr);
        if (exptStoreId.value() >= svr->getKVStoreCount()) {
            return{ ErrorCodes::ERR_PARSEOPT, "invalid storeId" };
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

}  // namespace tendisplus
