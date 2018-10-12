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

namespace tendisplus {

class FullSyncCommand: public Command {
 public:
    FullSyncCommand()
        :Command("fullsync") {
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
        LOG(FATAL) << "fullsync should not be called";
        // void compiler complain
        return {ErrorCodes::ERR_INTERNAL, "shouldn't be called"};
    }
} fullSyncCommand;

class IncrSyncCommand: public Command {
 public:
    IncrSyncCommand()
        :Command("incrsync") {
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

    // incrSync storeId dstStoreId binlogId
    // binlogId: the last binlog that has been applied
    Expected<std::string> run(Session *sess) final {
        LOG(FATAL) << "incrsync should not be called";

        // void compiler complain
        return {ErrorCodes::ERR_INTERNAL, "shouldn't be called"};
    }
} incrSyncCommand;

class ApplyBinlogsCommand: public Command {
 public:
    ApplyBinlogsCommand()
        :Command("applybinlogs") {
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
        if (exptStoreId.value() >= KVStore::INSTANCE_NUM) {
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
                    << " last record not marked begin";
            }
        }

        std::shared_ptr<ServerEntry> svr = sess->getServerEntry();
        INVARIANT(svr != nullptr);
        auto replMgr = svr->getReplManager();
        INVARIANT(replMgr != nullptr);

        StoreLock storeLock(storeId, mgl::LockMode::LOCK_IX);
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

class SlaveofCommand: public Command {
 public:
    SlaveofCommand()
        :Command("slaveof") {
    }

    Expected<std::string> runSlaveofSomeOne(Session* sess) {
        std::shared_ptr<ServerEntry> svr = sess->getServerEntry();
        INVARIANT(svr != nullptr);
        const auto& args = sess->getArgs();
        auto replMgr = svr->getReplManager();
        INVARIANT(replMgr != nullptr);

        std::string ip = args[1];
        uint64_t port;
        try {
            port = std::stoul(args[2]);
        } catch (std::exception& ex) {
            return {ErrorCodes::ERR_PARSEPKT, ex.what()};
        }
        if (args.size() == 3) {
            for (uint32_t i = 0; i < KVStore::INSTANCE_NUM; ++i) {
                StoreLock storeLock(i, mgl::LockMode::LOCK_X);
                Status s = replMgr->changeReplSource(i, ip, port, i);
                if (!s.ok()) {
                    return s;
                }
            }
            return Command::fmtOK();
        } else if (args.size() == 5) {
            uint64_t storeId;
            uint64_t sourceStoreId;
            try {
                storeId = std::stoul(args[3]);
                sourceStoreId = std::stoul(args[4]);
            } catch (std::exception& ex) {
                return {ErrorCodes::ERR_PARSEPKT, ex.what()};
            }
            if (storeId >= KVStore::INSTANCE_NUM ||
                    sourceStoreId >= KVStore::INSTANCE_NUM) {
                return {ErrorCodes::ERR_PARSEPKT, "invalid storeId"};
            }

            StoreLock storeLock(storeId, mgl::LockMode::LOCK_X);
            Status s = replMgr->changeReplSource(
                    storeId, ip, port, sourceStoreId);
            if (s.ok()) {
                return Command::fmtOK();
            }
            return s;
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
            uint64_t storeId = 0;
            try {
                storeId = std::stoul(args[3]);
            } catch (std::exception& ex) {
                return {ErrorCodes::ERR_PARSEPKT, ex.what()};
            }
            if (storeId >= KVStore::INSTANCE_NUM) {
                return {ErrorCodes::ERR_PARSEPKT, "invalid storeId"};
            }

            StoreLock storeLock(storeId, mgl::LockMode::LOCK_X);
            Status s = replMgr->changeReplSource(storeId, "", 0, 0);
            if (s.ok()) {
                return Command::fmtOK();
            }
            return s;
        } else {
            for (uint32_t i = 0; i < KVStore::INSTANCE_NUM; ++i) {
                StoreLock storeLock(i, mgl::LockMode::LOCK_X);
                Status s = replMgr->changeReplSource(i, "", 0, 0);
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

}  // namespace tendisplus
