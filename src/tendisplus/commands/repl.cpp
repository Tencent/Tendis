#include <string>
#include <utility>
#include <memory>
#include <algorithm>
#include <cctype>
#include <vector>
#include <clocale>
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

    Expected<uint32_t> parse(NetSession *sess) const {
        const auto& args = sess->getArgs();
        if (args.size() != 2) {
            return {ErrorCodes::ERR_PARSEPKT, "invalid  fullsync params"};
        }
        uint64_t storeId = 0;
        try {
            storeId = std::stoul(args[1]);
        } catch (std::exception& ex) {
            return {ErrorCodes::ERR_PARSEPKT, ex.what()};
        }
        if (storeId >= KVStore::INSTANCE_NUM) {
            return {ErrorCodes::ERR_PARSEPKT, "store it outof boundary"};
        }
        return storeId;
    }

    Expected<std::string> run(NetSession *sess) final {
        LOG(FATAL) << "fullsync should not be called";
        // void compiler complain
        return {ErrorCodes::ERR_INTERNAL, "shouldn't be called"};
    }
} fullSyncCommand;

class FetchBinlogCommand: public Command {
 public:
    FetchBinlogCommand()
        :Command("fetchbinlog") {
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

    // fetchbinlog storeId binlogId suggestBatch
    Expected<std::string> run(NetSession *sess) final {
        const std::vector<std::string>& args = sess->getArgs();
        INVARIANT(ssize_t(args.size()) == arity());
        uint64_t storeId;
        uint64_t binlogId;
        uint32_t suggestBatch;
        try {
            storeId = std::stoul(args[1]);
            binlogId = std::stoul(args[2]);
            suggestBatch = std::stoul(args[3]);
        } catch (std::exception& ex) {
            return {ErrorCodes::ERR_PARSEPKT, ex.what()};
        }
        if (storeId >= KVStore::INSTANCE_NUM) {
            return {ErrorCodes::ERR_PARSEPKT, "store it outof boundary"};
        }
        auto svr = sess->getServerEntry();
        INVARIANT(svr != nullptr);
        PStore store = svr->getSegmentMgr()->getInstanceById(storeId);
        INVARIANT(store != nullptr);

        auto ptxn = store->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }

        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        std::unique_ptr<BinlogCursor> cursor =
            txn->createBinlogCursor(binlogId);

        std::vector<ReplLog> binlogs;
        uint32_t cnt = 0;
        uint64_t nowId = 0;
        while (true) {
            Expected<ReplLog> explog = cursor->next();
            if (explog.ok()) {
                cnt += 1;
                const ReplLogKey& rlk = explog.value().getReplLogKey();
                if (nowId == 0 || nowId != rlk.getTxnId()) {
                    nowId = rlk.getTxnId();
                    if (cnt >= suggestBatch) {
                        break;
                    } else {
                        binlogs.emplace_back(std::move(explog.value()));
                    }
                } else {
                    binlogs.emplace_back(std::move(explog.value()));
                }
            } else if (explog.status().code() == ErrorCodes::ERR_EXHAUST) {
                break;
            } else {
                LOG(ERROR) << "iter binlog failed:"
                            << explog.status().toString();
                return explog.status();
            }
        }

        std::stringstream ss;
        Command::fmtMultiBulkLen(ss, binlogs.size()*2);
        for (auto& v : binlogs) {
            ReplLog::KV kv = v.encode();
            Command::fmtBulk(ss, kv.first);
            Command::fmtBulk(ss, kv.second);
        }
        return ss.str();
    }
} fetchBinlogCommand;

class SlaveofCommand: public Command {
 public:
    SlaveofCommand()
        :Command("slaveof") {
    }

    Expected<std::string> runSlaveofSomeOne(NetSession* sess) {
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

    Expected<std::string> runSlaveofNoOne(NetSession* sess) {
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

            Status s = replMgr->changeReplSource(storeId, "", 0, 0);
            if (s.ok()) {
                return Command::fmtOK();
            }
            return s;
        } else {
            for (uint32_t i = 0; i < KVStore::INSTANCE_NUM; ++i) {
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
    Expected<std::string> run(NetSession *sess) final {
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
