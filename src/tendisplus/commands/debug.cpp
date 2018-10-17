#include <string>
#include <sstream>
#include <utility>
#include <memory>
#include <algorithm>
#include <cctype>
#include <clocale>
#include <vector>
#include <set>
#include <list>
#include "glog/logging.h"
#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

class ShowCommand: public Command {
 public:
    ShowCommand()
        :Command("show") {
    }

    ssize_t arity() const {
        return -1;
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

    // show processlist [all]
    Expected<std::string> processList(Session *sess, bool all) {
        auto svr = sess->getServerEntry();
        INVARIANT(svr != nullptr);
        auto sesses = svr->getAllSessions();

        rapidjson::StringBuffer sb;
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(sb);
        writer.StartArray();
        for (const auto& sess : sesses) {
            SessionCtx *ctx = sess->getCtx();
            std::vector<std::string> args = ctx->getArgsBrief();
            if (args.size() == 0 && !all) {
                continue;
            }
            writer.StartObject();

            writer.Key("args");
            writer.StartArray();
            for (const auto& arg : args) {
                writer.String(arg);
            }
            writer.EndArray();

            writer.Key("authed");
            writer.Uint64(ctx->authed());

            writer.Key("dbid");
            writer.Uint64(ctx->getDbId());

            writer.Key("start_time");
            writer.Uint64(ctx->getProcessPacketStart()/1000000);

            writer.Key("session_ref");
            writer.Uint64(sess.use_count());

            SLSP waitlockStat = ctx->getWaitlock();
            if (waitlockStat.second != mgl::LockMode::LOCK_NONE) {
                writer.Key("waitlock");
                writer.StartArray();
                writer.Uint64(waitlockStat.first);
                writer.String(lockModeRepr(waitlockStat.second));
                writer.EndArray();
            }
            std::list<SLSP> lockStats = ctx->getLockStates();
            if (lockStats.size() > 0) {
                writer.Key("holdinglocks");
                writer.StartArray();
                for (const auto& v : lockStats) {
                    writer.StartArray();
                    writer.Uint64(v.first);
                    writer.String(lockModeRepr(v.second));
                    writer.EndArray();
                }
                writer.EndArray();
            }
            writer.EndObject();
        }
        writer.EndArray();
        return Command::fmtBulk(sb.GetString());
    }

    Expected<std::string> run(Session *sess) final {
        const std::vector<std::string>& args = sess->getArgs();
        if (args[1] == "processlist") {
            if (args.size() == 2) {
                return processList(sess, false);
            } else if (args.size() == 3) {
                if (args[2] == "all") {
                    return processList(sess, true);
                }
                return {ErrorCodes::ERR_PARSEOPT, "invalid param"};
            }
            return {ErrorCodes::ERR_PARSEOPT, "invalid param"};
        }
        return {ErrorCodes::ERR_PARSEOPT, "invalid show param"};
    }
} plCmd;

class ToggleFtmcCommand: public Command {
 public:
    ToggleFtmcCommand()
        :Command("toggleftmc") {
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
        bool enable = false;
        if (args[1] == "1") {
            enable = true;
        } else if (args[1] == "0") {
            enable = false;
        } else {
            return {ErrorCodes::ERR_PARSEOPT, "invalid toggleftmc para"};
        }
        std::shared_ptr<ServerEntry> svr = sess->getServerEntry();
        INVARIANT(svr != nullptr);
        svr->toggleFtmc(enable);
        return Command::fmtOK();
    }
} togFtmcCmd;

class CommandListCommand: public Command {
 public:
    CommandListCommand()
         :Command("commandlist") {
    }

    ssize_t arity() const {
        return 1;
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
        const auto& cmds = listCommands();
        std::stringstream ss;
        Command::fmtMultiBulkLen(ss, cmds.size());
        for (const auto& cmd : cmds) {
            Command::fmtBulk(ss, cmd);
        }
        return ss.str();
    }
} cmdList;

class BinlogPosCommand: public Command {
 public:
    BinlogPosCommand()
        :Command("binlogpos") {
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
        const std::vector<std::string>& args = sess->getArgs();
        Expected<uint64_t> storeId = ::tendisplus::stoul(args[1]);
        if (!storeId.ok()) {
            return storeId.status();
        }
        if (storeId.value() >= KVStore::INSTANCE_NUM) {
            return {ErrorCodes::ERR_PARSEOPT, "invalid instance num"};
        }
        StoreLock storeLock(storeId.value(), mgl::LockMode::LOCK_IS, sess);
        PStore kvstore = Command::getStoreById(sess, storeId.value());
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        std::unique_ptr<BinlogCursor> cursor =
            txn->createBinlogCursor(Transaction::MIN_VALID_TXNID);
        cursor->seekToLast();
        Expected<ReplLog> explog = cursor->next();
        if (!explog.ok()) {
            return explog.status();
        }
        return Command::fmtLongLong(explog.value().getReplLogKey().getTxnId());
    }
} binlogPosCommand;

class DebugCommand: public Command {
 public:
    DebugCommand()
        :Command("debug") {
    }

    ssize_t arity() const {
        return -1;
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
        std::set<std::string> sections;
        if (args.size() == 1) {
            sections.insert("stores");
            sections.insert("repl");
            sections.insert("commands");
            sections.insert("unseen_commands");
        } else {
            for (size_t i = 2; i < args.size(); ++i) {
                sections.insert(args[i]);
            }
        }
        std::shared_ptr<ServerEntry> svr = sess->getServerEntry();
        INVARIANT(svr != nullptr);
        const SegmentMgr *segMgr = svr->getSegmentMgr();
        INVARIANT(segMgr != nullptr);
        ReplManager *replMgr = svr->getReplManager();
        INVARIANT(replMgr != nullptr);

        rapidjson::StringBuffer sb;
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(sb);
        writer.StartObject();

        if (sections.find("stores") != sections.end()) {
            writer.Key("stores");
            writer.StartObject();
            for (uint32_t i = 0; i < KVStore::INSTANCE_NUM; ++i) {
                PStore store = segMgr->getInstanceById(i);
                std::stringstream ss;
                ss << "stores_" << i;
                writer.Key(ss.str().c_str());
                writer.StartObject();
                store->appendJSONStat(writer);
                writer.EndObject();
            }
            writer.EndObject();
        }
        if (sections.find("repl") != sections.end()) {
            writer.Key("repl");
            writer.StartObject();
            replMgr->appendJSONStat(writer);
            writer.EndObject();
        }
        if (sections.find("unseen_commands") != sections.end()) {
            writer.Key("unseen_commands");
            writer.StartObject();
            std::lock_guard<std::mutex> lk(Command::_mutex);
            for (const auto& kv : _unSeenCmds) {
                writer.Key(kv.first);
                writer.Uint64(kv.second);
            }
            writer.EndObject();
        }
        if (sections.find("commands") != sections.end()) {
            writer.Key("commands");
            writer.StartObject();
            for (const auto& kv : commandMap()) {
                writer.Key(kv.first);
                writer.StartObject();
                writer.Key("call_times");
                writer.Uint64(kv.second->getCallTimes());
                writer.Key("total_nanos");
                writer.Uint64(kv.second->getNanos());
                writer.EndObject();
            }
            writer.EndObject();
        }
        writer.EndObject();
        return Command::fmtBulk(std::string(sb.GetString()));
    }
} debugCommand;

}  // namespace tendisplus
