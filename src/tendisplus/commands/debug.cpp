#include <sys/time.h>
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

class DbsizeCommand: public Command {
 public:
    DbsizeCommand()
        :Command("dbsize") {
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
        return Command::fmtLongLong(0);
    }
} dbsizeCmd;

class PingCommand: public Command {
 public:
    PingCommand()
        :Command("ping") {
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
        if (sess->getArgs().size() == 1) {
            return Command::fmtBulk("PONG");
        }
        if (sess->getArgs().size() != 2) {
            return {ErrorCodes::ERR_PARSEOPT, "wrong number of arguments"};
        }
        return Command::fmtBulk(sess->getArgs()[1]);
    }
} pingCmd;

class EchoCommand: public Command {
 public:
    EchoCommand()
        :Command("echo") {
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
        return Command::fmtBulk(sess->getArgs()[1]);
    }
} echoCmd;

class TimeCommand: public Command {
 public:
    TimeCommand()
        :Command("time") {
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
        struct timeval tv;
        gettimeofday(&tv, NULL);
        std::stringstream ss;
        Command::fmtMultiBulkLen(ss, 2);
        Command::fmtBulk(ss, std::to_string(tv.tv_sec));
        Command::fmtBulk(ss, std::to_string(tv.tv_usec));
        return ss.str();
    }
} timeCmd;

class IterAllCommand: public Command {
 public:
    IterAllCommand()
        :Command("iterall") {
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

    // @input iterall storeId start_record_hex batchSize
    // @output next_record_hex + listof(type key subkey value)
    Expected<std::string> run(Session *sess) final {
        const std::vector<std::string>& args = sess->getArgs();
        Expected<uint64_t> estoreId = ::tendisplus::stoul(args[1]);
        if (!estoreId.ok()) {
            return estoreId.status();
        }
        Expected<uint64_t> ebatchSize = ::tendisplus::stoul(args[3]);
        if (!ebatchSize.ok()) {
            return ebatchSize.status();
        }
        if (ebatchSize.value() > 10000) {
            return {ErrorCodes::ERR_PARSEOPT, "batch exceed lim"};
        }
        if (estoreId.value() >= KVStore::INSTANCE_NUM) {
            return {ErrorCodes::ERR_PARSEOPT, "invalid store id"};
        }
        auto storeLock = std::make_unique<StoreLock>(
            estoreId.value(), 
            mgl::LockMode::LOCK_IS,
            sess);
        PStore kvstore = Command::getStoreById(sess, estoreId.value());
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        auto cursor = txn->createCursor();
        if (args[2] == "0") {
            cursor->seek("");
        } else {
            auto unhex = unhexlify(args[2]);
            if (!unhex.ok()) {
                return unhex.status();
            }
            cursor->seek(unhex.value());
        }

        std::list<Record> result;
        while (true) {
            if (result.size() >= ebatchSize.value() + 1) {
                break;
            }
            Expected<Record> exptRcd = cursor->next();
            if (exptRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
                break;
            }
            if (!exptRcd.ok()) {
                return exptRcd.status();
            }
            auto type = exptRcd.value().getRecordKey().getRecordType();
            if (type == RecordType::RT_BINLOG) {
                break;
            }
            if (type == RecordType::RT_META ||
                type == RecordType::RT_LIST_META ||
                    type == RecordType::RT_HASH_META ||
                    type == RecordType::RT_SET_META ||
                    type == RecordType::RT_ZSET_META ||
                    type == RecordType::RT_ZSET_S_ELE) {
                continue;
            }
            result.emplace_back(std::move(exptRcd.value()));
        }
        std::string nextCursor;
        if (result.size() == ebatchSize.value() + 1) {
            nextCursor = hexlify(result.back().getRecordKey().encode());
            result.pop_back();
        } else {
            nextCursor = "0";
        }
        std::stringstream ss;
        Command::fmtMultiBulkLen(ss, 2);
        Command::fmtBulk(ss, nextCursor);
        Command::fmtMultiBulkLen(ss, result.size());
        for (const auto& o : result) {
            Command::fmtMultiBulkLen(ss, 4);
            const auto& t = o.getRecordKey().getRecordType();
            Command::fmtBulk(ss, std::to_string(static_cast<uint32_t>(t)));
            switch (t) {
                case RecordType::RT_KV:
                    Command::fmtBulk(ss, o.getRecordKey().getPrimaryKey());
                    Command::fmtBulk(ss, "");
                    Command::fmtBulk(ss, o.getRecordValue().getValue());
                    break;
                case RecordType::RT_HASH_ELE:
                case RecordType::RT_LIST_ELE:
                case RecordType::RT_SET_ELE:
                case RecordType::RT_ZSET_H_ELE:
                    Command::fmtBulk(ss, o.getRecordKey().getPrimaryKey());
                    Command::fmtBulk(ss, o.getRecordKey().getSecondaryKey());
                    Command::fmtBulk(ss, o.getRecordValue().getValue());
                    break;
                default:
                    INVARIANT(0);
            }
        }
        return ss.str();
    }
} iterAllCmd;

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

class BinlogTimeCommand: public Command {
 public:
    BinlogTimeCommand()
        :Command("binlogtime") {
    }

    ssize_t arity() const {
        return 3;
    }

    int32_t firstkey() const {
        return 1;
    }

    int32_t lastkey() const {
        return 1;
    }

    int32_t keystep() const {
        return 1;
    }

    // binlogtime [storeId] [binlogId]
    Expected<std::string> run(Session *sess) final {
        const std::vector<std::string>& args = sess->getArgs();
        Expected<uint64_t> storeId = ::tendisplus::stoul(args[1]);
        if (!storeId.ok()) {
            return storeId.status();
        }
        if (storeId.value() >= KVStore::INSTANCE_NUM) {
            return {ErrorCodes::ERR_PARSEOPT, "invalid instance num"};
        }
        Expected<uint64_t> binlogId = ::tendisplus::stoul(args[2]);
        if (!binlogId.ok()) {
            return binlogId.status();
        }
        StoreLock storeLock(storeId.value(), mgl::LockMode::LOCK_IS, sess);
        PStore kvstore = Command::getStoreById(sess, storeId.value());
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        std::unique_ptr<BinlogCursor> cursor =
            txn->createBinlogCursor(binlogId.value(), true);
        Expected<ReplLog> explog = cursor->next();
        if (!explog.ok()) {
            return explog.status();
        }
        return Command::fmtLongLong(
            explog.value().getReplLogKey().getTimestamp());
    }
} binlogTimeCmd;

class BinlogPosCommand: public Command {
 public:
    BinlogPosCommand()
        :Command("binlogpos") {
    }

    ssize_t arity() const {
        return 2;
    }

    int32_t firstkey() const {
        return 1;
    }

    int32_t lastkey() const {
        return 1;
    }

    int32_t keystep() const {
        return 1;
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
            txn->createBinlogCursor(Transaction::MIN_VALID_TXNID, true);
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
            sections.insert("network");
            sections.insert("request");
            sections.insert("req_pool");
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

        std::set<std::string> serverSections;
        if (sections.find("network") != sections.end()) {
            serverSections.insert("network");
        }
        if (sections.find("req_pool") != sections.end()) {
            serverSections.insert("req_pool");
        }
        if (sections.find("request") != sections.end()) {
            serverSections.insert("request");
        }
        svr->appendJSONStat(writer, serverSections);
        writer.EndObject();
        return Command::fmtBulk(std::string(sb.GetString()));
    }
} debugCommand;

class ShutdownCommand: public Command {
 public:
    ShutdownCommand()
        :Command("shutdown") {
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
        std::shared_ptr<ServerEntry> svr = sess->getServerEntry();
        svr->stop();

        // avoid compiler complains
        return Command::fmtOK();
    }
} shutdownCmd;

class ObjectCommand: public Command {
 public:
    ObjectCommand()
        :Command("object") {
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
        std::shared_ptr<ServerEntry> svr = sess->getServerEntry();
        const std::string& key = sess->getArgs()[1];
        uint32_t storeId = Command::getStoreId(sess, key);
        SessionCtx *pCtx = sess->getCtx();

        const std::map<RecordType, std::string> m = {
            {RecordType::RT_KV, "raw"},
            {RecordType::RT_LIST_META, "linkedlist"},
            {RecordType::RT_HASH_META, "hashtable"},
            {RecordType::RT_SET_META, "ziplist"},
            {RecordType::RT_ZSET_META, "skiplist"},
        };
        for (auto type : {RecordType::RT_KV,
                          RecordType::RT_LIST_META,
                          RecordType::RT_HASH_META,
                          RecordType::RT_SET_META,
                          RecordType::RT_ZSET_META}) {
            RecordKey rk(pCtx->getDbId(), type, key, "");
            Expected<RecordValue> rv =
                Command::expireKeyIfNeeded(sess, storeId, rk);
            if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
                continue;
            } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
                continue;
            } else if (!rv.ok()) {
                return rv.status();
            }
            if (sess->getArgs()[2] == "recount") {
                return Command::fmtOne();
            } else if (sess->getArgs()[2] == "encoding") {
                return Command::fmtBulk(m.at(type));
            } else if (sess->getArgs()[2] == "idletime") {
                return Command::fmtLongLong(0);
            }
        }
        return Command::fmtNull();
    }
} objectCmd;

}  // namespace tendisplus
