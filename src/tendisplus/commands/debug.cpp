#ifndef _WIN32
#include <sys/time.h>
#include <sys/utsname.h>
#endif

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
#include "tendisplus/storage/varint.h"

namespace tendisplus {

class KeysCommand: public Command {
 public:
    KeysCommand()
        :Command("keys", "rs") {
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

    bool sameWithRedis() const {
        return false;
    }

    Expected<std::string> run(Session* sess) final {
        const std::vector<std::string>& args = sess->getArgs();
        auto pattern = args[1];
        bool allkeys = false;

        if (pattern == "*") {
            allkeys = true;
        }

        int32_t limit = 10000;
        if (args.size() > 3) {
            return{ ErrorCodes::ERR_WRONG_ARGS_SIZE, "" };
        }

        if (args.size() == 3) {
            auto l = tendisplus::stol(args[2]);
            if (!l.ok()) {
                return l.status();
            }
            limit = l.value();
        }

        // TODO(vinchen): too big
        if (limit < 0 || limit > 10000) {
            return{ ErrorCodes::ERR_PARSEOPT, "keys size limit to be 10000" };
        }

        auto ts = msSinceEpoch();

        // TODO(vinchen): should use a faster way
        auto server = sess->getServerEntry();
        std::list<std::string> result;
        for (ssize_t i = 0; i < server->getKVStoreCount(); i++) {
            auto expdb = server->getSegmentMgr()->getDb(sess, i,
                            mgl::LockMode::LOCK_IS);
            if (!expdb.ok()) {
                if (expdb.status().code() == ErrorCodes::ERR_STORE_NOT_OPEN) {
                    continue;
                }
                return expdb.status();
            }

            PStore kvstore = expdb.value().store;
            auto ptxn = kvstore->createTransaction();
            if (!ptxn.ok()) {
                return ptxn.status();
            }
            std::unique_ptr<Transaction> txn = std::move(ptxn.value());
            auto cursor = txn->createCursor();
            cursor->seek("");

            while (true) {
                Expected<Record> exptRcd = cursor->next();
                if (exptRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
                    break;
                }
                if (!exptRcd.ok()) {
                    return exptRcd.status();
                }
                auto keyType = exptRcd.value().getRecordKey().getRecordType();
                // NOTE(vinchen):
                // RecordType::RT_TTL_INDEX and RecordType::BINLOG
                // is always at the last of rocksdb, and the chunkid is very big
                auto chunkId = exptRcd.value().getRecordKey().getChunkId();
                if (chunkId >= server->getSegmentMgr()->getChunkSize()) {
                    break;
                }
                auto key = exptRcd.value().getRecordKey().getPrimaryKey();

                if (!allkeys &&
                    redis_port::stringmatchlen(pattern.c_str(), pattern.size(),
                        key.c_str(), key.size(), 0)) {
                    continue;
                }

                auto ttl = exptRcd.value().getRecordValue().getTtl();
                if (keyType != RecordType::RT_DATA_META ||
                    ttl !=0 && ttl < ts) {      // skip the expired key
                    continue;
                }
                result.emplace_back(std::move(key));
                if (result.size() >= limit) {
                    break;
                }
            }
            if (result.size() >= limit) {
                break;
            }
        }

        std::stringstream ss;
        Command::fmtMultiBulkLen(ss, result.size());
        for (const auto& v : result) {
            Command::fmtBulk(ss, v);
        }
        return ss.str();
    }
} keysCmd;

class DbsizeCommand: public Command {
 public:
    DbsizeCommand()
        :Command("dbsize", "rF") {
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

    bool sameWithRedis() const {
        return false;
    }

    Expected<std::string> run(Session *sess) final {
        const std::vector<std::string>& args = sess->getArgs();

        int64_t size = 0;
        auto currentDbid = sess->getCtx()->getDbId();
        auto ts = msSinceEpoch();

        // TODO(vinchen): should use a faster way
        auto server = sess->getServerEntry();
        std::list<std::string> result;
        for (ssize_t i = 0; i < server->getKVStoreCount(); i++) {
            auto expdb = server->getSegmentMgr()->getDb(sess, i,
                mgl::LockMode::LOCK_IS);
            if (!expdb.ok()) {
                if (expdb.status().code() == ErrorCodes::ERR_STORE_NOT_OPEN) {
                    continue;
                }
                return expdb.status();
            }

            PStore kvstore = expdb.value().store;
            auto ptxn = kvstore->createTransaction();
            if (!ptxn.ok()) {
                return ptxn.status();
            }
            std::unique_ptr<Transaction> txn = std::move(ptxn.value());
            auto cursor = txn->createCursor();
            cursor->seek("");

            while (true) {
                Expected<Record> exptRcd = cursor->next();
                if (exptRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
                    break;
                }
                if (!exptRcd.ok()) {
                    return exptRcd.status();
                }
                auto keyType = exptRcd.value().getRecordKey().getRecordType();
                // NOTE(vinchen):
                // RecordType::RT_TTL_INDEX and RecordType::BINLOG
                // is always at the last of rocksdb, and the chunkid is very big
                auto chunkId = exptRcd.value().getRecordKey().getChunkId();
                if (chunkId >= server->getSegmentMgr()->getChunkSize()) {
                    break;
                }
                auto dbid = exptRcd.value().getRecordKey().getDbId();
                if (dbid != currentDbid) {
                    continue;
                }
                auto ttl = exptRcd.value().getRecordValue().getTtl();
                if (keyType != RecordType::RT_DATA_META ||
                    ttl != 0 && ttl < ts) {      // skip the expired key
                    continue;
                }

                size++;
            }
        }
        return Command::fmtLongLong(size);
    }
} dbsizeCmd;

class PingCommand: public Command {
 public:
    PingCommand()
        :Command("ping", "tF") {
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
            return std::string("+PONG\r\n");
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
        :Command("echo", "F") {
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
        :Command("time", "RF") {
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
        :Command("iterall", "r") {
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

        auto server = sess->getServerEntry();
        if (estoreId.value() >= server->getKVStoreCount()) {
            return {ErrorCodes::ERR_PARSEOPT, "invalid store id"};
        }
        auto expdb = server->getSegmentMgr()->getDb(sess, estoreId.value(),
                        mgl::LockMode::LOCK_IS);
        if (!expdb.ok()) {
            return expdb.status();
        }
        PStore kvstore = expdb.value().store;
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
            auto keyType = exptRcd.value().getRecordKey().getRecordType();
            auto chunkId = exptRcd.value().getRecordKey().getChunkId();

            // NOTE(vinchen):RecordType::RT_TTL_INDEX and RecordType::BINLOG is
            // always at the last of rocksdb, and the chunkid is very big
            if (chunkId >= server->getSegmentMgr()->getChunkSize()) {
                break;
            }

            auto valueType = exptRcd.value().getRecordValue().getRecordType();
            if (!isRealEleType(keyType, valueType)) {
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
            const auto& vt = o.getRecordValue().getRecordType();
            Command::fmtBulk(ss, std::to_string(static_cast<uint32_t>(vt)));
            switch (t) {
                case RecordType::RT_DATA_META:
                    INVARIANT(vt == RecordType::RT_KV);
                    Command::fmtBulk(ss, o.getRecordKey().getPrimaryKey());
                    Command::fmtBulk(ss, "");
                    Command::fmtBulk(ss, o.getRecordValue().getValue());
                    break;
                case RecordType::RT_HASH_ELE:
                case RecordType::RT_LIST_ELE:
                case RecordType::RT_SET_ELE:
                    INVARIANT(vt == t);
                    Command::fmtBulk(ss, o.getRecordKey().getPrimaryKey());
                    Command::fmtBulk(ss, o.getRecordKey().getSecondaryKey());
                    Command::fmtBulk(ss, o.getRecordValue().getValue());
                    break;
                case RecordType::RT_ZSET_H_ELE:
                {
                    INVARIANT(vt == t);
                    Command::fmtBulk(ss, o.getRecordKey().getPrimaryKey());
                    Command::fmtBulk(ss, o.getRecordKey().getSecondaryKey());
                    auto d = tendisplus::doubleDecode(
                                        o.getRecordValue().getValue());
                    if (!d.ok()) {
                        return d.status();
                    }
                    Command::fmtBulk(ss, tendisplus::dtos(d.value()));
                    break;
                }
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
        :Command("show", "a") {
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
            if (std::get<2>(waitlockStat) != mgl::LockMode::LOCK_NONE) {
                writer.Key("waitlock");
                writer.StartArray();
                writer.Uint64(std::get<0>(waitlockStat));
                writer.String(std::get<1>(waitlockStat));
                writer.String(lockModeRepr(std::get<2>(waitlockStat)));
                writer.EndArray();
            }
            std::list<SLSP> lockStats = ctx->getLockStates();
            if (lockStats.size() > 0) {
                writer.Key("holdinglocks");
                writer.StartArray();
                for (const auto& v : lockStats) {
                    writer.StartArray();
                    writer.Uint64(std::get<0>(v));
                    writer.String(std::get<1>(v));
                    writer.String(lockModeRepr(std::get<2>(v)));
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
        :Command("toggleftmc", "a") {
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
         :Command("commandlist", "a") {
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
        auto& args = sess->getArgs();

        int flag = 0;
        bool checkCompatible = false;
        bool checkSupport = false;
        bool multi = false;

        if (args.size() == 1) {
            // all cmd
            flag = CMD_MASK;
        } else if (args.size() == 2) {
            auto type = toLower(args[1]);
            if (type == "readonly") {
                flag |= CMD_READONLY;
            } else if (type == "write") {
                flag |= CMD_WRITE;
            } else if (type == "readwrite") {
                flag |= CMD_READONLY | CMD_WRITE;
            } else if (type == "admin") {
                flag |= CMD_ADMIN;
            } else if (type == "multikey") {
                multi = true;
            } else if (type == "incompatible") {
                flag |= CMD_READONLY | CMD_WRITE;
                checkCompatible = true;
            } else if (type == "notsupport") {
                flag |= CMD_READONLY | CMD_WRITE;
                checkSupport = true;
            } else {
                return{ ErrorCodes::ERR_PARSEOPT, "invalid type" };
            }
        } else if (args.size() == 3) {
            auto type = toLower(args[1]);
            auto sflag = toLower(args[2]);
            if (type == "incompatible") {
                checkCompatible = true;
            } else if (type == "notsupport") {
                checkSupport = true;
            } else {
                return{ ErrorCodes::ERR_PARSEOPT, "invalid type" };
            }

            if (sflag == "readonly") {
                flag |= CMD_READONLY;
            } else if (sflag == "write") {
                flag |= CMD_WRITE;
            } else if (sflag == "readwrite") {
                flag |= CMD_READONLY | CMD_WRITE;
            } else if (sflag == "admin") {
                flag |= CMD_ADMIN;
            } else if (sflag == "all") {
                flag = CMD_MASK;
            } else {
                return{ ErrorCodes::ERR_PARSEOPT, "invalid type" };
            }
        } else {
            return{ ErrorCodes::ERR_WRONG_ARGS_SIZE, "use commandlist [type]" };
        }
        auto& cmdmap = commandMap();
        size_t count = 0;

        std::stringstream ss;
        if (checkCompatible) {
            for (const auto& cmd : cmdmap) {
                if (cmd.second->getFlags() & flag) {
                    auto rcmd = redis_port::getCommandFromTable(cmd.first.c_str());  // NOLINT
                    if (!rcmd) {
                        continue;
                    }

                    auto tcmd = cmd.second;
                    if (rcmd->flags != tcmd->getFlags() ||
                        rcmd->arity != tcmd->arity() ||
                        rcmd->firstkey != tcmd->firstkey() ||
                        rcmd->lastkey != tcmd->lastkey() ||
                        rcmd->keystep != tcmd->keystep() ||
                        !tcmd->sameWithRedis()) {
                        char buf[1024];
                        snprintf(buf, sizeof(buf), "%s flags(%d,%d), arity(%d,%d), firstkey(%d,%d), lastkey(%d,%d), keystep(%d,%d) sameWithRedis(%s)",    // NOLINT
                            cmd.first.c_str(),
                            rcmd->flags, tcmd->getFlags(),
                            rcmd->arity, static_cast<int>(tcmd->arity()),
                            rcmd->firstkey, tcmd->firstkey(),
                            rcmd->lastkey, tcmd->lastkey(),
                            rcmd->keystep, tcmd->keystep(),
                            tcmd->sameWithRedis() ? "true" : "false");
                        Command::fmtBulk(ss, buf);
                        count++;
                    }
                }
            }
        } else if (checkSupport) {
            int j;
            int numcommands = redis_port::getCommandCount();

            for (j = 0; j < numcommands; j++) {
                struct redis_port::redisCommand *c = redis_port::getCommandFromTable(j);  // NOLINT

                if (c->flags & flag) {
                    if (!cmdmap.count(c->name)) {
                        Command::fmtBulk(ss, c->name);
                        count++;
                    }
                }
            }

        } else {
            for (const auto& cmd : cmdmap) {
                if (cmd.second->getFlags() & flag) {
                    Command::fmtBulk(ss, cmd.first);
                    count++;
                } else if (multi && cmd.second->isMultiKey()) {
                    Command::fmtBulk(ss, cmd.first);
                    count++;
                }
            }
        }

        std::stringstream ret;
        Command::fmtMultiBulkLen(ret, count);
        ret << ss.str();

        return ret.str();
    }
} cmdList;

class BinlogTimeCommand: public Command {
 public:
    BinlogTimeCommand()
        :Command("binlogtime", "a") {
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

        auto server = sess->getServerEntry();
        if (storeId.value() >= server->getKVStoreCount()) {
            return {ErrorCodes::ERR_PARSEOPT, "invalid instance num"};
        }
        Expected<uint64_t> binlogId = ::tendisplus::stoul(args[2]);
        if (!binlogId.ok()) {
            return binlogId.status();
        }

        auto expdb = server->getSegmentMgr()->getDb(sess, storeId.value(),
                            mgl::LockMode::LOCK_IS);
        if (!expdb.ok()) {
            return expdb.status();
        }
        PStore kvstore = expdb.value().store;
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
        :Command("binlogpos", "a") {
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

        auto server = sess->getServerEntry();
        if (storeId.value() >= server->getKVStoreCount()) {
            return {ErrorCodes::ERR_PARSEOPT, "invalid instance num"};
        }

        auto expdb = server->getSegmentMgr()->getDb(sess, storeId.value(),
                    mgl::LockMode::LOCK_IS);
        if (!expdb.ok()) {
            return expdb.status();
        }
        PStore kvstore = expdb.value().store;
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
        :Command("debug", "a") {
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
        SegmentMgr *segMgr = svr->getSegmentMgr();
        INVARIANT(segMgr != nullptr);
        ReplManager *replMgr = svr->getReplManager();
        INVARIANT(replMgr != nullptr);

        rapidjson::StringBuffer sb;
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(sb);
        writer.StartObject();

        if (sections.find("stores") != sections.end()) {
            writer.Key("stores");
            writer.StartObject();
            for (uint32_t i = 0; i < svr->getKVStoreCount(); ++i) {
                auto expdb = segMgr->getDb(sess, i, mgl::LockMode::LOCK_IS,
                                    true);
                if (!expdb.ok()) {
                    return expdb.status();
                }
                PStore store = expdb.value().store;
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
        :Command("shutdown", "a") {
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
        // auto vv = dynamic_cast<NetSession*>(sess);
        // INVARIANT(vv != nullptr);
        // vv->setCloseAfterRsp();

        // TODO(vinchen): maybe it should log something here
        // shutdown asnyc
        sess->getServerEntry()->handleShutdownCmd();

        // avoid compiler complains
        return Command::fmtOK();
    }
} shutdownCmd;

class ClientCommand: public Command {
 public:
    ClientCommand()
        :Command("client", "as") {
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

    Expected<std::string> listClients(Session *sess) {
        auto svr = sess->getServerEntry();
        INVARIANT(svr != nullptr);
        auto sesses = svr->getAllSessions();

        std::stringstream ss;
        for (auto& v : sesses) {
            if (v->getFd() == -1) {
                // local sess
                continue;
            }
            SessionCtx *ctx = sess->getCtx();
            ss << "id=" << v->id()
                << " addr=" << v->getRemote()
                << " fd=" << v->getFd()
                << " name=" << v->getName()
                << " db=" << ctx->getDbId()
                << "\n";
        }
        return Command::fmtBulk(ss.str());
    }

    Expected<std::string> killClients(Session *sess) {
        const std::vector<std::string>& args = sess->getArgs();

        int skipme = 0;
        std::string remote = "";
        uint64_t id = 0;

        if (args.size() == 3) {
            remote = args[2];
            skipme = 0;
        } else if (args.size() > 3) {
            size_t i = 2;
            while (i < args.size()) {
                int moreargs = args.size() > i+1;
                if (args[i] == "id" && moreargs) {
                    Expected<uint64_t> eid = ::tendisplus::stoul(args[i+1]);
                    if (!eid.ok()) {
                        return eid.status();
                    }
                    id = eid.value();
                } else if (args[i] == "addr" && moreargs) {
                    remote = args[i+1];
                } else if (args[i] == "skipme" && moreargs) {
                    if (args[i+1] == "yes") {
                        skipme = 1;
                    } else if (args[i+1] == "no") {
                        skipme = 0;
                    } else {
                        return {ErrorCodes::ERR_PARSEOPT, "skipme yes|no"};
                    }
                } else {
                        return {ErrorCodes::ERR_PARSEOPT, "invalid syntax"};
                }
                i += 2;
            }
        }

        auto svr = sess->getServerEntry();
        INVARIANT(svr != nullptr);
        auto sesses = svr->getAllSessions();
        int closeThisClient = 0;
        for (auto& v : sesses) {
            if (v->getFd() == -1) {
                continue;
            }
            if (id != 0 && v->id() != id) {
                continue;
            }
            if (remote != "" && v->getRemote() != remote) {
                continue;
            }
            if (v->id() == sess->id()) {
                if (skipme) {
                    continue;
                } else {
                    closeThisClient = 1;
                }
            } else {
                v->cancel();
                return Command::fmtOK();
            }
        }
        if (closeThisClient) {
            auto vv = dynamic_cast<NetSession*>(sess);
            INVARIANT(vv != nullptr);
            vv->setCloseAfterRsp();
            return Command::fmtOK();
        }
        return {ErrorCodes::ERR_NOTFOUND, "No such client"};
    }

    Expected<std::string> run(Session *sess) final {
        const std::vector<std::string>& args = sess->getArgs();

        if (args[1] == "LIST" || args[1] == "list") {
            return listClients(sess);
        } else if (args[1] == "getname" || args[1] == "GETNAME") {
            std::string name = sess->getName();
            if (name == "") {
                return Command::fmtNull();
            } else {
                return Command::fmtBulk(name);
            }
        } else if ((args[1] == "setname" || args[1] == "SETNAME")
                        && args.size() == 3) {
            for (auto v : args[2]) {
                if (v < '!' || v > '~') {
                    LOG(INFO) << "word:" << args[2] << ' ' << v << " illegal";
                    return {ErrorCodes::ERR_PARSEOPT,
                               "Client names cannot contain spaces, newlines or special characters."};   // NOLINT
                }
            }
            sess->setName(args[2]);
            return Command::fmtOK();
        } else if (args[1] == "KILL" || args[1] == "kill") {
            return killClients(sess);
        } else {
            return {ErrorCodes::ERR_PARSEOPT,
                "Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name)"};  // NOLINT
        }
    }
} clientCmd;

class InfoCommand: public Command {
 public:
    InfoCommand()
        :Command("info", "lt") {
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

    bool sameWithRedis() const {
        return false;
    }

    Expected<std::string> run(Session *sess) final {
        std::stringstream result;
        std::string section;
        if (sess->getArgs().size() == 1) {
            section = "default";
        } else {
            section = sess->getArgs()[1];
        }
        auto server = sess->getServerEntry();
        uint64_t uptime = nsSinceEpoch() - server->getStartupTimeNs();

        bool allsections = (section == "all");
        bool defsections = (section == "default");
        if (allsections || defsections || section == "server") {
            std::stringstream ss;
            static int call_uname = 1;
#ifndef _WIN32
            static struct utsname name;
            if (call_uname) {
                call_uname = 0;
                uname(&name);
            }
#endif
            ss << "# Server\r\n"
#ifndef _WIN32
                << "os:" << name.sysname << " " << name.release << " " << name.machine << "\r\n"        // NOLINT
#endif
                << "arch_bits:" << ((sizeof(size_t) == 8) ? 64 : 32) << "\r\n"
                << "multiplexing_api:asio\r\n"
#ifdef __GNUC__
                << "gcc_version:" << __GNUC__ << ":" << __GNUC_MINOR__ << ":" << __GNUC_PATCHLEVEL__ << "\r\n"  // NOLINT
#else
                << "gcc_version:0.0.0\r\n"
#endif
                << "process_id:" << getpid() << "\r\n"
                << "uptime_in_seconds:" << uptime/1000000000 << "\r\n"
                << "uptime_in_days:" << uptime/1000000000/(3600*24) << "\r\n";
            result << ss.str();
        }
        if (allsections || defsections || section == "clients") {
            std::stringstream ss;
            ss << "# Clients\r\n"
                << "connected_clients:"
                << server->getAllSessions().size() << "\r\n";
            result << ss.str();
        }
        return  Command::fmtBulk(result.str());
    }
} infoCmd;

class ObjectCommand: public Command {
 public:
    ObjectCommand()
        :Command("object", "r") {
    }

    ssize_t arity() const {
        return -2;
    }

    int32_t firstkey() const {
        return 2;
    }

    int32_t lastkey() const {
        return 2;
    }

    int32_t keystep() const {
        return 2;
    }

    bool sameWithRedis() const {
        return false;
    }

    Expected<std::string> run(Session *sess) final {
        auto& args = sess->getArgs();

        if (args.size() == 2) {
            std::stringstream ss;

            Command::fmtLongLong(ss, 5);
            Command::fmtBulk(ss,
                "OBJECT <subcommand> key. Subcommands:");
            Command::fmtBulk(ss,
                "refcount -- Return the number of references of the value associated with the specified key. Always 1.");           // NOLINT
            Command::fmtBulk(ss,
                "encoding -- Return the kind of internal representation used in order to store the value associated with a key.");  // NOLINT
            Command::fmtBulk(ss,
                "idletime -- Return the idle time of the key, that is the approximated number of seconds elapsed since the last access to the key. Always 0");  // NOLINT
            Command::fmtBulk(ss,
                "freq -- Return the access frequency index of the key. The returned integer is proportional to the logarithm of the recent access frequency of the key. Always 0");  // NOLINT 
            return ss.str();
        } else if (args.size() == 3) {
            const std::string& key = sess->getArgs()[2];
            const std::map<RecordType, std::string> m = {
                {RecordType::RT_KV, "raw"},
                {RecordType::RT_LIST_META, "linkedlist"},
                {RecordType::RT_HASH_META, "hashtable"},
                {RecordType::RT_SET_META, "ziplist"},
                {RecordType::RT_ZSET_META, "skiplist"},
            };

            Expected<RecordValue> rv =
                Command::expireKeyIfNeeded(sess, key, RecordType::RT_DATA_META);
            if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
                rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
                return Command::fmtNull();
            } else if (!rv.ok()) {
                return rv.status();
            }
            auto vt = rv.value().getRecordType();
            if (args[1] == "refcount") {
                return Command::fmtOne();
            } else if (args[1] == "encoding") {
                return Command::fmtBulk(m.at(vt));
            } else if (args[1] == "idletime") {
                return Command::fmtLongLong(0);
            } else if (args[1] == "freq") {
                return Command::fmtLongLong(0);
            }
        }
        return{ ErrorCodes::ERR_PARSEOPT,
            "Unknown subcommand or wrong number of arguments. Try OBJECT help" };       // NOLINT
    }
} objectCmd;

class ConfigCommand : public Command {
 public:
    ConfigCommand()
        :Command("config", "lat") {
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

    bool sameWithRedis() const {
        return false;
    }

    Expected<std::string> run(Session *sess) final {
        // TODO(vinchen): support it later
        auto& args = sess->getArgs();

        if (args.size() != 4 && args.size() != 5) {
            return{ ErrorCodes::ERR_PARSEOPT, "args size incorrect!" };
        }
        if (toLower(args[1]) == "set") {
            if (toLower(args[2]) == "session") {
                if (args.size() != 5) {
                    return{ ErrorCodes::ERR_PARSEOPT, "args size incorrect!" };
                }

                if (toLower(args[3]) == "tendis_extended_protocol") {
                    sess->getCtx()->setExtendProtocol(isOptionOn(args[4]));
                }
            }
        }

        return Command::fmtOK();
    }
} configCmd;

#define EMPTYDB_NO_FLAGS 0      /* No flags. */
#define EMPTYDB_ASYNC (1<<0)    /* Reclaim memory in another thread. */

class FlushGeneric : public Command {
 public:
    FlushGeneric(const std::string& name, const char* sflags)
        : Command(name, sflags) {}

    Expected<int> getFlushCommandFlags(Session* sess) {
        auto args = sess->getArgs();
        if (args.size() > 1) {
            if (args.size() > 2 || toLower(args[1]) != "async") {
                return {ErrorCodes::ERR_PARSEOPT, "" };
            }
            return EMPTYDB_ASYNC;
        } else {
            return EMPTYDB_NO_FLAGS;
        }
    }

    Status runGeneric(Session* sess, int flags) {
        auto server = sess->getServerEntry();
        for (ssize_t i = 0; i < server->getKVStoreCount(); i++) {
            auto expdb = server->getSegmentMgr()->getDb(sess, i,
                                mgl::LockMode::LOCK_X);
            if (!expdb.ok()) {
                if (expdb.status().code() == ErrorCodes::ERR_STORE_NOT_OPEN) {
                    continue;
                }
                return expdb.status();
            }

            // TODO(vinchen): how to translate it to slave
            auto store = expdb.value().store;

            // TODO(vinchen): handle STORE_NONE database in the future
            INVARIANT(!store->isPaused() && store->isOpen());
            INVARIANT(store->isRunning());

            auto s = store->stop();
            if (!s.ok()) {
                return s;
            }

            s = store->clear();
            if (!s.ok()) {
                return s;
            }

            auto ret = store->restart(false);
            if (!ret.ok()) {
                return ret.status();
            }
        }
        return{ ErrorCodes::ERR_OK, "" };
    }
};

class FlushAllCommand : public FlushGeneric {
 public:
    FlushAllCommand()
        :FlushGeneric("flushall", "w") {
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
        auto args = sess->getArgs();

        auto flags = getFlushCommandFlags(sess);
        if (!flags.ok()) {
            return flags.status();
        }

        auto s = runGeneric(sess, flags.value());
        if (!s.ok()) {
            return s;
        }

        return Command::fmtOK();
    }
} flushallCmd;

class FlushdbCommand : public FlushGeneric {
 public:
    FlushdbCommand()
        :FlushGeneric("flushdb", "w") {
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
        auto args = sess->getArgs();

        auto flags = getFlushCommandFlags(sess);
        if (!flags.ok()) {
            return flags.status();
        }

        auto s = runGeneric(sess, flags.value());
        if (!s.ok()) {
            return s;
        }

        return Command::fmtOK();
    }
} flushdbCmd;

class FlushAllDiskCommand : public FlushGeneric {
 public:
    FlushAllDiskCommand()
        :FlushGeneric("flushalldisk", "w") {
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
        auto args = sess->getArgs();

        auto flags = getFlushCommandFlags(sess);
        if (!flags.ok()) {
            return flags.status();
        }

        // TODO(vinchen): only support db 0
        if (sess->getCtx()->getDbId() != 0) {
            return{ ErrorCodes::ERR_PARSEOPT, "only support db 0" };
        }

        auto s = runGeneric(sess, flags.value());
        if (!s.ok()) {
            return s;
        }

        return Command::fmtOK();
    }
} flushalldiskCmd;

class MonitorCommand : public Command {
 public:
    MonitorCommand()
        :Command("monitor", "as") {
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

    bool sameWithRedis() const {
        return false;
    }

    Expected<std::string> run(Session *sess) final {
        auto vv = dynamic_cast<NetSession*>(sess);
        INVARIANT(vv != nullptr);
        // TODO(vinchen): support it later
        vv->setCloseAfterRsp();
        return{ ErrorCodes::ERR_INTERNAL, "monitor not supported yet" };
    }
} monitorCmd;

// destroystore storeId [force]
// force is optional, it means whether check the store is empty.
// if force, no check
class DestroyStoreCommand : public Command {
 public:
    DestroyStoreCommand()
        :Command("destroystore", "a") {
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
        Expected<uint64_t> storeId = ::tendisplus::stoul(args[1]);
        bool isForce = false;
        if (!storeId.ok()) {
            return storeId.status();
        }

        auto server = sess->getServerEntry();
        if (storeId.value() >= server->getKVStoreCount()) {
            return{ ErrorCodes::ERR_PARSEOPT, "invalid instance num" };
        }

        if (args.size() > 2) {
            if (args[2] != "force") {
                return{ ErrorCodes::ERR_PARSEOPT,
                    "please use destroystore storeId [force]" };
            }
            isForce = true;
        }

        Status status = server->destroyStore(sess, storeId.value(), isForce);
        if (!status.ok()) {
            return status;
        }

        return Command::fmtOK();
    }
} destroyStoreCmd;

// pausestore storeId
// only disable get/set of the store
// when the server restart, the store should be resumed
// It should be safe to pausestore first, and destroystore later
class PauseStoreCommand : public Command {
 public:
    PauseStoreCommand()
        :Command("pausestore", "a") {
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
        Expected<uint64_t> storeId = ::tendisplus::stoul(args[1]);
        if (!storeId.ok()) {
            return storeId.status();
        }

        auto server = sess->getServerEntry();
        if (storeId.value() >= server->getKVStoreCount()) {
            return{ ErrorCodes::ERR_PARSEOPT, "invalid instance num" };
        }

        auto expdb = server->getSegmentMgr()->getDb(sess, storeId.value(),
                        mgl::LockMode::LOCK_X);
        if (!expdb.ok()) {
            return expdb.status();
        }

        auto store = expdb.value().store;
        Status status = store->pause();
        if (!status.ok()) {
            LOG(ERROR) << "pause store :" << storeId.value()
                << " failed:" << status.toString();
            return status;
        }
        INVARIANT(store->isRunning() && store->isPaused());

        return Command::fmtOK();
    }
} pauseStoreCmd;

// resumestore storeId
class ResumeStoreCommand : public Command {
 public:
    ResumeStoreCommand()
        :Command("resumestore", "a") {
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
        Expected<uint64_t> storeId = ::tendisplus::stoul(args[1]);
        if (!storeId.ok()) {
            return storeId.status();
        }

        auto server = sess->getServerEntry();
        if (storeId.value() >= server->getKVStoreCount()) {
            return{ ErrorCodes::ERR_PARSEOPT, "invalid instance num" };
        }

        auto expdb = server->getSegmentMgr()->getDb(sess, storeId.value(),
            mgl::LockMode::LOCK_X);
        if (!expdb.ok()) {
            return expdb.status();
        }

        auto store = expdb.value().store;
        Status status = store->resume();
        if (!status.ok()) {
            LOG(ERROR) << "pause store :" << storeId.value()
                << " failed:" << status.toString();
            return status;
        }
        INVARIANT(store->isRunning() && !store->isPaused());

        return Command::fmtOK();
    }
} resumeStoreCmd;

}  // namespace tendisplus
