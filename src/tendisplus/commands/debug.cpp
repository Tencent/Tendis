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
#include <map>
#include <sys/resource.h>
#include "glog/logging.h"
#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include "rocksdb/perf_context.h"
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
        auto server = sess->getServerEntry();
        if (!server->getParams()->openKeys) { // default close Keys command.
            return{ ErrorCodes::ERR_COMMAND_CLOSED, "" };
        }

        const std::vector<std::string>& args = sess->getArgs();
        auto pattern = args[1];
        bool allkeys = false;

        if (pattern == "*") {
            allkeys = true;
        }

        // TODO(comboqiu): 30000
        int32_t limit = 30000;
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
        if (limit < 0 || limit > 30000) {
            return{ ErrorCodes::ERR_PARSEOPT, "keys size limit to be 10000" };
        }

        auto ts = msSinceEpoch();

        // TODO(vinchen): should use a faster way
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
            auto ptxn = kvstore->createTransaction(sess);
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
                if (exptRcd.value().getRecordKey().getDbId() !=
                    sess->getCtx()->getDbId()) {
                    continue;
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
                    !redis_port::stringmatchlen(pattern.c_str(), pattern.size(),
                        key.c_str(), key.size(), 0)) {
                    continue;
                }

                auto ttl = exptRcd.value().getRecordValue().getTtl();
                if (keyType != RecordType::RT_DATA_META ||
                    (!Command::noExpire() && ttl !=0 && ttl < ts)) {  // skip the expired key
                    continue;
                }
                result.emplace_back(std::move(key));
                if (result.size() >= (size_t)limit) {
                    break;
                }
            }
            if (result.size() >= (size_t)limit) {
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
        auto server = sess->getServerEntry();
        if (!server->getParams()->openDbsize) { // default close Dbsize command.
            return{ ErrorCodes::ERR_COMMAND_CLOSED, "" };
        }

        int64_t size = 0;
        auto currentDbid = sess->getCtx()->getDbId();
        auto ts = msSinceEpoch();

        // TODO(vinchen): should use a faster way
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
            auto ptxn = kvstore->createTransaction(sess);
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
                    (!Command::noExpire() && ttl != 0 && ttl < ts)) {      // skip the expired key
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
            return {ErrorCodes::ERR_WRONG_ARGS_SIZE,
                "wrong number of arguments for 'ping' command"};
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
    // @output next_record_hex + listof(type dbid key subkey value)
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
        auto ptxn = kvstore->createTransaction(sess);
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

        std::unordered_map<std::string, uint64_t> lIdx;
        std::list<Record> result;
        uint64_t currentTs = msSinceEpoch();
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
            uint64_t targetTtl = exptRcd.value().getRecordValue().getTtl();
            if (!Command::noExpire()
                && 0 != targetTtl
                && currentTs > targetTtl) {
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
            Command::fmtMultiBulkLen(ss, 5);
            const auto& t = o.getRecordKey().getRecordType();
            const auto& vt = o.getRecordValue().getRecordType();
            Command::fmtBulk(ss, std::to_string(static_cast<uint32_t>(vt)));
            switch (t) {
                case RecordType::RT_DATA_META:
                    INVARIANT(vt == RecordType::RT_KV);
                    Command::fmtBulk(ss, std::to_string(o.getRecordKey().getDbId()));
                    Command::fmtBulk(ss, o.getRecordKey().getPrimaryKey());
                    Command::fmtBulk(ss, "");
                    Command::fmtBulk(ss, o.getRecordValue().getValue());
                    break;

                case RecordType::RT_HASH_ELE:
                case RecordType::RT_SET_ELE:
                    INVARIANT(vt == t);
                    Command::fmtBulk(ss, std::to_string(o.getRecordKey().getDbId()));
                    Command::fmtBulk(ss, o.getRecordKey().getPrimaryKey());
                    Command::fmtBulk(ss, o.getRecordKey().getSecondaryKey());
                    Command::fmtBulk(ss, o.getRecordValue().getValue());
                    break;

                case RecordType::RT_LIST_ELE:
                {
                    INVARIANT(vt == t);
                    if (lIdx.count(o.getRecordKey().prefixPk()) < 1) {
                        RecordKey metakey(o.getRecordKey().getChunkId(),
                                o.getRecordKey().getDbId(),
                                RecordType::RT_DATA_META,
                                o.getRecordKey().getPrimaryKey(),
                                "");
                        auto expRv = kvstore->getKV(metakey, txn.get());
                        if (!expRv.ok()) {
                            return expRv.status();
                        }
                        auto expLm = ListMetaValue::decode(
                                expRv.value().getValue());
                        if (!expLm.ok()) {
                            return expLm.status();
                        }
                        lIdx.emplace(std::make_pair(
                                o.getRecordKey().prefixPk(),
                                expLm.value().getHead()));
                    }
                    auto &head = lIdx[o.getRecordKey().prefixPk()];
                    auto expIdx = tendisplus::stoul(o.getRecordKey().getSecondaryKey());
                    if (!expIdx.ok()) {
                        return expIdx.status();
                    }
                    Command::fmtBulk(ss, std::to_string(o.getRecordKey().getDbId()));
                    Command::fmtBulk(ss, o.getRecordKey().getPrimaryKey());
                    Command::fmtBulk(ss, std::to_string(expIdx.value() - head));
                    Command::fmtBulk(ss, o.getRecordValue().getValue());
                    break;
                }
                case RecordType::RT_ZSET_H_ELE:
                {
                    INVARIANT(vt == t);
                    Command::fmtBulk(ss, std::to_string(o.getRecordKey().getDbId()));
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
            // TODO(vinchen): Does it has a better way?
            auto args = ctx->getArgsBrief();
            if (args.size() == 0 && !all) {
                continue;
            }
            writer.StartObject();

            writer.Key("args");
            writer.StartArray();
            for (const auto& arg : args) {
                writer.String(arg.data(), arg.size());
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
        auto ptxn = kvstore->createTransaction(sess);
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
#ifdef BINLOG_V1
        std::unique_ptr<BinlogCursor> cursor =
            txn->createBinlogCursor(binlogId.value(), true);
        Expected<ReplLog> explog = cursor->next();
        if (!explog.ok()) {
            return explog.status();
        }
        return Command::fmtLongLong(
            explog.value().getReplLogKey().getTimestamp());
#else
        auto cursor = txn->createRepllogCursorV2(binlogId.value(), true);
        auto explog = cursor->nextV2();
        if (!explog.ok()) {
            return explog.status();
        }
        return Command::fmtLongLong(explog.value().getTimestamp());
#endif
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
        auto ptxn = kvstore->createTransaction(sess);
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
#ifdef BINLOG_V1
        std::unique_ptr<BinlogCursor> cursor =
            txn->createBinlogCursor(Transaction::MIN_VALID_TXNID, true);
        cursor->seekToLast();
        Expected<ReplLog> explog = cursor->next();
        if (!explog.ok()) {
            return explog.status();
        }
        return Command::fmtLongLong(explog.value().getReplLogKey().getTxnId());
#else 
        auto expBinlogid = RepllogCursorV2::getMaxBinlogId(txn.get());
        if (expBinlogid.status().code() == ErrorCodes::ERR_EXHAUST) {
            return Command::fmtZero();
        }
        if (!expBinlogid.ok()) {
            return expBinlogid.status();
        }

        return Command::fmtLongLong(expBinlogid.value());
#endif //
    }
} binlogPosCommand;

class BinlogStartCommand: public Command {
 public:
    BinlogStartCommand()
        :Command("binlogstart", "a") {
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
        auto ptxn = kvstore->createTransaction(sess);
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        auto expBinlogid = RepllogCursorV2::getMinBinlogId(txn.get());
        if (expBinlogid.status().code() == ErrorCodes::ERR_EXHAUST) {
            return Command::fmtZero();
        }
        if (!expBinlogid.ok()) {
            return expBinlogid.status();
        }

        return Command::fmtLongLong(expBinlogid.value());
    }
} binlogStartCommand;

class BinlogFlushCommand: public Command {
 public:
    BinlogFlushCommand()
        :Command("binlogflush", "a") {
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
        auto server = sess->getServerEntry();
        INVARIANT(server != nullptr);

        auto replMgr = server->getReplManager();
        INVARIANT(replMgr != nullptr);
        const std::string& kvstore = sess->getArgs()[1];
        if (kvstore == "all") {
            for (uint32_t i = 0; i < server->getKVStoreCount(); ++i) {
                // TODO(takenliu) updateCurBinlogFs should return result
                replMgr->flushCurBinlogFs(i);
            }
        } else {
            Expected<uint64_t> exptStoreId = ::tendisplus::stoul(kvstore.c_str());
            if (!exptStoreId.ok()) {
                return exptStoreId.status();
            }
            uint32_t storeId = (uint32_t)exptStoreId.value();
            if (storeId >= server->getKVStoreCount()) {
                LOG(ERROR) << "binlogflush err storeId:" << storeId;
                return {ErrorCodes::ERR_PARSEOPT, "invalid instance num"};
            }
            replMgr->flushCurBinlogFs(storeId);
        }
        LOG(INFO) << "binlogflush succ:" << kvstore;
        return Command::fmtOK();
    }
} binlogFlushCommand;

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
        return Command::fmtBulk("");
    }
} debugCommand;

class tendisstatCommand: public Command {
 public:
    tendisstatCommand()
        :Command("tendisstat", "a") {
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
            sections.insert("network");
            sections.insert("request");
            sections.insert("req_pool");
            sections.insert("perf");
        } else {
            for (size_t i = 1; i < args.size(); ++i) {
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
        if (sections.find("perf") != sections.end()) {
            writer.Key("perf_context");
            writer.StartObject();

            rocksdb::PerfContext* perf_context = rocksdb::get_perf_context();
            string allKeyValue = perf_context->ToString();
            auto v = stringSplit(allKeyValue, ",");
            for (auto one : v) {
                auto key_value = stringSplit(one, "=");
                if (key_value.size() != 2) {
                    continue;
                }
                sdstrim(key_value[0], " ");
                sdstrim(key_value[1], " ");
                writer.Key(key_value[0]);
                writer.Uint64(std::stoul(key_value[1]));
            }

            writer.EndObject();
        }

        writer.EndObject();
        return Command::fmtBulk(std::string(sb.GetString()));
    }
} tendisstatCommand;

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
        section = toLower(section);

        bool allsections = (section == "all");
        bool defsections = (section == "default");

        infoServer(allsections, defsections, section, sess, result);
        infoClients(allsections, defsections, section, sess, result);
        infoMemory(allsections, defsections, section, sess, result);
        infoPersistence(allsections, defsections, section, sess, result);
        infoStats(allsections, defsections, section, sess, result);
        infoReplication(allsections, defsections, section, sess, result);
        infoCPU(allsections, defsections, section, sess, result);
        infoCommandStats(allsections, defsections, section, sess, result);
        infoKeyspace(allsections, defsections, section, sess, result);
        infoBackup(allsections, defsections, section, sess, result);

        return  Command::fmtBulk(result.str());
    }

private:
    void infoServer(bool allsections, bool defsections, std::string& section, Session *sess, std::stringstream& result) {
        if (allsections || defsections || section == "server") {
            auto server = sess->getServerEntry();
            uint64_t uptime = nsSinceEpoch() - server->getStartupTimeNs();

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
                << "redis_version:" << TENDISPLUS_VERSION << "\r\n"
                << "redis_git_sha1:" << REDIS_GIT_SHA1 << "\r\n"
                << "redis_git_dirty:" << REDIS_GIT_DIRTY << "\r\n"
                << "redis_build_id:" << REDIS_BUILD_ID << "\r\n"
                << "redis_mode:" << "standalone" << "\r\n"
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
                //<< "run_id:" << "" << "\r\n" // TODO(takenliu)
                << "tcp_port:" << server->getNetwork()->getPort() << "\r\n"
                << "uptime_in_seconds:" << uptime/1000000000 << "\r\n"
                << "uptime_in_days:" << uptime/1000000000/(3600*24) << "\r\n"
                //<< "hz:" << "" << "\r\n" // TODO(takenliu)
                //<< "lru_clock:" << "" << "\r\n" // TODO(takenliu)
                << "config_file:" << server->getParams()->getConfFile() << "\r\n";
            ss << "\r\n";
            result << ss.str();
        }

    }

    void infoClients(bool allsections, bool defsections, std::string& section, Session *sess, std::stringstream& result) {
        if (allsections || defsections || section == "clients") {
            auto server = sess->getServerEntry();
            std::stringstream ss;
            ss << "# Clients\r\n"
                << "connected_clients:"
                << server->getSessionCount() << "\r\n";
            ss << "\r\n";
            result << ss.str();
        }
    }

    int64_t getIntSize(string& str) {
        if (str.size() <= 2) {
            LOG(ERROR) << "getIntSize failed:" << str;
            return -1;
        }
        string value = str.substr(0, str.size() - 2);
        string unit = str.substr(str.size() - 2, 2);
        Expected<int64_t> size = ::tendisplus::stoll(value);
        if (!size.ok()) {
            LOG(ERROR) << "getIntSize failed:" << str;
            return -1;
        }
        if (unit == "kB") {
            return size.value() * 1024;
        }
        else if (unit == "mB") {
            return size.value() * 1024 * 1024;
        }
        else if (unit == "gB") {
            return size.value() * 1024 * 1024 * 1024;
        }
        LOG(ERROR) << "getIntSize failed:" << str;
        return -1;
    }

    void infoMemory(bool allsections, bool defsections, std::string& section, Session *sess, std::stringstream& result) {
        if (allsections || defsections || section == "memory") {
            auto server = sess->getServerEntry();
            std::stringstream ss;
            ss << "# Memory\r\n";

            string used_memory_vir_human;
            string used_memory_vir_peak_human;
            string used_memory_rss_human;
            string used_memory_rss_peak_human;

            ifstream file;
            file.open("/proc/self/status");
            if (file.is_open()) {
                string strline;
                while (getline(file, strline)) {
                    auto v = stringSplit(strline, ":");
                    if (v.size() != 2) {
                        continue;
                    }
                    if (v[0] == "VmSize") { // virtual memory
                        used_memory_vir_human = trim(v[1]);
                        strDelete(used_memory_vir_human, ' ');
                    }
                    else if (v[0] == "VmPeak") { // peak of virtual memory
                        used_memory_vir_peak_human = trim(v[1]);
                        strDelete(used_memory_vir_peak_human, ' ');
                    }
                    else if (v[0] == "VmRSS") { // physic memory
                        used_memory_rss_human = trim(v[1]);
                        strDelete(used_memory_rss_human, ' ');
                    }
                    else if (v[0] == "VmHWM") { // peak of physic memory
                        used_memory_rss_peak_human = trim(v[1]);
                        strDelete(used_memory_rss_peak_human, ' ');
                    }
                }
            }
            ss << "used_memory:" << -1 << "\r\n";
            ss << "used_memory_human:" << -1 << "\r\n";
            ss << "used_memory_rss:" << getIntSize(used_memory_rss_human) << "\r\n";
            ss << "used_memory_rss_human:" << used_memory_rss_human << "\r\n";
            ss << "used_memory_peak:" << -1 << "\r\n";
            ss << "used_memory_peak_human:" << "-1" << "\r\n";
            ss << "total_system_memory:" << "-1" << "\r\n";
            ss << "total_system_memory_human:" << "-1" << "\r\n";
            ss << "used_memory_lua:" << "-1" << "\r\n";
            ss << "used_memory_vir:" << getIntSize(used_memory_vir_human) << "\r\n";
            ss << "used_memory_vir_human:" << used_memory_vir_human << "\r\n";
            ss << "used_memory_vir_peak_human:" << used_memory_vir_peak_human << "\r\n";
            ss << "used_memory_rss_peak_human:" << used_memory_rss_peak_human << "\r\n";

            ss << "\r\n";
            result << ss.str();
        }
    }

    void infoPersistence(bool allsections, bool defsections, std::string& section, Session *sess, std::stringstream& result) {
        if (allsections || defsections || section == "persistence") {
            auto server = sess->getServerEntry();
            std::stringstream ss;
            ss << "# Persistence\r\n";

            ss << "loading:" << -1 << "\r\n";
            ss << "rdb_changes_since_last_save:" << -1 << "\r\n";
            ss << "rdb_bgsave_in_progress:" << -1 << "\r\n";
            ss << "rdb_last_save_time:" << -1 << "\r\n";
            ss << "rdb_last_bgsave_status:" << -1 << "\r\n";
            ss << "rdb_last_bgsave_time_sec:" << -1 << "\r\n";
            ss << "rdb_current_bgsave_time_sec:" << -1 << "\r\n";
            ss << "aof_enabled:" << -1 << "\r\n";
            ss << "aof_rewrite_in_progress:" << -1 << "\r\n";
            ss << "aof_rewrite_scheduled:" << -1 << "\r\n";
            ss << "aof_last_rewrite_time_sec:" << -1 << "\r\n";
            ss << "aof_current_rewrite_time_sec:" << -1 << "\r\n";
            ss << "aof_last_bgrewrite_status:" << -1 << "\r\n";
            ss << "aof_last_write_status:" << -1 << "\r\n";

            ss << "\r\n";
            result << ss.str();
        }

    }

    void infoStats(bool allsections, bool defsections, std::string& section, Session *sess, std::stringstream& result) {
        if (allsections || defsections || section == "stats") {
            auto server = sess->getServerEntry();
            std::stringstream ss;
            ss << "# Stats\r\n";
            server->getStatInfo(ss);
            ss << "\r\n";
            result << ss.str();
        }

    }

    void infoReplication(bool allsections, bool defsections, std::string& section, Session *sess, std::stringstream& result) {
        if (allsections || defsections || section == "replication") {
            auto server = sess->getServerEntry();
            auto replMgr = server->getReplManager();
            bool show_all = false;
            if (sess->getArgs().size()>=3 && toLower(sess->getArgs()[1]) == "replication" && toLower(sess->getArgs()[2]) == "all") {
                show_all = true;
            }
            std::stringstream ss;
            ss << "# Replication\r\n";
            replMgr->getReplInfo(ss, show_all);
            ss << "\r\n";
            result << ss.str();
        }

    }

    void infoCPU(bool allsections, bool defsections, std::string& section, Session *sess, std::stringstream& result) {
        if (allsections || defsections || section == "cpu") {
            auto server = sess->getServerEntry();
            std::stringstream ss;

            struct rusage self_ru, c_ru;
            getrusage(RUSAGE_SELF, &self_ru);
            getrusage(RUSAGE_CHILDREN, &c_ru);

            ss << std::fixed << std::setprecision(2);
            ss << "# CPU\r\n"
               << "used_cpu_sys:"
               << self_ru.ru_stime.tv_sec+(float)self_ru.ru_stime.tv_usec/1000000 << "\r\n"
               << "used_cpu_user:"
               << self_ru.ru_utime.tv_sec+(float)self_ru.ru_utime.tv_usec/1000000 << "\r\n"
               << "used_cpu_sys_children:"
               << c_ru.ru_stime.tv_sec+(float)c_ru.ru_stime.tv_usec/1000000 << "\r\n"
               << "used_cpu_user_children:"
               << c_ru.ru_utime.tv_sec+(float)c_ru.ru_utime.tv_usec/1000000 << "\r\n";
            ss << "\r\n";
            result << ss.str();
        }
    }

    void infoCommandStats(bool allsections, bool defsections, std::string& section, Session *sess, std::stringstream& result) {
        if (allsections || section == "commandstats") {
            auto server = sess->getServerEntry();
            std::stringstream ss;
            ss << "# CommandStats\r\n";
            for (const auto& kv : commandMap()) {
                ss << "cmdstat_" << kv.first << ":calls=" << kv.second->getCallTimes()
                    << ",usec=" << kv.second->getNanos() / 1000 << "\r\n"; // usec:microsecond
            }
            uint32_t unseenCmdNum = 0;
            uint64_t unseenCmdCalls = 0;
            {
                std::lock_guard<std::mutex> lk(Command::_mutex);
                unseenCmdNum = _unSeenCmds.size();
                for (const auto& kv : _unSeenCmds) {
                    unseenCmdCalls += kv.second;
                }
            }
            ss << "cmdstat_unseen:calls=" << unseenCmdCalls
                << ",num=" << unseenCmdNum << "\r\n";
            ss << "\r\n";
            result << ss.str();
        }
    }

    void infoKeyspace(bool allsections, bool defsections, std::string& section, Session *sess, std::stringstream& result) {
        if (allsections || defsections || section == "keyspace") {
            auto server = sess->getServerEntry();
            std::stringstream ss;
            ss << "# Keyspace\r\n";

            ss << "db0:keys=0,expires=0,avg_ttl=0\r\n";

            ss << "\r\n";
            result << ss.str();
        }
    }

    void infoBackup(bool allsections, bool defsections, std::string& section, Session *sess, std::stringstream& result) {
        if (allsections || defsections || section == "backup") {
            auto server = sess->getServerEntry();
            std::stringstream ss;
            ss << "# Backup\r\n";
            ss << "backup-count:" << server->getBackupTimes() << "\r\n";
            ss << "last-backup-time:" << server->getLastBackupTime() << "\r\n";
            if (server->getBackupFailedTimes() > 0) {
                ss << "backup-failed-count:" << server->getBackupFailedTimes() << "\r\n";
                ss << "last-backup-failed-time:" << server->getLastBackupFailedTime() << "\r\n";
                ss << "last-backup-failed-reason:" << server->getLastBackupFailedErr() << "\r\n";
            }
            ss << "\r\n";
            result << ss.str();
        }
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
            } else if (args[1] == "revision") {
                return Command::fmtLongLong(rv.value().getVersionEP());
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

    Expected<std::string> run(Session *sess) final {
        // TODO(vinchen): support it later
        auto& args = sess->getArgs();

        auto operation = toLower(args[1]);

        if (operation == "set") {
            if (args.size() < 3 || args.size() > 5) {
                return{ ErrorCodes::ERR_PARSEOPT, "args size incorrect!" };
            }

            auto configName = toLower(args[2]);
            if (configName == "session") {
                if (args.size() != 5) {
                    return{ ErrorCodes::ERR_PARSEOPT, "args size incorrect!" };
                }

                if (toLower(args[3]) == "tendis_protocol_extend") {
                    sess->getCtx()->setExtendProtocol(isOptionOn(args[4]));
                }
            } else if (configName == "requirepass") {
                sess->getServerEntry()->setRequirepass(args[3]);
            } else if (configName == "masterauth") {
                sess->getServerEntry()->setMasterauth(args[3]);
            } else if (configName == "appendonly") {
                // NOTE(takenliu): donothing, for tests/*.tcl
            } else {
                string errinfo;
                bool force = false;
                if (!sess->getServerEntry()->getParams()->setVar(configName, args[3], &errinfo, force)) {
                    return{ ErrorCodes::ERR_PARSEOPT, errinfo};
                }
            }
        } else if (operation == "get") {
            if (args.size() != 3) {
                return{ ErrorCodes::ERR_PARSEOPT, "args size incorrect!" };
            }
            //return Command::fmtLongLong(10000);

            auto configName = toLower(args[2]);
            string info;
            if (configName == "requirepass") {
                info = sess->getServerEntry()->requirepass();
            } else if (configName == "masterauth") {
                info = sess->getServerEntry()->masterauth();
            } else if (!sess->getServerEntry()->getParams()->showVar(configName, info)) {
                return{ ErrorCodes::ERR_PARSEOPT, "arg not found:" + configName};
            }
            return Command::fmtBulk(info);
        } else if (operation == "resetstat") {
            bool reset_all = false;
            string configName = "";
            if (args.size() == 2) {
                reset_all = true;
            } else if (args.size() == 3) {
                configName = toLower(args[2]);
            } else {
                return{ ErrorCodes::ERR_PARSEOPT, "args size incorrect!" };
            }

            if (reset_all || configName == "unseencommands") {
                LOG(INFO) << "reset unseencommands";
                std::lock_guard<std::mutex> lk(Command::_mutex);
                for (const auto& kv : _unSeenCmds) {
                    LOG(INFO) << "unseencommand:" << kv.first << " call-times:" << kv.second;
                }
                _unSeenCmds.clear();
            }
            if (reset_all || configName == "commandsstat") {
                LOG(INFO) << "reset commandsstat";
                for (const auto& kv : commandMap()) {
                    LOG(INFO) << "command:" << kv.first << " call-times:" << kv.second;
                    kv.second->resetStatInfo();
                }
            }
            return Command::fmtOK();
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
        auto& args = sess->getArgs();
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
        auto replMgr = server->getReplManager();
        INVARIANT(replMgr != nullptr);

        for (ssize_t i = 0; i < server->getKVStoreCount(); i++) {
            auto expdb = server->getSegmentMgr()->getDb(sess, i,
                                mgl::LockMode::LOCK_X);
            if (!expdb.ok()) {
                if (expdb.status().code() == ErrorCodes::ERR_STORE_NOT_OPEN) {
                    continue;
                }
                return expdb.status();
            }

            auto store = expdb.value().store;

            // TODO(vinchen): handle STORE_NONE database in the future
            INVARIANT_D(!store->isPaused() && store->isOpen());
            INVARIANT_D(store->isRunning());

            auto nextBinlogid = store->getNextBinlogSeq();

            auto eflush = store->flush(sess, nextBinlogid);

            // it is the first txn and binlog, because of LOCK_X
            INVARIANT_D(eflush.value() == nextBinlogid);

            replMgr->onFlush(i, eflush.value());
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
        auto flags = getFlushCommandFlags(sess);
        if (!flags.ok()) {
            return flags.status();
        }

        // TODO(vinchen): only support db 0
        //if (sess->getCtx()->getDbId() != 0) {
        //    return{ ErrorCodes::ERR_PARSEOPT, "only support db 0" };
        //}

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

    Expected<std::string> run(Session *sess) final {
        SessionCtx* pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);
        pCtx->setIsMonitor(true);

        sess->getServerEntry()->AddMonitor(sess->id());

        return Command::fmtOK();
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

#ifdef TENDIS_DEBUG
// only used for test. set key to a fixed storeid
class setInStoreCommand: public Command {
 public:
    setInStoreCommand()
        :Command("setinstore", "wm") {
    }

    ssize_t arity() const {
        return -3;
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
        const auto& args = sess->getArgs();
        if (args.size() < 3) {
            return {ErrorCodes::ERR_PARSEPKT, "invalid set params"};
        }

        Expected<uint64_t> eid = ::tendisplus::stoul(args[1]);
        if (!eid.ok()) {
            return eid.status();
        }
        uint64_t storeid = eid.value();
        auto key = args[2];
        auto value = args[3];

        auto server = sess->getServerEntry();
        if (storeid >= server->getKVStoreCount()) {
            return {ErrorCodes::ERR_PARSEOPT, "invalid store id"};
        }
        auto kvstore = server->getStores()[storeid];
        auto ptxn = kvstore->createTransaction(sess);
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());

        // need get the chunkId, otherwise can't find the key by "GET" command.
        auto expdb = server->getSegmentMgr()->getDbHasLocked(sess, key);
        if (!expdb.ok()) {
            return expdb.status();
        }
        auto chunkId = expdb.value().chunkId;

        LOG(INFO) << "setInStoreCommand storeid:" << storeid << " key:" << key << " val:" << value << " chunkId:" << chunkId;
        Status s = kvstore->setKV(
            Record(
                RecordKey(chunkId, sess->getCtx()->getDbId(), RecordType::RT_KV, key, ""),
                RecordValue(value, RecordType::RT_KV, -1)),
            txn.get());
        if (!s.ok()) {
            LOG(ERROR) << "setInStoreCommand failed:" << s.toString();
            return s;
        }

        Expected<uint64_t> exptCommitId = txn->commit();
        if (!exptCommitId.ok()) {
            LOG(ERROR) << "setInStoreCommand failed:" << exptCommitId.status().toString();
            return exptCommitId.status();
        }
        return Command::fmtOK();
    }
} setInStoreCommand;
#endif

class SyncVersionCommand: public Command {
 public:
    SyncVersionCommand()
        :Command("syncversion", "a") {
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

    Expected<std::string> run(Session *sess) final {
        const auto& args = sess->getArgs();
        if (args[4] != "v1") {
            return {ErrorCodes::ERR_PARSEOPT, ""};
        }

        if ((args[2] == "?") ^ (args[3] == "?")) {
            return {ErrorCodes::ERR_PARSEOPT,
                      "only support both or none \'?\'"};
        }

        const auto& name = args[1];
        const auto server = sess->getServerEntry();
        if (args[2] == "?" && args[3] == "?") {
            std::stringstream ss;
            Command::fmtMultiBulkLen(ss, 2);
            auto ts = server->confirmTs(name);
            auto ver = server->confirmVer(name);
            if (ts == 0 && ver == 0) {
                auto expdb = server->getSegmentMgr()->getDb(sess,
                        0, mgl::LockMode::LOCK_IX);
                if (!expdb.ok()) {
                    return expdb.status();
                }
                PStore store = expdb.value().store;
                auto expvm = server->getCatalog()->getVersionMeta(
                        store, name);
                if (expvm.status().code() == ErrorCodes::ERR_NOTFOUND) {
                    server->setTsVersion(name, -1, -1);
                    ts = ver = -1;
                } else if (!expvm.ok()) {
                    return expvm.status();
                } else {
                    server->setTsVersion(name,
                                         expvm.value()->timestamp, expvm.value()->version);
                    ts = expvm.value()->timestamp;
                    ver = expvm.value()->version;
                }
            }
            Command::fmtLongLong(ss, static_cast<int64_t>(ts));
            Command::fmtLongLong(ss, static_cast<int64_t>(ver));
            return ss.str();
        }

        auto eTs = tendisplus::stoull(args[2]);
        if (!eTs.ok()) {
            return eTs.status();
        }
        auto eVersion = tendisplus::stoull(args[3]);
        if (!eVersion.ok()) {
            return eVersion.status();
        }

        auto expdb = server->getSegmentMgr()->getDb(sess,
                0, mgl::LockMode::LOCK_IX);
        if (!expdb.ok()) {
            return expdb.status();
        }

        PStore store = expdb.value().store;
        auto ptxn = store->createTransaction(sess);
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        auto txn = std::move(ptxn.value());

        std::stringstream pkss;
        pkss << name << "_meta";
        RecordKey rk(0, 0, RecordType::RT_META, pkss.str(), "");
        rapidjson::StringBuffer sb;
        rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
        writer.StartObject();
        writer.Key("timestamp");
        writer.Uint64(eTs.value());
        writer.Key("version");
        writer.Uint64(eVersion.value());
        writer.EndObject();

        RecordValue rv(sb.GetString(), RecordType::RT_META, -1);
        Status s = store->setKV(rk, rv, txn.get());
        if (!s.ok()) {
            return s;
        }
        s = txn->commit().status();
        if (!s.ok()) {
            return s;
        }
        s = server->setTsVersion(name, eTs.value(), eVersion.value());
        if (!s.ok()) {
            return s;
        }
        return Command::fmtOK();
    }
} syncVersionCmd;

class StoreCommand: public Command {
 public:
    StoreCommand()
        :Command("store", "a") {
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
        const auto& args = sess->getArgs();
        auto server = sess->getServerEntry();
        if (toLower(args[1]) == "getack") {
            const auto& name = args[2];
            std::stringstream ss;
            Command::fmtMultiBulkLen(ss, 2);
            Command::fmtBulk(ss, "ack-revision");
            Command::fmtBulk(ss, std::to_string(server->confirmVer(name)));
            return ss.str();
        }
        return {ErrorCodes::ERR_PARSEOPT, "Unknown sub-command"};
    }
} storeCmd;

class EvictCommand: public Command {
 public:
    EvictCommand()
        :Command("evict", "w") {
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
        return Command::fmtOK();
    }
} evictCmd;

class PublishCommand: public Command {
 public:
    PublishCommand()
        :Command("publish", "pltF") {
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
        return Command::fmtOK();
    }
} publishCmd;

class multiCommand: public Command {
 public:
    multiCommand()
        :Command("multi", "sF") {
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
        auto pCtx = sess->getCtx();
        if (pCtx->isInMulti()) {
            return { ErrorCodes::ERR_PARSEPKT, "MULTI calls can not be nested" };
        }
        pCtx->setMulti();
        // maybe set txnVersion to multi's version.
        return Command::fmtOK();
    }
} multiCmd;

class execCommand: public Command {
 public:
    execCommand()
        :Command("exec", "sM") {
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
        auto pCtx = sess->getCtx();
        if (!pCtx->isInMulti()) {
            return { ErrorCodes::ERR_PARSEPKT, "EXEC without MULTI" };
        }
        // check version ?
        if (!pCtx->verifyVersion(pCtx->getVersionEP())) {
            return {ErrorCodes::ERR_WRONG_VERSION_EP, ""};
        }
        pCtx->resetMulti();
        // TODO(comboqiu): return value should include multiBulkLen and sub command results
        return Command::fmtOK();
    }
} execCmd;

class slowlogCommand: public Command {
 public:
    slowlogCommand()
        :Command("slowlog", "sM") {
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
        const auto& args = sess->getArgs();

        const auto server = sess->getServerEntry();
        if (toLower(args[1]) == "len") {
            std::stringstream ss;
            uint64_t num = server->getSlowlogNum();
            Command::fmtLongLong(ss, static_cast<uint64_t>(num));
            return ss.str();
        } else if (toLower(args[1]) == "reset") {
            server->resetSlowlogNum();
            return Command::fmtOK();
        } else {
            return { ErrorCodes::ERR_PARSEPKT, "unkown args" };
        }
    }
} slowlogCmd;

}  // namespace tendisplus
