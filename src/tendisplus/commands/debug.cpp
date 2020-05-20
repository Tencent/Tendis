#ifndef _WIN32
#include <sys/time.h>
#include <sys/utsname.h>
#include <sys/resource.h>
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
#include <thread>
#include <chrono>
#include "glog/logging.h"
#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/iostats_context.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/commands/release.h"
#include "tendisplus/commands/version.h"
#include "tendisplus/storage/varint.h"
#include "tendisplus/utils/scopeguard.h"

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

        const std::vector<std::string>& args = sess->getArgs();
        auto pattern = args[1];
        bool allkeys = false;

        if (pattern == "*") {
            allkeys = true;
        }

        int32_t limit = server->getParams()->keysDefaultLimit;
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
        if (limit < 0) {
            return{ ErrorCodes::ERR_PARSEOPT, "limit should >=0" };
        }

        auto ts = msSinceEpoch();

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


class IterAllKeysCommand : public Command {
public:
    IterAllKeysCommand()
        :Command("iterallkeys", "r") {
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

    // @input iterallkeys storeId start_record_hex batchSize
    // @output next_record_hex + listof(type dbid key)
    Expected<std::string> run(Session* sess) final {
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
            return { ErrorCodes::ERR_PARSEOPT, "batch exceed lim" };
        }

        auto server = sess->getServerEntry();
        if (estoreId.value() >= server->getKVStoreCount()) {
            return { ErrorCodes::ERR_PARSEOPT, "invalid store id" };
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
        }
        else {
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
            if (keyType != RecordType::RT_DATA_META) {
                continue;
            }
            INVARIANT_D(isDataMetaType(valueType));

            uint64_t targetTtl = exptRcd.value().getRecordValue().getTtl();
            if (0 != targetTtl
                && currentTs > targetTtl) {
                continue;
            }
            result.emplace_back(std::move(exptRcd.value()));
        }
        std::string nextCursor;
        if (result.size() == ebatchSize.value() + 1) {
            nextCursor = hexlify(result.back().getRecordKey().encode());
            result.pop_back();
        }
        else {
            nextCursor = "0";
        }
        std::stringstream ss;
        Command::fmtMultiBulkLen(ss, 2);
        Command::fmtBulk(ss, nextCursor);
        Command::fmtMultiBulkLen(ss, result.size());
        for (const auto& o : result) {
            Command::fmtMultiBulkLen(ss, 3);
            const auto& t = o.getRecordKey().getRecordType();
            const auto& vt = o.getRecordValue().getRecordType();
            Command::fmtBulk(ss, std::to_string(static_cast<uint32_t>(vt)));
            switch (t) {
            case RecordType::RT_DATA_META:
                Command::fmtBulk(ss, std::to_string(o.getRecordKey().getDbId()));
                Command::fmtBulk(ss, o.getRecordKey().getPrimaryKey());
                break;
            default:
                INVARIANT_D(0);
            }
        }
        return ss.str();
    }
} iterAllKeysCmd;


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
            // TODO(vinchen): the ttl isn't correct. Because it isn't meta record
            // except KV.
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
                    INVARIANT_D(vt == RecordType::RT_KV);
                    Command::fmtBulk(ss, std::to_string(o.getRecordKey().getDbId()));
                    Command::fmtBulk(ss, o.getRecordKey().getPrimaryKey());
                    Command::fmtBulk(ss, "");
                    Command::fmtBulk(ss, o.getRecordValue().getValue());
                    break;

                case RecordType::RT_HASH_ELE:
                case RecordType::RT_SET_ELE:
                    INVARIANT_D(vt == t);
                    Command::fmtBulk(ss, std::to_string(o.getRecordKey().getDbId()));
                    Command::fmtBulk(ss, o.getRecordKey().getPrimaryKey());
                    Command::fmtBulk(ss, o.getRecordKey().getSecondaryKey());
                    Command::fmtBulk(ss, o.getRecordValue().getValue());
                    break;

                case RecordType::RT_LIST_ELE:
                {
                    INVARIANT_D(vt == t);
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
                    INVARIANT_D(vt == t);
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
                    INVARIANT_D(0);
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
        auto now = nsSinceEpoch();
        for (const auto& sess : sesses) {
            SessionCtx *ctx = sess->getCtx();
            // TODO(vinchen): Does it has a better way?
            auto args = ctx->getArgsBrief();
            if (args.size() == 0 && !all) {
                continue;
            }
            writer.StartObject();

            writer.Key("id");
            writer.Uint64(sess->id());

            writer.Key("host");
            writer.String(sess->getRemote());

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

            writer.Key("duration");
            uint64_t duration = 0;
            auto start = ctx->getProcessPacketStart();
            if (start > 0) {
                duration = (now - start) / 1000000;
            }
            writer.Uint64(duration);

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
} showCmd;

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

class RocksPropertyCommand : public Command {
 public:
    RocksPropertyCommand()
        :Command("rocksproperty", "a") {
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

    Expected<std::string> run(Session* sess) final {
        std::shared_ptr<ServerEntry> svr = sess->getServerEntry();
        uint64_t first = 0, last = svr->getKVStoreCount();
        const auto& args = sess->getArgs();
        if (sess->getArgs().size() > 2) {
            auto e = ::tendisplus::stoll(args[2]);
            if (!e.ok()) {
                return e.status();
            }
            first = e.value();
            last = first + 1;

            if (first >= svr->getKVStoreCount()) {
                return { ErrorCodes::ERR_PARSEOPT, "invalid store id" };
            }
        }

        auto property = args[1];
        std::stringstream ss;
        Command::fmtMultiBulkLen(ss, last - first);
        for (; first < last; first++) {
            auto expdb = svr->getSegmentMgr()->getDb(sess, first, mgl::LockMode::LOCK_IS);
            if (!expdb.ok()) {
                return expdb.status();
            }
            PStore kvstore = expdb.value().store;
            std::string prefix = "rocksdb" + kvstore->dbId() + ".";

            std::string value;
            if (property == "all") {
                value = kvstore->getAllProperty();
                replaceAll(value, "rocksdb.", prefix);
                Command::fmtBulk(ss, value);
            } else {
                if (!kvstore->getProperty(property, &value)) {
                    return { ErrorCodes::ERR_PARSEOPT,
                        "invalid property " + property
                         + " in rocksdb " + kvstore->dbId() };
                }
                std::string newproperty = property;
                replaceAll(newproperty, "rocksdb.", prefix);
                Command::fmtBulk(ss, newproperty + ":" + value);
            }
        }

        return ss.str();
    }
} rocksPropCmd;

class CommandCommand : public Command {
 public:
    CommandCommand()
        :Command("command", "lt") {
    }

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

    /* Helper function for addReplyCommand() to output flags. */
    size_t addReplyCommandFlag(std::stringstream& ss, Command* cmd, int flag, const std::string& reply) {
        if (cmd->getFlags() & flag) {
            Command::fmtBulk(ss, reply);
            return 1;
        }
        return 0;
    }

    /* Output the representation of a Redis command. Used by the COMMAND command. */
    void applyCommand(std::stringstream& ss, Command* cmd) {
        if (!cmd) {
            ss << Command::fmtNull();
        } else {
            /* We are adding: command name, arg count, flags, first, last, offset */
            Command::fmtMultiBulkLen(ss, 6);
            Command::fmtBulk(ss, cmd->getName());
            Command::fmtLongLong(ss, cmd->arity());

            size_t flagcount = 0;
            std::stringstream ss2;
            flagcount += addReplyCommandFlag(ss2, cmd, CMD_WRITE, "write");
            flagcount += addReplyCommandFlag(ss2, cmd, CMD_READONLY, "readonly");
            flagcount += addReplyCommandFlag(ss2, cmd, CMD_DENYOOM, "denyoom");
            flagcount += addReplyCommandFlag(ss2, cmd, CMD_ADMIN, "admin");
            flagcount += addReplyCommandFlag(ss2, cmd, CMD_PUBSUB, "pubsub");
            flagcount += addReplyCommandFlag(ss2, cmd, CMD_NOSCRIPT, "noscript");
            flagcount += addReplyCommandFlag(ss2, cmd, CMD_RANDOM, "random");
            flagcount += addReplyCommandFlag(ss2, cmd, CMD_SORT_FOR_SCRIPT, "sort_for_script");
            flagcount += addReplyCommandFlag(ss2, cmd, CMD_LOADING, "loading");
            flagcount += addReplyCommandFlag(ss2, cmd, CMD_STALE, "stale");
            flagcount += addReplyCommandFlag(ss2, cmd, CMD_SKIP_MONITOR, "skip_monitor");
            flagcount += addReplyCommandFlag(ss2, cmd, CMD_ASKING, "asking");
            flagcount += addReplyCommandFlag(ss2, cmd, CMD_FAST, "fast");

            Command::fmtMultiBulkLen(ss, flagcount);
            ss << ss2.str();

            Command::fmtLongLong(ss, cmd->firstkey());
            Command::fmtLongLong(ss, cmd->lastkey());
            Command::fmtLongLong(ss, cmd->keystep());
        }
    }

    Expected<std::string> run(Session* sess) final {
        auto& args = sess->getArgs();

        std::stringstream ss;
        auto& cmdmap = commandMap();
        if (args.size() == 1) {
            Command::fmtMultiBulkLen(ss, cmdmap.size());
            for (auto& cmd : cmdmap) {
                applyCommand(ss, cmd.second);
            }
            return ss.str();
        }

        auto arg1 = toLower(args[1]);
        if (arg1 == "info") {
            Command::fmtMultiBulkLen(ss, args.size() - 2);
            for (size_t i = 2; i < args.size(); i++) {
                auto iter = cmdmap.find(args[i]);
                if (iter == cmdmap.end())
                    applyCommand(ss, nullptr);
                else
                    applyCommand(ss, iter->second);
            }
        } else if (arg1 == "count" && args.size() == 2) {
            Command::fmtLongLong(ss, cmdmap.size());
        } else if (arg1 == "getkeys" && args.size() >= 3) {
            auto iter = cmdmap.find(args[2]);
            if (iter == cmdmap.end()) {
                return { ErrorCodes::ERR_PARSEOPT,
                            "Invalid command specified: " + args[2] };
            }

            auto cmd = iter->second;
            if (!cmd) {
                return { ErrorCodes::ERR_PARSEOPT,
                        "Invalid command specified" };
            }

            int arity = cmd->arity();
            int realsize = args.size() - 2;
            if ((arity > 0 && arity != realsize) ||
                (realsize < -arity)) {
                return { ErrorCodes::ERR_PARSEOPT,
                    "Invalid number of arguments specified for command" };
            }

            auto first = args.begin() + 2;
            auto last = args.end();
            std::vector<std::string> newVec(first, last);

            auto keys = cmd->getKeysFromCommand(newVec);
            Command::fmtMultiBulkLen(ss, keys.size());
            for (auto i : keys) {
                Command::fmtBulk(ss, newVec[i]);
            }
        } else {
            return { ErrorCodes::ERR_PARSEOPT, "Unknown subcommand or wrong number of arguments." };
        }

        return ss.str();
    }
} commandCmd;

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
                            mgl::LockMode::LOCK_IS, false, 0);
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
                    mgl::LockMode::LOCK_IS, false, 0);
        if (!expdb.ok()) {
            return expdb.status();
        }
        PStore kvstore = expdb.value().store;
        return Command::fmtLongLong(kvstore->getHighestBinlogId());
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
                    mgl::LockMode::LOCK_IS, false, 0);
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

        LOG(INFO) << "shutdown command";
        // shutdown async
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
			// TODO(takenliu) : more information like redis
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

        auto arg1 = tendisplus::toLower(args[1]);

        if (arg1 == "id") {
            return Command::fmtLongLong(sess->id());
        } else if (arg1 == "list") {
            return listClients(sess);
        } else if (arg1 == "getname") {
            std::string name = sess->getName();
            if (name == "") {
                return Command::fmtNull();
            } else {
                return Command::fmtBulk(name);
            }
        } else if ((arg1 == "setname")
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
        } else if (arg1 == "kill") {
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

        if (sess->getArgs().size() > 2) {
            return { ErrorCodes::ERR_PARSEOPT, "invalid args size" };
        }

        bool allsections = (section == "all");
        bool defsections = (section == "default");

        infoServer(allsections, defsections, section, sess, result);
        infoClients(allsections, defsections, section, sess, result);
        infoMemory(allsections, defsections, section, sess, result);
        infoPersistence(allsections, defsections, section, sess, result);
        infoStats(allsections, defsections, section, sess, result);
        infoReplication(allsections, defsections, section, sess, result);
        infoBinlogInfo(allsections, defsections, section, sess, result);
        infoCPU(allsections, defsections, section, sess, result);
        infoCommandStats(allsections, defsections, section, sess, result);
        infoKeyspace(allsections, defsections, section, sess, result);
        infoBackup(allsections, defsections, section, sess, result);
        infoDataset(allsections, defsections, section, sess, result);
        infoCompaction(allsections, defsections, section, sess, result);
        infoLevelStats(allsections, defsections, section, sess, result);
        infoRocksdbStats(allsections, defsections, section, sess, result);
        infoRocksdbPerfStats(allsections, defsections, section, sess, result);
        infoRocksdbBgError(allsections, defsections, section, sess, result);

        return  Command::fmtBulk(result.str());
    }

    static void infoServer(bool allsections, bool defsections, const std::string& section, Session *sess, std::stringstream& result) {
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
                << "redis_git_sha1:" << TENDISPLUS_GIT_SHA1 << "\r\n"
                << "redis_git_dirty:" << TENDISPLUS_GIT_DIRTY << "\r\n"
                << "redis_build_id:" << redisBuildId() << "\r\n"
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
                //<< "run_id:" << "0000000000000000000000000000000000000000" << "\r\n" // TODO(takenliu)
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

    static void infoClients(bool allsections, bool defsections, const std::string& section, Session *sess, std::stringstream& result) {
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

    static int64_t getIntSize(const string& str) {
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
        } else if (unit == "mB") {
            return size.value() * 1024 * 1024;
        } else if (unit == "gB") {
            return size.value() * 1024 * 1024 * 1024;
        }
        LOG(ERROR) << "getIntSize failed:" << str;
        return -1;
    }

    static void infoMemory(bool allsections, bool defsections,
        const std::string& section, Session *sess, std::stringstream& result) {
        if (allsections || defsections || section == "memory") {
            auto server = sess->getServerEntry();
            std::stringstream ss;
            ss << "# Memory\r\n";

            string used_memory_vir_human;
            string used_memory_vir_peak_human;
            string used_memory_rss_human;
            string used_memory_rss_peak_human;
            size_t rss_human_size = -1;
            size_t vir_human_size = -1;

#ifndef _WIN32
            ifstream file;
            file.open("/proc/self/status");
            if (file.is_open()) {
                string strline;
                while (getline(file, strline)) {
                    auto v = stringSplit(strline, ":");
                    if (v.size() != 2) {
                        continue;
                    }
                    if (v[0] == "VmSize") {  // virtual memory
                        used_memory_vir_human = trim(v[1]);
                        strDelete(used_memory_vir_human, ' ');
                        vir_human_size = getIntSize(used_memory_vir_human);
                    } else if (v[0] == "VmPeak") {  // peak of virtual memory
                        used_memory_vir_peak_human = trim(v[1]);
                        strDelete(used_memory_vir_peak_human, ' ');
                    } else if (v[0] == "VmRSS") {  // physic memory
                        used_memory_rss_human = trim(v[1]);
                        strDelete(used_memory_rss_human, ' ');
                        rss_human_size = getIntSize(used_memory_rss_human);
                    } else if (v[0] == "VmHWM") {  // peak of physic memory
                        used_memory_rss_peak_human = trim(v[1]);
                        strDelete(used_memory_rss_peak_human, ' ');
                    }
                }
            }
#endif  // !_WIN32

            ss << "used_memory:" << -1 << "\r\n";
            ss << "used_memory_human:" << -1 << "\r\n";
            ss << "used_memory_rss:" << rss_human_size << "\r\n";
            ss << "used_memory_rss_human:" << used_memory_rss_human << "\r\n";
            ss << "used_memory_peak:" << -1 << "\r\n";
            ss << "used_memory_peak_human:" << "-1" << "\r\n";
            ss << "total_system_memory:" << "-1" << "\r\n";
            ss << "total_system_memory_human:" << "-1" << "\r\n";
            ss << "used_memory_lua:" << "-1" << "\r\n";
            ss << "used_memory_vir:" << vir_human_size << "\r\n";
            ss << "used_memory_vir_human:" << used_memory_vir_human << "\r\n";
            ss << "used_memory_vir_peak_human:" << used_memory_vir_peak_human << "\r\n";
            ss << "used_memory_rss_peak_human:" << used_memory_rss_peak_human << "\r\n";

            ss << "\r\n";
            result << ss.str();
        }
    }

    static void infoPersistence(bool allsections, bool defsections, const std::string& section, Session *sess, std::stringstream& result) {
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

    static void infoStats(bool allsections, bool defsections, const std::string& section, Session *sess, std::stringstream& result) {
        if (allsections || defsections || section == "stats") {
            auto server = sess->getServerEntry();
            std::stringstream ss;
            ss << "# Stats\r\n";
            server->getStatInfo(ss);
            ss << "\r\n";
            result << ss.str();
        }
    }

    static void infoReplication(bool allsections, bool defsections, const std::string& section, Session *sess, std::stringstream& result) {
        if (allsections || defsections || section == "replication") {
            auto server = sess->getServerEntry();
            auto replMgr = server->getReplManager();
            std::stringstream ss;
            ss << "# Replication\r\n";
            replMgr->getReplInfo(ss);
            ss << "\r\n";
            result << ss.str();
        }
    }

    static void infoCPU(bool allsections, bool defsections, const std::string& section, Session *sess, std::stringstream& result) {
#ifndef _WIN32
        if (allsections || defsections || section == "cpu") {
            auto server = sess->getServerEntry();
            std::stringstream ss;

            struct rusage self_ru, c_ru;
            getrusage(RUSAGE_SELF, &self_ru);
            getrusage(RUSAGE_CHILDREN, &c_ru);

            ss << std::fixed << std::setprecision(2);
            ss << "# CPU\r\n"
               << "used_cpu_sys:"
               << self_ru.ru_stime.tv_sec+ static_cast<float>(self_ru.ru_stime.tv_usec)/1000000 << "\r\n"
               << "used_cpu_user:"
               << self_ru.ru_utime.tv_sec+ static_cast<float>(self_ru.ru_utime.tv_usec)/1000000 << "\r\n"
               << "used_cpu_sys_children:"
               << c_ru.ru_stime.tv_sec+ static_cast<float>(c_ru.ru_stime.tv_usec)/1000000 << "\r\n"
               << "used_cpu_user_children:"
               << c_ru.ru_utime.tv_sec+ static_cast<float>(c_ru.ru_utime.tv_usec)/1000000 << "\r\n";
            ss << "\r\n";
            result << ss.str();
        }
#endif
    }

    static void infoCommandStats(bool allsections, bool defsections, const std::string& section, Session *sess, std::stringstream& result) {
        if (allsections || section == "commandstats") {
            auto server = sess->getServerEntry();
            std::stringstream ss;
            ss << "# CommandStats\r\n";
            for (const auto& kv : commandMap()) {
                auto calls = kv.second->getCallTimes();
                auto usec = kv.second->getNanos() / 1000;
                if (calls == 0)
                    continue;

                ss << "cmdstat_" << kv.first << ":calls=" << calls
                    << ",usec=" << usec
                    << ",usec_per_call=" <<
                    ((calls == 0) ? 0 : (static_cast<float>(usec) / calls))
                    << "\r\n";
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

    static void infoKeyspace(bool allsections, bool defsections, const std::string& section, Session *sess, std::stringstream& result) {
        if (allsections || defsections || section == "keyspace") {
            auto server = sess->getServerEntry();
            std::stringstream ss;
            ss << "# Keyspace\r\n";

            ss << "db0:keys=0,expires=0,avg_ttl=0\r\n";

            ss << "\r\n";
            result << ss.str();
        }
    }

    static void infoBackup(bool allsections, bool defsections, const std::string& section, Session *sess, std::stringstream& result) {
        if (allsections || defsections || section == "backup") {
            auto server = sess->getServerEntry();
            std::string runStr = server->getBackupRunning() ? "yes" : "no";
            std::stringstream ss;
            ss << "# Backup\r\n";
            ss << "backup-count:" << server->getBackupTimes() << "\r\n";
            ss << "last-backup-time:" << server->getLastBackupTime() << "\r\n";
            ss << "current-backup-running:" << runStr << "\r\n";
            if (server->getBackupFailedTimes() > 0) {
                ss << "backup-failed-count:" << server->getBackupFailedTimes() << "\r\n";
                ss << "last-backup-failed-time:" << server->getLastBackupFailedTime() << "\r\n";
                ss << "last-backup-failed-reason:" << server->getLastBackupFailedErr() << "\r\n";
            }

            ss << "\r\n";
            result << ss.str();
        }
    }

    static void infoDataset(bool allsections, bool defsections, const std::string& section, Session* sess, std::stringstream& result) {
        if (allsections || defsections || section == "dataset") {
            auto server = sess->getServerEntry();
            std::stringstream ss;
            uint64_t total = 0, live = 0, estimate = 0;
            server->getTotalIntProperty(sess, "rocksdb.total-sst-files-size", &total);
            server->getTotalIntProperty(sess, "rocksdb.live-sst-files-size", &live);
            server->getTotalIntProperty(sess, "rocksdb.estimate-live-data-size", &estimate);

            uint64_t numkeys = 0, memtables = 0, tablereaderMem = 0;
            uint64_t numCompaction = 0, mem_pending = 0, compaction_pending = 0;
            server->getTotalIntProperty(sess, "rocksdb.estimate-num-keys", &numkeys);
            server->getTotalIntProperty(sess, "rocksdb.cur-size-all-mem-tables", &memtables);
            server->getTotalIntProperty(sess, "rocksdb.estimate-table-readers-mem", &tablereaderMem);
            server->getTotalIntProperty(sess, "rocksdb.compaction-pending", &numCompaction);
            server->getTotalIntProperty(sess, "rocksdb.mem-table-flush-pending", &mem_pending);
            server->getTotalIntProperty(sess, "rocksdb.estimate-pending-compaction-bytes", &compaction_pending);


            ss << "# Dataset\r\n";
            ss << "rocksdb.kvstore-count:" << server->getKVStoreCount() << "\r\n";
            ss << "rocksdb.total-sst-files-size:" << total << "\r\n";
            ss << "rocksdb.live-sst-files-size:" << live << "\r\n";
            ss << "rocksdb.estimate-live-data-size:" << estimate << "\r\n";
            ss << "rocksdb.estimate-num-keys:" << numkeys << "\r\n";
            ss << "rocksdb.total-memory:" << memtables + tablereaderMem +
                    (uint64_t)server->getParams()->rocksBlockcacheMB * 1024 * 1024 << "\r\n";
            ss << "rocksdb.cur-size-all-mem-tables:" << memtables << "\r\n";
            ss << "rocksdb.estimate-table-readers-mem:" << tablereaderMem << "\r\n";
            ss << "rocksdb.blockcache:" << (uint64_t)server->getParams()->rocksBlockcacheMB*1024*1024 << "\r\n";
            ss << "rocksdb.mem-table-flush-pending:" << mem_pending << "\r\n";
            ss << "rocksdb.estimate-pending-compaction-bytes:" << compaction_pending << "\r\n";
            ss << "rocksdb.compaction-pending:" << numCompaction << "\r\n";
            ss << "\r\n";
            result << ss.str();
        }
    }

    static void infoCompaction(bool allsections, bool defsections, const std::string& section, Session* sess, std::stringstream& result) {
        if (allsections || defsections || section == "compaction") {
            bool running = sess->getServerEntry()->getCompactionStat().isRunning;
            auto startTime = sess->getServerEntry()->getCompactionStat().startTime;
            auto duration = sinceEpoch() - startTime;
            auto dbid = sess->getServerEntry()->getCompactionStat().curDBid;

            result << "# Compaction\r\n";
            result << "current-compaction-status:" << (running ? "running" : "stopped") << "\r\n";
            result << "time-since-lastest-compaction:" << duration << "\r\n";
            result << "current-compaction-dbid:" << dbid << "\r\n";
            result << "\r\n";
        }
    }

    static void infoLevelStats(bool allsections, bool defsections, const std::string& section, Session* sess, std::stringstream& result) {
        if (allsections || defsections || section == "levelstats") {
            auto server = sess->getServerEntry();

            result << "# Levelstats\r\n";
            for (uint64_t i = 0; i < server->getKVStoreCount(); ++i) {
                auto expdb = server->getSegmentMgr()->getDb(sess, i,
                    mgl::LockMode::LOCK_IS, false, 0);
                if (!expdb.ok()) {
                    continue;
                }

                auto store = expdb.value().store;
                std::string tmp;
                if (!store->getProperty("rocksdb.levelstatsex", &tmp)) {
                    LOG(WARNING) << "store id " << i << " rocksdb.levelstatsex not supported";
                    continue;
                }
                std::string prefix = "rocksdb" + store->dbId() + ".";
                replaceAll(tmp, "rocksdb.", prefix);

                result << tmp;
            }

            result << "\r\n";
        }
    }

    static void infoRocksdbStats(bool allsections, bool defsections, const std::string& section, Session* sess, std::stringstream& result) {
        if (allsections || section == "rocksdbstats") {
            auto server = sess->getServerEntry();
            std::map<std::string, uint64_t> map;

            result << "# Rocksdbstats\r\n";
            for (uint64_t i = 0; i < server->getKVStoreCount(); ++i) {
                auto expdb = server->getSegmentMgr()->getDb(sess, i,
                    mgl::LockMode::LOCK_IS, false, 0);
                if (!expdb.ok()) {
                    continue;
                }

                auto store = expdb.value().store;
                std::string tmp = store->getStatistics();

                auto v = stringSplit(tmp, "\n");
                for (auto one : v) {
                    // rocksdb1.block.cache.bytes.write COUNT : 0
                    auto key_value = stringSplit(one, ":");
                    if (key_value.size() != 2) {
                        continue;
                    }
                    sdstrim(key_value[0], " ");
                    sdstrim(key_value[1], " ");
                    if (key_value[0].find("COUNT") == string::npos) {
                        continue;
                    }

                    auto e = tendisplus::stoul(key_value[1]);
                    if (!e.ok()) {
                        continue;
                    }

                    auto iter = map.find(key_value[0]);
                    if (iter != map.end()) {
                        iter->second += e.value();
                    } else {
                        map[key_value[0]] = e.value();
                    }
                }

                std::string prefix = "rocksdb" + store->dbId() + ".";
                replaceAll(tmp, "rocksdb.", prefix);
                replaceAll(tmp, "\n", "\r\n");

                result << tmp;
            }

            for (auto v : map) {
                result << v.first << " : " << v.second << "\r\n";
            }

            result << "\r\n";
        }
    }

    static void infoRocksdbPerfStats(bool allsections, bool defsections, const std::string& section, Session* sess, std::stringstream& result) {
        if (allsections || section == "rocksdbperfstats") {
            result << "# RocksdbPerfstats\r\n";

            auto tmp = sess->getCtx()->getPerfContextStr();
            replaceAll(tmp, " = ", ":");
            replaceAll(tmp, ", ", "\r\n");

            result << tmp;

            tmp = sess->getCtx()->getIOstatsContextStr();
            replaceAll(tmp, " = ", ":");
            replaceAll(tmp, ", ", "\r\n");

            result << tmp;
            result << "\r\n";
        }
    }

    static void infoBinlogInfo(bool allsections, bool defsections, const std::string& section, Session* sess, std::stringstream& result) {
        if (allsections || section == "binloginfo") {
            auto server = sess->getServerEntry();

            result << "# BinlogInfo\r\n";
            for (uint32_t i = 0; i < server->getKVStoreCount(); i++) {
                auto expdb = server->getSegmentMgr()->getDb(sess, i,
                    mgl::LockMode::LOCK_IS, false, 0);
                if (!expdb.ok()) {
                    continue;
                }
                PStore kvstore = expdb.value().store;
                auto ptxn = kvstore->createTransaction(sess);
                if (!ptxn.ok()) {
                    continue;
                }
                std::unique_ptr<Transaction> txn = std::move(ptxn.value());
                auto eMin = RepllogCursorV2::getMinBinlog(txn.get());
                if (!eMin.ok()) {
                    if (eMin.status().code() == ErrorCodes::ERR_EXHAUST) {
                        continue;
                    }
                    continue;
                }
                auto eMax = RepllogCursorV2::getMaxBinlog(txn.get());
                if (!eMax.ok()) {
                    continue;
                }
                result << "rocksdb" + kvstore->dbId() << ":"
                    << "min=" << eMin.value().getBinlogId()
                    << ",minTs=" << eMin.value().getTimestamp()
                    << ",minRevision=" << (int64_t)eMin.value().getVersionEp()
                    << ",max=" << eMax.value().getBinlogId()
                    << ",maxTs=" << eMax.value().getTimestamp()
                    << ",maxRevision=" << (int64_t)eMax.value().getVersionEp()
                    << ",highestVisble=" << kvstore->getHighestBinlogId() << "\r\n";
            }

            result << "\r\n";
        }
    }

    static void infoRocksdbBgError(bool allsections, bool defsections, const std::string& section, Session* sess, std::stringstream& result) {
        if (allsections || defsections || section == "rocksdbbgerror") {
            auto server = sess->getServerEntry();

            result << "# RocksdbBgError\r\n";
            for (uint64_t i = 0; i < server->getKVStoreCount(); ++i) {
                auto expdb = server->getSegmentMgr()->getDb(sess, i,
                    mgl::LockMode::LOCK_IS, false, 0);
                if (!expdb.ok()) {
                    continue;
                }

                auto store = expdb.value().store;
                auto ret = store->getBgError();
                if (ret != "") {
                    result << "rocksdb" << store->dbId() << ":"
                        << ret << "\n";
                }
            }
            result << "\r\n";
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
                } else if (toLower(args[3]) == "perf_level") {
                    if (!sess->getCtx()->setPerfLevel(toLower(args[4]))) {
                        return{ ErrorCodes::ERR_PARSEOPT,
                            "invalid argument :\"" + args[4] + "\" for 'config set session perf_level'" };
                    }
                } else {
                    return{ ErrorCodes::ERR_PARSEOPT,
                        "invalid argument :\"" + args[3] + "\" for 'config set session'" };
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

            if (args.size() != 3) {
                return{ ErrorCodes::ERR_PARSEOPT, "args size incorrect, should be three" };
            }

            auto configName = toLower(args[2]);
            if (configName == "all") {
                reset_all = true;
            }

            if (reset_all || configName == "unseencommands") {
                LOG(INFO) << "reset unseencommands";
                std::lock_guard<std::mutex> lk(Command::_mutex);
                for (const auto& kv : _unSeenCmds) {
                    LOG(INFO) << "unseencommand:" << kv.first << " call-times:" << kv.second;
                }
                _unSeenCmds.clear();
            }
            if (reset_all || configName == "commandstats") {
                LOG(INFO) << "reset commandstats";
                std::stringstream ss;
                InfoCommand::infoCommandStats(true, true, "commandstats", sess, ss);
                LOG(INFO) << ss.str();
                for (const auto& kv : commandMap()) {
                    kv.second->resetStatInfo();
                }
            }
            if (reset_all || configName == "stats") {
                LOG(INFO) << "reset stats";
                std::stringstream ss;
                InfoCommand::infoStats(true, true, "stats", sess, ss);
                LOG(INFO) << ss.str();

                auto svr = sess->getServerEntry();
                svr->resetServerStat();
            }
            if (reset_all || configName == "rocksdbstats") {
                LOG(INFO) << "reset rocksdbstats";
                std::stringstream ss;
                InfoCommand::infoRocksdbStats(true, true, "rocksdbstats", sess, ss);
                LOG(INFO) << ss.str();
                auto svr = sess->getServerEntry();
                svr->resetRocksdbStats(sess);
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
            if (!eflush.ok()) {
                return eflush.status();
            }

            // it is the first txn and binlog, because of LOCK_X
            INVARIANT_COMPARE_D(eflush.value(), ==, nextBinlogid);

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
        // if (sess->getCtx()->getDbId() != 0) {
        //    return{ ErrorCodes::ERR_PARSEOPT, "only support db 0" };
        // }

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
        INVARIANT_D(store->isRunning() && store->isPaused());

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
        INVARIANT_D(store->isRunning() && !store->isPaused());

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
        // get ts && version from kvstores
        if (args[2] == "?" && args[3] == "?") {
            VersionMeta minMeta(UINT64_MAX - 1, UINT64_MAX - 1, name);
            for (uint32_t i = 0; i < server->getKVStoreCount(); i++) {
                auto expdb = server->getSegmentMgr()->getDb(sess,
                    i, mgl::LockMode::LOCK_IS);
                if (!expdb.ok()) {
                    return expdb.status();
                }
                PStore store = expdb.value().store;
                auto meta = store->getVersionMeta(name);
                if (!meta.ok()) {
                    return meta.status();
                }
                VersionMeta metaV = *(meta.value().get());
                minMeta = std::min(metaV, minMeta);
            }
            std::stringstream ss;
            Command::fmtMultiBulkLen(ss, 2);
            Command::fmtLongLong(ss, static_cast<int64_t>(minMeta.getTimeStamp()));
            Command::fmtLongLong(ss, static_cast<int64_t>(minMeta.getVersion()));
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
        for (uint32_t i = 0; i < server->getKVStoreCount(); i++) {
            auto expdb =
                server->getSegmentMgr()->getDb(sess, i, mgl::LockMode::LOCK_IS);
            if (!expdb.ok()) {
                return expdb.status();
            }
            PStore store = expdb.value().store;
            auto s = store->setVersionMeta(name, eTs.value(), eVersion.value());
            if (!s.ok()) {
                return s;
            }
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
        return {ErrorCodes::ERR_INTERNAL, "invalid command"};
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

class reshapeCommand: public Command {
 public:
    reshapeCommand()
            :Command("reshape", "sM") {
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
        const auto server = sess->getServerEntry();
        const auto& args = sess->getArgs();
        if (server->getCompactionStat().isRunning) {
            return { ErrorCodes::ERR_INTERNAL, "reshape is already running" };
        }

        const auto guard = MakeGuard([this, &server] {
            server->getCompactionStat().reset();
         });

        if (args.size() == 2) {
            auto expStoreId = tendisplus::stoull(args[1]);
            if (!expStoreId.ok()) {
                return expStoreId.status();
            }
            uint64_t storeid = expStoreId.value();
            auto expdb = server->getSegmentMgr()->getDb(sess, storeid,
                    mgl::LockMode::LOCK_IS);
            if (!expdb.ok()) {
                if (expdb.status().code() == ErrorCodes::ERR_STORE_NOT_OPEN) {
                    return {ErrorCodes::ERR_STORE_NOT_OPEN, ""};
                }
                return expdb.status();
            }
            PStore kvstore = expdb.value().store;
            server->getCompactionStat().isRunning = true;
            server->getCompactionStat().curDBid = kvstore->dbId();
            server->getCompactionStat().startTime = sinceEpoch();
            auto status = kvstore->fullCompact();
            if (!status.ok()) {
                return status;
            }
        } else {
            server->getCompactionStat().isRunning = true;
            server->getCompactionStat().startTime = sinceEpoch();
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
                server->getCompactionStat().curDBid = kvstore->dbId();
                auto status = kvstore->fullCompact();
                if (!status.ok()) {
                    return status;
                }
            }
        }
        return Command::fmtOK();
    }
} reshapeCmd;

class EmptyIntCommand: public Command {
 public:
    EmptyIntCommand()
            :Command("emptyint", "rF") {
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
        return Command::fmtLongLong(0);
    }
} emptyintCmd;

class EmptyOKCommand: public Command {
 public:
    EmptyOKCommand()
            :Command("emptyok", "rF") {
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
        return Command::fmtOK();
    }
} emptyokCmd;

class EmptyBulkCommand: public Command {
 public:
    EmptyBulkCommand()
            :Command("emptybulk", "rF") {
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
        std::stringstream ss;
        Command::fmtBulk(ss, "");
        return ss.str();
    }
} emptyBulkCmd;

class EmptyMultiBulkCommand: public Command {
 public:
    EmptyMultiBulkCommand()
            :Command("emptymultibulk", "rF") {
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
        std::stringstream ss;
        Command::fmtMultiBulkLen(ss, 0);
        return ss.str();
    }
} emptyMultiBulkCmd;

class TendisadminCommand : public Command {
 public:
    TendisadminCommand()
        :Command("tendisadmin", "lat") {
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

    Expected<std::string> run(Session* sess) final {
        auto& args = sess->getArgs();

        auto operation = toLower(args[1]);

        if (operation == "sleep") {
            if (args.size() != 3) {
                return{ ErrorCodes::ERR_PARSEOPT, "args size incorrect!" };
            }

            auto time = tendisplus::stoull(args[2]);
            if (!time.ok()) {
                return time.status();
            }

            std::list<std::unique_ptr<DbWithLock>> result;
            const auto server = sess->getServerEntry();
            for (ssize_t i = 0; i < server->getKVStoreCount(); i++) {
                auto expdb = server->getSegmentMgr()->getDb(sess,
                    i, mgl::LockMode::LOCK_X);
                if (!expdb.ok()) {
                    return expdb.status();
                }
                result.emplace_back(std::make_unique<DbWithLock>(std::move(expdb.value())));
            }

            std::this_thread::sleep_for(std::chrono::seconds(time.value()));
        } else if (operation == "recovery") {
            if (args.size() != 2) {
                return{ ErrorCodes::ERR_PARSEOPT, "args size incorrect!" };
            }
            const auto server = sess->getServerEntry();
            for (ssize_t i = 0; i < server->getKVStoreCount(); i++) {
                auto expdb = server->getSegmentMgr()->getDb(sess,
                    i, mgl::LockMode::LOCK_IX);
                if (!expdb.ok()) {
                    return expdb.status();
                }

                auto s = expdb.value().store->recoveryFromBgError();
                if (!s.ok()) {
                    return s;
                }
            }
        } else {
            return{ ErrorCodes::ERR_PARSEOPT, "invalid operation:" + operation };
        }

        return Command::fmtOK();
    }
} tendisadminCmd;

}  // namespace tendisplus
