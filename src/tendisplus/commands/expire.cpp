#include <string>
#include <utility>
#include <memory>
#include <algorithm>
#include <cctype>
#include <clocale>
#include <map>
#include "glog/logging.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/time.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

Expected<bool> expireBeforeNow(Session *sess,
                        RecordType type,
                        const std::string& key) {
    return Command::delKeyChkExpire(sess, key, type);
}

// return true if exists
// return false if not exists
// return error if has error
Expected<bool> expireAfterNow(Session *sess,
                        RecordType type,
                        const std::string& key,
                        uint64_t expireAt) {
    Expected<RecordValue> rv =
        Command::expireKeyIfNeeded(sess, key, type);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
        return false;
    } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
        return false;
    } else if (!rv.status().ok()) {
        return rv.status();
    }

    INVARIANT_D(type == RecordType::RT_DATA_META);
    // record exists and not expired
    auto server = sess->getServerEntry();
    auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess,
                key, mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
        return expdb.status();
    }
    // uint32_t storeId = expdb.value().dbId;
    PStore kvstore = expdb.value().store;
    SessionCtx *pCtx = sess->getCtx();
    RecordKey rk(expdb.value().chunkId, pCtx->getDbId(), type, key, "");
    // if (Command::isKeyLocked(sess, storeId, rk.encode())) {
    //     return {ErrorCodes::ERR_BUSY, "key locked"};
    // }
    for (uint32_t i = 0; i < Command::RETRY_CNT; ++i) {
        auto ptxn = kvstore->createTransaction(sess);
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        Expected<RecordValue> eValue = kvstore->getKV(rk, txn.get());
        if (eValue.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return false;
        } else if (!eValue.ok()) {
            return eValue.status();
        }
        auto rv = eValue.value();
        auto vt = rv.getRecordType();
        Status s;

        if (vt != RecordType::RT_KV) {
            if (!Command::noExpire()) {
                // delete old index entry
                auto oldTTL = rv.getTtl();
                if (oldTTL != 0) {
                    TTLIndex o_ictx(key, vt, pCtx->getDbId(), oldTTL);

                    s = txn->delKV(o_ictx.encode());
                    if (!s.ok()) {
                        return s;
                    }
                }

                // add new index entry
                TTLIndex n_ictx(key, vt, pCtx->getDbId(), expireAt);
                s = txn->setKV(n_ictx.encode(),
                    RecordValue(RecordType::RT_TTL_INDEX).encode());
                if (!s.ok()) {
                    return s;
                }
            }
        }

        // update
        rv.setTtl(expireAt);
        rv.setVersionEP(pCtx->getVersionEP());
        s = kvstore->setKV(rk, rv, txn.get());
        if (!s.ok()) {
            return s;
        }

        auto commitStatus = txn->commit();
        s = commitStatus.status();
        if (s.ok()) {
            return true;
        } else if (s.code() != ErrorCodes::ERR_COMMIT_RETRY) {
            return s;
        }
        // status == ERR_COMMIT_RETRY
        if (i == Command::RETRY_CNT - 1) {
            return s;
        } else {
            continue;
        }
    }

    INVARIANT_D(0);
    return {ErrorCodes::ERR_INTERNAL, "not reachable"};
}

Expected<std::string> expireGeneric(Session *sess,
                                    int64_t expireAt,
                                    const std::string& key) {
    if (expireAt >= (int64_t)msSinceEpoch()) {
        bool atLeastOne = false;
        for (auto type : {RecordType::RT_DATA_META}) {
            auto done = expireAfterNow(sess, type, key, expireAt);
            if (!done.ok()) {
                return done.status();
            }
            atLeastOne |= done.value();
        }
        return atLeastOne ? Command::fmtOne() : Command::fmtZero();
    } else {
        bool atLeastOne = false;
        for (auto type : {RecordType::RT_DATA_META}) {
            auto done = expireBeforeNow(sess, type, key);
            DLOG(INFO) << " expire before " << key << " " << rt2Char(type);
            if (!done.ok()) {
                return done.status();
            }
            atLeastOne |= done.value();
        }
        return atLeastOne ? Command::fmtOne() : Command::fmtZero();
    }
    INVARIANT_D(0);
    return {ErrorCodes::ERR_INTERNAL, "not reachable"};
}

class GeneralExpireCommand: public Command {
 public:
    GeneralExpireCommand(const std::string& name, const char* sflags)
        :Command(name, sflags) {
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

    Expected<std::string> run(Session *sess) final {
        const std::string& key = sess->getArgs()[1];
        auto expt = ::tendisplus::stoll(sess->getArgs()[2]);
        if (!expt.ok()) {
            return expt.status();
        }

        int64_t millsecs = 0;
        if (Command::getName() == "expire") {
            millsecs = msSinceEpoch() + expt.value()*1000;
        } else if (Command::getName() == "pexpire") {
            millsecs = msSinceEpoch() + expt.value();
        } else if (Command::getName() == "expireat") {
            millsecs = expt.value()*1000;
        } else if (Command::getName() == "pexpireat") {
            millsecs = expt.value();
        } else {
            INVARIANT_D(0);
        }
        return expireGeneric(sess, millsecs, key);
    }
};

class ExpireCommand: public GeneralExpireCommand {
 public:
    ExpireCommand()
        :GeneralExpireCommand("expire", "wF") {
    }
} expireCmd;

class PExpireCommand: public GeneralExpireCommand {
 public:
    PExpireCommand()
        :GeneralExpireCommand("pexpire", "wF") {
    }
} pexpireCmd;

class ExpireAtCommand: public GeneralExpireCommand {
 public:
    ExpireAtCommand()
        :GeneralExpireCommand("expireat", "wF") {
    }
} expireatCmd;

class PExpireAtCommand: public GeneralExpireCommand {
 public:
    PExpireAtCommand()
        :GeneralExpireCommand("pexpireat", "wF") {
    }
} pexpireatCmd;

class GenericTtlCommand: public Command {
 public:
    GenericTtlCommand(const std::string& name, const char* sflags)
        :Command(name, sflags) {
    }

    Expected<std::string> run(Session *sess) final {
        const std::string& key = sess->getArgs()[1];

        for (auto type : {RecordType::RT_DATA_META}) {
            Expected<RecordValue> rv =
                Command::expireKeyIfNeeded(sess, key, type);
            if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
                continue;
            } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
                continue;
            } else if (!rv.ok()) {
                return rv.status();
            }
            if (rv.value().getTtl() == 0) {
                return Command::fmtLongLong(-1);
            }
            int64_t ms = rv.value().getTtl() - msSinceEpoch();
            if (ms < 0) {
                ms = 1;
            }
            if (Command::getName() == "ttl") {
                return Command::fmtLongLong((ms + 500) / 1000);
            } else if (Command::getName() == "pttl") {
                return Command::fmtLongLong(ms);
            } else {
                INVARIANT_D(0);
            }
        }
        return Command::fmtLongLong(-2);
    }
};

class TtlCommand: public GenericTtlCommand {
 public:
    TtlCommand()
        :GenericTtlCommand("ttl", "rF") {
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
} ttlCmd;

class PTtlCommand: public GenericTtlCommand {
 public:
    PTtlCommand()
        :GenericTtlCommand("pttl", "rF") {
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
} pttlCmd;

class ExistsCommand: public Command {
 public:
    ExistsCommand()
        :Command("exists", "rF") {
    }

    ssize_t arity() const {
        return -2;
    }

    int32_t firstkey() const {
        return 1;
    }

    int32_t lastkey() const {
        return -1;
    }

    int32_t keystep() const {
        return 1;
    }

    Expected<std::string> run(Session *sess) final {
        auto& args = sess->getArgs();
        size_t count = 0;

        for (size_t j = 1; j < args.size(); j++) {
            const std::string& key = args[j];

            Expected<RecordValue> rv =
                Command::expireKeyIfNeeded(sess, key, RecordType::RT_DATA_META);
            if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
                continue;
            } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
                continue;
            } else if (!rv.ok()) {
                return rv.status();
            }
            count++;
        }
        return Command::fmtLongLong(count);
    }
} existsCmd;

class TypeCommand: public Command {
 public:
    TypeCommand()
        :Command("type", "rF") {
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
        const std::string& key = sess->getArgs()[1];

        const std::map<RecordType, std::string> lookup = {
            {RecordType::RT_KV, "string"},
            {RecordType::RT_LIST_META, "list"},
            {RecordType::RT_HASH_META, "hash"},
            {RecordType::RT_SET_META, "set"},
            {RecordType::RT_ZSET_META, "zset"},
        };
        for (const auto& typestr : {RecordType::RT_DATA_META}) {
            Expected<RecordValue> rv =
                Command::expireKeyIfNeeded(sess, key, typestr);
            if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
                continue;
            } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
                continue;
            } else if (!rv.ok()) {
                return rv.status();
            }
            auto vt = rv.value().getRecordType();
            return Command::fmtBulk(lookup.at(vt));
        }
        return Command::fmtBulk("none");
    }
} typeCmd;

class PersistCommand : public Command {
 public:
     PersistCommand()
        :Command("persist", "wF") {
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
        const std::string& key = sess->getArgs()[1];

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);

        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key,
                        mgl::LockMode::LOCK_X);
        if (!expdb.ok()) {
            return expdb.status();
        }

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_DATA_META);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
            rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtZero();
        } else if (!rv.ok()) {
            return rv.status();
        }

        // change the ttl of rv
        rv.value().setTtl(0);
        auto vt = rv.value().getRecordType();
        RecordKey mk(expdb.value().chunkId, sess->getCtx()->getDbId(),
                    vt, key, "");

        PStore kvstore = expdb.value().store;
        auto ptxn = kvstore->createTransaction(sess);
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());

        auto s = kvstore->setKV(mk, rv.value(), txn.get());
        if (!s.ok()) {
            return s;
        }

        auto s1 = txn->commit();
        if (!s1.ok()) {
            return s1.status();
        }

        return Command::fmtOne();
    }
} persistCmd;

}  // namespace tendisplus
