#include <string>
#include <utility>
#include <memory>
#include <algorithm>
#include <cctype>
#include <clocale>
#include "glog/logging.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

constexpr int32_t REDIS_SET_NO_FLAGS = 0;

// set if not exists
constexpr int32_t REDIS_SET_NX = (1<<0);

// set if exists
constexpr int32_t REDIS_SET_XX = (1<<1);

// set and expire if not exists
constexpr int32_t REDIS_SET_NXEX = (1<<2);

struct SetParams {
    SetParams()
        :key(""),
         value(""),
         flags(REDIS_SET_NO_FLAGS),
         expire(0) {
    }
    std::string key;
    std::string value;
    int32_t flags;
    uint64_t expire;
};

// TODO(deyukong): unittest of expire
Expected<std::string> setGeneric(PStore store, Transaction *txn,
            int32_t flags, const RecordKey& key, const RecordValue& val,
            const std::string& okReply, const std::string& abortReply) {
    if ((flags & REDIS_SET_NX) || (flags & REDIS_SET_XX)
            || (flags & REDIS_SET_NXEX)) {
        Expected<RecordValue> eValue = store->getKV(key, txn);
        if ((!eValue.ok()) &&
                eValue.status().code() != ErrorCodes::ERR_NOTFOUND) {
            return eValue.status();
        }

        uint64_t currentTs = 0;
        uint64_t targetTtl = 0;
        if (eValue.ok()) {
            currentTs = nsSinceEpoch()/1000000;
            targetTtl = eValue.value().getTtl();
        }
        bool needExpire = (targetTtl != 0
                           && currentTs >= eValue.value().getTtl());
        bool exists =
            (eValue.status().code() == ErrorCodes::ERR_OK) && (!needExpire);
        if ((flags & REDIS_SET_NX && exists) ||
                (flags & REDIS_SET_XX && (!exists)) ||
                (flags & REDIS_SET_NXEX && exists)) {
            // we will early return, we should del the expired key
            // if needed.
            if (needExpire) {
                Status status = store->delKV(key, txn);
                if (!status.ok()) {
                    return status;
                }
                Expected<uint64_t> exptCommit = txn->commit();
                if (!exptCommit.ok()) {
                    return exptCommit.status();
                }
            }
            return abortReply == "" ? Command::fmtNull() : abortReply;
        }
    }

    // here we have no need to check expire since we will overwrite it
    Status status = store->setKV(key, val, txn);
    TEST_SYNC_POINT("setGeneric::SetKV::1");
    if (!status.ok()) {
        return status;
    }
    Expected<uint64_t> exptCommit = txn->commit();
    if (!exptCommit.ok()) {
        return exptCommit.status();
    }
    return okReply == "" ? Command::fmtOK() : okReply;
}

class SetCommand: public Command {
 public:
    SetCommand()
        :Command("set") {
    }

    Expected<SetParams> parse(Session *sess) const {
        const auto& args = sess->getArgs();
        SetParams result;
        if (args.size() < 3) {
            return {ErrorCodes::ERR_PARSEPKT, "invalid set params"};
        }
        result.key = args[1];
        result.value = args[2];
        try {
            for (size_t i = 3; i < args.size(); i++) {
                const std::string& s = toLower(args[i]);
                if (s == "nx") {
                    result.flags |= REDIS_SET_NX;
                } else if (s == "xx") {
                    result.flags |= REDIS_SET_XX;
                } else if (s == "ex" && i+1 < args.size()) {
                    result.expire = std::stoul(args[i+1])*1000ULL;
                    i++;
                } else if (s == "px" && i+1 < args.size()) {
                    result.expire = std::stoul(args[i+1]);
                    i++;
                } else {
                    return {ErrorCodes::ERR_PARSEPKT, "syntax error"};
                }
            }
        } catch (std::exception& ex) {
            LOG(WARNING) << "parse setParams failed:" << ex.what();
            return {ErrorCodes::ERR_PARSEPKT,
                    "value is not an integer or out of range"};
        }
        return result;
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
        Expected<SetParams> exptParams = parse(sess);
        if (!exptParams.ok()) {
            return exptParams.status();
        }

        // NOTE(deyukong): no need to do a expireKeyIfNeeded
        // on a simple kv. We will overwrite it.
        const SetParams& params = exptParams.value();
        auto storeLock = Command::lockDBByKey(sess,
                                              params.key,
                                              mgl::LockMode::LOCK_IX);
        PStore kvstore = Command::getStore(sess, params.key);
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);

        RecordKey rk(pCtx->getDbId(), RecordType::RT_KV,
                     params.key, "");

        uint64_t ts = 0;
        if (params.expire != 0) {
            ts = nsSinceEpoch() / 1000000 + params.expire;
        }
        RecordValue rv(params.value, ts);

        for (int32_t i = 0; i < RETRY_CNT - 1; ++i) {
            auto result = setGeneric(kvstore, txn.get(), params.flags,
                                     rk, rv, "", "");
            if (result.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
                return result;
            }
            ptxn = kvstore->createTransaction();
            if (!ptxn.ok()) {
                return ptxn.status();
            }
            txn = std::move(ptxn.value());
        }
        return setGeneric(kvstore, txn.get(), params.flags,
            rk, rv, "", "");
    }
} setCommand;

class SetExCommand: public Command {
 public:
    SetExCommand()
        :Command("setex") {
    }

    ssize_t arity() const {
        return 4;
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
        const std::string& val = sess->getArgs()[3];
        Expected<uint64_t> eexpire = ::tendisplus::stoul(sess->getArgs()[2]);
        if (!eexpire.ok()) {
            return eexpire.status();
        }
        uint32_t storeId = Command::getStoreId(sess, key);
        auto storeLock = Command::lockDBByKey(sess,
                                              key,
                                              mgl::LockMode::LOCK_IX);
        PStore kvstore = Command::getStoreById(sess, storeId);
        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);

        RecordKey rk(pCtx->getDbId(), RecordType::RT_KV,
                     key, "");
        RecordValue rv(val, eexpire.value());
        for (int32_t i = 0; i < RETRY_CNT; ++i) {
            auto ptxn = kvstore->createTransaction();
            if (!ptxn.ok()) {
                return ptxn.status();
            }
            std::unique_ptr<Transaction> txn = std::move(ptxn.value());
            auto result = setGeneric(kvstore,
                                     txn.get(),
                                     REDIS_SET_NO_FLAGS,
                                     rk,
                                     rv,
                                     "",
                                     "");
            if (result.ok()) {
                return result.value();
            }
            if (result.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
                return result.status();
            }
            if (i == RETRY_CNT - 1) {
                return result.status();
            } else {
                continue;
            }
        }

        INVARIANT(0);
        return {ErrorCodes::ERR_INTERNAL, "not reachable"};
    }
} setexCmd;

class SetNxCommand: public Command {
 public:
    SetNxCommand()
        :Command("setnx") {
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
        const std::string& val = sess->getArgs()[2];
        uint32_t storeId = Command::getStoreId(sess, key);
        auto storeLock = Command::lockDBByKey(sess,
                                              key,
                                              mgl::LockMode::LOCK_IX);
        PStore kvstore = Command::getStoreById(sess, storeId);
        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);

        RecordKey rk(pCtx->getDbId(), RecordType::RT_KV,
                     key, "");
        RecordValue rv(val);
        for (int32_t i = 0; i < RETRY_CNT; ++i) {
            auto ptxn = kvstore->createTransaction();
            if (!ptxn.ok()) {
                return ptxn.status();
            }
            std::unique_ptr<Transaction> txn = std::move(ptxn.value());
            auto result = setGeneric(kvstore,
                                     txn.get(),
                                     REDIS_SET_NX,
                                     rk,
                                     rv,
                                     Command::fmtOne(),
                                     Command::fmtZero());
            if (result.ok()) {
                return result.value();
            }
            if (result.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
                return result.status();
            }
            if (i == RETRY_CNT - 1) {
                return result.status();
            } else {
                continue;
            }
        }

        INVARIANT(0);
        return {ErrorCodes::ERR_INTERNAL, "not reachable"};
    }
} setnxCmd;

class GetCommand: public Command {
 public:
    GetCommand()
        :Command("get") {
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
        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);
        const std::string& key = sess->getArgs()[1];
        RecordKey rk(pCtx->getDbId(), RecordType::RT_KV, key, "");
        uint32_t storeId = Command::getStoreId(sess, key);
        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, storeId, rk);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
            return fmtNull();
        } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return fmtNull();
        } else if (!rv.status().ok()) {
            return rv.status();
        } else {
            return fmtBulk(rv.value().getValue());
        }
    }
} getCommand;

class IncrDecrGeneral: public Command {
 public:
    explicit IncrDecrGeneral(const std::string& name)
        :Command(name) {
    }

    Expected<std::string> runWithParam(Session *sess,
                                       const std::string& key,
                                       int64_t incr) {
        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);

        // expire if possible
        RecordKey rk(pCtx->getDbId(), RecordType::RT_KV, key, "");
        uint32_t storeId = Command::getStoreId(sess, key);
        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, storeId, rk);
        if (rv.status().code() != ErrorCodes::ERR_OK &&
                rv.status().code() != ErrorCodes::ERR_EXPIRED &&
                rv.status().code() != ErrorCodes::ERR_NOTFOUND) {
            return rv.status();
        }

        auto storeLock = Command::lockDBByKey(sess,
                                              key,
                                              mgl::LockMode::LOCK_IX);
        PStore kvstore = Command::getStoreById(sess, storeId);

        for (int32_t i = 0; i < RETRY_CNT; ++i) {
            int64_t sum = 0;
            auto ptxn = kvstore->createTransaction();
            if (!ptxn.ok()) {
                return ptxn.status();
            }
            std::unique_ptr<Transaction> txn = std::move(ptxn.value());
            Expected<RecordValue> eValue = kvstore->getKV(rk, txn.get());
            if (!eValue.ok()
                    && eValue.status().code() != ErrorCodes::ERR_NOTFOUND) {
                return eValue.status();
            }
            if (eValue.ok()) {
                Expected<int64_t> val =
                    ::tendisplus::stoll(eValue.value().getValue());
                if (!val.ok()) {
                    return {ErrorCodes::ERR_DECODE,
                            "value is not an integer or out of range"};
                }
                sum = val.value();
            }

            if ((incr < 0 && sum < 0 && incr < (LLONG_MIN-sum)) ||
                    (incr > 0 && sum > 0 && incr > (LLONG_MAX-sum))) {
                return {ErrorCodes::ERR_OVERFLOW,
                        "increment or decrement would overflow"};
            }
            sum += incr;
            RecordValue newSum(std::to_string(sum));
            auto result = setGeneric(kvstore,
                                     txn.get(),
                                     REDIS_SET_NO_FLAGS,
                                     rk, newSum, "", "");
            if (result.ok()) {
                return Command::fmtLongLong(sum);
            }
            if (result.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
                return result.status();
            }
            if (i == RETRY_CNT - 1) {
                return result.status();
            } else {
                continue;
            }
        }

        INVARIANT(0);
        return {ErrorCodes::ERR_INTERNAL, "not reachable"};
    }
};

class IncrbyCommand: public IncrDecrGeneral {
 public:
    IncrbyCommand()
        :IncrDecrGeneral("incrby") {
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

    Expected<std::string> run(Session *sess) {
        const std::string& key = sess->getArgs()[1];
        const std::string& val = sess->getArgs()[2];
        Expected<int64_t> eInc = ::tendisplus::stoll(val);
        if (!eInc.ok()) {
            return eInc.status();
        }
        return runWithParam(sess, key, eInc.value());
    }
} incrbyCmd;

class IncrCommand: public IncrDecrGeneral {
 public:
    IncrCommand()
        :IncrDecrGeneral("incr") {
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

    Expected<std::string> run(Session *sess) {
        const std::string& key = sess->getArgs()[1];
        return runWithParam(sess, key, 1);
    }
} incrCmd;

class DecrbyCommand: public IncrDecrGeneral {
 public:
    DecrbyCommand()
        :IncrDecrGeneral("decrby") {
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

    Expected<std::string> run(Session *sess) {
        const std::string& key = sess->getArgs()[1];
        const std::string& val = sess->getArgs()[2];
        Expected<int64_t> eInc = ::tendisplus::stoll(val);
        if (!eInc.ok()) {
            return eInc.status();
        }
        return runWithParam(sess, key, -eInc.value());
    }
} decrbyCmd;

class DecrCommand: public IncrDecrGeneral {
 public:
    DecrCommand()
        :IncrDecrGeneral("decr") {
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

    Expected<std::string> run(Session *sess) {
        const std::string& key = sess->getArgs()[1];
        return runWithParam(sess, key, -1);
    }
} decrCmd;

}  // namespace tendisplus
