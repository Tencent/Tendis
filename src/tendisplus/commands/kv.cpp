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
#include "tendisplus/utils/redis_port.h"
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

class SetexGeneralCommand: public Command {
 public:
    explicit SetexGeneralCommand(const std::string& name)
        :Command(name) {
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

    Expected<std::string> runGeneral(Session *sess,
                                     const std::string& key,
                                     const std::string& val,
                                     uint64_t ttl) {
        uint32_t storeId = Command::getStoreId(sess, key);
        auto storeLock = Command::lockDBByKey(sess,
                                              key,
                                              mgl::LockMode::LOCK_IX);
        PStore kvstore = Command::getStoreById(sess, storeId);
        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);

        RecordKey rk(pCtx->getDbId(), RecordType::RT_KV, key, "");
        RecordValue rv(val, ttl);
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
};

class SetExCommand: public SetexGeneralCommand {
 public:
    SetExCommand()
        :SetexGeneralCommand("setex") {
    }

    Expected<std::string> run(Session *sess) final {
        const std::string& key = sess->getArgs()[1];
        const std::string& val = sess->getArgs()[3];
        Expected<uint64_t> eexpire = ::tendisplus::stoul(sess->getArgs()[2]);
        if (!eexpire.ok()) {
            return eexpire.status();
        }
        return runGeneral(sess, key, val,
                          nsSinceEpoch()/1000000 + eexpire.value()*1000);
    }
} setexCmd;

class PSetExCommand: public SetexGeneralCommand {
 public:
    PSetExCommand()
        :SetexGeneralCommand("psetex") {
    }

    Expected<std::string> run(Session *sess) final {
        const std::string& key = sess->getArgs()[1];
        const std::string& val = sess->getArgs()[3];
        Expected<uint64_t> eexpire = ::tendisplus::stoul(sess->getArgs()[2]);
        if (!eexpire.ok()) {
            return eexpire.status();
        }
        return runGeneral(sess, key, val,
                          nsSinceEpoch()/1000000 + eexpire.value());
    }
} psetexCmd;

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

class StrlenCommand: public Command {
 public:
    StrlenCommand()
        :Command("strlen") {
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
            return Command::fmtZero();
        } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtZero();
        } else if (!rv.status().ok()) {
            return rv.status();
        } else {
            return Command::fmtLongLong(rv.value().getValue().size());
        }
    }
} strlenCmd;

class BitCountCommand: public Command {
 public:
    BitCountCommand()
        :Command("bitcount") {
    }

    ssize_t arity() const {
        return -2;
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
            return Command::fmtZero();
        } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtZero();
        } else if (!rv.status().ok()) {
            return rv.status();
        }
        int64_t start = 0;
        const std::string& target = rv.value().getValue();
        int64_t end = target.size() - 1;
        if (sess->getArgs().size() == 4) {
            Expected<int64_t> estart = ::tendisplus::stoll(sess->getArgs()[2]);
            Expected<int64_t> eend = ::tendisplus::stoll(sess->getArgs()[3]);
            if (!estart.ok() || !eend.ok()) {
                return estart.ok() ? eend.status() : estart.status();
            }
            start = estart.value();
            end = eend.value();
            ssize_t len = rv.value().getValue().size();
            if (start < 0) {
                start = len + start;
            }
            if (end < 0) {
                end = len + end;
            }
            if (start < 0) {
                start = 0;
            }
            if (end < 0) {
                end = 0;
            }
            if (end >= len) {
                end = len - 1;
            }
        } else if (sess->getArgs().size() == 2) {
            // nothing
        } else {
            return {ErrorCodes::ERR_PARSEOPT, "syntax error"};
        }
        if (start > end) {
            return Command::fmtZero();
        }
        return Command::fmtLongLong(
            redis_port::popCount(target.c_str() + start, end - start + 1));
    }
} bitcntCmd;


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

class GetSetGeneral: public Command {
 public:
    explicit GetSetGeneral(const std::string& name)
        :Command(name) {
    }

    virtual bool replyNewValue() const {
        return true;
    }

    virtual Expected<RecordValue> newValueFromOld(Session* sess,
                            const Expected<RecordValue>& oldValue) const = 0;

    Expected<RecordValue> runGeneral(Session *sess) {
            const std::string& key = sess->getArgs()[firstkey()];
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
            const Expected<RecordValue>& newValue =
                newValueFromOld(sess, eValue);
            if (!newValue.ok()) {
                return newValue.status();
            }
            auto result = setGeneric(kvstore,
                                     txn.get(),
                                     REDIS_SET_NO_FLAGS,
                                     rk, newValue.value(), "", "");
            if (result.ok()) {
                if (replyNewValue()) {
                    return std::move(newValue.value());
                } else {
                    return eValue.ok() ? std::move(eValue.value()) : RecordValue("");
                }
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

class AppendCommand: public GetSetGeneral {
 public:
    AppendCommand()
        :GetSetGeneral("append") {
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

    Expected<RecordValue> newValueFromOld(Session* sess,
                          const Expected<RecordValue>& oldValue) const {
        const std::string& val = sess->getArgs()[2];
        std::string cat;
        if (!oldValue.ok()) {
            cat = val;
        } else {
            cat = oldValue.value().getValue();
            cat.insert(cat.end(), val.begin(), val.end());
        }

        uint64_t ttl = 0;
        if (oldValue.ok()) {
            ttl = oldValue.value().getTtl();
        }
        return std::move(RecordValue(std::move(cat), ttl));
    }

    Expected<std::string> run(Session *sess) final {
        const Expected<RecordValue>& rv = runGeneral(sess);
        if (!rv.ok()) {
            return rv.status();
        }
        return Command::fmtLongLong(rv.value().getValue().size());
    }
} appendCmd;

// TODO(unittest)
class SetRangeCommand: public GetSetGeneral {
 public:
    SetRangeCommand()
        :GetSetGeneral("setrange") {
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

    Expected<RecordValue> newValueFromOld(Session* sess,
                          const Expected<RecordValue>& oldValue) const {
        const std::string& val = sess->getArgs()[3];
        Expected<int64_t> eoffset = ::tendisplus::stoll(sess->getArgs()[2]);
        if (!eoffset.ok()) {
            return eoffset.status();
        }
        if (eoffset.value() < 0) {
            return {ErrorCodes::ERR_PARSEOPT, "offset is out of range"};
        }
        uint32_t offset = eoffset.value();
        if (offset + val.size() > 512*1024*1024) {
            return {ErrorCodes::ERR_PARSEOPT,
                    "string exceeds maximum allowed size (512MB)"};
        }
        std::string cat;
        if (oldValue.ok()) {
            cat = oldValue.value().getValue();
        }
        if (offset + val.size() > cat.size()) {
            cat.resize(offset + val.size(), 0);
        }
        for (size_t i = offset; i < offset + val.size(); ++i) {
            cat[i] = val[i-offset];
        }
        uint64_t ttl = 0;
        if (oldValue.ok()) {
            ttl = oldValue.value().getTtl();
        }
        return RecordValue(std::move(cat), ttl);
    }

    Expected<std::string> run(Session *sess) final {
        const Expected<RecordValue>& rv = runGeneral(sess);
        if (!rv.ok()) {
            return rv.status();
        }
        return Command::fmtLongLong(rv.value().getValue().size());
    }
} setrangeCmd;

class SetBitCommand: public GetSetGeneral {
 public:
    SetBitCommand()
        :GetSetGeneral("setbit") {
    }

    bool replyNewValue() const final {
        return false;
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

    Expected<RecordValue> newValueFromOld(Session* sess,
                          const Expected<RecordValue>& oldValue) const {
        Expected<uint64_t> epos = ::tendisplus::stoul(sess->getArgs()[2]);
        if (!epos.ok()) {
            return epos.status();
        }
        uint64_t pos = epos.value();
        int on = 0;
        std::string tomodify;
        if (oldValue.ok()) {
            tomodify = oldValue.value().getValue();
        }
        if ((pos >> 3) >= (512*1024*1024)) {
            return {ErrorCodes::ERR_PARSEOPT,
                    "bit offset is not an integer or out of range"};
        }
        if ((pos >> 3) > 4*1024*1024) {
            LOG(WARNING) << "meet large bitpos:" << pos;
        }
        if (sess->getArgs()[3] == "1") {
            on = 1;
        } else if (sess->getArgs()[3] == "0") {
            on = 0;
        } else {
            return {ErrorCodes::ERR_PARSEOPT,
                    "bit is not an integer or out of range"};
        }

        uint64_t byte = (pos>>3);
        if (tomodify.size() < byte+1) {
            tomodify.resize(byte+1, 0);
        }
        uint8_t byteval = static_cast<uint8_t>(tomodify[byte]);
        uint8_t bit = 7 - (pos & 0x7);
        byteval &= ~(1 << bit);
        byteval |= ((on & 0x1) << bit);
        tomodify[byte] = byteval;

        // incrby wont clear ttl
        uint64_t ttl = 0;
        if (oldValue.ok()) {
            ttl = oldValue.value().getTtl();
        }
        return RecordValue(std::move(tomodify), ttl);
    }

    Expected<std::string> run(Session *sess) final {
        const Expected<RecordValue>& rv = runGeneral(sess);
        if (!rv.ok()) {
            return rv.status();
        }

        Expected<uint64_t> epos = ::tendisplus::stoul(sess->getArgs()[2]);
        if (!epos.ok()) {
            return epos.status();
        }
        uint64_t pos = epos.value();
        std::string toreturn = rv.value().getValue();

        uint64_t byte = (pos>>3);
        if (toreturn.size() < byte+1) {
            toreturn.resize(byte+1, 0);
        }
        uint8_t byteval = static_cast<uint8_t>(toreturn[byte]);
        uint8_t bit = 7 - (pos & 0x7);
        uint8_t bitval = byteval & (1 << bit);
        return bitval ? Command::fmtOne() : Command::fmtZero();
    }
} setbitCmd;

class GetSetCommand: public GetSetGeneral {
 public:
    GetSetCommand()
        :GetSetGeneral("getset") {
    }

    bool replyNewValue() const final {
        return false;
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

    Expected<RecordValue> newValueFromOld(Session* sess,
                          const Expected<RecordValue>& oldValue) const {
        (void)oldValue;
        // getset overwrites ttl
        return RecordValue(sess->getArgs()[2], 0);
    }

    Expected<std::string> run(Session *sess) final {
        const Expected<RecordValue>& rv = runGeneral(sess);
        if (!rv.ok()) {
            return rv.status();
        }
        const std::string& v = rv.value().getValue();
        if (v.size()) {
            return Command::fmtBulk(v);
        }
        return Command::fmtNull();
    }
} getsetCmd;

class IncrDecrGeneral: public GetSetGeneral {
 public:
    explicit IncrDecrGeneral(const std::string& name)
        :GetSetGeneral(name) {
    }

    Expected<int64_t> sumIncr(const Expected<RecordValue>& esum,
                              int64_t incr) const {
        int64_t sum = 0;
        if (esum.ok()) {
            Expected<int64_t> val =
                ::tendisplus::stoll(esum.value().getValue());
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
        return sum;
    }

    virtual Expected<std::string> run(Session *sess) {
        const Expected<RecordValue>& rv = runGeneral(sess);
        if (!rv.ok()) {
            return rv.status();
        }
        Expected<int64_t> val = ::tendisplus::stoll(rv.value().getValue());
        if (!val.ok()) {
            return val.status();
        }
        return Command::fmtLongLong(val.value());
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

    Expected<RecordValue> newValueFromOld(
            Session* sess, const Expected<RecordValue>& oldValue) const {
        const std::string& val = sess->getArgs()[2];
        Expected<int64_t> eInc = ::tendisplus::stoll(val);
        if (!eInc.ok()) {
            return eInc.status();
        }
        Expected<int64_t> newSum = sumIncr(oldValue, eInc.value());
        if (!newSum.ok()) {
            return newSum.status();
        }

        // incrby wont clear ttl
        uint64_t ttl = 0;
        if (oldValue.ok()) {
            ttl = oldValue.value().getTtl();
        }
        return RecordValue(std::to_string(newSum.value()), ttl);
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

    Expected<RecordValue> newValueFromOld(Session* sess,
                          const Expected<RecordValue>& oldValue) const {
        Expected<int64_t> newSum = sumIncr(oldValue, 1);
        if (!newSum.ok()) {
            return newSum.status();
        }

        // incrby wont clear ttl
        uint64_t ttl = 0;
        if (oldValue.ok()) {
            ttl = oldValue.value().getTtl();
        }
        return RecordValue(std::to_string(newSum.value()), ttl);
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

    Expected<RecordValue> newValueFromOld(Session* sess,
                          const Expected<RecordValue>& oldValue) const {
        const std::string& val = sess->getArgs()[2];
        Expected<int64_t> eInc = ::tendisplus::stoll(val);
        if (!eInc.ok()) {
            return eInc.status();
        }
        Expected<int64_t> newSum = sumIncr(oldValue, -eInc.value());
        if (!newSum.ok()) {
            return newSum.status();
        }

        // incrby wont clear ttl
        uint64_t ttl = 0;
        if (oldValue.ok()) {
            ttl = oldValue.value().getTtl();
        }
        return RecordValue(std::to_string(newSum.value()), ttl);
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

    Expected<RecordValue> newValueFromOld(Session* sess,
                          const Expected<RecordValue>& oldValue) const {
        Expected<int64_t> newSum = sumIncr(oldValue, -1);
        if (!newSum.ok()) {
            return newSum.status();
        }

        // incrby wont clear ttl
        uint64_t ttl = 0;
        if (oldValue.ok()) {
            ttl = oldValue.value().getTtl();
        }
        return RecordValue(std::to_string(newSum.value()), ttl);
    }
} decrCmd;

}  // namespace tendisplus
