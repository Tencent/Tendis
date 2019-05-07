#include <string>
#include <utility>
#include <memory>
#include <algorithm>
#include <cctype>
#include <clocale>
#include <vector>
#include <list>
#include "glog/logging.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

Expected<std::string> hincrfloatGeneric(const RecordKey& metaRk,
                   const RecordKey& subRk,
                   long double inc,
                   PStore kvstore) {
    auto ptxn = kvstore->createTransaction();
    if (!ptxn.ok()) {
        return ptxn.status();
    }
    std::unique_ptr<Transaction> txn = std::move(ptxn.value());
    Expected<RecordValue> eValue = kvstore->getKV(metaRk, txn.get());
    if (!eValue.ok() && eValue.status().code() != ErrorCodes::ERR_NOTFOUND) {
        return eValue.status();
    }

    HashMetaValue hashMeta;
    uint64_t ttl = 0;
    if (eValue.ok()) {
        ttl = eValue.value().getTtl();
        Expected<HashMetaValue> exptHashMeta =
            HashMetaValue::decode(eValue.value().getValue());
        if (!exptHashMeta.ok()) {
            return exptHashMeta.status();
        }
        hashMeta = std::move(exptHashMeta.value());
    }  // no else, else not found , so subkeyCount = 0, ttl = 0

    auto getSubkeyExpt = kvstore->getKV(subRk, txn.get());
    long double nowVal = 0;
    if (getSubkeyExpt.ok()) {
        Expected<long double> val =
            ::tendisplus::stold(getSubkeyExpt.value().getValue());
        if (!val.ok()) {
            return {ErrorCodes::ERR_DECODE, "hash value is not a valid float"};
        }
        nowVal = val.value();
    } else if (getSubkeyExpt.status().code() == ErrorCodes::ERR_NOTFOUND) {
        nowVal = 0;
        hashMeta.setCount(hashMeta.getCount()+1);
    } else {
        return getSubkeyExpt.status();
    }

    nowVal += inc;
    RecordValue newVal(::tendisplus::ldtos(nowVal, true),
                        RecordType::RT_HASH_ELE);
    RecordValue metaValue(hashMeta.encode(), RecordType::RT_HASH_META, ttl, eValue);
    Status setStatus = kvstore->setKV(metaRk, metaValue, txn.get());
    if (!setStatus.ok()) {
        return setStatus;
    }
    setStatus = kvstore->setKV(subRk, newVal, txn.get());
    if (!setStatus.ok()) {
        return setStatus;
    }
    Expected<uint64_t> exptCommit = txn->commit();
    if (!exptCommit.ok()) {
        return exptCommit.status();
    } else {
        return Command::fmtBulk(tendisplus::ldtos(nowVal, true));
    }
}

Expected<std::string> hincrGeneric(const RecordKey& metaRk,
                   const RecordKey& subRk,
                   int64_t inc,
                   PStore kvstore) {
    auto ptxn = kvstore->createTransaction();
    if (!ptxn.ok()) {
        return ptxn.status();
    }
    std::unique_ptr<Transaction> txn = std::move(ptxn.value());
    Expected<RecordValue> eValue = kvstore->getKV(metaRk, txn.get());
    if (!eValue.ok() && eValue.status().code() != ErrorCodes::ERR_NOTFOUND) {
        return eValue.status();
    }

    HashMetaValue hashMeta;
    uint64_t ttl = 0;
    if (eValue.ok()) {
        ttl = eValue.value().getTtl();
        Expected<HashMetaValue> exptHashMeta =
            HashMetaValue::decode(eValue.value().getValue());
        if (!exptHashMeta.ok()) {
            return exptHashMeta.status();
        }
        hashMeta = std::move(exptHashMeta.value());
    }  // no else, else not found , so subkeyCount = 0, ttl = 0

    auto getSubkeyExpt = kvstore->getKV(subRk, txn.get());
    int64_t nowVal = 0;
    if (getSubkeyExpt.ok()) {
        Expected<int64_t> val =
            ::tendisplus::stoll(getSubkeyExpt.value().getValue());
        if (!val.ok()) {
            return {ErrorCodes::ERR_DECODE, "hash value is not an integer "};
        }
        nowVal = val.value();
    } else if (getSubkeyExpt.status().code() == ErrorCodes::ERR_NOTFOUND) {
        nowVal = 0;
        hashMeta.setCount(hashMeta.getCount()+1);
    } else {
        return getSubkeyExpt.status();
    }

    if ((inc < 0 && nowVal < 0 && inc < (LLONG_MIN - nowVal)) ||
            (inc > 0 && nowVal > 0 && inc > (LLONG_MAX - nowVal))) {
        return {ErrorCodes::ERR_OVERFLOW,
                    "increment or decrement would overflow"};
    }
    nowVal += inc;
    RecordValue newVal(std::to_string(nowVal), RecordType::RT_HASH_ELE);
    RecordValue metaValue(hashMeta.encode(), RecordType::RT_HASH_META, ttl, eValue);
    Status setStatus = kvstore->setKV(metaRk, metaValue, txn.get());
    if (!setStatus.ok()) {
        return setStatus;
    }
    setStatus = kvstore->setKV(subRk, newVal, txn.get());
    if (!setStatus.ok()) {
        return setStatus;
    }
    Expected<uint64_t> exptCommit = txn->commit();
    if (!exptCommit.ok()) {
        return exptCommit.status();
    } else {
        return Command::fmtLongLong(nowVal);
    }
}

class HLenCommand: public Command {
 public:
    HLenCommand()
        :Command("hlen") {
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
        const std::string& key = args[1];

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_HASH_META);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
            return fmtZero();
        } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return fmtZero();
        } else if (!rv.status().ok()) {
            return rv.status();
        }
        Expected<HashMetaValue> exptHashMeta =
            HashMetaValue::decode(rv.value().getValue());
        if (!exptHashMeta.ok()) {
            return exptHashMeta.status();
        }
        return fmtLongLong(exptHashMeta.value().getCount());
    }
} hlenCommand;

class HExistsCommand: public Command {
 public:
    HExistsCommand()
        :Command("hexists") {
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
        const std::vector<std::string>& args = sess->getArgs();
        const std::string& key = args[1];
        const std::string& subkey = args[2];

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_HASH_META);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
            return Command::fmtNull();
        } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtNull();
        } else if (!rv.status().ok()) {
            return rv.status();
        }

        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key,
                        mgl::LockMode::LOCK_S);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(),
                    RecordType::RT_HASH_META, key, "");
        // uint32_t storeId = expdb.value().dbId;
        std::string metaKeyEnc = metaRk.encode();
        RecordKey subRk(expdb.value().chunkId, pCtx->getDbId(),
                    RecordType::RT_HASH_ELE, key, subkey);
        PStore kvstore = expdb.value().store;

        // if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
        //     return {ErrorCodes::ERR_BUSY, "key locked"};
        // }

        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        Expected<RecordValue> eVal = kvstore->getKV(subRk, txn.get());
        if (eVal.ok()) {
            return Command::fmtOne();
        } else if (eVal.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtZero();
        } else {
            return eVal.status();
        }
    }
} hexistsCmd;

class HAllCommand: public Command {
 public:
    explicit HAllCommand(const std::string& name)
        :Command(name) {
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

    Expected<std::list<Record>> getRecords(Session *sess) {
        const std::vector<std::string>& args = sess->getArgs();
        const std::string& key = args[1];

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_HASH_META);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
            return std::list<Record>();
        } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return std::list<Record>();
        } else if (!rv.status().ok()) {
            return rv.status();
        }

        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key,
                        mgl::LockMode::LOCK_S);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(),
                    RecordType::RT_HASH_META, key, "");
        // uint32_t storeId = expdb.value().dbId;
        std::string metaKeyEnc = metaRk.encode();
        PStore kvstore = expdb.value().store;

        // if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
        //     return {ErrorCodes::ERR_BUSY, "key locked"};
        // }

        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        RecordKey fakeEle(expdb.value().chunkId,
                          metaRk.getDbId(),
                          RecordType::RT_HASH_ELE,
                          metaRk.getPrimaryKey(),
                          "");
        std::string prefix = fakeEle.prefixPk();
        auto cursor = txn->createCursor();
        cursor->seek(prefix);

        std::list<Record> result;
        while (true) {
            Expected<Record> exptRcd = cursor->next();
            if (exptRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
                break;
            }
            if (!exptRcd.ok()) {
                return exptRcd.status();
            }
            Record& rcd = exptRcd.value();
            const RecordKey& rcdKey = rcd.getRecordKey();
            if (rcdKey.prefixPk() != prefix) {
                break;
            }
            result.emplace_back(std::move(rcd));
        }
        return std::move(result);
    }
};

class HGetAllCommand: public HAllCommand {
 public:
    HGetAllCommand()
        :HAllCommand("hgetall") {
    }

    Expected<std::string> run(Session *sess) final {
        Expected<std::list<Record>> rcds = getRecords(sess);
        if (!rcds.ok()) {
            return rcds.status();
        }
        std::stringstream ss;
        Command::fmtMultiBulkLen(ss, rcds.value().size()*2);
        for (const auto& v : rcds.value()) {
            Command::fmtBulk(ss, v.getRecordKey().getSecondaryKey());
            Command::fmtBulk(ss, v.getRecordValue().getValue());
        }
        return ss.str();
    }
} hgetAllCmd;

class HKeysCommand: public HAllCommand {
 public:
    HKeysCommand()
        :HAllCommand("hkeys") {
    }

    Expected<std::string> run(Session *sess) final {
        Expected<std::list<Record>> rcds = getRecords(sess);
        if (!rcds.ok()) {
            return rcds.status();
        }
        std::stringstream ss;
        Command::fmtMultiBulkLen(ss, rcds.value().size());
        for (const auto& v : rcds.value()) {
            Command::fmtBulk(ss, v.getRecordKey().getSecondaryKey());
        }
        return ss.str();
    }
} hkeysCmd;

class HValsCommand: public HAllCommand {
 public:
    HValsCommand()
        :HAllCommand("hvals") {
    }

    Expected<std::string> run(Session *sess) final {
        Expected<std::list<Record>> rcds = getRecords(sess);
        if (!rcds.ok()) {
            return rcds.status();
        }
        std::stringstream ss;
        Command::fmtMultiBulkLen(ss, rcds.value().size());
        for (const auto& v : rcds.value()) {
            Command::fmtBulk(ss, v.getRecordValue().getValue());
        }
        return ss.str();
    }
} hvalsCmd;

class HGetRecordCommand: public Command {
 public:
    explicit HGetRecordCommand(const std::string& name)
        :Command(name) {
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

    Expected<Record> getRecord(Session *sess) {
        const std::vector<std::string>& args = sess->getArgs();
        const std::string& key = args[1];
        const std::string& subkey = args[2];

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_HASH_META);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
            return rv.status();
        } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return rv.status();
        } else if (!rv.status().ok()) {
            return rv.status();
        }

        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key,
            mgl::LockMode::LOCK_S);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(),
                        RecordType::RT_HASH_META, key, "");
        // uint32_t storeId = expdb.value().dbId;
        std::string metaKeyEnc = metaRk.encode();
        RecordKey subRk(expdb.value().chunkId, pCtx->getDbId(),
                    RecordType::RT_HASH_ELE, key, subkey);
        PStore kvstore = expdb.value().store;

        // if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
        //     return {ErrorCodes::ERR_BUSY, "key locked"};
        // }

        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        Expected<RecordValue> eVal = kvstore->getKV(subRk, txn.get());
        if (eVal.ok()) {
            return std::move(Record(std::move(subRk), std::move(eVal.value())));
        } else {
            return eVal.status();
        }
    }
};

class HGetCommand: public HGetRecordCommand {
 public:
    HGetCommand()
        :HGetRecordCommand("hget") {
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
        Expected<Record> ercd = getRecord(sess);
        if (ercd.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtNull();
        } else if (ercd.status().code() == ErrorCodes::ERR_EXPIRED) {
            return Command::fmtNull();
        } else if (!ercd.ok()) {
            return ercd.status();
        }
        return Command::fmtBulk(ercd.value().getRecordValue().getValue());
    }
} hgetCommand;

class HStrlenCommand: public HGetRecordCommand {
 public:
    HStrlenCommand()
        :HGetRecordCommand("hstrlen") {
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
        Expected<Record> ercd = getRecord(sess);
        if (ercd.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtZero();
        } else if (ercd.status().code() == ErrorCodes::ERR_EXPIRED) {
            return Command::fmtZero();
        } else if (!ercd.ok()) {
            return ercd.status();
        }
        uint64_t size = ercd.value().getRecordValue().getValue().size();
        return Command::fmtLongLong(size);
    }
} hstrlenCommand;

class HIncrByFloatCommand: public Command {
 public:
    HIncrByFloatCommand()
        :Command("hincrbyfloat") {
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
        const std::vector<std::string>& args = sess->getArgs();
        const std::string& key = args[1];
        const std::string& subkey = args[2];
        const std::string& val = args[3];
        Expected<long double> inc = ::tendisplus::stold(val);
        if (!inc.ok()) {
            return inc.status();
        }

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_HASH_META);
        if (rv.status().code() != ErrorCodes::ERR_OK &&
                rv.status().code() != ErrorCodes::ERR_EXPIRED &&
                rv.status().code() != ErrorCodes::ERR_NOTFOUND) {
            return rv.status();
        }

        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key,
                        mgl::LockMode::LOCK_X);
        if (!expdb.ok()) {
            return expdb.status();
        }

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(),
                        RecordType::RT_HASH_META, key, "");
        // uint32_t storeId = expdb.value().dbId;
        std::string metaKeyEnc = metaRk.encode();
        RecordKey subRk(expdb.value().chunkId, pCtx->getDbId(),
                        RecordType::RT_HASH_ELE, key, subkey);
        PStore kvstore = expdb.value().store;

        // now, we have no need to deal with expire, though it may still
        // be expired in a very rare situation since expireHash is in
        // a seperate txn (from code below)
        // if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
        //     return {ErrorCodes::ERR_BUSY, "key locked"};
        // }

        // here maybe one more time io than the original tendis
        for (int32_t i = 0; i < RETRY_CNT - 1; ++i) {
            auto result =
                hincrfloatGeneric(metaRk, subRk, inc.value(), kvstore);
            if (result.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
                return result;
            }
        }
        return hincrfloatGeneric(metaRk, subRk, inc.value(), kvstore);
    }
} hincrbyfloatCmd;

class HIncrByCommand: public Command {
 public:
    HIncrByCommand()
        :Command("hincrby") {
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
        const std::vector<std::string>& args = sess->getArgs();
        const std::string& key = args[1];
        const std::string& subkey = args[2];
        const std::string& val = args[3];
        Expected<int64_t> inc = ::tendisplus::stoll(val);
        if (!inc.ok()) {
            return inc.status();
        }

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_HASH_META);
        if (rv.status().code() != ErrorCodes::ERR_OK &&
                rv.status().code() != ErrorCodes::ERR_EXPIRED &&
                rv.status().code() != ErrorCodes::ERR_NOTFOUND) {
            return rv.status();
        }

        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key,
                            mgl::LockMode::LOCK_X);
        if (!expdb.ok()) {
            return expdb.status();
        }

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(),
                RecordType::RT_HASH_META, key, "");
        // uint32_t storeId = expdb.value().dbId;
        std::string metaKeyEnc = metaRk.encode();
        RecordKey subRk(expdb.value().chunkId, pCtx->getDbId(),
                RecordType::RT_HASH_ELE, key, subkey);
        PStore kvstore = expdb.value().store;

        // now, we have no need to deal with expire, though it may still
        // be expired in a very rare situation since expireHash is in
        // a seperate txn (from code below)
        // if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
        //     return {ErrorCodes::ERR_BUSY, "key locked"};
        // }

        // here maybe one more time io than the original tendis
        for (int32_t i = 0; i < RETRY_CNT - 1; ++i) {
            auto result = hincrGeneric(metaRk, subRk, inc.value(), kvstore);
            if (result.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
                return result;
            }
        }
        return hincrGeneric(metaRk, subRk, inc.value(), kvstore);
    }
} hincrbyCommand;

class HMGetGeneric: public Command {
 public:
    HMGetGeneric(const std::string& name)
            :Command(name) {
        if (name == "hmget") {
            _returnVsn = false;
        } else {
            // hmgetvsn
            _returnVsn = true;
        }
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
        const std::vector<std::string>& args = sess->getArgs();
        const std::string& key = args[1];

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_HASH_META);
        if (rv.status().code() == ErrorCodes::ERR_NOTFOUND ||
            rv.status().code() == ErrorCodes::ERR_EXPIRED) {
            std::stringstream ss;
            if (_returnVsn) {
                Command::fmtMultiBulkLen(ss, args.size()-1);
                Command::fmtNull(ss);
            } else {
                Command::fmtMultiBulkLen(ss, args.size()-2);
            }

            for (size_t i = 2; i < args.size(); ++i) {
                Command::fmtNull(ss);
            }
            return ss.str();
        } else if (!rv.ok()) {
            return rv.status();
        }

        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key,
                        mgl::LockMode::LOCK_S);
        if (!expdb.ok()) {
            return expdb.status();
        }
        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(),
                        RecordType::RT_HASH_META, key, "");
        // uint32_t storeId = expdb.value().dbId;
        std::string metaKeyEnc = metaRk.encode();
        PStore kvstore = expdb.value().store;

        // if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
        //     return {ErrorCodes::ERR_BUSY, "key locked"};
        // }
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());

        std::stringstream ss;
        if (_returnVsn) {
            Command::fmtMultiBulkLen(ss, args.size()-1);
            Expected<RecordValue> eValue = kvstore->getKV(metaRk, txn.get());
            if (!eValue.ok() &&
                eValue.status().code() != ErrorCodes::ERR_NOTFOUND) {
                return eValue.status();
            }
            if (!eValue.ok()) {
                Command::fmtNull(ss);
            } else {
                Command::fmtBulk(ss, std::to_string(eValue.value().getCas()));
            }
        } else {
            Command::fmtMultiBulkLen(ss, args.size()-2);
        }

        for (size_t i = 2; i < args.size(); ++i) {
            RecordKey subKey(expdb.value().chunkId, pCtx->getDbId(),
                        RecordType::RT_HASH_ELE, key, args[i]);
            Expected<RecordValue> eValue = kvstore->getKV(subKey, txn.get());
            if (!eValue.ok()) {
                if (eValue.status().code() == ErrorCodes::ERR_NOTFOUND) {
                    Command::fmtNull(ss);
                } else {
                    return eValue.status();
                }
            } else {
                Command::fmtBulk(ss, eValue.value().getValue());
            }
        }
        return ss.str();
    }

 private:
    bool _returnVsn;
};

class HMGetCommand: public HMGetGeneric {
 public:
    HMGetCommand()
        :HMGetGeneric("hmget") {
    }
} hmgetCmd;

class HMGetVsnCommand: public HMGetGeneric {
 public:
    HMGetVsnCommand()
        :HMGetGeneric("hmgetvsn") {
    }
} hmgetvsnCmd;

Status hmcas(Session *sess, const std::string& key,
             const std::vector<std::string>& subargs,
             uint64_t cmp, int64_t vsn, const Expected<int64_t>& newvsn) {
    SessionCtx *pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);

    {
        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_HASH_META);
        if (rv.status().code() != ErrorCodes::ERR_OK &&
                rv.status().code() != ErrorCodes::ERR_EXPIRED &&
                rv.status().code() != ErrorCodes::ERR_NOTFOUND) {
            return rv.status();
        }
    }

    auto server = sess->getServerEntry();
    auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key,
                    mgl::LockMode::LOCK_X);
    if (!expdb.ok()) {
        return expdb.status();
    }
    RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(),
                    RecordType::RT_HASH_META, key, "");
    // uint32_t storeId = expdb.value().dbId;
    std::string metaKeyEnc = metaRk.encode();
    PStore kvstore = expdb.value().store;

    // if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
    //     return {ErrorCodes::ERR_BUSY, "key locked"};
    // }

    auto ptxn = kvstore->createTransaction();
    if (!ptxn.ok()) {
        return ptxn.status();
    }
    std::unique_ptr<Transaction> txn = std::move(ptxn.value());
    Expected<RecordValue> eValue = kvstore->getKV(metaRk, txn.get());
    if (!eValue.ok() && eValue.status().code() != ErrorCodes::ERR_NOTFOUND) {
        return eValue.status();
    }

    HashMetaValue hashMeta;
    uint64_t ttl = 0;
    int64_t cas = -1;
    if (eValue.ok()) {
        ttl = eValue.value().getTtl();
        Expected<HashMetaValue> exptHashMeta =
            HashMetaValue::decode(eValue.value().getValue());
        if (!exptHashMeta.ok()) {
            return exptHashMeta.status();
        }
        hashMeta = std::move(exptHashMeta.value());
        cas = eValue.value().getCas();
    }  // no else, else not found , so subkeyCount = 0, ttl = 0, cas = 0

    if (cmp) {
        // kv should exist for comparison
        if (eValue.ok() && (int64_t)vsn != cas && cas != -1) {
            return {ErrorCodes::ERR_CAS, ""};
        }
    }

    std::map<std::string, uint64_t> uniqkeys;
    std::map<std::string, std::string> existkvs;
    for (size_t i = 0; i < subargs.size(); i += 3) {
        std::string subk = subargs[i];
        uniqkeys[subk] = i;
    }

    constexpr int OPSET = 0;
    constexpr int OPADD = 1;
    for (const auto& keyPos : uniqkeys) {
        bool exists = true;
        RecordKey rk(expdb.value().chunkId, pCtx->getDbId(),
                    RecordType::RT_HASH_ELE, key, keyPos.first);
        Expected<RecordValue> rv = kvstore->getKV(rk, txn.get());
        if (rv.ok()) {
            existkvs[keyPos.first] = rv.value().getValue();
        } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            exists = false;
        } else {
            return rv.status();
        }

        const std::string& opstr = subargs[keyPos.second+1];
        Expected<uint64_t> eop = ::tendisplus::stoul(opstr);
        if (!eop.ok()) {
            return eop.status();
        }

        RecordKey subrk(expdb.value().chunkId, pCtx->getDbId(),
                        RecordType::RT_HASH_ELE, key, keyPos.first);
        if (eop.value() == OPSET || !exists) {
            RecordValue subrv(subargs[keyPos.second+2],
                                RecordType::RT_HASH_ELE);
            Status s = kvstore->setKV(subrk, subrv, txn.get());
            if (!s.ok()) {
                return s;
            }
        } else if (eop.value() == OPADD) {
            Expected<int64_t> ev = ::tendisplus::stoll(existkvs[keyPos.first]);
            if (!ev.ok()) {
                return ev.status();
            }
            Expected<int64_t> ev1 =
                        ::tendisplus::stoll(subargs[keyPos.second+2]);
            if (!ev1.ok()) {
                return ev1.status();
            }
            RecordValue subrv(std::to_string(ev1.value() + ev.value()),
                                RecordType::RT_HASH_ELE);
            Status s = kvstore->setKV(subrk, subrv, txn.get());
            if (!s.ok()) {
                return s;
            }
        }
    }

    if (newvsn.ok()) {
        cas = newvsn.value();
    } else {
        if (cmp) {
            // if the kv does not exist before, use user passed cas
            if (eValue.ok() && cas != -1) {
                cas += 1;
            } else {
                cas = vsn;
            }
        } else {
            cas = vsn;
        }
    }
    hashMeta.setCount(hashMeta.getCount() + uniqkeys.size() - existkvs.size());
    RecordValue metaValue(hashMeta.encode(), RecordType::RT_HASH_META,
                            ttl, eValue);
    metaValue.setCas(cas);
    Status s = kvstore->setKV(metaRk, metaValue, txn.get());
    if (!s.ok()) {
        return s;
    }
    auto commitStat = txn->commit();
    return commitStat.status();
}

class HMCasV2Command: public Command {
 public:
    HMCasV2Command()
        :Command("hmcasv2") {
    }

    ssize_t arity() const {
        return -8;
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
        if ((args.size() - 5)%3 != 0) {
            return {ErrorCodes::ERR_PARSEOPT,
                "wrong number of arguments for hmcas"};
        }
        const std::string& key = args[1];
        Expected<uint64_t> ecmp = ::tendisplus::stoull(args[2]);
        if (!ecmp.ok()) {
            return ecmp.status();
        }
        Expected<int64_t> evsn = ::tendisplus::stoll(args[3]);
        if (!evsn.ok()) {
            return evsn.status();
        }
        Expected<int64_t> enewvsn = ::tendisplus::stoll(args[4]);
        if (!enewvsn.ok()) {
            return enewvsn.status();
        }
        uint64_t cmp = ecmp.value();
        int64_t vsn = evsn.value();
        int64_t newvsn = enewvsn.value();
        if (cmp != 0 && cmp != 1) {
            return {ErrorCodes::ERR_PARSEOPT, "cmp should be 0 or 1"};
        }
        auto server = sess->getServerEntry();
        if (server->versionIncrease()) {
            if (newvsn <= vsn) {
                return {ErrorCodes::ERR_PARSEOPT,
                    "new version must be greater than old version"};
            }
        }

        std::vector<std::string> subargs;
        for (size_t i = 5; i < args.size(); ++i) {
            subargs.push_back(args[i]);
        }
        for (uint32_t i = 0; i < RETRY_CNT; ++i) {
            Status s = hmcas(sess, key, subargs, cmp, vsn, newvsn);
            if (!s.ok()) {
                if (s.code() == ErrorCodes::ERR_CAS) {
                    return Command::fmtZero();
                } else if (s.code() == ErrorCodes::ERR_COMMIT_RETRY) {
                    if (i == RETRY_CNT-1) {
                        return s;
                    } else {
                        continue;
                    }
                } else {
                    return s;
                }
            } else {
                return Command::fmtOne();
            }
        }
        INVARIANT(0);
        return {ErrorCodes::ERR_INTERNAL, "never reaches here"};
    }
} hmcasV2Cmd;

class HMCasCommand: public Command {
 public:
    HMCasCommand()
        :Command("hmcas") {
    }

    ssize_t arity() const {
        return -7;
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
        if ((args.size() - 4)%3 != 0) {
            return {ErrorCodes::ERR_PARSEOPT,
                "wrong number of arguments for hmcas"};
        }
        const std::string& key = args[1];
        Expected<uint64_t> ecmp = ::tendisplus::stoul(args[2]);
        if (!ecmp.ok()) {
            return ecmp.status();
        }
        Expected<int64_t> evsn = ::tendisplus::stoll(args[3]);
        if (!evsn.ok()) {
            return evsn.status();
        }
        uint64_t cmp = ecmp.value();
        int64_t vsn = evsn.value();
        if (cmp != 0 && cmp != 1) {
            return {ErrorCodes::ERR_PARSEOPT, "cmp should be 0 or 1"};
        }

        std::vector<std::string> subargs;
        for (size_t i = 4; i < args.size(); ++i) {
            subargs.push_back(args[i]);
        }
        for (uint32_t i = 0; i < RETRY_CNT; ++i) {
            Status s = hmcas(sess, key, subargs, cmp, vsn,
                        {ErrorCodes::ERR_NOTFOUND, ""});
            if (!s.ok()) {
                if (s.code() == ErrorCodes::ERR_CAS) {
                    return Command::fmtZero();
                } else if (s.code() == ErrorCodes::ERR_COMMIT_RETRY) {
                    if (i == RETRY_CNT-1) {
                        return s;
                    } else {
                        continue;
                    }
                } else {
                    return s;
                }
            } else {
                return Command::fmtOne();
            }
        }
        INVARIANT(0);
        return {ErrorCodes::ERR_INTERNAL, "never reaches here"};
    }
} hmcasCmd;

class HMSetCommand: public Command {
 public:
    HMSetCommand()
        :Command("hmset") {
    }

    ssize_t arity() const {
        return -4;
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

    Expected<std::string> hmsetGeneric(const RecordKey& metaRk,
                        const std::vector<Record>& rcds,
                        PStore kvstore) {
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        Expected<RecordValue> eValue = kvstore->getKV(metaRk, txn.get());
        if (!eValue.ok()
                && eValue.status().code() != ErrorCodes::ERR_NOTFOUND) {
            return eValue.status();
        }

        HashMetaValue hashMeta;
        uint64_t ttl = 0;
        uint32_t inserted = 0;
        if (eValue.ok()) {
            ttl = eValue.value().getTtl();
            Expected<HashMetaValue> exptHashMeta =
                HashMetaValue::decode(eValue.value().getValue());
            if (!exptHashMeta.ok()) {
                return exptHashMeta.status();
            }
            hashMeta = std::move(exptHashMeta.value());
        }  // no else, else not found , so subkeyCount = 0, ttl = 0

        for (const auto& v : rcds) {
            auto getSubkeyExpt = kvstore->getKV(v.getRecordKey(), txn.get());
            if (!getSubkeyExpt.ok()) {
                if (getSubkeyExpt.status().code() != ErrorCodes::ERR_NOTFOUND) {
                    return getSubkeyExpt.status();
                }
                inserted += 1;
            }
            Status setStatus = kvstore->setKV(v.getRecordKey(),
                            v.getRecordValue(), txn.get());
            if (!setStatus.ok()) {
                return setStatus;
            }
        }
        hashMeta.setCount(hashMeta.getCount() + inserted);
        RecordValue metaValue(hashMeta.encode(), RecordType::RT_HASH_META, ttl, eValue);
        Status setStatus = kvstore->setKV(metaRk, metaValue, txn.get());
        if (!setStatus.ok()) {
            return setStatus;
        }
        Expected<uint64_t> exptCommit = txn->commit();
        if (!exptCommit.ok()) {
            return exptCommit.status();
        } else {
            return Command::fmtOK();
        }
    }

    Expected<std::string> run(Session *sess) final {
        const std::vector<std::string>& args = sess->getArgs();
        const std::string& key = args[1];
        if (args.size()-2 > 2048) {
            std::stringstream ss;
            ss << "exceed batch lim:" << args.size();
            return {ErrorCodes::ERR_INTERNAL, ss.str()};
        }
        if (args.size() % 2 != 0) {
            return {ErrorCodes::ERR_WRONG_ARGS_SIZE, ""};
        }

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_HASH_META);
        if (rv.status().code() != ErrorCodes::ERR_OK &&
                rv.status().code() != ErrorCodes::ERR_EXPIRED &&
                rv.status().code() != ErrorCodes::ERR_NOTFOUND) {
            return rv.status();
        }

        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key,
                    mgl::LockMode::LOCK_X);
        if (!expdb.ok()) {
            return expdb.status();
        }
        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(),
                    RecordType::RT_HASH_META, key, "");
        // uint32_t storeId = expdb.value().dbId;
        std::string metaKeyEnc = metaRk.encode();
        PStore kvstore = expdb.value().store;

        // if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
        //     return {ErrorCodes::ERR_BUSY, "key locked"};
        // }
        std::vector<Record> rcds;
        for (size_t i = 2; i < args.size(); i+=2) {
            RecordKey subKey(expdb.value().chunkId, pCtx->getDbId(),
                        RecordType::RT_HASH_ELE, key, args[i]);
            RecordValue subRv(args[i+1], RecordType::RT_HASH_ELE);
            rcds.emplace_back(Record(std::move(subKey), std::move(subRv)));
        }
        for (int32_t i = 0; i < RETRY_CNT - 1; ++i) {
            auto result = hmsetGeneric(metaRk, rcds, kvstore);
            if (result.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
                return result;
            }
        }
        return hmsetGeneric(metaRk, rcds, kvstore);
    }
} hmsetCommand;

class HSetGeneric: public Command {
 public:
    HSetGeneric(const std::string& name, bool setNx)
            :Command(name) {
        _setNx = setNx;
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
        const std::vector<std::string>& args = sess->getArgs();
        const std::string& key = args[1];
        const std::string& subkey = args[2];
        const std::string& val = args[3];

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_HASH_META);
        if (rv.status().code() != ErrorCodes::ERR_OK &&
                rv.status().code() != ErrorCodes::ERR_EXPIRED &&
                rv.status().code() != ErrorCodes::ERR_NOTFOUND) {
            return rv.status();
        }

        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key,
                    mgl::LockMode::LOCK_X);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaKey(expdb.value().chunkId, pCtx->getDbId(),
                        RecordType::RT_HASH_META, key, "");
        // uint32_t storeId = expdb.value().dbId;
        std::string metaKeyEnc = metaKey.encode();
        PStore kvstore = expdb.value().store;
        RecordKey subKey(expdb.value().chunkId, pCtx->getDbId(),
                        RecordType::RT_HASH_ELE, key, subkey);
        RecordValue subRv(val, RecordType::RT_HASH_ELE);

        // now, we have no need to deal with expire, though it may still
        // be expired in a very rare situation since expireHash is in
        // a seperate txn (from code below)
        // if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
        //     return {ErrorCodes::ERR_BUSY, "key locked"};
        // }

        // here maybe one more time io than the original tendis
        for (int32_t i = 0; i < RETRY_CNT - 1; ++i) {
            auto result = hsetGeneric(metaKey, subKey, subRv, kvstore);
            if (result.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
                return result;
            }
        }
        return hsetGeneric(metaKey, subKey, subRv, kvstore);
    }

    Expected<std::string> hsetGeneric(const RecordKey& metaRk,
                       const RecordKey& subRk,
                       const RecordValue& subRv,
                       PStore kvstore) {
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        Expected<RecordValue> eValue = kvstore->getKV(metaRk, txn.get());
        if (!eValue.ok()
                && eValue.status().code() != ErrorCodes::ERR_NOTFOUND) {
            return eValue.status();
        }

        HashMetaValue hashMeta;
        uint64_t ttl = 0;
        if (eValue.ok()) {
            ttl = eValue.value().getTtl();
            Expected<HashMetaValue> exptHashMeta =
                HashMetaValue::decode(eValue.value().getValue());
            if (!exptHashMeta.ok()) {
                return exptHashMeta.status();
            }
            hashMeta = std::move(exptHashMeta.value());
        }  // no else, else not found , so subkeyCount = 0, ttl = 0

        bool updated = false;
        auto getSubkeyExpt = kvstore->getKV(subRk, txn.get());
        if (getSubkeyExpt.ok()) {
            updated = true;
        } else if (getSubkeyExpt.status().code() == ErrorCodes::ERR_NOTFOUND) {
            updated = false;
            hashMeta.setCount(hashMeta.getCount()+1);
        } else {
            return getSubkeyExpt.status();
        }

        if (_setNx && updated) {
            return Command::fmtZero();
        }

        RecordValue metaValue(hashMeta.encode(), RecordType::RT_HASH_META, ttl, eValue);
        Status setStatus = kvstore->setKV(metaRk, metaValue, txn.get());
        if (!setStatus.ok()) {
            return setStatus;
        }
        setStatus = kvstore->setKV(subRk, subRv, txn.get());
        if (!setStatus.ok()) {
            return setStatus;
        }
        Expected<uint64_t> exptCommit = txn->commit();
        if (!exptCommit.ok()) {
            return exptCommit.status();
        } else {
            return updated ? Command::fmtZero() : Command::fmtOne();
        }
    }

 private:
    bool _setNx;
};

class HSetCommand: public HSetGeneric {
 public:
    HSetCommand()
        :HSetGeneric("hset", false) {
    }
} hsetCommand;

class HSetNxCommand: public HSetGeneric {
 public:
    HSetNxCommand()
        :HSetGeneric("hsetnx", true) {
    }
} hsetNxCommand;

class HDelCommand: public Command {
 public:
    HDelCommand()
        :Command("hdel") {
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

    Expected<uint32_t> delKeys(PStore kvstore,
                               const RecordKey& metaKey,
                               const std::vector<std::string>& args,
                               Transaction* txn) {
        uint32_t dbId = metaKey.getDbId();
        uint32_t realDel = 0;

        Expected<RecordValue> eValue = kvstore->getKV(metaKey, txn);
        if (eValue.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return 0;
        }
        if (!eValue.ok()) {
            return eValue.status();
        }

        HashMetaValue hashMeta;
        uint64_t ttl = 0;
        if (eValue.ok()) {
            ttl = eValue.value().getTtl();
            Expected<HashMetaValue> exptHashMeta =
                HashMetaValue::decode(eValue.value().getValue());
            if (!exptHashMeta.ok()) {
                return exptHashMeta.status();
            }
            hashMeta = std::move(exptHashMeta.value());
        }  // no else, else not found , so subkeyCount = 0, ttl = 0

        for (size_t i = 2; i < args.size(); ++i) {
            RecordKey subRk(metaKey.getChunkId(),
                            dbId,
                            RecordType::RT_HASH_ELE,
                            metaKey.getPrimaryKey(),
                            args[i]);
            Expected<RecordValue> eVal = kvstore->getKV(subRk, txn);
            if (eVal.status().code() == ErrorCodes::ERR_NOTFOUND) {
                continue;
            }
            if (!eVal.ok()) {
                return eVal.status();
            }
            Status s = kvstore->delKV(subRk, txn);
            if (!s.ok()) {
                return s;
            }
            realDel++;
        }

        // modify meta data
        INVARIANT(realDel <= hashMeta.getCount());
        Status s;
        if (realDel == hashMeta.getCount()) {
            s = kvstore->delKV(metaKey, txn);
        } else {
            hashMeta.setCount(hashMeta.getCount() - realDel);
            RecordValue metaValue(hashMeta.encode(),
                                    RecordType::RT_HASH_META, ttl, eValue);
            s = kvstore->setKV(metaKey, metaValue, txn);
        }
        if (!s.ok()) {
            return s;
        }
        Expected<uint64_t> commitStatus = txn->commit();
        if (!commitStatus.ok()) {
            return commitStatus.status();
        }
        return realDel;
    }

    Expected<std::string> run(Session* sess) final {
        const std::vector<std::string>& args = sess->getArgs();
        const std::string& key = args[1];
        if (args.size() >= 30000) {
            return {ErrorCodes::ERR_PARSEOPT, "exceed hdel batch lim"};
        }

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_HASH_META);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
            return fmtZero();
        } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return fmtZero();
        } else if (!rv.status().ok()) {
            return rv.status();
        }

        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key,
                        mgl::LockMode::LOCK_X);
        if (!expdb.ok()) {
            return expdb.status();
        }
        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(),
                        RecordType::RT_HASH_META, key, "");
        // uint32_t storeId = expdb.value().dbId;
        std::string metaKeyEnc = metaRk.encode();
        PStore kvstore = expdb.value().store;

        // if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
        //     return {ErrorCodes::ERR_BUSY, "key locked"};
        // }

        std::vector<RecordKey> rcds;
        for (size_t i = 2; i < args.size(); ++i) {
            rcds.emplace_back(
                RecordKey(expdb.value().chunkId,
                          pCtx->getDbId(),
                          RecordType::RT_HASH_ELE,
                          key,
                          args[i]));
        }

        for (uint32_t i = 0; i < RETRY_CNT; ++i) {
            auto ptxn = kvstore->createTransaction();
            if (!ptxn.ok()) {
                return ptxn.status();
            }
            std::unique_ptr<Transaction> txn = std::move(ptxn.value());
            Expected<uint32_t> delCount =
                    delKeys(kvstore, metaRk, args, txn.get());
            if (delCount.status().code() == ErrorCodes::ERR_COMMIT_RETRY) {
                if (i == RETRY_CNT - 1) {
                    return delCount.status();
                } else {
                    continue;
                }
            }
            if (!delCount.ok()) {
                return delCount.status();
            }
            return Command::fmtLongLong(delCount.value());
        }
        // never reaches here
        INVARIANT(0);
        return {ErrorCodes::ERR_INTERNAL, "never reaches here"};
    }
} hdelCommand;

}  // namespace tendisplus
