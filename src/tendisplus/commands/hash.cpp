#include <string>
#include <utility>
#include <memory>
#include <algorithm>
#include <cctype>
#include <clocale>
#include <vector>
#include "glog/logging.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

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

    RecordValue metaValue(hashMeta.encode(), ttl);
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

    Expected<std::string> run(NetSession *sess) final {
        const std::vector<std::string>& args = sess->getArgs();
        const std::string& key = args[1];

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);

        RecordKey metaRk(pCtx->getDbId(), RecordType::RT_HASH_META, key, "");
        std::string metaKeyEnc = metaRk.encode();
        uint32_t storeId = Command::getStoreId(sess, key);

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, storeId, metaRk);
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

    Expected<std::string> run(NetSession *sess) final {
        const std::vector<std::string>& args = sess->getArgs();
        const std::string& key = args[1];
        const std::string& subkey = args[2];

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);

        RecordKey metaRk(pCtx->getDbId(), RecordType::RT_HASH_META, key, "");
        std::string metaKeyEnc = metaRk.encode();
        RecordKey subRk(pCtx->getDbId(), RecordType::RT_HASH_ELE, key, subkey);
        uint32_t storeId = Command::getStoreId(sess, key);

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, storeId, metaRk);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
            return Command::fmtNull();
        } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtNull();
        } else if (!rv.status().ok()) {
            return rv.status();
        }

        auto storeLock = Command::lockDBByKey(sess,
                                              key,
                                              mgl::LockMode::LOCK_IS);
        if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
            return {ErrorCodes::ERR_BUSY, "key locked"};
        }

        PStore kvstore = Command::getStoreById(sess, storeId);
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

class HGetAllCommand: public Command {
 public:
    HGetAllCommand()
        :Command("hgetall") {
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

    Expected<std::string> run(NetSession *sess) final {
        const std::vector<std::string>& args = sess->getArgs();
        const std::string& key = args[1];

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);

        RecordKey metaRk(pCtx->getDbId(), RecordType::RT_HASH_META, key, "");
        std::string metaKeyEnc = metaRk.encode();
        uint32_t storeId = Command::getStoreId(sess, key);

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, storeId, metaRk);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
            return Command::fmtZeroBulkLen();
        } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtZeroBulkLen();
        } else if (!rv.status().ok()) {
            return rv.status();
        }

        auto storeLock = Command::lockDBByKey(sess,
                                              key,
                                              mgl::LockMode::LOCK_IS);
        if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
            return {ErrorCodes::ERR_BUSY, "key locked"};
        }

        PStore kvstore = Command::getStoreById(sess, storeId);
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        RecordKey fakeEle(metaRk.getDbId(),
                          RecordType::RT_HASH_ELE,
                          metaRk.getPrimaryKey(),
                          "");
        std::string prefix = fakeEle.prefixPk();
        auto cursor = txn->createCursor();
        cursor->seek(prefix);

        std::list<std::string> result;
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
                INVARIANT(rcdKey.getPrimaryKey() != metaRk.getPrimaryKey());
                break;
            }
            result.push_back(rcd.getRecordValue().getValue());
        }
        std::stringstream ss;
        Command::fmtMultiBulkLen(ss, result.size());
        for (const auto& v : result) {
            Command::fmtBulk(ss, v);
        }
        return ss.str();
    }
} hgetAllCmd;

class HGetCommand: public Command {
 public:
    HGetCommand()
        :Command("hget") {
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

    Expected<std::string> run(NetSession *sess) final {
        const std::vector<std::string>& args = sess->getArgs();
        const std::string& key = args[1];
        const std::string& subkey = args[2];

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);

        RecordKey metaRk(pCtx->getDbId(), RecordType::RT_HASH_META, key, "");
        std::string metaKeyEnc = metaRk.encode();
        RecordKey subRk(pCtx->getDbId(), RecordType::RT_HASH_ELE, key, subkey);
        uint32_t storeId = Command::getStoreId(sess, key);

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, storeId, metaRk);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
            return Command::fmtNull();
        } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtNull();
        } else if (!rv.status().ok()) {
            return rv.status();
        }

        auto storeLock = Command::lockDBByKey(sess,
                                              key,
                                              mgl::LockMode::LOCK_IS);
        if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
            return {ErrorCodes::ERR_BUSY, "key locked"};
        }

        PStore kvstore = Command::getStoreById(sess, storeId);
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        Expected<RecordValue> eVal = kvstore->getKV(subRk, txn.get());
        if (eVal.ok()) {
            return fmtBulk(eVal.value().getValue());
        } else if (eVal.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return fmtNull();
        } else {
            return eVal.status();
        }
    }
} hgetCommand;

class HSetCommand: public Command {
 public:
    HSetCommand()
        :Command("hset") {
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

    Expected<std::string> run(NetSession *sess) final {
        const std::vector<std::string>& args = sess->getArgs();
        const std::string& key = args[1];
        const std::string& subkey = args[2];
        const std::string& val = args[3];

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);
        RecordKey metaKey(pCtx->getDbId(), RecordType::RT_HASH_META, key, "");
        std::string metaKeyEnc = metaKey.encode();
        RecordKey subKey(pCtx->getDbId(), RecordType::RT_HASH_ELE, key, subkey);
        RecordValue subRv(val);
        uint32_t storeId = Command::getStoreId(sess, key);

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, storeId, metaKey);
        if (rv.status().code() != ErrorCodes::ERR_OK &&
                rv.status().code() != ErrorCodes::ERR_EXPIRED &&
                rv.status().code() != ErrorCodes::ERR_NOTFOUND) {
            return rv.status();
        }

        // now, we have no need to deal with expire, though it may still
        // be expired in a very rare situation since expireHash is in
        // a seperate txn (from code below)
        auto storeLock = Command::lockDBByKey(sess,
                                              key,
                                              mgl::LockMode::LOCK_IX);
        if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
            return {ErrorCodes::ERR_BUSY, "key locked"};
        }
        PStore kvstore = Command::getStoreById(sess, storeId);

        // here maybe one more time io than the original tendis
        for (int32_t i = 0; i < RETRY_CNT - 1; ++i) {
            auto result = hsetGeneric(metaKey, subKey, subRv, kvstore);
            if (result.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
                return result;
            }
        }
        return hsetGeneric(metaKey, subKey, subRv, kvstore);
    }
} hsetCommand;

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
            RecordKey subRk(dbId,
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
            RecordValue metaValue(hashMeta.encode(), ttl);
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

    Expected<std::string> run(NetSession* sess) final {
        const std::vector<std::string>& args = sess->getArgs();
        const std::string& key = args[1];
        if (args.size() >= 30000) {
            return {ErrorCodes::ERR_PARSEOPT, "exceed hdel batch lim"};
        }

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);
        uint32_t storeId = Command::getStoreId(sess, key);
        RecordKey metaKey(pCtx->getDbId(), RecordType::RT_HASH_META, key, "");
        std::string metaKeyEnc = metaKey.encode();

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, storeId, metaKey);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
            return fmtZero();
        } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return fmtZero();
        } else if (!rv.status().ok()) {
            return rv.status();
        }
        auto storeLock = Command::lockDBByKey(sess,
                                              key,
                                              mgl::LockMode::LOCK_IX);
        if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
            return {ErrorCodes::ERR_BUSY, "key locked"};
        }

        std::vector<RecordKey> rcds;
        for (size_t i = 2; i < args.size(); ++i) {
            rcds.emplace_back(
                RecordKey(pCtx->getDbId(),
                          RecordType::RT_HASH_ELE,
                          key,
                          args[i]));
        }

        PStore kvstore = Command::getStoreById(sess, storeId);
        for (uint32_t i = 0; i < RETRY_CNT; ++i) {
            auto ptxn = kvstore->createTransaction();
            if (!ptxn.ok()) {
                return ptxn.status();
            }
            std::unique_ptr<Transaction> txn = std::move(ptxn.value());
            Expected<uint32_t> delCount =
                    delKeys(kvstore, metaKey, args, txn.get());
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
