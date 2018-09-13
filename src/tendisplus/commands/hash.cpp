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
#include "tendisplus/storage/expirable.h"

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

        Status s = Command::expireKeyIfNeeded(sess, storeId, metaRk);
        if (!s.ok()) {
            return s;
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
        Expected<RecordValue> eMetaVal = kvstore->getKV(metaRk, txn.get());
        if (!eMetaVal.ok() &&
                eMetaVal.status().code() != ErrorCodes::ERR_NOTFOUND) {
            return eMetaVal.status();
        }

        if (!eMetaVal.ok() &&
                eMetaVal.status().code() != ErrorCodes::ERR_NOTFOUND) {
            return eMetaVal.status();
        }
        if (eMetaVal.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return fmtZero();
        }
        Expected<HashMetaValue> exptHashMeta =
            HashMetaValue::decode(eMetaVal.value().getValue());
        if (!exptHashMeta.ok()) {
            return exptHashMeta.status();
        }
        return fmtLongLong(exptHashMeta.value().getCount());
    }
} hlenCommand;

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

        Status s = Command::expireKeyIfNeeded(sess, storeId, metaRk);
        if (!s.ok()) {
            return s;
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
        Expected<RecordValue> eMetaVal = kvstore->getKV(metaRk, txn.get());
        if (!eMetaVal.ok() &&
                eMetaVal.status().code() != ErrorCodes::ERR_NOTFOUND) {
            return eMetaVal.status();
        }
        if (eMetaVal.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return fmtNull();
        }
        Expected<RecordValue> eVal = kvstore->getKV(subRk, txn.get());
        if (eVal.ok()) {
            return eVal.value().getValue();
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

        Status s = Command::expireKeyIfNeeded(sess, storeId, metaKey);
        if (!s.ok()) {
            return s;
        }
        // now, we have no need to deal with expire, though it may still
        // be expired in a very rere situation since expireHash is in
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
}  // namespace tendisplus
