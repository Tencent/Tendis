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
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

Expected<std::string> genericSRem(Session *sess,
                                  PStore kvstore,
                                  const RecordKey& metaRk,
                                  const std::vector<std::string>& args) {
    auto ptxn = kvstore->createTransaction();
    if (!ptxn.ok()) {
        return ptxn.status();
    }
    std::unique_ptr<Transaction> txn = std::move(ptxn.value());
    SetMetaValue sm;
    Expected<RecordValue> rv = kvstore->getKV(metaRk, txn.get());
    uint64_t ttl = 0;

    if (rv.ok()) {
        ttl = rv.value().getTtl();
        Expected<SetMetaValue> exptSm =
            SetMetaValue::decode(rv.value().getValue());
        INVARIANT(exptSm.ok());
        sm = std::move(exptSm.value());
    } else if (rv.status().code() != ErrorCodes::ERR_NOTFOUND) {
        return rv.status();
    }

    uint64_t cnt = 0;
    for (size_t i = 2; i < args.size(); ++i) {
        RecordKey subRk(metaRk.getDbId(),
                        RecordType::RT_SET_ELE,
                        metaRk.getPrimaryKey(),
                        args[i]);
        RecordValue subRv("");
        Expected<RecordValue> rv = kvstore->getKV(subRk, txn.get());
        if (rv.ok()) {
            cnt += 1;
        } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            continue;
        } else {
            return rv.status();
        }
        Status s = kvstore->delKV(subRk, txn.get());
        if (!s.ok()) {
            return s;
        }
    }
    INVARIANT(sm.getCount() >= cnt);
    Status s;
    if (sm.getCount() == cnt) {
        s = kvstore->delKV(metaRk, txn.get());
    } else {
        sm.setCount(sm.getCount()-cnt);
        s = kvstore->setKV(metaRk,
                           RecordValue(sm.encode(), ttl),
                           txn.get());
    }
    if (!s.ok()) {
        return s;
    }
    Expected<uint64_t> commitStatus = txn->commit();
    return Command::fmtLongLong(cnt);
}

Expected<std::string> genericSAdd(Session *sess,
                                  PStore kvstore,
                                  const RecordKey& metaRk,
                                  const std::vector<std::string>& args) {
    auto ptxn = kvstore->createTransaction();
    if (!ptxn.ok()) {
        return ptxn.status();
    }
    std::unique_ptr<Transaction> txn = std::move(ptxn.value());

    SetMetaValue sm;
    Expected<RecordValue> rv = kvstore->getKV(metaRk, txn.get());
    uint64_t ttl = 0;

    if (rv.ok()) {
        ttl = rv.value().getTtl();
        Expected<SetMetaValue> exptSm =
            SetMetaValue::decode(rv.value().getValue());
        INVARIANT(exptSm.ok());
        sm = std::move(exptSm.value());
    } else if (rv.status().code() != ErrorCodes::ERR_NOTFOUND) {
        return rv.status();
    }

    uint64_t cnt = 0;
    for (size_t i = 2; i < args.size(); ++i) {
        RecordKey subRk(metaRk.getDbId(),
                        RecordType::RT_SET_ELE,
                        metaRk.getPrimaryKey(),
                        args[i]);
        RecordValue subRv("");
        Expected<RecordValue> rv = kvstore->getKV(subRk, txn.get());
        if (rv.ok()) {
            continue;
        } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            cnt += 1;
        } else {
            return rv.status();
        }
        Status s = kvstore->setKV(subRk, subRv, txn.get());
        if (!s.ok()) {
            return s;
        }
    }
    sm.setCount(sm.getCount()+cnt);
    Status s = kvstore->setKV(metaRk,
                              RecordValue(sm.encode(), ttl),
                              txn.get());
    if (!s.ok()) {
        return s;
    }
    Expected<uint64_t> commitStatus = txn->commit();
    return Command::fmtLongLong(cnt);
}

class SIsMemberCommand: public Command {
 public:
    SIsMemberCommand()
        :Command("sismember") {
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
        int64_t idx = 0;
        try {
            idx = static_cast<int64_t>(std::stoll(args[2]));
        } catch (std::exception& ex) {
            return {ErrorCodes::ERR_PARSEOPT, ex.what()};
        }

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);

        RecordKey metaRk(pCtx->getDbId(), RecordType::RT_SET_META, key, "");
        std::string metaKeyEnc = metaRk.encode();
        uint32_t storeId = Command::getStoreId(sess, key);

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, storeId, metaRk);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
            return fmtZero();
        } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return fmtZero();
        } else if (!rv.ok()) {
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

        RecordKey subRk(pCtx->getDbId(),
                        RecordType::RT_SET_ELE,
                        key,
                        subkey);
        Expected<RecordValue> eSubVal = kvstore->getKV(subRk, txn.get());
        if (eSubVal.ok()) {
            return Command::fmtOne();
        } else if (eSubVal.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtZero();
        } else {
            return eSubVal.status();
        }
    }
} sIsMemberCmd;

class SaddCommand: public Command {
 public:
    SaddCommand()
        :Command("sadd") {
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

        if (args.size() >= 30000) {
            return {ErrorCodes::ERR_PARSEOPT, "exceed sadd batch lim"};
        }

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);

        RecordKey metaRk(pCtx->getDbId(), RecordType::RT_SET_META, key, "");
        std::string metaKeyEnc = metaRk.encode();
        uint32_t storeId = Command::getStoreId(sess, key);

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, storeId, metaRk);
        if (rv.status().code() != ErrorCodes::ERR_OK &&
                rv.status().code() != ErrorCodes::ERR_EXPIRED &&
                rv.status().code() != ErrorCodes::ERR_NOTFOUND) {
            return rv.status();
        }

        auto storeLock = Command::lockDBByKey(sess,
                                              key,
                                              mgl::LockMode::LOCK_IX);
        if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
            return {ErrorCodes::ERR_BUSY, "key locked"};
        }

        PStore kvstore = Command::getStoreById(sess, storeId);

        for (uint32_t i = 0; i < RETRY_CNT; ++i) {
            auto ptxn = kvstore->createTransaction();
            if (!ptxn.ok()) {
                return ptxn.status();
            }
            std::unique_ptr<Transaction> txn = std::move(ptxn.value());
            Expected<std::string> s =
                genericSAdd(sess, kvstore, metaRk, args);
            if (s.ok()) {
                return s.value();
            }
            if (s.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
                return s.status();
            }
            if (i == RETRY_CNT - 1) {
                return s.status();
            } else {
                continue;
            }
        }

        INVARIANT(0);
        return {ErrorCodes::ERR_INTERNAL, "not reachable"};
    }
} saddCommand;

class ScardCommand: public Command {
 public:
    ScardCommand()
        :Command("scard") {
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

        RecordKey metaRk(pCtx->getDbId(), RecordType::RT_SET_META, key, "");
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

        Expected<SetMetaValue> exptSetMeta =
            SetMetaValue::decode(rv.value().getValue());
        if (!exptSetMeta.ok()) {
            return exptSetMeta.status();
        }
        return fmtLongLong(exptSetMeta.value().getCount());
    }
} scardCommand;

class SRemCommand: public Command {
 public:
    SRemCommand()
        :Command("srem") {
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

        if (args.size() >= 30000) {
            return {ErrorCodes::ERR_PARSEOPT, "exceed sadd batch lim"};
        }

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);

        RecordKey metaRk(pCtx->getDbId(), RecordType::RT_SET_META, key, "");
        std::string metaKeyEnc = metaRk.encode();
        uint32_t storeId = Command::getStoreId(sess, key);

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, storeId, metaRk);
        if (rv.status().code() != ErrorCodes::ERR_OK &&
                rv.status().code() != ErrorCodes::ERR_EXPIRED &&
                rv.status().code() != ErrorCodes::ERR_NOTFOUND) {
            return rv.status();
        }

        auto storeLock = Command::lockDBByKey(sess,
                                              key,
                                              mgl::LockMode::LOCK_IX);
        if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
            return {ErrorCodes::ERR_BUSY, "key locked"};
        }

        PStore kvstore = Command::getStoreById(sess, storeId);

        for (uint32_t i = 0; i < RETRY_CNT; ++i) {
            auto ptxn = kvstore->createTransaction();
            if (!ptxn.ok()) {
                return ptxn.status();
            }
            std::unique_ptr<Transaction> txn = std::move(ptxn.value());
            Expected<std::string> s =
                genericSRem(sess, kvstore, metaRk, args);
            if (s.ok()) {
                return s.value();
            }
            if (s.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
                return s.status();
            }
            if (i == RETRY_CNT - 1) {
                return s.status();
            } else {
                continue;
            }
        }

        INVARIANT(0);
        return {ErrorCodes::ERR_INTERNAL, "not reachable"};
    }
} sremCommand;

/*
class SDiffCommand: public Command {
 public:
    SDiffCommand()
        :Command("sdiff") {
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

} sdiffCommand;
*/

}  // namespace tendisplus
