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
#include "tendisplus/storage/skiplist.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

Expected<std::string> genericZadd(Session *sess,
                            PStore kvstore,
                            const RecordKey& mk,
                            const std::map<std::string, uint64_t>& subKeys) {
    uint32_t cnt = 0;
    SessionCtx *pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    auto ptxn = kvstore->createTransaction();
    if (!ptxn.ok()) {
        return ptxn.status();
    }
    std::unique_ptr<Transaction> txn = std::move(ptxn.value());
    ZSlMetaValue meta;

    Expected<RecordValue> eMeta = kvstore->getKV(mk, txn.get());

    if (!eMeta.ok() && eMeta.status().code() != ErrorCodes::ERR_NOTFOUND) {
        return eMeta.status();
    }
    if (eMeta.ok()) {
        auto eMetaContent = ZSlMetaValue::decode(eMeta.value().getValue());
        if (!eMetaContent.ok()) {
            return eMetaContent.status();
        }
        meta = eMetaContent.value();
    } else {
        INVARIANT(eMeta.status().code() == ErrorCodes::ERR_NOTFOUND);
        // head node also included into the count
        ZSlMetaValue tmp(1/*lvl*/, ZSlMetaValue::MAX_LAYER, 1/*count*/);
        RecordValue rv(tmp.encode());
        Status s = kvstore->setKV(mk, rv, txn.get());
        if (!s.ok()) {
            return s;
        }
        RecordKey head(pCtx->getDbId(),
                       RecordType::RT_ZSET_S_ELE,
                       mk.getPrimaryKey(),
                       std::to_string(ZSlMetaValue::HEAD_ID));
        ZSlEleValue headVal;
        RecordValue subRv(headVal.encode());
        s = kvstore->setKV(head, subRv, txn.get());
        if (!s.ok()) {
            return s;
        }
        Expected<RecordValue> eMeta = kvstore->getKV(mk, txn.get());
        if (!eMeta.ok()) {
            return eMeta.status();
        }
        auto eMetaContent = ZSlMetaValue::decode(eMeta.value().getValue());
        if (!eMetaContent.ok()) {
            return eMetaContent.status();
        }
        meta = eMetaContent.value();
    }

    SkipList sl(mk.getDbId(), mk.getPrimaryKey(), meta, kvstore);
    for (const auto& entry : subKeys) {
        RecordKey hk(pCtx->getDbId(),
                         RecordType::RT_ZSET_H_ELE,
                         mk.getPrimaryKey(),
                         entry.first);
        RecordValue hv(std::to_string(entry.second));
        Expected<RecordValue> eValue = kvstore->getKV(hk, txn.get());
        if (!eValue.ok() && eValue.status().code() != ErrorCodes::ERR_NOTFOUND) {
            return eValue.status();
        }
        if (eValue.status().code() == ErrorCodes::ERR_NOTFOUND) {
            cnt += 1;
            Status s = sl.insert(entry.second, entry.first, txn.get());
            if (!s.ok()) {
                return s;
            }
            s = kvstore->setKV(hk, hv, txn.get());
            if (!s.ok()) {
                return s;
            }
        } else {
            // change score
            Expected<uint64_t> oldScore = ::tendisplus::stoul(eValue.value().getValue());
            if (!oldScore.ok()) {
                return oldScore.status();
            }
            Status s = sl.remove(oldScore.value(), entry.first, txn.get());
            if (!s.ok()) {
                return s;
            }
            s = sl.insert(entry.second, entry.first, txn.get());
            if (!s.ok()) {
                return s;
            }
            s = kvstore->setKV(hk, hv, txn.get());
            if (!s.ok()) {
                return s;
            }
        }
    }
    Status s = sl.save(txn.get());
    if (!s.ok()) {
        return s;
    }
    Expected<uint64_t> commitStatus = txn->commit();
    if (!commitStatus.ok()) {
        return commitStatus.status();
    }
    return Command::fmtLongLong(cnt);
}

Expected<std::string> genericZRank(Session *sess,
                                   PStore kvstore,
                                   const RecordKey& mk,
                                   const std::string& subkey) {
    auto ptxn = kvstore->createTransaction();
    if (!ptxn.ok()) {
        return ptxn.status();
    }
    std::unique_ptr<Transaction> txn = std::move(ptxn.value());
 
    RecordKey hk(mk.getDbId(),
                 RecordType::RT_ZSET_H_ELE,
                 mk.getPrimaryKey(),
                 subkey);
    Expected<RecordValue> eValue = kvstore->getKV(hk, txn.get());
    if (!eValue.ok()) {
        if (eValue.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtNull();
        }
        return eValue.status();
    }
    Expected<uint64_t> score = ::tendisplus::stoul(eValue.value().getValue());
    if (!score.ok()) {
        return score.status();
    }
    Expected<RecordValue> mv = kvstore->getKV(mk, txn.get());
    if (!mv.ok()) {
        // since we have found it in the hash structure
        INVARIANT(mv.status().code() != ErrorCodes::ERR_NOTFOUND);
        return mv.status();
    }

    auto eMetaContent = ZSlMetaValue::decode(mv.value().getValue());
    if (!eMetaContent.ok()) {
        return eMetaContent.status();
    }
    ZSlMetaValue meta = eMetaContent.value();
    SkipList sl(mk.getDbId(), mk.getPrimaryKey(), meta, kvstore);
    Expected<uint32_t> rank = sl.rank(score.value(), subkey, txn.get());
    if (!rank.ok()) {
        return rank.status();
    }
    return Command::fmtLongLong(rank.value()-1);
}

class ZRankCommand: public Command {
 public:
    ZRankCommand()
        :Command("zrank") {
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
        RecordKey metaKey(pCtx->getDbId(), RecordType::RT_ZSET_META, key, "");
        std::string metaKeyEnc = metaKey.encode();
        uint32_t storeId = Command::getStoreId(sess, key);

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, storeId, metaKey);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
                rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtNull();
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
        return genericZRank(sess, kvstore, metaKey, subkey);
    }
} zrankCommand;

class ZAddCommand: public Command {
 public:
    ZAddCommand()
        :Command("zadd") {
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

    Expected<std::string> run(Session *sess) final {
        const std::vector<std::string>& args = sess->getArgs();
        if ((args.size() - 2)%2 != 0) {
            return {ErrorCodes::ERR_PARSEOPT, "invalid zadd params len"};
        }
        const std::string& key = args[1];
        std::map<std::string, uint64_t> scoreMap;
        for (size_t i = 2; i < args.size(); i += 2) {
            const std::string& subkey = args[i+1];
            Expected<uint64_t> score = ::tendisplus::stoul(args[i]);
            if (!score.ok()) {
                return score.status();
            }
            scoreMap[subkey] = score.value();
        }
        if (scoreMap.size() > 1000) {
            return {ErrorCodes::ERR_PARSEOPT, "exceed batch lim"};
        }

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);
        RecordKey metaKey(pCtx->getDbId(), RecordType::RT_ZSET_META, key, "");
        std::string metaKeyEnc = metaKey.encode();
        uint32_t storeId = Command::getStoreId(sess, key);

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, storeId, metaKey);
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
        for (int32_t i = 0; i < RETRY_CNT; ++i) {
            Expected<std::string> s =
                genericZadd(sess, kvstore, metaKey, scoreMap);
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
} zaddCommand;

}  // namespace tendisplus
