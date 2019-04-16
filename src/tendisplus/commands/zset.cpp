#include <string>
#include <utility>
#include <memory>
#include <algorithm>
#include <cctype>
#include <clocale>
#include <vector>
#include <map>
#include "glog/logging.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/storage/skiplist.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

Expected<std::string> genericZrem(Session *sess,
                            PStore kvstore,
                            const RecordKey& mk,
                            const std::vector<std::string>& subkeys) {
    SessionCtx *pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    auto ptxn = kvstore->createTransaction();
    if (!ptxn.ok()) {
        return ptxn.status();
    }
    std::unique_ptr<Transaction> txn = std::move(ptxn.value());
    Expected<RecordValue> eMeta = kvstore->getKV(mk, txn.get());
    if (!eMeta.ok()) {
        if (eMeta.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtZero();
        } else {
            return eMeta.status();
        }
    }
    auto eMetaContent = ZSlMetaValue::decode(eMeta.value().getValue());
    if (!eMetaContent.ok()) {
        return eMetaContent.status();
    }
    ZSlMetaValue meta = eMetaContent.value();
    SkipList sl(mk.getChunkId(), mk.getDbId(), mk.getPrimaryKey(), meta, kvstore);

    uint32_t cnt = 0;
    for (const auto& subkey : subkeys) {
        RecordKey hk(mk.getChunkId(),
                     pCtx->getDbId(),
                     RecordType::RT_ZSET_H_ELE,
                     mk.getPrimaryKey(),
                     subkey);
        Expected<RecordValue> eValue = kvstore->getKV(hk, txn.get());
        if (!eValue.ok() &&
                eValue.status().code() != ErrorCodes::ERR_NOTFOUND) {
            return eValue.status();
        }
        if (eValue.status().code() == ErrorCodes::ERR_NOTFOUND) {
            continue;
        } else {
            cnt += 1;
            Expected<uint64_t> oldScore =
                ::tendisplus::stoul(eValue.value().getValue());
            if (!oldScore.ok()) {
                return oldScore.status();
            }
            Status s = sl.remove(oldScore.value(), subkey, txn.get());
            if (!s.ok()) {
                return s;
            }
            s = kvstore->delKV(hk, txn.get());
            if (!s.ok()) {
                return s;
            }
        }
    }
    Status s;
    if (sl.getCount() > 1) {
        s = sl.save(txn.get());
    } else {
        INVARIANT(sl.getCount() == 1);
        s = kvstore->delKV(mk, txn.get());
        if (!s.ok()) {
            return s;
        }
        RecordKey head(mk.getChunkId(),
                       pCtx->getDbId(),
                       RecordType::RT_ZSET_S_ELE,
                       mk.getPrimaryKey(),
                       std::to_string(ZSlMetaValue::HEAD_ID));
        s = kvstore->delKV(head, txn.get());
    }
    if (!s.ok()) {
        return s;
    }
    Expected<uint64_t> commitStatus = txn->commit();
    if (!commitStatus.ok()) {
        return commitStatus.status();
    }
    return Command::fmtLongLong(cnt);
}

Expected<std::string> genericZadd(Session *sess,
                            PStore kvstore,
                            const RecordKey& mk,
                            const std::map<std::string, uint64_t>& subKeys,
                            bool isUpdate) {
    uint32_t cnt = 0;
    uint64_t scores = 0;
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
        ZSlMetaValue tmp(1/*lvl*/,
                         ZSlMetaValue::MAX_LAYER,
                         1/*count*/,
                         0/*tail*/);
        RecordValue rv(tmp.encode());
        Status s = kvstore->setKV(mk, rv, txn.get());
        if (!s.ok()) {
            return s;
        }
        RecordKey head(mk.getChunkId(),
                       pCtx->getDbId(),
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

    SkipList sl(mk.getChunkId(), mk.getDbId(), mk.getPrimaryKey(), meta, kvstore);
    std::stringstream ss;
    // sl.traverse(ss, txn.get());
    for (const auto& entry : subKeys) {
        RecordKey hk(mk.getChunkId(),
                     pCtx->getDbId(),
                     RecordType::RT_ZSET_H_ELE,
                     mk.getPrimaryKey(),
                     entry.first);
        RecordValue hv(std::to_string(entry.second));
        Expected<RecordValue> eValue = kvstore->getKV(hk, txn.get());
        if (!eValue.ok() &&
                eValue.status().code() != ErrorCodes::ERR_NOTFOUND) {
            return eValue.status();
        }
        if (eValue.status().code() == ErrorCodes::ERR_NOTFOUND) {
            cnt += 1;
            scores += entry.second;
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
            Expected<uint64_t> oldScore =
                ::tendisplus::stoul(eValue.value().getValue());
            if (!oldScore.ok()) {
                return oldScore.status();
            }
            Status s = sl.remove(oldScore.value(), entry.first, txn.get());
            if (!s.ok()) {
                return s;
            }
            uint64_t newScore = entry.second;
            if (isUpdate) {
                newScore += oldScore.value();
            }
            scores += newScore;
            s = sl.insert(newScore, entry.first, txn.get());
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
    if (isUpdate) {
        return Command::fmtLongLong(scores);
    } else {
        return Command::fmtLongLong(cnt);
    }
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

    RecordKey hk(mk.getChunkId(),
                 mk.getDbId(),
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
    SkipList sl(mk.getChunkId(), mk.getDbId(), mk.getPrimaryKey(), meta, kvstore);
    Expected<uint32_t> rank = sl.rank(score.value(), subkey, txn.get());
    if (!rank.ok()) {
        return rank.status();
    }
    return Command::fmtLongLong(rank.value()-1);
}

class ZRemByRangeGenericCommand: public Command {
 public:
    enum class Type {
        RANK,
        SCORE,
        LEX,
    };

    ZRemByRangeGenericCommand(const std::string& name)
        :Command(name) {
        if (name == "zremrangebyscore") {
            _type = Type::SCORE;
        } else if (name == "zremrangebylex") {
            _type = Type::LEX;
        } else if (name == "zremrangebyrank") {
            _type = Type::RANK;
        } else {
            INVARIANT(0);
        }
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

    Expected<std::string> genericZremrange(Session *sess,
                            PStore kvstore,
                            const RecordKey& mk,
                            const Zrangespec& range,
                            const Zlexrangespec& lexrange,
                            int64_t start,
                            int64_t end) {
        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        Expected<RecordValue> eMeta = kvstore->getKV(mk, txn.get());
        if (!eMeta.ok()) {
            if (eMeta.status().code() == ErrorCodes::ERR_NOTFOUND) {
                return Command::fmtZero();
            } else {
                return eMeta.status();
            }
        }
        auto eMetaContent = ZSlMetaValue::decode(eMeta.value().getValue());
        if (!eMetaContent.ok()) {
            return eMetaContent.status();
        }
        ZSlMetaValue meta = eMetaContent.value();
        SkipList sl(mk.getChunkId(), mk.getDbId(), mk.getPrimaryKey(), meta, kvstore);

        if (_type == Type::RANK) {
            int64_t llen = sl.getCount() - 1;
            if (start < 0) {
                start = llen + start;
            }
            if (end < 0) {
                end = llen + end;
            }
            if (start < 0) {
                start = 0;
            }
            if (start > end || start >= llen) {
                return Command::fmtZero();
            }
            if (end >= llen) {
                end = llen - 1;
            }
        }

        std::list<std::pair<uint64_t, std::string>> result;
        if (_type == Type::RANK) {
            auto tmp = sl.removeRangeByRank(start+1, end+1, txn.get());
            if (!tmp.ok()) {
                return tmp.status();
            }
            result = std::move(tmp.value());
        } else if (_type == Type::SCORE) {
            auto tmp = sl.removeRangeByScore(range, txn.get());
            if (!tmp.ok()) {
                return tmp.status();
            }
            result = std::move(tmp.value());
        } else if (_type == Type::LEX) {
            auto tmp = sl.removeRangeByLex(lexrange, txn.get());
            if (!tmp.ok()) {
                return tmp.status();
            }
            result = std::move(tmp.value());
        }
        for (const auto& v : result) {
            RecordKey hk(mk.getChunkId(),
                         pCtx->getDbId(),
                         RecordType::RT_ZSET_H_ELE,
                         mk.getPrimaryKey(),
                         v.second);
            auto s = kvstore->delKV(hk, txn.get());
            if (!s.ok()) {
                return s;
            }
        }

        Status s;
        if (sl.getCount() > 1) {
            s = sl.save(txn.get());
        } else {
            INVARIANT(sl.getCount() == 1);
            s = kvstore->delKV(mk, txn.get());
            if (!s.ok()) {
                return s;
            }
            RecordKey head(mk.getChunkId(),
                           pCtx->getDbId(),
                           RecordType::RT_ZSET_S_ELE,
                           mk.getPrimaryKey(),
                           std::to_string(ZSlMetaValue::HEAD_ID));
            s = kvstore->delKV(head, txn.get());
        }
        if (!s.ok()) {
            return s;
        }
        Expected<uint64_t> commitStatus = txn->commit();
        if (!commitStatus.ok()) {
            return commitStatus.status();
        }
        return Command::fmtLongLong(result.size());
    }

    Expected<std::string> run(Session *sess) final {
        const std::vector<std::string>& args = sess->getArgs();
        const std::string& key = args[1];
        Zrangespec range;
        Zlexrangespec lexrange;
        int64_t start(0), end(0);
        if (_type == Type::RANK) {
            Expected<int64_t> estart = ::tendisplus::stoll(args[2]);
            if (!estart.ok()) {
                return estart.status();
            }
            Expected<int64_t> eend = ::tendisplus::stoll(args[3]);
            if (!eend.ok()) {
                return eend.status();
            }
            start = estart.value();
            end = eend.value();
        } else if (_type == Type::SCORE) {
            if (zslParseRange(args[2].c_str(), args[3].c_str(), &range) != 0) {
                return {ErrorCodes::ERR_PARSEOPT, "parse range failed"};
            }
        } else if (_type == Type::LEX) {
            if (zslParseLexRange(args[2].c_str(), args[3].c_str(), &lexrange) != 0) {
                return {ErrorCodes::ERR_PARSEOPT, "parse range failed"};
            }
        }

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_ZSET_META);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
                rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtZero();
        } else if (!rv.ok()) {
            return rv.status();
        }

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);
        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key, mgl::LockMode::LOCK_X);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_ZSET_META, key, "");
        // uint32_t storeId = expdb.value().dbId;
        std::string metaKeyEnc = metaRk.encode();
        PStore kvstore = expdb.value().store;
        // if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
        //     return {ErrorCodes::ERR_BUSY, "key locked"};
        // }

        for (int32_t i = 0; i < RETRY_CNT; ++i) {
            Expected<std::string> s = genericZremrange(sess,
                                            kvstore,
                                            metaRk,
                                            range,
                                            lexrange,
                                            start,
                                            end);
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

 private:
    Type _type;
};

class ZRemRangeByRankCommand: public ZRemByRangeGenericCommand {
 public:
    ZRemRangeByRankCommand()
        :ZRemByRangeGenericCommand("zremrangebyrank") {
    }
} zremrangebyrankCmd;

class ZRemRangeByScoreCommand: public ZRemByRangeGenericCommand {
 public:
    ZRemRangeByScoreCommand()
        :ZRemByRangeGenericCommand("zremrangebyscore") {
    }
} zremrangebyscoreCmd;

class ZRemRangeByLexCommand: public ZRemByRangeGenericCommand {
 public:
    ZRemRangeByLexCommand()
        :ZRemByRangeGenericCommand("zremrangebylex") {
    }
} zremrangebylexCmd;

class ZRemCommand: public Command {
 public:
    ZRemCommand()
        :Command("zrem") {
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
        std::vector<std::string> subkeys;
        for (size_t i = 2; i < args.size(); ++i) {
            subkeys.push_back(args[i]);
        }
        if (subkeys.size() > 1000) {
            return {ErrorCodes::ERR_PARSEOPT, "exceed batch lim"};
        }

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_ZSET_META);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
                rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtZero();
        } else if (!rv.ok()) {
            return rv.status();
        }

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);
        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key, mgl::LockMode::LOCK_X);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_ZSET_META, key, "");
        // uint32_t storeId = expdb.value().dbId;
        std::string metaKeyEnc = metaRk.encode();
        PStore kvstore = expdb.value().store;
        // if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
        //     return {ErrorCodes::ERR_BUSY, "key locked"};
        // }

        for (int32_t i = 0; i < RETRY_CNT; ++i) {
            Expected<std::string> s = genericZrem(sess,
                                            kvstore,
                                            metaRk,
                                            subkeys);
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
} zremCmd;

class ZCardCommand: public Command {
 public:
    ZCardCommand()
        :Command("zcard") {
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

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_ZSET_META);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
                rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtZero();
        } else if (!rv.ok()) {
            return rv.status();
        }
        auto eMetaContent = ZSlMetaValue::decode(rv.value().getValue());
        if (!eMetaContent.ok()) {
            return eMetaContent.status();
        }
        ZSlMetaValue meta = eMetaContent.value();
        INVARIANT(meta.getCount() > 1);
        return Command::fmtLongLong(meta.getCount()-1);
    }
} zcardCmd;

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

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_ZSET_META);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
                rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtNull();
        } else if (!rv.ok()) {
            return rv.status();
        }

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);
        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key, mgl::LockMode::LOCK_S);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_ZSET_META, key, "");
        // uint32_t storeId = expdb.value().dbId;
        std::string metaKeyEnc = metaRk.encode();
        PStore kvstore = expdb.value().store;
        // if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
        //     return {ErrorCodes::ERR_BUSY, "key locked"};
        // }

        return genericZRank(sess, kvstore, metaRk, subkey);
    }
} zrankCommand;

class ZIncrCommand: public Command {
 public:
    ZIncrCommand()
        :Command("zincrby") {
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
        Expected<uint64_t> score = ::tendisplus::stoul(args[2]);
        if (!score.ok()) {
            return score.status();
        }
        const std::string& subkey = args[3];

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_ZSET_META);
        if (rv.status().code() != ErrorCodes::ERR_OK &&
                rv.status().code() != ErrorCodes::ERR_EXPIRED &&
                rv.status().code() != ErrorCodes::ERR_NOTFOUND) {
            return rv.status();
        }

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);
        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key, mgl::LockMode::LOCK_X);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_ZSET_META, key, "");
        // uint32_t storeId = expdb.value().dbId;
        std::string metaKeyEnc = metaRk.encode();
        PStore kvstore = expdb.value().store;
        constexpr bool ISUPDATE = true;
        for (int32_t i = 0; i < RETRY_CNT; ++i) {
            Expected<std::string> s = genericZadd(sess,
                                            kvstore,
                                            metaRk,
                                            {{subkey, score.value()}},
                                            ISUPDATE);
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
} zincrbyCommand;

class ZCountCommand: public Command {
 public:
    ZCountCommand()
        :Command("zcount") {
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

        Zrangespec range;
        if (zslParseRange(args[2].c_str(), args[3].c_str(), &range) != 0) {
            return {ErrorCodes::ERR_PARSEOPT, "parse range failed"};
        }

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_ZSET_META);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
                rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtZero();
        } else if (!rv.ok()) {
            return rv.status();
        }

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);
        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key, mgl::LockMode::LOCK_S);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_ZSET_META, key, "");
        std::string metaKeyEnc = metaRk.encode();
        PStore kvstore = expdb.value().store;
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        Expected<RecordValue> eMeta = kvstore->getKV(metaRk, txn.get());
        if (!eMeta.ok()) {
            if (eMeta.status().code() == ErrorCodes::ERR_NOTFOUND) {
                return Command::fmtZero();
            } else {
                return eMeta.status();
            }
        }
        auto eMetaContent = ZSlMetaValue::decode(eMeta.value().getValue());
        if (!eMetaContent.ok()) {
            return eMetaContent.status();
        }
        ZSlMetaValue meta = eMetaContent.value();
        SkipList sl(metaRk.getChunkId(), metaRk.getDbId(), key, meta, kvstore);
        auto first = sl.firstInRange(range, txn.get());
        if (!first.ok()) {
            return first.status();
        }
        if (first.value() == nullptr) {
            return Command::fmtZero();
        }
        Expected<uint32_t> rank = sl.rank(
                    first.value()->getScore(),
                    first.value()->getSubKey(),
                    txn.get());
        if (!rank.ok()) {
            return rank.status();
        }
        // sl.getCount()-1 : total skiplist nodes exclude head
        uint32_t count = (sl.getCount() - 1 - (rank.value() - 1));
        auto last = sl.lastInRange(range, txn.get());
        if (!last.ok()) {
            return last.status();
        }
        if (last.value() == nullptr) {
            return Command::fmtLongLong(count);
        }
        rank = sl.rank(
                last.value()->getScore(),
                last.value()->getSubKey(),
                txn.get());
        if (!rank.ok()) {
            return rank.status();
        }
        return Command::fmtLongLong(count - (sl.getCount() - 1 - rank.value()));
    }
} zcountCommand;

class ZlexCountCommand: public Command {
 public:
    ZlexCountCommand()
        :Command("zlexcount") {
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

        Zlexrangespec range;
        if (zslParseLexRange(args[2].c_str(), args[3].c_str(), &range) != 0) {
            return {ErrorCodes::ERR_PARSEOPT, "parse range failed"};
        }

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_ZSET_META);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
                rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtZero();
        } else if (!rv.ok()) {
            return rv.status();
        }

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);
        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key, mgl::LockMode::LOCK_S);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_ZSET_META, key, "");
        std::string metaKeyEnc = metaRk.encode();
        PStore kvstore = expdb.value().store;
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        Expected<RecordValue> eMeta = kvstore->getKV(metaRk, txn.get());
        if (!eMeta.ok()) {
            if (eMeta.status().code() == ErrorCodes::ERR_NOTFOUND) {
                return Command::fmtZero();
            } else {
                return eMeta.status();
            }
        }
        auto eMetaContent = ZSlMetaValue::decode(eMeta.value().getValue());
        if (!eMetaContent.ok()) {
            return eMetaContent.status();
        }
        ZSlMetaValue meta = eMetaContent.value();
        SkipList sl(metaRk.getChunkId(), metaRk.getDbId(), key, meta, kvstore);

        auto first = sl.firstInLexRange(range, txn.get());
        if (!first.ok()) {
            return first.status();
        }
        if (first.value() == nullptr) {
            return Command::fmtZero();
        }
        Expected<uint32_t> rank = sl.rank(
                    first.value()->getScore(),
                    first.value()->getSubKey(),
                    txn.get());
        if (!rank.ok()) {
            return rank.status();
        }
        // sl.getCount()-1 : total skiplist nodes exclude head
        uint32_t count = (sl.getCount() - 1 - (rank.value() - 1));
        auto last = sl.lastInLexRange(range, txn.get());
        if (!last.ok()) {
            return last.status();
        }
        if (last.value() == nullptr) {
            return Command::fmtLongLong(count);
        }
        rank = sl.rank(
                last.value()->getScore(),
                last.value()->getSubKey(),
                txn.get());
        if (!rank.ok()) {
            return rank.status();
        }
        return Command::fmtLongLong(count - (sl.getCount() - 1 - rank.value()));
    }
} zlexCntCmd;

class ZRangeByScoreGenericCommand: public Command {
 public:
    ZRangeByScoreGenericCommand(const std::string& name)
            :Command(name) {
        if (name == "zrangebyscore") {
            _rev = false;
        } else {
            _rev = true;
        }
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
        const std::string& key = args[1];
        int64_t offset = 0;
        int64_t limit = -1;
        int withscore = 0;
        int minidx, maxidx;
        Zrangespec range;
        if (_rev) {
            maxidx = 2; minidx = 3;
        } else {
            minidx = 2; maxidx = 3;
        }
        if (zslParseRange(args[minidx].c_str(), args[maxidx].c_str(), &range) != 0) {
            return {ErrorCodes::ERR_PARSEOPT, "parse range failed"};
        }

        if (args.size() > 4) {
            int remaining = args.size() - 4;
            int pos = 4;
            while (remaining) {
                if (remaining >= 1 && args[pos] == "withscores") {
                    pos++;
                    remaining--;
                    withscore = 1;
                } else if (remaining >= 3 && args[pos] == "limit") {
                    Expected<int64_t> eoffset = ::tendisplus::stoll(args[pos+1]);
                    if (!eoffset.ok()) {
                        return eoffset.status();
                    }
                    Expected<int64_t> elimit = ::tendisplus::stoll(args[pos+2]);
                    if (!elimit.ok()) {
                        return elimit.status();
                    }
                    pos += 3;
                    remaining -= 3;
                } else {
                    return {ErrorCodes::ERR_PARSEOPT, "syntax error"};
                }
            }
        }

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_ZSET_META);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
                rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtZeroBulkLen();
        } else if (!rv.ok()) {
            return rv.status();
        }

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);
        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key, mgl::LockMode::LOCK_S);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_ZSET_META, key, "");
        std::string metaKeyEnc = metaRk.encode();
        PStore kvstore = expdb.value().store;
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        Expected<RecordValue> eMeta = kvstore->getKV(metaRk, txn.get());
        if (!eMeta.ok()) {
            if (eMeta.status().code() == ErrorCodes::ERR_NOTFOUND) {
                return Command::fmtZeroBulkLen();
            } else {
                return eMeta.status();
            }
        }

        auto eMetaContent = ZSlMetaValue::decode(eMeta.value().getValue());
        if (!eMetaContent.ok()) {
            return eMetaContent.status();
        }
        ZSlMetaValue meta = eMetaContent.value();
        SkipList sl(metaRk.getChunkId(), metaRk.getDbId(), metaRk.getPrimaryKey(), meta, kvstore);
        // std::cout<< range.min << ' ' << range.max << ' ' << offset << ' ' << limit << std::endl;
        auto arr = sl.scanByScore(range, offset, limit, _rev, txn.get());
        if (!arr.ok()) {
            return arr.status();
        }
        std::stringstream ss;
        if (withscore) {
            Command::fmtMultiBulkLen(ss, arr.value().size()*2);
        } else {
            Command::fmtMultiBulkLen(ss, arr.value().size());
        }
        for (const auto& v : arr.value()) {
            Command::fmtBulk(ss, v.second);
            if (withscore) {
                Command::fmtBulk(ss, std::to_string(v.first));
            }
        }
        return ss.str();
    }

 private:
    bool _rev;
};

class ZRangeByScoreCommand: public ZRangeByScoreGenericCommand {
 public:
    ZRangeByScoreCommand()
        :ZRangeByScoreGenericCommand("zrangebyscore") {
    }
} zrangebyscoreCmd;

class ZRevRangeByScoreCommand: public ZRangeByScoreGenericCommand {
 public:
    ZRevRangeByScoreCommand()
        :ZRangeByScoreGenericCommand("zrevrangebyscore") {
    }
} zrevrangebyscoreCmd;

class ZRangeByLexGenericCommand: public Command {
 public:
    ZRangeByLexGenericCommand(const std::string& name)
            :Command(name) {
        if (name == "zrangebylex") {
            _rev = false;
        } else {
            _rev = true;
        }
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
        const std::string& key = args[1];
        int64_t offset = 0;
        int64_t limit = -1;
        int minidx, maxidx;
        if (_rev) {
            maxidx = 2; minidx = 3;
        } else {
            minidx = 2; maxidx = 3;
        }

        Zlexrangespec range;
        if (zslParseLexRange(args[minidx].c_str(), args[maxidx].c_str(), &range) != 0) {
            return {ErrorCodes::ERR_PARSEOPT, "min or max not valid string range item"};
        }

        if (args.size() > 4) {
            int remaining = args.size() - 4;
            int pos = 4;
            while (remaining) {
                if (remaining >= 3 && args[pos] == "limit") {
                    Expected<int64_t> eoffset = ::tendisplus::stoll(args[pos+1]);
                    if (!eoffset.ok()) {
                        return eoffset.status();
                    }
                    Expected<int64_t> elimit = ::tendisplus::stoll(args[pos+2]);
                    if (!elimit.ok()) {
                        return elimit.status();
                    }
                    pos += 3;
                    remaining -= 3;
                } else {
                    return {ErrorCodes::ERR_PARSEOPT, "syntax error"};
                }
            }
        }

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_ZSET_META);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
                rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtZeroBulkLen();
        } else if (!rv.ok()) {
            return rv.status();
        }

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);
        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key, mgl::LockMode::LOCK_S);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_ZSET_META, key, "");
        std::string metaKeyEnc = metaRk.encode();
        PStore kvstore = expdb.value().store;
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        Expected<RecordValue> eMeta = kvstore->getKV(metaRk, txn.get());
        if (!eMeta.ok()) {
            if (eMeta.status().code() == ErrorCodes::ERR_NOTFOUND) {
                return Command::fmtZeroBulkLen();
            } else {
                return eMeta.status();
            }
        }

        auto eMetaContent = ZSlMetaValue::decode(eMeta.value().getValue());
        if (!eMetaContent.ok()) {
            return eMetaContent.status();
        }
        ZSlMetaValue meta = eMetaContent.value();
        SkipList sl(metaRk.getChunkId(), metaRk.getDbId(), metaRk.getPrimaryKey(), meta, kvstore);
        // std::cout<< range.min << ' ' << range.max << ' ' << offset << ' ' << limit << std::endl;
        auto arr = sl.scanByLex(range, offset, limit, _rev, txn.get());
        if (!arr.ok()) {
            return arr.status();
        }
        std::stringstream ss;
        Command::fmtMultiBulkLen(ss, arr.value().size());
        for (const auto& v : arr.value()) {
            Command::fmtBulk(ss, v.second);
        }
        return ss.str();
    }

 private:
    bool _rev;
};

class ZRangeByLexCommand: public ZRangeByLexGenericCommand {
 public:
    ZRangeByLexCommand()
        :ZRangeByLexGenericCommand("zrangebylex") {
    }
} zrangebylexCmd;

class ZRevRangeByLexCommand: public ZRangeByLexGenericCommand {
 public:
    ZRevRangeByLexCommand()
        :ZRangeByLexGenericCommand("zrevrangebylex") {
    }
} zrevrangebylexCmd;

class ZRangeGenericCommand: public Command {
 public:
    ZRangeGenericCommand(const std::string& name)
            :Command(name) {
        if (name == "zrange") {
            _rev = false;
        } else {
            _rev = true;
        }
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
        const std::string& key = args[1];
        Expected<int64_t> estart = ::tendisplus::stoll(args[2]);
        if (!estart.ok()) {
            return estart.status();
        }
        Expected<int64_t> eend = ::tendisplus::stoll(args[3]);
        if (!eend.ok()) {
            return eend.status();
        }
        int64_t start = estart.value();
        int64_t end = eend.value();
        bool withscore = (args.size() == 5 && args[4] == "withscores");
        if (args.size() > 5) {
            return {ErrorCodes::ERR_PARSEOPT, "syntax error"};
        }

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_ZSET_META);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
                rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtZeroBulkLen();
        } else if (!rv.ok()) {
            return rv.status();
        }

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);
        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key, mgl::LockMode::LOCK_S);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_ZSET_META, key, "");
        std::string metaKeyEnc = metaRk.encode();
        PStore kvstore = expdb.value().store;
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        Expected<RecordValue> eMeta = kvstore->getKV(metaRk, txn.get());
        if (!eMeta.ok()) {
            if (eMeta.status().code() == ErrorCodes::ERR_NOTFOUND) {
                return Command::fmtZeroBulkLen();
            } else {
                return eMeta.status();
            }
        }

        auto eMetaContent = ZSlMetaValue::decode(eMeta.value().getValue());
        if (!eMetaContent.ok()) {
            return eMetaContent.status();
        }
        ZSlMetaValue meta = eMetaContent.value();
        SkipList sl(metaRk.getChunkId(), metaRk.getDbId(), metaRk.getPrimaryKey(), meta, kvstore);
        int64_t len = sl.getCount() - 1;
        if (start < 0) {
            start = len + start;
        }
        if (end < 0) {
            end = len + end;
        }
        if (start < 0) {
            start = 0;
        }
        if (start > end || start >= len) {
            return Command::fmtZeroBulkLen();
        }
        if (end >= len) {
            end = len - 1;
        }
        int64_t rangeLen = end - start + 1;
        auto arr = sl.scanByRank(start, rangeLen, _rev, txn.get());
        if (!arr.ok()) {
            return arr.status();
        }
        std::stringstream ss;
        if (withscore) {
            Command::fmtMultiBulkLen(ss, arr.value().size()*2);
        } else {
            Command::fmtMultiBulkLen(ss, arr.value().size());
        }
        for (const auto& v : arr.value()) {
            Command::fmtBulk(ss, v.second);
            if (withscore) {
                Command::fmtBulk(ss, std::to_string(v.first));
            }
        }
        return ss.str();
    }

 private:
    bool _rev;
};

class ZRangeCommand: public ZRangeGenericCommand {
 public:
    ZRangeCommand()
        :ZRangeGenericCommand("zrange") {
    }
} zrangeCmd;

class ZRevRangeCommand: public ZRangeGenericCommand {
 public:
    ZRevRangeCommand()
        :ZRangeGenericCommand("zrevrange") {
    }
} zrevrangeCmd;

class ZScoreCommand: public Command {
 public:
    ZScoreCommand()
        :Command("zscore") {
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

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_ZSET_META);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
                rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtNull();
        } else if (!rv.ok()) {
            return rv.status();
        }

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);
        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key, mgl::LockMode::LOCK_S);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_ZSET_META, key, "");
        std::string metaKeyEnc = metaRk.encode();
        PStore kvstore = expdb.value().store;

        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        RecordKey hk(expdb.value().chunkId,
                     pCtx->getDbId(),
                     RecordType::RT_ZSET_H_ELE,
                     key,
                     subkey);
        Expected<RecordValue> eValue = kvstore->getKV(hk, txn.get());
        if (!eValue.ok() &&
                eValue.status().code() != ErrorCodes::ERR_NOTFOUND) {
            return eValue.status();
        }
        if (eValue.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtNull();
        }
        Expected<uint64_t> oldScore =
            ::tendisplus::stoul(eValue.value().getValue());
        if (!oldScore.ok()) {
            return oldScore.status();
        }
        return Command::fmtBulk(std::to_string(oldScore.value()));
    }
} zscoreCmd;

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

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_ZSET_META);
        if (rv.status().code() != ErrorCodes::ERR_OK &&
                rv.status().code() != ErrorCodes::ERR_EXPIRED &&
                rv.status().code() != ErrorCodes::ERR_NOTFOUND) {
            return rv.status();
        }

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);
        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key, mgl::LockMode::LOCK_X);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_ZSET_META, key, "");
        std::string metaKeyEnc = metaRk.encode();
        PStore kvstore = expdb.value().store;
        constexpr bool ISUPDATE = false;
        for (int32_t i = 0; i < RETRY_CNT; ++i) {
            Expected<std::string> s =
                genericZadd(sess, kvstore, metaRk, scoreMap, ISUPDATE);
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
