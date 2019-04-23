#include <string>
#include <utility>
#include <memory>
#include <algorithm>
#include <cctype>
#include <clocale>
#include <vector>
#include <map>
#include <cmath>
#include "glog/logging.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/storage/skiplist.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/storage/varint.h"

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
    SkipList sl(mk.getChunkId(), mk.getDbId(), mk.getPrimaryKey(),
                        meta, kvstore);

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
            Expected<double> oldScore =
                ::tendisplus::doubleDecode(eValue.value().getValue());
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
                            const std::map<std::string, double>& subKeys,
                            int flags) {
    /* The following vars are used in order to track what the command actually
    * did during the execution, to reply to the client and to trigger the
    * notification of keyspace change. */
    int added = 0;      /* Number of new elements added. */
    int updated = 0;    /* Number of elements with updated score. */
    /* Number of elements processed, may remain zero with options like XX. */
    int processed = 0;
    /* Turn options into simple to check vars. */
    bool incr = (flags & ZADD_INCR) != 0;
    bool nx = (flags & ZADD_NX) != 0;
    bool xx = (flags & ZADD_XX) != 0;
    bool ch = (flags & ZADD_CH) != 0;

    /* Check for incompatible options. */
    if (nx && xx) {
        return{ ErrorCodes::ERR_PARSEPKT,
            "XX and NX options at the same time are not compatible" };
    }

    if (incr && subKeys.size() > 1) {
        return{ ErrorCodes::ERR_PARSEPKT,
            "INCR option supports a single increment-element pair" };
    }

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

    SkipList sl(mk.getChunkId(), mk.getDbId(), mk.getPrimaryKey(),
                        meta, kvstore);
    std::stringstream ss;
    double newScore = 0;
    // sl.traverse(ss, txn.get());
    for (const auto& entry : subKeys) {
        RecordKey hk(mk.getChunkId(),
                     pCtx->getDbId(),
                     RecordType::RT_ZSET_H_ELE,
                     mk.getPrimaryKey(),
                     entry.first);
        newScore = entry.second;
        if (std::isnan(newScore)) {
            return { ErrorCodes::ERR_NAN, "" };
        }
        Expected<RecordValue> eValue = kvstore->getKV(hk, txn.get());
        if (!eValue.ok() &&
                eValue.status().code() != ErrorCodes::ERR_NOTFOUND) {
            return eValue.status();
        }
        if (eValue.status().code() == ErrorCodes::ERR_NOTFOUND) {
            if (xx) {
                continue;
            }
            added++;
            processed++;
            Status s = sl.insert(entry.second, entry.first, txn.get());
            if (!s.ok()) {
                return s;
            }
            RecordValue hv(newScore);
            s = kvstore->setKV(hk, hv, txn.get());
            if (!s.ok()) {
                return s;
            }
        } else {
            if (nx) {
                continue;
            }
            Expected<double> oldScore =
                ::tendisplus::doubleDecode(eValue.value().getValue());
            if (!oldScore.ok()) {
                return oldScore.status();
            }
            if (incr) {
                newScore += oldScore.value();
                if (std::isnan(newScore)) {
                    return { ErrorCodes::ERR_NAN, "" };
                }
            }

            if (newScore == oldScore.value()) {
                continue;
            }
            updated++;
            processed++;
            // change score
            Status s = sl.remove(oldScore.value(), entry.first, txn.get());
            if (!s.ok()) {
                return s;
            }
            s = sl.insert(newScore, entry.first, txn.get());
            if (!s.ok()) {
                return s;
            }
            RecordValue hv(newScore);
            s = kvstore->setKV(hk, hv, txn.get());
            if (!s.ok()) {
                return s;
            }
        }
    }
    // NOTE(vinchen): skiplist save one time
    Status s = sl.save(txn.get());
    if (!s.ok()) {
        return s;
    }
    Expected<uint64_t> commitStatus = txn->commit();
    if (!commitStatus.ok()) {
        return commitStatus.status();
    }
    if (incr) { /* ZINCRBY or INCR option. */
        if (processed)
            return Command::fmtBulk(::tendisplus::dtos(newScore));
        else
            return Command::fmtNull();
    } else { /* ZADD */
        return Command::fmtLongLong(ch ? added + updated : added);
    }
}

Expected<std::string> genericZRank(Session *sess,
                                   PStore kvstore,
                                   const RecordKey& mk,
                                   const std::string& subkey,
                                   bool reverse) {
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
    Expected<double> score =
                ::tendisplus::doubleDecode(eValue.value().getValue());
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
    SkipList sl(mk.getChunkId(), mk.getDbId(), mk.getPrimaryKey(),
                    meta, kvstore);
    Expected<uint32_t> rank = sl.rank(score.value(), subkey, txn.get());
    if (!rank.ok()) {
        return rank.status();
    }
    int64_t r = rank.value() - 1;
    if (reverse) {
        r = meta.getCount() - 2 - r;
    }
    INVARIANT(r >= 0);
    return Command::fmtLongLong(r);
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
        SkipList sl(mk.getChunkId(), mk.getDbId(), mk.getPrimaryKey(),
                    meta, kvstore);

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

        std::list<std::pair<double, std::string>> result;
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
            if (zslParseLexRange(args[2].c_str(), args[3].c_str(), &lexrange) != 0) {  // NOLINT:whitespace/line_length
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
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key,
                            mgl::LockMode::LOCK_X);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(),
                            RecordType::RT_ZSET_META, key, "");
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
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key,
                        mgl::LockMode::LOCK_X);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(),
                            RecordType::RT_ZSET_META, key, "");
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
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key,
                            mgl::LockMode::LOCK_S);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(),
                            RecordType::RT_ZSET_META, key, "");
        // uint32_t storeId = expdb.value().dbId;
        std::string metaKeyEnc = metaRk.encode();
        PStore kvstore = expdb.value().store;
        // if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
        //     return {ErrorCodes::ERR_BUSY, "key locked"};
        // }

        return genericZRank(sess, kvstore, metaRk, subkey, false);
    }
} zrankCmd;


class ZRevRankCommand : public Command {
 public:
    ZRevRankCommand()
        :Command("zrevrank") {
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
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key,
                                    mgl::LockMode::LOCK_S);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(),
                                RecordType::RT_ZSET_META, key, "");
        // uint32_t storeId = expdb.value().dbId;
        std::string metaKeyEnc = metaRk.encode();
        PStore kvstore = expdb.value().store;
        // if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
        //     return {ErrorCodes::ERR_BUSY, "key locked"};
        // }

        return genericZRank(sess, kvstore, metaRk, subkey, true);
    }
} zrevrankCmd;

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
        Expected<double> score = ::tendisplus::stod(args[2]);
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
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key,
                                    mgl::LockMode::LOCK_X);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(),
                                RecordType::RT_ZSET_META, key, "");
        // uint32_t storeId = expdb.value().dbId;
        std::string metaKeyEnc = metaRk.encode();
        PStore kvstore = expdb.value().store;
        int flag = ZADD_INCR;
        for (int32_t i = 0; i < RETRY_CNT; ++i) {
            Expected<std::string> s = genericZadd(sess,
                                            kvstore,
                                            metaRk,
                                            {{subkey, score.value()}},
                                            flag);
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
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key,
                                            mgl::LockMode::LOCK_S);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(),
                                RecordType::RT_ZSET_META, key, "");
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
        auto f = sl.firstInRange(range, txn.get());
        if (!f.ok()) {
            return f.status();
        }
        if (f.value() == SkipList::INVALID_POS) {
            return Command::fmtZero();
        }
        auto first = sl.getCacheNode(f.value());
        Expected<uint32_t> rank = sl.rank(
                    first->getScore(),
                    first->getSubKey(),
                    txn.get());
        if (!rank.ok()) {
            return rank.status();
        }
        // sl.getCount()-1 : total skiplist nodes exclude head
        uint32_t count = (sl.getCount() - 1 - (rank.value() - 1));
        auto l = sl.lastInRange(range, txn.get());
        if (!l.ok()) {
            return l.status();
        }
        if (l.value() == SkipList::INVALID_POS) {
            return Command::fmtLongLong(count);
        }
        auto last = sl.getCacheNode(l.value());
        rank = sl.rank(
                last->getScore(),
                last->getSubKey(),
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
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key,
                                            mgl::LockMode::LOCK_S);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(),
                                RecordType::RT_ZSET_META, key, "");
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

        auto f = sl.firstInLexRange(range, txn.get());
        if (!f.ok()) {
            return f.status();
        }
        if (f.value() == SkipList::INVALID_POS) {
            return Command::fmtZero();
        }
        auto first = sl.getCacheNode(f.value());
        Expected<uint32_t> rank = sl.rank(
                    first->getScore(),
                    first->getSubKey(),
                    txn.get());
        if (!rank.ok()) {
            return rank.status();
        }
        // sl.getCount()-1 : total skiplist nodes exclude head
        uint32_t count = (sl.getCount() - 1 - (rank.value() - 1));
        auto l = sl.lastInLexRange(range, txn.get());
        if (!l.ok()) {
            return l.status();
        }
        if (l.value() == SkipList::INVALID_POS) {
            return Command::fmtLongLong(count);
        }
        auto last = sl.getCacheNode(l.value());
        rank = sl.rank(
                last->getScore(),
                last->getSubKey(),
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
        if (zslParseRange(args[minidx].c_str(), args[maxidx].c_str(), &range) != 0) {  // NOLINT:whitespace/line_length
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
                    Expected<int64_t> eoffset =
                                    ::tendisplus::stoll(args[pos+1]);
                    if (!eoffset.ok()) {
                        return eoffset.status();
                    }
                    Expected<int64_t> elimit =
                                    ::tendisplus::stoll(args[pos+2]);
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
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key,
                                            mgl::LockMode::LOCK_S);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(),
                                            RecordType::RT_ZSET_META, key, "");
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
        SkipList sl(metaRk.getChunkId(), metaRk.getDbId(),
                    metaRk.getPrimaryKey(), meta, kvstore);
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
                Command::fmtBulk(ss, ::tendisplus::dtos(v.first));
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
        if (zslParseLexRange(args[minidx].c_str(), args[maxidx].c_str(), &range) != 0) { // NOLINT:whitespace/line_length
            return {ErrorCodes::ERR_PARSEOPT, "min or max not valid string range item"}; // NOLINT:whitespace/line_length
        }

        if (args.size() > 4) {
            int remaining = args.size() - 4;
            int pos = 4;
            while (remaining) {
                if (remaining >= 3 && args[pos] == "limit") {
                    Expected<int64_t> eoffset =
                                    ::tendisplus::stoll(args[pos+1]);
                    if (!eoffset.ok()) {
                        return eoffset.status();
                    }
                    Expected<int64_t> elimit =
                                    ::tendisplus::stoll(args[pos+2]);
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
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key,
                                                mgl::LockMode::LOCK_S);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(),
                         RecordType::RT_ZSET_META, key, "");
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
        SkipList sl(metaRk.getChunkId(), metaRk.getDbId(),
                    metaRk.getPrimaryKey(), meta, kvstore);
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
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key,
                        mgl::LockMode::LOCK_S);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(),
                            RecordType::RT_ZSET_META, key, "");
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
        SkipList sl(metaRk.getChunkId(), metaRk.getDbId(),
                        metaRk.getPrimaryKey(), meta, kvstore);
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
                Command::fmtBulk(ss, ::tendisplus::dtos(v.first));
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
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key,
                                mgl::LockMode::LOCK_S);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(),
                            RecordType::RT_ZSET_META, key, "");
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
        Expected<double> oldScore =
            ::tendisplus::doubleDecode(eValue.value().getValue());
        if (!oldScore.ok()) {
            return oldScore.status();
        }
        return Command::fmtBulk(::tendisplus::dtos(oldScore.value()));
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

    /* Add a new element or update the score of an existing element in a sorted
    * set, regardless of its encoding.
    *
    * The set of flags change the command behavior. They are passed with an integer
    * pointer since the function will clear the flags and populate them with
    * other flags to indicate different conditions.
    *
    * The input flags are the following:
    *
    * ZADD_INCR: Increment the current element score by 'score' instead of updating
    *            the current element score. If the element does not exist, we
    *            assume 0 as previous score.
    * ZADD_NX:   Perform the operation only if the element does not exist.
    * ZADD_XX:   Perform the operation only if the element already exist.
    *
    * When ZADD_INCR is used, the new score of the element is stored in
    * '*newscore' if 'newscore' is not NULL.
    *
    * The returned flags are the following:
    *
    * ZADD_NAN:     The resulting score is not a number.
    * ZADD_ADDED:   The element was added (not present before the call).
    * ZADD_UPDATED: The element score was updated.
    * ZADD_NOP:     No operation was performed because of NX or XX.
    *
    * Return value:
    *
    * The function returns 1 on success, and sets the appropriate flags
    * ADDED or UPDATED to signal what happened during the operation (note that
    * none could be set if we re-added an element using the same score it used
    * to have, or in the case a zero increment is used).
    *
    * The function returns 0 on erorr, currently only when the increment
    * produces a NAN condition, or when the 'score' value is NAN since the
    * start.
    *
    * The commad as a side effect of adding a new element may convert the sorted
    * set internal encoding from ziplist to hashtable+skiplist.
    *
    * Memory managemnet of 'ele':
    *
    * The function does not take ownership of the 'ele' SDS string, but copies
    * it if needed. */
    Expected<std::string> run(Session *sess) final {
        const std::vector<std::string>& args = sess->getArgs();

        int flag = ZADD_NONE;

        size_t i = 2;
        while (i < args.size()) {
            if (toLower(args[i]) == "nx") {
                flag |= ZADD_NX;
            } else if (toLower(args[i]) == "xx") {
                flag |= ZADD_XX;
            } else if (toLower(args[i]) == "ch") {
                flag |= ZADD_CH;
            } else if (toLower(args[i]) == "incr") {
                flag |= ZADD_INCR;
            } else {
                break;
            }
            i++;
        }

        if ((args.size() - i)%2 != 0 || args.size() - i == 0) {
            return {ErrorCodes::ERR_PARSEOPT, "invalid zadd params len"};
        }
        const std::string& key = args[1];
        std::map<std::string, double> scoreMap;
        for (; i < args.size(); i += 2) {
            const std::string& subkey = args[i+1];
            Expected<double> score = ::tendisplus::stod(args[i]);
            if (!score.ok()) {
                return score.status();
            }
            scoreMap[subkey] = score.value();
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
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key,
                                mgl::LockMode::LOCK_X);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(),
                            RecordType::RT_ZSET_META, key, "");
        std::string metaKeyEnc = metaRk.encode();
        PStore kvstore = expdb.value().store;
        for (int32_t i = 0; i < RETRY_CNT; ++i) {
            Expected<std::string> s =
                genericZadd(sess, kvstore, metaRk, scoreMap, flag);
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

class ZSetCountCommand : public Command {
 public:
    ZSetCountCommand()
        :Command("zsetcount") {
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
        // const std::vector<std::string>& args = sess->getArgs();

        // TODO(vinchen): support it later
        return Command::fmtOK();
    }
} zsetcountCmd;

}  // namespace tendisplus
