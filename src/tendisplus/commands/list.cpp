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
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

constexpr uint64_t MAXSEQ = 9223372036854775807ULL;
constexpr uint64_t INITSEQ = MAXSEQ/2ULL;
constexpr uint64_t MINSEQ = 1024;

enum class ListPos {
    LP_HEAD,
    LP_TAIL,
};

Expected<std::string> genericPop(Session *sess,
                                 PStore kvstore,
                                 const RecordKey& metaRk,
                                 ListPos pos) {
    auto ptxn = kvstore->createTransaction();
    if (!ptxn.ok()) {
        return ptxn.status();
    }
    std::unique_ptr<Transaction> txn = std::move(ptxn.value());

    ListMetaValue lm(INITSEQ, INITSEQ);
    Expected<RecordValue> rv = kvstore->getKV(metaRk, txn.get());

    if (!rv.ok()) {
        return rv.status();
    }

    uint64_t ttl = 0;
    ttl = rv.value().getTtl();
    Expected<ListMetaValue> exptLm =
        ListMetaValue::decode(rv.value().getValue());
    INVARIANT(exptLm.ok());
    lm = std::move(exptLm.value());

    uint64_t head = lm.getHead();
    uint64_t tail = lm.getTail();
    INVARIANT(head != tail);
    uint64_t idx;
    if (pos == ListPos::LP_HEAD) {
        idx = head++;
    } else {
        idx = --tail;
    }
    RecordKey subRk(metaRk.getChunkId(),
                    metaRk.getDbId(),
                    RecordType::RT_LIST_ELE,
                    metaRk.getPrimaryKey(),
                    std::to_string(idx));
    Expected<RecordValue> subRv = kvstore->getKV(subRk, txn.get());
    if (!subRv.ok()) {
        return subRv.status();
    }
    Status s = kvstore->delKV(subRk, txn.get());
    if (!s.ok()) {
        return s;
    }
    if (head == tail) {
        s = kvstore->delKV(metaRk, txn.get());
    } else {
        lm.setHead(head);
        lm.setTail(tail);
        s = kvstore->setKV(metaRk,
                           RecordValue(lm.encode(), RecordType::RT_LIST_META, ttl, rv),
                           txn.get());
    }
    if (!s.ok()) {
        return s;
    }
    auto commitStatus = txn->commit();
    if (!commitStatus.ok()) {
        return commitStatus.status();
    }
    return subRv.value().getValue();
}

Expected<std::string> genericPush(Session *sess,
                                  PStore kvstore,
                                  const RecordKey& metaRk,
                                  const std::vector<std::string>& args,
                                  ListPos pos,
                                  bool needExist) {
    auto ptxn = kvstore->createTransaction();
    if (!ptxn.ok()) {
        return ptxn.status();
    }
    std::unique_ptr<Transaction> txn = std::move(ptxn.value());

    ListMetaValue lm(INITSEQ, INITSEQ);
    Expected<RecordValue> rv = kvstore->getKV(metaRk, txn.get());
    uint64_t ttl = 0;

    if (rv.ok()) {
        ttl = rv.value().getTtl();
        Expected<ListMetaValue> exptLm =
            ListMetaValue::decode(rv.value().getValue());
        INVARIANT(exptLm.ok());
        lm = std::move(exptLm.value());
    } else if (rv.status().code() != ErrorCodes::ERR_NOTFOUND) {
        return rv.status();
    } else if (needExist) {
        return Command::fmtZero();
    }

    uint64_t head = lm.getHead();
    uint64_t tail = lm.getTail();
    for (size_t i = 0; i < args.size(); ++i) {
        uint64_t idx;
        if (pos == ListPos::LP_HEAD) {
            idx = --head;
        } else {
            idx = tail++;
        }
        RecordKey subRk(metaRk.getChunkId(),
                        metaRk.getDbId(),
                        RecordType::RT_LIST_ELE,
                        metaRk.getPrimaryKey(),
                        std::to_string(idx));
        RecordValue subRv(args[i], RecordType::RT_LIST_ELE);
        Status s = kvstore->setKV(subRk, subRv, txn.get());
        if (!s.ok()) {
            return s;
        }
    }
    lm.setHead(head);
    lm.setTail(tail);
    Status s = kvstore->setKV(metaRk,
                              RecordValue(lm.encode(), RecordType::RT_LIST_META, ttl, rv),
                              txn.get());
    if (!s.ok()) {
        return s;
    }
    Expected<uint64_t> commitStatus = txn->commit();
    return Command::fmtLongLong(lm.getTail() - lm.getHead());
}

class LLenCommand: public Command {
 public:
    LLenCommand()
        :Command("llen", "rF") {
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

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_LIST_META);

        if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
            return fmtZero();
        } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return fmtZero();
        } else if (!rv.status().ok()) {
            return rv.status();
        }

        Expected<ListMetaValue> exptListMeta =
            ListMetaValue::decode(rv.value().getValue());
        if (!exptListMeta.ok()) {
            return exptListMeta.status();
        }
        uint64_t tail = exptListMeta.value().getTail();
        uint64_t head = exptListMeta.value().getHead();
        return fmtLongLong(tail - head);
    }
} llenCommand;

class ListPopWrapper: public Command {
 public:
    explicit ListPopWrapper(ListPos pos, const char* sflags)
        :Command(pos == ListPos::LP_HEAD ? "lpop" : "rpop", sflags),
         _pos(pos) {
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
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_LIST_META);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
                rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtNull();
        }
        if (!rv.ok()) {
            return rv.status();
        }

        // record exists
        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key, mgl::LockMode::LOCK_X);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_LIST_META, key, "");
        // uint32_t storeId = expdb.value().dbId;
        std::string metaKeyEnc = metaRk.encode();
        PStore kvstore = expdb.value().store;

        // if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
        //     return {ErrorCodes::ERR_BUSY, "key locked"};
        // }

        for (uint32_t i = 0; i < RETRY_CNT; ++i) {
            auto ptxn = kvstore->createTransaction();
            if (!ptxn.ok()) {
                return ptxn.status();
            }
            std::unique_ptr<Transaction> txn = std::move(ptxn.value());
            Expected<std::string> s =
                genericPop(sess, kvstore, metaRk, _pos);
            if (s.ok()) {
                return Command::fmtBulk(s.value());
            }
            if (s.status().code() == ErrorCodes::ERR_NOTFOUND) {
                return Command::fmtNull();
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
    ListPos _pos;
};

class LPopCommand: public ListPopWrapper {
 public:
    LPopCommand()
        :ListPopWrapper(ListPos::LP_HEAD, "wF") {
    }
} LPopCommand;

class RPopCommand: public ListPopWrapper {
 public:
    RPopCommand()
        :ListPopWrapper(ListPos::LP_TAIL, "wF") {
    }
} rpopCommand;

class ListPushWrapper: public Command {
 public:
    explicit ListPushWrapper(const std::string& name, const char* sflags,
                            ListPos pos, bool needExist)
        :Command(name, sflags),
         _pos(pos),
         _needExist(needExist) {
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
            return {ErrorCodes::ERR_PARSEOPT, "exceed batch lim"};
        }

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_LIST_META);
        if (rv.status().code() != ErrorCodes::ERR_OK &&
                rv.status().code() != ErrorCodes::ERR_EXPIRED &&
                rv.status().code() != ErrorCodes::ERR_NOTFOUND) {
            return rv.status();
        }

        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key, mgl::LockMode::LOCK_X);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_LIST_META, key, "");
        // uint32_t storeId = expdb.value().dbId;
        std::string metaKeyEnc = metaRk.encode();
        PStore kvstore = expdb.value().store;
        // if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
        //     return {ErrorCodes::ERR_BUSY, "key locked"};
        // }

        std::vector<std::string> valargs;
        for (size_t i = 2; i < args.size(); ++i) {
            valargs.push_back(args[i]);
        }
        for (uint32_t i = 0; i < RETRY_CNT; ++i) {
            auto ptxn = kvstore->createTransaction();
            if (!ptxn.ok()) {
                return ptxn.status();
            }
            std::unique_ptr<Transaction> txn = std::move(ptxn.value());
            Expected<std::string> s =
                genericPush(sess, kvstore, metaRk, valargs, _pos, _needExist);
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
    ListPos _pos;
    bool _needExist;
};

class LPushCommand: public ListPushWrapper {
 public:
    LPushCommand()
        :ListPushWrapper("lpush", "wmF", ListPos::LP_HEAD, false) {
    }
} lpushCommand;

class RPushCommand: public ListPushWrapper {
 public:
    RPushCommand()
        :ListPushWrapper("rpush", "wmF", ListPos::LP_TAIL, false) {
    }
} rpushCommand;

class LPushXCommand: public ListPushWrapper {
 public:
    LPushXCommand()
        :ListPushWrapper("lpushx", "wmF", ListPos::LP_HEAD, true) {
    }
} lpushxCommand;

class RPushXCommand: public ListPushWrapper {
 public:
    RPushXCommand()
        :ListPushWrapper("rpushx", "wmF", ListPos::LP_TAIL, true) {
    }
} rpushxCommand;

// NOTE(deyukong): atomic is not guaranteed
class RPopLPushCommand: public Command {
 public:
    RPopLPushCommand()
        :Command("rpoplpush", "wm") {
    }

    ssize_t arity() const {
        return 3;
    }

    int32_t firstkey() const {
        return 1;
    }

    int32_t lastkey() const {
        return 2;
    }

    int32_t keystep() const {
        return 1;
    }

    Expected<std::string> run(Session *sess) final {
        const std::vector<std::string>& args = sess->getArgs();
        const std::string& key1 = args[1];
        const std::string& key2 = args[2];

        SessionCtx *pCtx = sess->getCtx();
        auto server = sess->getServerEntry();
        INVARIANT(pCtx != nullptr);

        auto index = getKeysFromCommand(args);
        auto locklist = server->getSegmentMgr()->getAllKeysLocked(sess, args, index,
            mgl::LockMode::LOCK_X);
        if (!locklist.ok()) {
            return locklist.status();
        }

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key1, RecordType::RT_LIST_META);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
            rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtNull();
        } else if (!rv.ok()) {
            return rv.status();
        }
        Expected<RecordValue> rv2 =
            Command::expireKeyIfNeeded(sess, key2, RecordType::RT_LIST_META);
        if (rv2.status().code() != ErrorCodes::ERR_OK &&
            rv2.status().code() != ErrorCodes::ERR_EXPIRED &&
            rv2.status().code() != ErrorCodes::ERR_NOTFOUND) {
            return rv2.status();
        }

        auto expdb1 = server->getSegmentMgr()->getDbHasLocked(sess, key1);
        if (!expdb1.ok()) {
            return expdb1.status();
        }
        RecordKey metaRk1(expdb1.value().chunkId, pCtx->getDbId(), RecordType::RT_LIST_META, key1, "");
        // uint32_t storeId1 = expdb1.value().dbId;
        std::string metaKeyEnc1 = metaRk1.encode();
        PStore kvstore1 = expdb1.value().store;

        auto expdb2 = server->getSegmentMgr()->getDbHasLocked(sess, key2);
        if (!expdb2.ok()) {
            return expdb2.status();
        }
        RecordKey metaRk2(expdb2.value().chunkId, pCtx->getDbId(), RecordType::RT_LIST_META, key2, "");
        // uint32_t storeId2 = expdb2.value().dbId;
        std::string metaKeyEnc2 = metaRk2.encode();
        PStore kvstore2 = expdb2.value().store;

        std::string val = "";
        for (uint32_t i = 0; i < RETRY_CNT; ++i) {
            Expected<std::string> s =
                genericPop(sess, kvstore1, metaRk1, ListPos::LP_TAIL);
            if (s.ok()) {
                val = std::move(s.value());
                break;
            }
            if (s.status().code() == ErrorCodes::ERR_NOTFOUND) {
                return Command::fmtNull();
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
        for (uint32_t i = 0; i < RETRY_CNT; ++i) {
            auto s = genericPush(sess,
                                 kvstore2,
                                 metaRk2,
                                 {val},
                                 ListPos::LP_HEAD,
                                 false /*need_exist*/);
            if (s.ok()) {
                return Command::fmtBulk(val);
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
} rpoplpushCmd;

class LtrimCommand: public Command {
 public:
    LtrimCommand()
        :Command("ltrim", "w") {
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

    Status trimListPessimistic(Session *sess, PStore kvstore,
                            const RecordKey& mk,
                            const ListMetaValue& lm,
                            int64_t start, int64_t end,
                            const Expected<RecordValue>& rv) {
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        uint64_t head = lm.getHead();
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        uint64_t cnt = 0;
        auto functor = [kvstore, sess, &cnt, &txn, &mk] (int64_t start, int64_t end) -> Status {
            SessionCtx *pCtx = sess->getCtx();
            for (int64_t i = start; i < end; ++i) {
                RecordKey subRk(mk.getChunkId(),
                        pCtx->getDbId(),
                        RecordType::RT_LIST_ELE,
                        mk.getPrimaryKey(),
                        std::to_string(i));
                Status s = kvstore->delKV(subRk, txn.get());
                if (!s.ok()) {
                    return s;
                }
                cnt += 1;
                if (cnt % 1000 == 0) {
                    auto v = txn->commit();
                    if (!v.ok()) {
                        return v.status();
                    }
                    auto ptxn = kvstore->createTransaction();
                    if (!ptxn.ok()) {
                        return ptxn.status();
                    }
                    txn = std::move(ptxn.value());
                }
            }
            return {ErrorCodes::ERR_OK, ""};
        };
        auto st = functor(head, start+head);
        if (!st.ok()) {
            return st;
        }
        st = functor(head+end+1, lm.getTail());
        if (!st.ok()) {
            return st;
        }
        if (cnt >= lm.getTail() - lm.getHead()) {
            st = kvstore->delKV(mk, txn.get());
            if (!st.ok()) {
                return st;
            }
        } else {
            ListMetaValue newLm(start+head, end+1+head);
            RecordValue metarcd(newLm.encode(), RecordType::RT_LIST_META, rv.value().getTtl(), rv);
            st = kvstore->setKV(mk, metarcd, txn.get());
            if (!st.ok()) {
                return st;
            }
        }
        auto commitstatus = txn->commit();
        return commitstatus.status();
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

        SessionCtx *pCtx = sess->getCtx();
        auto server = sess->getServerEntry();

        {
            Expected<RecordValue> rv =
                Command::expireKeyIfNeeded(sess, key, RecordType::RT_LIST_META);
            if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
                return Command::fmtOK();
            } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
                return Command::fmtOK();
            } else if (!rv.ok()) {
                return rv.status();
            }
        }

        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key, mgl::LockMode::LOCK_X);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_LIST_META, key, "");
        PStore kvstore = expdb.value().store;
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        Expected<RecordValue> rv = kvstore->getKV(metaRk, txn.get());
        if (!rv.ok()) {
            if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
                return Command::fmtZeroBulkLen();
            }
            return rv.status();
        }

        Expected<ListMetaValue> exptLm =
            ListMetaValue::decode(rv.value().getValue());
        INVARIANT(exptLm.ok());

        const ListMetaValue& lm = exptLm.value();
        uint64_t head = lm.getHead();
        uint64_t tail = lm.getTail();
        int64_t len = tail - head;
        INVARIANT(len > 0);

        int64_t start = estart.value();
        int64_t end = eend.value();
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
            start = len;
            end = len;
        }
        if (end >= len) {
            end = len - 1;
        }
        Status st = trimListPessimistic(sess, kvstore, metaRk, lm, start, end, rv);
        if (!st.ok()) {
            return st;
        }
        return Command::fmtOK();
    }
} ltrimCmd;

class LRangeCommand: public Command {
 public:
    LRangeCommand()
        :Command("lrange", "r") {
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
        Expected<int64_t> estart = ::tendisplus::stoll(args[2]);
        if (!estart.ok()) {
            return estart.status();
        }
        Expected<int64_t> eend = ::tendisplus::stoll(args[3]);
        if (!eend.ok()) {
            return eend.status();
        }

        {
            Expected<RecordValue> rv =
                Command::expireKeyIfNeeded(sess, key, RecordType::RT_LIST_META);
            if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
                return fmtZeroBulkLen();
            } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
                return fmtZeroBulkLen();
            } else if (!rv.ok()) {
                return rv.status();
            }
        }

        SessionCtx *pCtx = sess->getCtx();
        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key, mgl::LockMode::LOCK_S);
        if (!expdb.ok()) {
            return expdb.status();
        }
        PStore kvstore = expdb.value().store;
        // uint32_t storeId = expdb.value().dbId;
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_LIST_META, key, "");
        std::string metaKeyEnc = metaRk.encode();

        // if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
        //     return {ErrorCodes::ERR_BUSY, "key locked"};
        // }

        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        Expected<RecordValue> rv = kvstore->getKV(metaRk, txn.get());
        if (!rv.ok()) {
            if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
                return Command::fmtZeroBulkLen();
            }
            return rv.status();
        }

        Expected<ListMetaValue> exptLm =
            ListMetaValue::decode(rv.value().getValue());
        INVARIANT(exptLm.ok());

        const ListMetaValue& lm = exptLm.value();
        uint64_t head = lm.getHead();
        uint64_t tail = lm.getTail();
        int64_t len = tail - head;
        INVARIANT(len > 0);

        int64_t start = estart.value();
        int64_t end = eend.value();
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
        int64_t rangelen = (end - start) + 1;
        start += head;
        std::stringstream ss;
        Command::fmtMultiBulkLen(ss, rangelen);
        while (rangelen--) {
            RecordKey subRk(expdb.value().chunkId,
                    pCtx->getDbId(),
                    RecordType::RT_LIST_ELE,
                    key,
                    std::to_string(start));
            Expected<RecordValue> eSubVal = kvstore->getKV(subRk, txn.get());
            if (eSubVal.ok()) {
                Command::fmtBulk(ss, eSubVal.value().getValue());
            } else {
                return eSubVal.status();
            }
            start++;
        }
        return ss.str();
    }
} lrangeCmd;

class LIndexCommand: public Command {
 public:
    LIndexCommand()
        :Command("lindex", "r") {
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
        int64_t idx = 0;
        try {
            idx = static_cast<int64_t>(std::stoll(args[2]));
        } catch (std::exception& ex) {
            return {ErrorCodes::ERR_PARSEOPT, ex.what()};
        }

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_LIST_META);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
            return fmtNull();
        } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return fmtNull();
        } else if (!rv.ok()) {
            return rv.status();
        }

        SessionCtx *pCtx = sess->getCtx();
        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key, mgl::LockMode::LOCK_S);
        if (!expdb.ok()) {
            return expdb.status();
        }
        PStore kvstore = expdb.value().store;
        // uint32_t storeId = expdb.value().dbId;
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_LIST_META, key, "");
        std::string metaKeyEnc = metaRk.encode();

        // if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
        //    return {ErrorCodes::ERR_BUSY, "key locked"};
        // }

        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());

        Expected<ListMetaValue> exptLm =
            ListMetaValue::decode(rv.value().getValue());
        INVARIANT(exptLm.ok());

        const ListMetaValue& lm = exptLm.value();
        uint64_t head = lm.getHead();
        uint64_t tail = lm.getTail();
        uint64_t mappingIdx = 0;
        if (idx >= 0) {
            mappingIdx = idx + head;
        } else {
            mappingIdx = idx + tail;
        }
        if (mappingIdx < head || mappingIdx >= tail) {
            return fmtNull();
        }
        RecordKey subRk(expdb.value().chunkId,
                        pCtx->getDbId(),
                        RecordType::RT_LIST_ELE,
                        key,
                        std::to_string(mappingIdx));
        Expected<RecordValue> eSubVal = kvstore->getKV(subRk, txn.get());
        if (eSubVal.ok()) {
            return fmtBulk(eSubVal.value().getValue());
        } else {
            return eSubVal.status();
        }
    }
} lindexCommand;

class LSetCommand: public Command {
 public:
    LSetCommand()
        :Command("lset", "wm") {
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
        // TODO(vinchen) lset
        return fmtOK();
    }
} lsetCmd;

}  // namespace tendisplus
