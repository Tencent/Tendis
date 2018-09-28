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

constexpr uint64_t MAXSEQ = 9223372036854775807ULL;
constexpr uint64_t INITSEQ = MAXSEQ/2ULL;
constexpr uint64_t MINSEQ = 1024;

Expected<std::string> genericPush(NetSession *sess,
                                  PStore kvstore,
                                  const RecordKey& metaRk,
                                  const std::vector<std::string>& args) {
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
    }

    uint64_t tail = lm.getTail();
    for (size_t i = 2; i < args.size(); ++i) {
        RecordKey subRk(metaRk.getDbId(),
                        RecordType::RT_LIST_ELE,
                        metaRk.getPrimaryKey(),
                        std::to_string(tail++));
        RecordValue subRv(args[i]);
        Status s = kvstore->setKV(subRk, subRv, txn.get());
        if (!s.ok()) {
            return s;
        }
    }
    lm.setTail(tail);
    Status s = kvstore->setKV(metaRk,
                              RecordValue(lm.encode(), ttl),
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
        :Command("llen") {
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
        const std::string& key = sess->getArgs()[1];

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);

        RecordKey metaRk(pCtx->getDbId(), RecordType::RT_LIST_META, key, "");
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

class LPushCommand: public Command {
 public:
    LPushCommand()
        :Command("lpush") {
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

    Expected<std::string> run(NetSession *sess) final {
        const std::vector<std::string>& args = sess->getArgs();
        const std::string& key = args[1];

        if (args.size() >= 30000) {
            return {ErrorCodes::ERR_PARSEOPT, "exceed batch lim"};
        }

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);

        RecordKey metaRk(pCtx->getDbId(), RecordType::RT_LIST_META, key, "");
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
                genericPush(sess, kvstore, metaRk, args);
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
} lpushCommand;

class LIndexCommand: public Command {
 public:
    LIndexCommand()
        :Command("lindex") {
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
        int64_t idx = 0;
        try {
            idx = static_cast<int64_t>(std::stoll(args[2]));
        } catch (std::exception& ex) {
            return {ErrorCodes::ERR_PARSEOPT, ex.what()};
        }

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);

        RecordKey metaRk(pCtx->getDbId(), RecordType::RT_LIST_META, key, "");
        std::string metaKeyEnc = metaRk.encode();
        uint32_t storeId = Command::getStoreId(sess, key);

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, storeId, metaRk);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
            return fmtNull();
        } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return fmtNull();
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
        RecordKey subRk(pCtx->getDbId(),
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

}  // namespace tendisplus
