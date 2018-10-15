#include <string>
#include <utility>
#include <memory>
#include <algorithm>
#include <cctype>
#include <clocale>
#include <map>
#include "glog/logging.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/time.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

Expected<bool> expireBeforeNow(Session *sess,
                        RecordType type,
                        const std::string& key) {
    SessionCtx *pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    RecordKey rk(pCtx->getDbId(), type, key, "");
    uint32_t storeId = Command::getStoreId(sess, key);
    return Command::delKeyChkExpire(sess, storeId, rk);
}

// return true if exists
// return false if not exists
// return error if has error
Expected<bool> expireAfterNow(Session *sess,
                        RecordType type,
                        const std::string& key,
                        uint64_t expireAt) {
    SessionCtx *pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    RecordKey rk(pCtx->getDbId(), type, key, "");
    uint32_t storeId = Command::getStoreId(sess, key);
    Expected<RecordValue> rv =
        Command::expireKeyIfNeeded(sess, storeId, rk);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
        return false;
    } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
        return false;
    } else if (!rv.status().ok()) {
        return rv.status();
    }

    // record exists and not expired
    StoreLock storeLock(storeId, mgl::LockMode::LOCK_IX, sess);

    if (Command::isKeyLocked(sess, storeId, rk.encode())) {
        return {ErrorCodes::ERR_BUSY, "key locked"};
    }
    PStore kvstore = Command::getStoreById(sess, storeId);
    for (uint32_t i = 0; i < Command::RETRY_CNT; ++i) {
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        Expected<RecordValue> eValue = kvstore->getKV(rk, txn.get());
        if (eValue.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return false;
        } else if (!eValue.ok()) {
            return eValue.status();
        }
        auto rv = eValue.value();
        rv.setTtl(expireAt);
        Status s = kvstore->setKV(rk, rv, txn.get());
        if (!s.ok()) {
            return s;
        }

        auto commitStatus = txn->commit();
        s = commitStatus.status();
        if (s.ok()) {
            return true;
        } else if (s.code() != ErrorCodes::ERR_COMMIT_RETRY) {
            return s;
        }
        // status == ERR_COMMIT_RETRY
        if (i == Command::RETRY_CNT - 1) {
            return s;
        } else {
            continue;
        }
    }

    INVARIANT(0);
    return {ErrorCodes::ERR_INTERNAL, "not reachable"};
}

Expected<std::string> expireGeneric(Session *sess,
                                    uint64_t expireAt,
                                    const std::string& key) {
    if (expireAt >= nsSinceEpoch()/1000000) {
        bool atLeastOne = false;
        for (auto type : {RecordType::RT_KV,
                          RecordType::RT_LIST_META,
                          RecordType::RT_HASH_META,
                          RecordType::RT_SET_META}) {
            auto done = expireAfterNow(sess, type, key, expireAt);
            if (!done.ok()) {
                return done.status();
            }
            atLeastOne |= done.value();
        }
        return atLeastOne ? Command::fmtOne() : Command::fmtZero();
    } else {
        bool atLeastOne = false;
        for (auto type : {RecordType::RT_KV,
                          RecordType::RT_LIST_META,
                          RecordType::RT_HASH_META,
                          RecordType::RT_SET_META}) {
            auto done = expireBeforeNow(sess, type, key);
            if (!done.ok()) {
                return done.status();
            }
            atLeastOne |= done.value();
        }
        return atLeastOne ? Command::fmtOne() : Command::fmtZero();
    }
    INVARIANT(0);
    return {ErrorCodes::ERR_INTERNAL, "not reachable"};
}

class ExpireCommand: public Command {
 public:
    ExpireCommand()
        :Command("expire") {
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
        auto expSecs = ::tendisplus::stoll(sess->getArgs()[2]);
        if (!expSecs.ok()) {
            return expSecs.status();
        }
        uint64_t millsecs = expSecs.value()*1000;
        uint64_t now = nsSinceEpoch()/1000000;

        return expireGeneric(sess, now+millsecs, key);
    }
} expireCmd;

class ExistsCommand: public Command {
 public:
    ExistsCommand()
        :Command("exists") {
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
        uint32_t storeId = Command::getStoreId(sess, key);

        for (auto type : {RecordType::RT_KV,
                          RecordType::RT_LIST_META,
                          RecordType::RT_HASH_META,
                          RecordType::RT_SET_META}) {
            RecordKey rk(pCtx->getDbId(), type, key, "");
            Expected<RecordValue> rv =
                Command::expireKeyIfNeeded(sess, storeId, rk);
            if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
                continue;
            } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
                continue;
            } else if (!rv.ok()) {
                return rv.status();
            }
            return Command::fmtOne();
        }
        return Command::fmtZero();
    }
} existsCmd;

class TypeCommand: public Command {
 public:
    TypeCommand()
        :Command("type") {
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
        uint32_t storeId = Command::getStoreId(sess, key);

        const std::map<RecordType, std::string> lookup = {
            {RecordType::RT_KV, "string"},
            {RecordType::RT_LIST_META, "list"},
            {RecordType::RT_HASH_META, "hash"},
            {RecordType::RT_SET_META, "set"},
        };
        for (const auto& typestr : lookup) {
            RecordKey rk(pCtx->getDbId(), typestr.first, key, "");
            Expected<RecordValue> rv =
                Command::expireKeyIfNeeded(sess, storeId, rk);
            if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
                continue;
            } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
                continue;
            } else if (!rv.ok()) {
                return rv.status();
            }
            return Command::fmtBulk(typestr.second);
        }
        return Command::fmtBulk("none");
    }
} typeCmd;
}  // namespace tendisplus
