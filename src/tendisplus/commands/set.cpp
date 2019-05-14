#include <string>
#include <utility>
#include <memory>
#include <algorithm>
#include <cctype>
#include <clocale>
#include <vector>
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

Expected<bool> delGeneric(Session *sess, const std::string& key);

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
    for (size_t i = 0; i < args.size(); ++i) {
        RecordKey subRk(metaRk.getChunkId(),
                        metaRk.getDbId(),
                        RecordType::RT_SET_ELE,
                        metaRk.getPrimaryKey(),
                        args[i]);
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
                           RecordValue(sm.encode(), RecordType::RT_SET_META, ttl, rv),
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
        RecordKey subRk(metaRk.getChunkId(),
                        metaRk.getDbId(),
                        RecordType::RT_SET_ELE,
                        metaRk.getPrimaryKey(),
                        args[i]);
        Expected<RecordValue> rv = kvstore->getKV(subRk, txn.get());
        if (rv.ok()) {
            continue;
        } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            cnt += 1;
        } else {
            return rv.status();
        }
        RecordValue subRv("", RecordType::RT_SET_ELE);
        Status s = kvstore->setKV(subRk, subRv, txn.get());
        if (!s.ok()) {
            return s;
        }
    }
    sm.setCount(sm.getCount()+cnt);
    Status s = kvstore->setKV(metaRk,
                              RecordValue(sm.encode(), RecordType::RT_SET_META, ttl, rv),
                              txn.get());
    if (!s.ok()) {
        return s;
    }
    Expected<uint64_t> commitStatus = txn->commit();
    return Command::fmtLongLong(cnt);
}

class SMembersCommand: public Command {
 public:
    SMembersCommand()
        :Command("smembers") {
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

        {
            Expected<RecordValue> rv =
                Command::expireKeyIfNeeded(sess, key, RecordType::RT_SET_META);
            if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
                return Command::fmtZeroBulkLen();
            } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
                return Command::fmtZeroBulkLen();
            } else if (!rv.ok()) {
                return rv.status();
            }
        }

        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key, mgl::LockMode::LOCK_S);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_SET_META, key, "");
        // uint32_t storeId = expdb.value().dbId;
        std::string metaKeyEnc = metaRk.encode();

        // if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
        //     return {ErrorCodes::ERR_BUSY, "key locked"};
        // }

        PStore kvstore = expdb.value().store;
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());

        Expected<RecordValue> rv = kvstore->getKV(metaRk, txn.get());

        ssize_t ssize = 0, cnt = 0;
        if (rv.ok()) {
            Expected<SetMetaValue> exptSm =
                SetMetaValue::decode(rv.value().getValue());
            INVARIANT(exptSm.ok());
            ssize = exptSm.value().getCount();
        } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtZeroBulkLen();
        } else {
            return rv.status();
        }

        std::stringstream ss;
        Command::fmtMultiBulkLen(ss, ssize);
        auto cursor = txn->createCursor();
        RecordKey fake = {expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_SET_ELE, key, ""};
        cursor->seek(fake.prefixPk());
        while (true) {
            Expected<Record> exptRcd = cursor->next();
            if (exptRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
                break;
            }
            if (!exptRcd.ok()) {
                return exptRcd.status();
            }
            Record& rcd = exptRcd.value();
            const RecordKey& rcdkey = rcd.getRecordKey();
            if (rcdkey.prefixPk() != fake.prefixPk()) {
                break;
            }
            cnt += 1;
            Command::fmtBulk(ss, rcdkey.getSecondaryKey());
        }
        INVARIANT(cnt == ssize);
        return ss.str();
    }
} smemberscmd;

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

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_SET_META);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
            return fmtZero();
        } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return fmtZero();
        } else if (!rv.ok()) {
            return rv.status();
        }

        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key, mgl::LockMode::LOCK_S);
        if (!expdb.ok()) {
            return expdb.status();
        }
        // uint32_t storeId = expdb.value().dbId;
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_SET_META, key, "");
        std::string metaKeyEnc = metaRk.encode();

        // if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
        //     return {ErrorCodes::ERR_BUSY, "key locked"};
        // }

        PStore kvstore = expdb.value().store;
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());

        RecordKey subRk(expdb.value().chunkId,
                        pCtx->getDbId(),
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

// TODO(deyukong): unittest for srandmember
class SrandMemberCommand: public Command {
 public:
    SrandMemberCommand()
        :Command("srandmember") {
    }

    ssize_t arity() const {
        return -2;
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
        bool explictBulk = false;
        int64_t bulk = 1;
        bool negative = false;
        if (sess->getArgs().size() >= 3) {
            Expected<int64_t> ebulk = ::tendisplus::stoll(sess->getArgs()[2]);
            if (!ebulk.ok()) {
                return ebulk.status();
            }
            bulk = ebulk.value();
            if (bulk < 0) {
                negative = true;
                bulk = -bulk;
            }

            // bulk = 0 explictly, return empty list
            if (!bulk) {
                return Command::fmtZeroBulkLen();
            }
            explictBulk = true;
        }
        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);

        {
            Expected<RecordValue> rv =
                Command::expireKeyIfNeeded(sess, key, RecordType::RT_SET_META);
            if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
                rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
                if (bulk == 1 && !explictBulk) {
                    return Command::fmtNull();
                } else {
                    return Command::fmtZeroBulkLen();
                }
            } else if (!rv.status().ok()) {
                return rv.status();
            }
        }

        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key, mgl::LockMode::LOCK_S);
        if (!expdb.ok()) {
            return expdb.status();
        }
        // uint32_t storeId = expdb.value().dbId;
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_SET_META, key, "");
        std::string metaKeyEnc = metaRk.encode();

        // if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
        //     return {ErrorCodes::ERR_BUSY, "key locked"};
        // }

        PStore kvstore = expdb.value().store;
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        Expected<RecordValue> rv = kvstore->getKV(metaRk, txn.get());

        ssize_t ssize = 0;
        if (rv.ok()) {
            Expected<SetMetaValue> exptSm =
                SetMetaValue::decode(rv.value().getValue());
            INVARIANT(exptSm.ok());
            ssize = exptSm.value().getCount();
            INVARIANT(ssize != 0);
        } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            if (bulk == 1 && !explictBulk) {
                return Command::fmtNull();
            } else {
                return Command::fmtZeroBulkLen();
            }
        } else {
            return rv.status();
        }

        auto cursor = txn->createCursor();
        uint32_t beginIdx = 0;
        uint32_t cnt = 0;
        uint32_t peek = 0;
        uint32_t remain = 0;
        std::vector<std::string> vals;
        if (bulk > ssize) {
            beginIdx = 0;
            if (!negative) {
                remain = ssize;
            } else {
                remain = bulk;
            }
        } else {
            remain = bulk;

            std::srand((int32_t)msSinceEpoch());
            uint32_t offset = ssize - remain + 1;
            int r = std::rand();
            // TODO(vinchen): max scan count should be configable
            beginIdx = r % (offset > 1024 * 16 ? 1024 * 16 : offset);
        }

        if (remain > 1024 * 16) {
            // TODO(vinchen):  should be configable
            return{ ErrorCodes::ERR_INTERNAL, "bulk too big" };
        }
        RecordKey fake = {expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_SET_ELE, key, ""};
        cursor->seek(fake.prefixPk());
        while (true) {
            Expected<Record> exptRcd = cursor->next();
            if (exptRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
                break;
            }
            if (!exptRcd.ok()) {
                return exptRcd.status();
            }
            if (cnt++ < beginIdx) {
                continue;
            }
            if (cnt > ssize) {
                break;
            }
            if (peek < remain) {
                vals.emplace_back(exptRcd.value().getRecordKey().getSecondaryKey());
                peek++;
            } else {
                break;
            }
        }
        // TODO(vinchen): vals should be shuffle here
        INVARIANT(vals.size() != 0);
        if (bulk == 1 && !explictBulk) {
            return Command::fmtBulk(vals[0]);
        } else {
            std::stringstream ss;
            INVARIANT(remain == vals.size() || negative);
            Command::fmtMultiBulkLen(ss, remain);
            while (remain) {
                for (auto& v : vals) {
                    if (!remain)
                        break;
                    Command::fmtBulk(ss, v);
                    remain--;
                }
            }
           return ss.str();
        }
    }
} srandmembercmd;

class SpopCommand: public Command {
 public:
    SpopCommand()
        :Command("spop") {
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
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_SET_META);
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
        // uint32_t storeId = expdb.value().dbId;
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_SET_META, key, "");
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
            RecordKey fake = {expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_SET_ELE, key, ""};
            auto batch = Command::scan(fake.prefixPk(), "0", 1, txn.get());
            if (!batch.ok()) {
                return batch.status();
            }
            const auto& rcds = batch.value().second;
            if (rcds.size() == 0) {
                return Command::fmtNull();
            }
            const std::string& v = (*rcds.begin()).getRecordKey().getSecondaryKey();
            Expected<std::string> s =
                genericSRem(sess, kvstore, metaRk, {v});
            if (s.ok()) {
                return Command::fmtBulk(v);
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
} spopcmd;

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

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_SET_META);
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
        // uint32_t storeId = expdb.value().dbId;
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_SET_META, key, "");
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

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_SET_META);

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

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_SET_META);
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
        // uint32_t storeId = expdb.value().dbId;
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_SET_META, key, "");
        std::string metaKeyEnc = metaRk.encode();
        // if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
        //     return {ErrorCodes::ERR_BUSY, "key locked"};
        // }

        PStore kvstore = expdb.value().store;

        std::vector<std::string> valArgs;
        for (uint32_t i = 2; i < args.size(); ++i) {
            valArgs.push_back(args[i]);
        }
        for (uint32_t i = 0; i < RETRY_CNT; ++i) {
            auto ptxn = kvstore->createTransaction();
            if (!ptxn.ok()) {
                return ptxn.status();
            }
            std::unique_ptr<Transaction> txn = std::move(ptxn.value());
            Expected<std::string> s =
                genericSRem(sess, kvstore, metaRk, valArgs);
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

class SdiffgenericCommand: public Command {
 public:
    SdiffgenericCommand(const std::string& name, bool store)
        :Command(name),
         _store(store) {
    }

    Expected<std::string> run(Session *sess) final {
        const std::vector<std::string>& args = sess->getArgs();
        size_t startkey = _store ? 2 : 1;
        std::set<std::string> result;
        auto server = sess->getServerEntry();
        SessionCtx *pCtx = sess->getCtx();

        std::vector<int> index = getKeysFromCommand(args);
        auto lock = server->getSegmentMgr()->getAllKeysLocked(sess, args, index, mgl::LockMode::LOCK_X);
        if (!lock.ok()) {
            return lock.status();
        }

        for (size_t i = startkey; i < args.size(); ++i) {
            Expected<RecordValue> rv =
                Command::expireKeyIfNeeded(sess, args[i], RecordType::RT_SET_META);
            if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
                continue;
            } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
                continue;
            } else if (!rv.ok()) {
                return rv.status();
            }

            auto expdb = server->getSegmentMgr()->getDbHasLocked(sess, args[i]);
            if (!expdb.ok()) {
                return expdb.status();
            }
            RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_SET_META, args[i], "");
            std::string metaKeyEnc = metaRk.encode();
            PStore kvstore = expdb.value().store;

            // if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
            //      return {ErrorCodes::ERR_BUSY, "key locked"};
            // }

            auto ptxn = kvstore->createTransaction();
            if (!ptxn.ok()) {
                return ptxn.status();
            }
            std::unique_ptr<Transaction> txn = std::move(ptxn.value());
            auto cursor = txn->createCursor();
            RecordKey fake = {expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_SET_ELE, args[i], ""};
            cursor->seek(fake.prefixPk());
            while (true) {
                Expected<Record> exptRcd = cursor->next();
                if (exptRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
                    break;
                }
                if (!exptRcd.ok()) {
                    return exptRcd.status();
                }
                Record& rcd = exptRcd.value();
                const RecordKey& rcdkey = rcd.getRecordKey();
                if (rcdkey.prefixPk() != fake.prefixPk()) {
                    break;
                }
                if (i == startkey) {
                    result.insert(rcdkey.getSecondaryKey());
                } else {
                    result.erase(rcdkey.getSecondaryKey());
                }
            }
        }

        if (!_store) {
            std::stringstream ss;
            Command::fmtMultiBulkLen(ss, result.size());
            for (auto& v : result) {
                Command::fmtBulk(ss, v);
            }
            return ss.str();
        }

        const std::string& storeKey = args[1];
        Expected<bool> deleted = delGeneric(sess, storeKey);
        // Expected<bool> deleted = Command::delKeyChkExpire(sess, storeKey, RecordType::RT_SET_META);
        if (!deleted.ok()) {
            return deleted.status();
        }

        std::vector<std::string> newKeys(2);
        for (auto& v : result) {
            newKeys.push_back(std::move(v));
        }

        auto expdb = server->getSegmentMgr()->getDbHasLocked(sess, storeKey);
        if (!expdb.ok()) {
            return expdb.status();
        }
        PStore kvstore = expdb.value().store;
        RecordKey storeRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_SET_META, storeKey, "");
        for (uint32_t i=0; i < RETRY_CNT; ++i) {
            auto ptxn = kvstore->createTransaction();
            if (!ptxn.ok()) {
                return ptxn.status();
            }
            std::unique_ptr<Transaction> txn = std::move(ptxn.value());
            Expected<std::string> addStore = genericSAdd(sess, kvstore, storeRk, newKeys);
            if (addStore.ok()) {
                return addStore.value();
            }
            if (addStore.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
                return addStore.status();
            }
            if (i == RETRY_CNT - 1) {
                return addStore.status();
            } else {
                continue;
            }
        }
        return {ErrorCodes::ERR_INTERNAL, "currently unrechable"};
    }

 private:
    bool _store;
};

class SdiffCommand: public SdiffgenericCommand {
 public:
    SdiffCommand()
        :SdiffgenericCommand("sdiff", false) {
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
} sdiffcmd;

class SdiffStoreCommand: public SdiffgenericCommand {
public:
    SdiffStoreCommand()
        :SdiffgenericCommand("sdiffstore", true) {
    }

    ssize_t arity() const {
        return -3;
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
} sdiffstoreCommand;

// Implement O(n*m) intersection
// which n is the cardinality of the smallest set
// m is the num of sets input
class SintergenericCommand: public Command {
public:
    SintergenericCommand(const std::string& name, bool store)
            :Command(name),
             _store(store) {
    }

    Expected<std::string> run(Session *sess) final {
        const std::vector<std::string>& args = sess->getArgs();
        size_t startkey = _store ? 2 : 1;
        std::set<std::string> result;
        auto server = sess->getServerEntry();
        SessionCtx *pCtx = sess->getCtx();

        std::vector<int> index = getKeysFromCommand(args);
        auto lock = server->getSegmentMgr()->getAllKeysLocked(sess, args, index, mgl::LockMode::LOCK_X);
        if (!lock.ok()) {
            return lock.status();
        }

        // stored all sets sorted by their length
        std::vector<std::pair<size_t, uint64_t>> setList;
        for (size_t i = startkey; i < args.size(); i++) {
            Expected<RecordValue> rv =
                    Command::expireKeyIfNeeded(sess, args[i], RecordType::RT_SET_META);

            // if one set is empty, their intersection is empty set, so just return it.
            if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
                rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
                if (_store) {
                    continue;
                }
                return Command::fmtNull();
            } else if (!rv.ok()) {
                return rv.status();
            }

            Expected<SetMetaValue> expSetMeta =
                    SetMetaValue::decode(rv.value().getValue());

            uint64_t setLength = expSetMeta.value().getCount();
            if (setLength == 0) {
                if (_store) {
                    continue;
                }
                return Command::fmtNull();
            }
            setList.push_back(std::make_pair(i, setLength));
        }
        std::sort(setList.begin(), setList.end(), [](auto& left, auto& right) {
            return left.second < right.second;
        });

        for (size_t i = 0; i < setList.size(); i++) {
            const std::string& key = args[setList[i].first];
            auto expdb = server->getSegmentMgr()->getDbHasLocked(sess, key);
            if (!expdb.ok()) {
                return expdb.status();
            }
            PStore kvstore = expdb.value().store;
            auto ptxn = kvstore->createTransaction();
            if (!ptxn.ok()) {
                return ptxn.status();
            }
            std::unique_ptr<Transaction> txn = std::move(ptxn.value());
            if (i == 0) {
                auto cursor = txn->createCursor();
                RecordKey fakeRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_SET_ELE, key, "");
                cursor->seek(fakeRk.prefixPk());
                while (true) {
                    Expected<Record> expRcd = cursor->next();
                    if (expRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
                        break;
                    }
                    if (!expRcd.ok()) {
                        return expRcd.status();
                    }
                    Record& rcd = expRcd.value();
                    const RecordKey& rcdKey = rcd.getRecordKey();
                    if (rcdKey.prefixPk() != fakeRk.prefixPk()) {
                        break;
                    }
                    result.insert(rcdKey.getSecondaryKey());
                }
                // for the smallest set
                // input all its keys into set, then goto next loop;
                continue;
            }
            if (result.size() == 0) {
                if (_store) {
                    // we must del the storeKey before we return
                    break;
                }
                // has no intersection, just return empty set
                return Command::fmtNull();
            }

            for (auto iter = result.begin(); iter != result.end();) {
                RecordKey subRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_SET_ELE, key, *iter);
                Expected<RecordValue> subValue = kvstore->getKV(subRk, txn.get());
                // if key not found, erase it
                if (!subValue.ok() || subValue.status().code() == ErrorCodes::ERR_NOTFOUND) {
                    // then the old iterator will be invalid
                    // new value is the iterator to the next element
                    iter = result.erase(iter);
                } else {
                    // otherwise move forward manually
                    iter++;
                }
            }
        }

        if (!_store) {
            std::stringstream ss;
            Command::fmtMultiBulkLen(ss, result.size());
            for (auto& v : result) {
                Command::fmtBulk(ss, v);
            }
            return ss.str();
        }

        const std::string& storeKey = args[1];
        Expected<bool> deleted = delGeneric(sess, storeKey);
        // Expected<bool> deleted = Command::delKeyChkExpire(sess, storeKey, RecordType::RT_SET_META);
        if (!deleted.ok()) {
            return deleted.status();
        }

        std::vector<std::string> newKeys(2);
        for (auto& v : result) {
            newKeys.push_back(std::move(v));
        }
        if (newKeys.size() == 2) {
            return Command::fmtZero();
        }

        auto expdb = server->getSegmentMgr()->getDbHasLocked(sess, storeKey);
        if (!expdb.ok()) {
            return expdb.status();
        }
        PStore kvstore = expdb.value().store;
        RecordKey storeRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_SET_META, storeKey, "");
        for (uint32_t i=0; i < RETRY_CNT; ++i) {
            auto ptxn = kvstore->createTransaction();
            if (!ptxn.ok()) {
                return ptxn.status();
            }
            std::unique_ptr<Transaction> txn = std::move(ptxn.value());
            Expected<std::string> addStore = genericSAdd(sess, kvstore, storeRk, newKeys);
            if (addStore.ok()) {
                return addStore.value();
            }
            if (addStore.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
                return addStore.status();
            }
            if (i == RETRY_CNT - 1) {
                return addStore.status();
            } else {
                continue;
            }
        }
        return {ErrorCodes::ERR_INTERNAL, "currently unrechable"};
    }
private:
    bool _store;
};

class SinterCommand: public SintergenericCommand {
public:
    SinterCommand()
        :SintergenericCommand("sinter", false) {
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
} sinterCommand;

class SinterStoreCommand: public SintergenericCommand {
public:
    SinterStoreCommand()
        :SintergenericCommand("sinterstore", true) {
    }

    ssize_t arity() const {
        return -3;
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
} sinterstoreCommand;

class SmoveCommand: public Command {
public:
    SmoveCommand()
        :Command("smove") {
    }

    ssize_t arity() const {
        return 4;
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
        const std::string& source = args[1];
        const std::string& dest = args[2];
        const std::string& member = args[3];

        SessionCtx *pCtx = sess->getCtx();
        auto server = sess->getServerEntry();

        std::vector<int> index = getKeysFromCommand(args);
        auto lock = server->getSegmentMgr()->getAllKeysLocked(sess, args, index, mgl::LockMode::LOCK_X);

        Expected<RecordValue> rv = Command::expireKeyIfNeeded(sess, source, RecordType::RT_SET_META);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
            rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return Command::fmtZero();
        }

        auto srcDb = server->getSegmentMgr()->getDbHasLocked(sess, args[1]);
        if (!srcDb.ok()) {
            return srcDb.status();
        }
        auto destDb = server->getSegmentMgr()->getDbHasLocked(sess, args[2]);
        if (!destDb.ok()) {
            return destDb.status();
        }

        PStore srcStore = srcDb.value().store;
        PStore destStore = destDb.value().store;

        RecordKey remRk(srcDb.value().chunkId, pCtx->getDbId(), RecordType::RT_SET_META, source, "");
        RecordKey addRk(destDb.value().chunkId, pCtx->getDbId(), RecordType::RT_SET_META, dest, "");
        // directly remove member from source
        const std::string& formatRet = Command::fmtLongLong(1);
        for (uint32_t i = 0; i < RETRY_CNT; ++i) {
            auto ptxn = srcStore->createTransaction();
            if (!ptxn.ok()) {
                return ptxn.status();
            }
            std::unique_ptr<Transaction> txn = std::move(ptxn.value());
            Expected<std::string> remRet = genericSRem(sess, srcStore, remRk, {member});
            if (remRet.ok()) {
                if (remRet.value() == formatRet) {
                    break;
                } else {
                    return Command::fmtZero();
                }
            }
            if (remRet.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
                return remRet.status();
            }
            if (i == RETRY_CNT - 1) {
                return remRet.status();
            } else {
                continue;
            }
        }

        // add member to dest
        for (uint32_t i = 0; i < RETRY_CNT; ++i) {
            auto ptxn = destStore->createTransaction();
            if (!ptxn.ok()) {
                return ptxn.status();
            }
            std::unique_ptr<Transaction> txn = std::move(ptxn.value());
            Expected<std::string> addRet = genericSAdd(sess, destStore, addRk, {"","",member});
            if (addRet.ok()) {
                return addRet.value();
            }
            if (addRet.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
                return addRet.status();
            }
            if (i == RETRY_CNT - 1) {
                return addRet.status();
            } else {
                continue;
            }
        }

        return {ErrorCodes::ERR_INTERNAL, "not reachable"};
    }
} smoveCommand;

class SuniongenericCommand: public Command {
public:
    SuniongenericCommand(const std::string& name, bool store)
        :Command(name),
        _store(store) {
        }
    
    Expected<std::string> run(Session *sess) final {
        const std::vector<std::string>& args = sess->getArgs();
        size_t startkey = _store ? 2 : 1;
        std::unordered_set<std::string> result;
        auto server = sess->getServerEntry();
        SessionCtx *pCtx = sess->getCtx();

        std::vector<int> index = getKeysFromCommand(args);
        auto lock = server->getSegmentMgr()->getAllKeysLocked(sess, args, index, mgl::LockMode::LOCK_X);
        if (!lock.ok()) {
            return lock.status();
        }

        for (size_t i = startkey; i < args.size(); ++i) {
            Expected<RecordValue> rv =
                    Command::expireKeyIfNeeded(sess, args[i], RecordType::RT_SET_META);
            
            if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
                rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
                continue;
            } else if (!rv.ok()) {
                return rv.status();
            }

            auto expdb = server->getSegmentMgr()->getDbHasLocked(sess, args[i]);
            if (!expdb.ok()) {
                return expdb.status();
            }
            PStore kvstore = expdb.value().store;
            auto ptxn = kvstore->createTransaction();
            if (!ptxn.ok()) {
                return ptxn.status();
            }
            std::unique_ptr<Transaction> txn = std::move(ptxn.value());
            
            auto cursor = txn->createCursor();
            RecordKey fakeRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_SET_ELE, args[i], "");
            cursor->seek(fakeRk.prefixPk());
            while (true) {
                Expected<Record> exptRcd = cursor->next();
                if (!exptRcd.ok()) {
                    return exptRcd.status();
                }
                if (exptRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
                    break;
                }
                Record& rcd = exptRcd.value();
                const RecordKey& rcdkey = rcd.getRecordKey();
                if (rcdkey.prefixPk() != fakeRk.prefixPk()) {
                    break;
                }
                result.insert(rcdkey.getSecondaryKey());
            }
        }

        if (!_store) {
            std::stringstream ss;
            Command::fmtMultiBulkLen(ss, result.size());
            for (const auto& v: result) {
                Command::fmtBulk(ss, v);
            }
            return ss.str();
        }

        const std::string& storeKey = args[1];
        Expected<bool> deleted = delGeneric(sess, storeKey);
        if (!deleted.ok()) {
            return deleted.status();
        }

        std::vector<std::string> newKeys(2);
        for (const auto& v : result) {
            newKeys.push_back(std::move(v));
        }
        if (newKeys.size() == 2) {
            return Command::fmtZero();
        }

        auto expdb = server->getSegmentMgr()->getDbHasLocked(sess, storeKey);;
        if (!expdb.ok()) {
            return expdb.status();
        }
        PStore kvstore = expdb.value().store;
        RecordKey storeRk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_SET_META, storeKey, "");
        for (uint32_t i = 0; i < RETRY_CNT; ++i) {
            auto ptxn = kvstore->createTransaction();
            if (!ptxn.ok()) {
                return ptxn.status();
            }
            std::unique_ptr<Transaction> txn = std::move(ptxn.value());
            Expected<std::string> addStore = genericSAdd(sess, kvstore, storeRk, newKeys);
            if (addStore.ok()) {
                return addStore.value();
            }
            if (addStore.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
                return addStore.status();
            }
            if (i == RETRY_CNT - 1) {
                return addStore.status();
            } else {
                continue;
            }
        }
        return {ErrorCodes::ERR_INTERNAL, "not reachable"};
    }

private:
    bool _store;
};

class SunionCommand: public SuniongenericCommand {
public:
    SunionCommand()
        :SuniongenericCommand("sunion", false) {
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
} sunionCommand;

class SunionStoreCommand: public SuniongenericCommand {
public:
    SunionStoreCommand()
        :SuniongenericCommand("sunionstore", true) {
    }

    ssize_t arity() const {
        return -3;
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
} sunionstoreCommand;

}  // namespace tendisplus
