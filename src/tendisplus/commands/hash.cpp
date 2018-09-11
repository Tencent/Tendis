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
    auto dbWrap = std::make_unique<ExpirableDBWrapper>(kvstore);
    Expected<RecordValue> eValue = dbWrap->getKV(metaRk, txn.get());
    if (!eValue.ok() && eValue.status().code() != ErrorCodes::ERR_NOTFOUND) {
        return eValue.status();
    }

    uint64_t subkeyCount = 0;
    uint64_t ttl = 0;
    if (eValue.ok()) {
        auto v = ::tendisplus::stoul(eValue.value().getValue());
        ttl = eValue.value().getTtl();
        if (!v.ok()) {
            return v.status();
        } else {
            subkeyCount = v.value();
        }
    }  // no else, else not found , so subkeyCount = 0, ttl = 0
    subkeyCount += 1;
    RecordValue metaValue(std::to_string(subkeyCount), ttl);

    auto getSubkeyExpt = kvstore->getKV(subRk, txn.get());
    bool updated = getSubkeyExpt.ok();

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
        RecordKey subRk(pCtx->getDbId(), RecordType::RT_HASH_ELE, key, subkey);

        auto storeLock = Command::lockDBByKey(sess,
                                              key,
                                              mgl::LockMode::LOCK_IS);
        PStore kvstore = Command::getStore(sess, key);
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        auto dbWrap = std::make_unique<ExpirableDBWrapper>(kvstore);
        Expected<RecordValue> eMetaVal = dbWrap->getKV(metaRk, txn.get());
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
        auto storeLock = Command::lockDBByKey(sess,
                                              key,
                                              mgl::LockMode::LOCK_IX);
        PStore kvstore = Command::getStore(sess, key);
        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);
        RecordKey metaKey(pCtx->getDbId(), RecordType::RT_HASH_META, key, "");
        RecordKey subKey(pCtx->getDbId(), RecordType::RT_HASH_ELE, key, subkey);
        RecordValue subRv(val);
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
