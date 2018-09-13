#include <string>
#include <utility>
#include <memory>
#include "glog/logging.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/storage/expirable.h"

namespace tendisplus {

class GetCommand: public Command {
 public:
    GetCommand()
        :Command("get") {
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
        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);
        const std::string& key = sess->getArgs()[1];
        RecordKey rk(pCtx->getDbId(), RecordType::RT_KV, key, "");
        uint32_t storeId = Command::getStoreId(sess, key);
        Status s = Command::expireKeyIfNeeded(sess, storeId, rk);
        if (s.code() == ErrorCodes::ERR_EXPIRED) {
            return fmtNull();
        } else if (!s.ok()) {
            return s;
        }

        auto storeLock = Command::lockDBByKey(sess,
                                              key,
                                              mgl::LockMode::LOCK_IS);
        // NOTE(deyukong): currently we donot check if key is locked
        // because we know, a simple kv will never be locked
        // if this guarantee is broken in the future, remember to
        // rewrite the code below
        // if (Command::isKeyLocked(storeId, key.encode())) {
        //    return {ErrorCodes::ERR_BUSY, "key locked"};
        // }

        PStore kvstore = Command::getStoreById(sess, storeId);
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());

        Expected<RecordValue> eValue = kvstore->getKV(rk, txn.get());
        if (!eValue.ok()) {
            const Status& status = eValue.status();
            if (status.code() == ErrorCodes::ERR_NOTFOUND) {
                return fmtNull();
            }
            return status;
        }
        return fmtBulk(eValue.value().getValue());
    }
} getCommand;

}  // namespace tendisplus
