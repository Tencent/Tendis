#include <string>
#include <utility>
#include <memory>
#include "glog/logging.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/utils/invariant.h"

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
        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, storeId, rk);
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
            return fmtNull();
        } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            return fmtNull();
        } else if (!rv.status().ok()) {
            return rv.status();
        } else {
            return fmtBulk(rv.value().getValue());
        }
    }
} getCommand;

}  // namespace tendisplus
