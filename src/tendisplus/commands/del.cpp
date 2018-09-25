#include <string>
#include <utility>
#include <memory>
#include <algorithm>
#include <cctype>
#include <clocale>
#include "glog/logging.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/time.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

// return false if not exists
// return true if exists and del ok
// return error on error
Expected<bool> delGeneric(NetSession *sess, const std::string& key) {
    SessionCtx *pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    bool atLeastOne = false;
    for (auto type : {RecordType::RT_KV,
                      RecordType::RT_LIST_META,
                      RecordType::RT_HASH_META}) {
        RecordKey rk(pCtx->getDbId(), type, key, "");
        uint32_t storeId = Command::getStoreId(sess, key);
        Expected<bool> done = Command::delKeyChkExpire(sess, storeId, rk);
        if (!done.ok()) {
            return done.status();
        }
        atLeastOne |= done.value();
    }
    return atLeastOne;
}

class DelCommand: public Command {
 public:
    DelCommand()
        :Command("del") {
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

    Expected<std::string> run(NetSession *sess) final {
        const auto& args = sess->getArgs();
        uint64_t total = 0;
        for (size_t i = 1; i < args.size(); ++i) {
            Expected<bool> done = delGeneric(sess, args[i]);
            if (!done.ok()) {
                return done.status();
            }
            total += done.value() ? 1 : 0;
        }
        return Command::fmtLongLong(total);
    }
} delCommand;

}  // namespace tendisplus
