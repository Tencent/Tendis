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
