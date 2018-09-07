#include <string>
#include <utility>
#include <memory>
#include "glog/logging.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/storage/expirable.h"

namespace tendisplus {

struct GetParams {
    std::string key;
};

class GetCommand: public Command {
 public:
    GetCommand()
        :Command("get") {
    }

    Expected<GetParams> parse(NetSession *sess) const {
        const auto& args = sess->getArgs();
        if (args.size() != 2) {
            return {ErrorCodes::ERR_PARSEPKT, "invalid get params"};
        }
        return GetParams{args[1]};
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
        Expected<GetParams> params = parse(sess);
        if (!params.ok()) {
            return params.status();
        }

        PStore kvstore = getStore(sess, params.value().key);
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);

        RecordKey rk(pCtx->getDbId(), RecordType::RT_KV,
                params.value().key, "");

        auto dbWrap = std::make_unique<ExpirableDBWrapper>(kvstore);
        Expected<RecordValue> eValue = dbWrap->getKV(rk, txn.get());
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
