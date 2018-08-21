#include <string>
#include "glog/logging.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

struct GetParams {
 public:
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
        return std::string("abc");
        /*
        Expected<GetParams> s = parse(sess);
        if (!s.ok()) {
            return {s.status().code(), s.status().toString()};
        }

        PStore kvstore = getStore(sess, s.value().key);
        auto ptxn = kvStore->createTransaction();
        if (!ptxn.ok()) {
            return {s.status().code(), s.status().toString()};
        }
        std::unique_ptr<Transaction> txn = ptxn.value();
        Expected<std::string> eValue = kvstore->getKV(s.value().key, txn);
        if (!eValue.ok()) {
            const Status& status = eValue.status();
            if (status.code() == ErrorCodes::ERR_NOTFOUND) {
                return fmtNull();
            }
            return {s.status().code(), s.status().toString()};
        }
        return fmtBulk(eValue.value());
        */
    }
} getCommand;

}  // namespace tendisplus
