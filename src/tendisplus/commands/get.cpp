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
        auto s = parse(sess);
        if (!s.ok()) {
            return {ErrorCodes::ERR_PARSEPKT, s.status().toString()};
        }
        return std::string("dummyValue");
    }
} getCommand;

}  // namespace tendisplus
