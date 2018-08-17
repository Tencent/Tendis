#include <string>
#include <map>
#include "glog/logging.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

Command::CmdMap Command::_commands = {};

Command::Command(const std::string& name)
        :_name(name) {
    _commands[name] = this;
}

Status Command::precheck(NetSession *sess) {
    const auto& args = sess->getArgs();
    if (args.size() == 0) {
        LOG(FATAL) << "BUG: sess " << sess->getRemoteRepr() << " len 0 args";
    }
    auto it = _commands.find(args[0]);
    if (it == _commands.end()) {
        std::stringstream ss;
        ss << "unknown command '" << args[0] << "'";
        return {ErrorCodes::ERR_PARSEPKT, ss.str()};
    }
    ssize_t arity = it->second->arity();
    if ((arity > 0 && arity != ssize_t(args.size()))
            || ssize_t(args.size()) < -arity) {
        std::stringstream ss;
        ss << "wrong number of arguments for '" << args[0] << "' command";
        return {ErrorCodes::ERR_PARSEPKT, ss.str()};
    }
    return {ErrorCodes::ERR_OK, ""};
}

Expected<std::string> Command::runSessionCmd(NetSession *sess) {
    const auto& args = sess->getArgs();
    auto it = _commands.find(args[0]);
    if (it == _commands.end()) {
        LOG(FATAL) << "BUG: command:" << args[0] << " not found!";
    }
    return it->second->run(sess);
}

}  // namespace tendisplus
