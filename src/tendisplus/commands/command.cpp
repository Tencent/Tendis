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

const std::string& Command::getName() const {
    return _name;
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

    auto server = sess->getServerEntry();
    if (!server) {
        LOG(FATAL) << "BUG: get server from sess:"
                    << sess->getConnId()
                    << ",Ip:"
                    << sess->getRemoteRepr() << " empty";
    }

    if (server->requirepass() != "" && it->second->getName() != "auth") {
        return {ErrorCodes::ERR_AUTH, "-NOAUTH Authentication required.\r\n"};
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

PStore Command::getStore(NetSession *sess, const std::string& key) {
    auto server = sess->getServerEntry();
    if (!server) {
        LOG(FATAL) << "BUG: get server from sess:"
                    << sess->getConnId()
                    << ",Ip:"
                    << sess->getRemoteRepr() << " empty";
    }
    auto segMgr = server->getSegmentMgr();
    if (!segMgr) {
        LOG(FATAL) << "BUG: get segMgr from sess:"
                    << sess->getConnId()
                    << ",Ip:"
                    << sess->getRemoteRepr() << " empty";
    }
    auto kvStore = segMgr->calcInstance(key);
    if (!kvStore) {
        LOG(FATAL) << "BUG: get kvstore from sess:"
                    << sess->getConnId()
                    << ",Ip:"
                    << sess->getRemoteRepr() << " empty";
    }
    return kvStore;
}

std::string Command::fmtErr(const std::string& s) {
    if (s.size() != 0 && s[0] == '-') {
        return s;
    }
    std::stringstream ss;
    ss << "-ERR " << s << "\r\n";
    return ss.str();
}

std::string Command::fmtNull() {
    return "$-1\r\n";
}

std::string Command::fmtOK() {
    return "+OK\r\n";
}

std::string Command::fmtBulk(const std::string& s) {
    std::stringstream ss;
    ss << "$" << s.size() << "\r\n" << s << "\r\n";
    return ss.str();
}

}  // namespace tendisplus
