#include <string>
#include <memory>
#include <map>
#include "glog/logging.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/lock/lock.h"

namespace tendisplus {

std::map<std::string, Command*>& commandMap() {
    static std::map<std::string, Command*> map = {};
    return map;
}

Command::Command(const std::string& name)
        :_name(name) {
    commandMap()[name] = this;
}

const std::string& Command::getName() const {
    return _name;
}

Expected<std::string> Command::precheck(NetSession *sess) {
    const auto& args = sess->getArgs();
    if (args.size() == 0) {
        LOG(FATAL) << "BUG: sess " << sess->getRemoteRepr() << " len 0 args";
    }
    std::string commandName = toLower(args[0]);
    auto it = commandMap().find(commandName);
    if (it == commandMap().end()) {
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

    if (*server->requirepass() != "" && it->second->getName() != "auth") {
        return {ErrorCodes::ERR_AUTH, "-NOAUTH Authentication required.\r\n"};
    }

    return it->second->getName();
}

// NOTE(deyukong): call precheck before call runSessionCmd
// this function does no necessary checks
Expected<std::string> Command::runSessionCmd(NetSession *sess) {
    const auto& args = sess->getArgs();
    std::string commandName = toLower(args[0]);
    auto it = commandMap().find(commandName);
    if (it == commandMap().end()) {
        LOG(FATAL) << "BUG: command:" << args[0] << " not found!";
    }
    return it->second->run(sess);
}

std::unique_ptr<StoreLock> Command::lockDBByKey(NetSession *sess,
                                                const std::string& key,
                                                mgl::LockMode mode) {
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
    uint32_t storeId = segMgr->calcSegId(key);
    return std::make_unique<StoreLock>(storeId, mode);
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

std::string Command::fmtOne() {
    return ":1\r\n";
}

std::string Command::fmtZero() {
    return ":0\r\n";
}

std::stringstream& Command::fmtMultiBulkLen(std::stringstream& ss, uint64_t l) {
    ss << "*" << l << "\r\n";
    return ss;
}

std::stringstream& Command::fmtBulk(std::stringstream& ss,
        const std::string& s) {
    ss << "$" << s.size() << "\r\n";
    ss.write(s.c_str(), s.size());
    ss << "\r\n";
    return ss;
}

std::string Command::fmtBulk(const std::string& s) {
    std::stringstream ss;
    ss << "$" << s.size() << "\r\n";
    ss.write(s.c_str(), s.size());
    ss << "\r\n";
    return ss.str();
}

}  // namespace tendisplus
