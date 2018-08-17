#ifndef SRC_TENDISPLUS_COMMANDS_COMMAND_H_
#define SRC_TENDISPLUS_COMMANDS_COMMAND_H_

#include <string>
#include <map>
#include "tendisplus/utils/status.h"
#include "tendisplus/network/network.h"

namespace tendisplus {

class Command {
 public:
    using CmdMap = std::map<std::string, Command*>;
    explicit Command(const std::string& name);
    virtual Expected<std::string> run(NetSession *sess) = 0;
    virtual ssize_t arity() const = 0;
    virtual int32_t firstkey() const = 0;
    virtual int32_t lastkey() const = 0;
    virtual int32_t keystep() const = 0;
    static Status precheck(NetSession *sess);
    static Expected<std::string> runSessionCmd(NetSession *sess);
 private:
    const std::string _name;
    // NOTE(deyukong): all commands have been loaded at startup time
    // so there is no need to acquire a lock here.
    static CmdMap _commands;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_COMMANDS_COMMAND_H_
