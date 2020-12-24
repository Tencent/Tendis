
#include <string>
#include "tendisplus/commands/command.h"

namespace tendisplus {

class EvalCommand : public Command {
public:
  EvalCommand() : Command("eval", "s") {}

  ssize_t arity() const {
    return -3;
  }

  int32_t firstkey() const {
    return 0;
  }

  int32_t lastkey() const {
    return 0;
  }

  int32_t keystep() const {
    return 0;
  }

  bool sameWithRedis() const {
    return false;
  }

  Expected<std::string> run(Session* sess) final {
    auto server = sess->getServerEntry();
    auto ret = server->getScriptMgr()->run(sess);
    return ret;
  }
} evalCmd;

class ScriptCommand : public Command {
public:
  ScriptCommand() : Command("script", "s") {}

  ssize_t arity() const {
    return -2;
  }

  int32_t firstkey() const {
    return 0;
  }

  int32_t lastkey() const {
    return 0;
  }

  int32_t keystep() const {
    return 0;
  }

  bool sameWithRedis() const {
    return false;
  }

  Expected<std::string> run(Session* sess) final {
    auto server = sess->getServerEntry();
    const std::vector<std::string>& args = sess->getArgs();
    const std::string op = toLower(args[1]);
    if (op == "kill") {
      return server->getScriptMgr()->setLuaKill();
    } else {
      return {ErrorCodes::ERR_LUA, "Unknown SCRIPT subcommand or wrong # of args."};
    }
    return Command::fmtOK();
  }
} scriptCmd;

}  // namespace tendisplus
