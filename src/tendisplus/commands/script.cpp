// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <string>

#include "tendisplus/commands/command.h"

namespace tendisplus {

class EvalGenericCommand : public Command {
 public:
  EvalGenericCommand(const std::string& name, const char* sflags)
    : Command(name, sflags) {
    if (name == "evalsha") {
      _evalsha = 1;
    } else {
      _evalsha = 0;
    }
  }

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
    return sess->getServerEntry()->getScriptMgr()->run(sess, _evalsha);
  }

 private:
  int _evalsha;
};

class EvalCommand : public EvalGenericCommand {
 public:
  EvalCommand() : EvalGenericCommand("eval", "sw") {}
} evalCmd;

class EvalShaCommand : public EvalGenericCommand {
 public:
  EvalShaCommand() : EvalGenericCommand("evalsha", "sw") {}
} evalShaCmd;

class ScriptCommand : public Command {
 public:
  ScriptCommand() : Command("script", "sw") {}

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

  static std::string getHelpInfo(Session* sess) {
    static auto s = []() {
      std::stringstream ss;
      Command::fmtMultiBulkLen(ss, 11);
      Command::fmtBulk(ss,
                       "SCRIPT <subcommand> [<arg> [value] [opt] ...]."
                       "Subcommands are:");
      Command::fmtBulk(ss, "EXISTS <sha1> [<sha1> ...]");
      Command::fmtBulk(ss,
                       "    Return information about the existence of the "
                       "scripts in the script store.");
      Command::fmtBulk(ss, "FLUSH");
      Command::fmtBulk(ss,
                       "    Flush the Lua scripts store. Very dangerous on "
                       "replicas.");
      Command::fmtBulk(ss, "KILL");
      Command::fmtBulk(ss, "    Kill the currently executing Lua script.");
      Command::fmtBulk(ss, "LOAD <script>");
      Command::fmtBulk(ss,
                       "    Load a script into the scripts store without"
                       " executing it.");
      Command::fmtBulk(ss, "HELP");
      Command::fmtBulk(ss, "    Prints this help.");
      return ss.str();
    }();
    return s;
  }

  Expected<std::string> run(Session* sess) final {
    auto server = sess->getServerEntry();
    const auto& args = sess->getArgs();
    const auto& op = toLower(args[1]);
    if (op == "kill" && args.size() == 2) {
      return server->getScriptMgr()->setLuaKill();
    } else if (op == "flush" && args.size() == 2) {
      return server->getScriptMgr()->flush(sess);
    } else if (op == "load" && args.size() == 3) {
      return server->getScriptMgr()->saveLuaScript(sess, "", args[2]);
    } else if (op == "exists" && args.size() > 2) {
      return server->getScriptMgr()->checkIfScriptExists(sess);
    } else if (op == "help" && args.size() == 2) {
      return getHelpInfo(sess);
    } else {
      return {ErrorCodes::ERR_LUA,
              "ERR Unknown subcommand or wrong number of arguments for '" + op +
                "'. Try SCRIPT HELP."};
    }
    return Command::fmtOK();
  }
} scriptCmd;

}  // namespace tendisplus
