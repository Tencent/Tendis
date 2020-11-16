// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <string>
#include <utility>
#include <memory>
#include <algorithm>
#include <cctype>
#include <clocale>
#include "glog/logging.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

class SelectCommand : public Command {
 public:
  SelectCommand() : Command("select", "lF") {}

  ssize_t arity() const {
    return 2;
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

  Expected<std::string> run(Session* sess) final {
    auto dbId = ::tendisplus::stoll(sess->getArgs()[1]);
    if (!dbId.ok()) {
      return {ErrorCodes::ERR_PARSEOPT, "invalid DB index"};
    }

    auto id = dbId.value();
    if (id < 0 || id >= sess->getServerEntry()->dbNum()) {
      return {ErrorCodes::ERR_PARSEOPT, "DB index is out of range"};
    }

    if (sess->getServerEntry()->isClusterEnabled() && id != 0) {
      return {ErrorCodes::ERR_PARSEOPT,
              "SELECT is not allowed in cluster mode"};
    }

    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    pCtx->setDbId(id);

    return Command::fmtOK();
  }
} selectCommand;

class AuthCommand : public Command {
 public:
  AuthCommand() : Command("auth", "sltF") {}

  Expected<std::string> parse(Session* sess) const {
    const auto& args = sess->getArgs();
    if (args.size() != 2) {
      return {ErrorCodes::ERR_PARSEPKT, "invalid auth command"};
    }
    return args[1];
  }

  ssize_t arity() const {
    return 2;
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

  Expected<std::string> run(Session* sess) final {
    Expected<std::string> params = parse(sess);
    if (!params.ok()) {
      return params.status();
    }

    std::string requirePass = sess->getServerEntry()->requirepass();
    if (requirePass == "") {
      return {ErrorCodes::ERR_AUTH, "Client sent AUTH, but no password is set"};
    }
    if (requirePass != params.value()) {
      return {ErrorCodes::ERR_AUTH, "invalid password"};
    }

    SessionCtx* pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    pCtx->setAuthed();

    return Command::fmtOK();
  }
} authCommand;

}  // namespace tendisplus
