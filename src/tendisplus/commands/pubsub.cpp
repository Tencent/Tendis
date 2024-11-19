// Copyright (C) 2024 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <string>
#include <vector>

#include "tendisplus/commands/command.h"
#include "tendisplus/utils/string.h"

namespace tendisplus {

void subscribeReply(std::stringstream& ss,
                    const std::string& name,
                    const std::string& subName,
                    int num) {
  Command::fmtMultiBulkLen(ss, 3);
  Command::fmtBulk(ss, name);
  if (subName.empty()) {
    Command::fmtNull(ss);
  } else {
    Command::fmtBulk(ss, subName);
  }
  Command::fmtLongLong(ss, num);
}

void unsubscribeAll(std::stringstream& ss, Session* sess) {
  auto pCtx = sess->getCtx();
  INVARIANT(pCtx != nullptr);
  auto server = sess->getServerEntry();

  if (pCtx->getSubChannelCount() == 0) {
    subscribeReply(ss, "unsubscribe", "", pCtx->getSubPatternCount());
    return;
  }
  auto subChannels = pCtx->getSubscribeChannels();
  int removed = 0;
  for (const auto& curChannel : subChannels) {
    server->UnsubscribeChannel(sess, curChannel);
    ++removed;
    subscribeReply(ss,
                   "unsubscribe",
                   curChannel,
                   pCtx->getSubChannelCount() - removed +
                     pCtx->getSubPatternCount());
  }
  pCtx->clearSubscribeChannel();
}

void punsubscribeAll(std::stringstream& ss, Session* sess) {
  auto pCtx = sess->getCtx();
  INVARIANT(pCtx != nullptr);
  auto server = sess->getServerEntry();

  if (pCtx->getSubPatternCount() == 0) {
    subscribeReply(ss, "punsubscribe", "", pCtx->getSubChannelCount());
    return;
  }
  auto subPatterns = pCtx->getSubscribePatterns();
  int removed = 0;
  for (const auto& curPattern : subPatterns) {
    server->UnsubscribePattern(sess, curPattern);
    ++removed;
    subscribeReply(ss,
                   "punsubscribe",
                   curPattern,
                   pCtx->getSubPatternCount() - removed +
                     pCtx->getSubChannelCount());
  }
  pCtx->clearSubscribePattern();
}

class PublishCommand : public Command {
 public:
  PublishCommand() : Command("publish", "pltF") {}

  ssize_t arity() const {
    return 3;
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

  Expected<std::string> run(Session* sess) final {
    const std::string& key = sess->getArgs()[firstkey()];
    auto server = sess->getServerEntry();

    auto check = server->getSegmentMgr()->handleRedirectByKey(sess, key);
    if (!check.ok())
      return check.status();

    int reveiverCount = server->PublishMessage(sess);

    return Command::fmtLongLong(reveiverCount);
  }
} publishCmd;


class SubscribeCommand : public Command {
 public:
  SubscribeCommand() : Command("subscribe", "pslt") {}

  ssize_t arity() const {
    return -2;
  }

  int32_t firstkey() const {
    return 1;
  }

  int32_t lastkey() const {
    return -1;
  }

  int32_t keystep() const {
    return 1;
  }

  Expected<std::string> run(Session* sess) final {
    const auto& args = sess->getArgs();
    const auto& cfg = sess->getServerEntry()->getParams();
    auto server = sess->getServerEntry();
    auto pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);

    std::stringstream reply;
    if (cfg->enableMovePubSubRequest) {
      auto index = getKeysFromCommand(args);
      auto check = server->getSegmentMgr()->getAllKeysLocked(
      sess, args, index, mgl::LockMode::LOCK_NONE, getFlags());
      if (!check.ok()) {
        return check.status();
      }
    }
    for (size_t i = 1; i < args.size(); ++i) {
      pCtx->subscribeChannel(args[i]);
      server->SubscribeChannel(sess->id(), args[i]);
      subscribeReply(reply,
                     "subscribe",
                     args[i],
                     pCtx->getSubChannelCount() + pCtx->getSubPatternCount());
    }
    return reply.str();
  }
} subscribeCmd;

class UnsubscribeCommand : public Command {
 public:
  UnsubscribeCommand() : Command("unsubscribe", "pslt") {}

  ssize_t arity() const {
    return -1;
  }

  int32_t firstkey() const {
    return 1;
  }

  int32_t lastkey() const {
    return -1;
  }

  int32_t keystep() const {
    return 1;
  }

  Expected<std::string> run(Session* sess) final {
    auto pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    auto server = sess->getServerEntry();
    const auto& cfg = sess->getServerEntry()->getParams();
    const auto& args = sess->getArgs();

    std::stringstream reply;

    if (args.size() == 1) {
      unsubscribeAll(reply, sess);
      return reply.str();
    }

    if (cfg->enableMovePubSubRequest) {
      auto index = getKeysFromCommand(args);
      auto check = server->getSegmentMgr()->getAllKeysLocked(
      sess, args, index, mgl::LockMode::LOCK_NONE, getFlags());
      if (!check.ok()) {
        return check.status();
      }
    }
    for (size_t i = 1; i < args.size(); ++i) {
      const auto& curChannel = args[i];
      pCtx->unsubscribeChannel(curChannel);
      server->UnsubscribeChannel(sess, curChannel);
      subscribeReply(reply,
                     "unsubscribe",
                     curChannel,
                     pCtx->getSubChannelCount() + pCtx->getSubPatternCount());
    }

    return reply.str();
  }
} unsubscribeCmd;

class PSubscribeCommand : public Command {
 public:
  PSubscribeCommand() : Command("psubscribe", "pslt") {}

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

  Expected<std::string> run(Session* sess) final {
    auto pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    auto server = sess->getServerEntry();
    const auto& args = sess->getArgs();

    std::stringstream reply;
    for (size_t i = 1; i < args.size(); ++i) {
      pCtx->subscribePattern(args[i]);
      server->SubscribePattern(sess->id(), args[i]);
      subscribeReply(reply,
                     "psubscribe",
                     args[i],
                     pCtx->getSubChannelCount() + pCtx->getSubPatternCount());
    }

    return reply.str();
  }
} psubscribeCmd;

class PUnSubscribeCommand : public Command {
 public:
  PUnSubscribeCommand() : Command("punsubscribe", "pslt") {}

  ssize_t arity() const {
    return -1;
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
    auto pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    auto server = sess->getServerEntry();

    const auto& args = sess->getArgs();

    std::stringstream reply;

    if (args.size() == 1) {
      punsubscribeAll(reply, sess);
      return reply.str();
    }

    for (size_t i = 1; i < args.size(); ++i) {
      const auto& curChannel = args[i];
      pCtx->unsubscribePattern(curChannel);
      server->UnsubscribePattern(sess, curChannel);
      subscribeReply(reply,
                     "punsubscribe",
                     curChannel,
                     pCtx->getSubChannelCount() + pCtx->getSubPatternCount());
    }

    return reply.str();
  }
} punsubscribeCmd;

class PubSubCommand : public Command {
 public:
  PubSubCommand() : Command("pubsub", "pltR") {}

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

  Expected<std::string> run(Session* sess) final {
    auto pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    auto server = sess->getServerEntry();

    const auto& args = sess->getArgs();
    const auto& subCommand = toLower(args[1]);

    if (subCommand == "numpat") {
      if (args.size() == 2) {
        return Command::fmtLongLong(pCtx->getSubPatternCount());
      } else {
        return {ErrorCodes::ERR_WRONG_ARGS_SIZE,
                "ERR Unknown subcommand or wrong number of arguments"};
      }
    }

    std::stringstream reply;

    if (subCommand == "numsub") {
      std::vector<std::pair<std::string, int>> channelSubNum(args.size() - 2);
      for (size_t i = 2; i < args.size(); ++i) {
        channelSubNum[i - 2].first = args[i];
        channelSubNum[i - 2].second = 0;
      }
      server->ListChannelSubscribeNum(&channelSubNum);
      Command::fmtMultiBulkLen(reply, channelSubNum.size() * 2);
      for (const auto& [curChannel, curNum] : channelSubNum) {
        Command::fmtBulk(reply, curChannel);
        Command::fmtLongLong(reply, curNum);
      }
      return reply.str();
    }

    if (subCommand == "channels") {
      std::string pattern;
      if (args.size() == 3) {
        pattern = args[2];
      }
      std::vector<std::string> channels;
      server->ListChannelByPattern(pattern, &channels);
      Command::fmtMultiBulkLen(reply, channels.size());
      for (const auto& curChannel : channels) {
        Command::fmtBulk(reply, curChannel);
      }
      return reply.str();
    }

    return {ErrorCodes::ERR_WRONG_ARGS_SIZE,
            "ERR Unknown subcommand or wrong number of arguments"};
  }
} pubsubCmd;

}  // namespace tendisplus
