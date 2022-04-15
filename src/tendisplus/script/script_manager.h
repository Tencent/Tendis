// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_SCRIPT_SCRIPT_MANAGER_H_
#define SRC_TENDISPLUS_SCRIPT_SCRIPT_MANAGER_H_

#include <list>
#include <map>
#include <memory>
#include <string>
#include <shared_mutex>
#include "tendisplus/server/server_entry.h"
#include "tendisplus/script/lua_state.h"

namespace tendisplus {

class LuaState;

class ScriptManager {
 public:
  explicit ScriptManager(std::shared_ptr<ServerEntry> svr);
  Status startup(uint32_t luaStateNum);
  Status stopStore(uint32_t storeId);
  void cron();
  void stop();
  Expected<std::string> run(Session* sess, int evalsha);
  Expected<std::string> setLuaKill();
  Expected<std::string> flush(Session* sess);
  Expected<std::string> getScriptContent(Session* sess,
                                         const std::string& sha);
  Expected<std::string> saveLuaScript(Session* sess,
                                      const std::string& sha,
                                      const std::string& script);
  Expected<std::string> checkIfScriptExists(Session* sess);
  bool luaKill() const {
    return _luaKill;
  }
  bool stopped() const {
    return _stopped.load(std::memory_order_relaxed);
  }

 private:
  std::shared_ptr<LuaState> getLuaStateBelongToThisThread();

 private:
  std::shared_ptr<ServerEntry> _svr;

  mutable std::shared_timed_mutex _mutex;
  std::map<uint64_t, std::shared_ptr<LuaState>> _mapLuaState;

  std::atomic<bool> _luaKill;
  std::atomic<bool> _stopped;

  static const uint32_t LUASCRIPT_DEFAULT_DBID = 0;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_SCRIPT_SCRIPT_MANAGER_H_
