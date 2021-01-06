// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_SCRIPT_SCRIPT_MANAGER_H_
#define SRC_TENDISPLUS_SCRIPT_SCRIPT_MANAGER_H_

#include <list>
#include <map>
#include <memory>
#include <string>
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
  Expected<std::string> run(Session* sess);
  Expected<std::string> setLuaKill();
  Expected<std::string> flush();
  bool luaKill();
  bool stopped();

 private:
  std::shared_ptr<ServerEntry> _svr;

  mutable std::mutex _mutex;
  std::map<uint32_t, std::shared_ptr<LuaState>> _luaRunningList;
  std::list<std::shared_ptr<LuaState>> _luaIdleList;
  std::atomic<uint32_t> _idGen;

  bool lua_kill;
  bool _stopped;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_SCRIPT_SCRIPT_MANAGER_H_
