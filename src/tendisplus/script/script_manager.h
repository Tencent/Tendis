//
// Created by takenliu on 2020/11/24.
//

#ifndef TENDIS_PLUS_SCRIPT_MANAGER_H
#define TENDIS_PLUS_SCRIPT_MANAGER_H

#include "tendisplus/server/server_entry.h"
#include "tendisplus/script/lua_state.h"

namespace tendisplus {

class LuaState;

class ScriptManager {
public:
  explicit ScriptManager(std::shared_ptr<ServerEntry> svr);
  Status startup(uint32_t luaStateNum);
  Status stopStore(uint32_t storeId);
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

#endif //TENDIS_PLUS_SCRIPT_MANAGER_H
