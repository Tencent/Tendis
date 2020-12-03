//
// Created by takenliu on 2020/11/24.
//

#include <string>
#include "script_manager.h"

namespace tendisplus {

ScriptManager::ScriptManager(std::shared_ptr<ServerEntry> svr)
  :_svr(svr),
   _idGen(0) {
}

Expected<std::string> ScriptManager::run(Session* sess) {
  if (_kill) {
    if (_luaRunningList.size() > 0) {
      LOG(WARNING) << "script kill not finished:" << _luaRunningList.size();
      return {ErrorCodes::ERR_LUA, "script kill not finished."};
    } else {
      std::lock_guard<std::mutex> lk(_mutex);
      _kill = false;
    }
  }
  std::shared_ptr<LuaState> luaState;
  {
    std::lock_guard<std::mutex> lk(_mutex);
    if (_luaIdleList.size() > 0) {
      luaState = _luaIdleList.front();
      _luaIdleList.pop_front();
    } else {
      uint32_t id = _idGen.fetch_add(1);
      luaState = std::make_shared<LuaState>(_svr, id);
    }
    _luaRunningList[luaState->Id()] = luaState;
  }
  auto ret = luaState->run(sess);
  {
    std::lock_guard<std::mutex> lk(_mutex);
    // may be erased by flush()
    if (_luaRunningList.find(luaState->Id()) != _luaRunningList.end()) {
      _luaIdleList.push_back(luaState);
      _luaRunningList.erase(luaState->Id());
    }
    if (_luaIdleList.size() + _luaRunningList.size() > _svr->getParams()->executorThreadNum) {
      LOG(WARNING) << "luaState too much,_luaIdleList:" << _luaIdleList.size()
        << " _luaRunningList:" << _luaRunningList.size()
        << " executorThreadNum:" << _svr->getParams()->executorThreadNum;
    }
  }
  return ret;
}

Expected<std::string> ScriptManager::kill() {
  std::lock_guard<std::mutex> lk(_mutex);
  if (_luaRunningList.size() <= 0) {
    return {ErrorCodes::ERR_LUA, "-NOTBUSY No scripts in execution right now."};
  }
  _kill = true;
  return {ErrorCodes::ERR_OK, ""};
}

Expected<std::string> ScriptManager::flush() {
  std::lock_guard<std::mutex> lk(_mutex);
  for(auto iter = _luaRunningList.begin(); iter != _luaRunningList.end();) {
    iter->second->LuaClose(); // what will happen in lua_stat.run() ???
    _luaIdleList.push_back(iter->second);
    iter = _luaRunningList.erase(iter);
  }
  for(auto iter = _luaIdleList.begin(); iter != _luaIdleList.end();iter++) {
    // _luaIdleList whether need lua_close() ???
    iter->get()->initLua(0);
  }
  return {ErrorCodes::ERR_OK, ""};
}

Status ScriptManager::startup(uint32_t luaStateNum) {
  LOG(INFO) << "ScriptManager::startup begin";
  for (uint32_t i = 0; i < luaStateNum; ++i) {
    uint32_t id = _idGen.fetch_add(1);
    LuaState s(_svr, id);
    _luaIdleList.push_back(std::make_shared<LuaState>(s));
  }
  return {ErrorCodes::ERR_OK, ""};
}

Status ScriptManager::stopStore(uint32_t storeId) {
  return {ErrorCodes::ERR_OK, ""};
}

void ScriptManager::stop() {
}

}  // namespace tendisplus