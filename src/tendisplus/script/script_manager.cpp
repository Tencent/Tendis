// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.


#include <memory>
#include <string>
#include <shared_mutex>
#include "tendisplus/script/script_manager.h"

namespace tendisplus {

ScriptManager::ScriptManager(std::shared_ptr<ServerEntry> svr)
  :_svr(svr),
   _luaKill(false),
   _stopped(false) {
}

Expected<std::string> ScriptManager::run(Session* sess) {
  // NOTE(takenliu):
  //   use shared_lock in every command with high frequency,
  //   otherwise use unique_lock with low frequency.
  if (_luaKill) {
    std::unique_lock<std::shared_timed_mutex> lock(_mutex);
    for (auto iter = _mapLuaState.begin(); iter != _mapLuaState.end();
         ++iter) {
      if (iter->second->isRunning()) {
        LOG(WARNING) << "script kill or flush not finished:"
          << _mapLuaState.size();
        return {ErrorCodes::ERR_LUA, "script kill not finished."};
      }
    }
    LOG(WARNING) << "script kill or flush all finished.";
    _luaKill = false;
  }
  std::shared_ptr<LuaState> luaState = nullptr;
  uint64_t threadid = getCurThreadId();
  {
    std::shared_lock<std::shared_timed_mutex> lock(_mutex);
    auto iter = _mapLuaState.find(threadid);
    if (iter != _mapLuaState.end()) {
      luaState = iter->second;
      luaState->setRunning(true);
    }
  }
  if (luaState == nullptr) {
    luaState = std::make_shared<LuaState>(_svr, threadid);
    luaState->setRunning(true);
    std::unique_lock<std::shared_timed_mutex> lock(_mutex);
    _mapLuaState[threadid] = luaState;
    LOG(INFO) << "new LuaState, threadid:" << threadid
      << " _mapLuaState size:" << _mapLuaState.size();
  }
  auto ret = luaState->evalCommand(sess);
  {
    std::shared_lock<std::shared_timed_mutex> lock(_mutex);
    luaState->setLastEndTime(msSinceEpoch());
    luaState->setRunning(false);
  }
  return ret;
}

Expected<std::string> ScriptManager::setLuaKill() {
  std::unique_lock<std::shared_timed_mutex> lock(_mutex);
  bool someRunning = false;
  for (auto iter = _mapLuaState.begin(); iter != _mapLuaState.end();
       ++iter) {
    if (iter->second->isRunning()) {
      someRunning = true;
    }
  }
  if (!someRunning) {
    LOG(WARNING) << "script kill, no running script.";
    return {ErrorCodes::ERR_LUA,
            "-NOTBUSY No scripts in execution right now.\r\n"};
  }

  // NOTE(takenliu) can kill when someone is luaWriteDirty.

  _luaKill = true;
  LOG(WARNING) << "script kill set ok.";
  return Command::fmtOK();
}

bool ScriptManager::luaKill() {
  return _luaKill;
}

Expected<std::string> ScriptManager::flush() {
  std::unique_lock<std::shared_timed_mutex> lock(_mutex);
  for (auto iter = _mapLuaState.begin(); iter != _mapLuaState.end();
    iter++) {
    if (iter->second->isRunning()) {
      return {ErrorCodes::ERR_LUA,
          "-BUSY Redis is busy running a script."
          " You can only call SCRIPT KILL or SHUTDOWN NOSAVE.\r\n"};
    }
  }

  for (auto iter = _mapLuaState.begin(); iter != _mapLuaState.end();
    iter++) {
    iter->second->LuaClose();
    iter->second->setRunning(false);
    iter->second->initLua(0);
  }
  return Command::fmtOK();
}

Status ScriptManager::startup(uint32_t luaStateNum) {
  LOG(INFO) << "ScriptManager::startup begin.";
  return {ErrorCodes::ERR_OK, ""};
}

void ScriptManager::cron() {
  std::unique_lock<std::shared_timed_mutex> lock(_mutex);
  uint64_t cur = msSinceEpoch();
  static uint64_t maxIdelTime = _svr->getParams()->luaStateMaxIdleTime;
  for (auto iter = _mapLuaState.begin(); iter != _mapLuaState.end();) {
    if (!iter->second->isRunning() &&
      cur - iter->second->lastEndTime() > maxIdelTime) {
      LOG(INFO) << "delete LuaState, threadid:" << iter->first
                << " idletime:" << cur - iter->second->lastEndTime()
                << " running size:" << _mapLuaState.size();
      iter = _mapLuaState.erase(iter);
    } else {
      iter++;
    }
  }
}

Status ScriptManager::stopStore(uint32_t storeId) {
  return {ErrorCodes::ERR_OK, ""};
}

void ScriptManager::stop() {
  LOG(INFO) << "ScriptManager stop.";
  _stopped = true;
}

bool ScriptManager::stopped() {
  return _stopped;
}

}  // namespace tendisplus
