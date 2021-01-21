// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_SCRIPT_LUA_STATE_H_
#define SRC_TENDISPLUS_SCRIPT_LUA_STATE_H_

extern "C" {
#include "lua.h"
#include "lauxlib.h"
#include "lualib.h"
}

#include <memory>
#include <string>
#include "tendisplus/commands/command.h"
#include "tendisplus/network/blocking_tcp_client.h"
#include "tendisplus/server/session.h"
#include "tendisplus/script/rand.h"
#include "tendisplus/script/script_manager.h"

namespace tendisplus {

class ScriptManager;

class LuaState {
 public:
  explicit LuaState(std::shared_ptr<ServerEntry> svr, uint32_t id);
  ~LuaState();

  lua_State* initLua(int setup);
  uint32_t Id() {
    return _id;
  }
  Expected<std::string> evalCommand(Session* sess);
  Expected<std::string> evalGenericCommand(Session *sess, int evalsha);
  void LuaClose();
  bool luaWriteDirty() {
    return lua_write_dirty;
  }
  void setLastEndTime(uint64_t val) {
    lua_time_end = val;
  }
  uint64_t lastEndTime() {
    return lua_time_end;
  }
  void setRunning(bool val) {
    running = val;
  }
  bool isRunning() {
    return running;
  }

 private:
  void updateFakeClient();
  static void sha1hex(char *digest, char *script, size_t len);
  static int luaRedisSha1hexCommand(lua_State *lua);
  void luaRemoveUnsupportedFunctions(lua_State *lua);
  int luaRedisReplicateCommandsCommand(lua_State *lua);
  Expected<std::string> luaCreateFunction(lua_State *lua,
        const std::string& body);
  Expected<std::string> luaReplyToRedisReply(lua_State *lua);
  static int luaRedisCallCommand(lua_State *lua);
  static int luaRedisPCallCommand(lua_State *lua);
  static int luaRedisGenericCommand(lua_State *lua, int raise_error);
  static void luaMaskCountHook(lua_State *lua, lua_Debug *ar);
  void pushThisToLua(lua_State *lua);
  static LuaState* getLuaStateFromLua(lua_State *lua);
  static int redis_math_random(lua_State *L);
  static int redis_math_randomseed(lua_State *L);

 private:
  uint32_t _id;
  lua_State* _lua;
  std::shared_ptr<ServerEntry> _svr;
  ScriptManager* _scriptMgr;
  Session* _sess;
  std::unique_ptr<LocalSessionGuard> _fakeSess;
  std::atomic<bool> running{false};
  int inuse = 0;   /* Recursive calls detection. */
  uint64_t lua_time_start;  // ms
  uint64_t lua_time_end;  // ms
  int lua_timedout;  // True if we reached the time limit for script execution.
  std::atomic<int> lua_write_dirty;  // True if a write command was called
                          // during the execution of the current script.
  int lua_random_dirty;  // True if a random command was called during the
                         // execution of the current script.
  int lua_replicate_commands; /* True if we are doing single commands repl. */
  int lua_multi_emitted; /* True if we already proagated MULTI. */
  // bool has_command_error;  // if one redis command has error,
                           // dont commit all transactions.
  RedisRandom _rand;
};

}  // namespace tendisplus
#endif  // SRC_TENDISPLUS_SCRIPT_LUA_STATE_H_
