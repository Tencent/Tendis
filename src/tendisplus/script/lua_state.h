//
// Created by takenliu on 2020/11/30.
//

#ifndef TENDIS_PLUS_LUA_STATE_H
#define TENDIS_PLUS_LUA_STATE_H

extern "C"{
#include "lua.h"
#include "lauxlib.h"
#include "lualib.h"
}

#include "tendisplus/commands/command.h"
#include "tendisplus/network/blocking_tcp_client.h"
#include "tendisplus/server/session.h"

namespace tendisplus {

class LuaState {
public:
  explicit LuaState(std::shared_ptr<ServerEntry> svr, uint32_t id);
  ~LuaState();

  lua_State* initLua(int setup);
  uint32_t Id() {
    return _id;
  }
  Expected<std::string> run(Session* sess);
  void LuaClose();

private:
  void newFakeClient();
  void sha1hex(char *digest, char *script, size_t len);
  void luaRemoveUnsupportedFunctions(lua_State *lua);
  Expected<std::string> luaCreateFunction(lua_State *lua, const std::string& body);
  Expected<std::string> luaReplyToRedisReply(lua_State *lua);
  static int luaRedisCallCommand(lua_State *lua);
  static int luaRedisPCallCommand(lua_State *lua);
  static int luaRedisGenericCommand(lua_State *lua, int raise_error);

private:
  uint32_t _id;
  lua_State* _lua;
  std::shared_ptr<ServerEntry> _svr;
  std::shared_ptr<BlockingTcpClient> _client;
};

}  // namespace tendisplus
#endif //TENDIS_PLUS_LUA_STATE_H
