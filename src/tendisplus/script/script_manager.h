//
// Created by takenliu on 2020/11/24.
//

#ifndef TENDIS_PLUS_SCRIPT_MANAGER_H
#define TENDIS_PLUS_SCRIPT_MANAGER_H

extern "C"{
#include "lua.h"
#include "lauxlib.h"
#include "lualib.h"
}
#include "tendisplus/server/server_entry.h"

namespace tendisplus {

class ScriptManager {
public:
    explicit ScriptManager(std::shared_ptr<ServerEntry> svr);
    Status startup();
    Status stopStore(uint32_t storeId);
    void stop();
      void init(int);
    void sha1hex(char *digest, char *script, size_t len);
    lua_State *getLuaState() {
      return _lua;
    }

private:
    void luaLoadLib(lua_State *lua, const char *libname, lua_CFunction luafunc);
      void luaLoadLibraries(lua_State *lua);
    void luaRemoveUnsupportedFunctions(lua_State *lua);

      private:
    lua_State *_lua;
    std::shared_ptr<ServerEntry> _svr;
};

}  // namespace tendisplus

#endif //TENDIS_PLUS_SCRIPT_MANAGER_H
