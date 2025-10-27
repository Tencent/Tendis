set(LUA_SRC src/fpconv.c
src/lapi.c
src/lauxlib.c
src/lbaselib.c
src/lcode.c
src/ldblib.c
src/ldebug.c
src/ldo.c
src/ldump.c
src/lfunc.c
src/lgc.c
src/linit.c
src/liolib.c
src/llex.c
src/lmathlib.c
src/lmem.c
src/loadlib.c
src/lobject.c
src/lopcodes.c
src/loslib.c
src/lparser.c
src/lstate.c
src/lstring.c
src/lstrlib.c
src/ltable.c
src/ltablib.c
src/ltm.c
src/lua.c
src/lua_bit.c
src/lua_cjson.c
src/lua_cmsgpack.c
src/lua_struct.c
src/luac.c
src/lundump.c
src/lvm.c
src/lzio.c
src/print.c
src/strbuf.c)

add_definitions(-Wall -DLUA_ANSI -DENABLE_CJSON_GLOBAL -DLUA_USE_MKSTEMP)
add_library(lua STATIC ${LUA_SRC})
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_compile_options(lua PRIVATE -O0 -DLUA_USE_APICHECK)
endif()
