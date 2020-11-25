cmake_minimum_required (VERSION 2.8.0)

project (lua)  # project here actually means solution in premake

if(WIN32)
        add_definitions( -D_CRT_SECURE_NO_WARNINGS )
endif()

set (SRC_LIST src/fpconv.c src/lapi.c src/lauxlib.c src/lbaselib.c src/lcode.c src/ldblib.c src/ldebug.c src/ldo.c src/ldump.c src/lfunc.c src/lgc.c src/linit.c src/liolib.c src/llex.c src/lmathlib.c src/lmem.c src/loadlib.c src/lobject.c src/lopcodes.c src/loslib.c src/lparser.c src/lstate.c src/lstring.c src/lstrlib.c src/ltable.c src/ltablib.c src/ltm.c src/lua.c src/lua_bit.c src/lua_cjson.c src/lua_cmsgpack.c src/lua_struct.c src/luac.c src/lundump.c src/lvm.c src/lzio.c src/print.c src/strbuf.c)

add_library (lua STATIC ${SRC_LIST})

