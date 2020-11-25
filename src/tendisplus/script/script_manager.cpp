//
// Created by takenliu on 2020/11/24.
//

#include <string>
#include "script_manager.h"
extern "C" {
#include "sha1.h"
}
namespace tendisplus {

#define LUA_CMD_OBJCACHE_SIZE 32
#define LUA_CMD_OBJCACHE_MAX_LEN 64
int luaRedisGenericCommand(lua_State *lua, int raise_error) {
//  int j, argc = lua_gettop(lua);
//  struct redisCommand *cmd;
//  client *c = server.lua_client;
//  sds reply;
//
//  /* Cached across calls. */
//  static robj **argv = NULL;
//  static int argv_size = 0;
//  static robj *cached_objects[LUA_CMD_OBJCACHE_SIZE];
//  static size_t cached_objects_len[LUA_CMD_OBJCACHE_SIZE];
//  static int inuse = 0;   /* Recursive calls detection. */
//
//  /* Reflect MULTI state */
//  if (server.lua_multi_emitted || (server.lua_caller->flags & CLIENT_MULTI)) {
//    c->flags |= CLIENT_MULTI;
//  } else {
//    c->flags &= ~CLIENT_MULTI;
//  }
//
//  /* By using Lua debug hooks it is possible to trigger a recursive call
//   * to luaRedisGenericCommand(), which normally should never happen.
//   * To make this function reentrant is futile and makes it slower, but
//   * we should at least detect such a misuse, and abort. */
//  if (inuse) {
//    char *recursion_warning =
//            "luaRedisGenericCommand() recursive call detected. "
//            "Are you doing funny stuff with Lua debug hooks?";
//    serverLog(LL_WARNING,"%s",recursion_warning);
//    luaPushError(lua,recursion_warning);
//    return 1;
//  }
//  inuse++;
//
//  /* Require at least one argument */
//  if (argc == 0) {
//    luaPushError(lua,
//                 "Please specify at least one argument for redis.call()");
//    inuse--;
//    return raise_error ? luaRaiseError(lua) : 1;
//  }
//
//  /* Build the arguments vector */
//  if (argv_size < argc) {
//    argv = zrealloc(argv,sizeof(robj*)*argc);
//    argv_size = argc;
//  }
//
//  for (j = 0; j < argc; j++) {
//    char *obj_s;
//    size_t obj_len;
//    char dbuf[64];
//
//    if (lua_type(lua,j+1) == LUA_TNUMBER) {
//      /* We can't use lua_tolstring() for number -> string conversion
//       * since Lua uses a format specifier that loses precision. */
//      lua_Number num = lua_tonumber(lua,j+1);
//
//      obj_len = snprintf(dbuf,sizeof(dbuf),"%.17g",(double)num);
//      obj_s = dbuf;
//    } else {
//      obj_s = (char*)lua_tolstring(lua,j+1,&obj_len);
//      if (obj_s == NULL) break; /* Not a string. */
//    }
//
//    /* Try to use a cached object. */
//    if (j < LUA_CMD_OBJCACHE_SIZE && cached_objects[j] &&
//        cached_objects_len[j] >= obj_len)
//    {
//      sds s = cached_objects[j]->ptr;
//      argv[j] = cached_objects[j];
//      cached_objects[j] = NULL;
//      memcpy(s,obj_s,obj_len+1);
//      sdssetlen(s, obj_len);
//    } else {
//      argv[j] = createStringObject(obj_s, obj_len);
//    }
//  }
//
//  /* Check if one of the arguments passed by the Lua script
//   * is not a string or an integer (lua_isstring() return true for
//   * integers as well). */
//  if (j != argc) {
//    j--;
//    while (j >= 0) {
//      decrRefCount(argv[j]);
//      j--;
//    }
//    luaPushError(lua,
//                 "Lua redis() command arguments must be strings or integers");
//    inuse--;
//    return raise_error ? luaRaiseError(lua) : 1;
//  }
//
//  /* Setup our fake client for command execution */
//  c->argv = argv;
//  c->argc = argc;
//
//  /* Log the command if debugging is active. */
//  if (ldb.active && ldb.step) {
//    sds cmdlog = sdsnew("<redis>");
//    for (j = 0; j < c->argc; j++) {
//      if (j == 10) {
//        cmdlog = sdscatprintf(cmdlog," ... (%d more)",
//                              c->argc-j-1);
//        break;
//      } else {
//        cmdlog = sdscatlen(cmdlog," ",1);
//        cmdlog = sdscatsds(cmdlog,c->argv[j]->ptr);
//      }
//    }
//    ldbLog(cmdlog);
//  }
//
//  /* Command lookup */
//  cmd = lookupCommand(argv[0]->ptr);
//  if (!cmd || ((cmd->arity > 0 && cmd->arity != argc) ||
//               (argc < -cmd->arity)))
//  {
//    if (cmd)
//      luaPushError(lua,
//                   "Wrong number of args calling Redis command From Lua script");
//    else
//      luaPushError(lua,"Unknown Redis command called from Lua script");
//    goto cleanup;
//  }
//  c->cmd = c->lastcmd = cmd;
//
//  /* There are commands that are not allowed inside scripts. */
//  if (cmd->flags & CMD_NOSCRIPT) {
//    luaPushError(lua, "This Redis command is not allowed from scripts");
//    goto cleanup;
//  }
//
//  /* Write commands are forbidden against read-only slaves, or if a
//   * command marked as non-deterministic was already called in the context
//   * of this script. */
//  if (cmd->flags & CMD_WRITE) {
//    if (server.lua_random_dirty && !server.lua_replicate_commands) {
//      luaPushError(lua,
//                   "Write commands not allowed after non deterministic commands. Call redis.replicate_commands() at the start of your script in order to switch to single commands replication mode.");
//      goto cleanup;
//    } else if (server.masterhost && server.repl_slave_ro &&
//               !server.loading &&
//               !(server.lua_caller->flags & CLIENT_MASTER))
//    {
//      luaPushError(lua, shared.roslaveerr->ptr);
//      goto cleanup;
//    } else if (server.stop_writes_on_bgsave_err &&
//               server.saveparamslen > 0 &&
//               server.lastbgsave_status == C_ERR)
//    {
//      luaPushError(lua, shared.bgsaveerr->ptr);
//      goto cleanup;
//    }
//  }
//
//  /* If we reached the memory limit configured via maxmemory, commands that
//   * could enlarge the memory usage are not allowed, but only if this is the
//   * first write in the context of this script, otherwise we can't stop
//   * in the middle. */
//  if (server.maxmemory && server.lua_write_dirty == 0 &&
//      (cmd->flags & CMD_DENYOOM))
//  {
//    if (freeMemoryIfNeeded() == C_ERR) {
//      luaPushError(lua, shared.oomerr->ptr);
//      goto cleanup;
//    }
//  }
//
//  if (cmd->flags & CMD_RANDOM) server.lua_random_dirty = 1;
//  if (cmd->flags & CMD_WRITE) server.lua_write_dirty = 1;
//
//  /* If this is a Redis Cluster node, we need to make sure Lua is not
//   * trying to access non-local keys, with the exception of commands
//   * received from our master or when loading the AOF back in memory. */
//  if (server.cluster_enabled && !server.loading &&
//      !(server.lua_caller->flags & CLIENT_MASTER))
//  {
//    /* Duplicate relevant flags in the lua client. */
//    c->flags &= ~(CLIENT_READONLY|CLIENT_ASKING);
//    c->flags |= server.lua_caller->flags & (CLIENT_READONLY|CLIENT_ASKING);
//    if (getNodeByQuery(c,c->cmd,c->argv,c->argc,NULL,NULL) !=
//        server.cluster->myself)
//    {
//      luaPushError(lua,
//                   "Lua script attempted to access a non local key in a "
//                   "cluster node");
//      goto cleanup;
//    }
//  }
//
//  /* If we are using single commands replication, we need to wrap what
//   * we propagate into a MULTI/EXEC block, so that it will be atomic like
//   * a Lua script in the context of AOF and slaves. */
//  if (server.lua_replicate_commands &&
//      !server.lua_multi_emitted &&
//      !(server.lua_caller->flags & CLIENT_MULTI) &&
//      server.lua_write_dirty &&
//      server.lua_repl != PROPAGATE_NONE)
//  {
//    execCommandPropagateMulti(server.lua_caller);
//    server.lua_multi_emitted = 1;
//  }
//
//  /* Run the command */
//  int call_flags = CMD_CALL_SLOWLOG | CMD_CALL_STATS;
//  if (server.lua_replicate_commands) {
//    /* Set flags according to redis.set_repl() settings. */
//    if (server.lua_repl & PROPAGATE_AOF)
//      call_flags |= CMD_CALL_PROPAGATE_AOF;
//    if (server.lua_repl & PROPAGATE_REPL)
//      call_flags |= CMD_CALL_PROPAGATE_REPL;
//  }
//  curClient = c;
//  call(c,call_flags);
//  curClient = NULL;
//
//  /* Convert the result of the Redis command into a suitable Lua type.
//   * The first thing we need is to create a single string from the client
//   * output buffers. */
//  if (listLength(c->reply) == 0 && c->bufpos < PROTO_REPLY_CHUNK_BYTES) {
//    /* This is a fast path for the common case of a reply inside the
//     * client static buffer. Don't create an SDS string but just use
//     * the client buffer directly. */
//    c->buf[c->bufpos] = '\0';
//    reply = c->buf;
//    c->bufpos = 0;
//  } else {
//    reply = sdsnewlen(c->buf,c->bufpos);
//    c->bufpos = 0;
//    while(listLength(c->reply)) {
//      sds o = listNodeValue(listFirst(c->reply));
//
//      reply = sdscatsds(reply,o);
//      listDelNode(c->reply,listFirst(c->reply));
//    }
//  }
//  if (raise_error && reply[0] != '-') raise_error = 0;
//  redisProtocolToLuaType(lua,reply);
//
//  /* If the debugger is active, log the reply from Redis. */
//  if (ldb.active && ldb.step)
//    ldbLogRedisReply(reply);
//
//  /* Sort the output array if needed, assuming it is a non-null multi bulk
//   * reply as expected. */
//  if ((cmd->flags & CMD_SORT_FOR_SCRIPT) &&
//      (server.lua_replicate_commands == 0) &&
//      (reply[0] == '*' && reply[1] != '-')) {
//    luaSortArray(lua);
//  }
//  if (reply != c->buf) sdsfree(reply);
//  c->reply_bytes = 0;
//
//  cleanup:
//  /* Clean up. Command code may have changed argv/argc so we use the
//   * argv/argc of the client instead of the local variables. */
//  for (j = 0; j < c->argc; j++) {
//    robj *o = c->argv[j];
//
//    /* Try to cache the object in the cached_objects array.
//     * The object must be small, SDS-encoded, and with refcount = 1
//     * (we must be the only owner) for us to cache it. */
//    if (j < LUA_CMD_OBJCACHE_SIZE &&
//        o->refcount == 1 &&
//        (o->encoding == OBJ_ENCODING_RAW ||
//         o->encoding == OBJ_ENCODING_EMBSTR) &&
//        sdslen(o->ptr) <= LUA_CMD_OBJCACHE_MAX_LEN)
//    {
//      sds s = o->ptr;
//      if (cached_objects[j]) decrRefCount(cached_objects[j]);
//      cached_objects[j] = o;
//      cached_objects_len[j] = sdsalloc(s);
//    } else {
//      decrRefCount(o);
//    }
//  }
//
//  if (c->argv != argv) {
//    zfree(c->argv);
//    argv = NULL;
//    argv_size = 0;
//  }
//
//  if (raise_error) {
//    /* If we are here we should have an error in the stack, in the
//     * form of a table with an "err" field. Extract the string to
//     * return the plain error. */
//    inuse--;
//    return luaRaiseError(lua);
//  }
//  inuse--;
  return 1;
}

/* redis.call() */
int luaRedisCallCommand(lua_State *lua) {
  return luaRedisGenericCommand(lua,1);
}

/* redis.pcall() */
int luaRedisPCallCommand(lua_State *lua) {
  return luaRedisGenericCommand(lua,0);
}

void ScriptManager::luaLoadLib(lua_State *lua, const char *libname, lua_CFunction luafunc) {
  lua_pushcfunction(lua, luafunc);
  lua_pushstring(lua, libname);
  lua_call(lua, 1, 0);
}

extern "C" {
LUALIB_API int (luaopen_cjson)(lua_State *L);
LUALIB_API int (luaopen_struct)(lua_State *L);
LUALIB_API int (luaopen_cmsgpack)(lua_State *L);
LUALIB_API int (luaopen_bit)(lua_State *L);
//LUALIB_API int luaL_loadbuffer (lua_State *L, const char *buff, size_t size,
//    const char *name);
}

void ScriptManager::luaLoadLibraries(lua_State *lua) {
  luaLoadLib(lua, "", luaopen_base);
  luaLoadLib(lua, LUA_TABLIBNAME, luaopen_table);
  luaLoadLib(lua, LUA_STRLIBNAME, luaopen_string);
  luaLoadLib(lua, LUA_MATHLIBNAME, luaopen_math);
  luaLoadLib(lua, LUA_DBLIBNAME, luaopen_debug);
  luaLoadLib(lua, "cjson", luaopen_cjson);
  luaLoadLib(lua, "struct", luaopen_struct);
  luaLoadLib(lua, "cmsgpack", luaopen_cmsgpack);
  luaLoadLib(lua, "bit", luaopen_bit);

#if 0 /* Stuff that we don't load currently, for sandboxing concerns. */
  luaLoadLib(lua, LUA_LOADLIBNAME, luaopen_package);
  luaLoadLib(lua, LUA_OSLIBNAME, luaopen_os);
#endif
}

/* Remove a functions that we don't want to expose to the Redis scripting
 * environment. */
void ScriptManager::luaRemoveUnsupportedFunctions(lua_State *lua) {
  lua_pushnil(lua);
  lua_setglobal(lua,"loadfile");
  lua_pushnil(lua);
  lua_setglobal(lua,"dofile");
}

/* This function installs metamethods in the global table _G that prevent
 * the creation of globals accidentally.
 *
 * It should be the last to be called in the scripting engine initialization
 * sequence, because it may interact with creation of globals. */
void scriptingEnableGlobalsProtection(lua_State *lua) {
  std::string code;

  /* strict.lua from: http://metalua.luaforge.net/src/lib/strict.lua.html.
   * Modified to be adapted to Redis. */
  code+="local dbg=debug\n";
  code+="local mt = {}\n";
  code+="setmetatable(_G, mt)\n";
  code+="mt.__newindex = function (t, n, v)\n";
  code+="  if dbg.getinfo(2) then\n";
  code+="    local w = dbg.getinfo(2, \"S\").what\n";
  code+="    if w ~= \"main\" and w ~= \"C\" then\n";
  code+="      error(\"Script attempted to create global variable '\"..tostring(n)..\"'\", 2)\n";
  code+="    end\n";
  code+="  end\n";
  code+="  rawset(t, n, v)\n";
  code+="end\n";
  code+="mt.__index = function (t, n)\n";
  code+="  if dbg.getinfo(2) and dbg.getinfo(2, \"S\").what ~= \"C\" then\n";
  code+="    error(\"Script attempted to access nonexistent global variable '\"..tostring(n)..\"'\", 2)\n";
  code+="  end\n";
  code+="  return rawget(t, n)\n";
  code+="end\n";
  code+="debug = nil\n";

  luaL_loadbuffer(lua,code.c_str(),code.length(),"@enable_strict_lua");
  lua_pcall(lua,0,0,0);
}

/* Initialize the scripting environment.
*
* This function is called the first time at server startup with
* the 'setup' argument set to 1.
*
* It can be called again multiple times during the lifetime of the Redis
* process, with 'setup' set to 0, and following a scriptingRelease() call,
* in order to reset the Lua scripting environment.
*
* However it is simpler to just call scriptingReset() that does just that. */
void ScriptManager::init(int setup) {
  lua_State *lua = lua_open();

  if (setup) {
//    server.lua_client = NULL;
//    server.lua_caller = NULL;
//    server.lua_timedout = 0;
//    server.lua_always_replicate_commands = 0; /* Only DEBUG can change it.*/
//    ldbInit();
  }

  luaLoadLibraries(lua);
  luaRemoveUnsupportedFunctions(lua);

  /* Initialize a dictionary we use to map SHAs to scripts.
   * This is useful for replication, as we need to replicate EVALSHA
   * as EVAL, so we need to remember the associated script. */
  //server.lua_scripts = dictCreate(&shaScriptObjectDictType,NULL);

  /* Register the redis commands table and fields */
  lua_newtable(lua);

  /* redis.call */
  lua_pushstring(lua,"call");
  lua_pushcfunction(lua,luaRedisCallCommand);
  lua_settable(lua,-3);

  /* redis.pcall */
  lua_pushstring(lua,"pcall");
  lua_pushcfunction(lua,luaRedisPCallCommand);
  lua_settable(lua,-3);

  /* redis.log and log levels. */
//  lua_pushstring(lua,"log");
//  lua_pushcfunction(lua,luaLogCommand);
//  lua_settable(lua,-3);
//
//  lua_pushstring(lua,"LOG_DEBUG");
//  lua_pushnumber(lua,LL_DEBUG);
//  lua_settable(lua,-3);
//
//  lua_pushstring(lua,"LOG_VERBOSE");
//  lua_pushnumber(lua,LL_VERBOSE);
//  lua_settable(lua,-3);
//
//  lua_pushstring(lua,"LOG_NOTICE");
//  lua_pushnumber(lua,LL_NOTICE);
//  lua_settable(lua,-3);
//
//  lua_pushstring(lua,"LOG_WARNING");
//  lua_pushnumber(lua,LL_WARNING);
//  lua_settable(lua,-3);

//  /* redis.sha1hex */
//  lua_pushstring(lua, "sha1hex");
//  lua_pushcfunction(lua, luaRedisSha1hexCommand);
//  lua_settable(lua, -3);
//
//  /* redis.error_reply and redis.status_reply */
//  lua_pushstring(lua, "error_reply");
//  lua_pushcfunction(lua, luaRedisErrorReplyCommand);
//  lua_settable(lua, -3);
//  lua_pushstring(lua, "status_reply");
//  lua_pushcfunction(lua, luaRedisStatusReplyCommand);
//  lua_settable(lua, -3);
//
//  /* redis.replicate_commands */
//  lua_pushstring(lua, "replicate_commands");
//  lua_pushcfunction(lua, luaRedisReplicateCommandsCommand);
//  lua_settable(lua, -3);
//
//  /* redis.set_repl and associated flags. */
//  lua_pushstring(lua,"set_repl");
//  lua_pushcfunction(lua,luaRedisSetReplCommand);
//  lua_settable(lua,-3);

//  lua_pushstring(lua,"REPL_NONE");
//  lua_pushnumber(lua,PROPAGATE_NONE);
//  lua_settable(lua,-3);
//
//  lua_pushstring(lua,"REPL_AOF");
//  lua_pushnumber(lua,PROPAGATE_AOF);
//  lua_settable(lua,-3);
//
//  lua_pushstring(lua,"REPL_SLAVE");
//  lua_pushnumber(lua,PROPAGATE_REPL);
//  lua_settable(lua,-3);
//
//  lua_pushstring(lua,"REPL_ALL");
//  lua_pushnumber(lua,PROPAGATE_AOF|PROPAGATE_REPL);
//  lua_settable(lua,-3);
//
//  /* redis.breakpoint */
//  lua_pushstring(lua,"breakpoint");
//  lua_pushcfunction(lua,luaRedisBreakpointCommand);
//  lua_settable(lua,-3);
//
//  /* redis.debug */
//  lua_pushstring(lua,"debug");
//  lua_pushcfunction(lua,luaRedisDebugCommand);
//  lua_settable(lua,-3);

  /* Finally set the table as 'redis' global var. */
  lua_setglobal(lua,"redis");

  /* Replace math.random and math.randomseed with our implementations. */
//  lua_getglobal(lua,"math");
//
//  lua_pushstring(lua,"random");
//  lua_pushcfunction(lua,redis_math_random);
//  lua_settable(lua,-3);
//
//  lua_pushstring(lua,"randomseed");
//  lua_pushcfunction(lua,redis_math_randomseed);
//  lua_settable(lua,-3);
//
//  lua_setglobal(lua,"math");

  /* Add a helper function that we use to sort the multi bulk output of non
   * deterministic commands, when containing 'false' elements. */
  {
    std::string compare_func =    "function __redis__compare_helper(a,b)\n"
                            "  if a == false then a = '' end\n"
                            "  if b == false then b = '' end\n"
                            "  return a<b\n"
                            "end\n";
    luaL_loadbuffer(lua,compare_func.c_str(),compare_func.length(),"@cmp_func_def");
    lua_pcall(lua,0,0,0);
  }

  /* Add a helper function we use for pcall error reporting.
   * Note that when the error is in the C function we want to report the
   * information about the caller, that's what makes sense from the point
   * of view of the user debugging a script. */
  {
    std::string errh_func =       "local dbg = debug\n"
                            "function __redis__err__handler(err)\n"
                            "  local i = dbg.getinfo(2,'nSl')\n"
                            "  if i and i.what == 'C' then\n"
                            "    i = dbg.getinfo(3,'nSl')\n"
                            "  end\n"
                            "  if i then\n"
                            "    return i.source .. ':' .. i.currentline .. ': ' .. err\n"
                            "  else\n"
                            "    return err\n"
                            "  end\n"
                            "end\n";
    luaL_loadbuffer(lua,errh_func.c_str(),errh_func.length(),"@err_handler_def");
    lua_pcall(lua,0,0,0);
  }

  /* Create the (non connected) client that we use to execute Redis commands
   * inside the Lua interpreter.
   * Note: there is no need to create it again when this function is called
   * by scriptingReset(). */
  //if (server.lua_client == NULL) {
  //  server.lua_client = createClient(-1);
  //  server.lua_client->flags |= CLIENT_LUA;
  //}

  /* Lua beginners often don't use "local", this is likely to introduce
   * subtle bugs in their code. To prevent problems we protect accesses
   * to global variables. */
  scriptingEnableGlobalsProtection(lua);

  _lua = lua;
}

void ScriptManager::sha1hex(char *digest, char *script, size_t len) {
  SHA1_CTX ctx;
  unsigned char hash[20];
  char cset[] = "0123456789abcdef";
  int j;

  SHA1Init(&ctx);
  SHA1Update(&ctx,(unsigned char*)script,len);
  SHA1Final(hash,&ctx);

  for (j = 0; j < 20; j++) {
    digest[j*2] = cset[((hash[j]&0xF0)>>4)];
    digest[j*2+1] = cset[(hash[j]&0xF)];
  }
  digest[40] = '\0';
}

ScriptManager::ScriptManager(std::shared_ptr<ServerEntry> svr)
  :_svr(svr) {
}

Status ScriptManager::startup() {
  LOG(INFO) << "ScriptManager::startup begin";
  init(1);
  return {ErrorCodes::ERR_OK, ""};
}

Status ScriptManager::stopStore(uint32_t storeId) {
  return {ErrorCodes::ERR_OK, ""};
}

void ScriptManager::stop() {
}

}  // namespace tendisplus