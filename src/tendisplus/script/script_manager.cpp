// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.


#include <memory>
#include <string>
#include <shared_mutex>
#include <utility>
#include <vector>
#include "tendisplus/script/script_manager.h"

namespace tendisplus {

ScriptManager::ScriptManager(std::shared_ptr<ServerEntry> svr)
  : _svr(std::move(svr)),
    _luaKill(false),
    _stopped(false) {}

std::shared_ptr<LuaState> ScriptManager::getLuaStateBelongToThisThread() {
  std::shared_ptr<LuaState> luaState;
  const auto& threadid = getCurThreadId();
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
    LOG(INFO) << "new LuaState, threadid: " << threadid
              << " _mapLuaState size: " << _mapLuaState.size();
  }
  return luaState;
}

Expected<std::string> ScriptManager::run(Session* sess, int evalsha) {
  // NOTE(takenliu):
  //   use shared_lock in every command with high frequency,
  //   otherwise use unique_lock with low frequency.
  if (_luaKill) {
    std::unique_lock<std::shared_timed_mutex> lock(_mutex);
    for (const auto& iter : _mapLuaState) {
      if (iter.second->isRunning()) {
        LOG(WARNING) << "script kill or flush not finished: "
                     << _mapLuaState.size();
        return {ErrorCodes::ERR_LUA, "script kill not finished."};
      }
    }
    LOG(WARNING) << "script kill or flush all finished.";
    _luaKill = false;
  }
  auto luaState = getLuaStateBelongToThisThread();
  auto ret = evalsha == 0 ?
             luaState->evalCommand(sess) :
             luaState->evalShaCommand(sess);
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
  for (const auto& iter : _mapLuaState) {
    if (iter.second->isRunning()) {
      someRunning = true;
      break;
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

Expected<std::string> ScriptManager::flush(Session* sess) {
  std::unique_lock<std::shared_timed_mutex> lock(_mutex);

  // check if there are scripts still running.
  for (const auto& iter : _mapLuaState) {
    if (iter.second->isRunning()) {
      return {ErrorCodes::ERR_LUA,
              "-BUSY Redis is busy running a script."
              " You can only call SCRIPT KILL or SHUTDOWN NOSAVE.\r\n"};
    }
  }

  // delete all lua scripts in kvstore and add deleterange binlog.
  auto expdb = _svr->getSegmentMgr()->getDb(
    sess, LUASCRIPT_DEFAULT_DBID, mgl::LockMode::LOCK_IX);
  RET_IF_ERR_EXPECTED(expdb);
  auto kvstore = expdb.value().store;
  /*
   * every valid sha record key must have sha code (40 bytes),
   * so a recordkey with empty sha code is a suitable lowerbound of
   * deleterange, and a recordkey with LuaScript::CHUNKID+1 as CHUNKID should
   * bigger than any sha record key, which is a suitable upperbound.
   */
  RecordKey min_sha_rk(
    LuaScript::CHUNKID, LuaScript::DBID, RecordType::RT_META, "", "");
  RecordKey max_sha_rk(
    LuaScript::CHUNKID + 1, LuaScript::DBID, RecordType::RT_META, "", "");
  auto s = kvstore->deleteRange(min_sha_rk.prefixChunkid(),
                                max_sha_rk.prefixChunkid());
  RET_IF_ERR(s);
  LOG(INFO) << "script flush done.";

  // stop and reset all LuaState to clear script cache in lua vm.
  _mapLuaState.clear();
  return Command::fmtOK();
}

Expected<std::string> ScriptManager::getScriptContent(Session* sess,
                                                      const std::string& sha) {
  auto expdb = _svr->getSegmentMgr()->getDb(
    sess, LUASCRIPT_DEFAULT_DBID, mgl::LockMode::LOCK_IS);
  RET_IF_ERR_EXPECTED(expdb);
  auto& kvstore = expdb.value().store;
  auto ptxn = kvstore->createTransaction(sess);
  RET_IF_ERR_EXPECTED(ptxn);
  auto& txn = ptxn.value();
  RecordKey rk(
    LuaScript::CHUNKID, LuaScript::DBID, RecordType::RT_META, sha, "");
  auto expRv = kvstore->getKV(rk, txn.get());
  RET_IF_ERR_EXPECTED(expRv);
  return expRv.value().getValue();
}

Expected<std::string> ScriptManager::saveLuaScript(Session* sess,
                                                   const std::string& sha,
                                                   const std::string& script) {
  auto luaState = getLuaStateBelongToThisThread();
  auto expLoadState = luaState->tryLoadLuaScript(script);
  RET_IF_ERR_EXPECTED(expLoadState);
  {
    std::shared_lock<std::shared_timed_mutex> lock(_mutex);
    luaState->setRunning(false);
  }
  auto expdb = _svr->getSegmentMgr()->getDb(
    sess, LUASCRIPT_DEFAULT_DBID, mgl::LockMode::LOCK_IX);
  RET_IF_ERR_EXPECTED(expdb);
  auto kvstore = expdb.value().store;
  auto ptxn = kvstore->createTransaction(sess);
  RET_IF_ERR_EXPECTED(ptxn);
  auto& txn = ptxn.value();
  // if sha is empty, calculate sha code for script first.
  std::string tmpSha;
  if (sha.empty()) {
    tmpSha = LuaState::getShaEncode(script);
  } else {
    tmpSha = toLower(sha);
  }
  RecordKey rk(
    LuaScript::CHUNKID, LuaScript::DBID, RecordType::RT_META, tmpSha, "");
  RecordValue rv(script, RecordType::RT_META, -1);
  auto s = kvstore->setKV(rk, rv, txn.get());
  RET_IF_ERR(s);
  auto commitStatus = txn->commit();
  RET_IF_ERR_EXPECTED(commitStatus);

  return Command::fmtBulk(tmpSha);
}

Expected<std::string> ScriptManager::checkIfScriptExists(Session* sess) {
  const std::vector<std::string>& args = sess->getArgs();
  auto expdb = _svr->getSegmentMgr()->getDb(
    sess, LUASCRIPT_DEFAULT_DBID, mgl::LockMode::LOCK_IS);
  RET_IF_ERR_EXPECTED(expdb);
  auto kvstore = expdb.value().store;
  auto ptxn = kvstore->createTransaction(sess);
  RET_IF_ERR_EXPECTED(ptxn);
  auto& txn = ptxn.value();
  std::stringstream ss;
  Command::fmtMultiBulkLen(ss, args.size() - 2);
  for (uint32_t i = 2; i < args.size(); ++i) {
    auto tmpSha = toLower(args[i]);
    RecordKey rk(
      LuaScript::CHUNKID, LuaScript::DBID, RecordType::RT_META, tmpSha, "");
    auto expStr = kvstore->getKV(rk, txn.get());
    Command::fmtLongLong(ss, expStr.ok() ? 1 : 0);
  }
  return ss.str();
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
  _stopped.store(true, std::memory_order_relaxed);
}

}  // namespace tendisplus
