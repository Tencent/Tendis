// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <string>
#include <memory>
#include <map>
#include <utility>
#include <list>
#include <limits>
#include <vector>
#include <unordered_set>
#include "glog/logging.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/lock/lock.h"
#include "tendisplus/storage/record.h"
#include "tendisplus/utils/sync_point.h"

namespace tendisplus {

std::mutex Command::_mutex;
mgl::LockMode Command::_expRdLk = mgl::LockMode::LOCK_X;

std::map<std::string, uint64_t> Command::_unSeenCmds = {};

std::map<std::string, Command*>& commandMap() {
  static std::map<std::string, Command*> map = {};
  return map;
}

Command::Command(const std::string& name, const char* sflags)
  : _name(toLower(name)),
    _sflags(sflags),
    _flags(redis_port::getCommandFlags(sflags)) {
  commandMap()[_name] = this;
  if (_flags & CMD_ADMIN && !(_flags & CMD_NOSCRIPT)) {
    std::cerr << name << " command with a flags and dont contain s flags"
      << std::endl;
    INVARIANT_D(0);
  }
}

const std::string& Command::getName() const {
  return _name;
}

void Command::incrCallTimes() {
  _callTimes.fetch_add(1, std::memory_order_relaxed);
}

void Command::incrNanos(uint64_t v) {
  _totalNanoSecs.fetch_add(v, std::memory_order_relaxed);
}

void Command::resetStatInfo() {
  _callTimes = 0;
  _totalNanoSecs = 0;
}

uint64_t Command::getCallTimes() const {
  return _callTimes.load(std::memory_order_relaxed);
}

uint64_t Command::getNanos() const {
  return _totalNanoSecs.load(std::memory_order_relaxed);
}

bool Command::isReadOnly() const {
  return (_flags & CMD_READONLY) != 0;
}

bool Command::isWriteable() const {
  return (_flags & CMD_WRITE) != 0;
}

bool Command::isAdmin() const {
  return (_flags & CMD_ADMIN) != 0;
}

mgl::LockMode Command::RdLock() {
  return _expRdLk;
}

void Command::changeCommand(const string& changeCmdList, string mode) {
  LOG(INFO) << "changeCommand begin,mode:" << mode << " list:" << changeCmdList;
  std::stringstream ssAll(changeCmdList);
  string one;
  while (std::getline(ssAll, one, ',')) {
    std::vector<std::string> kv;
    std::stringstream ssOne(one);
    string temp;
    while (std::getline(ssOne, temp, ' ')) {
      kv.push_back(temp);
    }
    if (kv.size() == 0) {
      continue;
    }
    if (kv.size() != 2) {
      LOG(FATAL) << "changeCommand error rename:" << one;
      continue;
    }
    string& oldname = kv[0];
    string& newname = kv[1];
    if (mode == "rename") {
      if (commandMap().find(oldname) == commandMap().end()) {
        LOG(FATAL) << "changeCommand error mode:" << mode
                   << " oldname not exist:" << one;
        continue;
      }
      if (commandMap().find(newname) != commandMap().end()) {
        LOG(FATAL) << "changeCommand error mode:" << mode
                   << " newname allready exist:" << one;
        continue;
      }
      commandMap()[newname] = commandMap()[oldname];
      commandMap().erase(oldname);
      LOG(INFO) << "changeCommand ok mode:" << mode << " cmd:" << one;
    } else if (mode == "mapping") {
      if (commandMap().find(oldname) == commandMap().end()) {
        LOG(FATAL) << "changeCommand error mode:" << mode
                   << " oldname not exist:" << one;
        continue;
      }
      if (commandMap().find(newname) == commandMap().end()) {
        LOG(FATAL) << "changeCommand error mode:" << mode
                   << " newname not exist:" << one;
        continue;
      }
      commandMap()[oldname] = commandMap()[newname];
      LOG(INFO) << "changeCommand ok mode:" << mode << " cmd:" << one;
    }
  }
}

bool Command::isMultiKey() const {
  return lastkey() != firstkey() && firstkey() != 0;
}

int Command::getFlags() const {
  return _flags;
}

size_t Command::getFlagsCount() const {
  static std::vector<int> flagarr = {CMD_WRITE,
                                     CMD_READONLY,
                                     CMD_DENYOOM,
                                     CMD_MODULE,
                                     CMD_ADMIN,
                                     CMD_PUBSUB,
                                     CMD_NOSCRIPT,
                                     CMD_RANDOM,
                                     CMD_SORT_FOR_SCRIPT,
                                     CMD_LOADING,
                                     CMD_STALE,
                                     CMD_SKIP_MONITOR,
                                     CMD_ASKING,
                                     CMD_FAST,
                                     CMD_MODULE_GETKEYS,
                                     CMD_MODULE_NO_CLUSTER};

  size_t size = 0;
  for (auto flag : flagarr) {
    if (_flags & flag) {
      size++;
    }
  }

  return size;
}

std::vector<std::string> Command::listCommands() {
  std::vector<std::string> lst;
  const auto& v = commandMap();
  for (const auto& kv : v) {
    lst.push_back(kv.first);
  }
  return lst;
}

Command* Command::getCommand(Session* sess) {
  const auto& args = sess->getArgs();
  if (args.size() == 0) {
    return nullptr;
  }
  std::string commandName = toLower(args[0]);
  auto it = commandMap().find(commandName);
  if (it == commandMap().end()) {
    return nullptr;
  }
  return it->second;
}

Expected<Command*> Command::precheck(Session* sess) {
  const auto& args = sess->getArgs();
  if (args.size() == 0) {
    LOG(FATAL) << "BUG: sess " << sess->id() << " len 0 args";
  }
  std::string commandName = toLower(args[0]);
  auto it = commandMap().find(commandName);
  if (it == commandMap().end()) {
    {
      std::lock_guard<std::mutex> lk(_mutex);
      if (_unSeenCmds.find(commandName) == _unSeenCmds.end()) {
        if (_unSeenCmds.size() < _maxUnseenCmdNum) {
          _unSeenCmds[commandName] = 1;
        } else {
          LOG(ERROR) << "Command::precheck _unSeenCmds is full:"
                     << _maxUnseenCmdNum << ", unseenCmd:" << args[0];
        }
      } else {
        _unSeenCmds[commandName] += 1;
      }
      LOG(ERROR) << "Command::precheck unseenCmd:" << args[0]
                 << ", unseenCmdNum:" << _unSeenCmds.size();
    }
    std::stringstream ss;
    ss << "unknown command '" << args[0] << "'";
    return {ErrorCodes::ERR_PARSEPKT, ss.str()};
  }
  if (!isAdminCmd(commandName)) {
    auto s = sess->processExtendProtocol();
    if (!s.ok()) {
      return s;
    }
  }
  ssize_t arity = it->second->arity();
  if ((arity > 0 && arity != ssize_t(args.size())) ||
      ssize_t(args.size()) < -arity) {
    std::stringstream ss;
    ss << "wrong number of arguments for '" << args[0] << "' command";
    return {ErrorCodes::ERR_WRONG_ARGS_SIZE, ss.str()};
  }

  auto server = sess->getServerEntry();
  if (!server) {
    LOG(FATAL) << "BUG: get server from sess:" << sess->id()
               << ",Ip:" << sess->id() << " empty";
  }

  SessionCtx* pCtx = sess->getCtx();
  INVARIANT(pCtx != nullptr);
  bool authed = pCtx->authed();
  if (!authed && server->requirepass() != "" &&
      it->second->getName() != "auth") {
    return {ErrorCodes::ERR_AUTH, "-NOAUTH Authentication required.\r\n"};
  }

  return it->second;
}

// NOTE(deyukong): call precheck before call runSessionCmd
// this function does no necessary checks
Expected<std::string> Command::runSessionCmd(Session* sess) {
  const auto& args = sess->getArgs();
  std::string commandName = toLower(args[0]);
  auto it = commandMap().find(commandName);
  if (it == commandMap().end()) {
    LOG(FATAL) << "BUG: command:" << args[0] << " not found!";
  }

  // TODO(vinchen): here there is a copy, it is a waste.
  sess->getCtx()->setArgsBrief(sess->getArgs());
  it->second->incrCallTimes();
  auto now = nsSinceEpoch();
  auto guard = MakeGuard([it, now, sess, commandName] {
    sess->getCtx()->clearRequestCtx();
    auto duration = nsSinceEpoch() - now;
    it->second->incrNanos(duration);
    sess->getServerEntry()->slowlogPushEntryIfNeeded(
      now / 1000, duration / 1000, sess);
  });
  auto v = it->second->run(sess);
  if (v.ok()) {
    if (sess->getCtx()->isEp()) {
      sess->getServerEntry()->setTsEp(sess->getCtx()->getTsEP());
    }
  } else {
    if (sess->getCtx()->isReplOnly()) {
      // NOTE(vinchen): If it's a slave, the connection should be closed
      // when there is an error. And the error should be log
      ServerEntry::logError(v.status().toString(), sess);
      auto vv = dynamic_cast<NetSession*>(sess);
      if (vv) {
        vv->setCloseAfterRsp();
      }
    } else if (v.status().code() == ErrorCodes::ERR_INTERNAL ||
               v.status().code() == ErrorCodes::ERR_DECODE ||
               v.status().code() == ErrorCodes::ERR_LOCK_TIMEOUT) {
      ServerEntry::logError(v.status().toString(), sess);
    }
  }
  return v;
}

// should be called with store locked
// bool Command::isKeyLocked(Session *sess,
//                           uint32_t storeId,
//                           const std::string& encodedKey) {
//     auto server = sess->getServerEntry();
//     INVARIANT(server != nullptr);
//     auto pessimisticMgr = server->getPessimisticMgr();
//     INVARIANT(pessimisticMgr != nullptr);
//     auto shard = pessimisticMgr->getShard(storeId);
//     return shard->isLocked(encodedKey);
// }

// requirement: intentionlock held
Status Command::delKeyPessimisticInLock(Session* sess,
                                        uint32_t storeId,
                                        const RecordKey& mk,
                                        RecordType valueType,
                                        const TTLIndex* ictx) {
  std::string keyEnc = mk.encode();
  auto server = sess->getServerEntry();

  DLOG(INFO) << "begin delKeyPessimistic key:" << hexlify(mk.getPrimaryKey());

  auto expdb =
    server->getSegmentMgr()->getDb(sess, storeId, mgl::LockMode::LOCK_NONE);
  if (!expdb.ok()) {
    return expdb.status();
  }
  // NOTE(takenliu): use kvstore->createTransaction interface,
  //   and commit in this function.
  PStore kvstore = expdb.value().store;
  auto ptxn = kvstore->createTransaction(sess);
  if (!ptxn.ok()) {
    return ptxn.status();
  }
  std::unique_ptr<Transaction> txn = std::move(ptxn.value());

  Expected<string> ret = delSubkeysRange(
          sess, storeId, mk, valueType, txn.get());
  if (!ret.ok()) {
    return ret.status();
  }

  Status s = kvstore->delKV(mk, txn.get());
  if (!s.ok()) {
    return s;
  }

  if (ictx && ictx->getType() != RecordType::RT_KV) {
    Status s = txn->delKV(ictx->encode());
    if (!s.ok()) {
      return s;
    }
  }

  Expected<uint64_t> commitStatus = txn->commit();
  return commitStatus.status();
}

Expected<std::pair<std::string, std::list<Record>>> Command::scan(
  const std::string& pk,
  const std::string& from,
  uint64_t cnt,
  Transaction* txn) {
  auto cursor = txn->createDataCursor();
  if (from == "0") {
    cursor->seek(pk);
  } else {
    auto unhex = unhexlify(from);
    if (!unhex.ok()) {
      return unhex.status();
    }
    cursor->seek(unhex.value());
  }
  std::list<Record> result;
  while (true) {
    if (result.size() >= cnt + 1) {
      break;
    }
    Expected<Record> exptRcd = cursor->next();
    if (exptRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
      break;
    }
    if (!exptRcd.ok()) {
      return exptRcd.status();
    }
    Record& rcd = exptRcd.value();
    const RecordKey& rcdKey = rcd.getRecordKey();
    if (rcdKey.prefixPk() != pk) {
      break;
    }
    result.emplace_back(std::move(exptRcd.value()));
  }
  std::string nextCursor;
  if (result.size() == cnt + 1) {
    nextCursor = hexlify(result.back().getRecordKey().encode());
    result.pop_back();
  } else {
    nextCursor = "0";
  }
  return std::move(
    std::pair<std::string, std::list<Record>>(nextCursor, std::move(result)));
}

// requirement: intentionlock held
Status Command::delKeyOptimismInLock(Session* sess,
                                     uint32_t storeId,
                                     const RecordKey& rk,
                                     RecordType valueType,
                                     Transaction* txn,
                                     const TTLIndex* ictx) {
  auto s = Command::partialDelSubKeys(sess,
                                      storeId,
                                      std::numeric_limits<uint32_t>::max(),
                                      rk,
                                      valueType,
                                      true,
                                      txn,
                                      ictx);
  return s.status();
}

Expected<string> Command::delSubkeysRange(Session* sess,
                                          uint32_t storeId,
                                          const RecordKey& mk,
                                          RecordType valueType,
                                          Transaction* txn) {
  Status s(ErrorCodes::ERR_OK, "");
  auto guard = MakeGuard([&s] {
    if (!s.ok()) {
      INVARIANT_D(0);
    }
  });

  auto server = sess->getServerEntry();
  INVARIANT(server != nullptr);
  auto expdb = server->getSegmentMgr()->getDb(nullptr, storeId,
                  mgl::LockMode::LOCK_NONE);
  RET_IF_ERR_EXPECTED(expdb);

  PStore kvstore = expdb.value().store;
  INVARIANT_D(mk.getRecordType() == RecordType::RT_DATA_META);
  if (valueType == RecordType::RT_KV) {
    INVARIANT_D(0);
    return {ErrorCodes::ERR_WRONG_TYPE, ""};
  }
  std::vector<RecordKey> prefixes;
  if (valueType == RecordType::RT_HASH_META) {
    RecordKey start(mk.getChunkId(),
                    mk.getDbId(),
                    RecordType::RT_HASH_ELE,
                    mk.getPrimaryKey(),
                    "",
                    0);
    RecordKey end(mk.getChunkId(),
                  mk.getDbId(),
                  RecordType::RT_HASH_ELE,
                  mk.getPrimaryKey(),
                  "",
                  UINT64_MAX);
    prefixes.push_back(start);
    prefixes.push_back(end);
  } else if (valueType == RecordType::RT_LIST_META) {
    RecordKey start(mk.getChunkId(),
                    mk.getDbId(),
                    RecordType::RT_LIST_ELE,
                    mk.getPrimaryKey(),
                    "",
                    0);
    RecordKey end(mk.getChunkId(),
                  mk.getDbId(),
                  RecordType::RT_LIST_ELE,
                  mk.getPrimaryKey(),
                  "",
                  UINT64_MAX);
    prefixes.push_back(start);
    prefixes.push_back(end);
  } else if (valueType == RecordType::RT_SET_META) {
    RecordKey start(mk.getChunkId(),
                    mk.getDbId(),
                    RecordType::RT_SET_ELE,
                    mk.getPrimaryKey(),
                    "",
                    0);
    RecordKey end(mk.getChunkId(),
                  mk.getDbId(),
                  RecordType::RT_SET_ELE,
                  mk.getPrimaryKey(),
                  "",
                  UINT64_MAX);
    prefixes.push_back(start);
    prefixes.push_back(end);
  } else if (valueType == RecordType::RT_ZSET_META) {
    RecordKey start(mk.getChunkId(),
                    mk.getDbId(),
                    RecordType::RT_ZSET_S_ELE,
                    mk.getPrimaryKey(),
                    "",
                    0);
    RecordKey end(mk.getChunkId(),
                  mk.getDbId(),
                  RecordType::RT_ZSET_S_ELE,
                  mk.getPrimaryKey(),
                  "",
                  UINT64_MAX);
    prefixes.push_back(start);
    prefixes.push_back(end);
    RecordKey startH(mk.getChunkId(),
                     mk.getDbId(),
                     RecordType::RT_ZSET_H_ELE,
                     mk.getPrimaryKey(),
                     "",
                     0);
    RecordKey endH(mk.getChunkId(),
                   mk.getDbId(),
                   RecordType::RT_ZSET_H_ELE,
                   mk.getPrimaryKey(),
                   "",
                   UINT64_MAX);
    prefixes.push_back(startH);
    prefixes.push_back(endH);
  } else {
    INVARIANT_D(0);
  }

  // NOTE(takenliu): deleteRange and delete meta is not in the same txn.
  for (uint32_t i = 0; i < prefixes.size(); i+=2) {
    string start = prefixes[i].prefixPk();
    string end = prefixes[i+1].prefixPk();
    auto s = kvstore->deleteRange(start, end);
    if (!s.ok()) {
      LOG(ERROR) << "delSubkeysRange::deleteRange commit failed, start:"
                 << start << " end:" << end << " err:" << s.toString();
      return s;
    }
  }

  return {ErrorCodes::ERR_OK, ""};
}

Expected<uint32_t> Command::partialDelSubKeys(Session* sess,
                                              uint32_t storeId,
                                              uint32_t subCount,
                                              const RecordKey& mk,
                                              RecordType valueType,
                                              bool deleteMeta,
                                              Transaction* txn,
                                              const TTLIndex* ictx) {
  Status s(ErrorCodes::ERR_OK, "");
  auto guard = MakeGuard([&s] {
    if (!s.ok()) {
      INVARIANT_D(0);
    }
  });
  if (deleteMeta && subCount != std::numeric_limits<uint32_t>::max()) {
    s = Status{ErrorCodes::ERR_PARSEOPT, "delmeta with limited subcount"};
    return s;
  }
  auto server = sess->getServerEntry();
  INVARIANT(server != nullptr);
  auto expdb =
    server->getSegmentMgr()->getDb(nullptr, storeId, mgl::LockMode::LOCK_NONE);
  RET_IF_ERR_EXPECTED(expdb);

  PStore kvstore = expdb.value().store;
  INVARIANT_D(mk.getRecordType() == RecordType::RT_DATA_META);
  if (valueType == RecordType::RT_KV) {
    s = kvstore->delKV(mk, txn);
    RET_IF_ERR(s);

    return 1;
  }
  std::vector<std::string> prefixes;
  if (valueType == RecordType::RT_HASH_META) {
    RecordKey fakeEle(mk.getChunkId(),
                      mk.getDbId(),
                      RecordType::RT_HASH_ELE,
                      mk.getPrimaryKey(),
                      "");
    prefixes.push_back(fakeEle.prefixPk());
  } else if (valueType == RecordType::RT_LIST_META) {
    RecordKey fakeEle(mk.getChunkId(),
                      mk.getDbId(),
                      RecordType::RT_LIST_ELE,
                      mk.getPrimaryKey(),
                      "");
    prefixes.push_back(fakeEle.prefixPk());
  } else if (valueType == RecordType::RT_SET_META) {
    RecordKey fakeEle(mk.getChunkId(),
                      mk.getDbId(),
                      RecordType::RT_SET_ELE,
                      mk.getPrimaryKey(),
                      "");
    prefixes.push_back(fakeEle.prefixPk());
  } else if (valueType == RecordType::RT_ZSET_META) {
    RecordKey fakeEle(mk.getChunkId(),
                      mk.getDbId(),
                      RecordType::RT_ZSET_S_ELE,
                      mk.getPrimaryKey(),
                      "");
    prefixes.push_back(fakeEle.prefixPk());
    RecordKey fakeEle1(mk.getChunkId(),
                       mk.getDbId(),
                       RecordType::RT_ZSET_H_ELE,
                       mk.getPrimaryKey(),
                       "");
    prefixes.push_back(fakeEle1.prefixPk());
  } else {
    INVARIANT_D(0);
  }

  std::list<RecordKey> pendingDelete;
  for (const auto& prefix : prefixes) {
    auto cursor = txn->createDataCursor();
    cursor->seek(prefix);

    while (true) {
      if (pendingDelete.size() >= subCount) {
        break;
      }
      Expected<Record> exptRcd = cursor->next();
      if (exptRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
        break;
      }
      RET_IF_ERR_EXPECTED(exptRcd);

      Record& rcd = exptRcd.value();
      const RecordKey& rcdKey = rcd.getRecordKey();
      if (rcdKey.prefixPk() != prefix) {
        break;
      }
      pendingDelete.push_back(rcdKey);
    }
  }

  if (deleteMeta) {
    pendingDelete.push_back(mk);
  }
  for (auto& v : pendingDelete) {
    s = kvstore->delKV(v, txn);
    RET_IF_ERR(s);
  }

  if (ictx && ictx->getType() != RecordType::RT_KV) {
    s = txn->delKV(ictx->encode());
    RET_IF_ERR(s);
  }

  return pendingDelete.size();
}

// not comitted, caller need commit itself.
Expected<bool> Command::delKeyChkExpire(Session* sess,
                                        const std::string& key,
                                        RecordType tp,
                                        Transaction* txn) {
  Expected<RecordValue> rv = Command::expireKeyIfNeeded(sess, key, tp);
  if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
    return false;
  } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
    return false;
  } else if (!rv.status().ok()) {
    return rv.status();
  }

  // key exists and not expired, now we delete it
  Status s = Command::delKey(sess, key, tp, txn);
  if (s.code() == ErrorCodes::ERR_NOTFOUND) {
    return false;
  }
  if (s.ok()) {
    return true;
  }
  return s;
}

// del meta and it's ttlindex
Status Command::delKeyAndTTL(Session* sess,
                             const RecordKey& mk,
                             const RecordValue& val,
                             Transaction* txn) {
  Status s(ErrorCodes::ERR_OK, "");
  s = txn->delKV(mk.encode());
  if (!s.ok()) {
    return s;
  }

  if (val.getTtl() > 0 && val.getRecordType() != RecordType::RT_KV) {
    TTLIndex ictx(mk.getPrimaryKey(),
                  val.getRecordType(),
                  sess->getCtx()->getDbId(),
                  val.getTtl());
    s = txn->delKV(ictx.encode());
    if (!s.ok()) {
      return s;
    }
  }
  return s;
}

// not comitted, caller need commit itself.
Status Command::delKey(Session* sess, const std::string& key, RecordType tp,
        Transaction* txn) {
  auto server = sess->getServerEntry();
  INVARIANT(server != nullptr);
  SessionCtx* pCtx = sess->getCtx();
  INVARIANT(pCtx != nullptr);

  auto expdb =
    server->getSegmentMgr()->getDbWithKeyLock(sess, key, mgl::LockMode::LOCK_X);
  if (!expdb.ok()) {
    return expdb.status();
  }
  PStore kvstore = expdb.value().store;
  uint32_t storeId = expdb.value().dbId;

  RecordKey mk(expdb.value().chunkId, pCtx->getDbId(), tp, key, "");

  for (uint32_t i = 0; i < RETRY_CNT; ++i) {
    Expected<RecordValue> eValue = kvstore->getKV(mk, txn);
    if (!eValue.ok()) {
      return eValue.status();
    }
    RecordType valueType = eValue.value().getRecordType();
    auto cnt = rcd_util::getSubKeyCount(mk, eValue.value());
    if (!cnt.ok()) {
      return cnt.status();
    }

    TTLIndex ictx(
      key, valueType, sess->getCtx()->getDbId(), eValue.value().getTtl());
    if (cnt.value() >= 2048 ||
        (valueType == RecordType::RT_ZSET_META && cnt.value() >= 1024)) {
      LOG(INFO) << "bigkey delete:" << hexlify(mk.getPrimaryKey())
                << ",rcdType:" << rt2Char(valueType) << ",size:" << cnt.value();
      return Command::delKeyPessimisticInLock(
        sess, storeId, mk, valueType, ictx.getTTL() > 0 ? &ictx : nullptr);
    } else {
      Status s =
        Command::delKeyOptimismInLock(sess,
                                      storeId,
                                      mk,
                                      valueType,
                                      txn,
                                      ictx.getTTL() > 0 ? &ictx : nullptr);
      if (s.code() == ErrorCodes::ERR_COMMIT_RETRY && i != RETRY_CNT - 1) {
        continue;
      }
      return s;
    }
  }
  // should never reach here
  INVARIANT_D(0);
  return {ErrorCodes::ERR_INTERNAL, "not reachable"};
}

// NOTE(takenliu) txn is committed in this function.
Expected<RecordValue> Command::expireKeyIfNeeded(Session* sess,
                                                 const std::string& key,
                                                 RecordType tp,
                                                 bool hasVersion) {
  auto server = sess->getServerEntry();
  INVARIANT(server != nullptr);
  auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key, RdLock());
  if (!expdb.ok()) {
    return expdb.status();
  }
  uint32_t storeId = expdb.value().dbId;

  // NOTE(wayenchen) change session args to store a del command in session
  LocalSessionGuard sg(server, sess);
  if (sess->getArgs().size() >= 2) {
    sg.getSession()->setArgs({"del", key});
  }

  RecordKey mk(expdb.value().chunkId, sess->getCtx()->getDbId(), tp, key, "");
  PStore kvstore = expdb.value().store;
  for (uint32_t i = 0; i < RETRY_CNT; ++i) {
    // NOTE(takenliu) expireKeyIfNeeded don't use txn from params,
    //   because it need rewrite codes too much.
    //   so, we need new txn and commit txn in this function,
    //   and then, we can't use sess->createTransaction
    auto ptxn = kvstore->createTransaction(sg.getSession());
    if (!ptxn.ok()) {
      return ptxn.status();
    }
    std::unique_ptr<Transaction> txn = std::move(ptxn.value());
    Expected<RecordValue> eValue = kvstore->getKV(mk, txn.get());
    if (!eValue.ok()) {
      // maybe ErrorCodes::ERR_NOTFOUND
      ++sess->getServerEntry()->getServerStat().keyspaceMisses;
      return eValue.status();
    }

    uint64_t currentTs = msSinceEpoch();
    uint64_t targetTtl = eValue.value().getTtl();
    RecordType valueType = eValue.value().getRecordType();
    if (server->getParams()->noexpire || targetTtl == 0 ||
        currentTs < targetTtl) {
      if (valueType != tp && tp != RecordType::RT_DATA_META) {
        return {ErrorCodes::ERR_WRONG_TYPE, ""};
      }
      if (hasVersion) {
        auto pCtx = sess->getCtx();
        if (!pCtx->verifyVersion(eValue.value().getVersionEP())) {
          ++sess->getServerEntry()->getServerStat().keyspaceIncorrectEp;
          return {ErrorCodes::ERR_WRONG_VERSION_EP, ""};
        }
      }
      ++sess->getServerEntry()->getServerStat().keyspaceHits;
      return eValue.value();
    } else if (txn->isReplOnly()) {
      // NOTE(vinchen): if replOnly, it can't delete record, but return
      // ErrorCodes::ERR_EXPIRED
      return {ErrorCodes::ERR_EXPIRED, ""};
    }
    auto cnt = rcd_util::getSubKeyCount(mk, eValue.value());
    if (!cnt.ok()) {
      return cnt.status();
    }

    TTLIndex ictx(key, valueType, sess->getCtx()->getDbId(), targetTtl);
    if (cnt.value() >= 2048) {
      LOG(INFO) << "bigkey delete:" << hexlify(mk.getPrimaryKey())
                << ",rcdType:" << rt2Char(valueType) << ",size:" << cnt.value();
      Status s = Command::delKeyPessimisticInLock(
        sg.getSession(), storeId, mk, valueType, &ictx);
      if (s.ok()) {
        return {ErrorCodes::ERR_EXPIRED, ""};
      } else {
        return s;
      }
    } else {
      Status s = Command::delKeyOptimismInLock(
        sg.getSession(), storeId, mk, valueType, txn.get(), &ictx);
      if (!s.ok()) {
        return s;
      }
      auto eCmt = txn.get()->commit();
      if (!eCmt.ok()) {
        return eCmt.status();
      }
      return {ErrorCodes::ERR_EXPIRED, ""};
    }
  }
  // should never reach here
  INVARIANT_D(0);
  return {ErrorCodes::ERR_INTERNAL, "not reachable"};
}

std::string Command::fmtErr(const std::string& s) {
  if (s.size() != 0 && s[0] == '-') {
    return s;
  }
  std::stringstream ss;
  ss << "-ERR " << s << "\r\n";
  return ss.str();
}

std::string Command::fmtNull() {
  return "$-1\r\n";
}

std::string Command::fmtOK() {
  return "+OK\r\n";
}

std::string Command::fmtOne() {
  return ":1\r\n";
}

std::string Command::fmtZero() {
  return ":0\r\n";
}

std::string Command::fmtLongLong(int64_t v) {
  std::stringstream ss;
  ss << ":" << v << "\r\n";
  return ss.str();
}

Expected<uint64_t> Command::getInt64FromFmtLongLong(const std::string& str) {
  if (str[0] != ':') {
    return {ErrorCodes::ERR_INTERGER, "not a fmtLongLong"};
  }

  size_t end = str.find('\r');
  if (end == std::string::npos) {
    return {ErrorCodes::ERR_INTERGER, "not a fmtLongLong"};
  }

  std::string s(str.c_str() + 1, end - 1);
  return tendisplus::stoull(s);
}


std::string Command::fmtBusyKey() {
  return "-BUSYKEY Target key name already exists.\r\n";
}

std::string Command::fmtZeroBulkLen() {
  return "*0\r\n";
}

std::stringstream& Command::fmtMultiBulkLen(std::stringstream& ss, uint64_t l) {
  ss << "*" << l << "\r\n";
  return ss;
}

std::stringstream& Command::fmtBulk(std::stringstream& ss,
                                    const std::string& s) {
  ss << "$" << s.size() << "\r\n";
  ss.write(s.c_str(), s.size());
  ss << "\r\n";
  return ss;
}

std::stringstream& Command::fmtNull(std::stringstream& ss) {
  ss << "$-1\r\n";
  return ss;
}

std::stringstream& Command::fmtLongLong(std::stringstream& ss, int64_t v) {
  ss << ":" << v << "\r\n";
  return ss;
}

std::string Command::fmtBulk(const std::string& s) {
  std::stringstream ss;
  ss << "$" << s.size() << "\r\n";
  ss.write(s.c_str(), s.size());
  ss << "\r\n";
  return ss.str();
}

std::string Command::fmtStatus(const std::string& s) {
  std::stringstream ss;
  ss << "+";
  ss.write(s.c_str(), s.size());
  ss << "\r\n";
  return ss.str();
}

std::stringstream& Command::fmtStatus(std::stringstream& ss,
                                      const std::string& s) {
  ss << "+";
  ss.write(s.c_str(), s.size());
  ss << "\r\n";
  return ss;
}

std::vector<int> Command::getKeysFromCommand(
  const std::vector<std::string>& argv) {
  int argc = argv.size();
  std::vector<int> keyindex;

  int32_t first = firstkey();
  if (first == 0) {
    return keyindex;
  }
  int32_t last = lastkey();
  if (last < 0) {
    last = argc + last;
  }

  for (int i = first; i <= last; i += keystep()) {
    INVARIANT_D(i <= argc);
    keyindex.push_back(i);
  }

  return keyindex;
}

bool Command::isAdminCmd(const std::string& cmd) {
  static const auto sAdmin = []() {
    std::unordered_set<std::string> tmp;
    for (auto iter = commandMap().begin(); iter != commandMap().end(); iter++) {
      if (iter->second->isAdmin()) {
        tmp.emplace(iter->first);
      }
    }
    return tmp;
  }();

  return sAdmin.count(cmd);
}

}  // namespace tendisplus
