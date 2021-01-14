// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_COMMANDS_COMMAND_H_
#define SRC_TENDISPLUS_COMMANDS_COMMAND_H_

#include <string>
#include <map>
#include <memory>
#include <vector>
#include <list>
#include <utility>
#include "tendisplus/utils/status.h"
#include "tendisplus/server/session.h"
#include "tendisplus/network/session_ctx.h"
#include "tendisplus/lock/lock.h"
#include "tendisplus/storage/kvstore.h"
#include "tendisplus/server/server_entry.h"

namespace tendisplus {

class Command {
 public:
  using CmdMap = std::map<std::string, Command*>;
  explicit Command(const std::string& name, const char* sflags);
  virtual ~Command() = default;
  virtual Expected<std::string> run(Session* sess) = 0;

  // if arity() > 0, it means the arguments count must equal to arity();
  // else, it means the arguments count must bigger than -arity();
  virtual ssize_t arity() const = 0;
  virtual int32_t firstkey() const = 0;
  virtual int32_t lastkey() const = 0;
  virtual int32_t keystep() const = 0;
  virtual bool sameWithRedis() const {
    return true;
  }
  virtual bool isBgCmd() const {
    return false;
  }
  virtual std::vector<int> getKeysFromCommand(
    const std::vector<std::string>& argv);
  const std::string& getName() const;
  void incrCallTimes();
  void incrNanos(uint64_t);
  uint64_t getCallTimes() const;
  uint64_t getNanos() const;
  void resetStatInfo();
  bool isReadOnly() const;
  bool isMultiKey() const;
  bool isWriteable() const;
  bool isAdmin() const;
  static mgl::LockMode RdLock();
  static void changeCommand(const string& renameCmdList, string mode);
  int getFlags() const;
  size_t getFlagsCount() const;
  static std::vector<std::string> listCommands();
  static Command* getCommand(Session* sess);
  // precheck returns command name
  static Expected<Command*> precheck(Session* sess);
  static Expected<std::string> runSessionCmd(Session* sess);
  static bool isAdminCmd(const std::string& cmd);
  // static bool isKeyLocked(Session *sess,
  //                         uint32_t storeId,
  //                         const std::string& encodedKey);

  // return ERR_OK if not expired
  // return ERR_EXPIRED if expired
  // return errors on other unexpected conditions
  static Expected<RecordValue> expireKeyIfNeeded(Session* sess,
                                                 const std::string& key,
                                                 RecordType tp,
                                                 bool hasVersion = true);

  static Expected<std::pair<std::string, std::list<Record>>> scan(
    const std::string& pk,
    const std::string& from,
    uint64_t cnt,
    Transaction* txn);

  static Status delKeyAndTTL(Session* sess,
                             const RecordKey& mk,
                             const RecordValue& val,
                             Transaction* txn);
  static Status delKey(Session* sess, const std::string& key, RecordType tp,
          Transaction* txn);

  // return true if exists and delete succ
  // return false if not exists
  // return error if has error
  static Expected<bool> delKeyChkExpire(Session* sess,
                                        const std::string& key,
                                        RecordType tp,
                                        Transaction* txn);

  static std::string fmtErr(const std::string& s);
  static std::string fmtNull();
  static std::string fmtOK();
  static std::string fmtOne();
  static std::string fmtZero();
  static std::string fmtLongLong(int64_t);
  static Expected<uint64_t> getInt64FromFmtLongLong(const std::string& str);
  static std::string fmtBusyKey();

  static std::string fmtBulk(const std::string& s);
  static std::string fmtStatus(const std::string& s);

  static std::string fmtZeroBulkLen();
  static std::stringstream& fmtMultiBulkLen(std::stringstream&, uint64_t);
  static std::stringstream& fmtBulk(std::stringstream&, const std::string&);
  static std::stringstream& fmtStatus(std::stringstream&, const std::string&);
  static std::stringstream& fmtNull(std::stringstream&);
  static std::stringstream& fmtLongLong(std::stringstream&, int64_t);

  static constexpr int32_t RETRY_CNT = 3;

 protected:
  static std::mutex _mutex;
  // protected by mutex
  static const uint32_t _maxUnseenCmdNum = 10000;
  static std::map<std::string, uint64_t> _unSeenCmds;

  static mgl::LockMode _expRdLk;

 private:
  static Status delKeyPessimisticInLock(Session* sess,
                                        uint32_t storeId,
                                        const RecordKey& rk,
                                        RecordType valueType,
                                        const TTLIndex* ictx = nullptr);

  static Status delKeyOptimismInLock(Session* sess,
                                     uint32_t storeId,
                                     const RecordKey& rk,
                                     RecordType valueType,
                                     Transaction* txn,
                                     const TTLIndex* ictx = nullptr);

  static Expected<string> delSubkeysRange(Session* sess,
                                          uint32_t storeId,
                                          const RecordKey& mk,
                                          RecordType valueType,
                                          Transaction* txn);

  static Expected<uint32_t> partialDelSubKeys(Session* sess,
                                              uint32_t storeId,
                                              uint32_t subCount,
                                              const RecordKey& mk,
                                              RecordType valueType,
                                              bool deleteMeta,
                                              Transaction* txn,
                                              const TTLIndex* ictx = nullptr);

  const std::string _name;
  /* Flags as string representation, one char per flag. */
  const std::string _sflags;
  /* The actual flags, obtained from the 'sflags' field. */
  int _flags;
  // NOTE(deyukong): all commands have been loaded at startup time
  // so there is no need to acquire a lock here.

  std::atomic<uint64_t> _callTimes;
  std::atomic<uint64_t> _totalNanoSecs;
};

std::map<std::string, Command*>& commandMap();

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_COMMANDS_COMMAND_H_
