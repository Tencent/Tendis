#ifndef SRC_TENDISPLUS_COMMANDS_COMMAND_H_
#define SRC_TENDISPLUS_COMMANDS_COMMAND_H_

#include <string>
#include <map>
#include <memory>
#include <vector>
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
    explicit Command(const std::string& name);
    virtual ~Command() = default;
    virtual Expected<std::string> run(Session *sess) = 0;
    virtual ssize_t arity() const = 0;
    virtual int32_t firstkey() const = 0;
    virtual int32_t lastkey() const = 0;
    virtual int32_t keystep() const = 0;
    const std::string& getName() const;
    void incrCallTimes();
    void incrNanos(uint64_t);
    uint64_t getCallTimes() const;
    uint64_t getNanos() const;
    static std::vector<std::string> listCommands();
    // precheck returns command name
    static Expected<std::string> precheck(Session *sess);
    static Expected<std::string> runSessionCmd(Session *sess);
    // where should I put this function ?
    static PStore getStore(Session *sess, const std::string&);
    static uint32_t getStoreId(Session *sess, const std::string&);
    static PStore getStoreById(Session *sess, uint32_t storeId);

    static std::unique_ptr<StoreLock> lockDBByKey(Session *sess,
                                                  const std::string& key,
                                                  mgl::LockMode mode);

    static bool isKeyLocked(Session *sess,
                            uint32_t storeId,
                            const std::string& encodedKey);

    // return ERR_OK if not expired
    // return ERR_EXPIRED if expired
    // return errors on other unexpected conditions
    static Expected<RecordValue> expireKeyIfNeeded(Session *sess,
                                    uint32_t storeId,
                                    const RecordKey& mk);

    static Expected<std::pair<std::string, std::list<Record>>>
    scan(const std::string& pk,
         const std::string& from,
         uint64_t cnt,
         Transaction *txn);

    static Status delKey(Session *sess, uint32_t storeId,
                            const RecordKey& rk);

    // return true if exists and delete succ
    // return false if not exists
    // return error if has error
    static Expected<bool> delKeyChkExpire(Session *sess, uint32_t storeId,
                                          const RecordKey& rk);

    static std::string fmtErr(const std::string& s);
    static std::string fmtNull();
    static std::string fmtOK();
    static std::string fmtOne();
    static std::string fmtZero();
    static std::string fmtLongLong(uint64_t);

    static std::string fmtBulk(const std::string& s);

    static std::string fmtZeroBulkLen();
    static std::stringstream& fmtMultiBulkLen(std::stringstream&, uint64_t);
    static std::stringstream& fmtBulk(std::stringstream&, const std::string&);
    static std::stringstream& fmtNull(std::stringstream&);

    static constexpr int32_t RETRY_CNT = 3;

 protected:
    static std::mutex _mutex;
    // protected by mutex
    static std::map<std::string, uint64_t> _unSeenCmds;

 private:
    static Status delKeyPessimistic(Session *sess, uint32_t storeId,
                            const RecordKey& rk);

    static Status delKeyOptimism(Session *sess, uint32_t storeId,
                            const RecordKey& rk, Transaction* txn);

    static Expected<uint32_t> partialDelSubKeys(Session *sess,
                                 uint32_t storeId,
                                 uint32_t subCount,
                                 const RecordKey& mk,
                                 bool deleteMeta,
                                 Transaction *txn);

    const std::string _name;
    // NOTE(deyukong): all commands have been loaded at startup time
    // so there is no need to acquire a lock here.

    std::atomic<uint64_t> _callTimes;
    std::atomic<uint64_t> _totalNanoSecs;
};

std::map<std::string, Command*>& commandMap();

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_COMMANDS_COMMAND_H_
