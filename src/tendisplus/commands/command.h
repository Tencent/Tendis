#ifndef SRC_TENDISPLUS_COMMANDS_COMMAND_H_
#define SRC_TENDISPLUS_COMMANDS_COMMAND_H_

#include <string>
#include <map>
#include <memory>
#include "tendisplus/utils/status.h"
#include "tendisplus/network/network.h"
#include "tendisplus/lock/lock.h"

namespace tendisplus {

class Command {
 public:
    using CmdMap = std::map<std::string, Command*>;
    explicit Command(const std::string& name);
    virtual ~Command() = default;
    virtual Expected<std::string> run(NetSession *sess) = 0;
    virtual ssize_t arity() const = 0;
    virtual int32_t firstkey() const = 0;
    virtual int32_t lastkey() const = 0;
    virtual int32_t keystep() const = 0;
    const std::string& getName() const;
    // precheck returns command name
    static Expected<std::string> precheck(NetSession *sess);
    static Expected<std::string> runSessionCmd(NetSession *sess);
    // where should I put this function ?
    static PStore getStore(NetSession *sess, const std::string&);
    static uint32_t getStoreId(NetSession *sess, const std::string&);
    static PStore getStoreById(NetSession *sess, uint32_t storeId);

    static std::unique_ptr<StoreLock> lockDBByKey(NetSession *sess,
                                                  const std::string& key,
                                                  mgl::LockMode mode);

    static bool isKeyLocked(NetSession *sess,
                            uint32_t storeId,
                            const std::string& encodedKey);

    // return ERR_OK if not expired
    // return ERR_EXPIRED if expired
    // return errors on other unexpected conditions
    static Expected<RecordValue> expireKeyIfNeeded(NetSession *sess,
                                    uint32_t storeId,
                                    const RecordKey& mk);

    static Status delKey(NetSession *sess, uint32_t storeId,
                            const RecordKey& rk);

    // return true if exists and delete succ
    // return false if not exists
    // return error if has error
    static Expected<bool> delKeyChkExpire(NetSession *sess, uint32_t storeId,
                                          const RecordKey& rk);

    static std::string fmtErr(const std::string& s);
    static std::string fmtNull();
    static std::string fmtOK();
    static std::string fmtOne();
    static std::string fmtZero();
    static std::string fmtLongLong(uint64_t);

    static std::string fmtBulk(const std::string& s);
    static std::stringstream& fmtMultiBulkLen(std::stringstream&, uint64_t);
    static std::stringstream& fmtBulk(std::stringstream&, const std::string&);

    static constexpr int32_t RETRY_CNT = 3;

 private:
    static Status delKeyPessimistic(NetSession *sess, uint32_t storeId,
                            const RecordKey& rk);

    static Status delKeyOptimism(NetSession *sess, uint32_t storeId,
                            const RecordKey& rk, Transaction* txn);

    static Expected<uint32_t> partialDelSubKeys(NetSession *sess,
                                 uint32_t storeId,
                                 uint32_t subCount,
                                 const RecordKey& mk,
                                 bool deleteMeta,
                                 Transaction *txn);
    const std::string _name;
    // NOTE(deyukong): all commands have been loaded at startup time
    // so there is no need to acquire a lock here.
};

std::map<std::string, Command*>& commandMap();

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_COMMANDS_COMMAND_H_
