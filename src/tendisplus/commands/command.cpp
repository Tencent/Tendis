#include <string>
#include <memory>
#include <map>
#include "glog/logging.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/lock/lock.h"
#include "tendisplus/storage/record.h"

namespace tendisplus {

std::map<std::string, Command*>& commandMap() {
    static std::map<std::string, Command*> map = {};
    return map;
}

Command::Command(const std::string& name)
        :_name(name) {
    commandMap()[name] = this;
}

const std::string& Command::getName() const {
    return _name;
}

Expected<std::string> Command::precheck(NetSession *sess) {
    const auto& args = sess->getArgs();
    if (args.size() == 0) {
        LOG(FATAL) << "BUG: sess " << sess->getRemoteRepr() << " len 0 args";
    }
    std::string commandName = toLower(args[0]);
    auto it = commandMap().find(commandName);
    if (it == commandMap().end()) {
        std::stringstream ss;
        ss << "unknown command '" << args[0] << "'";
        return {ErrorCodes::ERR_PARSEPKT, ss.str()};
    }
    ssize_t arity = it->second->arity();
    if ((arity > 0 && arity != ssize_t(args.size()))
            || ssize_t(args.size()) < -arity) {
        std::stringstream ss;
        ss << "wrong number of arguments for '" << args[0] << "' command";
        return {ErrorCodes::ERR_PARSEPKT, ss.str()};
    }

    auto server = sess->getServerEntry();
    if (!server) {
        LOG(FATAL) << "BUG: get server from sess:"
                    << sess->getConnId()
                    << ",Ip:"
                    << sess->getRemoteRepr() << " empty";
    }

    if (*server->requirepass() != "" && it->second->getName() != "auth") {
        return {ErrorCodes::ERR_AUTH, "-NOAUTH Authentication required.\r\n"};
    }

    return it->second->getName();
}

// NOTE(deyukong): call precheck before call runSessionCmd
// this function does no necessary checks
Expected<std::string> Command::runSessionCmd(NetSession *sess) {
    const auto& args = sess->getArgs();
    std::string commandName = toLower(args[0]);
    auto it = commandMap().find(commandName);
    if (it == commandMap().end()) {
        LOG(FATAL) << "BUG: command:" << args[0] << " not found!";
    }
    return it->second->run(sess);
}

std::unique_ptr<StoreLock> Command::lockDBByKey(NetSession *sess,
                                                const std::string& key,
                                                mgl::LockMode mode) {
    auto server = sess->getServerEntry();
    INVARIANT(server != nullptr);
    auto segMgr = server->getSegmentMgr();
    INVARIANT(segMgr != nullptr);
    uint32_t storeId = segMgr->calcInstanceId(key);
    return std::make_unique<StoreLock>(storeId, mode);
}

bool Command::isKeyLocked(NetSession *sess,
                          uint32_t storeId,
                          const std::string& encodedKey) {
    auto server = sess->getServerEntry();
    INVARIANT(server != nullptr);
    auto pessimisticMgr = server->getPessimisticMgr();
    INVARIANT(pessimisticMgr != nullptr);
    auto shard = pessimisticMgr->getShard(storeId);
    return shard->isLocked(encodedKey);
}

Status Command::delBigkey(NetSession *sess, uint32_t storeId, const RecordKey& mk) {
    return {ErrorCodes::ERR_OK, ""};
}

Status Command::delCompondKey(NetSession *sess, uint32_t storeId,
                             const RecordKey& mk, Transaction* txn) {
    return {ErrorCodes::ERR_OK, ""};
}

Status Command::expireKeyIfNeeded(NetSession *sess,
                                  uint32_t storeId,
                                  const RecordKey& mk) {
    // currently, a simple kv will not be locked
    if (mk.getRecordType() != RecordType::RT_KV) {
        std::string mkEnc = mk.encode();
        if (Command::isKeyLocked(sess, storeId, mkEnc)) {
            return {ErrorCodes::ERR_BUSY, "key locked"};
        }
    }
    auto storeLock = std::make_unique<StoreLock>(storeId,
                    mgl::LockMode::LOCK_IX);

    PStore kvstore = Command::getStoreById(sess, storeId);
    for (uint32_t i = 0; i < RETRY_CNT; ++i) {
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());

        Expected<RecordValue> eValue = kvstore->getKV(mk, txn.get());
        if (!eValue.ok()) {
            return eValue.status();
        }
        uint64_t currentTs = nsSinceEpoch()/1000;
        if (currentTs < eValue.value().getTtl()) {
            return {ErrorCodes::ERR_OK, ""};
        }
        Expected<uint64_t> cnt = getSubKeyCount(mk, eValue.value());
        if (!cnt.ok()) {
            return cnt.status();
        }
        if (cnt.value() >= 2048) {
            LOG(INFO) << "bigkey delete:" << mk.getPrimaryKey()
                      << ",size:" << cnt.value();
            // reset storelock, or deadlock
            storeLock.reset();
            // reset txn, it is no longer used
            txn.reset();
            Status s = Command::delBigkey(sess, storeId, mk);
            if (s.ok()) {
                return {ErrorCodes::ERR_EXPIRED, ""};
            } else {
                return s;
            }
        } else {
            Status s = Command::delCompondKey(sess, storeId, mk, txn.get());
            if (s.code() == ErrorCodes::ERR_COMMIT_RETRY
                    && i != RETRY_CNT - 1) {
                continue;
            }
            if (s.ok()) {
                return {ErrorCodes::ERR_EXPIRED, ""};
            } else {
                return s;
            }
        }
    }
    // should never reach here
    INVARIANT(0);
    return {ErrorCodes::ERR_INTERNAL, "not reachable"};
}

PStore Command::getStore(NetSession *sess, const std::string& key) {
    auto server = sess->getServerEntry();
    INVARIANT(server != nullptr);
    auto segMgr = server->getSegmentMgr();
    INVARIANT(segMgr != nullptr);
    auto kvStore = segMgr->calcInstance(key);
    INVARIANT(kvStore != nullptr);
    return kvStore;
}

uint32_t Command::getStoreId(NetSession *sess, const std::string& key) {
    auto server = sess->getServerEntry();
    INVARIANT(server != nullptr);
    auto segMgr = server->getSegmentMgr();
    INVARIANT(segMgr != nullptr);
    return segMgr->calcInstanceId(key);
}

PStore Command::getStoreById(NetSession *sess, uint32_t storeId) {
    auto server = sess->getServerEntry();
    INVARIANT(server != nullptr);
    auto segMgr = server->getSegmentMgr();
    INVARIANT(segMgr != nullptr);
    return segMgr->getInstanceById(storeId);
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

std::string Command::fmtLongLong(uint64_t v) {
    std::stringstream ss;
    ss << ":" << v << "\r\n";
    return ss.str();
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

std::string Command::fmtBulk(const std::string& s) {
    std::stringstream ss;
    ss << "$" << s.size() << "\r\n";
    ss.write(s.c_str(), s.size());
    ss << "\r\n";
    return ss.str();
}

}  // namespace tendisplus
