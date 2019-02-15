#include <string>
#include <memory>
#include <map>
#include <utility>
#include <list>
#include <limits>
#include <vector>
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

std::map<std::string, uint64_t> Command::_unSeenCmds = {};

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

void Command::incrCallTimes() {
    _callTimes.fetch_add(1, std::memory_order_relaxed);
}

void Command::incrNanos(uint64_t v) {
    _totalNanoSecs.fetch_add(v, std::memory_order_relaxed);
}

uint64_t Command::getCallTimes() const {
    return _callTimes.load(std::memory_order_relaxed);
}

uint64_t Command::getNanos() const {
    return _totalNanoSecs.load(std::memory_order_relaxed);
}

std::vector<std::string> Command::listCommands() {
    std::vector<std::string> lst;
    const auto& v = commandMap();
    for (const auto& kv : v) {
        lst.push_back(kv.first);
    }
    return lst;
}

Expected<std::string> Command::precheck(Session *sess) {
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
                _unSeenCmds[commandName] = 1;
            } else {
                _unSeenCmds[commandName] += 1;
            }
        }
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
                    << sess->id()
                    << ",Ip:"
                    << sess->id() << " empty";
    }

    SessionCtx *pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);
    bool authed = pCtx->authed();
    if (!authed && *server->requirepass() != ""
            && it->second->getName() != "auth") {
        return {ErrorCodes::ERR_AUTH, "-NOAUTH Authentication required."};
    }

    return it->second->getName();
}

// NOTE(deyukong): call precheck before call runSessionCmd
// this function does no necessary checks
Expected<std::string> Command::runSessionCmd(Session *sess) {
    const auto& args = sess->getArgs();
    std::string commandName = toLower(args[0]);
    auto it = commandMap().find(commandName);
    if (it == commandMap().end()) {
        LOG(FATAL) << "BUG: command:" << args[0] << " not found!";
    }

    sess->getCtx()->setArgsBrief(sess->getArgs());
    it->second->incrCallTimes();
    auto now = nsSinceEpoch();
    auto guard = MakeGuard([it, now, sess] {
        sess->getCtx()->clearRequestCtx();
        it->second->incrNanos(nsSinceEpoch() - now);
    });
    return it->second->run(sess);
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

// requirement: StoreLock not held, add storelock inside
Status Command::delKeyPessimistic(Session *sess, uint32_t storeId,
                          const RecordKey& mk) {
    std::string keyEnc = mk.encode();
    auto server = sess->getServerEntry();

    LOG(INFO) << "begin delKeyPessimistic key:"
                << hexlify(mk.getPrimaryKey());

    // storeLock destructs after guard, it's guaranteed.
    // unlock the key at last
    auto expdb = server->getSegmentMgr()->getDb(sess, storeId, mgl::LockMode::LOCK_IX);
    if (!expdb.ok()) {
        return expdb.status();
    }

    PStore kvstore = expdb.value().store;
    uint64_t totalCount = 0;
    const uint32_t batchSize = 2048;
    while (true) {
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        Expected<uint32_t> deleteCount =
            partialDelSubKeys(sess, storeId, batchSize, mk, false, txn.get());
        if (!deleteCount.ok()) {
            return deleteCount.status();
        }
        totalCount += deleteCount.value();
        if (deleteCount.value() == batchSize) {
            continue;
        }
        TEST_SYNC_POINT_CALLBACK("delKeyPessimistic::TotalCount", &totalCount);
        ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        txn = std::move(ptxn.value());
        Status s = kvstore->delKV(mk, txn.get());
        if (!s.ok()) {
            return s;
        }
        Expected<uint64_t> commitStatus = txn->commit();
        return commitStatus.status();
    }
}

Expected<std::pair<std::string, std::list<Record>>>
Command::scan(const std::string& pk, const std::string& from, uint64_t cnt, Transaction *txn) {
    auto cursor = txn->createCursor();
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
        std::pair<std::string, std::list<Record>>(
            nextCursor, std::move(result)));
}

// requirement: StoreLock held
Status Command::delKeyOptimism(Session *sess,
                                           uint32_t storeId,
                                           const RecordKey& rk,
                                           Transaction* txn) {
    auto s = Command::partialDelSubKeys(sess,
                              storeId,
                              std::numeric_limits<uint32_t>::max(),
                              rk,
                              true,
                              txn);
    return s.status();
}

Expected<uint32_t> Command::partialDelSubKeys(Session *sess,
                                       uint32_t storeId,
                                       uint32_t subCount,
                                       const RecordKey& mk,
                                       bool deleteMeta,
                                       Transaction* txn) {
    if (deleteMeta && subCount != std::numeric_limits<uint32_t>::max()) {
        return {ErrorCodes::ERR_PARSEOPT, "delmeta with limited subcount"};
    }
    auto server = sess->getServerEntry();
    INVARIANT(server != nullptr);
    auto expdb = server->getSegmentMgr()->getDb(nullptr, storeId, mgl::LockMode::LOCK_NONE);
    if (!expdb.ok()) {
        return expdb.status();
    }
    PStore kvstore = expdb.value().store;
    if (mk.getRecordType() == RecordType::RT_KV) {
        Status s = kvstore->delKV(mk, txn);
        if (!s.ok()) {
            return s;
        }
        auto commitStatus = txn->commit();
        if (!commitStatus.ok()) {
            return commitStatus.status();
        }
        return 1;
    }
    std::vector<std::string> prefixes;
    if (mk.getRecordType() == RecordType::RT_HASH_META) {
        RecordKey fakeEle(mk.getChunkId(),
                          mk.getDbId(),
                          RecordType::RT_HASH_ELE,
                          mk.getPrimaryKey(),
                          "");
        prefixes.push_back(fakeEle.prefixPk());
    } else if (mk.getRecordType() == RecordType::RT_LIST_META) {
        RecordKey fakeEle(mk.getChunkId(),
                          mk.getDbId(),
                          RecordType::RT_LIST_ELE,
                          mk.getPrimaryKey(),
                          "");
        prefixes.push_back(fakeEle.prefixPk());
    } else if (mk.getRecordType() == RecordType::RT_SET_META) {
        RecordKey fakeEle(mk.getChunkId(),
                          mk.getDbId(),
                          RecordType::RT_SET_ELE,
                          mk.getPrimaryKey(),
                          "");
        prefixes.push_back(fakeEle.prefixPk());
    } else if (mk.getRecordType() == RecordType::RT_ZSET_META) {
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
        INVARIANT(0);
    }

    std::list<RecordKey> pendingDelete;
    for (const auto& prefix : prefixes) {
        auto cursor = txn->createCursor();
        cursor->seek(prefix);

        while (true) {
            if (pendingDelete.size() >= subCount) {
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
        Status s = kvstore->delKV(v, txn);
        if (!s.ok()) {
            return s;
        }
    }
    Expected<uint64_t> commitStatus = txn->commit();
    if (commitStatus.ok()) {
        return pendingDelete.size();
    } else {
        return commitStatus.status();
    }
}

Expected<bool> Command::delKeyChkExpire(Session *sess,
                                        const std::string& key,
                                        RecordType tp) {
    Expected<RecordValue> rv =
        Command::expireKeyIfNeeded(sess, key, tp);
    if (rv.status().code() == ErrorCodes::ERR_EXPIRED) {
        return false;
    } else if (rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
        return false;
    } else if (!rv.status().ok()) {
        return rv.status();
    }

    // key exists and not expired, now we delete it
    Status s = Command::delKey(sess, key, tp);
    if (s.code() == ErrorCodes::ERR_NOTFOUND) {
        return false;
    }
    if (s.ok()) {
        return true;
    }
    return s;
}

Status Command::delKey(Session *sess, const std::string& key, RecordType tp) {
    auto server = sess->getServerEntry();
    INVARIANT(server != nullptr);
    SessionCtx *pCtx = sess->getCtx();
    INVARIANT(pCtx != nullptr);

    auto expdb = server->getSegmentMgr()->getDb(sess, key, mgl::LockMode::LOCK_IX);
    if (!expdb.ok()) {
        return expdb.status();
    }
    PStore kvstore = expdb.value().store;
    uint32_t storeId = expdb.value().dbId;

    RecordKey mk(expdb.value().chunkId, pCtx->getDbId(), tp, key, "");
    // currently, a simple kv will not be locked
    // if (mk.getRecordType() != RecordType::RT_KV) {
    //     std::string mkEnc = mk.encode();
    //     if (Command::isKeyLocked(sess, storeId, mkEnc)) {
    //         return {ErrorCodes::ERR_BUSY, "key locked"};
    //     }
    // }
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
        auto cnt = rcd_util::getSubKeyCount(mk, eValue.value());
        if (!cnt.ok()) {
            return cnt.status();
        }
        if (cnt.value() >= 2048 ||
                (mk.getRecordType() == RecordType::RT_ZSET_META && cnt.value() >= 1024)) {
            LOG(INFO) << "bigkey delete:" << hexlify(mk.getPrimaryKey())
                      << ",rcdType:" << rt2Char(mk.getRecordType())
                      << ",size:" << cnt.value();
            // reset storelock, or will deadlock
            expdb.value().lk.reset();
            // reset txn, it is no longer used
            txn.reset();
            return Command::delKeyPessimistic(sess, storeId, mk);
        } else {
            Status s = Command::delKeyOptimism(sess, storeId, mk, txn.get());
            if (s.code() == ErrorCodes::ERR_COMMIT_RETRY
                    && i != RETRY_CNT - 1) {
                continue;
            }
            return s;
        }
    }
    // should never reach here
    INVARIANT(0);
    return {ErrorCodes::ERR_INTERNAL, "not reachable"};
}

Expected<RecordValue> Command::expireKeyIfNeeded(Session *sess, const std::string& key, RecordType tp) {
    auto server = sess->getServerEntry();
    INVARIANT(server != nullptr);
    auto expdb = server->getSegmentMgr()->getDb(sess, key, mgl::LockMode::LOCK_IX);
    if (!expdb.ok()) {
        return expdb.status();
    }
    auto storeLock = std::move(expdb.value().lk);
    uint32_t storeId = expdb.value().dbId;
    RecordKey mk(expdb.value().chunkId, expdb.value().dbId, tp, key, "");

    // currently, a simple kv will not be locked
    // if (mk.getRecordType() != RecordType::RT_KV) {
    //     std::string mkEnc = mk.encode();
    //     if (Command::isKeyLocked(sess, storeId, mkEnc)) {
    //         return {ErrorCodes::ERR_BUSY, "key locked"};
    //     }
    // }
    PStore kvstore = expdb.value().store;
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
        uint64_t currentTs = nsSinceEpoch()/1000000;
        uint64_t targetTtl = eValue.value().getTtl();
        if (targetTtl == 0 || currentTs < targetTtl) {
            return eValue.value();
        }
        auto cnt = rcd_util::getSubKeyCount(mk, eValue.value());
        if (!cnt.ok()) {
            return cnt.status();
        }
        if (cnt.value() >= 2048) {
            LOG(INFO) << "bigkey delete:" << hexlify(mk.getPrimaryKey())
                      << ",rcdType:" << rt2Char(mk.getRecordType())
                      << ",size:" << cnt.value();
            // reset storelock, or will deadlock
            storeLock.reset();
            // reset txn, it is no longer used
            txn.reset();
            Status s = Command::delKeyPessimistic(sess, storeId, mk);
            if (s.ok()) {
                return {ErrorCodes::ERR_EXPIRED, ""};
            } else {
                return s;
            }
        } else {
            Status s = Command::delKeyOptimism(sess, storeId, mk, txn.get());
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

}  // namespace tendisplus
