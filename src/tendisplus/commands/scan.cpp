#include <string>
#include <utility>
#include <memory>
#include <algorithm>
#include <cctype>
#include <clocale>
#include <vector>
#include "glog/logging.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/storage/varint.h"

namespace tendisplus {
class ScanGenericCommand: public Command {
 public:
    ScanGenericCommand(const std::string& name)
        :Command(name) {
    }

    virtual RecordType getRcdType() const = 0;

    virtual RecordKey genFakeRcd(uint32_t chunkId, uint32_t dbId,
                                const std::string& key) const = 0;

    virtual Expected<std::string> genResult(const std::string& cursor,
                    const std::list<Record>& rcds) = 0;

    ssize_t arity() const {
        return -3;
    }

    int32_t firstkey() const {
        return 1;
    }

    int32_t lastkey() const {
        return 1;
    }

    int32_t keystep() const {
        return 1;
    }

    Expected<std::string> run(Session *sess) final {
        const std::vector<std::string>& args = sess->getArgs();
        const std::string& key = args[1];
        const std::string& cursor = args[2];
        size_t i = 3;
        int j;
        std::string pat;
        int usePatten = 0;
        uint64_t count = 10;
        while (i < args.size()) {
            j = args.size() - i;
            if (toLower(args[i]) == "count" && j >= 2) {
                Expected<uint64_t> ecnt = ::tendisplus::stoul(args[i+1]);
                if (!ecnt.ok()) {
                    return ecnt.status();
                }
                if (ecnt.value() < 1) {
                    return {ErrorCodes::ERR_PARSEOPT, "syntax error"};
                }
                count = ecnt.value();
                i += 2;
            } else if (toLower(args[i]) == "match" && j >= 2) {
                pat = args[i+1];
                usePatten = !(pat[0] == '*' && pat.size() == 1);
                i += 2;
            } else {
                return {ErrorCodes::ERR_PARSEOPT, "syntax error"};
            }
        }

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, getRcdType());
        if (rv.status().code() == ErrorCodes::ERR_EXPIRED ||
                rv.status().code() == ErrorCodes::ERR_NOTFOUND) {
            std::stringstream ss;
            Command::fmtMultiBulkLen(ss, 2);
            Command::fmtBulk(ss, "0");
            Command::fmtMultiBulkLen(ss, 0);
            return ss.str();
        } else if (!rv.ok()) {
            return rv.status();
        }

        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(sess, key,
                                mgl::LockMode::LOCK_S);
        if (!expdb.ok()) {
            return expdb.status();
        }
        SessionCtx *pCtx = sess->getCtx();
        RecordKey metaRk(expdb.value().chunkId, pCtx->getDbId(),
                            getRcdType(), key, "");
        // uint32_t storeId = expdb.value().dbId;
        std::string metaKeyEnc = metaRk.encode();
        PStore kvstore = expdb.value().store;

        // if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
        //     return {ErrorCodes::ERR_BUSY, "key locked"};
        // }

        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());

        RecordKey fake = genFakeRcd(expdb.value().chunkId,
                                    pCtx->getDbId(), key);

        auto batch = Command::scan(fake.prefixPk(), cursor, count, txn.get());
        if (!batch.ok()) {
            return batch.status();
        }
        const bool NOCASE = false;
        for (std::list<Record>::iterator it = batch.value().second.begin();
                it != batch.value().second.end(); ) {
             if (usePatten && !redis_port::stringmatchlen(pat.c_str(),
                               pat.size(),
                               it->getRecordKey().getSecondaryKey().c_str(),
                               it->getRecordKey().getSecondaryKey().size(),
                               NOCASE)) {
                it = batch.value().second.erase(it);
            } else {
                ++it;
            }
        }
        return genResult(batch.value().first, batch.value().second);
    }
};

class ZScanCommand: public ScanGenericCommand {
 public:
    ZScanCommand()
        :ScanGenericCommand("zscan") {
    }

    RecordType getRcdType() const final {
        return RecordType::RT_ZSET_META;
    }

    RecordKey genFakeRcd(uint32_t chunkId, uint32_t dbId,
                        const std::string& key) const final {
        return {chunkId, dbId, RecordType::RT_ZSET_H_ELE, key, ""};
    }

    Expected<std::string> genResult(const std::string& cursor,
                    const std::list<Record>& rcds) {
        std::stringstream ss;
        Command::fmtMultiBulkLen(ss, 2);
        Command::fmtBulk(ss, cursor);
        Command::fmtMultiBulkLen(ss, rcds.size()*2);
        for (const auto& v : rcds) {
            Command::fmtBulk(ss, v.getRecordKey().getSecondaryKey());
            auto d = tendisplus::doubleDecode(v.getRecordValue().getValue());
            INVARIANT(d.ok());
            if (!d.ok()) {
                return d.status();
            }
            Command::fmtBulk(ss, tendisplus::dtos(d.value()));
        }
        return ss.str();
    }
} zscanCmd;

class SScanCommand: public ScanGenericCommand {
 public:
    SScanCommand()
        :ScanGenericCommand("sscan") {
    }

    RecordType getRcdType() const final {
        return RecordType::RT_SET_META;
    }

    RecordKey genFakeRcd(uint32_t chunkId, uint32_t dbId,
                        const std::string& key) const final {
        return {chunkId, dbId, RecordType::RT_SET_ELE, key, ""};
    }

    Expected<std::string> genResult(const std::string& cursor,
                    const std::list<Record>& rcds) {
        std::stringstream ss;
        Command::fmtMultiBulkLen(ss, 2);
        Command::fmtBulk(ss, cursor);
        Command::fmtMultiBulkLen(ss, rcds.size());
        for (const auto& v : rcds) {
            Command::fmtBulk(ss, v.getRecordKey().getSecondaryKey());
        }
        return ss.str();
    }
} sscanCmd;

class HScanCommand: public ScanGenericCommand {
 public:
    HScanCommand()
        :ScanGenericCommand("hscan") {
    }

    RecordType getRcdType() const final {
        return RecordType::RT_HASH_META;
    }

    RecordKey genFakeRcd(uint32_t chunkId, uint32_t dbId,
                        const std::string& key) const final {
        return {chunkId, dbId, RecordType::RT_HASH_ELE, key, ""};
    }

    Expected<std::string> genResult(const std::string& cursor,
                    const std::list<Record>& rcds) final {
        std::stringstream ss;
        Command::fmtMultiBulkLen(ss, 2);
        Command::fmtBulk(ss, cursor);
        Command::fmtMultiBulkLen(ss, rcds.size()*2);
        for (const auto& v : rcds) {
            Command::fmtBulk(ss, v.getRecordKey().getSecondaryKey());
            Command::fmtBulk(ss, v.getRecordValue().getValue());
        }
        return ss.str();
    }
} hscanCmd;

class ScanCommand: public Command {
 public:
    ScanCommand()
        :Command("scan") {
    }

    ssize_t arity() const {
        return -2;
    }

    int32_t firstkey() const {
        return 1;
    }

    int32_t lastkey() const {
        return 1;
    }

    int32_t keystep() const {
        return 1;
    }

    // NOTE(deyukong): tendis did not impl this api
    Expected<std::string> run(Session* sess) final {
        return Command::fmtZeroBulkLen();
    }
} scanCmd;

/*
class KeysCommand: public Command {
 public:
    KeysCommand()
        :Command("keys") {
    }

    ssize_t arity() const {
        return 2;
    }

    int32_t firstkey() const {
        return 1;
    }

    int32_t lastkey() const {
        return 1;
    }

    int32_t keystep() const {
        return 1;
    }

    Expected<std::string> run(Session* sess) final {
        const std::string& pat = sess->getArgs()[1];
        bool allkeys = (pat == "*");

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);

        RecordKey rv(pCtx->getDbId(), RecordType::RT_KV, "fake", "");

        auto storeLock = std::make_unique<StoreLock>(0, mgl::LockMode::LOCK_IS, sess);

        PStore kvstore = Command::getStoreById(sess, 0);
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());

        std::string pfx = rv.prefixDbidType();
        auto cursor = txn->createCursor();
        cursor->seek(pfx);

        std::list<Record> result;
        constexpr uint64_t LIM = 1024;
        uint64_t currentTs = nsSinceEpoch()/1000000;

        while (true) {
            if (result.size() >= LIM) {
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
            if (rcdKey.getRecordType() != RecordType::RT_KV) {
                break;
            }
            if (!allkeys && !redis_port::stringmatchlen(pat.c_str(),
                               pat.size(),
                               rcdKey.getPrimaryKey().c_str(), 
                               rcdKey.getPrimaryKey().size(),
                               0)) {
                continue;
            }
            uint64_t targetTtl = rcd.getRecordValue().getTtl();
            if (targetTtl != 0 && currentTs >= targetTtl) {
                continue;
            }
            result.emplace_back(std::move(exptRcd.value()));
        }

        std::stringstream ss;
        Command::fmtMultiBulkLen(ss, result.size());
        for (const auto& v : result) {
            Command::fmtBulk(ss, v.getRecordKey().getPrimaryKey());
        }
        return ss.str();
    }
} keysCmd;
*/
}  // namespace tendisplus
