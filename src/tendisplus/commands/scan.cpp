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

namespace tendisplus {
class ScanGenericCommand: public Command {
 public:
    ScanGenericCommand(const std::string& name)
        :Command(name) {
    }

    virtual RecordKey genMetaRcd(uint32_t dbId, const std::string& key) = 0;

    virtual RecordKey genFakeRcd(uint32_t dbId, const std::string& key) = 0;

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
            if (args[i] == "count" && j >= 2) {
                Expected<uint64_t> ecnt = ::tendisplus::stoul(args[i+1]);
                if (!ecnt.ok()) {
                    return ecnt.status();
                }
                if (ecnt.value() < 1) {
                    return {ErrorCodes::ERR_PARSEOPT, "syntax error"};
                }
                count = ecnt.value();
                i += 2;
            } else if (args[i] == "match" && j >= 2) {
                pat = args[i+1];
                usePatten = !(pat[0] == '*' && pat.size() == 1);
                i += 2;
            } else {
                return {ErrorCodes::ERR_PARSEOPT, "syntax error"};
            }
        }

        SessionCtx *pCtx = sess->getCtx();
        INVARIANT(pCtx != nullptr);

        RecordKey metaRk = genMetaRcd(pCtx->getDbId(), key);
        std::string metaKeyEnc = metaRk.encode();
        uint32_t storeId = Command::getStoreId(sess, key);

        Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, storeId, metaRk);
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

        auto storeLock = Command::lockDBByKey(sess,
                                              key,
                                              mgl::LockMode::LOCK_IS);
        if (Command::isKeyLocked(sess, storeId, metaKeyEnc)) {
            return {ErrorCodes::ERR_BUSY, "key locked"};
        }

        PStore kvstore = Command::getStoreById(sess, storeId);
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());

        RecordKey fake = genFakeRcd(pCtx->getDbId(), key);

        auto batch = Command::scan(fake.prefixPk(), cursor, count, txn.get());
        if (!batch.ok()) {
            return batch.status();
        }
        bool NOCASE = false;
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

    RecordKey genMetaRcd(uint32_t dbId, const std::string& key) final {
        return {dbId, RecordType::RT_ZSET_META, key, ""};
    }

    RecordKey genFakeRcd(uint32_t dbId, const std::string& key) final {
        return {dbId, RecordType::RT_ZSET_H_ELE, key, ""};
    }

    Expected<std::string> genResult(const std::string& cursor,
                    const std::list<Record>& rcds) {
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
} zscanCmd;

class SScanCommand: public ScanGenericCommand {
 public:
    SScanCommand()
        :ScanGenericCommand("sscan") {
    }

    RecordKey genMetaRcd(uint32_t dbId, const std::string& key) final {
        return {dbId, RecordType::RT_SET_META, key, ""};
    }

    RecordKey genFakeRcd(uint32_t dbId, const std::string& key) final {
        return {dbId, RecordType::RT_SET_ELE, key, ""};
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

    RecordKey genMetaRcd(uint32_t dbId, const std::string& key) final {
        return {dbId, RecordType::RT_HASH_META, key, ""};
    }

    RecordKey genFakeRcd(uint32_t dbId, const std::string& key) final {
        return {dbId, RecordType::RT_HASH_ELE, key, ""};
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

}  // namespace tendisplus
