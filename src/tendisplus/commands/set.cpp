#include <string>
#include <utility>
#include <memory>
#include <algorithm>
#include <cctype>
#include <clocale>
#include "glog/logging.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

constexpr int32_t REDIS_SET_NO_FLAGS = 0;

// set if not exists
constexpr int32_t REDIS_SET_NX = (1<<0);

// set if exists
constexpr int32_t REDIS_SET_XX = (1<<1);

// set and expire if not exists
constexpr int32_t REDIS_SET_NXEX = (1<<2);

struct SetParams {
    SetParams()
        :key(""),
         value(""),
         flags(REDIS_SET_NO_FLAGS),
         expire(0) {
    }
    std::string key;
    std::string value;
    int32_t flags;
    uint64_t expire;
};

Expected<std::string> setGeneric(PStore store, Transaction *txn,
            int32_t flags, const RecordKey& key, const RecordValue& val,
            const std::string& okReply, const std::string& abortReply) {
    if ((flags & REDIS_SET_NX) || (flags & REDIS_SET_XX)
            || (flags & REDIS_SET_NXEX)) {
        Expected<RecordValue> eValue = store->getKV(key, txn);
        if ((!eValue.ok()) &&
                eValue.status().code() != ErrorCodes::ERR_NOTFOUND) {
            return eValue.status();
        }
        bool exists = (eValue.status().code() == ErrorCodes::ERR_OK);
        if ((flags & REDIS_SET_NX && exists) ||
                (flags & REDIS_SET_XX && (!exists)) ||
                (flags & REDIS_SET_NXEX && exists)) {
            return abortReply == "" ? Command::fmtNull() : abortReply;
        }
    }

    // TODO(deyukong): eliminate this copy
    Record kv(key, val);
    Status status = store->setKV(kv, txn);
    TEST_SYNC_POINT("setGeneric::SetKV::1");
    if (!status.ok()) {
        return status;
    }
    Expected<Transaction::CommitId> exptCommit = txn->commit();
    if (!exptCommit.ok()) {
        return exptCommit.status();
    }
    return okReply == "" ? Command::fmtOK() : okReply;
}

class SetCommand: public Command {
 public:
    SetCommand()
        :Command("set") {
    }

    Expected<SetParams> parse(NetSession *sess) const {
        const auto& args = sess->getArgs();
        SetParams result;
        if (args.size() < 3) {
            return {ErrorCodes::ERR_PARSEPKT, "invalid set params"};
        }
        result.key = args[1];
        result.value = args[2];
        try {
            for (size_t i = 3; i < args.size(); i++) {
                const std::string& s = toLower(args[i]);
                if (s == "nx") {
                    result.flags |= REDIS_SET_NX;
                } else if (s == "xx") {
                    result.flags |= REDIS_SET_XX;
                } else if (s == "ex" && i+1 < args.size()) {
                    result.expire = std::stoul(args[i+1])*1000ULL;
                    i++;
                } else if (s == "px" && i+1 < args.size()) {
                    result.expire = std::stoul(args[i+1]);
                    i++;
                } else {
                    return {ErrorCodes::ERR_PARSEPKT, "syntax error"};
                }
            }
        } catch (std::exception& ex) {
            LOG(WARNING) << "parse setParams failed:" << ex.what();
            return {ErrorCodes::ERR_PARSEPKT,
                    "value is not an integer or out of range"};
        }
        return result;
    }

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

    Expected<std::string> run(NetSession *sess) final {
        Expected<SetParams> params = parse(sess);
        if (!params.ok()) {
            return params.status();
        }

        PStore kvstore = getStore(sess, params.value().key);
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());

        SessionCtx *pCtx = sess->getCtx();
        assert(pCtx);

        RecordKey rk(pCtx->getDbId(), RecordType::RT_KV,
                params.value().key, "");

        uint64_t ts = 0;
        if (params.value().expire != 0) {
            ts = nsSinceEpoch() / 1000000 + params.value().expire;
        }
        RecordValue rv(params.value().value, ts);

        for (int32_t i = 0; i < RETRY_CNT - 1; ++i) {
            auto result = setGeneric(kvstore, txn.get(), params.value().flags,
                    rk, rv, "", "");
            if (result.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
                return result;
            }
            ptxn = kvstore->createTransaction();
            if (!ptxn.ok()) {
                return ptxn.status();
            }
            std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        }
        return setGeneric(kvstore, txn.get(), params.value().flags,
            rk, rv, "", "");
    }
} setCommand;

}  // namespace tendisplus
