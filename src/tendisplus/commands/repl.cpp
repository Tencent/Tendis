#include <string>
#include <utility>
#include <memory>
#include <algorithm>
#include <cctype>
#include <clocale>
#include "glog/logging.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

class FullSyncCommand: public Command {
 public:
    FullSyncCommand()
        :Command("fullsync") {
    }

    ssize_t arity() const {
        return 2;
    }

    int32_t firstkey() const {
        return 0;
    }

    int32_t lastkey() const {
        return 0;
    }

    int32_t keystep() const {
        return 0;
    }

    Expected<uint32_t> parse(NetSession *sess) const {
        const auto& args = sess->getArgs();
        if (args.size() != 2) {
            return {ErrorCodes::ERR_PARSEPKT, "invalid  fullsync params"};
        }
        uint64_t storeId = 0;
        try {
            storeId = std::stoul(args[1]);
        } catch (std::exception& ex) {
            return {ErrorCodes::ERR_PARSEPKT, ex.what()};
        }
        if (storeId >= KVStore::INSTANCE_NUM) {
            return {ErrorCodes::ERR_PARSEPKT, "store it outof boundary"};
        }
        return storeId;
    }

    Expected<std::string> run(NetSession *sess) final {
        LOG(FATAL) << "fullsync should not be called";
        // void compiler complain
        return {ErrorCodes::ERR_INTERNAL, "shouldn't be called"};
    }
} fullSyncCommand;

class SlaveofCommand: public Command {
 public:
    SlaveofCommand()
        :Command("slaveof") {
    }

    // slaveof no one
    // slaveof no one myStoreId
    // slaveof ip port
    // slaveof ip port myStoreId sourceStoreId
    Expected<std::string> run(NetSession *sess) final {
        const auto& args = sess->getArgs();
        assert(args.size() >= 3);
        std::shared_ptr<ServerEntry> svr = sess->getServerEntry();
        assert(svr);
        if (toLower(args[1]) == "no" && toLower(args[2]) == "one") {
            if (args.size() == 4) {
                uint64_t storeId = 0;
                try {
                    storeId = std::stoul(args[1]);
                } catch (std::exception& ex) {
                    return {ErrorCodes::ERR_PARSEPKT, ex.what()};
                }
                if (storeId >= KVStore::INSTANCE_NUM) {
                    return {ErrorCodes::ERR_PARSEPKT, "invalid storeId"};
                }

                Status s = svr->getReplManager()->changeReplSource(storeId, "", 0, -1);
                if (s.ok()) {
                    return Command::fmtOK();
                }
                return s;
            } else {
                for (uint32_t i = 0; i < KVStore::INSTANCE_NUM; ++i) {
                    Status s = svr->getReplManager()->changeReplSource(i, "", 0, -1);
                    if (!s.ok()) {
                        return s;
                    }
                }
                return Command::fmtOK();
            }
        } else {
            std::string ip = args[1];
            uint64_t port;
            try {
                port = std::stoul(args[1]);
            } catch (std::exception& ex) {
                return {ErrorCodes::ERR_PARSEPKT, ex.what()};
            }
            if (args.size() == 3) {
                for (uint32_t i = 0; i < KVStore::INSTANCE_NUM; ++i) {
                    Status s = svr->getReplManager()->changeReplSource(i, ip, port, i);
                    if (!s.ok()) {
                        return s;
                    }
                }
                return Command::fmtOK();
            } else if (args.size() == 5) {
                uint64_t storeId;
                uint64_t sourceStoreId;
                try {
                    storeId = std::stoul(args[3]);
                    sourceStoreId = std::stoul(args[4]);
                } catch (std::exception& ex) {
                    return {ErrorCodes::ERR_PARSEPKT, ex.what()};
                }
                if (storeId >= KVStore::INSTANCE_NUM || sourceStoreId >= KVStore::INSTANCE_NUM) {
                    return {ErrorCodes::ERR_PARSEPKT, "invalid storeId"};
                }
                Status s = svr->getReplManager()->changeReplSource(storeId, ip, port, sourceStoreId);
                if (s.ok()) {
                    return Command::fmtOK();
                }
                return s;
            } else {
                return {ErrorCodes::ERR_PARSEPKT, "bad argument num"};
            }
        }
    }

    ssize_t arity() const {
        return -3;
    }

    int32_t firstkey() const {
        return 0;
    }

    int32_t lastkey() const {
        return 0;
    }

    int32_t keystep() const {
        return 0;
    }

} slaveofCommand;

}  // namespace tendisplus
