#include <string>
#include <utility>
#include <memory>
#include <algorithm>
#include <cctype>
#include <clocale>
#include "glog/logging.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

class FullSyncCommand: public Command {
 public:
    FullSyncCommand()
        :Command("fullsync") {
    }

    Expected<uint32_t> parse(NetSession *sess) const {
        const auto& args = setss->getArgs();
        if (args.size() != 2) {
            return {ErrorCodes::ERR_PARSEPKT, "invalid  fullsync params"};
        }
        uint32_t storeId = 0;
        try {
            storeId = std::stoi(args[1]);
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

        /*
        Expected<uint32_t> params = parse(sess);
        if (!params.ok()) {
            return params.status();
        }

        PStore kvstore = getStore(sess, params.value().key);
        assert(kvstore);
        if (!kvstore->isRunning()) {
            return {ErrorCodes::ERR_INTERNAL, "store is not running"};
        }

        auto serverEntry = sess->getServerEntry();
        assert(serverEntry);
        // NOTE(deyukong): this judge is not precise
        // even it's not full at this time, it can be full during schedule.
        if (serverEntry->getReplManager()->isFullSupplierFull()) {
            return {ErrorCodes::ERR_BUSY, "schedule pool is busy"};
        }
        Expected<KVStore::BackupInfo> bkInfo = kvstore->backup();
        if (!bkInfo.ok()) {
            return {ErrorCodes::ERR_INTERNAL, bkInfo.status().toString()};
        }
        rapidjson::StringBuffer sb;
        rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
        for (const auto& kv : bkInfo.value()) {
            writer.Key(kv->first);
            writer.Uint64(kv->second);
        }
        return writer.GetString();
        */
    }
};

}  // namespace tendisplus
