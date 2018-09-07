#include <string>
#include <utility>
#include <memory>
#include <algorithm>
#include <cctype>
#include <clocale>
#include "glog/logging.h"
#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/commands/command.h"

namespace tendisplus {

class DebugCommand: public Command {
 public:
    DebugCommand()
        :Command("debug") {
    }

    ssize_t arity() const {
        return -1;
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

    Expected<std::string> run(NetSession *sess) final {
        std::shared_ptr<ServerEntry> svr = sess->getServerEntry();
        INVARIANT(svr != nullptr);
        const SegmentMgr *segMgr = svr->getSegmentMgr(); 
        INVARIANT(segMgr != nullptr);
        ReplManager *replMgr = svr->getReplManager();
        INVARIANT(replMgr != nullptr);

        rapidjson::StringBuffer sb;
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(sb);
        writer.StartObject();

        writer.Key("Stores");
        writer.StartObject();
        for (uint32_t i = 0; i < KVStore::INSTANCE_NUM; ++i) {
            PStore store = segMgr->getInstanceById(i);
            std::stringstream ss;
            ss << "Stores_" << i;
            writer.Key(ss.str().c_str());
            writer.StartObject();
            store->appendJSONStat(writer);
            writer.EndObject();
        }
        writer.EndObject();

        writer.Key("repl");
        writer.StartObject();
        replMgr->appendJSONStat(writer);
        writer.EndObject();

        writer.EndObject();
        return Command::fmtBulk(std::string(sb.GetString()));
    }
} debugCommand;

}  // namespace tendisplus
