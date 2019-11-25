
#ifndef _WIN32
#include <sys/time.h>
#include <sys/utsname.h>
#endif

#include <string>
#include <utility>
#include <memory>
#include <algorithm>
#include <cctype>
#include <vector>
#include <clocale>
#include <map>
#include <list>
#include "glog/logging.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/sync_point.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/utils/scopeguard.h"
#include "tendisplus/utils/base64.h"
#include "tendisplus/storage/varint.h"

namespace tendisplus {

class ClusterCommand: public Command {
 public:
    ClusterCommand()
        :Command("cluster", "rs") {
    }

    ssize_t arity() const {
        return -6;
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

    bool sameWithRedis() const {
        return false;
    }

    Expected<std::string> run(Session* sess) final {
        std::shared_ptr<ServerEntry> svr = sess->getServerEntry();
        INVARIANT(svr != nullptr);
        const auto& args = sess->getArgs();
        auto migrateMgr = svr->getMigrateManager();
        INVARIANT(migrateMgr != nullptr);

        if (args[1] == "setslot") {
            Expected<uint64_t> exptChunkid = ::tendisplus::stoul(args[2]);
            if (!exptChunkid.ok()) {
                return exptChunkid.status();
            }
            uint32_t chunkid = (uint32_t)exptChunkid.value();

            string ip = args[4];
            Expected<uint64_t> exptPort = ::tendisplus::stoul(args[5]);
            if (!exptPort.ok()) {
                return exptPort.status();
            }
            uint16_t port = (uint16_t)exptPort.value();
            Status s;
            if (args[3] == "migrating") {
                LOG(INFO) << "cluster setslot migrating," << chunkid << " " << ip << ":" << port;
                s = migrateMgr->migrating(chunkid, ip, port);
            } else if (args[3] == "importing") {
                LOG(INFO) << "cluster setslot importing," << chunkid << " " << ip << ":" << port;
                s = migrateMgr->importing(chunkid, ip, port);
            }
            if (!s.ok()) {
                return Command::fmtErr(s.toString());
            }
        }
		
		if (!svr->isClusterEnabled()) {
            return { ErrorCodes::ERR_CLUSTER,
                    "This instance has cluster support disabled" };
        }

        const auto& clusterState = svr->getClusterMgr()->getClusterState();
        auto& args = sess->getArgs();
        const std::string arg1 = toLower(args[1]);
        auto argSize = sess->getArgs().size();
        if (arg1 == "meet" && (argSize == 4 || argSize == 5)) {
            /* CLUSTER MEET <ip> <port> [cport] */
            uint64_t port, cport;

            auto& host = args[2];
            auto eport = ::tendisplus::stoul(args[3]);
            if (!eport.ok()) {
                return{ ErrorCodes::ERR_CLUSTER,
                        "Invalid TCP base port specified " + args[3] };
            }
            port = eport.value();

            if (argSize == 5) {
                auto ecport = ::tendisplus::stoul(args[4]);
                if (!ecport.ok()) {
                    return{ ErrorCodes::ERR_CLUSTER,
                            "Invalid TCP bus port specified " + args[4] };
                }
                cport = ecport.value();
            } else {
                cport = port + CLUSTER_PORT_INCR;
            }

            clusterState->clusterStartHandshake(host,
                            port, cport);

            return{ ErrorCodes::ERR_OK, "" };
        }

        return{ ErrorCodes::ERR_CLUSTER,
                "Invalid cluster command " + args[1] };
		
        return Command::fmtOK();
    }
} clusterCmd;

class ReadymigrateCommand: public Command {
public:
    ReadymigrateCommand()
            :Command("readymigrate", "a") {
    }

    ssize_t arity() const {
        return 3;
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

    Expected<std::string> run(Session *sess) final {
        LOG(FATAL) << "readymigrate should not be called";
        // void compiler complain
        return {ErrorCodes::ERR_INTERNAL, "shouldn't be called"};
    }
} readymigrateCommand;

class MigrateendCommand: public Command {
public:
    MigrateendCommand()
            :Command("migrateend", "rs") {
    }

    ssize_t arity() const {
        return 3;
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

    bool sameWithRedis() const {
        return false;
    }

    Expected<std::string> run(Session* sess) final {
        std::shared_ptr<ServerEntry> svr = sess->getServerEntry();
        INVARIANT(svr != nullptr);
        const auto& args = sess->getArgs();
        auto migrateMgr = svr->getMigrateManager();
        INVARIANT(migrateMgr != nullptr);

        Expected<uint64_t> exptChunkid = ::tendisplus::stoul(args[1]);
        if (!exptChunkid.ok()) {
            return exptChunkid.status();
        }
        uint32_t chunkid = (uint32_t)exptChunkid.value();

        auto s = migrateMgr->supplyMigrateEnd(chunkid);
        if (!s.ok()) {
            return s;
        }
        return Command::fmtOK();
    }
} migrateendCmd;

}  // namespace tendisplus
