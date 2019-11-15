#ifndef _WIN32
#include <sys/time.h>
#include <sys/utsname.h>
#endif

#include <string>
#include "glog/logging.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/utils/invariant.h"

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
