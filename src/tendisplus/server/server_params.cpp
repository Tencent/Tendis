#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>

#include "glog/logging.h"
#include "tendisplus/utils/status.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/server/server_params.h"
#include "tendisplus/utils/redis_port.h"

namespace tendisplus {

static std::string trim_left(const std::string& str) {
  const std::string pattern = " \f\n\r\t\v";
  return str.substr(str.find_first_not_of(pattern));
}

static std::string trim_right(const std::string& str) {
  const std::string pattern = " \f\n\r\t\v";
  return str.substr(0, str.find_last_not_of(pattern) + 1);
}

static std::string trim(const std::string& str) {
  return trim_left(trim_right(str));
}

std::string ServerParams::toString() const {
    std::stringstream ss;
    ss << "\nbindIp:" << bindIp
        << ",\nport:" << port
        << ",\nlogLevel:" << logLevel
        << ",\nlogDir:" << logDir
        << ",\ndumpDir:" << dumpPath
        << ",\nstorageEngine:" << storageEngine
        << ",\ndbPath:" << dbPath
        << ",\nrocksBlockCacheMB:" << rocksBlockcacheMB
        << ",\nrequirepass:" << requirepass
        << ",\nmasterauth:" << masterauth
        << ",\npidFile:" << pidFile
        << ",\ngenerallog:" << generalLog
        << ",\nchunkSize:" << chunkSize
        << ",\nkvStoreCount:" << kvStoreCount
        << std::endl;
    return ss.str();
}

Status ServerParams::parseFile(const std::string& filename) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        std::stringstream ss;
        ss << "open file:" << filename << " failed";
        return {ErrorCodes::ERR_PARSEOPT, ss.str()};
    }
    std::vector<std::string> tokens;
    try {
        std::string line;
        while (std::getline(file, line)) {
            line = trim(line);
            if (line.size() == 0 || line[0] == '#') {
                continue;
            }
            std::stringstream ss(line);
            tokens.clear();
            std::string tmp;
            while (std::getline(ss, tmp, ' ')) {
                bool caseSensitive = false;
                if (tokens.size() == 1) {
                    if (toLower(tokens[0]) == "dir" ||
                        toLower(tokens[0]) == "logdir" ||
                        toLower(tokens[0]) == "dumpdir" ||
                        toLower(tokens[0]) == "pidfile" ||
                        toLower(tokens[0]) == "masterauth" ||
                        toLower(tokens[0]) == "requirepass") {
                        // can't change dir to lower
                        caseSensitive = true;
                    }
                }
                if (!caseSensitive) {
                    std::transform(tmp.begin(), tmp.end(),
                                    tmp.begin(), tolower);
                }

                tokens.emplace_back(tmp);
            }

            if (tokens.size() == 0) {
                continue;
            } else if (tokens[0] == "bind") {
                if (tokens.size() != 2) {
                    return{ ErrorCodes::ERR_PARSEOPT,
                                "invalid bind configure" };
                }
                bindIp = tokens[1];
            } else if (tokens[0] == "port") {
                if (tokens.size() != 2) {
                    return{ ErrorCodes::ERR_PARSEOPT,
                                "invalid port configure" };
                }
                port = static_cast<uint16_t>(std::stoi(tokens[1]));
            } else if (tokens[0] == "loglevel") {
                if (tokens.size() != 2) {
                    return{ ErrorCodes::ERR_PARSEOPT,
                               "invalid loglevel configure" };
                }
                if (tokens[1] != "debug" && tokens[1] != "verbose"
                    && tokens[1] != "notice" && tokens[1] != "warning") {
                    return{ ErrorCodes::ERR_PARSEOPT,
                                    "invalid loglevel configure" };
                }
                logLevel = tokens[1];
            } else if (tokens[0] == "logdir") {
                if (tokens.size() != 2) {
                    return{ ErrorCodes::ERR_PARSEOPT,
                                    "invalid logdir configure" };
                }
                logDir = tokens[1];
            } else if (tokens[0] == "dir") {
                if (tokens.size() != 2) {
                    return{ ErrorCodes::ERR_PARSEOPT,
                                    "invalid dir configure" };
                }
                dbPath = tokens[1];
            } else if (tokens[0] == "dumpdir") {
                if (tokens.size() != 2) {
                    return{ ErrorCodes::ERR_PARSEOPT,
                                "invalid dumpdir configure" };
                }
                dumpPath = tokens[1];
            } else if (tokens[0] == "storage") {
                // currently only support rocks engine
                if (tokens.size() != 2 || tokens[1] != "rocks") {
                    return{ ErrorCodes::ERR_PARSEOPT,
                                "invalid storage configure" };
                }
            } else if (tokens[0] == "rocks.blockcachemb") {
                if (tokens.size() != 2) {
                    return{ ErrorCodes::ERR_PARSEOPT,
                        "invalid rocks.blockcache configure" };
                }
                rocksBlockcacheMB = std::stoi(tokens[1]);
            } else if (tokens[0] == "requirepass") {
                if (tokens.size() != 2) {
                    return{ ErrorCodes::ERR_PARSEOPT,
                        "invalid requirepass configure" };
                }
                requirepass = tokens[1];
            } else if (tokens[0] == "masterauth") {
                if (tokens.size() != 2) {
                    return{ ErrorCodes::ERR_PARSEOPT,
                        "invalid masterauth configure" };
                }
                masterauth = tokens[1];
            } else if (tokens[0] == "pidfile") {
                if (tokens.size() != 2) {
                    return{ ErrorCodes::ERR_PARSEOPT,
                        "invalid pidfile configure" };
                }
                pidFile = tokens[1];
            } else if (tokens[0] == "delcntindexmgr") {
                if (tokens.size() != 2) {
                    return{ ErrorCodes::ERR_PARSEOPT,
                      "invalid delcntindexmgr config" };
                }
                delCntIndexMgr = std::stoi(tokens[1]);
            } else if (tokens[0] == "deljobcntindexmgr") {
                if (tokens.size() != 2) {
                    return{ ErrorCodes::ERR_PARSEOPT,
                      "invalid deljobcntindexmgr config" };
                }
                delJobCntIndexMgr = std::stoi(tokens[1]);
            } else if (tokens[0] == "scancntindexmgr") {
                if (tokens.size() != 2) {
                    return{ ErrorCodes::ERR_PARSEOPT,
                      "invalid scancntindexmgr config" };
                }
                scanCntIndexMgr = std::stoi(tokens[1]);
            } else if (tokens[0] == "scanjobcntindexmgr") {
                if (tokens.size() != 2) {
                    return{ ErrorCodes::ERR_PARSEOPT,
                      "invalid scanjobcntindexmgr config" };
                }
                scanJobCntIndexMgr = std::stoi(tokens[1]);
            } else if (tokens[0] == "pausetimeindexmgr") {
                if (tokens.size() != 2) {
                    return{ ErrorCodes::ERR_PARSEOPT,
                      "invalid pausetimeindexmgr config" };
                }
                pauseTimeIndexMgr = std::stoi(tokens[1]);
            //} else if (tokens[0] == "chunkSize") {
            //    if (tokens.size() != 2) {
            //        return{ ErrorCodes::ERR_PARSEOPT,
            //            "invalid chunkSize config" };
            //    }
            //    chunkSize = std::stoi(tokens[1]);
            } else if (tokens[0] == "kvStoreCount") {
                if (tokens.size() != 2) {
                    return{ ErrorCodes::ERR_PARSEOPT,
                        "invalid kvStoreCount config" };
                }
                kvStoreCount = std::stoi(tokens[1]);
            } else if (tokens[0] == "version-increase") {
                if (tokens.size() != 2) {
                    return{ ErrorCodes::ERR_PARSEOPT,
                      "invalid version-increase config" };
                }
                if (!isOptionOn(tokens[1])) {
                    versionIncrease = false;
                }
            } else if (tokens[0] == "generallog") {
                if (tokens.size() != 2) {
                    return{ ErrorCodes::ERR_PARSEOPT,
                        "invalid version-increase config" };
                }
                if (isOptionOn(tokens[1])) {
                    generalLog = true;
                }
            } else if (tokens[0] == "checkkeytypeforsetcmd") {
                if (tokens.size() != 2) {
                    return{ ErrorCodes::ERR_PARSEOPT,
                        "invalid samekeydifftype config" };
                }
                if (isOptionOn(tokens[1])) {
                    checkKeyTypeForSet = true;
                }
            } else if (tokens[0] == "proto-max-bulk-len") {
                if (tokens.size() != 2) {
                    return{ ErrorCodes::ERR_PARSEOPT,
                        "invalid proto-max-bulk-len config" };
                }
                protoMaxBulkLen = std::stoi(tokens[1]);
            } else if (tokens[0] == "databases") {
                if (tokens.size() != 2) {
                    return{ ErrorCodes::ERR_PARSEOPT,
                        "Invalid number of databases" };
                }
                dbNum = std::stoi(tokens[1]);
            }
        }
    } catch (const std::exception& ex) {
        std::stringstream ss;
        ss << "invalid " << tokens[0] << " config: " << ex.what();
        return{ ErrorCodes::ERR_PARSEOPT, ss.str() };
    }

    return {ErrorCodes::ERR_OK, ""};
}

ServerParams::ServerParams()
        :bindIp("127.0.0.1"),
         port(8903),
         logLevel(""),
         logDir("./"),
         storageEngine("rocks"),
         dbPath("./db"),
         dumpPath("./dump"),
         rocksBlockcacheMB(4096),
         requirepass(""),
         masterauth(""),
         pidFile("./tendisplus.pid") {
    scanCntIndexMgr = 1000;
    scanJobCntIndexMgr = 1;
    delCntIndexMgr = 10000;
    delJobCntIndexMgr = 1;
    pauseTimeIndexMgr = 10;
    versionIncrease = true;
    generalLog = false;
    kvStoreCount = 10;
    chunkSize = 0x4000;  // same as rediscluster
    checkKeyTypeForSet = false;
    protoMaxBulkLen = CONFIG_DEFAULT_PROTO_MAX_BULK_LEN;
    dbNum = CONFIG_DEFAULT_DBNUM;
}


}  // namespace tendisplus
