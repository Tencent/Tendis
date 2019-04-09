#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>

#include "glog/logging.h"
#include "tendisplus/utils/status.h"
#include "tendisplus/server/server_params.h"

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
    std::string line;
    while (std::getline(file, line)) {
        line = trim(line);
        if (line.size() == 0 || line[0] == '#') {
            continue;
        }
        std::stringstream ss(line);
        std::vector<std::string> tokens;
        std::string tmp;
        while (std::getline(ss, tmp, ' ')) {
            bool isDir = false;
            if (tokens.size() == 1) {
                if (tokens[0] == "dir" ||
                        tokens[0] == "logdir" ||
                        tokens[0] == "dumpdir" ||
                        tokens[0] == "pidfile") {

                    // can't change dir to lower
                    isDir = true;
                }
            } 
            if (!isDir) {
                std::transform(tmp.begin(), tmp.end(), tmp.begin(), tolower);
            }
            tokens.emplace_back(tmp);
        }

        if (tokens.size() == 0) {
            continue;
        } else if (tokens[0] == "bind") {
            if (tokens.size() != 2) {
                return {ErrorCodes::ERR_PARSEOPT, "invalid bind configure"};
            }
            bindIp = tokens[1];
        } else if (tokens[0] == "port") {
            if (tokens.size() != 2) {
                return {ErrorCodes::ERR_PARSEOPT, "invalid port configure"};
            }
            try {
                port = static_cast<uint16_t>(std::stoi(tokens[1]));
            } catch (std::exception& ex) {
                return {ErrorCodes::ERR_PARSEOPT, ex.what()};
            }
        } else if (tokens[0] == "loglevel") {
            if (tokens.size() != 2) {
                return {ErrorCodes::ERR_PARSEOPT, "invalid loglevel configure"};
            }
            if (tokens[1] != "debug" && tokens[1] != "verbose"
                    && tokens[1] != "notice" && tokens[1] != "warning") {
                return {ErrorCodes::ERR_PARSEOPT, "invalid loglevel configure"};
            }
            logLevel = tokens[1];
        } else if (tokens[0] == "logdir") {
            if (tokens.size() != 2) {
                return {ErrorCodes::ERR_PARSEOPT, "invalid logdir configure"};
            }
            logDir = tokens[1];
        } else if (tokens[0] == "dir") {
            if (tokens.size() != 2) {
                return {ErrorCodes::ERR_PARSEOPT, "invalid dir configure"};
            }
            dbPath = tokens[1];
        } else if (tokens[0] == "dumpdir") {
            if (tokens.size() != 2) {
                return {ErrorCodes::ERR_PARSEOPT, "invalid dumpdir configure"};
            }
            dumpPath = tokens[1];
        } else if (tokens[0] == "storage") {
            // currently only support rocks engine
            if (tokens.size() != 2 || tokens[1] != "rocks") {
                return {ErrorCodes::ERR_PARSEOPT, "invalid storage configure"};
            }
        } else if (tokens[0] == "rocks.blockcachemb") {
            if (tokens.size() != 2) {
                return {ErrorCodes::ERR_PARSEOPT,
                    "invalid rocks.blockcache configure"};
            }
            try {
                rocksBlockcacheMB = std::stoi(tokens[1]);
            } catch (std::exception& ex) {
                return {ErrorCodes::ERR_PARSEOPT, ex.what()};
            }
        } else if (tokens[0] == "requirepass") {
            if (tokens.size() != 2) {
                return {ErrorCodes::ERR_PARSEOPT,
                    "invalid requirepass configure"};
            }
            requirepass = tokens[1];
        } else if (tokens[0] == "masterauth") {
            if (tokens.size() != 2) {
                return {ErrorCodes::ERR_PARSEOPT,
                    "invalid masterauth configure"};
            }
            masterauth = tokens[1];
        } else if (tokens[0] == "pidfile") {
            if (tokens.size() != 2) {
                return {ErrorCodes::ERR_PARSEOPT,
                    "invalid pidfile configure"};
            }
            pidFile = tokens[1];
        } else if (tokens[0] == "delcntindexmgr") {
            if (tokens.size() != 2) {
                return {ErrorCodes::ERR_PARSEOPT,
                  "invalid delcntindexmgr config"};
            }
            delCntIndexMgr = std::stoi(tokens[1]);
        } else if (tokens[0] == "deljobcntindexmgr") {
            if (tokens.size() != 2) {
                return {ErrorCodes::ERR_PARSEOPT,
                  "invalid deljobcntindexmgr config"};
            }
            delJobCntIndexMgr = std::stoi(tokens[1]);
        } else if (tokens[0] == "scancntindexmgr") {
            if (tokens.size() != 2) {
                return {ErrorCodes::ERR_PARSEOPT,
                  "invalid scancntindexmgr config"};
            }
            scanCntIndexMgr = std::stoi(tokens[1]);
        } else if (tokens[0] == "scanjobcntindexmgr") {
            if (tokens.size() != 2) {
                return {ErrorCodes::ERR_PARSEOPT,
                  "invalid scanjobcntindexmgr config"};
            }
            scanJobCntIndexMgr = std::stoi(tokens[1]);
        } else if (tokens[0] == "pausetimeindexmgr") {
            if (tokens.size() != 2) {
                return {ErrorCodes::ERR_PARSEOPT,
                  "invalid pausetimeindexmgr config"};
            }
            pauseTimeIndexMgr = std::stoi(tokens[1]);
        }
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
    delJobCntIndexMgr= 1;
    pauseTimeIndexMgr = 10;
}


}  // namespace tendisplus
