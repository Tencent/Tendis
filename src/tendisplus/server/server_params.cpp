#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>

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
  return str.substr(0,str.find_last_not_of(pattern) + 1);
}

static std::string trim(const std::string& str) {
  return trim_left(trim_right(str));
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
            tokens.emplace_back(tmp);
        }
        if (tokens.size() == 0) {
            continue;
        }
        if (tokens[0] == "bind") {
            if (tokens.size() != 2) {
                return {ErrorCodes::ERR_PARSEOPT, "invalid bind configure"};
            }
            bindIp = tokens[1];
        }
        if (tokens[0] == "port") {
            if (tokens.size() != 2) {
                return {ErrorCodes::ERR_PARSEOPT, "invalid port configure"};
            }
            try {
                port = std::stoi(tokens[1]);
            } catch (std::exception& ex) {
                return {ErrorCodes::ERR_PARSEOPT, ex.what()};
            }
        }
    }
}

ServerParams::ServerParams()
        :bindIp("127.0.0.1"),
         port(8903) {
}


}  // namespace tendisplus
