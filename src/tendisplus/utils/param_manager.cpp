// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include "tendisplus/utils/param_manager.h"

#include <string.h>
#include <stdlib.h>
#include <iostream>

namespace tendisplus {

void ParamManager::init(int argc, char** argv) {
  for (int i = 0; i < argc; ++i) {
    if (strstr(argv[i], "--") == argv[i]) {
      std::string key = "";
      std::string value = "";
      char* pos = strstr(argv[i], "=");
      if (pos == NULL) {
        key = std::string(argv[i] + 2);
      } else {
        key = std::string(argv[i] + 2, pos - argv[i] - 2);
        value = std::string(pos + 1);
      }
      _dict[key] = value;
    }
  }
}

uint64_t ParamManager::getUint64(const char* param,
                                 uint64_t default_value) const {
  auto iter = _dict.find(param);
  if (iter != _dict.end()) {
    return strtoull(iter->second.c_str(), NULL, 10);
  }
  return default_value;
}

std::string ParamManager::getString(const char* param,
                                    std::string default_value) const {
  auto iter = _dict.find(param);
  if (iter != _dict.end()) {
    return iter->second;
  }
  return default_value;
}

}  // namespace tendisplus
