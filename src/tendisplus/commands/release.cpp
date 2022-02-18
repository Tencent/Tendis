// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <string.h>

#include "tendisplus/commands/release.h"
#include "tendisplus/commands/version.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/commands/version.h"
#include "rocksdb/version.h"
#include <iostream>
#include <sstream>
#include <string>

uint64_t redisBuildId(void) {
  std::stringstream buildidstr;
  buildidstr << TENDISPLUS_VERSION_PRE << "." << __ROCKSDB_MAJOR__ << "." 
    << __ROCKSDB_MINOR__ << "." << __ROCKSDB_PATCH__ << TENDISPLUS_BUILD_ID 
    << TENDISPLUS_GIT_DIRTY << TENDISPLUS_GIT_SHA1;

  std::string buildid = buildidstr.str();
  return tendisplus::redis_port::crc64(
    0, (unsigned char*)buildid.c_str(), buildid.length());
}

std::string getTendisPlusVersion() {
  std::stringstream tendisver;
  tendisver << TENDISPLUS_VERSION_PRE << "." << __ROCKSDB_MAJOR__ << "."
     << __ROCKSDB_MINOR__ << "." << __ROCKSDB_PATCH__;
  return tendisver.str();
}
