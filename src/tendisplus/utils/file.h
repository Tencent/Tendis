// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_UTILS_FILE_H_
#define SRC_TENDISPLUS_UTILS_FILE_H_

#include <memory>
#include <string>
#include "rocksdb/db.h"
#include "tendisplus/utils/status.h"

namespace tendisplus {

struct AlignedBuff {
  ~AlignedBuff() {
    free(buf);
  }
  char* buf;
  size_t bufSize;
  size_t logicalBlockSize;
};

std::shared_ptr<AlignedBuff> newAlignedBuff(std::string path,
        int32_t sizeMultiple = 16);

std::unique_ptr<rocksdb::WritableFile> openWritableFile(
        const std::string& fullFileName, bool use_direct_writes, bool reOpen);

}  // namespace tendisplus
#endif  // SRC_TENDISPLUS_UTILS_FILE_H_
