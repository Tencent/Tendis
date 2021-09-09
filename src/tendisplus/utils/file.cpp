// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <sys/file.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <sys/statfs.h>
#include <utility>
#include "glog/logging.h"
#include "tendisplus/utils/file.h"

namespace tendisplus {

std::shared_ptr<AlignedBuff> newAlignedBuff(std::string path,
        int32_t sizeMultiple) {
  struct statfs s;
  if (statfs(path.c_str(), &s)) {
    LOG(ERROR) << "statfs failed:" << path << " " << strerror(errno);
    return nullptr;
  }
  size_t logicalBlockSize = s.f_bsize;
  size_t bufSize = logicalBlockSize * sizeMultiple;
  char* buf;
  int ret = posix_memalign(reinterpret_cast<void **>(&buf),
          logicalBlockSize, bufSize);
  if (ret) {
    LOG(ERROR) << "posix_memalign failed:" << bufSize;
    return nullptr;
  }

  return std::shared_ptr<AlignedBuff>(
    new AlignedBuff{buf, bufSize, logicalBlockSize});
}

std::unique_ptr<rocksdb::WritableFile> openWritableFile(
        const std::string& fullFileName, bool use_direct_writes, bool reOpen) {
  std::unique_ptr<rocksdb::WritableFile> writable_file;  // PosixWritableFile
  rocksdb::EnvOptions options;
  options.use_direct_writes = use_direct_writes;
  options.use_mmap_writes = false;
  rocksdb::Env* env = rocksdb::Env::Default();  // PosixEnv
  rocksdb::Status rS;
  if (reOpen) {
    rS = env->ReopenWritableFile(fullFileName, &writable_file, options);
  } else {
    rS = env->NewWritableFile(fullFileName, &writable_file, options);
  }
  if (!rS.ok()) {
    LOG(ERROR) << "open WritableFile failed:" << rS.ToString();
    return nullptr;
  }
  return std::move(writable_file);
}

}  // namespace tendisplus
