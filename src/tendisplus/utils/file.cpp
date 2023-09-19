// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include "tendisplus/utils/file.h"

#ifndef _WIN32
#include <sys/file.h>
#include <sys/statfs.h>
#endif
#include <sys/stat.h>

#include <cstdlib>
#include <utility>

#include "glog/logging.h"

namespace tendisplus {

#ifdef _WIN32
static int check_align(size_t align) {
  for (size_t i = sizeof(void*); i != 0; i *= 2)
    if (align == i)
      return 0;
  return EINVAL;
}

int posix_memalign(void** ptr, size_t align, size_t size) {
  if (check_align(align))
    return EINVAL;

  int saved_errno = errno;
  void* p = _aligned_malloc(size, align);
  if (p == NULL) {
    errno = saved_errno;
    return ENOMEM;
  }

  *ptr = p;
  return 0;
}
#endif

std::shared_ptr<AlignedBuff> newAlignedBuff(const std::string& path,
                                            int32_t sizeMultiple) {
#ifdef _WIN32
  size_t logicalBlockSize = 512;
#else
  struct statfs s;
  if (statfs(path.c_str(), &s)) {
    LOG(ERROR) << "statfs failed:" << path << " " << strerror(errno);
    return nullptr;
  }
  size_t logicalBlockSize = s.f_bsize;
#endif

  size_t bufSize = logicalBlockSize * sizeMultiple;
  char* buf;
  int ret =
    posix_memalign(reinterpret_cast<void**>(&buf), logicalBlockSize, bufSize);
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
  return writable_file;
}

}  // namespace tendisplus
