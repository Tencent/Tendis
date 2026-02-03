// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#pragma once

#include <memory>
#include <string>

#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/status.h"
namespace ROCKSDB_NAMESPACE {

// Unified interface for creating shared file systems
// Supports: nfs://, hdfs://, s3://, etc.
Status CreateSharedFileSystem(const std::shared_ptr<FileSystem>& base,
                              const std::string& uri,
                              std::shared_ptr<FileSystem>* result);

// Create Env from shared file system URI
Status CreateSharedFileSystemEnv(const std::string& uri,
                                 std::unique_ptr<Env>* result);

// Helper: Convert local path to shared filesystem URI
// This is used when RocksDB uses local paths but we want to use shared storage
std::string ConvertLocalPathToSharedURI(const std::string& local_path,
                                        const std::string& shared_fs_uri,
                                        const std::string& mount_point = "");

// Helper: Check if a path is a shared filesystem URI
bool IsSharedFilesystemURI(const std::string& path);

// Unified interface for registering additional path prefixes to shared
// filesystem This is useful for path-based filesystems (like NFS) that need to
// know which local paths should be mapped to the shared storage. For URI-based
// filesystems (like HDFS, S3), this is typically a no-op.
//
// @param fs The shared filesystem instance
// @param prefix The local path prefix to register (e.g., wal_dir, db_log_dir)
// @return true if successfully registered (or if registration not needed),
// false on error
bool RegisterSharedFileSystemPathPrefix(const std::shared_ptr<FileSystem>& fs,
                                        const std::string& prefix);

}  // namespace ROCKSDB_NAMESPACE
