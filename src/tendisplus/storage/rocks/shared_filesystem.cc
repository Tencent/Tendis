// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include "tendisplus/storage/rocks/shared_filesystem.h"

#include "tendisplus/storage/rocks/nfs_filesystem.h"
#ifdef HDFS
#include "tendisplus/storage/rocks/plugin/hdfs/env_hdfs.h"
#endif

#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>

namespace ROCKSDB_NAMESPACE {

Status CreateSharedFileSystem(const std::shared_ptr<FileSystem>& base,
                              const std::string& uri,
                              std::shared_ptr<FileSystem>* result) {
  result->reset();

  if (uri.empty()) {
    return Status::InvalidArgument("URI cannot be empty");
  }

  // Determine filesystem type from URI scheme
  if (uri.find("nfs://") == 0) {
    // NFS filesystem
    std::unique_ptr<FileSystem> nfs_fs;
    Status s = NFSFileSystem::Create(base, uri, &nfs_fs);
    if (!s.ok()) {
      return s;
    }
    result->reset(nfs_fs.release());
    return Status::OK();
  }
#ifdef HDFS
  if (uri.find("hdfs://") == 0) {
    // HDFS filesystem
    std::unique_ptr<FileSystem> hdfs_fs;
    Status s = HdfsFileSystem::Create(base, uri, &hdfs_fs);
    if (!s.ok()) {
      return s;
    }
    result->reset(hdfs_fs.release());
    return Status::OK();
  }
#endif
  // Future: Add support for S3, GCS, etc.
  return Status::NotSupported("Unsupported filesystem URI scheme: " + uri);
}

Status CreateSharedFileSystemEnv(const std::string& uri,
                                 std::unique_ptr<Env>* result) {
  result->reset();

  std::shared_ptr<FileSystem> fs;
  Status s = CreateSharedFileSystem(FileSystem::Default(), uri, &fs);
  if (!s.ok()) {
    return s;
  }

  *result = NewCompositeEnv(fs);
  return Status::OK();
}

bool RegisterSharedFileSystemPathPrefix(const std::shared_ptr<FileSystem>& fs,
                                        const std::string& prefix) {
  if (!fs || prefix.empty()) {
    return true;  // Nothing to register
  }

  // Try to cast to NFSFileSystem (path-based filesystem)
  auto* nfs_fs = dynamic_cast<NFSFileSystem*>(fs.get());
  if (nfs_fs) {
    nfs_fs->RegisterPathPrefix(prefix);
    return true;
  }

  // For other filesystems (HDFS, S3, etc.), path registration is typically not
  // needed because they use URI-based paths directly. Return true to indicate
  // success.
  //
  // If future filesystems need path registration, add similar dynamic_cast
  // checks here.

  return true;  // Registration not needed for this filesystem type
}

std::string ConvertLocalPathToSharedURI(const std::string& local_path,
                                        const std::string& shared_fs_uri,
                                        const std::string& mount_point) {
  if (shared_fs_uri.empty() || local_path.empty()) {
    return local_path;  // Return as-is if invalid
  }

  // If local_path is already a URI, return as-is
  if (IsSharedFilesystemURI(local_path)) {
    return local_path;
  }

  // Convert local_path to absolute path
  std::string abs_local_path = local_path;
  if (!local_path.empty() && local_path[0] != '/') {
    char cwd[PATH_MAX];
    if (getcwd(cwd, sizeof(cwd)) != nullptr) {
      abs_local_path = std::string(cwd) + "/" + local_path;
      // Normalize
      size_t pos;
      while ((pos = abs_local_path.find("/./")) != std::string::npos) {
        abs_local_path.erase(pos, 2);
      }
    }
  }

  // Determine mount point
  std::string actual_mount_point = mount_point;
  if (actual_mount_point.empty()) {
    // Auto-infer: find the common base directory
    // For db_path like "./home/db/0", we want to find the working directory as
    // mount point Strategy: go up 3 levels from db_path to get the mount point
    // (workdir) Example: "/path/to/home/db/0" -> "/path/to" (go up 3 levels:
    // remove "0", "db", and "home") This ensures relative_path includes
    // "home/db/0" which matches NFS structure
    std::string path = abs_local_path;
    // Remove last component (e.g., "0" from "home/db/0")
    size_t last_slash = path.rfind('/');
    if (last_slash != std::string::npos && last_slash > 0) {
      path = path.substr(0, last_slash);
      // Remove second-to-last component (e.g., "db" from "home/db")
      last_slash = path.rfind('/');
      if (last_slash != std::string::npos && last_slash > 0) {
        path = path.substr(0, last_slash);
        // Remove third-to-last component (e.g., "home" from "home")
        last_slash = path.rfind('/');
        if (last_slash != std::string::npos && last_slash > 0) {
          actual_mount_point = path.substr(0, last_slash);
        } else {
          // Fallback: use parent of "home" (workdir)
          actual_mount_point = path;
        }
      } else {
        // Fallback: use parent of local_path
        actual_mount_point = path;
      }
    } else {
      // Cannot infer, return as-is
      return local_path;
    }
  } else {
    // Convert mount_point to absolute path
    if (mount_point[0] != '/') {
      char cwd[PATH_MAX];
      if (getcwd(cwd, sizeof(cwd)) != nullptr) {
        actual_mount_point = std::string(cwd) + "/" + mount_point;
        // Normalize
        size_t pos;
        while ((pos = actual_mount_point.find("/./")) != std::string::npos) {
          actual_mount_point.erase(pos, 2);
        }
        if (actual_mount_point.find("./") == 0) {
          actual_mount_point = actual_mount_point.substr(2);
        }
      }
    }
  }

  // Check if local_path starts with mount_point
  if (abs_local_path.find(actual_mount_point) != 0) {
    // Doesn't match, return as-is
    return local_path;
  }

  // Extract relative path
  std::string relative_path =
    abs_local_path.substr(actual_mount_point.length());
  while (!relative_path.empty() && relative_path[0] == '/') {
    relative_path = relative_path.substr(1);
  }

  // Debug output
  std::cerr << "[ConvertLocalPathToSharedURI] Path conversion:" << std::endl;
  std::cerr << "  local_path: " << local_path << std::endl;
  std::cerr << "  abs_local_path: " << abs_local_path << std::endl;
  std::cerr << "  mount_point (config): " << mount_point << std::endl;
  std::cerr << "  actual_mount_point (inferred): " << actual_mount_point
            << std::endl;
  std::cerr << "  relative_path: " << relative_path << std::endl;
  std::cerr.flush();

  // Build URI
  size_t proto_end = shared_fs_uri.find("://");
  if (proto_end == std::string::npos) {
    std::cerr << "[ConvertLocalPathToSharedURI] ERROR: Invalid URI format: "
              << shared_fs_uri << std::endl;
    std::cerr.flush();
    return local_path;  // Invalid URI
  }

  size_t path_start = shared_fs_uri.find('/', proto_end + 3);
  if (path_start == std::string::npos) {
    // No path in URI, just append
    std::string result = shared_fs_uri + "/" + relative_path;
    std::cerr << "[ConvertLocalPathToSharedURI] Result (no base path): "
              << result << std::endl;
    std::cerr.flush();
    return result;
  }

  std::string server_part = shared_fs_uri.substr(0, path_start);
  std::string base_path = shared_fs_uri.substr(path_start);
  if (base_path.back() != '/') {
    base_path += "/";
  }

  std::string result = server_part + base_path + relative_path;
  std::cerr << "[ConvertLocalPathToSharedURI] Result: " << result << std::endl;
  std::cerr.flush();
  return result;
}

bool IsSharedFilesystemURI(const std::string& path) {
  return path.find("nfs://") == 0 || path.find("hdfs://") == 0 ||
    path.find("s3://") == 0 || path.find("gcs://") == 0 ||
    path.find("file://") == 0;
}

}  // namespace ROCKSDB_NAMESPACE
