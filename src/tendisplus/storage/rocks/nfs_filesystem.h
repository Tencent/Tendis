// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include "rocksdb/status.h"
// Choose one based on the actual installation
#if __has_include(<nfsc/libnfs.h>)
#include <nfsc/libnfs.h>
#elif __has_include(<nfs/libnfs.h>)
#include <nfs/libnfs.h>
#elif __has_include(<libnfs.h>)
#include <libnfs.h>
#else
#error "libnfs header not found"
#endif
namespace ROCKSDB_NAMESPACE {

class NFSFileSystem : public FileSystemWrapper {
 public:
  static const char* kClassName() {
    return "NFSFileSystem";
  }
  static const char* kNickName() {
    return "nfs";
  }
  static constexpr const char* kProto = "nfs://";

  explicit NFSFileSystem(const std::string& nfs_url,
                         const std::string& local_prefix,
                         const std::shared_ptr<FileSystem>& base);

  ~NFSFileSystem() override;

  const char* Name() const override {
    return kClassName();
  }
  const char* NickName() const override {
    return kNickName();
  }

  std::string GetId() const override;

  Status ValidateOptions(const DBOptions& db_opts,
                         const ColumnFamilyOptions& cf_opts) const override;

  // Get configuration information
  const std::string& GetNFSUrl() const {
    return nfs_url_;
  }
  const std::string& GetLocalPrefix() const {
    return local_prefix_;
  }

  // Add an additional path prefix to be mapped to NFS
  // This is useful for mapping wal_dir, db_log_dir, etc. to NFS
  void RegisterPathPrefix(const std::string& prefix);

  // Create NFSFileSystem from URI (similar to HdfsFileSystem::Create)
  // URI format: nfs://server/path or nfs://server:port/path
  static Status Create(const std::shared_ptr<FileSystem>& base,
                       const std::string& uri,
                       std::unique_ptr<FileSystem>* result);

  // Key methods for rewriting FileSystem (declare, not implement)
  IOStatus NewSequentialFile(const std::string& fname,
                             const FileOptions& options,
                             std::unique_ptr<FSSequentialFile>* result,
                             IODebugContext* dbg) override;

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& options,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override;

  IOStatus NewWritableFile(const std::string& fname,
                           const FileOptions& options,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override;

  // Rewrite NewDirectory to open directory handles (for fsync, etc.)
  IOStatus NewDirectory(const std::string& name,
                        const IOOptions& io_opts,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override;

  // Reopen the file in append mode (for LOG files, etc.)
  IOStatus ReopenWritableFile(const std::string& fname,
                              const FileOptions& options,
                              std::unique_ptr<FSWritableFile>* result,
                              IODebugContext* dbg) override;

  IOStatus FileExists(const std::string& fname,
                      const IOOptions& options,
                      IODebugContext* dbg) override;

  IOStatus GetChildren(const std::string& dir,
                       const IOOptions& options,
                       std::vector<std::string>* result,
                       IODebugContext* dbg) override;

  IOStatus DeleteFile(const std::string& fname,
                      const IOOptions& options,
                      IODebugContext* dbg) override;
  // Catalog operations
  IOStatus CreateDir(const std::string& dirname,
                     const IOOptions& options,
                     IODebugContext* dbg) override;

  IOStatus CreateDirIfMissing(const std::string& dirname,
                              const IOOptions& options,
                              IODebugContext* dbg) override;

  IOStatus DeleteDir(const std::string& dirname,
                     const IOOptions& options,
                     IODebugContext* dbg) override;

  IOStatus GetFileSize(const std::string& fname,
                       const IOOptions& options,
                       uint64_t* file_size,
                       IODebugContext* dbg) override;

  IOStatus GetFileModificationTime(const std::string& fname,
                                   const IOOptions& options,
                                   uint64_t* file_mtime,
                                   IODebugContext* dbg) override;

  IOStatus RenameFile(const std::string& src,
                      const std::string& target,
                      const IOOptions& options,
                      IODebugContext* dbg) override;

  // Rewrite LockFile and UnlockFile
  IOStatus LockFile(const std::string& fname,
                    const IOOptions& options,
                    FileLock** lock,
                    IODebugContext* dbg) override;

  IOStatus UnlockFile(FileLock* lock,
                      const IOOptions& options,
                      IODebugContext* dbg) override;

  // Rewrite NewLogger, making sure to use our NewWritableFile
  IOStatus NewLogger(const std::string& fname,
                     const IOOptions& io_opts,
                     std::shared_ptr<Logger>* result,
                     IODebugContext* dbg) override;

  IOStatus IsDirectory(const std::string& path,
                       const IOOptions& options,
                       bool* is_dir,
                       IODebugContext* dbg) override;

  // Rewrite SupportedOps to avoid errors when calling target_
  void SupportedOps(int64_t& supported_ops) override {
    supported_ops = 0;
    // NFS doesn't support asynchronous IO, so don't set kAsyncIO
  }

 private:
  std::string nfs_url_;       // NFS remote URL
  std::string local_prefix_;  // Local path prefix (primary)
  std::vector<std::string>
    additional_prefixes_;  // Additional path prefixes to map to NFS
  struct nfs_context* nfs_ctx_;
  std::mutex nfs_mutex_;

  void InitNFSContext();

  // Check if the file is a data file (.sst, .blob, .ldb)
  bool IsDataFile(const std::string& path) const;

  // Check if the path should be accessed via NFS
  // Supports both URI format (nfs://server/path) and path matching (legacy)
  // ALL files under local_prefix_ and additional_prefixes_ go to NFS to ensure
  // consistency between local Tendis and remote workers
  bool IsNFSPath(const std::string& path) const;

  // Convert the local path or URI to an NFS-relative path
  // URI format: "nfs://server/path/to/file" -> "/path/to/file"
  // Path matching: "/mnt/nfs_rocksdb/db/0/xxx.sst" -> "/0/xxx.sst"
  // Auto-convert: If path matches local_prefix_, automatically convert to URI
  // internally
  std::string ConvertToNFSPath(const std::string& path) const;

  // Convert local path to URI (if it matches local_prefix_)
  // Example: "/mnt/rocksdb/db/0/xxx.sst" ->
  // "nfs://localhost/shared/rocksdb/db/0/xxx.sst" Returns empty string if path
  // doesn't match or URI conversion is not needed
  std::string ConvertLocalPathToURI(const std::string& path) const;

  // Extract NFS path from URI
  // "nfs://server/path/to/file" -> "/path/to/file"
  // Returns path relative to the NFS mount point
  std::string ExtractNFSPathFromURI(const std::string& uri) const;

  // Check if path is a NFS URI
  static bool IsNFSURI(const std::string& path);

  // Recursively create a directory (for internal use, need to be called with a
  // lock in place)
  IOStatus CreateDirRecursive(const std::string& nfs_path);

  // Add an additional path prefix to be mapped to NFS
  void AddPathPrefix(const std::string& prefix);
};

// Factory function
// nfs_url: NFS server address, such as "nfs://192.168.1.100/shared/rocksdb"
// local_prefix: Local path prefix, the path used by RocksDB maps to NFS if it
// starts with this
Status NewNFSFileSystem(const std::string& nfs_url,
                        const std::string& local_prefix,
                        std::shared_ptr<FileSystem>* result);
}  // namespace ROCKSDB_NAMESPACE
