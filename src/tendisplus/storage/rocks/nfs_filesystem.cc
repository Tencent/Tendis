// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include "tendisplus/storage/rocks/nfs_filesystem.h"

#include <fcntl.h>     // for O_RDONLY, O_WRONLY, O_CREAT, O_TRUNC
#include <sys/stat.h>  // for S_ISDIR
#include <unistd.h>    // for getcwd

#include <climits>  // for PATH_MAX
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "logging/env_logger.h"  // for EnvLogger

namespace {
// Convert relative path to absolute path
std::string ToAbsolutePath(const std::string& path) {
  if (path.empty()) {
    return path;
  }
  // Already an absolute path
  if (path[0] == '/') {
    return path;
  }
  // Relative path, convert to absolute path
  char cwd[PATH_MAX];
  if (getcwd(cwd, sizeof(cwd)) != nullptr) {
    std::string abs_path = std::string(cwd) + "/" + path;
    // Normalize path: handle ./ and ../
    // Simple handling: remove ./
    size_t pos;
    while ((pos = abs_path.find("/./")) != std::string::npos) {
      abs_path.erase(pos, 2);
    }
    // Handle leading ./
    if (abs_path.find("./") == 0) {
      abs_path = abs_path.substr(2);
    }
    return abs_path;
  }
  return path;  // Failed to get cwd, return original path
}
}  // anonymous namespace

namespace ROCKSDB_NAMESPACE {

// NFS sequential reading of files
class NFSSequentialFile : public FSSequentialFile {
 public:
  NFSSequentialFile(struct nfs_context* ctx,
                    struct nfsfh* fh,
                    const std::string& fname,
                    std::mutex& mutex)
    : nfs_ctx_(ctx), nfs_fh_(fh), filename_(fname), nfs_mutex_(mutex) {}

  ~NFSSequentialFile() override {
    if (nfs_fh_) {
      std::lock_guard<std::mutex> lock(nfs_mutex_);
      nfs_close(nfs_ctx_, nfs_fh_);
    }
  }

  IOStatus Read(size_t n,
                const IOOptions& options,
                Slice* result,
                char* scratch,
                IODebugContext* dbg) override {
    std::lock_guard<std::mutex> lock(nfs_mutex_);
    int bytes_read = nfs_read(nfs_ctx_, nfs_fh_, n, scratch);
    if (bytes_read < 0) {
      return IOStatus::IOError("NFS read failed: " + filename_ + ", error: " +
                               std::string(nfs_get_error(nfs_ctx_)));
    }
    *result = Slice(scratch, bytes_read);
    return IOStatus::OK();
  }

  IOStatus Skip(uint64_t n) override {
    std::lock_guard<std::mutex> lock(nfs_mutex_);
    if (nfs_lseek(nfs_ctx_, nfs_fh_, n, SEEK_CUR, nullptr) < 0) {
      return IOStatus::IOError("NFS lseek failed: " + filename_);
    }
    return IOStatus::OK();
  }

 private:
  struct nfs_context* nfs_ctx_;
  struct nfsfh* nfs_fh_;
  std::string filename_;
  std::mutex& nfs_mutex_;
};

// NFS random reading of files
class NFSRandomAccessFile : public FSRandomAccessFile {
 public:
  NFSRandomAccessFile(struct nfs_context* ctx,
                      struct nfsfh* fh,
                      const std::string& fname,
                      std::mutex* mutex)
    : nfs_ctx_(ctx), nfs_fh_(fh), filename_(fname), nfs_mutex_(mutex) {}

  ~NFSRandomAccessFile() override {
    if (nfs_fh_) {
      std::lock_guard<std::mutex> lock(*nfs_mutex_);
      nfs_close(nfs_ctx_, nfs_fh_);
    }
  }

  IOStatus Read(uint64_t offset,
                size_t n,
                const IOOptions& options,
                Slice* result,
                char* scratch,
                IODebugContext* dbg) const override {
    std::lock_guard<std::mutex> lock(*nfs_mutex_);
    int bytes_read = nfs_pread(nfs_ctx_, nfs_fh_, offset, n, scratch);
    if (bytes_read < 0) {
      return IOStatus::IOError("NFS pread failed: " + filename_ + ", error: " +
                               std::string(nfs_get_error(nfs_ctx_)));
    }
    *result = Slice(scratch, bytes_read);
    return IOStatus::OK();
  }

 private:
  struct nfs_context* nfs_ctx_;
  struct nfsfh* nfs_fh_;
  std::string filename_;
  std::mutex* nfs_mutex_;
};

// NFS writable files
class NFSWritableFile : public FSWritableFile {
 public:
  NFSWritableFile(struct nfs_context* ctx,
                  struct nfsfh* fh,
                  const std::string& fname,
                  std::mutex& mutex)
    : nfs_ctx_(ctx),
      nfs_fh_(fh),
      filename_(fname),
      nfs_mutex_(mutex),
      filesize_(0) {}

  ~NFSWritableFile() override {
    if (nfs_fh_) {
      std::lock_guard<std::mutex> lock(nfs_mutex_);
      nfs_close(nfs_ctx_, nfs_fh_);
    }
  }

  IOStatus Append(const Slice& data,
                  const IOOptions& options,
                  IODebugContext* dbg) override {
    std::lock_guard<std::mutex> lock(nfs_mutex_);
    const char* src = data.data();
    size_t left = data.size();

    while (left > 0) {
      int written = nfs_write(nfs_ctx_, nfs_fh_, left, const_cast<char*>(src));
      if (written < 0) {
        return IOStatus::IOError(
          "NFS write failed: " + filename_ +
          ", error: " + std::string(nfs_get_error(nfs_ctx_)));
      }
      left -= written;
      src += written;
    }
    filesize_ += data.size();
    return IOStatus::OK();
  }

  IOStatus Append(const Slice& data,
                  const IOOptions& options,
                  const DataVerificationInfo& verification_info,
                  IODebugContext* dbg) override {
    return Append(data, options, dbg);
  }

  IOStatus PositionedAppend(const Slice& data,
                            uint64_t offset,
                            const IOOptions& options,
                            IODebugContext* dbg) override {
    std::lock_guard<std::mutex> lock(nfs_mutex_);
    const char* src = data.data();
    size_t left = data.size();
    uint64_t pos = offset;

    while (left > 0) {
      int written =
        nfs_pwrite(nfs_ctx_, nfs_fh_, pos, left, const_cast<char*>(src));
      if (written < 0) {
        return IOStatus::IOError(
          "NFS pwrite failed: " + filename_ +
          ", error: " + std::string(nfs_get_error(nfs_ctx_)));
      }
      left -= written;
      src += written;
      pos += written;
    }
    if (offset + data.size() > filesize_) {
      filesize_ = offset + data.size();
    }
    return IOStatus::OK();
  }

  IOStatus PositionedAppend(const Slice& data,
                            uint64_t offset,
                            const IOOptions& options,
                            const DataVerificationInfo& verification_info,
                            IODebugContext* dbg) override {
    return PositionedAppend(data, offset, options, dbg);
  }

  IOStatus Truncate(uint64_t size,
                    const IOOptions& options,
                    IODebugContext* dbg) override {
    std::lock_guard<std::mutex> lock(nfs_mutex_);
    if (nfs_ftruncate(nfs_ctx_, nfs_fh_, size) != 0) {
      return IOStatus::IOError(
        "NFS ftruncate failed: " + filename_ +
        ", error: " + std::string(nfs_get_error(nfs_ctx_)));
    }
    filesize_ = size;
    return IOStatus::OK();
  }

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override {
    if (nfs_fh_) {
      std::lock_guard<std::mutex> lock(nfs_mutex_);
      if (nfs_close(nfs_ctx_, nfs_fh_) != 0) {
        return IOStatus::IOError("NFS close failed: " + filename_);
      }
      nfs_fh_ = nullptr;
    }
    return IOStatus::OK();
  }

  IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override {
    // There is no explicit flush, and writes are synchronous
    return IOStatus::OK();
  }

  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override {
    std::lock_guard<std::mutex> lock(nfs_mutex_);
    if (nfs_fsync(nfs_ctx_, nfs_fh_) != 0) {
      return IOStatus::IOError("NFS fsync failed: " + filename_ + ", error: " +
                               std::string(nfs_get_error(nfs_ctx_)));
    }
    return IOStatus::OK();
  }

  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
    return Sync(options, dbg);
  }

  uint64_t GetFileSize(const IOOptions& options, IODebugContext* dbg) override {
    return filesize_;
  }

  bool IsSyncThreadSafe() const override {
    return true;
  }

 private:
  struct nfs_context* nfs_ctx_;
  struct nfsfh* nfs_fh_;
  std::string filename_;
  std::mutex& nfs_mutex_;
  uint64_t filesize_;
};

// NFS directory (for operations like fsync)
class NFSDirectory : public FSDirectory {
 public:
  NFSDirectory(struct nfs_context* ctx,
               const std::string& dirname,
               std::mutex& mutex)
    : nfs_ctx_(ctx), dirname_(dirname), nfs_mutex_(mutex) {}

  ~NFSDirectory() override {}

  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
    // fsync for NFS directories is usually no-op because NFS is synchronous
    // But we can try the fsync directory (if libnfs supports it)
    // For now, simply return to OK
    return IOStatus::OK();
  }

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override {
    return IOStatus::OK();
  }

 private:
  struct nfs_context* nfs_ctx_;
  std::string dirname_;
  std::mutex& nfs_mutex_;
};

// NFS file lock
class NFSFileLock : public FileLock {
 public:
  NFSFileLock(struct nfs_context* ctx,
              struct nfsfh* fh,
              const std::string& fname,
              std::mutex& mutex)
    : nfs_ctx_(ctx), nfs_fh_(fh), filename_(fname), nfs_mutex_(mutex) {}

  ~NFSFileLock() override {
    // Automatically release the lock when destructuring
    if (nfs_fh_) {
      std::lock_guard<std::mutex> lock(nfs_mutex_);
      nfs_close(nfs_ctx_, nfs_fh_);
    }
  }

  struct nfsfh* GetFileHandle() const {
    return nfs_fh_;
  }
  const std::string& GetFilename() const {
    return filename_;
  }
  void ClearFileHandle() {
    nfs_fh_ = nullptr;
  }

 private:
  struct nfs_context* nfs_ctx_;
  struct nfsfh* nfs_fh_;
  std::string filename_;
  std::mutex& nfs_mutex_;
};


NFSFileSystem::NFSFileSystem(const std::string& nfs_url,
                             const std::string& local_prefix,
                             const std::shared_ptr<FileSystem>& base)
  : FileSystemWrapper(base),
    nfs_url_(nfs_url),
    local_prefix_(ToAbsolutePath(local_prefix)),  // Convert to absolute path
    nfs_ctx_(nullptr) {
  // Ensure that the local_prefix does not end in '/' for easy subsequent
  // processing
  if (!local_prefix_.empty() && local_prefix_.back() == '/') {
    local_prefix_.pop_back();
  }
  InitNFSContext();
  std::cout << "[NFSFileSystem] Initialized with:"
            << "\n  NFS URL: " << nfs_url_
            << "\n  Local Prefix (original): " << local_prefix
            << "\n  Local Prefix (absolute): " << local_prefix_ << std::endl;
}

void NFSFileSystem::RegisterPathPrefix(const std::string& prefix) {
  AddPathPrefix(prefix);
}

void NFSFileSystem::AddPathPrefix(const std::string& prefix) {
  if (prefix.empty()) {
    return;
  }
  std::string abs_prefix = ToAbsolutePath(prefix);
  // Normalize: remove trailing slash
  while (!abs_prefix.empty() && abs_prefix.back() == '/') {
    abs_prefix.pop_back();
  }
  if (!abs_prefix.empty()) {
    // Check if already exists
    for (const auto& existing : additional_prefixes_) {
      if (existing == abs_prefix) {
        return;  // Already registered
      }
    }
    additional_prefixes_.push_back(abs_prefix);
    std::cout << "[NFSFileSystem] Registered additional path prefix: " << prefix
              << " (absolute: " << abs_prefix << ")" << std::endl;
  }
}

NFSFileSystem::~NFSFileSystem() {
  if (nfs_ctx_) {
    std::cerr << "Destroying NFSFileSystem(" << nfs_url_ << ")" << std::endl;
    nfs_destroy_context(nfs_ctx_);
  }
}

std::string NFSFileSystem::GetId() const {
  if (nfs_url_.empty()) {
    return kProto;
  } else if (nfs_url_.find(kProto) == 0) {
    return nfs_url_;
  } else {
    std::string id = kProto;
    return id.append("localhost").append(nfs_url_);
  }
}

Status NFSFileSystem::ValidateOptions(
  const DBOptions& db_opts, const ColumnFamilyOptions& cf_opts) const {
  if (nfs_ctx_ != nullptr) {
    return FileSystemWrapper::ValidateOptions(db_opts, cf_opts);
  } else {
    return Status::InvalidArgument("Failed to connect to NFS ", nfs_url_);
  }
}

void NFSFileSystem::InitNFSContext() {
  nfs_ctx_ = nfs_init_context();
  if (!nfs_ctx_) {
    throw std::runtime_error("Failed to init NFS context");
  }

  struct nfs_url* url = nfs_parse_url_dir(nfs_ctx_, nfs_url_.c_str());
  if (!url) {
    nfs_destroy_context(nfs_ctx_);
    nfs_ctx_ = nullptr;
    throw std::runtime_error("Invalid NFS URL: " + nfs_url_);
  }

  if (nfs_mount(nfs_ctx_, url->server, url->path) != 0) {
    std::string error_msg =
      "Failed to mount NFS: " + std::string(nfs_get_error(nfs_ctx_));
    nfs_destroy_url(url);
    nfs_destroy_context(nfs_ctx_);
    nfs_ctx_ = nullptr;
    throw std::runtime_error(error_msg);
  }

  nfs_destroy_url(url);
}

bool NFSFileSystem::IsDataFile(const std::string& path) const {
  // Check if the file is a data file (.sst, .blob, .ldb)
  size_t len = path.length();
  if (len > 4 && path.substr(len - 4) == ".sst")
    return true;
  if (len > 5 && path.substr(len - 5) == ".blob")
    return true;
  if (len > 4 && path.substr(len - 4) == ".ldb")
    return true;
  return false;
}

bool NFSFileSystem::IsNFSPath(const std::string& path) const {
  // Support three modes:
  // 1. URI mode: path is "nfs://server/path/to/file" - always use NFS
  // 2. Pure URI mode: local_prefix_ is empty but nfs_url_ is set - ALL paths go
  // to NFS
  // 3. Auto-convert mode: local path matches local_prefix_ -> automatically
  // convert to URI internally
  // 4. Legacy path matching mode: check if path matches local_prefix_ or
  // additional_prefixes_

  bool is_nfs = false;
  std::string reason;

  // First check if it's a NFS URI
  if (IsNFSURI(path)) {
    is_nfs = true;
    reason = "NFS URI format";
    std::cout << "[NFSFileSystem] IsNFSPath: " << path << " -> NFS (URI format)"
              << std::endl;
    return is_nfs;
  }

  // Pure URI mode: If local_prefix_ is empty but nfs_url_ is set, ALL paths go
  // to NFS This is the recommended mode for remote deployment where we don't
  // need path matching
  if (local_prefix_.empty() && !nfs_url_.empty()) {
    is_nfs = true;
    reason = "pure URI mode (all paths go to NFS)";
    std::cout << "[NFSFileSystem] IsNFSPath: " << path
              << " -> NFS (pure URI mode, nfs_url=" << nfs_url_ << ")"
              << std::endl;
    return is_nfs;
  }

  // Auto-convert mode: Try to convert local path to URI
  // If conversion succeeds, treat it as NFS path (will use URI internally)
  std::string uri = ConvertLocalPathToURI(path);
  if (!uri.empty()) {
    is_nfs = true;
    reason = "auto-converted to URI: " + uri;
    std::cout << "[NFSFileSystem] IsNFSPath: " << path
              << " -> NFS (auto-converted to URI: " << uri << ")" << std::endl;
    return is_nfs;
  }

  // Legacy path matching mode (fallback)
  // ALL files go to NFS to ensure consistency between local Tendis and remote
  // workers. Both local Tendis and remote compaction workers access the same
  // files via NFS. This avoids any inconsistency issues between local SSD and
  // shared storage.
  //
  // The path is considered NFS if:
  // 1. Path matches local_prefix_ (the DB directory)
  // 2. Path matches any additional_prefixes_ (wal_dir, db_log_dir, etc.)
  // 3. Path starts with /nfs/

  // First convert input path to absolute path for comparison
  std::string abs_path = path;
  if (!path.empty() && path[0] != '/') {
    // Relative path - assume it's within the DB directory, so it goes to NFS
    char cwd[PATH_MAX];
    if (getcwd(cwd, sizeof(cwd)) != nullptr) {
      abs_path = std::string(cwd) + "/" + path;
      // Normalize path: remove ./
      size_t pos;
      while ((pos = abs_path.find("/./")) != std::string::npos) {
        abs_path.erase(pos, 2);
      }
    }
  }

  // Check if path matches local_prefix_ (ALL files under local_prefix_ go to
  // NFS)
  if (!local_prefix_.empty() && abs_path.find(local_prefix_) == 0) {
    is_nfs = true;
    reason = "matches local_prefix (all files go to NFS)";
  } else {
    // Check additional prefixes (wal_dir, db_log_dir, etc.)
    for (const auto& prefix : additional_prefixes_) {
      if (!prefix.empty()) {
        std::string abs_prefix = ToAbsolutePath(prefix);
        // Normalize prefix (remove trailing slash for comparison)
        while (!abs_prefix.empty() && abs_prefix.back() == '/') {
          abs_prefix.pop_back();
        }
        if (!abs_prefix.empty() && abs_path.find(abs_prefix) == 0) {
          is_nfs = true;
          reason = "matches additional_prefix: " + prefix;
          break;
        }
      }
    }

    if (!is_nfs) {
      if (path.find("/nfs/") == 0) {
        is_nfs = true;
        reason = "starts with /nfs/";
      } else {
        reason = "no match (path='" + abs_path + "', local_prefix='" +
          local_prefix_ + "')";
      }
    }
  }

  std::cout << "[NFSFileSystem] IsNFSPath: " << path << " -> "
            << (is_nfs ? "NFS" : "LOCAL") << " (" << reason << ")" << std::endl;

  return is_nfs;
}

// Convert local path to URI (if it matches local_prefix_)
std::string NFSFileSystem::ConvertLocalPathToURI(
  const std::string& path) const {
  // If path is already a URI, return as-is
  if (IsNFSURI(path)) {
    return path;
  }

  // Convert to absolute path
  std::string abs_path = path;
  if (!path.empty() && path[0] != '/') {
    char cwd[PATH_MAX];
    if (getcwd(cwd, sizeof(cwd)) != nullptr) {
      abs_path = std::string(cwd) + "/" + path;
      size_t pos;
      while ((pos = abs_path.find("/./")) != std::string::npos) {
        abs_path.erase(pos, 2);
      }
    }
  }

  // Check if path matches local_prefix_ or additional_prefixes_
  std::string matched_prefix;
  std::string relative_path;

  if (!local_prefix_.empty() && abs_path.find(local_prefix_) == 0) {
    matched_prefix = local_prefix_;
    relative_path = abs_path.substr(local_prefix_.length());
    while (!relative_path.empty() && relative_path[0] == '/') {
      relative_path = relative_path.substr(1);
    }
  } else {
    // Try additional prefixes
    for (const auto& prefix : additional_prefixes_) {
      if (!prefix.empty()) {
        std::string abs_prefix = ToAbsolutePath(prefix);
        while (!abs_prefix.empty() && abs_prefix.back() == '/') {
          abs_prefix.pop_back();
        }
        if (!abs_prefix.empty() && abs_path.find(abs_prefix) == 0) {
          matched_prefix = abs_prefix;
          relative_path = abs_path.substr(abs_prefix.length());
          while (!relative_path.empty() && relative_path[0] == '/') {
            relative_path = relative_path.substr(1);
          }
          break;
        }
      }
    }
  }

  // If matched, build URI
  if (!matched_prefix.empty()) {
    // Extract base path from nfs_url_: nfs://server/path -> /path
    size_t proto_end = nfs_url_.find("://");
    if (proto_end != std::string::npos) {
      size_t path_start = nfs_url_.find('/', proto_end + 3);
      if (path_start != std::string::npos) {
        std::string server_part = nfs_url_.substr(0, path_start);
        std::string base_path = nfs_url_.substr(path_start);
        if (base_path.back() != '/') {
          base_path += "/";
        }
        std::string uri = server_part + base_path + relative_path;
        std::cout << "[NFSFileSystem] ConvertLocalPathToURI: " << path << " -> "
                  << uri << " (matched prefix: " << matched_prefix << ")"
                  << std::endl;
        return uri;
      }
    }
  }

  // No match, return empty (use path matching mode)
  return "";
}

std::string NFSFileSystem::ConvertToNFSPath(const std::string& path) const {
  // Support both URI mode and path matching mode (legacy)
  // URI mode: "nfs://server/path/to/file" -> "/path/to/file"
  // Pure URI mode: local_prefix_ is empty -> use path directly (relative to NFS
  // mount point) Path matching mode: "/mnt/nfs_rocksdb/db/0/xxx.sst" ->
  // "0/xxx.sst" Auto-convert mode: Convert matching local paths to URI
  // internally

  // Check if it's a NFS URI
  if (IsNFSURI(path)) {
    std::string nfs_path = ExtractNFSPathFromURI(path);
    std::cout << "[NFSFileSystem] ConvertToNFSPath (URI): " << path << " -> "
              << nfs_path << std::endl;
    return nfs_path;
  }

  // Pure URI mode: If local_prefix_ is empty, use path directly (relative to
  // NFS mount point) In this mode, we assume the path structure matches between
  // local and NFS Example: path = "./home/db/0/xxx.sst" -> "home/db/0/xxx.sst"
  // (relative to mount point)
  if (local_prefix_.empty() && !nfs_url_.empty()) {
    std::string nfs_path = path;
    // Remove leading "./" if present
    if (nfs_path.length() >= 2 && nfs_path.substr(0, 2) == "./") {
      nfs_path = nfs_path.substr(2);
    }
    // Remove leading "/" if present (we want relative path)
    while (!nfs_path.empty() && nfs_path[0] == '/') {
      nfs_path = nfs_path.substr(1);
    }
    std::cout << "[NFSFileSystem] ConvertToNFSPath (pure URI mode): " << path
              << " -> " << (nfs_path.empty() ? "." : nfs_path) << std::endl;
    return nfs_path.empty() ? "." : nfs_path;
  }

  // Try to convert local path to URI first (if it matches)
  std::string uri = ConvertLocalPathToURI(path);
  if (!uri.empty()) {
    // Successfully converted to URI, extract NFS path from URI
    std::string nfs_path = ExtractNFSPathFromURI(uri);
    std::cout << "[NFSFileSystem] ConvertToNFSPath (auto-converted to URI): "
              << path << " -> URI: " << uri << " -> NFS path: " << nfs_path
              << std::endl;
    return nfs_path;
  }

  // Legacy path matching mode (fallback)
  // Convert local path to NFS relative path
  //
  // Mapping: local_prefix or additional_prefixes -> nfs_url (already mounted in
  // InitNFSContext) So we just need to extract the relative path part
  //
  // Example: local_prefix_ = "/mnt/nfs_rocksdb/db"
  //          nfs_url_ = "nfs://192.168.1.100/shared/rocksdb"
  //          path = "/mnt/nfs_rocksdb/db/0/xxx.sst"
  //
  //          Extract relative path: "0/xxx.sst" (without leading slash)
  //          libnfs will access: nfs://192.168.1.100/shared/rocksdb/0/xxx.sst

  // First convert input path to absolute path
  std::string abs_path = path;
  if (!path.empty() && path[0] != '/') {
    char cwd[PATH_MAX];
    if (getcwd(cwd, sizeof(cwd)) != nullptr) {
      abs_path = std::string(cwd) + "/" + path;
      size_t pos;
      while ((pos = abs_path.find("/./")) != std::string::npos) {
        abs_path.erase(pos, 2);
      }
    }
  }

  // Try local_prefix first
  if (!local_prefix_.empty() && abs_path.find(local_prefix_) == 0) {
    std::string relative_path = abs_path.substr(local_prefix_.length());
    // Remove the leading slash and return to the relative path
    while (!relative_path.empty() && relative_path[0] == '/') {
      relative_path = relative_path.substr(1);
    }
    std::cout << "[NFSFileSystem] ConvertToNFSPath (legacy): " << path
              << " (abs: " << abs_path << ")"
              << " -> " << (relative_path.empty() ? "." : relative_path)
              << " (matched prefix: " << local_prefix_ << ")" << std::endl;
    return relative_path.empty() ? "." : relative_path;
  } else if (!local_prefix_.empty()) {
    // Log when path doesn't match local_prefix for debugging
    std::cout << "[NFSFileSystem] ConvertToNFSPath: path " << path
              << " (abs: " << abs_path
              << ") does not match local_prefix: " << local_prefix_
              << std::endl;
  }

  // Try additional prefixes
  for (const auto& prefix : additional_prefixes_) {
    if (!prefix.empty()) {
      std::string abs_prefix = ToAbsolutePath(prefix);
      // Normalize prefix (remove trailing slash for comparison)
      while (!abs_prefix.empty() && abs_prefix.back() == '/') {
        abs_prefix.pop_back();
      }
      if (!abs_prefix.empty() && abs_path.find(abs_prefix) == 0) {
        std::string relative_path = abs_path.substr(abs_prefix.length());
        // Remove the leading slash
        while (!relative_path.empty() && relative_path[0] == '/') {
          relative_path = relative_path.substr(1);
        }
        std::cout << "[NFSFileSystem] ConvertToNFSPath (legacy): " << path
                  << " -> " << (relative_path.empty() ? "." : relative_path)
                  << " (via prefix: " << prefix << ")" << std::endl;
        return relative_path.empty() ? "." : relative_path;
      }
    }
  }

  if (path.find("/nfs/") == 0) {
    std::string rel = path.substr(5);  // Remove the "/nfs/" prefix
    return rel.empty() ? "." : rel;
  }

  std::cout << "[NFSFileSystem] ConvertToNFSPath: " << path << " -> " << path
            << " (unchanged, not NFS path)" << std::endl;
  return path;
}

// Recursively create a catalog
IOStatus NFSFileSystem::CreateDirRecursive(const std::string& nfs_path) {
  // Create step by step starting from the root
  std::string current_path;
  size_t pos = 0;

  while (pos < nfs_path.length()) {
    size_t next_slash = nfs_path.find('/', pos + 1);
    if (next_slash == std::string::npos) {
      next_slash = nfs_path.length();
    }

    current_path = nfs_path.substr(0, next_slash);
    pos = next_slash;

    if (current_path.empty() || current_path == "/") {
      continue;
    }

    // Check if the catalog exists
    struct nfs_stat_64 st;
    if (nfs_stat64(nfs_ctx_, current_path.c_str(), &st) != 0) {
      // The directory does not exist, create it
      if (nfs_mkdir(nfs_ctx_, current_path.c_str()) != 0) {
        std::string err = nfs_get_error(nfs_ctx_);
        // Ignore "already exists" error (possibly concurrent creation)
        if (err.find("exist") == std::string::npos &&
            err.find("EXIST") == std::string::npos) {
          return IOStatus::IOError("Failed to create NFS directory: " +
                                   current_path + ", error: " + err);
        }
      }
    }
  }

  return IOStatus::OK();
}


IOStatus NFSFileSystem::NewSequentialFile(
  const std::string& fname,
  const FileOptions& options,
  std::unique_ptr<FSSequentialFile>* result,
  IODebugContext* dbg) {
  if (!IsNFSPath(fname)) {
    return FileSystemWrapper::NewSequentialFile(fname, options, result, dbg);
  }

  // Check if NFS context is available
  if (!nfs_ctx_) {
    std::cerr << "[NFSFileSystem] NFS context not available, falling back to "
                 "local filesystem for: "
              << fname << std::endl;
    return FileSystemWrapper::NewSequentialFile(fname, options, result, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);

  std::string nfs_path = ConvertToNFSPath(fname);
  std::cerr << "[NFSFileSystem] NewSequentialFile: " << fname
            << " -> NFS path: " << nfs_path << std::endl;
  std::cerr << "[NFSFileSystem] NFS URL (mount point): " << nfs_url_
            << std::endl;
  std::cerr << "[NFSFileSystem] Full NFS path (relative to mount): " << nfs_path
            << std::endl;
  std::cerr.flush();

  struct nfsfh* fh = nullptr;
  if (nfs_open(nfs_ctx_, nfs_path.c_str(), O_RDONLY, &fh) != 0) {
    std::string error = nfs_get_error(nfs_ctx_);
    std::cerr << "[NFSFileSystem] ERROR: Failed to open NFS file: " << fname
              << std::endl;
    std::cerr << "[NFSFileSystem] ERROR: NFS path used: " << nfs_path
              << std::endl;
    std::cerr << "[NFSFileSystem] ERROR: NFS error: " << error << std::endl;
    std::cerr.flush();
    return IOStatus::IOError("Failed to open NFS file for reading: " + fname +
                             ", error: " + error);
  }

  result->reset(new NFSSequentialFile(nfs_ctx_, fh, fname, nfs_mutex_));
  return IOStatus::OK();
}

IOStatus NFSFileSystem::NewRandomAccessFile(
  const std::string& fname,
  const FileOptions& options,
  std::unique_ptr<FSRandomAccessFile>* result,
  IODebugContext* dbg) {
  if (!IsNFSPath(fname)) {
    return FileSystemWrapper::NewRandomAccessFile(fname, options, result, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);

  std::string nfs_path = ConvertToNFSPath(fname);
  std::cout << "[NFSFileSystem] NewRandomAccessFile: " << fname
            << " -> NFS path: " << nfs_path << std::endl;

  struct nfsfh* fh = nullptr;
  if (nfs_open(nfs_ctx_, nfs_path.c_str(), O_RDONLY, &fh) != 0) {
    return IOStatus::IOError(
      "Failed to open NFS file for random access: " + fname +
      ", error: " + std::string(nfs_get_error(nfs_ctx_)));
  }

  result->reset(new NFSRandomAccessFile(nfs_ctx_, fh, fname, &nfs_mutex_));
  return IOStatus::OK();
}

IOStatus NFSFileSystem::NewWritableFile(const std::string& fname,
                                        const FileOptions& options,
                                        std::unique_ptr<FSWritableFile>* result,
                                        IODebugContext* dbg) {
  if (!IsNFSPath(fname)) {
    return FileSystemWrapper::NewWritableFile(fname, options, result, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);

  std::string nfs_path = ConvertToNFSPath(fname);
  std::cout << "[NFSFileSystem] NewWritableFile: " << fname
            << " -> NFS path: " << nfs_path << std::endl;

  // Ensure that the parent directory exists
  size_t last_slash = nfs_path.rfind('/');
  if (last_slash != std::string::npos && last_slash > 0) {
    std::string parent_dir = nfs_path.substr(0, last_slash);
    IOStatus dir_status = CreateDirRecursive(parent_dir);
    if (!dir_status.ok()) {
      return dir_status;
    }
  }

  struct nfsfh* fh = nullptr;
  // nfs_creat Create a file using the mode parameter
  int mode = 0644;  // rw-r--r--

  if (nfs_creat(nfs_ctx_, nfs_path.c_str(), mode, &fh) != 0) {
    return IOStatus::IOError(
      "Failed to create NFS file: " + fname +
      ", error: " + std::string(nfs_get_error(nfs_ctx_)));
  }

  result->reset(new NFSWritableFile(nfs_ctx_, fh, fname, nfs_mutex_));
  return IOStatus::OK();
}

// Reopen the file in append mode (for LOG files, etc.)
IOStatus NFSFileSystem::ReopenWritableFile(
  const std::string& fname,
  const FileOptions& options,
  std::unique_ptr<FSWritableFile>* result,
  IODebugContext* dbg) {
  if (!IsNFSPath(fname)) {
    return FileSystemWrapper::ReopenWritableFile(fname, options, result, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);

  std::string nfs_path = ConvertToNFSPath(fname);
  std::cout << "[NFSFileSystem] ReopenWritableFile (append): " << fname
            << " -> NFS path: " << nfs_path << std::endl;

  // Ensure that the parent directory exists
  size_t last_slash = nfs_path.rfind('/');
  if (last_slash != std::string::npos && last_slash > 0) {
    std::string parent_dir = nfs_path.substr(0, last_slash);
    IOStatus dir_status = CreateDirRecursive(parent_dir);
    if (!dir_status.ok()) {
      return dir_status;
    }
  }

  struct nfsfh* fh = nullptr;

  // Check if the file exists
  struct nfs_stat_64 st;
  uint64_t initial_size = 0;
  bool file_exists = (nfs_stat64(nfs_ctx_, nfs_path.c_str(), &st) == 0);

  if (file_exists) {
    // The file exists, opening in append mode
    if (nfs_open(nfs_ctx_, nfs_path.c_str(), O_WRONLY | O_APPEND, &fh) != 0) {
      return IOStatus::IOError(
        "Failed to open NFS file for appending: " + fname +
        ", error: " + std::string(nfs_get_error(nfs_ctx_)));
    }
    initial_size = st.nfs_size;
  } else {
    // The file does not exist, create a new file
    int mode = 0644;  // rw-r--r--
    if (nfs_creat(nfs_ctx_, nfs_path.c_str(), mode, &fh) != 0) {
      return IOStatus::IOError(
        "Failed to create NFS file: " + fname +
        ", error: " + std::string(nfs_get_error(nfs_ctx_)));
    }
  }

  result->reset(new NFSWritableFile(nfs_ctx_, fh, fname, nfs_mutex_));
  return IOStatus::OK();
}

IOStatus NFSFileSystem::DeleteFile(const std::string& fname,
                                   const IOOptions& options,
                                   IODebugContext* dbg) {
  if (!IsNFSPath(fname)) {
    return FileSystemWrapper::DeleteFile(fname, options, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);

  std::string nfs_path = ConvertToNFSPath(fname);
  std::cout << "[NFSFileSystem] DeleteFile: " << fname
            << " -> NFS path: " << nfs_path << std::endl;

  if (nfs_unlink(nfs_ctx_, nfs_path.c_str()) != 0) {
    return IOStatus::IOError(
      "Failed to delete NFS file: " + fname +
      ", error: " + std::string(nfs_get_error(nfs_ctx_)));
  }

  return IOStatus::OK();
}

IOStatus NFSFileSystem::RenameFile(const std::string& src,
                                   const std::string& target,
                                   const IOOptions& options,
                                   IODebugContext* dbg) {
  bool src_is_nfs = IsNFSPath(src);
  bool target_is_nfs = IsNFSPath(target);

  std::cout << "[NFSFileSystem] RenameFile check: src=" << src
            << " (is_nfs=" << src_is_nfs << "), target=" << target
            << " (is_nfs=" << target_is_nfs << ")" << std::endl;

  if (!src_is_nfs && !target_is_nfs) {
    std::cout << "[NFSFileSystem] RenameFile: Both local, delegating to base FS"
              << std::endl;
    return FileSystemWrapper::RenameFile(src, target, options, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);

  std::string nfs_src = ConvertToNFSPath(src);
  std::string nfs_target = ConvertToNFSPath(target);

  std::cout << "[NFSFileSystem] RenameFile NFS: " << nfs_src << " -> "
            << nfs_target << std::endl;

  if (nfs_rename(nfs_ctx_, nfs_src.c_str(), nfs_target.c_str()) != 0) {
    std::string err = nfs_get_error(nfs_ctx_);
    std::cerr << "[NFSFileSystem] RenameFile FAILED: " << err << std::endl;
    return IOStatus::IOError("Failed to rename NFS file: " + src + " to " +
                             target + ", error: " + err);
  }

  std::cout << "[NFSFileSystem] RenameFile SUCCESS" << std::endl;
  return IOStatus::OK();
}

IOStatus NFSFileSystem::GetFileSize(const std::string& fname,
                                    const IOOptions& options,
                                    uint64_t* file_size,
                                    IODebugContext* dbg) {
  if (!IsNFSPath(fname)) {
    return FileSystemWrapper::GetFileSize(fname, options, file_size, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);

  std::string nfs_path = ConvertToNFSPath(fname);
  struct nfs_stat_64 st;

  if (nfs_stat64(nfs_ctx_, nfs_path.c_str(), &st) != 0) {
    return IOStatus::IOError("Failed to stat NFS file: " + fname + ", error: " +
                             std::string(nfs_get_error(nfs_ctx_)));
  }

  *file_size = st.nfs_size;
  return IOStatus::OK();
}

IOStatus NFSFileSystem::FileExists(const std::string& fname,
                                   const IOOptions& options,
                                   IODebugContext* dbg) {
  if (!IsNFSPath(fname)) {
    return FileSystemWrapper::FileExists(fname, options, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);

  std::string nfs_path = ConvertToNFSPath(fname);
  struct nfs_stat_64 st;

  if (nfs_stat64(nfs_ctx_, nfs_path.c_str(), &st) != 0) {
    return IOStatus::NotFound(fname);
  }

  return IOStatus::OK();
}

IOStatus NFSFileSystem::CreateDir(const std::string& dirname,
                                  const IOOptions& options,
                                  IODebugContext* dbg) {
  if (!IsNFSPath(dirname)) {
    return FileSystemWrapper::CreateDir(dirname, options, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);

  std::string nfs_path = ConvertToNFSPath(dirname);
  std::cout << "[NFSFileSystem] CreateDir: " << dirname
            << " -> NFS path: " << nfs_path << std::endl;

  if (nfs_mkdir(nfs_ctx_, nfs_path.c_str()) != 0) {
    std::string err = nfs_get_error(nfs_ctx_);
    // If the directory already exists, it is not an error
    if (err.find("exist") != std::string::npos ||
        err.find("EXIST") != std::string::npos) {
      return IOStatus::OK();
    }
    return IOStatus::IOError("Failed to create NFS directory: " + dirname +
                             ", error: " + err);
  }

  return IOStatus::OK();
}

IOStatus NFSFileSystem::CreateDirIfMissing(const std::string& dirname,
                                           const IOOptions& options,
                                           IODebugContext* dbg) {
  if (!IsNFSPath(dirname)) {
    return FileSystemWrapper::CreateDirIfMissing(dirname, options, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);

  std::string nfs_path = ConvertToNFSPath(dirname);
  std::cout << "[NFSFileSystem] CreateDirIfMissing: " << dirname
            << " -> NFS path: " << nfs_path << std::endl;

  // Check if the directory exists first
  struct nfs_stat_64 st;
  if (nfs_stat64(nfs_ctx_, nfs_path.c_str(), &st) == 0) {
    // The catalog already exists
    return IOStatus::OK();
  }

  // Recursively create a catalog
  return CreateDirRecursive(nfs_path);
}

IOStatus NFSFileSystem::GetChildren(const std::string& dir,
                                    const IOOptions& options,
                                    std::vector<std::string>* result,
                                    IODebugContext* dbg) {
  if (!IsNFSPath(dir)) {
    return FileSystemWrapper::GetChildren(dir, options, result, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);

  std::string nfs_path = ConvertToNFSPath(dir);

  struct nfsdir* nfsdir_handle;
  if (nfs_opendir(nfs_ctx_, nfs_path.c_str(), &nfsdir_handle) != 0) {
    return IOStatus::IOError(
      "Failed to open NFS directory: " + dir +
      ", error: " + std::string(nfs_get_error(nfs_ctx_)));
  }

  result->clear();
  struct nfsdirent* dirent;
  while ((dirent = nfs_readdir(nfs_ctx_, nfsdir_handle)) != nullptr) {
    std::string name = dirent->name;
    if (name != "." && name != "..") {
      result->push_back(name);
    }
  }

  nfs_closedir(nfs_ctx_, nfsdir_handle);
  return IOStatus::OK();
}

IOStatus NFSFileSystem::GetFileModificationTime(const std::string& fname,
                                                const IOOptions& options,
                                                uint64_t* file_mtime,
                                                IODebugContext* dbg) {
  if (!IsNFSPath(fname)) {
    return FileSystemWrapper::GetFileModificationTime(
      fname, options, file_mtime, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);

  std::string nfs_path = ConvertToNFSPath(fname);
  struct nfs_stat_64 st;

  if (nfs_stat64(nfs_ctx_, nfs_path.c_str(), &st) != 0) {
    return IOStatus::IOError("Failed to stat NFS file: " + fname + ", error: " +
                             std::string(nfs_get_error(nfs_ctx_)));
  }

  *file_mtime = st.nfs_mtime;
  return IOStatus::OK();
}

// Rewrite NewDirectory to open directory handles
IOStatus NFSFileSystem::NewDirectory(const std::string& name,
                                     const IOOptions& io_opts,
                                     std::unique_ptr<FSDirectory>* result,
                                     IODebugContext* dbg) {
  if (!IsNFSPath(name)) {
    return FileSystemWrapper::NewDirectory(name, io_opts, result, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);

  std::string nfs_path = ConvertToNFSPath(name);
  std::cout << "[NFSFileSystem] NewDirectory: " << name
            << " -> NFS path: " << nfs_path << std::endl;

  // Check if the catalog exists
  struct nfs_stat_64 st;
  if (nfs_stat64(nfs_ctx_, nfs_path.c_str(), &st) != 0) {
    return IOStatus::IOError(
      "Failed to open NFS directory: " + name +
      ", error: " + std::string(nfs_get_error(nfs_ctx_)));
  }

  // Check if it's a table of contents
  if (!S_ISDIR(st.nfs_mode)) {
    return IOStatus::IOError("Not a directory: " + name);
  }

  result->reset(new NFSDirectory(nfs_ctx_, nfs_path, nfs_mutex_));
  return IOStatus::OK();
}

// Delete directory
IOStatus NFSFileSystem::DeleteDir(const std::string& dirname,
                                  const IOOptions& options,
                                  IODebugContext* dbg) {
  if (!IsNFSPath(dirname)) {
    return FileSystemWrapper::DeleteDir(dirname, options, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);

  std::string nfs_path = ConvertToNFSPath(dirname);
  std::cout << "[NFSFileSystem] DeleteDir: " << dirname
            << " -> NFS path: " << nfs_path << std::endl;

  if (nfs_rmdir(nfs_ctx_, nfs_path.c_str()) != 0) {
    return IOStatus::IOError(
      "Failed to delete NFS directory: " + dirname +
      ", error: " + std::string(nfs_get_error(nfs_ctx_)));
  }

  return IOStatus::OK();
}

// Check if path is a directory
IOStatus NFSFileSystem::IsDirectory(const std::string& path,
                                    const IOOptions& options,
                                    bool* is_dir,
                                    IODebugContext* dbg) {
  if (!IsNFSPath(path)) {
    return FileSystemWrapper::IsDirectory(path, options, is_dir, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);

  std::string nfs_path = ConvertToNFSPath(path);
  struct nfs_stat_64 st;

  if (nfs_stat64(nfs_ctx_, nfs_path.c_str(), &st) != 0) {
    return IOStatus::IOError("Failed to stat NFS path: " + path + ", error: " +
                             std::string(nfs_get_error(nfs_ctx_)));
  }

  if (is_dir != nullptr) {
    *is_dir = S_ISDIR(st.nfs_mode);
  }
  return IOStatus::OK();
}

// Rewrite LockFile
IOStatus NFSFileSystem::LockFile(const std::string& fname,
                                 const IOOptions& options,
                                 FileLock** lock,
                                 IODebugContext* dbg) {
  if (!IsNFSPath(fname)) {
    return FileSystemWrapper::LockFile(fname, options, lock, dbg);
  }

  std::lock_guard<std::mutex> lk(nfs_mutex_);

  std::string nfs_path = ConvertToNFSPath(fname);
  std::cout << "[NFSFileSystem] LockFile: " << fname
            << " -> NFS path: " << nfs_path << std::endl;

  // Ensure that the parent directory exists
  size_t last_slash = nfs_path.rfind('/');
  if (last_slash != std::string::npos && last_slash > 0) {
    std::string parent_dir = nfs_path.substr(0, last_slash);
    IOStatus dir_status = CreateDirRecursive(parent_dir);
    if (!dir_status.ok()) {
      return dir_status;
    }
  }

  // Create or open a lock file
  struct nfsfh* fh = nullptr;
  int mode = 0644;

  // Try creating a file if it doesn't exist
  if (nfs_creat(nfs_ctx_, nfs_path.c_str(), mode, &fh) != 0) {
    // If the creation fails, try to open the existing file
    if (nfs_open(nfs_ctx_, nfs_path.c_str(), O_RDWR, &fh) != 0) {
      return IOStatus::IOError(
        "Failed to open lock file: " + fname +
        ", error: " + std::string(nfs_get_error(nfs_ctx_)));
    }
  }

  // Note: NFS has limited file lock support, here we simply keep the file open
  // True locking requires the use of the NLM (Network Lock Manager) protocol
  // For single-instance scenarios, keeping the file open is sufficient

  *lock = new NFSFileLock(nfs_ctx_, fh, fname, nfs_mutex_);
  return IOStatus::OK();
}

// Rewrite the UnlockFile
IOStatus NFSFileSystem::UnlockFile(FileLock* lock,
                                   const IOOptions& options,
                                   IODebugContext* dbg) {
  if (lock == nullptr) {
    return IOStatus::OK();
  }

  NFSFileLock* nfs_lock = dynamic_cast<NFSFileLock*>(lock);
  if (nfs_lock == nullptr) {
    // Not our lock, leave it to the parent to handle
    return FileSystemWrapper::UnlockFile(lock, options, dbg);
  }

  std::cout << "[NFSFileSystem] UnlockFile: " << nfs_lock->GetFilename()
            << std::endl;

  // NFSFileLock's destructor closes the file handle
  delete nfs_lock;
  return IOStatus::OK();
}

// Rewrite NewLogger, making sure to use our NewWritableFile
IOStatus NFSFileSystem::NewLogger(const std::string& fname,
                                  const IOOptions& io_opts,
                                  std::shared_ptr<Logger>* result,
                                  IODebugContext* dbg) {
  // For NFS paths, we need to use our own NewWritableFile
  // Instead of calling target_->NewLogger (local file system will be used)

  if (!IsNFSPath(fname)) {
    return FileSystemWrapper::NewLogger(fname, io_opts, result, dbg);
  }

  std::cout << "[NFSFileSystem] NewLogger: " << fname << std::endl;

  FileOptions options;
  options.io_options = io_opts;
  options.writable_file_max_buffer_size = 1024 * 1024;

  std::unique_ptr<FSWritableFile> writable_file;
  // Call our own NewWritableFile
  const IOStatus status = NewWritableFile(fname, options, &writable_file, dbg);
  if (!status.ok()) {
    return status;
  }

  // Create an EnvLogger (using rocksdb's EnvLogger)
  *result = std::make_shared<EnvLogger>(
    std::move(writable_file), fname, options, Env::Default());
  return IOStatus::OK();
}


// Check if path is a NFS URI
bool NFSFileSystem::IsNFSURI(const std::string& path) {
  return path.find(kProto) == 0;
}

// Extract NFS path from URI
// "nfs://server/path/to/file" -> relative path to mount point
// Since nfs_mount already mounted the base path, we need to return
// the path relative to the mount point
std::string NFSFileSystem::ExtractNFSPathFromURI(const std::string& uri) const {
  if (!IsNFSURI(uri)) {
    return uri;  // Not a URI, return as-is
  }

  // Find the path part after "nfs://server" or "nfs://server:port"
  size_t proto_len = strlen(kProto);  // "nfs://"
  size_t start = proto_len;

  // Skip server name (find first slash after protocol)
  size_t slash = uri.find('/', start);

  if (slash == std::string::npos) {
    // No path, return "/"
    return "/";
  }

  // Extract full path from URI
  std::string full_path =
    uri.substr(slash);  // e.g., "/shared/rocksdb/0/000036.log"
  if (full_path.empty()) {
    return "/";
  }

  // Extract mount point path from nfs_url_
  // nfs_url_ = "nfs://localhost/shared/rocksdb"
  // We need to get "/shared/rocksdb" as the mount point
  size_t url_proto_end = nfs_url_.find("://");
  if (url_proto_end != std::string::npos) {
    size_t url_path_start = nfs_url_.find('/', url_proto_end + 3);
    if (url_path_start != std::string::npos) {
      std::string mount_path =
        nfs_url_.substr(url_path_start);  // "/shared/rocksdb"

      // If full_path starts with mount_path, extract relative path
      if (full_path.find(mount_path) == 0) {
        std::string relative = full_path.substr(mount_path.length());
        // Ensure it starts with / (relative to mount point root)
        if (relative.empty()) {
          relative = "/";
        } else if (relative[0] != '/') {
          relative = "/" + relative;
        }
        std::cout << "[NFSFileSystem] ExtractNFSPathFromURI: " << uri
                  << " -> mount_path: " << mount_path
                  << " -> relative: " << relative << std::endl;
        return relative;  // "/0/000036.log"
      }
    }
  }

  // Fallback: return full path (shouldn't happen in normal cases)
  std::cout << "[NFSFileSystem] ExtractNFSPathFromURI (fallback): " << uri
            << " -> " << full_path << std::endl;
  return full_path;
}

// Create NFSFileSystem from URI (similar to HdfsFileSystem::Create)
Status NFSFileSystem::Create(const std::shared_ptr<FileSystem>& base,
                             const std::string& uri,
                             std::unique_ptr<FileSystem>* result) {
  result->reset();

  if (uri.empty() || uri == kProto) {
    return Status::InvalidArgument("NFS URI cannot be empty or just 'nfs://'");
  }

  if (!IsNFSURI(uri)) {
    return Status::InvalidArgument("URI must start with 'nfs://': " + uri);
  }

  // Parse URI: nfs://server/path or nfs://server:port/path
  size_t proto_len = strlen(kProto);  // "nfs://"
  size_t start = proto_len;

  // Find server and port
  size_t colon = uri.find(':', start);
  size_t slash = uri.find('/', start);

  std::string server;
  std::string mount_path = "/";

  // Check for port (colon before slash)
  if (colon != std::string::npos &&
      (slash == std::string::npos || colon < slash)) {
    // Has port: nfs://server:port/path
    server = uri.substr(start, colon - start);
    // Port is not used by libnfs (it uses standard NFS port), but we skip it
    if (slash != std::string::npos) {
      mount_path = uri.substr(slash);
    }
  } else if (slash != std::string::npos) {
    // No port: nfs://server/path
    server = uri.substr(start, slash - start);
    mount_path = uri.substr(slash);
  } else {
    // No path: nfs://server
    server = uri.substr(start);
    mount_path = "/";
  }

  if (server.empty()) {
    return Status::InvalidArgument("NFS server cannot be empty in URI: " + uri);
  }

  // Construct full NFS URL for libnfs
  std::string nfs_url = kProto + server + mount_path;

  // Create NFSFileSystem with empty local_prefix (URI mode doesn't need it)
  // Store original URI in a custom way - we'll use nfs_url_ to store the full
  // URI
  try {
    // Create NFSFileSystem - it will store the full URI in nfs_url_
    auto nfs_fs = std::make_unique<NFSFileSystem>(nfs_url, "", base);
    // Override nfs_url_ to store the original URI for GetId()
    // Note: This is a workaround since we can't modify nfs_url_ after
    // construction In practice, GetId() will return the nfs_url_ which is the
    // full mount path
    result->reset(nfs_fs.release());
    return Status::OK();
  } catch (const std::exception& e) {
    return Status::IOError("Failed to create NFS FileSystem from URI: " + uri +
                           ", error: " + std::string(e.what()));
  }
}

Status NewNFSFileSystem(const std::string& nfs_url,
                        const std::string& local_prefix,
                        std::shared_ptr<FileSystem>* result) {
  try {
    auto base_fs = FileSystem::Default();
    *result = std::make_shared<NFSFileSystem>(nfs_url, local_prefix, base_fs);
    return Status::OK();
  } catch (const std::exception& e) {
    return Status::IOError("Failed to create NFS FileSystem: " +
                           std::string(e.what()));
  }
}

}  // namespace ROCKSDB_NAMESPACE
