// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.
#include <fcntl.h>

#include <cassert>
#include <memory>
#include <string>

#include "tendisplus/storage/rocks/nfs_filesystem.h"

namespace ROCKSDB_NAMESPACE {

// NFS Sequential File
class NFSSequentialFile : public FSSequentialFile {
 public:
  NFSSequentialFile(struct nfs_context* nfs_ctx,
                    const std::string& fname,
                    struct nfsfh* fh)
    : nfs_ctx_(nfs_ctx), fname_(fname), fh_(fh), offset_(0) {}

  ~NFSSequentialFile() override {
    if (fh_) {
      nfs_close(nfs_ctx_, fh_);
    }
  }

  IOStatus Read(size_t n,
                const IOOptions& options,
                Slice* result,
                char* scratch,
                IODebugContext* dbg) override {
    assert(fh_);
    int bytes_read = nfs_read(nfs_ctx_, fh_, n, scratch);
    if (bytes_read < 0) {
      return IOStatus::IOError("NFS read failed: " +
                               std::string(nfs_get_error(nfs_ctx_)));
    }
    *result = Slice(scratch, bytes_read);
    offset_ += bytes_read;
    return IOStatus::OK();
  }

  IOStatus Skip(uint64_t n) override {
    offset_ += n;
    uint64_t new_offset = 0;
    if (nfs_lseek(nfs_ctx_, fh_, offset_, SEEK_SET, &new_offset) < 0) {
      return IOStatus::IOError("NFS seek failed: " +
                               std::string(nfs_get_error(nfs_ctx_)));
    }
    offset_ = new_offset;
    return IOStatus::OK();
  }

 private:
  struct nfs_context* nfs_ctx_;
  std::string fname_;
  struct nfsfh* fh_;
  uint64_t offset_;
};

// NFS Random Access File
class NFSRandomAccessFile : public FSRandomAccessFile {
 public:
  NFSRandomAccessFile(struct nfs_context* nfs_ctx,
                      const std::string& fname,
                      struct nfsfh* fh)
    : nfs_ctx_(nfs_ctx), fname_(fname), fh_(fh) {}

  ~NFSRandomAccessFile() override {
    if (fh_) {
      nfs_close(nfs_ctx_, fh_);
    }
  }

  IOStatus Read(uint64_t offset,
                size_t n,
                const IOOptions& options,
                Slice* result,
                char* scratch,
                IODebugContext* dbg) const override {
    int bytes_read = nfs_pread(nfs_ctx_, fh_, offset, n, scratch);
    if (bytes_read < 0) {
      return IOStatus::IOError("NFS pread failed: " +
                               std::string(nfs_get_error(nfs_ctx_)));
    }
    *result = Slice(scratch, bytes_read);
    return IOStatus::OK();
  }

 private:
  struct nfs_context* nfs_ctx_;
  std::string fname_;
  struct nfsfh* fh_;
};


// NFS Writable File
class NFSWritableFile : public FSWritableFile {
 public:
  NFSWritableFile(struct nfs_context* nfs_ctx,
                  const std::string& fname,
                  struct nfsfh* fh)
    : nfs_ctx_(nfs_ctx), fname_(fname), fh_(fh) {}

  ~NFSWritableFile() override {
    if (fh_) {
      Close(IOOptions(), nullptr);
    }
  }

  // Use the correct Append signature
  using FSWritableFile::Append;  // Inheriting other reloaded versions

  IOStatus Append(const Slice& data,
                  const IOOptions& options,
                  IODebugContext* dbg) override {
    int bytes_written =
      nfs_write(nfs_ctx_, fh_, data.size(), const_cast<char*>(data.data()));
    if (bytes_written < 0) {
      return IOStatus::IOError("NFS write failed: " +
                               std::string(nfs_get_error(nfs_ctx_)));
    }
    if (static_cast<size_t>(bytes_written) != data.size()) {
      return IOStatus::IOError("NFS partial write");
    }
    return IOStatus::OK();
  }

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override {
    if (fh_) {
      if (nfs_close(nfs_ctx_, fh_) < 0) {
        return IOStatus::IOError("NFS close failed: " +
                                 std::string(nfs_get_error(nfs_ctx_)));
      }
      fh_ = nullptr;
    }
    return IOStatus::OK();
  }

  IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override {
    if (!fh_) {
      return IOStatus::OK();
    }
    if (nfs_fsync(nfs_ctx_, fh_) < 0) {
      return IOStatus::IOError("NFS fsync failed: " +
                               std::string(nfs_get_error(nfs_ctx_)));
    }
    return IOStatus::OK();
  }

  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override {
    return Flush(options, dbg);
  }

 private:
  struct nfs_context* nfs_ctx_;
  std::string fname_;
  struct nfsfh* fh_;
};

// NFSFileSystem file operation implementation
IOStatus NFSFileSystem::NewSequentialFile(
  const std::string& fname,
  const FileOptions& options,
  std::unique_ptr<FSSequentialFile>* result,
  IODebugContext* dbg) {
  if (!IsNFSPath(fname)) {
    return FileSystemWrapper::NewSequentialFile(fname, options, result, dbg);
  }

  std::lock_guard<std::mutex> lock(nfs_mutex_);

  std::string nfs_path = ConvertToNFSPath(fname);
  struct nfsfh* fh;

  if (nfs_open(nfs_ctx_, nfs_path.c_str(), O_RDONLY, &fh) != 0) {
    return IOStatus::IOError("Failed to open NFS file: " + fname + " - " +
                             std::string(nfs_get_error(nfs_ctx_)));
  }

  result->reset(new NFSSequentialFile(nfs_ctx_, fname, fh));
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
  struct nfsfh* fh;

  if (nfs_open(nfs_ctx_, nfs_path.c_str(), O_RDONLY, &fh) != 0) {
    return IOStatus::IOError("Failed to open NFS file: " + fname + " - " +
                             std::string(nfs_get_error(nfs_ctx_)));
  }

  result->reset(new NFSRandomAccessFile(nfs_ctx_, fname, fh));
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
  struct nfsfh* fh;

  if (nfs_creat(nfs_ctx_, nfs_path.c_str(), 0644, &fh) != 0) {
    return IOStatus::IOError("Failed to create NFS file: " + fname + " - " +
                             std::string(nfs_get_error(nfs_ctx_)));
  }

  result->reset(new NFSWritableFile(nfs_ctx_, fname, fh));
  return IOStatus::OK();
}

}  // namespace ROCKSDB_NAMESPACE
