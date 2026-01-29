// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#pragma once
#include <atomic>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/options.h"

#include "tendisplus/storage/rocks/remote_compaction/remote_compaction_config.h"

namespace ROCKSDB_NAMESPACE {

// Remote compaction mode (only shared storage is supported)
enum class RemoteCompactionMode {
  kSharedStorage =
    0  // Use shared storage (NFS/HDFS/S3) - both sides access same files
};

struct RemoteOpenAndCompactOptions : public OpenAndCompactOptions {
  // Allows cancellation of an in-progress compaction.
  std::atomic<bool>* canceled = nullptr;

  // CSA server address (empty means disabled)
  // Should be set from configuration, e.g., "localhost:8010" or
  // "192.168.1.100:8010"
  std::string csa_address;

  // Shared FileSystem configuration (supports NFS, HDFS, S3, etc.)
  // URI format: "nfs://host/path", "hdfs://host:port/path",
  // "s3://bucket/prefix" Should be set from configuration
  std::string shared_fs_uri;
  std::string shared_fs_local_prefix;

  // Remote compaction mode (only shared storage is supported)
  // kSharedStorage: Use shared storage (NFS/HDFS/S3) - both sides access same
  // files Should be set from configuration
  RemoteCompactionMode mode = RemoteCompactionMode::kSharedStorage;

  // Advanced settings (should be set from configuration)
  int64_t csa_max_concurrent_tasks =
    0;                                // Max concurrent tasks (0 = use default)
  int64_t grpc_max_message_size = 0;  // Max gRPC message size (0 = use default)
  int32_t check_time_interval = 0;    // Check time interval (0 = use default)
  uint64_t max_reschedule = 0;        // Max reschedule times (0 = use default)

  // Helper methods to get values with defaults
  int64_t GetMaxConcurrentTasks() const {
    return RemoteCompactionConfig::GetOrDefault(
      csa_max_concurrent_tasks,
      RemoteCompactionConfig::kDefaultMaxConcurrentTasks);
  }

  int64_t GetGrpcMaxMessageSize() const {
    return RemoteCompactionConfig::GetOrDefault(
      grpc_max_message_size,
      RemoteCompactionConfig::kDefaultGrpcMaxMessageSize);
  }

  int32_t GetCheckTimeInterval() const {
    return RemoteCompactionConfig::GetOrDefault(
      check_time_interval, RemoteCompactionConfig::kDefaultCheckTimeInterval);
  }

  uint64_t GetMaxReschedule() const {
    return RemoteCompactionConfig::GetOrDefault(
      max_reschedule, RemoteCompactionConfig::kDefaultMaxReschedule);
  }
};
}  // namespace ROCKSDB_NAMESPACE
