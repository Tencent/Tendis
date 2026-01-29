// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include "tendisplus/storage/rocks/compaction_service.h"

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <unistd.h>

#include <chrono>
#include <climits>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include "csa.grpc.pb.h"  // NOLINT(build/include_subdir)
#include "glog/logging.h"
#include "rocksdb/db/compaction/compaction_job.h"

#include "tendisplus/storage/rocks/remote_compaction/def.h"
#include "tendisplus/storage/rocks/shared_filesystem.h"

// Note: Max gRPC message size is now configurable via
// remote_options_.GetGrpcMaxMessageSize() Default is 16MB, but can be
// overridden in configuration

// CSA server status codes (must match csa_server.cc)
constexpr int kCSACodeBusy = 100;       // CSA server is busy
constexpr int kCSACodeStaleTask = 101;  // Input files not found, stale task

class CSAClient {
 public:
  explicit CSAClient(const std::shared_ptr<grpc::Channel>& channel)
    : stub_(csa::CSAService::NewStub(channel)) {}

  // Shared storage mode: execute compaction using shared filesystem
  ROCKSDB_NAMESPACE::Status OpenAndCompact(
    const ROCKSDB_NAMESPACE::OpenAndCompactOptions& options,
    const std::string& name,
    const std::string& output_directory,
    const std::string& input,
    std::string* output,
    const ROCKSDB_NAMESPACE::CompactionServiceOptionsOverride& override_options,
    const std::string& shared_fs_uri = "",
    const std::string& shared_fs_local_prefix = "") {
    csa::CompactionArgs compaction_args;
    compaction_args.set_name(name);
    compaction_args.set_output_directory(output_directory);
    compaction_args.set_input(input);
    // Pass the shared file system configuration to the CSA server
    if (!shared_fs_uri.empty()) {
      compaction_args.set_shared_fs_uri(shared_fs_uri);
    }
    if (!shared_fs_local_prefix.empty()) {
      compaction_args.set_shared_fs_local_prefix(shared_fs_local_prefix);
    }

    std::cerr << "[CSAClient] ========== Calling CSA Server =========="
              << std::endl;
    std::cerr << "[CSAClient] Name: " << name << std::endl;
    std::cerr << "[CSAClient] Output Directory: " << output_directory
              << std::endl;
    std::cerr << "[CSAClient] Shared FS URI: "
              << (shared_fs_uri.empty() ? "(NOT SET!)" : shared_fs_uri)
              << std::endl;
    std::cerr << "[CSAClient] Input size: " << input.size() << " bytes"
              << std::endl;
    std::cerr.flush();

    csa::CompactionReply compaction_reply;
    grpc::ClientContext context;

    // Set timeout for the RPC call (30 seconds)
    std::chrono::system_clock::time_point deadline =
      std::chrono::system_clock::now() + std::chrono::seconds(30);
    context.set_deadline(deadline);

    std::cerr << "[CSAClient] Sending gRPC request to CSA server..."
              << std::endl;
    std::cerr.flush();
    grpc::Status status = stub_->ExecuteCompactionTask(
      &context, compaction_args, &compaction_reply);

    std::cerr << "[CSAClient] gRPC call completed. Status: "
              << (status.ok() ? "OK" : "FAILED") << std::endl;
    if (!status.ok()) {
      std::cerr << "[CSAClient] Error code: " << status.error_code()
                << std::endl;
      std::cerr << "[CSAClient] Error message: " << status.error_message()
                << std::endl;
    }
    std::cerr.flush();

    if (!status.ok()) {
      std::string error_msg = "gRPC call failed: " + status.error_message();
      if (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
        error_msg += " (timeout)";
      } else if (status.error_code() == grpc::StatusCode::UNAVAILABLE) {
        error_msg += " (service unavailable)";
      }
      std::cerr << "[CSAClient] ERROR: " << error_msg << std::endl;
      return ROCKSDB_NAMESPACE::Status::IOError(error_msg);
    }

    std::cerr << "[CSAClient] Compaction reply code: "
              << compaction_reply.code() << std::endl;
    std::cerr << "[CSAClient] Compaction reply result size: "
              << compaction_reply.result().size() << " bytes" << std::endl;
    std::cerr.flush();

    // Check for CSA server busy status
    if (compaction_reply.code() == kCSACodeBusy) {
      std::cerr << "[CSAClient] CSA server is busy, should use local compaction"
                << std::endl;
      std::cerr.flush();
      return ROCKSDB_NAMESPACE::Status::Busy("CSA server busy");
    }

    // Check for stale task (input files not found)
    // This is detected early by CSA server before expensive OpenAndCompact
    if (compaction_reply.code() == kCSACodeStaleTask) {
      std::cerr << "[CSAClient] CSA server detected stale task (input files "
                   "not found), returning empty result"
                << std::endl;
      std::cerr.flush();
      // Return a special status that will be handled by WaitForCompleteV2
      return ROCKSDB_NAMESPACE::Status::NotFound(
        "Stale task: input file not found");
    }

    if (compaction_reply.code() != 0) {
      std::cerr << "[CSAClient] ERROR: Remote compaction failed with code: "
                << compaction_reply.code() << std::endl;
      std::cerr.flush();
      return ROCKSDB_NAMESPACE::Status::IOError(
        "Remote compaction failed with code: " +
        std::to_string(compaction_reply.code()) +
        (compaction_reply.code() == 5 ? " (IOError/NOENT)" : ""));
    }

    if (compaction_reply.result().empty()) {
      std::cerr << "[CSAClient] ERROR: Remote compaction returned empty result"
                << std::endl;
      std::cerr.flush();
      return ROCKSDB_NAMESPACE::Status::IOError(
        "Remote compaction returned empty result");
    }

    output->assign(compaction_reply.result());
    std::cerr << "[CSAClient] ========== SUCCESS: Remote compaction completed "
                 "=========="
              << std::endl;
    std::cerr.flush();
    return ROCKSDB_NAMESPACE::Status::OK();
  }

 private:
  std::unique_ptr<csa::CSAService::Stub> stub_;
};

namespace ROCKSDB_NAMESPACE {

namespace {
// Convert relative path to absolute path (helper function)
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

CompactionServiceJobStatus MyTestCompactionService::StartV2(
  const CompactionServiceJobInfo& info,
  const std::string& compaction_service_input) {
  InstrumentedMutexLock l(&mutex_);
  start_info_ = info;
  assert(info.db_name == db_path_);
  jobs_.emplace(info.job_id, compaction_service_input);

  std::cerr << "[CompactionService] ========== StartV2 CALLED =========="
            << std::endl;
  std::cerr << "[CompactionService] Job ID: " << info.job_id << std::endl;
  std::cerr << "[CompactionService] DB Path: " << db_path_ << std::endl;
  std::cerr << "[CompactionService] CSA Address: "
            << (remote_options_.csa_address.empty()
                  ? "(NOT CONFIGURED!)"
                  : remote_options_.csa_address)
            << std::endl;
  std::cerr << "[CompactionService] Shared FS URI: "
            << (remote_options_.shared_fs_uri.empty()
                  ? "(NOT CONFIGURED!)"
                  : remote_options_.shared_fs_uri)
            << std::endl;
  std::cerr << "[CompactionService] Input size: "
            << compaction_service_input.size() << " bytes" << std::endl;
  std::cerr << "[CompactionService] ===================================="
            << std::endl;
  std::cerr.flush();

  CompactionServiceJobStatus s = CompactionServiceJobStatus::kSuccess;
  if (is_override_start_status_) {
    return override_start_status_;
  }
  return s;
}

CompactionServiceJobStatus MyTestCompactionService::WaitForCompleteV2(
  const CompactionServiceJobInfo& info,
  std::string* compaction_service_result) {
  std::string compaction_input;
  assert(info.db_name == db_path_);
  {
    InstrumentedMutexLock l(&mutex_);
    wait_info_ = info;
    auto i = jobs_.find(info.job_id);
    if (i == jobs_.end()) {
      return CompactionServiceJobStatus::kFailure;
    }
    compaction_input = std::move(i->second);
    jobs_.erase(i);
  }

  if (is_override_wait_status_) {
    return override_wait_status_;
  }

  CompactionServiceOptionsOverride options_override;
  // CRITICAL: Always use the same env as the main DB to ensure consistency
  // If NFS is enabled, this will be NFS env, ensuring all compaction (remote or
  // local) uses NFS
  options_override.env = options_.env;
  options_override.file_checksum_gen_factory =
    options_.file_checksum_gen_factory;
  options_override.comparator = options_.comparator;
  options_override.merge_operator = options_.merge_operator;
  options_override.compaction_filter = options_.compaction_filter;
  options_override.compaction_filter_factory =
    options_.compaction_filter_factory;
  options_override.prefix_extractor = options_.prefix_extractor;
  options_override.table_factory = options_.table_factory;
  options_override.sst_partitioner_factory = options_.sst_partitioner_factory;
  options_override.statistics = statistics_;
  if (!listeners_.empty()) {
    options_override.listeners = listeners_;
  }

  if (!table_properties_collector_factories_.empty()) {
    options_override.table_properties_collector_factories =
      table_properties_collector_factories_;
  }

  Status s;
  std::string job_id = std::to_string(info.job_id);
  std::string output_dir = db_path_ + "/" + job_id;

  std::cerr
    << "[CompactionService] ========== WaitForCompleteV2 CALLED =========="
    << std::endl;
  std::cerr << "[CompactionService] Job ID: " << job_id << std::endl;
  std::cerr << "[CompactionService] DB Path: " << db_path_ << std::endl;
  std::cerr << "[CompactionService] CSA Address: "
            << (remote_options_.csa_address.empty()
                  ? "(NOT CONFIGURED!)"
                  : remote_options_.csa_address)
            << std::endl;
  std::cerr << "[CompactionService] Shared FS URI: "
            << (remote_options_.shared_fs_uri.empty()
                  ? "(NOT CONFIGURED!)"
                  : remote_options_.shared_fs_uri)
            << std::endl;
  std::cerr << "[CompactionService] Input size: " << compaction_input.size()
            << " bytes" << std::endl;
  std::cerr
    << "[CompactionService] ==============================================="
    << std::endl;
  std::cerr.flush();

  // Check if NFS is enabled (for consistency, all compaction must use NFS if
  // enabled)
  bool nfs_enabled = !remote_options_.shared_fs_uri.empty() ||
    !remote_options_.shared_fs_local_prefix.empty();

  // Check if remote compaction is enabled (CSA address must be configured)
  if (remote_options_.csa_address.empty()) {
    std::cerr << "[CompactionService] ERROR: CSA address is EMPTY! "
              << "Remote compaction will NOT be used. "
              << "Please configure csa_address in config file." << std::endl;
    if (nfs_enabled) {
      std::cout << "[CompactionService] Falling back to local compaction "
                   "(using NFS for consistency)"
                << std::endl;
    } else {
      std::cerr << "[CompactionService] Falling back to local compaction"
                << std::endl;
    }
    // Return kUseLocal - RocksDB will use options_override.env which is NFS env
    // if NFS is enabled
    return CompactionServiceJobStatus::kUseLocal;
  }

  // Validate CSA address format (basic check: should contain ':')
  if (remote_options_.csa_address.find(':') == std::string::npos) {
    if (nfs_enabled) {
      std::cerr << "[CompactionService] Invalid CSA address format: "
                << remote_options_.csa_address
                << " (expected format: host:port), falling back to local "
                   "compaction (using NFS for consistency)"
                << std::endl;
    } else {
      std::cerr
        << "[CompactionService] Invalid CSA address format: "
        << remote_options_.csa_address
        << " (expected format: host:port), falling back to local compaction"
        << std::endl;
    }
    // Return kUseLocal - RocksDB will use options_override.env which is NFS env
    // if NFS is enabled
    return CompactionServiceJobStatus::kUseLocal;
  }

  // Create channel with configured message size limit
  int64_t max_msg_size = remote_options_.GetGrpcMaxMessageSize();
  std::cerr << "[CompactionService] Creating gRPC channel to "
            << remote_options_.csa_address
            << " with max message size: " << max_msg_size << " bytes"
            << std::endl;
  std::cerr.flush();

  grpc::ChannelArguments channel_args;
  channel_args.SetMaxReceiveMessageSize(static_cast<int>(max_msg_size));
  channel_args.SetMaxSendMessageSize(static_cast<int>(max_msg_size));

  auto channel = grpc::CreateCustomChannel(remote_options_.csa_address,
                                           grpc::InsecureChannelCredentials(),
                                           channel_args);

  if (!channel) {
    std::cerr << "[CompactionService] ERROR: Failed to create gRPC channel to "
              << remote_options_.csa_address
              << ", falling back to local compaction" << std::endl;
    std::cerr.flush();
    // Return kUseLocal - RocksDB will use options_override.env which is NFS env
    // if NFS is enabled
    return CompactionServiceJobStatus::kUseLocal;
  }

  std::cerr << "[CompactionService] gRPC channel created successfully"
            << std::endl;
  std::cerr.flush();

  CSAClient csa_client(channel);

  // Shared storage mode (using NFS/HDFS)
  // CSA server reads from shared storage (read-only via URI), Tendisplus
  // installs results
  std::cerr
    << "[CompactionService] ========== Using REMOTE COMPACTION =========="
    << std::endl;
  std::cerr << "[CompactionService] Job ID: " << job_id << std::endl;
  std::cerr << "[CompactionService] CSA Server: " << remote_options_.csa_address
            << std::endl;
  std::cerr << "[CompactionService] Shared FS URI: "
            << remote_options_.shared_fs_uri << std::endl;
  std::cerr
    << "[CompactionService] =============================================="
    << std::endl;
  std::cerr.flush();

  // Build URI by directly replacing local mount point with shared filesystem
  // URI Example: db_path="/mnt/rocksdb/db",
  // shared_fs_uri="nfs://localhost/shared/rocksdb"
  //          -> Replace "/mnt/rocksdb" with "nfs://localhost/shared/rocksdb"
  //          -> Result: "nfs://localhost/shared/rocksdb/db"
  // Uses unified shared filesystem configuration
  // (remote_compaction.shared_fs_uri)
  std::string db_path_uri;
  std::string output_dir_uri;

  // Unified approach: Use shared filesystem URI for all filesystem types
  // Supports: nfs://, hdfs://, s3://, etc.
  if (!remote_options_.shared_fs_uri.empty() &&
      IsSharedFilesystemURI(remote_options_.shared_fs_uri)) {
    // Use unified helper to convert local path to shared filesystem URI
    // This works for all filesystem types (NFS, HDFS, S3, etc.)
    db_path_uri = ConvertLocalPathToSharedURI(
      db_path_,
      remote_options_.shared_fs_uri,
      remote_options_.shared_fs_local_prefix);  // Optional mount point

    output_dir_uri =
      ConvertLocalPathToSharedURI(output_dir,
                                  remote_options_.shared_fs_uri,
                                  remote_options_.shared_fs_local_prefix);

    // If output_dir doesn't match mount point, append job_id to db_path_uri
    if (output_dir_uri == ToAbsolutePath(output_dir)) {
      // Conversion failed, use db_path_uri as base
      output_dir_uri = db_path_uri + "/" + job_id;
    }

    std::cout
      << "[CompactionService] URI conversion (unified shared filesystem):"
      << "\n  shared_fs_uri: " << remote_options_.shared_fs_uri
      << "\n  local_mount_point: "
      << (remote_options_.shared_fs_local_prefix.empty()
            ? "(auto-inferred)"
            : remote_options_.shared_fs_local_prefix)
      << "\n  db_path: " << db_path_ << " -> " << db_path_uri
      << "\n  output_dir: " << output_dir << " -> " << output_dir_uri
      << std::endl;
  } else {
    // No shared_fs_uri or not a shared filesystem URI, use absolute paths
    db_path_uri = ToAbsolutePath(db_path_);
    output_dir_uri = ToAbsolutePath(output_dir);

    std::cout << "[CompactionService] Using absolute paths (no shared storage):"
              << "\n  db_path: " << db_path_ << " -> " << db_path_uri
              << "\n  output_dir: " << output_dir << " -> " << output_dir_uri
              << std::endl;
  }

  // Use the configured remote options
  RemoteOpenAndCompactOptions opts = remote_options_;

  std::cerr << "[CompactionService] About to call CSA server OpenAndCompact:"
            << std::endl;
  std::cerr << "  db_path_uri: " << db_path_uri << std::endl;
  std::cerr << "  output_dir_uri: " << output_dir_uri << std::endl;
  std::cerr << "  shared_fs_uri: " << remote_options_.shared_fs_uri
            << std::endl;
  std::cerr << "  compaction_input size: " << compaction_input.size()
            << " bytes" << std::endl;
  std::cerr.flush();

  s = csa_client.OpenAndCompact(
    opts,
    db_path_uri,
    output_dir_uri,
    compaction_input,
    compaction_service_result,
    options_override,
    remote_options_.shared_fs_uri,
    "");  // Don't pass local_prefix to CSA - it uses URI directly

  std::cerr << "[CompactionService] CSA server OpenAndCompact returned: "
            << s.ToString() << std::endl;
  std::cerr << "[CompactionService] Result size: "
            << compaction_service_result->size() << " bytes" << std::endl;
  std::cerr.flush();

  if (is_override_wait_result_) {
    *compaction_service_result = override_wait_result_;
  }
  compaction_num_.fetch_add(1);

  LOG(INFO) << "[CompactionService] WaitForCompleteV2 result: "
            << (s.ok() ? "OK" : s.ToString())
            << ", result_size=" << compaction_service_result->size();

  if (s.ok()) {
    return CompactionServiceJobStatus::kSuccess;
  } else {
    std::string err_msg = s.ToString();

    LOG(WARNING) << "[CompactionService] Error details - IsIOError: "
                 << s.IsIOError() << ", IsBusy: " << s.IsBusy()
                 << ", code: " << static_cast<int>(s.code())
                 << ", msg: " << err_msg;

    // Check if CSA server is busy - should fallback to local immediately
    if (s.IsBusy()) {
      LOG(INFO) << "[CompactionService] CSA server busy, falling back to local "
                   "compaction";
      return CompactionServiceJobStatus::kUseLocal;
    }

    // Check if this is a stale task detected early by CSA server
    // CSA server checks input file existence before OpenAndCompact
    if (s.IsNotFound()) {
      LOG(INFO) << "[CompactionService] Early stale task detection by CSA, "
                << "constructing empty result and returning kSuccess";
      CompactionServiceResult empty_result;
      empty_result.status = Status::OK();
      empty_result.Write(compaction_service_result);
      return CompactionServiceJobStatus::kSuccess;
    }

    // Check if this is a stale task (file not found on remote) vs
    // connection/other errors Stale task: CSA returned code 5 with NOENT - file
    // was already compacted/deleted Connection error: gRPC failed to connect -
    // should fallback to local
    bool is_file_not_found = s.IsIOError() &&
      (err_msg.find("NOENT") != std::string::npos ||
       err_msg.find("No such file") != std::string::npos);

    bool is_connection_error = s.IsIOError() &&
      (err_msg.find("gRPC call failed") != std::string::npos ||
       err_msg.find("Connection refused") != std::string::npos ||
       err_msg.find("connect") != std::string::npos);

    if (is_file_not_found) {
      // Stale task: file was already compacted by another task, return empty
      // result
      LOG(WARNING) << "[CompactionService] File not found (stale task), "
                   << "constructing empty result and returning kSuccess";
      CompactionServiceResult empty_result;
      empty_result.status = Status::OK();
      empty_result.Write(compaction_service_result);
      return CompactionServiceJobStatus::kSuccess;
    }

    if (is_connection_error) {
      LOG(WARNING) << "[CompactionService] CSA connection failed, falling back "
                      "to local compaction";
    } else if (nfs_enabled) {
      LOG(WARNING) << "[CompactionService] Remote compaction error, falling "
                      "back to local compaction "
                   << "(using NFS for consistency): " << err_msg;
    } else {
      LOG(WARNING) << "[CompactionService] Remote compaction error, falling "
                      "back to local: "
                   << err_msg;
    }
    // Return kUseLocal - RocksDB will use options_override.env which is NFS env
    // if NFS is enabled
    return CompactionServiceJobStatus::kUseLocal;
  }
}
}  // namespace ROCKSDB_NAMESPACE
