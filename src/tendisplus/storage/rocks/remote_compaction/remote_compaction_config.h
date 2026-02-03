// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace ROCKSDB_NAMESPACE {

// Remote compaction configuration helper
// Provides default values and configuration management
class RemoteCompactionConfig {
 public:
  // Default values (can be overridden by configuration)
  static constexpr const char* kDefaultMode = "shared_storage";
  static constexpr int64_t kDefaultMaxConcurrentTasks = 5;
  static constexpr int64_t kDefaultGrpcMaxMessageSize =
    16 * 1024 * 1024;  // 16MB
  static constexpr int32_t kDefaultCheckTimeInterval = 1;
  static constexpr uint64_t kDefaultMaxReschedule = 5;

  // Mode strings
  static constexpr const char* kModeSharedStorage = "shared_storage";

  // Get default CSA address (empty means disabled by default)
  static std::string GetDefaultCsaAddress() {
    return "";
  }

  // Helper to get value with default
  template <typename T>
  static T GetOrDefault(const T& config_value, const T& default_value) {
    return config_value != T{} ? config_value : default_value;
  }

  // Helper to get string value with default
  static std::string GetStringOrDefault(const std::string& config_value,
                                        const std::string& default_value) {
    return config_value.empty() ? default_value : config_value;
  }

  // Validate mode string
  static bool IsValidMode(const std::string& mode) {
    return mode == kModeSharedStorage || mode.empty();  // empty means disabled
  }

  // Check if remote compaction is enabled
  static bool IsEnabled(const std::string& csa_address,
                        const std::string& mode) {
    return !csa_address.empty() && !mode.empty();
  }
};

}  // namespace ROCKSDB_NAMESPACE
