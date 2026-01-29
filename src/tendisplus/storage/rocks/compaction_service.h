// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#pragma once
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "monitoring/instrumented_mutex.h"
#include "rocksdb/metadata.h"
#include "rocksdb/options.h"

#include "tendisplus/storage/rocks/remote_compaction/def.h"

namespace ROCKSDB_NAMESPACE {

class MyTestCompactionService : public CompactionService {
 public:
  MyTestCompactionService(
    std::string db_path,
    Options& options,
    std::shared_ptr<Statistics>& statistics,
    std::vector<std::shared_ptr<EventListener>>& listeners,
    std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>
      table_properties_collector_factories,
    const RemoteOpenAndCompactOptions& remote_options =
      RemoteOpenAndCompactOptions())
    : db_path_(std::move(db_path)),
      options_(options),
      statistics_(statistics),
      start_info_("na", "na", "na", 0, Env::TOTAL),
      wait_info_("na", "na", "na", 0, Env::TOTAL),
      listeners_(listeners),
      table_properties_collector_factories_(
        std::move(table_properties_collector_factories)),
      remote_options_(remote_options) {}

  static const char* kClassName() {
    return "MyTestCompactionService";
  }

  const char* Name() const override {
    return kClassName();
  }

  CompactionServiceJobStatus StartV2(
    const CompactionServiceJobInfo& info,
    const std::string& compaction_service_input) override;

  CompactionServiceJobStatus WaitForCompleteV2(
    const CompactionServiceJobInfo& info,
    std::string* compaction_service_result) override;

  int GetCompactionNum() {
    return compaction_num_.load();
  }

  CompactionServiceJobInfo GetCompactionInfoForStart() {
    return start_info_;
  }
  CompactionServiceJobInfo GetCompactionInfoForWait() {
    return wait_info_;
  }

  void OverrideStartStatus(CompactionServiceJobStatus s) {
    is_override_start_status_ = true;
    override_start_status_ = s;
  }

  void OverrideWaitStatus(CompactionServiceJobStatus s) {
    is_override_wait_status_ = true;
    override_wait_status_ = s;
  }

  void OverrideWaitResult(std::string str) {
    is_override_wait_result_ = true;
    override_wait_result_ = std::move(str);
  }

  void ResetOverride() {
    is_override_wait_result_ = false;
    is_override_start_status_ = false;
    is_override_wait_status_ = false;
  }

  void SetCanceled(bool canceled) {
    canceled_ = canceled;
  }

 private:
  InstrumentedMutex mutex_;
  std::atomic_int compaction_num_{0};
  std::map<uint64_t, std::string> jobs_;
  const std::string db_path_;
  Options options_;
  std::shared_ptr<Statistics> statistics_;
  CompactionServiceJobInfo start_info_;
  CompactionServiceJobInfo wait_info_;
  bool is_override_start_status_ = false;
  CompactionServiceJobStatus override_start_status_ =
    CompactionServiceJobStatus::kFailure;
  bool is_override_wait_status_ = false;
  CompactionServiceJobStatus override_wait_status_ =
    CompactionServiceJobStatus::kFailure;
  bool is_override_wait_result_ = false;
  std::string override_wait_result_;
  std::vector<std::shared_ptr<EventListener>> listeners_;
  std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>
    table_properties_collector_factories_;
  std::atomic_bool canceled_{false};

  // Remote compaction configuration (all settings from config file)
  RemoteOpenAndCompactOptions remote_options_;
};
}  // namespace ROCKSDB_NAMESPACE
