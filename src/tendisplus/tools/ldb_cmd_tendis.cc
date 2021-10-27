
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE
#include "rocksdb/utilities/ldb_cmd.h"

#include <cinttypes>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>

#include "db/db_impl/db_impl.h"
#include "db/dbformat.h"
#include "db/log_reader.h"
#include "db/write_batch_internal.h"
#include "env/composite_env_wrapper.h"
#include "file/filename.h"
#include "port/port_dirent.h"
#include "rocksdb/cache.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/utilities/backupable_db.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/utilities/debug.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/scoped_arena_iterator.h"
#include "table/sst_file_dumper.h"
#include "./ldb_cmd_impl.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/file_checksum_helper.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"
#include "utilities/merge_operators.h"
#include "utilities/ttl/db_ttl_impl.h"
#include "tendisplus/storage/record.h"
#include "tendisplus/storage/kvstore.h"
using namespace tendisplus;

// ----------------------------------------------------------------------------

namespace ROCKSDB_NAMESPACE {
TScanCommand::TScanCommand(const std::vector<std::string>& /*params*/,
                           const std::map<std::string, std::string>& options,
                           const std::vector<std::string>& flags)
  : LDBCommand(options,
               flags,
               true,
               BuildCmdLineOptions({ARG_TTL,
                                    ARG_NO_VALUE,
                                    ARG_HEX,
                                    ARG_KEY_HEX,
                                    ARG_TO,
                                    ARG_VALUE_HEX,
                                    ARG_FROM,
                                    ARG_TIMESTAMP,
                                    ARG_MAX_KEYS,
                                    ARG_TTL_START,
                                    ARG_TTL_END,
                                    "printlog"})),
    start_key_specified_(false),
    end_key_specified_(false),
    max_keys_scanned_(-1),
    no_value_(true),
    print_log_(false) {
  std::map<std::string, std::string>::const_iterator itr =
    options.find(ARG_FROM);
  if (itr != options.end()) {
    start_key_ = itr->second;
    if (is_key_hex_) {
      start_key_ = HexToString(start_key_);
    }
    start_key_specified_ = true;
  }
  itr = options.find(ARG_TO);
  if (itr != options.end()) {
    end_key_ = itr->second;
    if (is_key_hex_) {
      end_key_ = HexToString(end_key_);
    }
    end_key_specified_ = true;
  }

  std::vector<std::string>::const_iterator vitr =
    std::find(flags.begin(), flags.end(), ARG_NO_VALUE);
  if (vitr != flags.end()) {
    no_value_ = true;
  }

  itr = options.find(ARG_MAX_KEYS);
  if (itr != options.end()) {
    try {
#if defined(CYGWIN)
      max_keys_scanned_ = strtol(itr->second.c_str(), 0, 10);
#else
      max_keys_scanned_ = std::stoi(itr->second);
#endif
    } catch (const std::invalid_argument&) {
      exec_state_ =
        LDBCommandExecuteResult::Failed(ARG_MAX_KEYS + " has an invalid value");
    } catch (const std::out_of_range&) {
      exec_state_ = LDBCommandExecuteResult::Failed(
        ARG_MAX_KEYS + " has a value out-of-range");
    }
  }

  vitr = std::find(flags.begin(), flags.end(), "printlog");
  if (vitr != flags.end()) {
    print_log_ = true;
  }
}

void TScanCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(TScanCommand::Name());
  ret.append(HelpRangeCmdArgs());
  ret.append(" [--" + ARG_TTL + "]");
  ret.append(" [--" + ARG_TIMESTAMP + "]");
  ret.append(" [--" + ARG_MAX_KEYS + "=<N>q] ");
  ret.append(" [--" + ARG_TTL_START + "=<N>:- is inclusive]");
  ret.append(" [--" + ARG_TTL_END + "=<N>:- is exclusive]");
  ret.append(" [--" + ARG_NO_VALUE + "]");
  ret.append(" [--printlog]");
  ret.append("\n");
}

void TScanCommand::printLog(const std::string& value) {
  Expected<ReplLogValueV2> logValue = ReplLogValueV2::decode(value);
  if (!logValue.ok()) {
    std::cout << "decode logvalue failed";
  }

  std::cout << " txnid:" << logValue.value().getTxnId()
            << " chunkid:" << logValue.value().getChunkId()
            << " ts:" << logValue.value().getTimestamp()
            << " cmdstr:" << logValue.value().getCmd() << std::endl;

  if (logValue.value().getChunkId() == Transaction::CHUNKID_FLUSH) {
    std::cout << "  op:"
              << "FLUSH"
              << " cmd:" << logValue.value().getCmd() << std::endl;
  } else if (logValue.value().getChunkId() == Transaction::CHUNKID_MIGRATE) {
    std::cout << "  op:"
              << "MIGRATE"
              << " cmd:" << logValue.value().getCmd() << std::endl;
  }

  size_t offset = logValue.value().getHdrSize();
  auto data = logValue.value().getData();
  size_t dataSize = logValue.value().getDataSize();
  while (offset < dataSize) {
    size_t size = 0;
    auto entry = ReplLogValueEntryV2::decode(
      (const char*)data + offset, dataSize - offset, &size);
    if (!entry.ok()) {
      std::cout << "ReplLogValueEntryV2::decode failed";
    }
    offset += size;

    Expected<RecordKey> opkey = RecordKey::decode(entry.value().getOpKey());
    if (!opkey.ok()) {
      std::cerr << "decode opkey failed, err:" << opkey.status().toString()
                << std::endl;
      std::cout << "RecordKey::decode failed.";
    }
    if (entry.value().getOp() == ReplOp::REPL_OP_DEL) {
      std::cout << "  op:" << (uint32_t)entry.value().getOp()
                << " fkey:" << opkey.value().getPrimaryKey()
                << " skey:" << opkey.value().getSecondaryKey() << std::endl;
    } else {
      Expected<RecordValue> opvalue =
        RecordValue::decode(entry.value().getOpValue());
      if (!opvalue.ok()) {
        std::cerr << "decode opvalue failed, err:"
                  << opvalue.status().toString() << std::endl;
        std::cout << "RecordValue::decode failed.";
      }
      std::cout << "  op:" << (uint32_t)entry.value().getOp()
                << " fkey:" << opkey.value().getPrimaryKey()
                << " skey:" << opkey.value().getSecondaryKey()
                << " opvalue:" << opvalue.value().getValue() << std::endl;
    }
  }
}

void TScanCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }

  int num_keys_scanned = 0;
  ReadOptions scan_read_opts;
  scan_read_opts.total_order_seek = true;
  Iterator* it = db_->NewIterator(scan_read_opts, GetCfHandle());
  if (start_key_specified_) {
    it->Seek(start_key_);
  } else {
    it->SeekToFirst();
  }
  int ttl_start;
  if (!ParseIntOption(option_map_, ARG_TTL_START, ttl_start, exec_state_)) {
    ttl_start = DBWithTTLImpl::kMinTimestamp;  // TTL introduction time
  }
  int ttl_end;
  if (!ParseIntOption(option_map_, ARG_TTL_END, ttl_end, exec_state_)) {
    ttl_end = DBWithTTLImpl::kMaxTimestamp;  // Max time allowed by TTL feature
  }
  if (ttl_end < ttl_start) {
    fprintf(stderr, "Error: End time can't be less than start time\n");
    delete it;
    return;
  }
  if (is_db_ttl_ && timestamp_) {
    fprintf(stdout,
            "Scanning key-values from %s to %s\n",
            TimeToHumanString(ttl_start).c_str(),
            TimeToHumanString(ttl_end).c_str());
  }
  for (;
       it->Valid() && (!end_key_specified_ || it->key().ToString() < end_key_);
       it->Next()) {
    if (is_db_ttl_) {
      TtlIterator* it_ttl = static_cast_with_check<TtlIterator>(it);
      int rawtime = it_ttl->ttl_timestamp();
      if (rawtime < ttl_start || rawtime >= ttl_end) {
        continue;
      }
      if (timestamp_) {
        fprintf(stdout, "%s ", TimeToHumanString(rawtime).c_str());
      }
    }

    Slice key_slice = it->key();

    std::string formatted_key = key_slice.ToString(false);

    std::string formatted_value;
    Slice val_slice = it->value();
    if (is_value_hex_) {
      formatted_value = "0x" + val_slice.ToString(true /* hex */);
    } else {
      formatted_value = val_slice.ToString(false);
    }

    auto exptRcd = Record::decode(formatted_key, formatted_value);

    if (!exptRcd.ok()) {
      LOG(ERROR) << "reord decode fail:" << exptRcd.status().toString();
    }

    auto record = exptRcd.value();
    auto keyType = record.getRecordKey().getRecordType();
    /* NOTE(wayenchen) ignore if not primary key */
    if (keyType != RecordType::RT_DATA_META &&
        keyType != RecordType::RT_BINLOG && no_value_) {
      continue;
    }
    // value length of string
    uint64_t vallen = 0;
    // num of value of secondary key
    uint64_t fiedlen = 0;

    auto keyR = record.getRecordKey();
    auto value = record.getRecordValue();
    std::string type = "";

    auto valueType = value.getRecordType();
    if (valueType == RecordType::RT_KV) {
      vallen = value.getValue().size();
      type = "a";
    } else if (valueType == RecordType::RT_HASH_META) {
      Expected<HashMetaValue> exptHashMeta =
        HashMetaValue::decode(value.getValue());
      if (!exptHashMeta.ok()) {
        LOG(ERROR) << "hash meta decode fail:"
                   << exptHashMeta.status().toString();
      }
      fiedlen = exptHashMeta.value().getCount();
      type = "H";
    } else if (valueType == RecordType::RT_SET_META) {
      Expected<SetMetaValue> setHashMeta =
        SetMetaValue::decode(value.getValue());
      if (!setHashMeta.ok()) {
        LOG(ERROR) << "set meta decode fail:"
                   << setHashMeta.status().toString();
      }
      fiedlen = setHashMeta.value().getCount();
      type = "S";
    } else if (valueType == RecordType::RT_ZSET_META) {
      Expected<ZSlMetaValue> zsetHashMeta =
        ZSlMetaValue::decode(value.getValue());
      if (!zsetHashMeta.ok()) {
        LOG(ERROR) << "zset meta decode fail:"
                   << zsetHashMeta.status().toString();
      }
      /*NOTE(wayenchen) sam value as zcard*/
      fiedlen = zsetHashMeta.value().getCount() - 1;
      type = "Z";
    } else if (valueType == RecordType::RT_LIST_META) {
      Expected<ListMetaValue> listHashMeta =
        ListMetaValue::decode(value.getValue());
      if (!listHashMeta.ok()) {
        LOG(ERROR) << "list meta decode fail:"
                   << listHashMeta.status().toString();
      }
      fiedlen = listHashMeta.value().getTail() - listHashMeta.value().getHead();
      type = "L";
    } else if (keyType == RecordType::RT_BINLOG) {
      fiedlen = value.encode().size();
      type = "B";
      if (print_log_) {
        printLog(formatted_value);
      }
    }

    std::string pKey = record.getRecordKey().getPrimaryKey();
    // type dbid PrimaryKey ttl value(kv) subKeysCount(list/set/zset/hash)
    fprintf(stdout,
            "%.*s %" PRIu32 " %.*s %" PRIu64 " %" PRIu64 " %" PRIu64 "\n",
            static_cast<int>(type.size()),
            type.data(),
            record.getRecordKey().getDbId(),
            static_cast<int>(pKey.size()),
            pKey.data(),
            record.getRecordValue().getTtl(),
            vallen,
            fiedlen);

    num_keys_scanned++;
    if (max_keys_scanned_ >= 0 && num_keys_scanned >= max_keys_scanned_) {
      break;
    }
  }
  if (!it->status().ok()) {  // Check for any errors found during the scan
    exec_state_ = LDBCommandExecuteResult::Failed(it->status().ToString());
  }
  delete it;
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
