// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include "rocksdb/ldb_tool.h"
#include "rocksdb/utilities/ldb_cmd.h"
#include "rocksdb/utilities/ttl/db_ttl_impl.h"
#include "tendisplus/storage/record.h"

using namespace ROCKSDB_NAMESPACE;
using namespace tendisplus;

class TScanCommand : public LDBCommand {
 public:
  static std::string Name() {
    return "tscan";
  }

  TScanCommand(const std::vector<std::string>& params,
               const std::map<std::string, std::string>& options,
               const std::vector<std::string>& flags);

  void DoCommand() override;

  static void Help(std::string& ret);

 private:
  void printLog(const std::string& value);

 private:
  std::string start_key_;
  std::string end_key_;
  bool start_key_specified_;
  bool end_key_specified_;
  int max_keys_scanned_;
  bool no_value_;
  bool print_log_;
};

static const std::string ARG_PRINT_LOG = "printlog";

TScanCommand::TScanCommand(const std::vector<std::string>& params,
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
                                    ARG_PRINT_LOG})),
    start_key_specified_(false),
    end_key_specified_(false),
    max_keys_scanned_(-1),
    no_value_(false),
    print_log_(false) {
  auto itr = options.find(ARG_FROM);
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

  vitr = std::find(flags.begin(), flags.end(), ARG_PRINT_LOG);
  if (vitr != flags.end()) {
    print_log_ = true;
  }
}

void TScanCommand::Help(std::string& ret) {
  ret.append(
    "\
usage:\n\
  /path/to/ldb_tendis --db=path --column_family=cf tscan [option]\n\
\n\
  ldb_tendis options:\n\
    --db=path\n\
          rocksdb dir path\n\
    --column_family=cf\n\
          'default' for data, 'binlog_cf' for binlog\n\
  tscan options:\n\
    --max_keys=key_num\n\
          at most key_num key outputed\n\
    --printlog\n\
          print additional binlog content if cf is binlog_cf\n\
\n\
output format:\n\
  type dbid primaryKey ttl valueLen fieldLen\n\
\n\
    type:\n\
      a for normal kv\n\
      H for hash\n\
      S for set\n\
      Z for zset\n\
      L for list\n\
      B for binlog\n\
    dbid:\n\
      redis dbid\n\
      (dbid for binlog is 4294967041)\n\
    primaryKey:\n\
      key name\n\
    ttl:\n\
      expire timestamp in millisecond(0 if not set ttl)\n\
    valueLen:\n\
      value length for normal kv or 0 for other data structs\n\
    fieldLen:\n\
      element number in data structs or 0 for normal key\n\
      binlog size for binlog\n\n\
  example:\n\
    (normal kv: set a b)\n\
      a 0 a 0 1 0\n\
    (list: lpush l1 a)\n\
      L 0 l1 0 0 1\n\
    (list: lpush l1 b)\n\
      L 0 l1 0 0 2\n\
    (list: lpush l1 x)\n\
      L 0 l1 0 0 3\n\n\
    (binlog for [set a b])\n\
      txnid:12 chunkid:15495 ts:1651829068877 cmdstr:set\n\
        op:1 fkey:a skey: opvalue:b\n\
      B 4294967041 1 0 0 72\n\
");
}

static std::string opToString(ReplOp op) {
  switch (op) {
    case ReplOp::REPL_OP_SET:
      return "set";
    case ReplOp::REPL_OP_DEL:
      return "del";
    case ReplOp::REPL_OP_STMT:
      return "stmt";
    case ReplOp::REPL_OP_SPEC:
      return "spec";
    case ReplOp::REPL_OP_DEL_RANGE:
      return "del_range";
    case ReplOp::REPL_OP_DEL_FILES_INCLUDE_END:
      return "del_files_include_end";
    case ReplOp::REPL_OP_DEL_FILES_EXCLUDE_END:
      return "del_files_exclude_end";
    default:
      std::cerr << "INVALID OP:" << static_cast<int>(op) << std::endl;
      assert(0);
      return "";
  }
}

void TScanCommand::printLog(const std::string& value) {
  auto eLogValue = ReplLogValueV2::decode(value);
  if (!eLogValue.ok()) {
    std::cerr << "ReplLogValueV2::decode failed!"
              << eLogValue.status().toString();
    return;
  }
  const auto& logValue = eLogValue.value();

  std::cout << "  txnid:" << logValue.getTxnId()
            << " slot:" << logValue.getChunkId()
            << " ts:" << logValue.getTimestamp() << " cmd:" << logValue.getCmd()
            << std::endl;

  if (logValue.getChunkId() == Transaction::CHUNKID_FLUSH) {
    std::cout << "    op:flush" << std::endl;
    return;
  } else if (logValue.getChunkId() == Transaction::CHUNKID_MIGRATE) {
    std::cout << "    op:migrate" << std::endl;
    return;
  }

  size_t offset = logValue.getHdrSize();
  auto data = logValue.getData();
  size_t dataSize = logValue.getDataSize();
  while (offset < dataSize) {
    size_t size = 0;
    auto eEntry = ReplLogValueEntryV2::decode(
      (const char*)data + offset, dataSize - offset, &size);
    if (!eEntry.ok()) {
      std::cerr << "ReplLogValueEntryV2::decode failed!"
                << eEntry.status().toString();
      return;
    }
    const auto& entry = eEntry.value();
    offset += size;

    auto eRecordKey = RecordKey::decode(entry.getOpKey());
    if (!eRecordKey.ok()) {
      std::cerr << "RecordKey::decode failed!"
                << eRecordKey.status().toString();
      return;
    }
    const auto& recordKey = eRecordKey.value();
    std::string pkey = recordKey.getChunkId() == TTLIndex::CHUNKID
      ? recordKey.getPrimaryKey().substr(13)
      : recordKey.getPrimaryKey();
    if (entry.getOp() == ReplOp::REPL_OP_DEL) {
      std::cout << "    op:" << opToString(entry.getOp()) << " pkey:" << pkey
                << " skey:" << recordKey.getSecondaryKey() << std::endl;
      continue;
    }
    auto eRecordValue = RecordValue::decode(entry.getOpValue());
    if (!eRecordValue.ok()) {
      std::cerr << "RecordValue::decode failed!"
                << eRecordValue.status().toString();
      return;
    }
    const auto& recordValue = eRecordValue.value();
    std::string opvalue =
      recordValue.getRecordType() == RecordType::RT_TTL_INDEX
      ? "ttl_index"
      : recordValue.getValue();
    std::cout << "    op:" << opToString(entry.getOp()) << " pkey:" << pkey
              << " skey:" << recordKey.getSecondaryKey()
              << " opvalue:" << opvalue << std::endl;
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

    std::string formatted_key = it->key().ToString(false);
    std::string formatted_value = it->value().ToString(false);
    auto exptRcd = Record::decode(formatted_key, formatted_value);
    if (!exptRcd.ok()) {
      std::cerr << "record decode fail:" << exptRcd.status().toString();
      continue;
    }

    const auto& record = exptRcd.value();
    const auto& rk = record.getRecordKey();
    const auto& rv = record.getRecordValue();
    auto keyType = rk.getRecordType();
    /* NOTE(wayenchen) ignore if not primary key */
    if (keyType != RecordType::RT_DATA_META &&
        keyType != RecordType::RT_BINLOG) {
      continue;
    }
    // value length of string
    uint64_t vallen = 0;
    // num of value of secondary key
    uint64_t fieldlen = 0;
    // record type
    char type = ' ';

    auto valueType = rv.getRecordType();
    if (valueType == RecordType::RT_KV) {
      vallen = rv.getValue().size();
      type = 'a';
    } else if (valueType == RecordType::RT_HASH_META) {
      Expected<HashMetaValue> eHashMeta = HashMetaValue::decode(rv.getValue());
      if (!eHashMeta.ok()) {
        std::cerr << "hash meta decode fail:" << eHashMeta.status().toString();
        continue;
      }
      fieldlen = eHashMeta.value().getCount();
      type = 'H';
    } else if (valueType == RecordType::RT_SET_META) {
      Expected<SetMetaValue> eSetMeta = SetMetaValue::decode(rv.getValue());
      if (!eSetMeta.ok()) {
        std::cerr << "set meta decode fail:" << eSetMeta.status().toString();
        continue;
      }
      fieldlen = eSetMeta.value().getCount();
      type = 'S';
    } else if (valueType == RecordType::RT_ZSET_META) {
      Expected<ZSlMetaValue> eZsetMeta = ZSlMetaValue::decode(rv.getValue());
      if (!eZsetMeta.ok()) {
        std::cerr << "zset meta decode fail:" << eZsetMeta.status().toString();
        continue;
      }
      /*NOTE(wayenchen) same value as zcard*/
      fieldlen = eZsetMeta.value().getCount() - 1;
      type = 'Z';
    } else if (valueType == RecordType::RT_LIST_META) {
      Expected<ListMetaValue> eListMeta = ListMetaValue::decode(rv.getValue());
      if (!eListMeta.ok()) {
        std::cerr << "list meta decode fail:" << eListMeta.status().toString();
        continue;
      }
      fieldlen = eListMeta.value().getTail() - eListMeta.value().getHead();
      type = 'L';
    } else if (keyType == RecordType::RT_BINLOG) {
      fieldlen = rv.encode().size();
      type = 'B';
    }

    auto pKey = rk.getPrimaryKey();
    if (keyType == RecordType::RT_BINLOG) {
      // binlog pk is binlogid and reversed bit
      auto pt = reinterpret_cast<uint64_t*>(const_cast<char*>(pKey.data()));
      pKey = std::to_string(htobe64(*pt));
    }
    // type dbid PrimaryKey ttl value(kv) subKeysCount(list/set/zset/hash)
    fprintf(stdout,
            "%c %" PRIu32 " %.*s %" PRIu64 " %" PRIu64 " %" PRIu64 "\n",
            type,
            rk.getDbId(),
            static_cast<int>(pKey.size()),
            pKey.data(),
            rv.getTtl(),
            vallen,
            fieldlen);
    if (keyType == RecordType::RT_BINLOG && print_log_) {
      printLog(formatted_value);
    }

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

static void PrintHelp(const LDBOptions& ldb_options,
                      const char* /*exec_name*/,
                      bool to_stderr) {
  std::string ret;
  ret.append("ldb_tendis - Tendisplus Tool");
  ret.append("\n\n");
  TScanCommand::Help(ret);
  fprintf(to_stderr ? stderr : stdout, "%s\n", ret.c_str());
}

int main(int argc, char** argv) {
  const LDBOptions& ldb_options = LDBOptions();
  if (argc <= 2) {
    if (argc <= 1) {
      PrintHelp(ldb_options, argv[0], /*to_stderr*/ true);
      return 1;
    } else if (std::string(argv[1]) == "--version") {
      printf("ldb_tendis from RocksDB %d.%d.%d\n",
             ROCKSDB_MAJOR,
             ROCKSDB_MINOR,
             ROCKSDB_PATCH);
      return 0;
    } else if (std::string(argv[1]) == "--help") {
      PrintHelp(ldb_options, argv[0], /*to_stderr*/ false);
      return 0;
    } else {
      PrintHelp(ldb_options, argv[0], /*to_stderr*/ true);
      return 1;
    }
  }

  Options options = Options();
  const std::vector<ColumnFamilyDescriptor>* column_families = nullptr;
  std::vector<std::string> args;
  for (int i = 1; i < argc; i++) {
    args.push_back(argv[i]);
  }
  auto selectFunc = [&](const LDBCommand::ParsedParams& parsed_params) {
    return parsed_params.cmd == TScanCommand::Name()
      ? new TScanCommand(parsed_params.cmd_params,
                         parsed_params.option_map,
                         parsed_params.flags)
      : nullptr;
  };
  LDBCommand* cmdObj = LDBCommand::InitFromCmdLineArgs(
    args, options, ldb_options, column_families, selectFunc);
  if (cmdObj == nullptr) {
    fprintf(stderr, "Unknown command\n");
    PrintHelp(ldb_options, argv[0], /*to_stderr*/ true);
    return 1;
  }

  if (!cmdObj->ValidateCmdLineOptions()) {
    return 1;
  }

  cmdObj->Run();
  LDBCommandExecuteResult ret = cmdObj->GetExecuteState();
  if (!ret.ToString().empty()) {
    fprintf(stderr, "%s\n", ret.ToString().c_str());
  }
  delete cmdObj;

  return ret.IsFailed() ? 1 : 0;
}
