// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.
#include <iostream>
#include <string>
#include <vector>
#include <set>
#include "tendisplus/commands/command.h"
#include "tendisplus/commands/release.h"
#include "tendisplus/commands/version.h"
#include "tendisplus/utils/param_manager.h"
#include "tendisplus/utils/base64.h"
#include "tendisplus/storage/kvstore.h"
#include "tendisplus/storage/varint.h"
#include "tendisplus/storage/record.h"

namespace tendisplus {

// TODO(takenliu) print error to stderr or logfile?
class BinlogScanner {
 public:
  enum TOOL_MODE { TEXT_SHOW = 0, BASE64_SHOW, TEXT_SHOW_SCOPE, AOF_SHOW };

  void init(const tendisplus::ParamManager& pm) {
    _logfile = pm.getString("logfile");
    _startDatetime = pm.getUint64("start-datetime", _startDatetime);
    _endDatetime = pm.getUint64("end-datetime", _endDatetime);
    _startPosition = pm.getUint64("start-position", _startPosition);
    _endPosition = pm.getUint64("end-position", _endPosition);
    _mode = TOOL_MODE::TEXT_SHOW;
    if (pm.getString("mode") == "base64") {
      _mode = TOOL_MODE::BASE64_SHOW;
    } else if (pm.getString("mode") == "scope") {
      _mode = TOOL_MODE::TEXT_SHOW_SCOPE;
    } else if (pm.getString("mode") == "aof") {
      _mode = TOOL_MODE ::AOF_SHOW;
    }
    _matchKey = pm.getString("match-key", "");
    _matchField = pm.getString("match-field", "");
    if ((_matchKey != "" || _matchField != "")
        && _mode != TEXT_SHOW) {
      std::cout << "param error, match-key and match-filed need mode=text."
                << std::endl;
      exit(-1);
    }
    std::string keyList = pm.getString("match-key-list", "");
    if (keyList != "") {
      auto keys = stringSplit(keyList, ",");
      for (auto key : keys) {
        _matchKeySet.insert(key);
      }
    }
    std::string detail = pm.getString("detail", "");
    if (detail == "true") {
      _detail = true;
    } else if (detail == "false") {
      _detail = false;
    }
  }

  bool isFiltered(const ReplLogKeyV2& logkey, const ReplLogValueV2& logValue) {
    if (logkey.getBinlogId() < _startPosition ||
        logkey.getBinlogId() > _endPosition) {
      return true;
    }
    if (logValue.getTimestamp() < _startDatetime ||
        logValue.getTimestamp() > _endDatetime) {
      return true;
    }
    return false;
  }

  std::string scanLogEntries(const Expected<ReplLogKeyV2>& logkey,
                                Expected<ReplLogValueV2>& logValue,
                                uint32_t storeId) {
    std::stringstream binlogInfo;
    binlogInfo << "ts:" << logValue.value().getTimestamp()
      << " tsv:" << epochToDatetimeInOneStr(
      logValue.value().getTimestamp() / 1000)
      << " cmdstr:" << logValue.value().getCmd();
    if (_detail) {
      binlogInfo << " storeid:" << storeId
      << " binlogid:" << logkey.value().getBinlogId()
      << " txnid:" << logValue.value().getTxnId()
      << " chunkid:" << logValue.value().getChunkId();
    }
    binlogInfo << std::endl;

    if (logValue.value().getChunkId() == Transaction::CHUNKID_FLUSH) {
      std::cout << binlogInfo.str();
      std::cout << "  op:"
        << "FLUSH"
        << " cmd:" << logValue.value().getCmd() << std::endl;
      return "";
    } else if (logValue.value().getChunkId() ==
      Transaction::CHUNKID_MIGRATE) {
      std::cout << binlogInfo.str();
      std::cout << "  op:"
        << "MIGRATE"
        << " cmd:" << logValue.value().getCmd() << std::endl;
      return "";
    }

    bool needOutputBinlogInfo = true;
    if (_matchKey == "" && _matchField == "" && _matchKeySet.empty()) {
      std::cout << binlogInfo.str();
      needOutputBinlogInfo = false;
    }
    size_t offset = logValue.value().getHdrSize();
    auto data = logValue.value().getData();
    size_t dataSize = logValue.value().getDataSize();
    while (offset < dataSize) {
      size_t size = 0;
      auto entry = ReplLogValueEntryV2::decode(
        (const char*)data + offset, dataSize - offset, &size);
      if (!entry.ok()) {
        return "ReplLogValueEntryV2::decode failed";
      }
      offset += size;

      Expected<RecordKey> opkey =
        RecordKey::decode(entry.value().getOpKey());
      if (!opkey.ok()) {
        std::cerr << "decode opkey failed, err:" << opkey.status().toString()
          << std::endl;
        return "RecordKey::decode failed.";
      }
      auto pkey = opkey.value().getPrimaryKey();
      auto skey = opkey.value().getSecondaryKey();
      if (_matchKey != "" && _matchKey != pkey) {
        continue;
      }
      if (_matchField != "" && _matchField != skey) {
        continue;
      }
      if (!_matchKeySet.empty() && !_matchKeySet.count(pkey)) {
        continue;
      }
      if (needOutputBinlogInfo) {
        std::cout << binlogInfo.str();
        needOutputBinlogInfo = false;
      }
      if (entry.value().getOp() == ReplOp::REPL_OP_DEL) {
        std::cout << "  op:" << entry.value().getOpStr()
          << " key:" << pkey
          << " field:" << skey
          << std::endl;
      } else {
        Expected<RecordValue> opvalue =
          RecordValue::decode(entry.value().getOpValue());
        if (!opvalue.ok()) {
          std::cerr << "decode opvalue failed, err:"
            << opvalue.status().toString() << std::endl;
          return "RecordValue::decode failed.";
        }
        std::cout << "  op:" << entry.value().getOpStr()
          << " key:" << pkey
          << " field:" << skey
          << " opvalue:" << opvalue.value().getValue()
          << std::endl;
      }
    }
    return "";
  }

  std::string process(const std::string& key,
                      const std::string& value,
                      uint32_t storeId) {
    Expected<ReplLogKeyV2> logkey = ReplLogKeyV2::decode(key);
    if (!logkey.ok()) {
      return "decode logkey failed";
    }

    Expected<ReplLogValueV2> logValue = ReplLogValueV2::decode(value);
    if (!logValue.ok()) {
      return "decode logvalue failed";
    }

    if (isFiltered(logkey.value(), logValue.value())) {
      return "";
    }

    if (_mode == TOOL_MODE::TEXT_SHOW) {
      return scanLogEntries(logkey, logValue, storeId);
    } else if (_mode == TOOL_MODE::BASE64_SHOW) {
      std::string baseKey =
        Base64::Encode((unsigned char*)key.c_str(), key.size());
      std::string baseValue =
        Base64::Encode((unsigned char*)value.c_str(), value.size());
      std::cout << "restorebinlogv2 " << storeId << " " << baseKey << " "
                << baseValue << std::endl;
    } else if (_mode == TOOL_MODE::TEXT_SHOW_SCOPE) {
      if (_firstbinlogid == UINT64_MAX) {
        _firstbinlogid = logkey.value().getBinlogId();
        _firstbinlogtime = logValue.value().getTimestamp();
      }
      _lastbinlogid = logkey.value().getBinlogId();
      _lastbinlogtime = logValue.value().getTimestamp();
    } else if (_mode == TOOL_MODE::AOF_SHOW) {
      std::stringstream ss;
      std::string cmdStr = logValue.value().getCmd();

      std::cout << "aof:" << std::endl << cmdStr << std::endl;
    }

    return "";
  }

  Expected<std::string> scan() {
    FILE* pf = fopen(_logfile.c_str(), "r");
    if (pf == NULL) {
      return {ErrorCodes::ERR_INTERNAL, "fopen failed"};
    }
    const uint32_t kBuffLen = 4096;
    char buff[kBuffLen];

    int ret = fread(buff, BINLOG_HEADER_V2_LEN, 1, pf);
    if (ret != 1 || strstr(buff, BINLOG_HEADER_V2) != buff) {
      fclose(pf);
      return {ErrorCodes::ERR_INTERNAL, "read head failed"};
    }
    buff[BINLOG_HEADER_V2_LEN] = '\0';
    uint32_t storeId =
      be32toh(*reinterpret_cast<uint32_t*>(buff + strlen(BINLOG_HEADER_V2)));

    while (!feof(pf)) {
      // keylen
      uint32_t keylen = 0;
      ret = fread(buff, sizeof(uint32_t), 1, pf);
      if (ret != 1) {
        if (feof(pf)) {
          fclose(pf);
          return {ErrorCodes::ERR_OK, ""};
        }
        fclose(pf);
        return {ErrorCodes::ERR_INTERNAL, "read keylen failed"};
      }
      buff[sizeof(uint32_t)] = '\0';
      keylen = int32Decode(buff);

      // key
      std::string key;
      key.resize(keylen);
      ret = fread(const_cast<char*>(key.c_str()), keylen, 1, pf);
      if (ret != 1) {
        fclose(pf);
        return {ErrorCodes::ERR_INTERNAL, "read key failed"};
      }

      // valuelen
      uint32_t valuelen = 0;
      ret = fread(buff, sizeof(uint32_t), 1, pf);
      if (ret != 1) {
        fclose(pf);
        return {ErrorCodes::ERR_INTERNAL, "read valuelen failed"};
      }
      buff[sizeof(uint32_t)] = '\0';
      valuelen = int32Decode(buff);

      // value
      std::string value;
      value.resize(valuelen);
      ret = fread(const_cast<char*>(value.c_str()), valuelen, 1, pf);
      if (ret != 1) {
        fclose(pf);
        return {ErrorCodes::ERR_INTERNAL, "read value failed"};
      }

      auto retStr = process(key, value, storeId);
      if (!retStr.empty()) {
        return retStr;
      }
    }

    fclose(pf);
    return {ErrorCodes::ERR_OK, ""};
  }

  Expected<std::string> run() {
    auto e = scan();
    if (_mode == TOOL_MODE::TEXT_SHOW_SCOPE) {
      std::cout << "firstbinlogid:" << _firstbinlogid << std::endl;
      std::cout << "lastbinlogid:" << _lastbinlogid << std::endl;
      std::cout << "firstbinlogtime:" << _firstbinlogtime << std::endl;
      std::cout << "lastbinlogtime:" << _lastbinlogtime << std::endl;
    }

    if (!e.ok()) {
      return {e.status().code(),
              e.status().getErrmsg() + ". file name: " + _logfile};
    }

    return e;
  }

 private:
  std::string _logfile;
  TOOL_MODE _mode;
  std::string _matchKey;
  std::string _matchField;
  std::set<std::string> _matchKeySet;
  bool _detail = false;
  uint64_t _startDatetime = 0;
  uint64_t _endDatetime = UINT64_MAX;
  uint64_t _startPosition = 0;
  uint64_t _endPosition = UINT64_MAX;

  uint64_t _firstbinlogid = UINT64_MAX;
  uint64_t _lastbinlogid = UINT64_MAX;
  uint64_t _firstbinlogtime = UINT64_MAX;
  uint64_t _lastbinlogtime = UINT64_MAX;
};

}  // namespace tendisplus

void usage() {
  std::cerr << "binlog_tool --logfile=binlog.log --mode=text|base64|scope|aof"
            << " --start-datetime=1111 --end-datetime=22222"
            << " --start-position=333333 --end-position=55555"
            << " [--match-key=testKey --match-filed=testFiled]"
            << " [--match-key-list=testKey1,testKey2...]"
            << " [--detail=true/false]"
            << std::endl;
}

int main(int argc, char** argv) {
  if (argc < 2) {
    usage();
    return 0;
  }
  if (strcmp(argv[1], "-v") == 0) {
    std::cout << "Tendisplus v=" << getTendisPlusVersion()
              << " sha=" << TENDISPLUS_GIT_SHA1
              << " dirty=" << TENDISPLUS_GIT_DIRTY
              << " build=" << TENDISPLUS_BUILD_ID << std::endl;
    return 0;
  }
  tendisplus::ParamManager pm;
  pm.init(argc, argv);

  tendisplus::BinlogScanner bs;
  bs.init(pm);
  auto e = bs.run();
  if (e.ok()) {
    return 0;
  }

  std::cerr << e.status().getErrmsg() << std::endl;
  return 1;
}
