// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.
#include <iostream>
#include <string>
#include <vector>
#include "tendisplus/commands/command.h"
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
      std::cout << "storeid:" << storeId
                << " binlogid:" << logkey.value().getBinlogId()
                << " txnid:" << logValue.value().getTxnId()
                << " chunkid:" << logValue.value().getChunkId()
                << " ts:" << logValue.value().getTimestamp()
                << " cmdstr:" << logValue.value().getCmd() << std::endl;

      if (logValue.value().getChunkId() == Transaction::CHUNKID_FLUSH) {
        std::cout << "  op:"
                  << "FLUSH"
                  << " cmd:" << logValue.value().getCmd() << std::endl;
      } else if (logValue.value().getChunkId() ==
                 Transaction::CHUNKID_MIGRATE) {
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
        if (entry.value().getOp() == ReplOp::REPL_OP_DEL) {
          std::cout << "  op:" << (uint32_t)entry.value().getOp()
            << " fkey:" << opkey.value().getPrimaryKey()
            << " skey:" << opkey.value().getSecondaryKey()
            << std::endl;
        } else {
          Expected<RecordValue> opvalue =
              RecordValue::decode(entry.value().getOpValue());
          if (!opvalue.ok()) {
            std::cerr << "decode opvalue failed, err:"
              << opvalue.status().toString() << std::endl;
            return "RecordValue::decode failed.";
          }
          std::cout << "  op:" << (uint32_t)entry.value().getOp()
            << " fkey:" << opkey.value().getPrimaryKey()
            << " skey:" << opkey.value().getSecondaryKey()
            << " opvalue:" << opvalue.value().getValue()
            << std::endl;
        }
      }
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
  std::cerr << "binlog_tool --logfile=binlog.log --mode=text|base64|scope"
            << " --start-datetime=1111 --end-datetime=22222"
            << " --start-position=333333 --end-position=55555"
            << /*" --keys=1,2,4,5,6,7,8,9" <<*/ std::endl;
}

int main(int argc, char** argv) {
  if (argc < 2) {
    usage();
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
