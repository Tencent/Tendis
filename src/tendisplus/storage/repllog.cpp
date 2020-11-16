// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <type_traits>
#include <utility>
#include <memory>
#include <vector>
#include <limits>
#include "glog/logging.h"
#include "tendisplus/storage/varint.h"
#include "tendisplus/storage/record.h"
#include "tendisplus/utils/status.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/invariant.h"

namespace tendisplus {

ReplLogKeyV2::ReplLogKeyV2() : _binlogId(0), _version("v2") {}

ReplLogKeyV2::ReplLogKeyV2(ReplLogKeyV2&& o) : _binlogId(o._binlogId) {
  o._binlogId = 0;
}

ReplLogKeyV2::ReplLogKeyV2(uint64_t binlogid)
  : _binlogId(binlogid), _version("v2") {}

Expected<ReplLogKeyV2> ReplLogKeyV2::decode(const RecordKey& rk) {
  auto type = rk.getRecordType();
  if (type != RecordType::RT_BINLOG) {
    return {ErrorCodes::ERR_DECODE,
            "ReplLogKeyV2::decode:it is not a valid binlog type " +
              std::string(1, rt2Char(type))};  // NOLINT
  }

  if (rk.getChunkId() != ReplLogKeyV2::CHUNKID ||
      rk.getDbId() != ReplLogKeyV2::DBID) {
    return {ErrorCodes::ERR_DECODE,
            "ReplLogKeyV2::decode:chunkid or dbid is invalied"};  // NOLINT
  }

  const std::string& key = rk.getPrimaryKey();
  if (key.size() != sizeof(_binlogId)) {
    return {ErrorCodes::ERR_DECODE, "invalid keylen"};
  }
  uint64_t binlogId = int64Decode(key.c_str());

  if (rk.getSecondaryKey().size() != 0) {
    return {ErrorCodes::ERR_DECODE, "invalid seccondkeylen"};
  }

  return ReplLogKeyV2(binlogId);
}


Expected<ReplLogKeyV2> ReplLogKeyV2::decode(const std::string& rawKey) {
  Expected<RecordKey> rk = RecordKey::decode(rawKey);
  if (!rk.ok()) {
    return rk.status();
  }
  return decode(rk.value());
}

std::string ReplLogKeyV2::encode() const {
  std::string key;
  key.resize(sizeof(_binlogId));
  int64Encode(&key[0], _binlogId);

  // NOTE(vinchen): the subkey of ReplLogKeyV2 is empty.
  RecordKey tmpRk(ReplLogKeyV2::CHUNKID,
                  ReplLogKeyV2::DBID,
                  RecordType::RT_BINLOG,
                  std::move(key),
                  "");
  return tmpRk.encode();
}

// std::string& ReplLogKeyV2::updateBinlogId(std::string& encodeStr,
//        uint64_t newBinlogId) {
//    INVARIANT_D(ReplLogKeyV2::decode(encodeStr).ok());
//    size_t offset = RecordKey::getHdrSize();
//    for (size_t i = 0; i < sizeof(_binlogId); i++) {
//        encodeStr[offset + i] = static_cast<char>((newBinlogId >>
//        ((sizeof(_binlogId) - i - 1) * 8)) & 0xff);   // NOLINT
//    }
//    return encodeStr;
//}

bool ReplLogKeyV2::operator==(const ReplLogKeyV2& o) const {
  return _binlogId == o._binlogId;
}

ReplLogKeyV2& ReplLogKeyV2::operator=(const ReplLogKeyV2& o) {
  if (this == &o) {
    return *this;
  }
  _binlogId = o._binlogId;
  return *this;
}

ReplLogValueEntryV2::ReplLogValueEntryV2()
  : _op(ReplOp::REPL_OP_NONE), _timestamp(0), _key(""), _val("") {}

ReplLogValueEntryV2::ReplLogValueEntryV2(ReplLogValueEntryV2&& o)
  : _op(o._op),
    _timestamp(o._timestamp),
    _key(std::move(o._key)),
    _val(std::move(o._val)) {
  o._op = ReplOp::REPL_OP_NONE;
  o._timestamp = 0;
}

ReplLogValueEntryV2::ReplLogValueEntryV2(ReplOp op,
                                         uint64_t ts,
                                         const std::string& key,
                                         const std::string& val)
  : _op(op), _timestamp(ts), _key(key), _val(val) {}

ReplLogValueEntryV2::ReplLogValueEntryV2(ReplOp op,
                                         uint64_t ts,
                                         std::string&& key,
                                         std::string&& val)
  : _op(op), _timestamp(ts), _key(std::move(key)), _val(std::move(val)) {}

ReplLogValueEntryV2& ReplLogValueEntryV2::operator=(ReplLogValueEntryV2&& o) {
  if (&o == this) {
    return *this;
  }

  _op = o._op;
  _timestamp = o._timestamp;
  _val = std::move(o._val);
  _key = std::move(o._key);

  o._op = ReplOp::REPL_OP_NONE;
  o._timestamp = 0;

  return *this;
}

Expected<ReplLogValueEntryV2> ReplLogValueEntryV2::decode(const char* rawVal,
                                                          size_t maxSize,
                                                          size_t* decodeSize) {
  const uint8_t* valCstr = reinterpret_cast<const uint8_t*>(rawVal);
  if (maxSize <= sizeof(_op)) {
    return {ErrorCodes::ERR_DECODE, "invalid replvalueentry len"};
  }
  size_t offset = 0;

  // op
  uint8_t op = valCstr[0];
  offset += sizeof(uint8_t);

  // timestamp
  auto expt = varintDecodeFwd(valCstr + offset, maxSize - offset);
  if (!expt.ok()) {
    return expt.status();
  }
  offset += expt.value().second;
  uint64_t timestamp = expt.value().first;

  // key
  auto eKey = lenStrDecode(rawVal + offset, maxSize - offset);
  if (!eKey.ok()) {
    return {ErrorCodes::ERR_DECODE, "invalid replvalueentry len"};
  }
  offset += eKey.value().second;

  // val
  auto eVal = lenStrDecode(rawVal + offset, maxSize - offset);
  if (!eVal.ok()) {
    return {ErrorCodes::ERR_DECODE, "invalid replvalueentry len"};
  }
  offset += eVal.value().second;

  // output
  *decodeSize = offset;

  return ReplLogValueEntryV2(static_cast<ReplOp>(op),
                             timestamp,
                             std::move(eKey.value().first),
                             std::move(eVal.value().first));
}

size_t ReplLogValueEntryV2::encodeSize() const {
  return sizeof(uint8_t) + varintEncodeSize(_timestamp) +
    lenStrEncodeSize(_key) + lenStrEncodeSize(_val);
}

size_t ReplLogValueEntryV2::encode(uint8_t* dest, size_t destSize) const {
  INVARIANT_D(destSize >= encodeSize());

  size_t offset = 0;
  // op
  dest[offset++] = static_cast<char>(_op);

  // ts
  offset += varintEncodeBuf(dest + offset, destSize - offset, _timestamp);

  // key
  offset += lenStrEncode(
    reinterpret_cast<char*>(dest) + offset, destSize - offset, _key);

  // val
  offset += lenStrEncode(
    reinterpret_cast<char*>(dest) + offset, destSize - offset, _val);

  INVARIANT_D(offset == encodeSize());

  return offset;
}

std::string ReplLogValueEntryV2::encode() const {
  std::string val;
  val.resize(encodeSize());
  uint8_t* desc =
    const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(val.c_str()));
  size_t offset = encode(desc, val.size());
  INVARIANT_D(offset == encodeSize());

  return val;
}

bool ReplLogValueEntryV2::operator==(const ReplLogValueEntryV2& o) const {
  return _op == o._op && _timestamp == o._timestamp && _key == o._key &&
    _val == o._val;
}

ReplLogValueV2::ReplLogValueV2()
  : _chunkId(0),
    _flag(ReplFlag::REPL_GROUP_MID),
    _txnId(Transaction::TXNID_UNINITED),
    _timestamp(0),
    _versionEp(0),
    _cmdStr(""),
    _data(nullptr),
    _dataSize(0) {}

ReplLogValueV2::ReplLogValueV2(ReplLogValueV2&& o)
  : _chunkId(o._chunkId),
    _flag(o._flag),
    _txnId(o._txnId),
    _timestamp(o._timestamp),
    _versionEp(o._versionEp),
    _cmdStr(std::move(o._cmdStr)),
    _data(o._data),
    _dataSize(o._dataSize) {
  o._chunkId = 0;
  o._flag = ReplFlag::REPL_GROUP_MID;
  o._txnId = Transaction::TXNID_UNINITED;
  o._timestamp = 0;
  o._versionEp = 0;
  o._cmdStr = "";
  o._data = nullptr;
  o._dataSize = 0;
}

ReplLogValueV2::ReplLogValueV2(uint32_t chunkId,
                               ReplFlag flag,
                               uint64_t txnid,
                               uint64_t timestamp,
                               uint64_t versionEp,
                               const std::string& cmd,
                               const uint8_t* data,
                               size_t dataSize)
  : _chunkId(chunkId),
    _flag(flag),
    _txnId(txnid),
    _timestamp(timestamp),
    _versionEp(versionEp),
    _cmdStr(cmd),
    _data(data),
    _dataSize(dataSize) {}

size_t ReplLogValueV2::fixedHeaderSize() {
  return ReplLogValueV2::FIXED_HEADER_SIZE;
}

size_t ReplLogValueV2::getHdrSize() const {
  return ReplLogValueV2::fixedHeaderSize() + varintEncodeSize(_cmdStr.size()) +
    _cmdStr.size();
}

std::string ReplLogValueV2::encodeHdr() const {
  std::string header;
  size_t hdrSize = getHdrSize();
  header.resize(hdrSize);

  size_t offset = 0;

  // CHUNKID
  auto size = int32Encode(&header[offset], _chunkId);
  offset += size;

  // FLAG
  size = int16Encode(&header[offset], static_cast<uint16_t>(_flag));
  offset += size;

  // TXNID
  size = int64Encode(&header[offset], _txnId);
  offset += size;

  // timestamp
  size = int64Encode(&header[offset], _timestamp);
  offset += size;

  // versionEP
  size = int64Encode(&header[offset], _versionEp);
  offset += size;

  INVARIANT_D(offset == fixedHeaderSize());

  size = lenStrEncode(&header[offset], hdrSize - offset, _cmdStr);
  offset += size;
  INVARIANT_D(offset == hdrSize);

  return header;
}

std::string ReplLogValueV2::encode(
  const std::vector<ReplLogValueEntryV2>& vec) const {
  std::string val = encodeHdr();
  size_t offset = val.size();

  size_t allocSize = offset;
  for (const auto& v : vec) {
    allocSize += v.encodeSize();
  }

  val.resize(allocSize);

  for (const auto& v : vec) {
    uint8_t* desc =
      const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(val.c_str())) +
      offset;
    size_t len = v.encode(desc, allocSize - offset);
    INVARIANT_D(len > 0);
    offset += len;
  }

  INVARIANT_D(offset == allocSize);

  RecordValue tmpRv(std::move(val), RecordType::RT_BINLOG, -1);

  return tmpRv.encode();
}

Expected<std::vector<ReplLogValueEntryV2>> ReplLogValueV2::getLogList() const {
  std::vector<ReplLogValueEntryV2> result;

  size_t curSize = FIXED_HEADER_SIZE;
  while (curSize < _dataSize) {
    size_t oneSize = 0;
    Expected<ReplLogValueEntryV2> logEntry = ReplLogValueEntryV2::decode(
      (const char*)_data + curSize, _dataSize - curSize, &oneSize);
    if (!logEntry.ok()) {
      return {ErrorCodes::ERR_DECODE, "decode error."};
    }
    curSize += oneSize;
    result.push_back(logEntry.value());
  }

  return result;
}

Expected<ReplLogValueV2> ReplLogValueV2::decode(const std::string& s) {
  auto type = RecordValue::decodeType(s.c_str(), s.size());
  if (type != RecordType::RT_BINLOG) {
    return {ErrorCodes::ERR_DECODE,
            "ReplLogValueV2::decode: it is not a valid binlog type" +
              std::string(1, rt2Char(type))};  // NOLINT
  }

  auto hdrSize = RecordValue::decodeHdrSizeNoMeta(s);
  if (!hdrSize.ok()) {
    return hdrSize.status();
  }

  return decode(s.c_str() + hdrSize.value(), s.size() - hdrSize.value());
}
Expected<ReplLogValueV2> ReplLogValueV2::decode(const char* str, size_t size) {
  uint32_t chunkid = 0;
  uint64_t txnid = 0;
  uint64_t timestamp = 0;
  uint64_t versionEp = 0;

  if (size < fixedHeaderSize()) {
    return {ErrorCodes::ERR_DECODE,
            "ReplLogValueV2::decode() error, too small"};
  }

  // chunkid
  chunkid = int32Decode(str + CHUNKID_OFFSET);
  // flag
  auto flag = static_cast<ReplFlag>(int16Decode(str + FLAG_OFFSET));
  // txnid
  txnid = int64Decode(str + TXNID_OFFSET);
  // timestamp
  timestamp = int64Decode(str + TIMESTAMP_OFFSET);
  // versionEp
  versionEp = int64Decode(str + VERSIONEP_OFFSET);

  auto eCmd = lenStrDecode(str + FIXED_HEADER_SIZE, size - FIXED_HEADER_SIZE);
  if (!eCmd.ok()) {
    return {ErrorCodes::ERR_DECODE,
            "ReplLogValueV2::decode() error:" + eCmd.status().toString()};
  }

  auto keyCstr = reinterpret_cast<const uint8_t*>(str);

  return ReplLogValueV2(chunkid,
                        flag,
                        txnid,
                        timestamp,
                        versionEp,
                        eCmd.value().first,
                        keyCstr,
                        size);
}

bool ReplLogValueV2::isEqualHdr(const ReplLogValueV2& o) const {
  return _chunkId == o._chunkId && _flag == o._flag && _txnId == o._txnId &&
    _timestamp == o._timestamp && _versionEp == o._versionEp &&
    _cmdStr == o._cmdStr;
}

// std::string& ReplLogValueV2::updateTxnId(std::string& encodeStr,
//                uint64_t newTxnId) {
//    INVARIANT_D(ReplLogValueV2::decode(encodeStr).ok());
//    auto s = RecordValue::decodeHdrSize(encodeStr);
//    INVARIANT_D(s.ok());
//
//    // header + chunkid + flag;
//    size_t offset = s.value() + sizeof(uint32_t) + sizeof(uint16_t);
//
//    for (size_t i = 0; i < sizeof(_txnId); i++) {
//        encodeStr[offset + i] = static_cast<char>((newTxnId >>
//        ((sizeof(_txnId) - i - 1) * 8)) & 0xff);   // NOLINT
//    }
//    return encodeStr;
//
//}

ReplLogRawV2::ReplLogRawV2(const std::string& key, const std::string& value)
  : _key(key), _val(value) {}

ReplLogRawV2::ReplLogRawV2(const Record& record)
  : _key(record.getRecordKey().encode()),
    _val(record.getRecordValue().encode()) {}

ReplLogRawV2::ReplLogRawV2(std::string&& key, std::string&& value)
  : _key(std::move(key)), _val(std::move(value)) {}

ReplLogRawV2::ReplLogRawV2(ReplLogRawV2&& o)
  : _key(std::move(o._key)), _val(std::move(o._val)) {}

uint64_t ReplLogRawV2::getBinlogId() {
  if (_key.size() < RecordKey::minSize()) {
    INVARIANT_D(0);
    return Transaction::TXNID_UNINITED;
  }

  return int64Decode(_key.c_str() + ReplLogKeyV2::BINLOG_OFFSET);
}

uint64_t ReplLogRawV2::getVersionEp() {
  if (_val.size() <
      RecordValue::minSize() + ReplLogValueV2::fixedHeaderSize()) {
    INVARIANT_D(0);
    return (uint64_t)-1;
  }

  auto rvHdrSize = RecordValue::decodeHdrSizeNoMeta(_val);
  if (!rvHdrSize.ok()) {
    INVARIANT_D(0);
    return (uint64_t)-1;
  }

  return int64Decode(_val.c_str() + rvHdrSize.value() +
                     ReplLogValueV2::VERSIONEP_OFFSET);
}

uint64_t ReplLogRawV2::getTimestamp() {
  if (_val.size() <
      RecordValue::minSize() + ReplLogValueV2::fixedHeaderSize()) {
    INVARIANT_D(0);
    return (uint64_t)0;
  }
  auto rvHdrSize = RecordValue::decodeHdrSizeNoMeta(_val);
  if (!rvHdrSize.ok()) {
    INVARIANT_D(0);
    return (uint64_t)0;
  }

  return int64Decode(_val.c_str() + rvHdrSize.value() +
                     ReplLogValueV2::TIMESTAMP_OFFSET);
}

uint32_t ReplLogRawV2::getChunkId() {
  if (_val.size() <
      RecordValue::minSize() + ReplLogValueV2::fixedHeaderSize()) {
    INVARIANT_D(0);
    return (uint32_t)Transaction::CHUNKID_UNINITED;
  }
  auto rvHdrSize = RecordValue::decodeHdrSizeNoMeta(_val);
  if (!rvHdrSize.ok()) {
    INVARIANT_D(0);
    return (uint32_t)Transaction::CHUNKID_UNINITED;
  }

  return int32Decode(_val.c_str() + rvHdrSize.value() +
                     ReplLogValueV2::CHUNKID_OFFSET);
}

size_t Binlog::writeHeader(std::stringstream& ss) {
  std::string s;
  s.resize(Binlog::HEADERSIZE);
  s[0] = (uint8_t)Binlog::VERSION;

  INVARIANT_D(Binlog::HEADERSIZE == 1);

  ss << s;
  return Binlog::HEADERSIZE;
}

size_t Binlog::decodeHeader(const char* str, size_t size) {
  INVARIANT_D(str[0] == (uint8_t)Binlog::VERSION);
  if (str[0] != Binlog::VERSION) {
    return std::numeric_limits<size_t>::max();
  }

  return Binlog::HEADERSIZE;
}

size_t Binlog::writeRepllogRaw(std::stringstream& ss,
                               const ReplLogRawV2& repllog) {
  size_t size = 0;

  size += lenStrEncode(ss, repllog.getReplLogKey());
  size += lenStrEncode(ss, repllog.getReplLogValue());

  return size;
}

BinlogWriter::BinlogWriter(size_t maxSize, uint32_t maxCount)
  : _curSize(0),
    _maxSize(maxSize),
    _curCnt(0),
    _maxCnt(maxCount),
    _flag(BinlogFlag::NORMAL) {
  _curSize += Binlog::writeHeader(_ss);
}

bool BinlogWriter::writeRepllogRaw(const ReplLogRawV2& repllog) {
  _curSize += Binlog::writeRepllogRaw(_ss, repllog);
  _curCnt++;
  if (_curSize >= _maxSize || _curCnt >= _maxCnt) {
    return true;
  }

  return false;
}

void BinlogWriter::resetWriter() {
  _curSize = 0;
  _curCnt = 0;
  _ss.str("");
  _curSize += Binlog::writeHeader(_ss);
  _flag = BinlogFlag::NORMAL;
}

bool BinlogWriter::writerFull() {
  return (_curSize >= _maxSize || _curCnt >= _maxCnt);
}

BinlogReader::BinlogReader(const std::string& s) : _pos(0), _val(s) {}

Expected<ReplLogRawV2> BinlogReader::next() {
  if (_pos == 0) {
    // first read
    auto size = Binlog::decodeHeader(_val.data(), _val.size());
    if (size > _val.size()) {
      return {ErrorCodes::ERR_DECODE, "invalid binlog"};
    }

    _pos = size;
  }

  if (_pos == _val.size()) {
    return {ErrorCodes::ERR_EXHAUST, ""};
  }
  INVARIANT(_pos < _val.size());

  auto ptr = _val.data();
  auto totalSize = _val.size();
  auto offset = _pos;

  auto eKey = lenStrDecode(ptr + offset, totalSize - offset);
  if (!eKey.ok()) {
    return {ErrorCodes::ERR_DECODE,
            "invalid binlog format" + eKey.status().toString()};
  }
  offset += eKey.value().second;

  auto eValue = lenStrDecode(ptr + offset, totalSize - offset);
  if (!eValue.ok()) {
    return {ErrorCodes::ERR_DECODE,
            "invalid binlog format" + eValue.status().toString()};
  }
  offset += eValue.value().second;

  _pos = offset;
  return ReplLogRawV2(eKey.value().first, eValue.value().first);
}

Expected<ReplLogV2> BinlogReader::nextV2() {
  if (_pos == 0) {
    // first read
    auto size = Binlog::decodeHeader(_val.data(), _val.size());
    if (size > _val.size()) {
      return {ErrorCodes::ERR_DECODE, "invalid binlog"};
    }

    _pos = size;
  }

  if (_pos == _val.size()) {
    return {ErrorCodes::ERR_EXHAUST, ""};
  }
  INVARIANT(_pos < _val.size());

  auto ptr = _val.data();
  auto totalSize = _val.size();
  auto offset = _pos;

  auto eKey = lenStrDecode(ptr + offset, totalSize - offset);
  if (!eKey.ok()) {
    return {ErrorCodes::ERR_DECODE,
            "invalid binlog format" + eKey.status().toString()};
  }
  offset += eKey.value().second;

  auto eValue = lenStrDecode(ptr + offset, totalSize - offset);
  if (!eValue.ok()) {
    return {ErrorCodes::ERR_DECODE,
            "invalid binlog format" + eValue.status().toString()};
  }
  offset += eValue.value().second;

  _pos = offset;

  return ReplLogV2::decode(eKey.value().first, eValue.value().first);
}

ReplLogV2::ReplLogV2(ReplLogKeyV2&& key,
                     ReplLogValueV2&& value,
                     std::vector<ReplLogValueEntryV2>&& entrys)
  : _key(std::move(key)), _val(std::move(value)), _entrys(std::move(entrys)) {}

ReplLogV2::ReplLogV2(ReplLogV2&& o)
  : _key(std::move(o._key)),
    _val(std::move(o._val)),
    _entrys(std::move(o._entrys)) {}

Expected<ReplLogV2> ReplLogV2::decode(const std::string& key,
                                      const std::string& value) {
  auto k = ReplLogKeyV2::decode(key);
  if (!k.ok()) {
    return k.status();
  }

  auto v = ReplLogValueV2::decode(value);
  if (!v.ok()) {
    return v.status();
  }

  std::vector<ReplLogValueEntryV2> entrys;

  size_t offset = v.value().getHdrSize();
  auto data = v.value().getData();
  size_t dataSize = v.value().getDataSize();
  while (offset < dataSize) {
    size_t size = 0;
    auto entry = ReplLogValueEntryV2::decode(
      (const char*)data + offset, dataSize - offset, &size);
    INVARIANT_D(entry.ok());
    if (!entry.ok()) {
      return entry.status();
    }
    offset += size;

    entrys.emplace_back(std::move(entry.value()));
  }

  if (offset != dataSize) {
    return {ErrorCodes::ERR_DECODE, "invalid repllogvaluev2 value len"};
  }

  return ReplLogV2(
    std::move(k.value()), std::move(v.value()), std::move(entrys));
}

uint64_t ReplLogV2::getTimestamp() const {
  INVARIANT_D(_entrys.size() > 0);
  INVARIANT_D(_val.getTimestamp() == _entrys.rbegin()->getTimestamp());
  return _entrys.rbegin()->getTimestamp();
}

}  // namespace tendisplus
