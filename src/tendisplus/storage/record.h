// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_STORAGE_RECORD_H_
#define SRC_TENDISPLUS_STORAGE_RECORD_H_

#include <string>
#include <utility>
#include <memory>
#include <vector>
#include <limits>
#include <sstream>
#include "tendisplus/utils/status.h"
#include "tendisplus/storage/kvstore.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/invariant.h"

namespace tendisplus {

const uint32_t VERSIONMETA_CHUNKID = 0XFFFE0000U;
const uint32_t TTLINDEX_CHUNKID = 0XFFFF0000U;
const uint32_t REPLLOGKEY_CHUNKID = 0XFFFFFF00U;
const uint32_t REPLLOGKEYV2_CHUNKID = 0XFFFFFF01U;
const uint32_t ADMINCMD_CHUNKID = 0XFFFE0001U;

const uint32_t VERSIONMETA_DBID = 0XFFFE0000U;
const uint32_t TTLINDEX_DBID = 0XFFFF0000U;
const uint32_t REPLLOGKEY_DBID = 0XFFFFFF00U;
const uint32_t REPLLOGKEYV2_DBID = 0XFFFFFF01U;
const uint32_t ADMINCMD_DBID = 0XFFFE0001U;

/* NOTE(vinchen): if you want to add new RecordType, make sure you handle
   the below functions correctly.

   uint8_t rt2Char(RecordType t);
   RecordType char2Rt(uint8_t t);
   std::string rt2Str(RecordType t);
   RecordType getRealKeyType(RecordType t);
   bool isRealEleType(RecordType keyType, RecordType valueType);
*/
enum class RecordType {
  RT_INVALID,
  RT_META,       /* For catalog */
  RT_KV,         /* For realtype in RecordValue */
  RT_LIST_META,  /* For realtype in RecordValue */
  RT_LIST_ELE,   /* For list subkey type in RecordKey and RecordValue */
  RT_HASH_META,  /* For realtype in RecordValue */
  RT_HASH_ELE,   /* For hash subkey type in RecordKey and RecordValue  */
  RT_SET_META,   /* For realtype in RecordValue */
  RT_SET_ELE,    /* For set subkey type in RecordKey and RecordValue  */
  RT_ZSET_META,  /* For realtype in RecordValue */
  RT_ZSET_S_ELE, /* For zset subkey type in RecordKey and RecordValue  */
  RT_ZSET_H_ELE, /* For zset subkey type in RecordKey and RecordValue  */
  RT_BINLOG,     /* For binlog in RecordKey and RecordValue  */
  RT_TTL_INDEX,  /* For ttl index  in RecordKey and RecordValue  */
  RT_DATA_META,  /* For key type in RecordKey */
};

uint8_t rt2Char(RecordType t);
RecordType char2Rt(uint8_t t);
std::string rt2Str(RecordType t);
bool isDataMetaType(RecordType t);
bool isKeyType(RecordType t);
RecordType getRealKeyType(RecordType t);
bool isRealEleType(RecordType keyType, RecordType valueType);

// ********************* key format ***********************************
// ChunkId + DBID + Type + PK + 0 + VERSION + SK + len(PK) + 1B reserved
// ChunkId is an uint32 in big-endian, it's mainly used for logic migra
// -tion
// DBID is an uint32 in big-endian
// Type is a char, 'a' for RT_KV, 'm' for RT_META, 'L' for RT_LIST_META
// 'l' for RT_LIST_ELE an so on, see rt2Char
// PK is primarykey, its length is described in len(PK)
// 0
// VERSION is varint, it means multi-version of record. For *_META, it
//   always 0. Maybe it would be useful for _ELE. It would be always 0 now.
// SK is secondarykey, its length is not stored
// len(PK) is varint32 stored in bigendian, so we can read from the end
// backwards. the last 1B are reserved.
// ********************* value format *********************************
// TYPE + TTL + VERSION + VERSIONEP + CAS + PIECESIZE + TOTALSIZE + UserValue
// TYPE is one byte for real type of record
// TTL is a varint64
// VERSION is a varint64, for multi-version. Reversed, always 0
// VERSIONEP is a varint64, for extended protocol. Reversed, always 0
// CAS is a varint64, for cas cmd
// PIECESIZE is a varint64, for very big value. Reversed, always 0
// TOTALSIZE is varint64. Now it is always == UserValue.size().
// UserValue is string.
// ********************************************************************

class RecordKey {
 public:
  using TRSV = uint8_t;
  RecordKey();
  RecordKey(const RecordKey&) = default;
  // we should not rely on default move constructor.
  // refer to the manual, int types have no move constructor
  // so copy constructor is applied, the move-from object will
  // be in a dangling state
  RecordKey(RecordKey&&);
  RecordKey(uint32_t chunkId,
            uint32_t dbid,
            RecordType type,
            const std::string& pk,
            const std::string& sk,
            uint64_t version = 0);
  RecordKey(uint32_t chunkId,
            uint32_t dbid,
            RecordType type,
            std::string&& pk,
            std::string&& sk,
            uint64_t version = 0);
  const std::string& getPrimaryKey() const;
  const std::string& getSecondaryKey() const;
  uint32_t getChunkId() const;
  uint32_t getDbId() const;

  // an encoded prefix until prefix and a padding zero.
  // mainly for prefix scan.
  std::string prefixPk() const;

  std::string prefixSlotType() const;
  std::string prefixChunkid() const;

  /*
  // an encoded prefix with db & type, with no padding zero.
  std::string prefixDbidType() const;
  */
  static const std::string& prefixReplLogV2();
  static const std::string& prefixTTLIndex();
  static const std::string& prefixVersionMeta();

  RecordType getRecordType() const;
  RecordType getRecordValueType() const;
  std::string encode() const;
  static uint32_t decodeChunkId(const std::string& key);
  static uint32_t decodeDbId(const std::string& key);
  static RecordType decodeType(const std::string& key);
  static Expected<RecordKey> decode(const std::string& key);
  static RecordType decodeType(const char* key, size_t size);
  static Expected<bool> validate(const std::string& key,
                                 RecordType type = RecordType::RT_INVALID);
  static size_t minSize();
  static size_t getHdrSize() {
    return PK_OFFSET;
  }
  bool operator==(const RecordKey& other) const;
  bool operator!=(const RecordKey& other) const;

  static constexpr size_t CHUNKID_OFFSET = 0;
  static constexpr size_t TYPE_OFFSET = CHUNKID_OFFSET + sizeof(uint32_t);
  static constexpr size_t DBID_OFFSET = TYPE_OFFSET + sizeof(uint8_t);
  static constexpr size_t PK_OFFSET = DBID_OFFSET + sizeof(uint32_t);

 private:
  void encodePrefixPk(std::vector<uint8_t>*) const;
  uint32_t _chunkId;
  uint32_t _dbId;
  RecordType _type;
  RecordType _valueType;
  std::string _pk;
  std::string _sk;
  // version for subkey, it would be always 0 for *_META.
  uint64_t _version;
  TRSV _fmtVsn;
};

class RecordValue {
 public:
  explicit RecordValue(RecordType type);
  RecordValue(const RecordValue&) = default;

  // for zset score
  RecordValue(double v, RecordType type);

  // we should not rely on default move constructor.
  // refer to the manual, int types have no move constructor
  // so copy constructor is applied, the move-from object will
  // be in a dangling state
  RecordValue(RecordValue&&);
  RecordValue& operator=(RecordValue&&) noexcept;
  explicit RecordValue(const std::string& val,
                       RecordType type,
                       uint64_t versionEp,
                       uint64_t ttl = 0,
                       int64_t cas = -1,
                       uint64_t version = 0,
                       uint64_t pieceSize = (uint64_t)-1);
  explicit RecordValue(std::string&& val,
                       RecordType type,
                       uint64_t versionEp,
                       uint64_t ttl = 0,
                       int64_t cas = -1,
                       uint64_t version = 0,
                       uint64_t pieceSize = (uint64_t)-1);

  RecordValue(const std::string& val,
              RecordType type,
              uint64_t versionEp,
              uint64_t ttl,
              const Expected<RecordValue>& oldRV);
  RecordValue(const std::string&& val,
              RecordType type,
              uint64_t versionEp,
              uint64_t ttl,
              const Expected<RecordValue>& oldRV);
  const std::string& getValue() const;
  uint64_t getTtl() const;
  void setTtl(uint64_t);
  int64_t getCas() const;
  void setCas(int64_t);
  RecordType getRecordType() const {
    return _type;
  }
  void setRecordType(RecordType type) {
    _type = type;
  }
  uint64_t getVersion() const {
    return _version;
  }
  void setVersion(uint64_t version) {
    _version = version;
  }
  uint64_t getVersionEP() const {
    return _versionEP;
  }
  void setVersionEP(uint64_t versionEP) {
    _versionEP = versionEP;
  }
  uint64_t getPieceSize() const {
    return _pieceSize;
  }
  void setPieceSize(uint64_t size) {
    _pieceSize = size;
  }
  uint64_t getTotalSize() const {
    return _totalSize;
  }
  std::string encode() const;
  static Expected<RecordValue> decode(const std::string& value);
  static Expected<size_t> decodeHdrSize(const std::string& value);
  static Expected<size_t> decodeHdrSizeNoMeta(const std::string& value);
  static Expected<bool> validate(const std::string& value,
                                 RecordType type = RecordType::RT_INVALID);
  static uint64_t decodeTtl(const char* value, size_t size);
  static RecordType decodeType(const char* value, size_t size);
  static size_t minSize();
  bool operator==(const RecordValue& other) const;

  static constexpr size_t TYPE_OFFSET = 0;
  static constexpr size_t TTL_OFFSET = TYPE_OFFSET + sizeof(uint8_t);
  uint64_t getEleCnt() const;
  RecordType getEleType() const;
  bool isBigKey(uint64_t valueSize, uint64_t eleCnt) const;

 private:
  // if RecordKey._type = META, _typeForMeta means the real
  // meta type. For other RecordKey._type, it's useless.
  RecordType _type;
  uint64_t _ttl;
  // version for subkey, maybe it useful for big key deletion, reversed
  uint64_t _version;
  // version for extended protocol, reversed
  uint64_t _versionEP;
  // cas
  int64_t _cas;
  // For very big values, it may split into multi pieces;  reversed
  uint64_t _pieceSize;
  // if (_pieceSize > _totalSize)
  //      _valus.size() == _totalSize
  // else
  //      _totalSize > _values.size()
  // TODO(vinchen) it would be useful for append and bitmap
  // the whole value size, maybe > _value.size()
  uint64_t _totalSize;
  std::string _value;
};

class Record {
 public:
  using KV = std::pair<std::string, std::string>;
  Record();
  Record(const Record&) = default;
  Record(Record&&);
  Record(const RecordKey& key, const RecordValue& value);
  Record(RecordKey&& key, RecordValue&& value);
  const RecordKey& getRecordKey() const;
  const RecordValue& getRecordValue() const;
  static Expected<Record> decode(const std::string& key,
                                 const std::string& value);
  KV encode() const;
  bool operator==(const Record& other) const;
  std::string toString() const;

 private:
  RecordKey _key;
  RecordValue _value;
};

enum class ReplFlag : std::uint16_t {
  REPL_GROUP_MID = 0,
  REPL_GROUP_START = (1 << 0),
  REPL_GROUP_END = (1 << 1),
};

enum class ReplOp : std::uint8_t {
  REPL_OP_NONE = 0,
  REPL_OP_SET = 1,
  REPL_OP_DEL = 2,
  REPL_OP_STMT = 3,  // statement
  REPL_OP_SPEC = 4,  // special
  REPL_OP_DEL_RANGE = 5,
};

class ReplLogKeyV2 {
 public:
  ReplLogKeyV2();
  ReplLogKeyV2(const ReplLogKeyV2&) = delete;
  ReplLogKeyV2(ReplLogKeyV2&&);
  explicit ReplLogKeyV2(uint64_t binlogid);
  static Expected<ReplLogKeyV2> decode(const RecordKey&);
  static Expected<ReplLogKeyV2> decode(const std::string&);
  // static std::string& updateBinlogId(std::string& encodeStr, uint64_t
  // newBinlogId);
  std::string encode() const;
  bool operator==(const ReplLogKeyV2&) const;
  ReplLogKeyV2& operator=(const ReplLogKeyV2&);
  void setBinlogId(uint64_t id) {
    _binlogId = id;
  }
  uint64_t getBinlogId() const {
    return _binlogId;
  }

  static constexpr uint32_t DBID = REPLLOGKEYV2_DBID;
  static constexpr uint32_t CHUNKID = REPLLOGKEYV2_CHUNKID;
  static constexpr size_t BINLOG_OFFSET = RecordKey::PK_OFFSET;
  static constexpr size_t BINLOG_SIZE = sizeof(uint64_t);

 private:
  uint64_t _binlogId;
  std::string _version;
};

class ReplLogValueEntryV2 {
 public:
  ReplLogValueEntryV2();
  ReplLogValueEntryV2(const ReplLogValueEntryV2&) = default;
  ReplLogValueEntryV2(ReplLogValueEntryV2&&);
  ReplLogValueEntryV2(ReplOp op,
                      uint64_t ts,
                      const std::string&,
                      const std::string&);
  ReplLogValueEntryV2(ReplOp op,
                      uint64_t ts,
                      std::string&& key,
                      std::string&& val);
  const std::string& getOpKey() const {
    return _key;
  }
  const std::string& getOpValue() const {
    return _val;
  }
  uint64_t getTimestamp() const {
    return _timestamp;
  }
  ReplOp getOp() const {
    return _op;
  }

  static Expected<ReplLogValueEntryV2> decode(const char* rawVal,
                                              size_t maxSize,
                                              size_t* decodeSize);
  std::string encode() const;
  size_t encode(uint8_t* dest, size_t destSize) const;
  size_t encodeSize() const;
  ReplLogValueEntryV2& operator=(ReplLogValueEntryV2&&);
  bool operator==(const ReplLogValueEntryV2&) const;

 private:
  ReplOp _op;
  uint64_t _timestamp;    // in milliseconds
  std::string _key;
  std::string _val;
};

class ReplLogValueV2 {
 public:
  ReplLogValueV2();
  ReplLogValueV2(const ReplLogValueV2&) = delete;
  ReplLogValueV2(ReplLogValueV2&&);
  ReplLogValueV2(uint32_t chunkId,
                 ReplFlag flag,
                 uint64_t txnid,
                 uint64_t timestamp,
                 uint64_t versionEp,
                 const std::string& cmd,
                 const uint8_t* data,
                 size_t dataSize);
  static Expected<ReplLogValueV2> decode(const std::string& rvStr);
  static Expected<ReplLogValueV2> decode(const char* str, size_t size);
  // not include cmdStr
  static size_t fixedHeaderSize();
  // include cmdStr
  size_t getHdrSize() const;
  std::string encodeHdr() const;
  std::string encode(const std::vector<ReplLogValueEntryV2>& vec) const;
  bool isEqualHdr(const ReplLogValueV2&) const;
  const uint8_t* getData() const {
    return _data;
  }
  size_t getDataSize() const {
    return _dataSize;
  }
  ReplFlag getReplFlag() const {
    return _flag;
  }
  uint64_t getTxnId() const {
    return _txnId;
  }
  uint32_t getChunkId() const {
    return _chunkId;
  }
  uint64_t getTimestamp() const {
    return _timestamp;
  }
  uint64_t getVersionEp() const {
    return _versionEp;
  }
  Expected<std::vector<ReplLogValueEntryV2>> getLogList() const;
  const std::string& getCmd() const {
    return _cmdStr;
  }

  static constexpr size_t CHUNKID_OFFSET = 0;
  static constexpr size_t FLAG_OFFSET = CHUNKID_OFFSET + sizeof(uint32_t);
  static constexpr size_t TXNID_OFFSET = FLAG_OFFSET + sizeof(uint16_t);
  static constexpr size_t TIMESTAMP_OFFSET = TXNID_OFFSET + sizeof(uint64_t);
  static constexpr size_t VERSIONEP_OFFSET =
    TIMESTAMP_OFFSET + sizeof(uint64_t);
  static constexpr size_t FIXED_HEADER_SIZE =
    VERSIONEP_OFFSET + sizeof(uint64_t);

 private:
  uint32_t _chunkId;
  ReplFlag _flag;
  uint64_t _txnId;
  uint64_t _timestamp;
  uint64_t _versionEp;
  std::string _cmdStr;
  // NOTE(vinchen) : take care about "_data", the caller should guarantee the
  // memory is ok;
  // printer to the RecordValue.getValue().c_str()
  const uint8_t* _data;
  size_t _dataSize;
};

class ReplLogRawV2 {
 public:
  using KV = std::pair<std::string, std::string>;
  ReplLogRawV2() = delete;
  ReplLogRawV2(const ReplLogRawV2&) = delete;
  ReplLogRawV2(ReplLogRawV2&&);
  ReplLogRawV2(const std::string& key, const std::string& value);
  explicit ReplLogRawV2(const Record& record);
  ReplLogRawV2(std::string&& key, std::string&& value);
  uint64_t getBinlogId();
  uint64_t getVersionEp();
  uint64_t getTimestamp();
  uint32_t getChunkId();
  const std::string& getReplLogKey() const {
    return _key;
  }
  const std::string& getReplLogValue() const {
    return _val;
  }

 private:
  std::string _key;
  std::string _val;
};


class ReplLogV2 {
 public:
  using KV = std::pair<std::string, std::string>;
  ReplLogV2() = delete;
  ReplLogV2(const ReplLogV2&) = default;
  ReplLogV2(ReplLogV2&&);
  ReplLogV2(ReplLogKeyV2&& key,
            ReplLogValueV2&& value,
            std::vector<ReplLogValueEntryV2>&& entry);
  // ReplLogV2(const ReplLogRawV2& log);
  // ReplLogV2(const std::string& key, const std::string& value);
  const ReplLogKeyV2& getReplLogKey() const {
    return _key;
  }
  const ReplLogValueV2& getReplLogValue() const {
    return _val;
  }
  uint64_t getTimestamp() const;
  const std::vector<ReplLogValueEntryV2>& getReplLogValueEntrys() const {
    return _entrys;
  }
  static Expected<ReplLogV2> decode(const std::string& key,
                                    const std::string& value);
  // encode() see: ReplManager::applySingleTxnV2()
  // KV encode() const;

  // bool operator==(const ReplLog&) const;

 private:
  ReplLogKeyV2 _key;
  ReplLogValueV2 _val;
  std::vector<ReplLogValueEntryV2> _entrys;
};

enum class BinlogFlag {
  NORMAL = 0,
  FLUSH = 1,
  MIGRATE = 2,
};

enum class BinlogApplyMode { KEEP_BINLOG_ID, NEW_BINLOG_ID };

class Binlog {
 public:
  Binlog() = default;
  static size_t writeHeader(std::stringstream& s);
  static size_t writeRepllogRaw(std::stringstream& s,
                                const ReplLogRawV2& repllog);
  static size_t decodeHeader(const char* str, size_t size);

  static constexpr size_t HEADERSIZE = 1;
  static constexpr uint8_t VERSION = 2;
  static constexpr uint8_t INVALID_VERSION = (uint8_t)-1;

 private:
};

class BinlogWriter {
 public:
  BinlogWriter(size_t maxSize, uint32_t maxCount);
  bool writeRepllogRaw(const ReplLogRawV2& repllog);

  uint32_t getCount() const {
    return _curCnt;
  }
  uint32_t getSize() const {
    return _curSize;
  }
  BinlogFlag getFlag() const {
    return _flag;
  }
  bool writerFull();
  void resetWriter();
  void setFlag(BinlogFlag flag) {
    _flag = flag;
  }
  std::string getBinlogStr() {
    return _ss.str();
  }

 private:
  size_t _curSize;
  size_t _maxSize;
  uint32_t _curCnt;
  uint32_t _maxCnt;
  BinlogFlag _flag;
  std::stringstream _ss;
};

class BinlogReader {
 public:
  explicit BinlogReader(const std::string& s);
  Expected<ReplLogRawV2> next();
  Expected<ReplLogV2> nextV2();

 private:
  size_t _pos;
  mystring_view _val;
};

class ListMetaValue {
 public:
  ListMetaValue(uint64_t head, uint64_t tail);
  ListMetaValue(ListMetaValue&&);
  static Expected<ListMetaValue> decode(const std::string&);
  ListMetaValue& operator=(ListMetaValue&&);
  std::string encode() const;
  void setHead(uint64_t head);
  uint64_t getHead() const;
  void setTail(uint64_t tail);
  uint64_t getTail() const;

 private:
  uint64_t _head;
  uint64_t _tail;
};

class HashMetaValue {
 public:
  HashMetaValue();
  explicit HashMetaValue(uint64_t count);
  HashMetaValue(HashMetaValue&&);
  static Expected<HashMetaValue> decode(const std::string&);
  HashMetaValue& operator=(HashMetaValue&&);
  std::string encode() const;
  void setCount(uint64_t count);
  // void setCas(int64_t cas);
  uint64_t getCount() const;
  // uint64_t getCas() const;

 private:
  uint64_t _count;
};

class SetMetaValue {
 public:
  SetMetaValue();
  explicit SetMetaValue(uint64_t count);
  static Expected<SetMetaValue> decode(const std::string&);
  std::string encode() const;
  void setCount(uint64_t count);
  uint64_t getCount() const;

 private:
  uint64_t _count;
};


/*

META: *1
CHUNK|DBID|ZSET_META|KEY|
LEVEL|MAX_LEVEL|COUNT+1|TAIL|POSALLOC|

S_ELE: *(COUNT+1)
CHUNK|DBID|S_ELE|KEY|POS|  -- HEAD_ID(first element)
forword*(MAX_LEVEL+1)|span*(MAX_LEVEL+1)|score|backward|subkey|

H_ELE: *(COUNT)
CHUNK|DBID|H_ELE|KEY|SUBKEY|
score

*/

// ZsetSkipListMetaValue
class ZSlMetaValue {
 public:
  ZSlMetaValue();
  ZSlMetaValue(uint8_t lvl, uint32_t count, uint64_t tail);
  ZSlMetaValue(uint8_t lvl, uint32_t count, uint64_t tail, uint64_t alloc);
  static Expected<ZSlMetaValue> decode(const std::string&);
  std::string encode() const;
  uint8_t getMaxLevel() const;
  uint8_t getLevel() const;
  uint32_t getCount() const;
  uint64_t getTail() const;
  uint64_t getPosAlloc() const;
  // can not dynamicly change
  static constexpr int8_t MAX_LAYER = ZSKIPLIST_MAXLEVEL;
  static constexpr uint32_t MAX_NUM = (1 << 31);
  static constexpr uint64_t MIN_POS = 128;
  // NOTE(deyukong): static constexpr uint32_t HEAD_ID = 1
  // fails to unlink, I don't know why, and give up, the
  // definition of HEAD_ID is in record.cpp
  static uint32_t HEAD_ID;

 private:
  uint8_t _level;
  uint8_t _maxLevel;
  uint32_t _count;
  uint64_t _tail;
  uint64_t _posAlloc;
};

class ZSlEleValue {
 public:
  ZSlEleValue();
  // NOTE(vinchen): if we want to change maxLevel,
  // it can't use default value here
  ZSlEleValue(double score,
              const std::string& subkey,
              uint32_t maxLevel = ZSlMetaValue::MAX_LAYER);
  static Expected<ZSlEleValue> decode(const std::string&);
  std::string encode() const;
  uint64_t getForward(uint8_t layer) const;
  uint64_t getBackward() const;
  void setForward(uint8_t layer, uint64_t pointer);
  void setBackward(uint64_t pointer);
  double getScore() const;
  const std::string& getSubKey() const;
  uint32_t getSpan(uint8_t layer) const;
  void setSpan(uint8_t layer, uint32_t span);
  bool isChanged() {
    return _changed;
  }
  void setChanged(bool v) {
    _changed = v;
  }

 private:
  // forward elements index in each level
  std::vector<uint64_t> _forward;
  // step to forward element in each level
  std::vector<uint32_t> _span;
  double _score;
  // backward element index in level 1
  uint64_t _backward;
  // whether _changed after skiplist::getnode()
  bool _changed;

  std::string _subKey;
};

enum class IndexType : std::uint8_t {
  IT_TTL,
  IT_INVALID,
};

uint8_t it2Char(IndexType t);
IndexType char2It(uint8_t t);

class TTLIndex {
 public:
  TTLIndex() = default;
  TTLIndex& operator=(const TTLIndex& o);

  TTLIndex(const std::string& priKey,
           RecordType type,
           uint32_t dbid,
           uint64_t ttl = 0)
    :  // more furthur index attributed put here
      _priKey(priKey),
      _type(type),
      _dbId(dbid),
      _ttl(ttl) {}

  const std::string ttlIndex() const;

  static std::string decodePriKey(const std::string& index);
  static RecordType decodeType(const std::string& index);
  static std::uint32_t decodeDBId(const std::string& index);
  static std::uint64_t decodeTTL(const std::string& index);

  std::string encode() const;
  static Expected<TTLIndex> decode(const RecordKey& rk);

  std::string getPriKey() const {
    return _priKey;
  }
  RecordType getType() const {
    return _type;
  }
  std::uint32_t getDbId() const {
    return _dbId;
  }
  std::uint64_t getTTL() const {
    return _ttl;
  }

 private:
  std::string _priKey;
  RecordType _type;
  uint32_t _dbId;
  uint64_t _ttl;

 public:
  static constexpr uint32_t CHUNKID = TTLINDEX_DBID;
  static constexpr uint32_t DBID = TTLINDEX_CHUNKID;
};

class VersionMeta {
 public:
  VersionMeta() : VersionMeta(0, 0, "") {}
  VersionMeta(const VersionMeta&) = default;
  VersionMeta(VersionMeta&&) = default;
  VersionMeta(uint64_t ts, uint64_t v, const std::string& name)
    : _timestamp(ts), _version(v), _name(name) {}
  VersionMeta& operator=(const VersionMeta& meta) {
    if (this != &meta) {
      _timestamp = meta.getTimeStamp();
      _version = meta.getVersion();
      _name = meta.getName();
    }
    return *this;
  }
  uint64_t getTimeStamp() const {
    return _timestamp;
  }
  uint64_t getVersion() const {
    return _version;
  }
  std::string getName() const {
    return _name;
  }

  void setTimeStamp(uint64_t ts) {
    _timestamp = ts;
  }
  void setVersion(uint64_t version) {
    _version = version;
  }

  static Expected<VersionMeta> decode(const RecordKey& rk,
                                      const RecordValue& rv);

  friend bool operator<(const VersionMeta& lhs, const VersionMeta& rhs) {
    INVARIANT_D(lhs.getName() == rhs.getName());
    if (lhs.getVersion() == UINT64_MAX) {
      return true;
    } else if (rhs.getVersion() == UINT64_MAX) {
      return false;
    } else {
      return lhs.getVersion() < rhs.getVersion();
    }
  }

  uint64_t _timestamp;
  uint64_t _version;
  std::string _name;
  static constexpr uint32_t DBID = VERSIONMETA_DBID;
  static constexpr uint32_t CHUNKID = VERSIONMETA_CHUNKID;
};

namespace rcd_util {
Expected<uint64_t> getSubKeyCount(const RecordKey& key, const RecordValue& val);

std::string makeInvalidErrStr(RecordType type,
                              const std::string& key,
                              uint64_t metaCnt,
                              uint64_t eleCnt);

}  // namespace rcd_util
}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_STORAGE_RECORD_H_
