// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_COMMANDS_DUMP_H_
#define SRC_TENDISPLUS_COMMANDS_DUMP_H_

#include <string>
#include <vector>
#include <map>
#include <memory>
#include <cstddef>
#include <list>
#include "tendisplus/commands/command.h"
#include "tendisplus/utils/status.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/server/session.h"
#include "tendisplus/storage/record.h"
namespace tendisplus {

using byte = unsigned char;

static const uint16_t RDB_VERSION = 8;
// static const uint16_t RDB_VERSION = 9;    // for test only

static const uint8_t RDB_6BITLEN = 0;
static const uint8_t RDB_14BITLEN = 1;
static const uint8_t RDB_ENCVAL = 3;
static const uint8_t RDB_32BITLEN = 0x80;  // 10000000
static const uint8_t RDB_64BITLEN = 0x81;  // 10000001

static const uint8_t RDB_ENC_INT8 = 0;
static const uint8_t RDB_ENC_INT16 = 1;
static const uint8_t RDB_ENC_INT32 = 2;
static const uint8_t RDB_ENC_LZF = 3;

static const uint8_t ZIP_STR_MASK = 0xc0;
static const uint8_t ZIP_INT_16B = 0xc0 | 0 << 4;
static const uint8_t ZIP_INT_32B = 0xc0 | 1 << 4;
static const uint8_t ZIP_INT_64B = 0xc0 | 2 << 4;
static const uint8_t ZIP_INT_24B = 0xc0 | 3 << 4;
static const uint8_t ZIP_INT_8B = 0xfe;

static const uint8_t ZIP_INT_IMM_MASK = 0x0f;
static const uint8_t ZIP_INT_IMM_MIN = 0xf1;
static const uint8_t ZIP_INT_IMM_MAX = 0xfd;

enum class DumpType : uint8_t {
  RDB_TYPE_STRING = 0,
  RDB_TYPE_QUICKLIST = 14,
  RDB_TYPE_SET = 2,
  RDB_TYPE_ZSET = 5,
  RDB_TYPE_HASH = 4,
};

// utility
constexpr uint32_t ZLBYTE_LIMIT = 8192;
constexpr uint32_t ZLLEN_LIMIT = 256;

// this `extern` is a little weird here i think..
constexpr uint64_t MAXSEQ = 9223372036854775807ULL;
constexpr uint64_t INITSEQ = MAXSEQ / 2ULL;

Expected<bool> delGeneric(Session* sess, const std::string& key,
        Transaction* txn);
Expected<std::string> genericZadd(Session* sess,
                                  PStore kvstore,
                                  const RecordKey& mk,
                                  const Expected<RecordValue>& eMeta,
                                  const std::map<std::string, double>& subKeys,
                                  int flags,
                                  Transaction* txn);

template <typename T>
size_t easyCopy(std::vector<byte>* buf, size_t* pos, T element);
template <typename T>
size_t easyCopy(std::vector<byte>* buf,
                size_t* pos,
                const T* array,
                size_t len);
template <typename T>
size_t easyCopy(T* dest, const std::string& buf, size_t* pos);
uint8_t decodeType(RecordType type);

class Serializer {
 public:
  explicit Serializer(Session* sess,
                      const std::string& key,
                      DumpType type,
                      RecordValue&& rv);
  Serializer(Serializer&& rhs) = default;
  virtual ~Serializer() = default;
  Expected<std::vector<byte>> dump(bool prefixVer = false);
  virtual Expected<size_t> dumpObject(std::vector<byte>* buf) = 0;
  // virtual Expected<std::vector<byte>> restore() = 0;

  static Expected<size_t> saveObjectType(std::vector<byte>* payload,
                                         size_t* pos,
                                         DumpType type);
  static Expected<size_t> saveLen(std::vector<byte>* payload,
                                  size_t* pos,
                                  size_t len);
  static size_t saveString(std::vector<byte>* payload,
                           size_t* pos,
                           const std::string& str);

  uint64_t getTTL() {
    return _rv.getTtl();
  }

  size_t _begin, _end;

 protected:
  Session* _sess;
  std::string _key;
  DumpType _type;
  size_t _pos;
  RecordValue _rv;
};
Expected<std::unique_ptr<Serializer>> getSerializer(Session* sess,
                                                    const std::string& key);

class Deserializer {
 public:
  explicit Deserializer(Session* sess,
                        const std::string& payload,
                        const std::string& key,
                        const uint64_t ttl);
  virtual ~Deserializer() = default;
  virtual Status restore(Transaction* txn) = 0;
  static Status preCheck(const std::string& payload);
  static Expected<DumpType> loadObjectType(const std::string& payload,
                                           size_t&& pos);
  static Expected<size_t> loadLen(const std::string& payload,
                                  size_t* pos,
                                  bool* isencoded = nullptr);
  static std::string loadString(const std::string& payload, size_t* pos);
  static Expected<int64_t> loadIntegerString(const std::string& payload,
                                             size_t* pos,
                                             uint8_t encType);
  static Expected<std::string> loadLzfString(const std::string& payload,
                                             size_t* pos);

 protected:
  Session* _sess;
  std::string _payload;
  std::string _key;
  uint64_t _ttl;
  size_t _pos;
};

class IncrMeta {
 public:
  IncrMeta(std::string key,
           ReplOp op,
           uint8_t type,
           uint64_t timestamp,
           uint64_t ttl,
           uint64_t ver)
    : _key(key),
      _op(op),
      _timestamp(timestamp),
      _type(type),
      _ttl(ttl),
      _version(ver) {}

  std::string _key;
  ReplOp _op;
  uint64_t _timestamp;
  uint8_t _type;
  uint64_t _ttl;
  uint64_t _version;
};

struct IncrMetaHash {
  size_t operator()(const struct IncrMeta& meta) const {
    return std::hash<std::string>()(meta._key);
  }
};

inline bool operator==(const struct IncrMeta& X, const struct IncrMeta& Y) {
  return std::hash<std::string>()(X._key) == std::hash<std::string>()(Y._key);
}

Expected<std::unique_ptr<Deserializer>> getDeserializer(
  Session* sess,
  const std::string& payload,
  const std::string& key,
  const uint64_t ttl);

Expected<std::string> recordList2Aof(const std::list<Record>& list);
Expected<std::string> key2Aof(Session* sess, const std::string& key);

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_COMMANDS_DUMP_H_
