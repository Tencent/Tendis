#ifndef SRC_TENDISPLUS_STORAGE_RECORD_H_
#define SRC_TENDISPLUS_STORAGE_RECORD_H_

#include <string>
#include <utility>
#include <memory>
#include <vector>
#include <limits>
#include "tendisplus/utils/status.h"
#include "tendisplus/storage/kvstore.h"

namespace tendisplus {

enum class RecordType {
    RT_INVALID,
    RT_META,
    RT_KV,
    RT_LIST_META,
    RT_LIST_ELE,
    RT_HASH_META,
    RT_HASH_ELE,
    RT_SET_META,
    RT_SET_ELE,
    RT_ZSET_META,
    RT_ZSET_S_ELE,
    RT_ZSET_H_ELE,
    RT_BINLOG,
};

uint8_t rt2Char(RecordType t);
RecordType char2Rt(uint8_t t);

// ********************* key format ***********************************
// DBID + Type + PK + SK + 0 + len(PK) + 1B reserved
// DBID is an int32 in big-endian
// Type is a char, 'a' for RT_KV, 'm' for RT_META, 'L' for RT_LIST_META
// 'l' for RT_LIST_ELE an so on, see rt2Char
// PK is primarykey, its length is described in len(PK)
// SK is secondarykey, its length is not stored
// len(PK) is varint32 stored in bigendian, so we can read
// from the end backwards.
// the last 1B are reserved.
// ********************* value format *********************************
// TTL + UserValue
// TTL is a varint64
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
    RecordKey(RecordType type, const std::string& pk,
        const std::string& sk);
    RecordKey(uint32_t dbid, RecordType type, const std::string& pk,
        const std::string& sk);
    RecordKey(uint32_t dbid, RecordType type, std::string&& pk,
        std::string&& sk);
    const std::string& getPrimaryKey() const;
    const std::string& getSecondaryKey() const;
    uint32_t getDbId() const;

    // an encoded prefix until prefix and a padding zero.
    // mainly for prefix scan.
    std::string prefixPk() const;
    static const std::string& prefixReplLog();

    RecordType getRecordType() const;
    std::string encode() const;
    static Expected<RecordKey> decode(const std::string& key);
    bool operator==(const RecordKey& other) const;

 private:
    void encodePrefixPk(std::vector<uint8_t>*) const;
    uint32_t _dbId;
    RecordType _type;
    std::string _pk;
    std::string _sk;
    TRSV _fmtVsn;
};

class RecordValue {
 public:
    RecordValue();
    RecordValue(const RecordValue&) = default;

    // we should not rely on default move constructor.
    // refer to the manual, int types have no move constructor
    // so copy constructor is applied, the move-from object will
    // be in a dangling state
    RecordValue(RecordValue&&);
    explicit RecordValue(const std::string& val, uint64_t ttl = 0);
    explicit RecordValue(std::string&& val, uint64_t ttl = 0);
    const std::string& getValue() const;
    uint64_t getTtl() const;
    void setTtl(uint64_t);
    std::string encode() const;
    static Expected<RecordValue> decode(const std::string& value);
    bool operator==(const RecordValue& other) const;

 private:
    uint64_t _ttl;
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

 private:
    RecordKey _key;
    RecordValue _value;
};

enum class ReplFlag: std::uint16_t {
    REPL_GROUP_MID = 0,
    REPL_GROUP_START = (1<<0),
    REPL_GROUP_END = (1<<1),
};

enum class ReplOp: std::uint8_t {
    REPL_OP_NONE = 0,
    REPL_OP_SET = 1,
    REPL_OP_DEL = 2,
};

// ********************* repl key format ******************************
// txnId + localId + flag + timestamp + reserved
// txnId is a uint64_t in big endian. big endian ensures the scan order
// is same with value order
// localId is a uint16 in big endian. stores the operation counter in
// the same transaction.
// flag is a uint16 in big endian.
// timestamp is a uint32 in big endian.
// reserved is a uint8.
// ********************* repl value format ****************************
// op + keylen + key + valuelen + value
// op is a uint8
// keylen is a varint
// valuelen is a varint
// ********************************************************************
class ReplLogKey {
 public:
    ReplLogKey();
    ReplLogKey(const ReplLogKey&) = default;
    ReplLogKey(ReplLogKey&&);
    ReplLogKey(uint64_t txnid, uint16_t localid, ReplFlag flag,
        uint32_t timestamp, uint8_t reserved = 0);
    static Expected<ReplLogKey> decode(const RecordKey&);
    static Expected<ReplLogKey> decode(const std::string&);
    std::string encode() const;
    bool operator==(const ReplLogKey&) const;
    ReplLogKey& operator=(const ReplLogKey&);
    uint64_t getTxnId() const { return _txnId; }
    uint16_t getLocalId() const { return _localId; }
    uint32_t getTimestamp() const { return _timestamp; }
    void setFlag(ReplFlag f) { _flag = f; }
    ReplFlag getFlag() const { return _flag; }

    static std::string prefix(uint64_t commitId);
    static constexpr uint32_t DBID = std::numeric_limits<uint32_t>::max();

 private:
    uint64_t _txnId;
    uint16_t _localId;
    ReplFlag _flag;
    uint32_t _timestamp;
    uint8_t _reserved;
};

class ReplLogValue {
 public:
    ReplLogValue();
    ReplLogValue(const ReplLogValue&) = default;
    ReplLogValue(ReplLogValue&&);
    ReplLogValue(ReplOp op, const std::string&, const std::string&);
    const std::string& getOpKey() const;
    const std::string& getOpValue() const;
    ReplOp getOp() const;
    static Expected<ReplLogValue> decode(const std::string&);
    static Expected<ReplLogValue> decode(const RecordValue&);
    std::string encode() const;
    bool operator==(const ReplLogValue&) const;

 private:
    ReplOp _op;
    std::string _key;
    std::string _val;
};

class ReplLog {
 public:
    using KV = std::pair<std::string, std::string>;
    ReplLog();
    ReplLog(const ReplLog&) = default;
    ReplLog(ReplLog&&);
    ReplLog(const ReplLogKey& key, const ReplLogValue& value);
    ReplLog(ReplLogKey&& key, ReplLogValue&& value);
    const ReplLogKey& getReplLogKey() const;
    ReplLogKey& getReplLogKey();
    const ReplLogValue& getReplLogValue() const;
    static Expected<ReplLog> decode(const std::string& key,
            const std::string& value);
    KV encode() const;
    bool operator==(const ReplLog&) const;

 private:
    ReplLogKey _key;
    ReplLogValue _val;
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
    HashMetaValue(uint64_t count, uint64_t cas);
    HashMetaValue(HashMetaValue&&);
    static Expected<HashMetaValue> decode(const std::string&);
    HashMetaValue& operator=(HashMetaValue&&);
    std::string encode() const;
    void setCount(uint64_t count);
    void setCas(uint64_t cas);
    uint64_t getCount() const;
    uint64_t getCas() const;

 private:
    uint64_t _count;
    uint64_t _cas;
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

// ZsetSkipListMetaValue
class ZSlMetaValue {
 public:
    ZSlMetaValue();
    ZSlMetaValue(uint8_t lvl, uint8_t maxLvl, uint32_t count, uint64_t tail);
    ZSlMetaValue(uint8_t lvl, uint8_t maxLvl, uint32_t count, uint64_t tail, uint64_t alloc);
    static Expected<ZSlMetaValue> decode(const std::string&);
    std::string encode() const;
    uint8_t getMaxLevel() const;
    uint8_t getLevel() const;
    uint32_t getCount() const;
    uint64_t getTail() const;
    uint64_t getPosAlloc() const;
    // can not dynamicly change
    static constexpr int8_t MAX_LAYER = 24;
    static constexpr uint32_t MAX_NUM = (1<<31);
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
    ZSlEleValue(uint64_t score, const std::string& subkey);
    static Expected<ZSlEleValue> decode(const std::string&);
    std::string encode() const;
    uint64_t getForward(uint8_t layer) const;
    uint64_t getBackward() const;
    void setForward(uint8_t layer, uint64_t pointer);
    void setBackward(uint64_t pointer);
    uint64_t getScore() const;
    const std::string& getSubKey() const;
    uint32_t getSpan(uint8_t layer) const;
    void setSpan(uint8_t layer, uint32_t span);

 private:
    std::vector<uint64_t> _forward;
    std::vector<uint32_t> _span;
    uint64_t _score;
    uint64_t _backward;
    std::string _subKey;
};

namespace rcd_util {
Expected<uint64_t> getSubKeyCount(const RecordKey& key,
                                  const RecordValue& val);
}  // namespace rcd_util
}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_STORAGE_RECORD_H_
