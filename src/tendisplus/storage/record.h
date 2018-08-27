#ifndef SRC_TENDISPLUS_STORAGE_RECORD_H_
#define SRC_TENDISPLUS_STORAGE_RECORD_H_

#include <string>
#include <utility>
#include <memory>
#include "tendisplus/utils/status.h"

namespace tendisplus {

enum class RecordType {
    RT_INVALID,
    RT_META,
    RT_KV,
    RT_LIST_META,
    RT_LIST_ELE,
    RT_BINLOG,
};

char rt2Char(RecordType t);
RecordType char2Rt(char t);

// ********************* key format ***********************************
// DBID + Type + PK + SK + len(PK) + len(SK) + 1B reserved
// DBID is a varint32 in little endian
// Type is a char, 'a' for RT_KV, 'm' for RT_META, 'L' for RT_LIST_META
// 'l' for RT_LIST_ELE
// PK is primarykey, its length is described in len(PK)
// SK is secondarykey, its length is described in len(SK)
// len(PK) and len(SK) are varint32 stored in bigendian, so we can read
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
    std::string encode() const;
    static Expected<RecordKey> decode(const std::string& key);
    bool operator==(const RecordKey& other) const;

 private:
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
    static Expected<ReplLogKey> decode(const std::string&);
    std::string encode() const;
    bool operator==(const ReplLogKey&) const;
    ReplLogKey& operator=(const ReplLogKey&);
    uint64_t getTxnId() const { return _txnId; }
    uint16_t getLocalId() const { return _localId; }
    ReplFlag getFlag() const { return _flag; }

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
    const ReplLogValue& getReplLogValue() const;
    static Expected<ReplLog> decode(const std::string& key,
            const std::string& value);
    KV encode() const;
    bool operator==(const ReplLog&) const;

 private:
    ReplLogKey _key;
    ReplLogValue _val;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_STORAGE_RECORD_H_
