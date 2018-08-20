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
};

char rt2Char(RecordType t);
RecordType char2Rt(char t);

// ********************* key format ***********************************
// DBID + Type + PK + SK + len(PK) + len(SK) + 8B reserved
// DBID is varint32 in little endian
// Type is a char, 'a' for RT_KV, 'm' for RT_META, 'L' for RT_LIST_META
// 'l' for RT_LIST_ELE
// PK is primarykey, its length is described in len(PK)
// SK is secondarykey, its length is described in len(SK)
// len(PK) and len(SK) are varint32 stored in bigendian, so we can read
// from the end backwards.
// the last 8B are reserved.
// ********************* value format *********************************
// TTL + UserValue
// TTL is a varint64
// ********************************************************************
class Record {
 public:
    using KV = std::pair<std::string, std::string>;
    Record();
    Record(const Record&) = default;
    // we should not rely on default move constructor.
    // refer to the manual, int types have no move constructor
    // so copy constructor is applied, the move-from object will
    // be in a dangling state
    Record(Record&&);
    Record(RecordType type, const std::string& pk,
        const std::string& sk, const std::string& val);
    Record(RecordType type, const std::string& pk,
        const std::string& sk, const std::string& val, uint64_t ttl);
    Record(uint32_t dbid, RecordType type, const std::string& pk,
        const std::string& sk, const std::string& val);
    Record(uint32_t dbid, RecordType type, const std::string& pk,
        const std::string& sk, const std::string& val, uint64_t ttl);
    static Expected<std::unique_ptr<Record>> decode(const std::string& key,
        const std::string& value);
    KV encode() const;

 private:
    uint32_t _dbId;
    RecordType _type;
    std::string _pk;
    std::string _sk;
    uint64_t _reserved;
    uint64_t _ttl;
    std::string _value;
};

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_STORAGE_RECORD_H_
