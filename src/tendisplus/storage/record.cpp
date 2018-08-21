#include <type_traits>
#include <utility>
#include <memory>
#include <vector>
#include "glog/logging.h"
#include "tendisplus/storage/varint.h"
#include "tendisplus/storage/record.h"
#include "tendisplus/utils/status.h"

namespace tendisplus {

char rt2Char(RecordType t) {
    switch (t) {
        case RecordType::RT_META:
            return 'M';
        case RecordType::RT_KV:
            return 'a';
        case RecordType::RT_LIST_META:
            return 'L';
        case RecordType::RT_LIST_ELE:
            return 'l';
        default:
            LOG(FATAL) << "invalid recordtype:" << static_cast<uint32_t>(t);
            // never reaches here, void compiler complain
            return 0;
    }
}

RecordType char2Rt(char t) {
    switch (t) {
        case 'M':
            return RecordType::RT_META;
        case 'a':
            return RecordType::RT_KV;
        case 'L':
            return RecordType::RT_LIST_META;
        case 'l':
            return RecordType::RT_LIST_ELE;
        default:
            LOG(FATAL) << "invalid recordchar:" << t;
            // never reaches here, void compiler complain
            return RecordType::RT_INVALID;
    }
}

RecordKey::RecordKey()
    :_dbId(0),
     _type(RecordType::RT_INVALID),
     _pk(""),
     _sk(""),
     _fmtVsn(0) {
}

RecordKey::RecordKey(RecordKey&& o)
        :_dbId(o._dbId),
         _type(o._type),
         _pk(std::move(o._pk)),
         _sk(std::move(o._sk)),
         _fmtVsn(o._fmtVsn) {
    o._dbId = 0;
    o._type = RecordType::RT_INVALID;
    o._fmtVsn = 0;
}

RecordKey::RecordKey(uint32_t dbid, RecordType type,
    const std::string& pk, const std::string& sk)
        :_dbId(dbid),
         _type(type),
         _pk(pk),
         _sk(sk),
         _fmtVsn(0) {
}

RecordKey::RecordKey(RecordType type, const std::string& pk,
    const std::string& sk)
        :RecordKey(0, type, pk, sk) {
}

std::string RecordKey::encode() const {
    std::vector<uint8_t> key;
    key.reserve(128);

    // --------key encoding
    // DBID
    auto dbIdBytes = varintEncode(_dbId);
    key.insert(key.end(), dbIdBytes.begin(), dbIdBytes.end());

    // Type
    key.emplace_back(static_cast<uint8_t>(rt2Char(_type)));

    // PK
    key.insert(key.end(), _pk.begin(), _pk.end());

    // SK
    key.insert(key.end(), _sk.begin(), _sk.end());

    // len(PK)
    auto lenPK = varintEncode(_pk.size());
    key.insert(key.end(), lenPK.rbegin(), lenPK.rend());

    // len(SK)
    auto lenSK = varintEncode(_sk.size());
    key.insert(key.end(), lenSK.rbegin(), lenSK.rend());

    // reserved
    const uint8_t *p = reinterpret_cast<const uint8_t*>(&_fmtVsn);
    static_assert(sizeof(_fmtVsn) == 1, "invalid fmtversion size");
    key.insert(key.end(), p, p + (sizeof(_fmtVsn)));

    return std::string(reinterpret_cast<const char *>(
                key.data()), key.size());
}

Expected<RecordKey> RecordKey::decode(const std::string& key) {
    constexpr size_t rsvd = sizeof(TRSV);
    size_t offset = 0;
    size_t rvsOffset = 0;
    uint64_t pkLen = 0;
    uint64_t skLen = 0;
    uint32_t dbid = 0;
    RecordType type = RecordType::RT_INVALID;
    std::string pk = "";
    std::string sk = "";

    // dbid
    const uint8_t *keyCstr = reinterpret_cast<const uint8_t*>(key.c_str());
    auto expt = varintDecodeFwd(keyCstr + offset, key.size());
    if (!expt.ok()) {
        return {expt.status().code(), expt.status().toString()};
    }
    offset += expt.value().second;
    dbid = expt.value().first;

    // type
    char typec = keyCstr[offset++];
    type = char2Rt(typec);

    // sklen and pklen are stored in the reverse order
    // sklen
    const uint8_t *p = keyCstr + key.size() - rsvd - 1;
    expt = varintDecodeRvs(p, key.size() - rsvd - offset);
    if (!expt.ok()) {
        return {expt.status().code(), expt.status().toString()};
    }
    rvsOffset += expt.value().second;
    skLen = expt.value().first;

    // pklen
    p = keyCstr + key.size() - rsvd - 1 - rvsOffset;
    expt = varintDecodeRvs(p, key.size() - rsvd - offset - rvsOffset);
    if (!expt.ok()) {
        return {expt.status().code(), expt.status().toString()};
    }
    rvsOffset += expt.value().second;
    pkLen = expt.value().first;

    // do a double check
    if (offset + pkLen + skLen + rvsOffset + rsvd != key.size()) {
        // TODO(deyukong): hex the string
        std::stringstream ss;
        ss << "marshaled key content:" << key;
        return {ErrorCodes::ERR_DECODE, ss.str()};
    }

    // pk and sk
    pk = std::string(key.c_str() + offset, pkLen);
    sk = std::string(key.c_str() + offset + pkLen, skLen);

    // dont bother about copies. move-constructor or at least RVO
    // will handle everything.
    return RecordKey(dbid, type, pk, sk);
}

bool RecordKey::operator==(const RecordKey& other) const {
    return _dbId == other._dbId &&
            _type == other._type &&
            _pk == other._pk &&
            _sk == other._sk &&
            _fmtVsn == other._fmtVsn;
}

RecordValue::RecordValue()
    :_ttl(0),
     _value("") {
}

RecordValue::RecordValue(RecordValue&& o)
        :_ttl(o._ttl),
         _value(std::move(o._value)) {
    o._ttl = 0;
}

RecordValue::RecordValue(const std::string& val, uint64_t ttl)
        :_ttl(ttl),
         _value(val) {
}

std::string RecordValue::encode() const {
    // --------value encoding
    // TTL
    std::vector<uint8_t> value;
    value.reserve(128);
    auto ttlBytes = varintEncode(_ttl);

    // Value
    value.insert(value.end(), ttlBytes.begin(), ttlBytes.end());
    value.insert(value.end(), _value.begin(), _value.end());

    return std::string(reinterpret_cast<const char *>(
        value.data()), value.size());
}

Expected<RecordValue> RecordValue::decode(const std::string& value) {
    // value
    const uint8_t *valueCstr = reinterpret_cast<const uint8_t *>(value.c_str());
    auto expt = varintDecodeFwd(valueCstr, value.size());
    if (!expt.ok()) {
        return {expt.status().code(), expt.status().toString()};
    }
    size_t offset = expt.value().second;
    uint64_t ttl = expt.value().first;

    // NOTE(deyukong): value must not be empty
    // so we use >= rather than > here
    if (offset >= value.size()) {
        return {ErrorCodes::ERR_DECODE, "marshaled value content"};
    }
    std::string rawValue = std::string(value.c_str() + offset,
        value.size() - offset);
    return RecordValue(rawValue, ttl);
}

bool RecordValue::operator==(const RecordValue& other) const {
    return _ttl == other._ttl && _value == other._value;
}

Record::Record()
    :_key(RecordKey()),
     _value(RecordValue()) {
}

Record::Record(Record&& o)
    :_key(std::move(o._key)),
     _value(std::move(o._value)) {
}

Record::Record(const RecordKey& key, const RecordValue& value)
    :_key(key),
     _value(value) {
}

Record::Record(RecordKey&& key, RecordValue&& value)
    :_key(std::move(key)),
     _value(std::move(value)) {
}

Record::KV Record::encode() const {
    return {_key.encode(), _value.encode()};
}

Expected<Record> Record::decode(const std::string& key,
        const std::string& value) {
    auto e = RecordKey::decode(key);
    if (!e.ok()) {
        return {e.status().code(), e.status().toString()};
    }
    auto e1 = RecordValue::decode(value);
    if (!e1.ok()) {
        return {e1.status().code(), e1.status().toString()};
    }
    return Record(e.value(), e1.value());
}

bool Record::operator==(const Record& other) const {
    return _key == other._key && _value == other._value;
}

}  // namespace tendisplus
