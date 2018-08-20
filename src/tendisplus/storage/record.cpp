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

Record::Record()
    :_dbId(0),
     _type(RecordType::RT_INVALID),
     _pk(""),
     _sk(""),
     _reserved(0),
     _ttl(0),
     _value("") {
}

Record::Record(Record&& o)
        :_dbId(o._dbId),
         _type(o._type),
         _pk(std::move(o._pk)),
         _sk(std::move(o._sk)),
         _reserved(o._reserved),
         _ttl(o._ttl),
         _value(std::move(o._value)) {
    o._dbId = 0;
    o._type = RecordType::RT_INVALID;
    o._reserved = 0;
    o._ttl = 0;
}

Record::Record(RecordType type, const std::string& pk,
        const std::string& sk, const std::string& val)
    :Record(0, type, pk, sk, val, 0) {
}

Record::Record(RecordType type, const std::string& pk,
        const std::string& sk, const std::string& val, uint64_t ttl)
    :Record(0, type, pk, sk, val, ttl) {
}

Record::Record(uint32_t dbid, RecordType type, const std::string& pk,
        const std::string& sk, const std::string& val, uint64_t ttl)
    :_dbId(dbid),
     _type(type),
     _pk(pk),
     _sk(sk),
     _reserved(0),
     _ttl(ttl),
     _value(val) {
}

Record::KV Record::encode() const {
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
    const uint8_t *p = reinterpret_cast<const uint8_t*>(&_reserved);
    static_assert(sizeof(_reserved) == 8, "invalid _reserved size");
    key.insert(key.end(), p, p + (sizeof(_reserved)));


    // --------value encoding
    // TTL
    std::vector<uint8_t> value;
    value.reserve(128);
    auto ttlBytes = varintEncode(_ttl);

    // Value
    value.insert(value.end(), ttlBytes.begin(), ttlBytes.end());
    value.insert(value.end(), _value.begin(), _value.end());

    return {std::string(reinterpret_cast<const char *>(
                key.data()), key.size()),
            std::string(reinterpret_cast<const char *>(
                value.data()), value.size())};
}

Expected<std::unique_ptr<Record>> Record::decode(const std::string& key,
        const std::string& value) {
    constexpr size_t rsvd = sizeof(uint64_t);
    size_t offset = 0;
    size_t rvsOffset = 0;
    uint64_t pkLen = 0;
    uint64_t skLen = 0;
    uint32_t dbid = 0;
    RecordType type = RecordType::RT_INVALID;
    std::string pk = "";
    std::string sk = "";

    uint64_t ttl = 0;
    std::string rawValue = "";

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

    // value
    const uint8_t *valueCstr = reinterpret_cast<const uint8_t *>(value.c_str());
    expt = varintDecodeFwd(valueCstr, value.size());
    if (!expt.ok()) {
        return {expt.status().code(), expt.status().toString()};
    }
    offset = expt.value().second;
    ttl = expt.value().first;

    // NOTE(deyukong): value must not be empty
    // so we use >= rather than > here
    if (offset >= value.size()) {
        std::stringstream ss;
        ss << "marshaled value content of key" << key;
        return {ErrorCodes::ERR_DECODE, ss.str()};
    }
    rawValue = std::string(value.c_str() + offset, value.size() - offset);

    return std::make_unique<Record>(
        dbid,
        type,
        pk,
        sk,
        rawValue,
        ttl);
}

bool Record::operator==(const Record& other) const {
    return _dbId == other._dbId &&
            _type == other._type &&
            _pk == other._pk &&
            _sk == other._sk &&
            _reserved == other._reserved &&
            _ttl == other._ttl &&
            _value == other._value;
}

}  // namespace tendisplus
