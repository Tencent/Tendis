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

uint8_t rt2Char(RecordType t) {
    switch (t) {
        case RecordType::RT_META:
            return 'M';
        case RecordType::RT_KV:
            return 'a';
        case RecordType::RT_LIST_META:
            return 'L';
        case RecordType::RT_LIST_ELE:
            return 'l';
        case RecordType::RT_HASH_META:
            return 'H';
        case RecordType::RT_HASH_ELE:
            return 'h';
        case RecordType::RT_SET_META:
            return 'S';
        case RecordType::RT_SET_ELE:
            return 's';
        case RecordType::RT_ZSET_META:
            return 'Z';
        case RecordType::RT_ZSET_H_ELE:
            return 'c';
        case RecordType::RT_ZSET_S_ELE:
            return 'z';
        case RecordType::RT_TTL_INDEX:
            return std::numeric_limits<uint8_t>::max()-1;
        // it's convinent (for seek) to have BINLOG to pos
        // at the rightmost of a lsmtree
        // NOTE(deyukong): DO NOT change RT_BINLOG's char represent
        // the underlying cursor iteration relys on it to be at
        // the right most part.
        case RecordType::RT_BINLOG:
            return std::numeric_limits<uint8_t>::max();
        default:
            LOG(FATAL) << "invalid recordtype:" << static_cast<uint32_t>(t);
            // never reaches here, void compiler complain
            return 0;
    }
}

RecordType char2Rt(uint8_t t) {
    switch (t) {
        case 'M':
            return RecordType::RT_META;
        case 'a':
            return RecordType::RT_KV;
        case 'L':
            return RecordType::RT_LIST_META;
        case 'l':
            return RecordType::RT_LIST_ELE;
        case 'H':
            return RecordType::RT_HASH_META;
        case 'h':
            return RecordType::RT_HASH_ELE;
        case 'S':
            return RecordType::RT_SET_META;
        case 's':
            return RecordType::RT_SET_ELE;
        case 'Z':
            return RecordType::RT_ZSET_META;
        case 'z':
            return RecordType::RT_ZSET_S_ELE;
        case 'c':
            return RecordType::RT_ZSET_H_ELE;
        case std::numeric_limits<uint8_t>::max()-1:
            return RecordType::RT_TTL_INDEX;
        case std::numeric_limits<uint8_t>::max():
            return RecordType::RT_BINLOG;
        default:
            LOG(FATAL) << "invalid rcdchr:" << static_cast<uint32_t>(t);
            // never reaches here, void compiler complain
            return RecordType::RT_INVALID;
    }
}

RecordKey::RecordKey()
    :_chunkId(0),
     _dbId(0),
     _type(RecordType::RT_INVALID),
     _pk(""),
     _sk(""),
     _fmtVsn(0) {
}

RecordKey::RecordKey(RecordKey&& o)
        :_chunkId(o._chunkId),
         _dbId(o._dbId),
         _type(o._type),
         _pk(std::move(o._pk)),
         _sk(std::move(o._sk)),
         _fmtVsn(o._fmtVsn) {
    o._chunkId = 0;
    o._dbId = 0;
    o._type = RecordType::RT_INVALID;
    o._fmtVsn = 0;
}

RecordKey::RecordKey(uint32_t chunkId, uint32_t dbid, RecordType type,
    const std::string& pk, const std::string& sk)
        :_chunkId(chunkId),
         _dbId(dbid),
         _type(type),
         _pk(pk),
         _sk(sk),
         _fmtVsn(0) {
}

RecordKey::RecordKey(uint32_t chunkId, uint32_t dbid,
    RecordType type, std::string&& pk, std::string&& sk)
    :_chunkId(chunkId),
     _dbId(dbid),
     _type(type),
     _pk(std::move(pk)),
     _sk(std::move(sk)),
     _fmtVsn(0) {
}

void RecordKey::encodePrefixPk(std::vector<uint8_t>* arr) const {
    // --------key encoding
    // CHUNKID
    for (size_t i = 0; i < sizeof(_chunkId); ++i) {
        arr->emplace_back((_chunkId>>((sizeof(_chunkId)-i-1)*8))&0xff);
    }

    // DBID
    for (size_t i = 0; i < sizeof(_dbId); ++i) {
        arr->emplace_back((_dbId>>((sizeof(_dbId)-i-1)*8))&0xff);
    }

    // Type
    arr->emplace_back(rt2Char(_type));

    // PK
    std::string hexPk = hexlify(_pk);
    arr->insert(arr->end(), hexPk.begin(), hexPk.end());

    // NOTE(deyukong): 0 never exists in hex string.
    // a padding 0 avoids prefixes intersect with
    // each other in physical space
    arr->push_back(0);
}

uint32_t RecordKey::getChunkId() const {
    return _chunkId;
}

uint32_t RecordKey::getDbId() const {
    return _dbId;
}

std::string RecordKey::prefixPk() const {
    std::vector<uint8_t> key;
    key.reserve(128);
    encodePrefixPk(&key);
    return std::string(reinterpret_cast<const char *>(
                key.data()), key.size());
}

/* NOTE(deyukong): after chunkid prefix, the dbid-type prefix is
// meaningless
std::string RecordKey::prefixDbidType() const {
    std::vector<uint8_t> key;
    key.reserve(128);

    // --------key encoding
    // DBID
    for (size_t i = 0; i < sizeof(_dbId); ++i) {
        key.emplace_back((_dbId>>((sizeof(_dbId)-i-1)*8))&0xff);
    }

    // Type
    key.emplace_back(rt2Char(_type));
    return std::string(reinterpret_cast<const char *>(
                key.data()), key.size());
}
*/

const std::string& RecordKey::prefixReplLog() {
    static std::string s = []() {
        std::string result;
        static_assert(ReplLogKey::DBID == 0XFFFFFFFFU,
                        "invalid ReplLogKey::DBID");
        static_assert(ReplLogKey::CHUNKID == 0XFFFFFFFFU,
                        "invalid ReplLogKey::CHUNKID");
        result.push_back(0xFF);
        result.push_back(0xFF);
        result.push_back(0xFF);
        result.push_back(0xFF);
        result.push_back(0xFF);
        result.push_back(0xFF);
        result.push_back(0xFF);
        result.push_back(0xFF);
        result.push_back(rt2Char(RecordType::RT_BINLOG));
        return result;
    }();
    return s;
}

const std::string& RecordKey::prefixTTLIndex() {
    static std::string s = []() {
      std::string result;

      static_assert(TTLIndex::DBID == 0XFFFFFFFFU,
                    "invalid TTLIndex::DBID");
      static_assert(TTLIndex::CHUNKID == 0XFFFFFFFEU,
                    "invalid TTLIndex::CHUNKID");
      result.push_back(0xFF);
      result.push_back(0xFF);
      result.push_back(0xFF);
      result.push_back(0xFE);
      result.push_back(0xFF);
      result.push_back(0xFF);
      result.push_back(0xFF);
      result.push_back(0xFF);
      result.push_back(rt2Char(RecordType::RT_TTL_INDEX));
      return result;
    }();

    return s;
}

RecordType RecordKey::getRecordType() const {
    return _type;
}

std::string RecordKey::encode() const {
    std::vector<uint8_t> key;
    key.reserve(128);

    encodePrefixPk(&key);

    // SK
    key.insert(key.end(), _sk.begin(), _sk.end());

    // len(PK)
    auto lenPK = varintEncode(_pk.size());
    // NOTE(vinchen): big endian
    key.insert(key.end(), lenPK.rbegin(), lenPK.rend());

    // reserved
    const uint8_t *p = reinterpret_cast<const uint8_t*>(&_fmtVsn);
    static_assert(sizeof(_fmtVsn) == 1, "invalid fmtversion size");
    key.insert(key.end(), p, p + (sizeof(_fmtVsn)));

    return std::string(reinterpret_cast<const char *>(
                key.data()), key.size());
}

const std::string& RecordKey::getPrimaryKey() const {
    return _pk;
}

const std::string& RecordKey::getSecondaryKey() const {
    return _sk;
}

#define INVALID_RK_OFFSET ((size_t)-1)

static size_t recordKeyDecodeFixPrefix(const uint8_t* keyCstr, size_t size,
        uint32_t* chunkidOut, uint32_t* dbidOut, RecordType* typeOut) {
    size_t offset = 0;
    uint32_t chunkid = 0;
    uint32_t dbid = 0;
    uint8_t typec;
    RecordType type = RecordType::RT_INVALID;

    if (size < sizeof(chunkid) + sizeof(dbid) + sizeof(uint8_t)
        + sizeof(RecordKey::TRSV)) {
        // invalid key
        return INVALID_RK_OFFSET;
    }

    // chunkid
    for (size_t i = 0; i < sizeof(chunkid); i++) {
        chunkid = (chunkid << 8) | keyCstr[i + offset];
    }
    offset += sizeof(chunkid);

    // dbid
    for (size_t i = 0; i < sizeof(dbid); i++) {
        dbid = (dbid << 8) | keyCstr[i + offset];
    }
    offset += sizeof(dbid);

    // type
    typec = keyCstr[offset++];
    type = char2Rt(typec);

    *chunkidOut = chunkid;
    *dbidOut = dbid;
    *typeOut = type;

    return offset;
}

Expected<RecordKey> RecordKey::decode(const std::string& key) {
    constexpr size_t rsvd = sizeof(TRSV);
    size_t offset = 0;
    size_t rvsOffset = 0;
    uint64_t pkLen = 0;
    uint64_t skLen = 0;
    uint32_t chunkid = 0;
    uint32_t dbid = 0;
    RecordType type = RecordType::RT_INVALID;
    std::string pk = "";
    std::string sk = "";

    const uint8_t *keyCstr = reinterpret_cast<const uint8_t*>(key.c_str());
    offset = recordKeyDecodeFixPrefix(keyCstr, key.size(),
            &chunkid, &dbid, &type);
    if (offset == INVALID_RK_OFFSET) {
        return {ErrorCodes::ERR_DECODE, "invalid recordkey"};
    }

    // pklen is stored in the reverse order
    // pklen
    const uint8_t *p = keyCstr + key.size() - rsvd - 1;
    auto expt = varintDecodeRvs(p, key.size() - rsvd - offset);
    if (!expt.ok()) {
        return expt.status();
    }
    rvsOffset += expt.value().second;
    pkLen = expt.value().first;

    // 2*pklen: pk is hexed
    // here -1 for the padding 0 after pk
    if (key.size() < offset + rsvd + rvsOffset + 2*pkLen + 1) {
        return {ErrorCodes::ERR_DECODE, "invalid sk len"};
    }
    skLen = key.size() - offset - rsvd - rvsOffset - 2*pkLen - 1;

    // pk and sk
    pk = std::string(key.c_str() + offset, 2*pkLen);

    Expected<std::string> unhexPk = unhexlify(pk);
    if (!unhexPk.ok()) {
        return unhexPk.status();
    }

    sk = std::string(key.c_str() + offset + 2*pkLen + 1, skLen);

    // dont bother about copies. move-constructor or at least RVO
    // will handle everything.
    return RecordKey(chunkid, dbid, type, unhexPk.value(), sk);
}

RecordType RecordKey::getRecordTypeRaw(const char* key, size_t size) {
    size_t offset = 0;
    uint32_t chunkid = 0;
    uint32_t dbid = 0;
    RecordType type = RecordType::RT_INVALID;

    offset = recordKeyDecodeFixPrefix(reinterpret_cast<const uint8_t*>(key),
        size, &chunkid, &dbid, &type);

    return type;
}

bool RecordKey::operator==(const RecordKey& other) const {
    return  _chunkId == other._chunkId &&
            _dbId == other._dbId &&
            _type == other._type &&
            _pk == other._pk &&
            _sk == other._sk &&
            _fmtVsn == other._fmtVsn;
}

RecordValue::RecordValue()
    :_cas(-1),
     _ttl(0),
     _value("") {
}

RecordValue::RecordValue(RecordValue&& o)
        :_cas(o._cas),
         _ttl(o._ttl),
         _value(std::move(o._value)) {
    o._ttl = 0;
    o._cas = 0;
}

RecordValue::RecordValue(const std::string& val, uint64_t ttl, int64_t cas)
        :_cas(cas),
         _ttl(ttl),
         _value(val) {
}

RecordValue::RecordValue(std::string&& val, uint64_t ttl, int64_t cas)
        :_cas(cas),
         _ttl(ttl),
         _value(std::move(val)) {
}

std::string RecordValue::encode() const {
    std::vector<uint8_t> value;
    value.reserve(128);

    // CAS
    // NOTE(vinchen): cas should initialize -1, not zero.
    // And it should be store as (cas + 1) in the kvstore
    // to improve storage efficiency
    auto casBytes = varintEncode(_cas + 1);
    value.insert(value.end(), casBytes.begin(), casBytes.end());

    // TTL
    auto ttlBytes = varintEncode(_ttl);
    value.insert(value.end(), ttlBytes.begin(), ttlBytes.end());

    // Value
    if (_value.size() > 0) {
        value.insert(value.end(), _value.begin(), _value.end());
    }
    return std::string(reinterpret_cast<const char *>(
        value.data()), value.size());
}

Expected<RecordValue> RecordValue::decode(const std::string& value) {
    const uint8_t *valueCstr = reinterpret_cast<const uint8_t *>(value.c_str());
    size_t offset = 0;
    auto expt = varintDecodeFwd(valueCstr, value.size());
    if (!expt.ok()) {
        return expt.status();
    }
    offset += expt.value().second;
    // NOTE(vinchen): cas should initialize -1, not zero.
    // And it should be store as (cas + 1) in the kvstore
    // to improve storage efficiency
    int64_t cas = expt.value().first - 1;

    expt = varintDecodeFwd(valueCstr+offset, value.size()-offset);
    if (!expt.ok()) {
        return expt.status();
    }
    offset += expt.value().second;
    uint64_t ttl = expt.value().first;

    if (offset > value.size()) {
        std::stringstream ss;
        ss << "marshaled value content, offset:" << offset << ",ttl:" << ttl;
        return {ErrorCodes::ERR_DECODE, ss.str()};
    }
    std::string rawValue;
    if (value.size() > offset) {
        rawValue = std::string(value.c_str() + offset,
            value.size() - offset);
    }
    return RecordValue(std::move(rawValue), ttl, cas);
}

uint64_t RecordValue::getTtlRaw(const char* value, size_t size) {
    const uint8_t *valueCstr = reinterpret_cast<const uint8_t *>(value);
    size_t offset = 0;
    auto expt = varintDecodeFwd(valueCstr, size);
    if (!expt.ok()) {
        return 0;
    }
    offset += expt.value().second;

    expt = varintDecodeFwd(valueCstr + offset, size - offset);
    if (!expt.ok()) {
        return 0;
    }
    offset += expt.value().second;
    uint64_t ttl = expt.value().first;;

    return ttl;
}

const std::string& RecordValue::getValue() const {
    return _value;
}

uint64_t RecordValue::getTtl() const {
    return _ttl;
}

void RecordValue::setTtl(uint64_t ttl) {
    _ttl = ttl;
}

int64_t RecordValue::getCas() const {
    return _cas;
}

void RecordValue::setCas(int64_t cas) {
    _cas = cas;
}

bool RecordValue::operator==(const RecordValue& other) const {
    return _ttl == other._ttl && _value == other._value && _cas == other._cas;
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

const RecordKey& Record::getRecordKey() const {
    return _key;
}

const RecordValue& Record::getRecordValue() const {
    return _value;
}

Record::KV Record::encode() const {
    return {_key.encode(), _value.encode()};
}

Expected<Record> Record::decode(const std::string& key,
        const std::string& value) {
    auto e = RecordKey::decode(key);
    if (!e.ok()) {
        return e.status();
    }
    auto e1 = RecordValue::decode(value);
    if (!e1.ok()) {
        return e1.status();
    }
    return Record(e.value(), e1.value());
}

bool Record::operator==(const Record& other) const {
    return _key == other._key && _value == other._value;
}

ReplLogKey::ReplLogKey()
        :_txnId(0),
         _localId(0),
         _flag(ReplFlag::REPL_GROUP_MID),
         _timestamp(0),
         _reserved(0) {
}

ReplLogKey::ReplLogKey(ReplLogKey&& o)
        :_txnId(o._txnId),
         _localId(o._localId),
         _flag(o._flag),
         _timestamp(o._timestamp),
         _reserved(o._reserved) {
    o._txnId = 0;
    o._localId = 0;
    o._flag = ReplFlag::REPL_GROUP_MID;
    o._timestamp = 0;
    o._reserved = 0;
}

ReplLogKey::ReplLogKey(uint64_t txnid, uint16_t localid, ReplFlag flag,
        uint32_t timestamp, uint8_t reserved)
    :_txnId(txnid),
     _localId(localid),
     _flag(flag),
     _timestamp(timestamp),
     _reserved(reserved) {
}

Expected<ReplLogKey> ReplLogKey::decode(const RecordKey& rk) {
    const std::string& key = rk.getPrimaryKey();
    const uint8_t *keyCstr = reinterpret_cast<const uint8_t*>(key.c_str());
    if (key.size() != sizeof(_txnId) + sizeof(_localId) + sizeof(_flag)
            + sizeof(_timestamp) + sizeof(_reserved)) {
        return {ErrorCodes::ERR_DECODE, "invalid keylen"};
    }

    uint64_t txnid = 0;
    uint16_t localid = 0;
    ReplFlag flag = ReplFlag::REPL_GROUP_MID;
    uint32_t timestamp = 0;
    uint8_t reserved = 0;
    size_t offset = 0;

    // txnid
    for (size_t i = 0; i < sizeof(txnid); i++) {
        txnid = (txnid << 8)|keyCstr[i];
    }
    offset += sizeof(txnid);

    // localid
    localid = static_cast<uint16_t>((keyCstr[offset] << 8)|keyCstr[offset+1]);
    offset += sizeof(localid);

    // flag
    flag = static_cast<ReplFlag>((keyCstr[offset] << 8)|keyCstr[offset+1]);
    offset += sizeof(flag);

    // timestamp
    for (size_t i = 0; i < sizeof(_timestamp); i++) {
        timestamp = (timestamp << 8)|keyCstr[offset+i];
    }
    offset += sizeof(timestamp);

    // reserved
    reserved = keyCstr[key.size()-1];

    return ReplLogKey(txnid, localid, flag, timestamp, reserved);
}

Expected<ReplLogKey> ReplLogKey::decode(const std::string& rawKey) {
    Expected<RecordKey> rk = RecordKey::decode(rawKey);
    if (!rk.ok()) {
        return rk.status();
    }
    return decode(rk.value());
}

std::string ReplLogKey::prefix(uint64_t commitId) {
    // NOTE(deyukong): currently commitId is defined as uint64,
    // we do a compiletime assert here. change the logic if commitId is
    // redefined to different structure
    static_assert(sizeof(commitId) == 8,
        "commitId size not 8, reimpl the logic");
    std::string p = RecordKey::prefixReplLog();
    std::string p1;

    for (size_t i = 0; i < sizeof(commitId); i++) {
        p1.push_back((commitId>>((sizeof(commitId)-i-1)*8))&0xff);
    }
    return p + hexlify(p1);
}

std::string ReplLogKey::encode() const {
    std::vector<uint8_t> key;
    key.reserve(128);
    for (size_t i = 0; i < sizeof(_txnId); i++) {
        key.emplace_back((_txnId>>((sizeof(_txnId)-i-1)*8))&0xff);
    }
    key.emplace_back(_localId>>8);
    key.emplace_back(_localId&0xff);
    key.emplace_back(static_cast<uint16_t>(_flag)>>8);
    key.emplace_back(static_cast<uint16_t>(_flag)&0xff);
    for (size_t i = 0; i < sizeof(_timestamp); i++) {
        key.emplace_back((_timestamp>>((sizeof(_timestamp)-i-1)*8))&0xff);
    }
    key.emplace_back(_reserved);
    std::string partial(reinterpret_cast<const char *>(key.data()), key.size());
    RecordKey tmpRk(ReplLogKey::CHUNKID,
                    ReplLogKey::DBID,
                    RecordType::RT_BINLOG,
                    std::move(partial), "");
    return tmpRk.encode();
}

bool ReplLogKey::operator==(const ReplLogKey& o) const {
    return _txnId == o._txnId &&
            _localId == o._localId &&
            _flag == o._flag &&
            _timestamp == o._timestamp &&
            _reserved == o._reserved;
}

ReplLogKey& ReplLogKey::operator=(const ReplLogKey& o) {
    if (this == &o) {
        return *this;
    }
    _txnId = o._txnId;
    _localId = o._localId;
    _flag = o._flag;
    _timestamp = o._timestamp;
    _reserved = o._reserved;
    return *this;
}

ReplLogValue::ReplLogValue()
        :_op(ReplOp::REPL_OP_NONE),
         _key(""),
         _val("") {
}

ReplLogValue::ReplLogValue(ReplLogValue&& o)
        :_op(o._op),
         _key(std::move(o._key)),
         _val(std::move(o._val)) {
    o._op = ReplOp::REPL_OP_NONE;
}

ReplLogValue::ReplLogValue(ReplOp op, const std::string& key,
        const std::string& val)
    :_op(op),
     _key(key),
     _val(val) {
}

Expected<ReplLogValue> ReplLogValue::decode(const RecordValue& rawVal) {
    const std::string& o = rawVal.getValue();
    const uint8_t *valCstr = reinterpret_cast<const uint8_t*>(o.c_str());
    if (o.size() <= sizeof(_op)) {
        return {ErrorCodes::ERR_DECODE, "invalid replvalue len"};
    }
    size_t offset = 0;
    uint8_t op = valCstr[0];
    std::string key;
    std::string val;
    offset += sizeof(uint8_t);

    auto expt = varintDecodeFwd(valCstr + offset, o.size() - offset);
    if (!expt.ok()) {
        return expt.status();
    }
    offset += expt.value().second;
    if (offset + expt.value().first >= o.size()) {
        return {ErrorCodes::ERR_DECODE, "invalid replvalue len"};
    }

    key = std::string(o.c_str() + offset, expt.value().first);
    offset += expt.value().first;

    expt = varintDecodeFwd(valCstr + offset, o.size() - offset);
    if (!expt.ok()) {
        return expt.status();
    }
    offset += expt.value().second;
    if (offset + expt.value().first != o.size()) {
        return {ErrorCodes::ERR_DECODE, "invalid replvalue len"};
    }
    val = std::string(o.c_str() + offset, expt.value().first);
    return ReplLogValue(static_cast<ReplOp>(op), key, val);
}

Expected<ReplLogValue> ReplLogValue::decode(const std::string& rawVal) {
    Expected<RecordValue> exptVal = RecordValue::decode(rawVal);
    if (!exptVal.ok()) {
        return exptVal.status();
    }
    return decode(exptVal.value());
}

const std::string& ReplLogValue::getOpKey() const {
    return _key;
}

const std::string& ReplLogValue::getOpValue() const {
    return _val;
}

ReplOp ReplLogValue::getOp() const {
    return _op;
}

std::string ReplLogValue::encode() const {
    std::vector<uint8_t> val;
    val.reserve(128);
    val.emplace_back(static_cast<uint8_t>(_op));
    auto keyBytes = varintEncode(_key.size());
    val.insert(val.end(), keyBytes.begin(), keyBytes.end());
    val.insert(val.end(), _key.begin(), _key.end());
    auto valBytes = varintEncode(_val.size());
    val.insert(val.end(), valBytes.begin(), valBytes.end());
    val.insert(val.end(), _val.begin(), _val.end());
    std::string partial(reinterpret_cast<const char *>(val.data()), val.size());
    RecordValue tmpRv(std::move(partial));
    return tmpRv.encode();
}

bool ReplLogValue::operator==(const ReplLogValue& o) const {
    return _op == o._op &&
            _key == o._key &&
            _val == o._val;
}

ReplLog::ReplLog()
        :_key(ReplLogKey()),
         _val(ReplLogValue()) {
}

ReplLog::ReplLog(ReplLog&& o)
        :_key(std::move(o._key)),
         _val(std::move(o._val)) {
}

ReplLog::ReplLog(const ReplLogKey& key, const ReplLogValue& value)
        :_key(key),
         _val(value) {
}

ReplLog::ReplLog(ReplLogKey&& key, ReplLogValue&& val)
        :_key(std::move(key)),
         _val(std::move(val)) {
}

ReplLogKey& ReplLog::getReplLogKey() {
    return _key;
}

const ReplLogKey& ReplLog::getReplLogKey() const {
    return _key;
}

const ReplLogValue& ReplLog::getReplLogValue() const {
    return _val;
}

Expected<ReplLog> ReplLog::decode(const std::string& key,
        const std::string& val) {
    auto e = ReplLogKey::decode(key);
    if (!e.ok()) {
        return e.status();
    }
    auto e1 = ReplLogValue::decode(val);
    if (!e1.ok()) {
        return e1.status();
    }
    return ReplLog(std::move(e.value()), std::move(e1.value()));
}

ReplLog::KV ReplLog::encode() const {
    return {_key.encode(), _val.encode()};
}

bool ReplLog::operator==(const ReplLog& o) const {
    return _key == o._key && _val == o._val;
}

HashMetaValue::HashMetaValue()
    :HashMetaValue(0) {
}

HashMetaValue::HashMetaValue(uint64_t count)
    :_count(count) {
}

HashMetaValue::HashMetaValue(HashMetaValue&& o)
        :_count(o._count) {
    o._count = 0;
}

std::string HashMetaValue::encode() const {
    std::vector<uint8_t> value;
    value.reserve(128);
    auto countBytes = varintEncode(_count);
    value.insert(value.end(), countBytes.begin(), countBytes.end());
    return std::string(reinterpret_cast<const char *>(
                value.data()), value.size());
}

Expected<HashMetaValue> HashMetaValue::decode(const std::string& val) {
    const uint8_t *valCstr = reinterpret_cast<const uint8_t*>(val.c_str());
    size_t offset = 0;
    uint64_t count = 0;
    auto expt = varintDecodeFwd(valCstr + offset, val.size());
    if (!expt.ok()) {
        return expt.status();
    }
    offset += expt.value().second;
    count = expt.value().first;

    return HashMetaValue(count);
}

HashMetaValue& HashMetaValue::operator=(HashMetaValue&& o) {
    if (&o == this) {
        return *this;
    }
    _count = o._count;
    o._count = 0;
    return *this;
}

void HashMetaValue::setCount(uint64_t count) {
    _count = count;
}

uint64_t HashMetaValue::getCount() const {
    return _count;
}

ListMetaValue::ListMetaValue(uint64_t head, uint64_t tail)
        :_head(head),
         _tail(tail) {
}

ListMetaValue::ListMetaValue(ListMetaValue&& v)
        :_head(v._head),
         _tail(v._tail) {
    v._head = 0;
    v._tail = 0;
}

std::string ListMetaValue::encode() const {
    std::vector<uint8_t> value;
    value.reserve(128);
    auto headBytes = varintEncode(_head);
    value.insert(value.end(), headBytes.begin(), headBytes.end());
    auto tailBytes = varintEncode(_tail);
    value.insert(value.end(), tailBytes.begin(), tailBytes.end());
    return std::string(reinterpret_cast<const char *>(
                value.data()), value.size());
}

Expected<ListMetaValue> ListMetaValue::decode(const std::string& val) {
    const uint8_t *valCstr = reinterpret_cast<const uint8_t*>(val.c_str());
    size_t offset = 0;
    uint64_t head = 0;
    uint64_t tail = 0;
    auto expt = varintDecodeFwd(valCstr + offset, val.size());
    if (!expt.ok()) {
        return expt.status();
    }
    offset += expt.value().second;
    head = expt.value().first;

    expt = varintDecodeFwd(valCstr + offset, val.size() - offset);
    if (!expt.ok()) {
        return expt.status();
    }
    offset += expt.value().second;
    tail = expt.value().first;
    return ListMetaValue(head, tail);
}

ListMetaValue& ListMetaValue::operator=(ListMetaValue&& o) {
    if (&o == this) {
        return *this;
    }
    _head = o._head;
    _tail = o._tail;
    o._head = 0;
    o._tail = 0;
    return *this;
}

void ListMetaValue::setHead(uint64_t head) {
    _head = head;
}

void ListMetaValue::setTail(uint64_t tail) {
    _tail = tail;
}

uint64_t ListMetaValue::getHead() const {
    return _head;
}

uint64_t ListMetaValue::getTail() const {
    return _tail;
}

SetMetaValue::SetMetaValue()
    :_count(0) {
}

SetMetaValue::SetMetaValue(uint64_t count)
    :_count(count) {
}

Expected<SetMetaValue> SetMetaValue::decode(const std::string& val) {
    const uint8_t *valCstr = reinterpret_cast<const uint8_t*>(val.c_str());
    size_t offset = 0;
    auto expt = varintDecodeFwd(valCstr + offset, val.size());
    if (!expt.ok()) {
        return expt.status();
    }
    offset += expt.value().second;
    uint64_t count = expt.value().first;
    return SetMetaValue(count);
}

std::string SetMetaValue::encode() const {
    std::vector<uint8_t> value;
    value.reserve(8);
    auto countBytes = varintEncode(_count);
    value.insert(value.end(), countBytes.begin(), countBytes.end());
    return std::string(reinterpret_cast<const char *>(
                value.data()), value.size());
}

void SetMetaValue::setCount(uint64_t count) {
    _count = count;
}

uint64_t SetMetaValue::getCount() const {
    return _count;
}

uint32_t ZSlMetaValue::HEAD_ID = 1;

ZSlMetaValue::ZSlMetaValue()
        :ZSlMetaValue(0, 0, 0, 0) {
}

ZSlMetaValue::ZSlMetaValue(uint8_t lvl,
                           uint8_t maxLvl,
                           uint32_t count,
                           uint64_t tail)
        :_level(lvl),
         _maxLevel(maxLvl),
         _count(count),
         _tail(tail),
         _posAlloc(ZSlMetaValue::MIN_POS) {
}

ZSlMetaValue::ZSlMetaValue(uint8_t lvl,
                           uint8_t maxLvl,
                           uint32_t count,
                           uint64_t tail,
                           uint64_t alloc)
        :_level(lvl),
         _maxLevel(maxLvl),
         _count(count),
         _tail(tail),
         _posAlloc(alloc) {
}

std::string ZSlMetaValue::encode() const {
    std::vector<uint8_t> value;
    value.reserve(128);

    auto bytes = varintEncode(_level);
    value.insert(value.end(), bytes.begin(), bytes.end());

    bytes = varintEncode(_maxLevel);
    value.insert(value.end(), bytes.begin(), bytes.end());

    bytes = varintEncode(_count);
    value.insert(value.end(), bytes.begin(), bytes.end());

    bytes = varintEncode(_tail);
    value.insert(value.end(), bytes.begin(), bytes.end());

    bytes = varintEncode(_posAlloc);
    value.insert(value.end(), bytes.begin(), bytes.end());

    return std::string(reinterpret_cast<const char *>(
                value.data()), value.size());
}

Expected<ZSlMetaValue> ZSlMetaValue::decode(const std::string& val) {
    const uint8_t *keyCstr = reinterpret_cast<const uint8_t*>(val.c_str());
    size_t offset = 0;
    ZSlMetaValue result;

    // _level
    auto expt = varintDecodeFwd(keyCstr + offset, val.size()-offset);
    if (!expt.ok()) {
        return expt.status();
    }
    offset += expt.value().second;
    result._level = expt.value().first;

    // _maxLevel
    expt = varintDecodeFwd(keyCstr + offset, val.size()-offset);
    if (!expt.ok()) {
        return expt.status();
    }
    offset += expt.value().second;
    result._maxLevel = expt.value().first;

    // _count
    expt = varintDecodeFwd(keyCstr + offset, val.size()-offset);
    if (!expt.ok()) {
        return expt.status();
    }
    offset += expt.value().second;
    result._count = expt.value().first;

    // _tail
    expt = varintDecodeFwd(keyCstr + offset, val.size()-offset);
    if (!expt.ok()) {
        return expt.status();
    }
    offset += expt.value().second;
    result._tail = expt.value().first;

    // _posAlloc
    expt = varintDecodeFwd(keyCstr + offset, val.size()-offset);
    if (!expt.ok()) {
        return expt.status();
    }
    offset += expt.value().second;
    result._posAlloc = expt.value().first;

    return result;
}

uint8_t ZSlMetaValue::getMaxLevel() const {
    return _maxLevel;
}

uint8_t ZSlMetaValue::getLevel() const {
    return _level;
}

uint32_t ZSlMetaValue::getCount() const {
    return _count;
}

uint64_t ZSlMetaValue::getTail() const {
    return _tail;
}

uint64_t ZSlMetaValue::getPosAlloc() const {
    return _posAlloc;
}

/*
ZslEleSubKey::ZslEleSubKey()
    :ZslEleSubKey(0, "") {
}

ZslEleSubKey::ZslEleSubKey(uint64_t score, const std::string& subkey)
    :ZslEleSubKey(score, subkey) {
}

std::string ZslEleSubKey::encode() const {
    std::vector<uint8_t> key;
    key.reserve(128);
    const uint8_t *scoreBuf = reinterpret_cast<const uint8_t*>(&_score);
    for (size_t i = 0; i < sizeof(_score); i++) {
        key.emplace_back(scoreBuf[sizeof(_score)-1-i]);
    }
    key.insert(key.end(), _subKey.begin(), _subKey.end());
    return std::string(reinterpret_cast<const char *>(
                key.data()), key.size());
}

Expected<ZslEleSubKey> ZslEleSubKey::decode(const std::string& key) {
    uint64_t score = 0;
    std::string subKey;

    if (key.size() <= sizeof(score)) {
        return {ErrorCodes::ERR_DECODE, "ZslEleSubKey bad size"};
    }
    const uint8_t *keyCstr = reinterpret_cast<const uint8_t*>(key.c_str());
    size_t offset = 0;

    for (size_t i = 0; i < sizeof(score); i++) {
        score = (score << 8)|keyCstr[i];
    }
    offset += sizeof(score);
    subKey = std::string(key.c_str()+offset, key.size()-offset);
    return ZslEleSubKey(score, subKey);
}
*/

ZSlEleValue::ZSlEleValue()
        :ZSlEleValue(0, "") {
}

ZSlEleValue::ZSlEleValue(double score, const std::string& subkey)
         :_score(score),
          _backward(0),
          _subKey(subkey) {
    _forward.resize(ZSlMetaValue::MAX_LAYER+1);
    _span.resize(ZSlMetaValue::MAX_LAYER+1);
    for (size_t i = 0; i < _forward.size(); ++i) {
        _forward[i] = 0;
    }
    for (size_t i = 0; i < _span.size(); ++i) {
        _span[i] = 0;
    }
}

uint64_t ZSlEleValue::getBackward() const {
    return _backward;
}

void ZSlEleValue::setBackward(uint64_t pointer) {
    _changed = true;
    _backward = pointer;
}

uint64_t ZSlEleValue::getForward(uint8_t layer) const {
    return _forward[layer];
}

void ZSlEleValue::setForward(uint8_t layer, uint64_t pointer) {
    _changed = true;
    _forward[layer] = pointer;
}

uint32_t ZSlEleValue::getSpan(uint8_t layer) const {
    return _span[layer];
}

void ZSlEleValue::setSpan(uint8_t layer, uint32_t span) {
    _changed = true;
    _span[layer] = span;
}

double ZSlEleValue::getScore() const {
    return _score;
}

const std::string& ZSlEleValue::getSubKey() const {
    return _subKey;
}

std::string ZSlEleValue::encode() const {
    std::vector<uint8_t> value;
    value.reserve(128);
    for (auto& v : _forward) {
        auto bytes = varintEncode(v);
        value.insert(value.end(), bytes.begin(), bytes.end());
    }
    for (auto& v : _span) {
        auto bytes = varintEncode(v);
        value.insert(value.end(), bytes.begin(), bytes.end());
    }

    auto bytes = doubleEncode(_score);
    value.insert(value.end(), bytes.begin(), bytes.end());

    bytes = varintEncode(_backward);
    value.insert(value.end(), bytes.begin(), bytes.end());

    bytes = varintEncode(_subKey.size());
    value.insert(value.end(), bytes.begin(), bytes.end());
    value.insert(value.end(), _subKey.begin(), _subKey.end());

    return std::string(reinterpret_cast<const char *>(
                value.data()), value.size());
}

Expected<ZSlEleValue> ZSlEleValue::decode(const std::string& val) {
    const uint8_t *keyCstr = reinterpret_cast<const uint8_t*>(val.c_str());
    size_t offset = 0;
    ZSlEleValue result;

    // forwardlist
    for (uint32_t i = 0; i <= ZSlMetaValue::MAX_LAYER; ++i) {
        auto expt = varintDecodeFwd(keyCstr + offset, val.size()-offset);
        if (!expt.ok()) {
            return expt.status();
        }
        offset += expt.value().second;
        result._forward[i] = expt.value().first;
    }

    // spanlist
    for (uint32_t i = 0; i <= ZSlMetaValue::MAX_LAYER; ++i) {
        auto expt = varintDecodeFwd(keyCstr + offset, val.size()-offset);
        if (!expt.ok()) {
            return expt.status();
        }
        offset += expt.value().second;
        result._span[i] = expt.value().first;
    }

    // score
    auto d = doubleDecode(keyCstr + offset, val.size()-offset);
    if (!d.ok()) {
        return d.status();
    }
    offset += sizeof(double);
    result._score = d.value();

    // backward
    auto expt = varintDecodeFwd(keyCstr + offset, val.size()-offset);
    if (!expt.ok()) {
        return expt.status();
    }
    offset += expt.value().second;
    result._backward = expt.value().first;

    // subKey
    expt = varintDecodeFwd(keyCstr + offset, val.size()-offset);
    if (!expt.ok()) {
        return expt.status();
    }
    offset += expt.value().second;
    size_t keyLen = expt.value().first;
    if (offset + keyLen != val.size()) {
        return {ErrorCodes::ERR_DECODE, "invalid skiplist key len"};
    }
    result._subKey = std::string(val.c_str()+offset, keyLen);
    offset += keyLen;
    return result;
}

uint8_t it2Char(IndexType t) {
  switch (t) {
    case IndexType::IT_TTL:
      return 't';
    default:
      LOG(FATAL) << "invalid index type: " << static_cast<uint8_t>(t);
      return 0;
  }
}

IndexType char2It(uint8_t t) {
  switch (t) {
    case 't':
      return IndexType::IT_TTL;
    default:
      LOG(FATAL) << "invalid index type char:" << static_cast<uint8_t>(t);
      return IndexType::IT_INVALID;
  }
}

TTLIndex&
TTLIndex::operator=(const TTLIndex& o) {
    _ttl = o.getTTL();
    _type = o.getType();
    _priKey = o.getPriKey();
    _dbId = o.getDbId();
    return *this;
}

const std::string TTLIndex::ttlIndex() const {
    std::string ttlIdx;

    for (size_t i = 0; i < sizeof(_ttl); ++i) {
        ttlIdx.push_back(
            static_cast<char>((_ttl>>((sizeof(_ttl)-i-1)*8))&0xff));
    }

    for (size_t i = 0; i < sizeof(_dbId); ++i) {
        ttlIdx.push_back(
            static_cast<char>((_dbId>>((sizeof(_dbId)-i-1)*8))&0xff));
    }

    ttlIdx.push_back(rt2Char(_type));
    ttlIdx.append(_priKey);

    return ttlIdx;
}

std::uint64_t TTLIndex::decodeTTL(const std::string &index) {
    size_t offset = 0;
    uint64_t ttl = 0;

    for (size_t i = 0; i < sizeof(ttl); ++i) {
        ttl = (ttl << 8) | static_cast<uint8_t>(index[offset+i]);
    }

    return ttl;
}

std::uint32_t TTLIndex::decodeDBId(const std::string &index) {
    size_t offset = sizeof(_ttl);
    uint32_t dbid = 0;

    for (size_t i = 0; i < sizeof(dbid); ++i) {
        dbid = (dbid << 8) | index[offset+i];
    }

    return dbid;
}

RecordType TTLIndex::decodeType(const std::string &index) {
  size_t offset = sizeof(_ttl) + sizeof(_dbId);

  return char2Rt(index[offset]);
}

std::string TTLIndex::decodePriKey(const std::string &index) {
  size_t offset = sizeof(_ttl) + sizeof(_dbId) + sizeof(uint8_t);

  return index.substr(offset);
}

std::string TTLIndex::encode() const {
  std::string partial = ttlIndex();

  RecordKey tmpRk(TTLIndex::CHUNKID,
                  TTLIndex::DBID,
                  RecordType::RT_TTL_INDEX,
                  std::move(partial), "");

  return tmpRk.encode();
}

Expected<TTLIndex> TTLIndex::decode(const RecordKey& rk) {
    const std::string &index = rk.getPrimaryKey();
    if (index.size() < sizeof(_ttl) + sizeof(uint8_t) + sizeof(_dbId)) {
        return {ErrorCodes::ERR_DECODE, "Invalid keylen"};
    }

    uint64_t ttl = decodeTTL(index);
    uint32_t dbId = decodeDBId(index);
    RecordType type = decodeType(index);
    std::string priKey = decodePriKey(index);

    return TTLIndex(priKey, type, dbId, ttl);
}

namespace rcd_util {
Expected<uint64_t> getSubKeyCount(const RecordKey& key,
                                  const RecordValue& val) {
     switch (key.getRecordType()) {
        case RecordType::RT_KV: {
            return 1;
        }
        case RecordType::RT_HASH_META: {
            auto v = HashMetaValue::decode(val.getValue());
            if (!v.ok()) {
                return v.status();
            }
            return v.value().getCount();
        }
        case RecordType::RT_LIST_META: {
            auto v = ListMetaValue::decode(val.getValue());
            if (!v.ok()) {
                return v.status();
            }
            return v.value().getTail() - v.value().getHead();
        }
        case RecordType::RT_SET_META: {
            auto v = SetMetaValue::decode(val.getValue());
            if (!v.ok()) {
                return v.status();
            }
            return v.value().getCount();
        }
        case RecordType::RT_ZSET_META: {
            auto v = ZSlMetaValue::decode(val.getValue());
            if (!v.ok()) {
                return v.status();
            }
            return v.value().getCount();
        }
        default: {
            return {ErrorCodes::ERR_INTERNAL, "not support"};
        }
    }
}
}  // namespace rcd_util
}  // namespace tendisplus
