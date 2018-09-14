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
        case RecordType::RT_HASH_META:
            return 'H';
        case RecordType::RT_HASH_ELE:
            return 'h';
        //  it's convinent (for seek) to have BINLOG to pos
        //  at the rightmost of a lsmtree
        case RecordType::RT_BINLOG:
            return std::numeric_limits<char>::max();
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
        case 'H':
            return RecordType::RT_HASH_META;
        case 'h':
            return RecordType::RT_HASH_ELE;
        case std::numeric_limits<char>::max():
            return RecordType::RT_BINLOG;
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

RecordKey::RecordKey(uint32_t dbid, RecordType type,
        std::string&& pk, std::string&& sk)
    :_dbId(dbid),
     _type(type),
     _pk(std::move(pk)),
     _sk(std::move(sk)),
     _fmtVsn(0) {
}

uint32_t RecordKey::getDbId() const {
    return _dbId;
}

RecordType RecordKey::getRecordType() const {
    return _type;
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
    std::string hexPk = hexlify(_pk);
    key.insert(key.end(), hexPk.begin(), hexPk.end());

    // NOTE(deyukong): 0 never exists in hex string.
    // a padding o avoids prefixes intersect with
    // each other in physical space
    key.push_back(0);

    // SK
    key.insert(key.end(), _sk.begin(), _sk.end());

    // len(PK)
    auto lenPK = varintEncode(_pk.size());
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
        return expt.status();
    }
    offset += expt.value().second;
    dbid = static_cast<uint32_t>(expt.value().first);

    // type
    char typec = keyCstr[offset++];
    type = char2Rt(typec);

    // pklen is stored in the reverse order
    // pklen
    const uint8_t *p = keyCstr + key.size() - rsvd - 1;
    expt = varintDecodeRvs(p, key.size() - rsvd - offset);
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
    return RecordKey(dbid, type, unhexPk.value(), sk);
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

RecordValue::RecordValue(std::string&& val, uint64_t ttl)
        :_ttl(ttl),
         _value(std::move(val)) {
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
        return expt.status();
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

const std::string& RecordValue::getValue() const {
    return _value;
}

uint64_t RecordValue::getTtl() const {
    return _ttl;
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

const std::string& ReplLogKey::prefix() {
    static std::string s = []() {
        std::string result;
        result.push_back(0);
        result.push_back(rt2Char(RecordType::RT_BINLOG));
        return result;
    }();
    return s;
}

std::string ReplLogKey::prefix(uint64_t commitId) {
    // NOTE(deyukong): currently commitId is defined as uint64,
    // we do a compiletime assert here. change the logic if commitId is
    // redefined to different structure
    static_assert(sizeof(commitId) == 8,
        "commitId size not 8, reimpl the logic");
    std::string p = ReplLogKey::prefix();
    const uint8_t *txnBuf = reinterpret_cast<const uint8_t*>(&commitId);
    for (size_t i = 0; i < sizeof(commitId); i++) {
        p.push_back(txnBuf[sizeof(commitId)-1-i]);
    }
    return p;
}

std::string ReplLogKey::encode() const {
    std::vector<uint8_t> key;
    key.reserve(128);
    const uint8_t *txnBuf = reinterpret_cast<const uint8_t*>(&_txnId);
    for (size_t i = 0; i < sizeof(_txnId); i++) {
        key.emplace_back(txnBuf[sizeof(_txnId)-1-i]);
    }
    key.emplace_back(_localId>>8);
    key.emplace_back(_localId&0xff);
    key.emplace_back(static_cast<uint16_t>(_flag)>>8);
    key.emplace_back(static_cast<uint16_t>(_flag)&0xff);
    const uint8_t *tsBuf = reinterpret_cast<const uint8_t*>(&_timestamp);
    for (size_t i = 0; i < sizeof(_timestamp); i++) {
        key.emplace_back(tsBuf[sizeof(_timestamp)-1-i]);
    }
    key.emplace_back(_reserved);
    std::string partial(reinterpret_cast<const char *>(key.data()), key.size());
    RecordKey tmpRk(0, RecordType::RT_BINLOG, std::move(partial), "");
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
    :HashMetaValue(0, 0) {
}

HashMetaValue::HashMetaValue(uint64_t count, uint64_t cas)
    :_count(count),
     _cas(cas) {
}

HashMetaValue::HashMetaValue(HashMetaValue&& o)
        :_count(o._count),
         _cas(o._cas) {
    o._count = 0;
    o._cas = 0;
}

std::string HashMetaValue::encode() const {
    std::vector<uint8_t> value;
    value.reserve(128);
    auto countBytes = varintEncode(_count);
    value.insert(value.end(), countBytes.begin(), countBytes.end());
    auto casBytes = varintEncode(_cas);
    value.insert(value.end(), casBytes.begin(), casBytes.end());
    return std::string(reinterpret_cast<const char *>(
                value.data()), value.size());
}

Expected<HashMetaValue> HashMetaValue::decode(const std::string& val) {
    const uint8_t *valCstr = reinterpret_cast<const uint8_t*>(val.c_str());
    size_t offset = 0;
    uint64_t count = 0;
    uint64_t cas = 0;
    auto expt = varintDecodeFwd(valCstr + offset, val.size());
    if (!expt.ok()) {
        return expt.status();
    }
    offset += expt.value().second;
    count = expt.value().first;

    expt = varintDecodeFwd(valCstr + offset, val.size() - offset);
    if (!expt.ok()) {
        return expt.status();
    }
    offset += expt.value().second;
    cas = expt.value().first;
    return HashMetaValue(count, cas);
}

HashMetaValue& HashMetaValue::operator=(HashMetaValue&& o) {
    if (&o == this) {
        return *this;
    }
    _count = o._count;
    _cas = o._cas;
    o._count = 0;
    o._cas = 0;
    return *this;
}

void HashMetaValue::setCount(uint64_t count) {
    _count = count;
}

void HashMetaValue::setCas(uint64_t cas) {
    _cas = cas;
}

uint64_t HashMetaValue::getCount() const {
    return _count;
}

uint64_t HashMetaValue::getCas() const {
    return _cas;
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
        default: {
            return {ErrorCodes::ERR_INTERNAL, "not support"};
        }
    }
}
}  // namespace rcd_util
}  // namespace tendisplus
