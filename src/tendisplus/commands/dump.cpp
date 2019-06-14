#include <algorithm>
#include <string>
#include <utility>
#include "tendisplus/commands/dump.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/storage/skiplist.h"
#include "tendisplus/utils/string.h"

namespace tendisplus {
template <typename T>
size_t easyCopy(std::vector<byte> *buf, size_t *pos, T element) {
    if (*pos + sizeof(T) > buf->size()) {
        buf->resize(*pos + sizeof(T));
    }
    auto* ptr = reinterpret_cast<byte*>(&element);
    std::copy(ptr, (ptr + sizeof(T)), buf->begin() + *pos);
    *pos += sizeof(T);
    return sizeof(T);
}

template <typename T>
size_t easyCopy(std::vector<byte> *buf, size_t *pos,
                const T *array, size_t len) {
    if (*pos + len > buf->size()) {
        buf->resize(*pos + len * sizeof(T));
    }
    auto* ptr = const_cast<byte*>(reinterpret_cast<const byte*>(array));
    std::copy(ptr, (ptr + len * sizeof(T)), buf->begin() + *pos);
    *pos += len * sizeof(T);
    return len * sizeof(T);
}

template <typename T>
size_t easyCopy(T *dest, const std::string &buf, size_t *pos) {
    if (buf.size() < *pos) {
        return 0;
    }
    byte *ptr = reinterpret_cast<byte*>(dest);
    size_t end = *pos + sizeof(T);
    std::copy(&buf[*pos], &buf[end], ptr);
    *pos += sizeof(T);
    return sizeof(T);
}

// dump
// Base class
Serializer::Serializer(Session *sess, const std::string& key, DumpType type)
        : _sess(sess), _key(key), _type(type), _pos(0) {
}

Expected<size_t> Serializer::saveObjectType(
        std::vector<byte> *payload, size_t *pos, DumpType type) {
    return easyCopy(payload, pos, type);
}

Expected<size_t> Serializer::saveLen(
        std::vector<byte> *payload, size_t *pos, size_t len) {
    byte header[2];

    if (len < (1 << 6)) {
        header[0] = (len & 0xff)|(RDB_6BITLEN << 6);
        return easyCopy(payload, pos, header, 1);
    } else if (len < (1 << 14)) {
        header[0] = ((len >> 8) & 0xff) | (RDB_14BITLEN << 6);
        header[1] = len & 0xff;
        return easyCopy(payload, pos, header, 2);
    } else if (len <= UINT32_MAX) {
        header[0] = RDB_32BITLEN;
        if (1 != easyCopy(payload, pos, header, 1)) {
            return { ErrorCodes::ERR_INTERNAL, "copy len to buffer failed" };
        }
        uint32_t len32 = htonl(len);
        return (1 + easyCopy(payload, pos, len32));
    } else {
        header[0] = RDB_64BITLEN;
        if (1 != easyCopy(payload, pos, header, 1)) {
            return { ErrorCodes::ERR_INTERNAL, "copy len to buffer failed"};
        }
        uint64_t len64 = redis_port::htonll(static_cast<uint64_t>(len));
        return (1 + easyCopy(payload, pos, len64));
    }
}

size_t Serializer::saveString(std::vector<byte> *payload,
        size_t *pos, const std::string &str) {
    size_t written(0);
    auto wr = Serializer::saveLen(payload, pos, str.size());
    INVARIANT(wr.value() > 0);
    written += wr.value();
    written += easyCopy(payload, pos, str.c_str(), str.size());
    return written;
}

Expected<std::vector<byte>> Serializer::dump() {
    std::vector<byte> payload;

    Serializer::saveObjectType(&payload, &_pos, _type);
    INVARIANT(_pos);

    auto expRet = dumpObject(payload);
    if (!expRet.ok()) {
        return expRet.status();
    }

    /* Write the footer, this is how it looks like:
     * ----------------+---------------------+---------------+
     * ... RDB payload | 2 bytes RDB version | 8 bytes CRC64 |
     * ----------------+---------------------+---------------+
     * RDB version and CRC are both in little endian.
     */
    byte version[2];
    version[0] = RDB_VERSION & 0xff;
    version[1] = (RDB_VERSION >> 8) & 0xff;
    easyCopy(&payload, &_pos, version, 2);

    uint64_t crc = redis_port::crc64(0, &payload[_begin], _pos - _begin);
    easyCopy(&payload, &_pos, crc);
    _end = _pos;
    return payload;
}

// Command who can only see base class Serializer.
class DumpCommand: public Command {
 public:
    DumpCommand()
        :Command("dump", "r") {
    }

    ssize_t arity() const {
        return 2;
    }

    int32_t firstkey() const {
        return 1;
    }
    int32_t lastkey() const {
        return 1;
    }

    int32_t keystep() const {
        return 1;
    }

    Expected<std::string> run(Session *sess) final {
        const std::string& key = sess->getArgs()[1];
        std::vector<byte> buf;

        auto server = sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbWithKeyLock(
                sess, key, mgl::LockMode::LOCK_X);
        auto exps = getSerializer(sess, key);
        if (!exps.ok()) {
            if (exps.status().code() == ErrorCodes::ERR_EXPIRED ||
                exps.status().code() == ErrorCodes::ERR_NOTFOUND) {
                return Command::fmtNull();
            }
            return exps.status();
        }

        auto expBuf = exps.value()->dump();
        if (!expBuf.ok()) {
            return expBuf.status();
        }
        buf = std::move(expBuf.value());

        // expect our g++ compiler will do cow in this ctor
        std::string output(buf.begin() + exps.value()->_begin,
                           buf.begin() + exps.value()->_end);
        return Command::fmtBulk(output);
    }
} dumpCommand;

// derived classes, each of them should handle the dump of different object.
class KvSerializer: public Serializer {
 public:
    explicit KvSerializer(Session *sess,
                          const std::string& key,
                          RecordValue&& rv)
        :Serializer(sess, key, DumpType::RDB_TYPE_STRING),
        _rv(std::forward<RecordValue>(rv)) {
    }

    Expected<size_t> dumpObject(std::vector<byte>& payload) {
        Serializer::saveString(&payload, &_pos, _rv.getValue());
        _begin = 0;
        return _pos - _begin;
    }

 private:
    RecordValue _rv;
};

class ListSerializer: public Serializer {
 public:
    explicit ListSerializer(Session *sess,
                            const std::string& key,
                            RecordValue&& rv)
        :Serializer(sess, key, DumpType::RDB_TYPE_QUICKLIST),
        _rv(std::forward<RecordValue>(rv)) {
    }

    Expected<size_t> dumpObject(std::vector<byte>& payload) {
        size_t qlbytes(0);
        auto expwr = saveLen(&payload, &_pos, 1);
        if (!expwr.ok()) {
            return expwr.status();
        }
        size_t notAligned = _pos;
        size_t qlEnd = notAligned + 9;

        {
            // make room for quicklist length first,
            // remember to move first several bytes to align after.
            payload.resize(payload.size() + 9);
            _pos += 9;
        }

        auto expListMeta = ListMetaValue::decode(_rv.getValue());
        if (!expListMeta.ok()) {
            return expListMeta.status();
        }
        uint64_t tail = expListMeta.value().getTail();
        uint64_t head = expListMeta.value().getHead();
        uint64_t len = tail - head;
        INVARIANT(len > 0);

        auto server = _sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbHasLocked(_sess, _key);
        if (!expdb.ok()) {
            return expdb.status();
        }
        PStore kvstore = expdb.value().store;

        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());

        /* in this loop we should emulate to build a quicklist(or to say, many ziplists)
         * then compress it using lzf(not implemented), or just write raw to buffer, both can work.*/
        size_t zlInitPos(_pos);
        _pos += 8;
        if (len > UINT16_MAX) {
            return { ErrorCodes::ERR_INTERNAL, "Currently not support" };
        }
        uint32_t zlbytes(10), zltail(0);
        uint64_t prevlen(0);
        uint16_t zllen = len;
        easyCopy(&payload, &_pos, zllen);
        for (size_t i = head; i != tail; i++) {
            RecordKey nodeKey(expdb.value().chunkId, _sess->getCtx()->getDbId(),
                    RecordType::RT_LIST_ELE, _key, std::to_string(i));
            auto expNodeVal = kvstore->getKV(nodeKey, txn.get());
            if (!expNodeVal.ok()) {
                return expNodeVal.status();
            }
            RecordValue nodeVal = std::move(expNodeVal.value());
            size_t written(0);
            // prevlen
            if (prevlen > 254) {
                written += easyCopy(&payload, &_pos,
                        static_cast<unsigned char>(0xfe));
                written += easyCopy(&payload, &_pos, prevlen);
            } else {
                written += easyCopy(&payload, &_pos,
                        static_cast<unsigned char>(prevlen));
            }

            written += saveString(&payload, &_pos, nodeVal.getValue());
            prevlen = written;
            zlbytes += written;
        }
        zlbytes += easyCopy(&payload, &_pos,
                static_cast<unsigned char>(0xff));
        zltail = zlbytes - 1 - prevlen;
        easyCopy(&payload, &zlInitPos, zlbytes);
        easyCopy(&payload, &zlInitPos, zltail);

        qlbytes += zlbytes;
        auto expQlUsed = saveLen(&payload, &notAligned, qlbytes);
        if (!expQlUsed.ok()) {
            return expQlUsed.status();
        }
        if (expQlUsed.value() < 9) {
            auto offset = notAligned;
            std::copy_backward(payload.begin(),
                    payload.begin() + offset,
                    payload.begin() + qlEnd);
        }
        _begin = 9 - expQlUsed.value();
        _end = payload.size() - _begin;

        return zlbytes + expwr.value();
    }

 private:
    RecordValue _rv;
};

class SetSerializer: public Serializer {
 public:
    explicit SetSerializer(Session *sess,
                           const std::string &key,
                           RecordValue&& rv)
        :Serializer(sess, key, DumpType::RDB_TYPE_SET),
        _rv(std::move(rv)) {
    }

    Expected<size_t> dumpObject(std::vector<byte>& payload) {
        Expected<SetMetaValue> expMeta = SetMetaValue::decode(_rv.getValue());
        size_t len = expMeta.value().getCount();
        INVARIANT(len > 0);

        auto expwr = saveLen(&payload, &_pos, len);
        if (!expwr.ok()) {
            return expwr.status();
        }

        auto server = _sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbHasLocked(_sess, _key);
        if (!expdb.ok()) {
            return expdb.status();
        }
        PStore kvstore = expdb.value().store;
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());

        auto cursor = txn->createCursor();
        RecordKey fakeRk(expdb.value().chunkId,
                _sess->getCtx()->getDbId(),
                RecordType::RT_SET_ELE,
                _key, "");
        cursor->seek(fakeRk.prefixPk());
        while (true) {
            Expected<Record> eRcd = cursor->next();
            if (eRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
                break;
            }
            if (!eRcd.ok()) {
                return eRcd.status();
            }
            Record &rcd = eRcd.value();
            const RecordKey &rcdKey = rcd.getRecordKey();
            if (rcdKey.prefixPk() != fakeRk.prefixPk()) {
                break;
            }

            const std::string &subk = rcdKey.getSecondaryKey();
            Serializer::saveString(&payload, &_pos, subk);
        }

        _begin = 0;
        return _pos - _begin;
    }

 private:
    RecordValue _rv;
};

class ZsetSerializer: public Serializer {
 public:
    explicit ZsetSerializer(Session *sess,
                            const std::string& key,
                            RecordValue&& rv)
        :Serializer(sess, key, DumpType::RDB_TYPE_ZSET),
        _rv(std::move(rv)) {
    }

    Expected<size_t> dumpObject(std::vector<byte>& payload) {
        auto server = _sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbHasLocked(_sess, _key);
        if (!expdb.ok()) {
            return expdb.status();
        }
        PStore kvstore = expdb.value().store;
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());

        auto eMeta = ZSlMetaValue::decode(_rv.getValue());
        if (!eMeta.ok()) {
            return eMeta.status();
        }
        ZSlMetaValue meta = eMeta.value();
        SkipList zsl(expdb.value().chunkId,
                _sess->getCtx()->getDbId(),
                _key, meta, kvstore);

        auto expwr = saveLen(&payload, &_pos, zsl.getCount() - 1);
        if (!expwr.ok()) {
            return expwr.status();
        }

        auto rev = zsl.scanByRank(0, zsl.getCount() - 1, true, txn.get());
        if (!rev.ok()) {
            return rev.status();
        }
        for (auto& ele : rev.value()) {
            Serializer::saveString(
                    &payload, &_pos, std::forward<std::string>(ele.second));
            // save binary double score
            double score = ele.first;
            easyCopy(&payload, &_pos, score);
        }
        _begin = 0;
        return _pos - _begin;
    }

 private:
    RecordValue _rv;
};

class HashSerializer: public Serializer {
 public:
    explicit HashSerializer(Session *sess,
                            const std::string &key,
                            RecordValue&& rv)
        :Serializer(sess, key, DumpType::RDB_TYPE_HASH),
        _rv(std::forward<RecordValue>(rv)) {
    }

    Expected<size_t> dumpObject(std::vector<byte> &payload) {
        Expected<HashMetaValue> expHashMeta =
                HashMetaValue::decode(_rv.getValue());
        if (!expHashMeta.ok()) {
            return expHashMeta.status();
        }
        auto expwr = saveLen(&payload, &_pos, expHashMeta.value().getCount());
        if (!expwr.ok()) {
            return expwr.status();
        }

        auto server = _sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbHasLocked(_sess, _key);
        if (!expdb.ok()) {
            return expdb.status();
        }

        PStore kvstore = expdb.value().store;
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());

        RecordKey fakeRk(
                expdb.value().chunkId,
                _sess->getCtx()->getDbId(),
                RecordType::RT_HASH_ELE, _key, "");
        auto cursor = txn->createCursor();
        cursor->seek(fakeRk.prefixPk());
        while (true) {
            Expected<Record> expRcd = cursor->next();
            if (expRcd.status().code() == ErrorCodes::ERR_EXHAUST) {
                break;
            }
            if (!expRcd.ok()) {
                return expRcd.status();
            }
            if (expRcd.value().getRecordKey().prefixPk() != fakeRk.prefixPk()) {
                break;
            }
            const std::string &field =
                    expRcd.value().getRecordKey().getSecondaryKey();
            const std::string &value =
                    expRcd.value().getRecordValue().getValue();
            Serializer::saveString(&payload, &_pos, field);
            Serializer::saveString(&payload, &_pos, value);
        }
        _begin = 0;
        return _pos - _begin;
    }

 private:
    RecordValue _rv;
};

// outlier function
Expected<std::unique_ptr<Serializer>> getSerializer(Session *sess,
        const std::string &key) {
    Expected<RecordValue> rv =
            Command::expireKeyIfNeeded(sess, key, RecordType::RT_DATA_META);
    if (!rv.ok()) {
        return rv.status();
    }

    std::unique_ptr<Serializer> ptr;
    auto type = rv.value().getRecordType();
    switch (type) {
        case RecordType::RT_KV:
            ptr = std::move(std::unique_ptr<Serializer>(
                    new KvSerializer(sess, key, std::move(rv.value()))));
            break;
        case RecordType::RT_LIST_META:
            ptr = std::move(std::unique_ptr<Serializer>(
                    new ListSerializer(sess, key, std::move(rv.value()))));
            break;
        case RecordType::RT_HASH_META:
            ptr = std::move(std::unique_ptr<Serializer>(
                    new HashSerializer(sess, key, std::move(rv.value()))));
            break;
        case RecordType::RT_SET_META:
            ptr = std::move(std::unique_ptr<Serializer>(
                    new SetSerializer(sess, key, std::move(rv.value()))));
            break;
        case RecordType::RT_ZSET_META:
            ptr = std::move(std::unique_ptr<Serializer>(
                    new ZsetSerializer(sess, key, std::move(rv.value()))));
            break;
        default:
            return {ErrorCodes::ERR_WRONG_TYPE, "type can not be dumped"};
    }

    return std::move(ptr);
}

// restore
Deserializer::Deserializer(
        Session *sess,
        const std::string &payload,
        const std::string &key,
        const uint64_t ttl)
    : _sess(sess), _payload(payload), _key(key), _ttl(ttl), _pos(1) {}

Expected<DumpType> Deserializer::loadObjectType(
        const std::string &payload,
        size_t &&pos) {
    uint8_t t;
    easyCopy(&t, payload, &pos);
    return static_cast<DumpType>(t);
}

Expected<size_t> Deserializer::loadLen(
        const std::string &payload,
        size_t *pos) {
    byte buf[2];
    size_t ret;
    INVARIANT(easyCopy(&buf[0], payload, pos) == 1);
    uint8_t encType = static_cast<uint8_t>((buf[0]&0xC0) >> 6);
    if (encType == RDB_6BITLEN) {
        ret = buf[0] & 0x3F;
    } else if (encType == RDB_14BITLEN) {
        INVARIANT(easyCopy(&buf[1], payload, pos) == 1);
        ret = ((buf[0]&0x3F) << 8) | buf[1];
    } else if (buf[0] == RDB_32BITLEN) {
        uint32_t len32;
        INVARIANT(easyCopy(&len32, payload, pos) == 4);
        ret = ntohl(len32);
    } else if (buf[0] == RDB_64BITLEN) {
        uint64_t len64;
        INVARIANT(easyCopy(&len64, payload, pos) == 8);
        ret = redis_port::ntohll(len64);
    } else {
        return {ErrorCodes::ERR_INTERNAL, "Unknown length encoding"};
    }
    return ret;
}

std::string Deserializer::loadString(const std::string &payload, size_t *pos) {
    auto expLen = Deserializer::loadLen(payload, pos);
    if (!expLen.ok()) {
        return std::string("");
    }
    size_t len = expLen.value();
    *pos += len;
    return std::string(payload.begin() + *pos - len, payload.begin() + *pos);
}

class RestoreCommand: public Command {
 public:
    RestoreCommand()
        :Command("restore", "wm") {
    }

    ssize_t arity() const {
        return -4;
    }

    int32_t firstkey() const {
        return 1;
    }

    int32_t lastkey() const {
        return 1;
    }

    int32_t keystep() const {
        return 1;
    }

    static Status verifyDumpPayload(const std::string &payload) {
        uint8_t buf[2];
        uint64_t crc;

        size_t len = payload.size();
        if (len < 10) {
            return { ErrorCodes::ERR_INTERNAL, "len cannot be lt 10" };
        }
        size_t chkpos = len - 10;
        INVARIANT(easyCopy(&buf[0], payload, &chkpos) == 1 &&
                  easyCopy(&buf[1], payload, &chkpos) == 1);

        uint16_t rdbver = (buf[1] << 8) | buf[0];
        if (rdbver > RDB_VERSION) {
            return { ErrorCodes::ERR_INTERNAL, "rdb version not match" };
        }

        crc = redis_port::crc64(0,
                reinterpret_cast<const byte*>(payload.c_str()),
                chkpos);
        if (memcmp(&crc, payload.c_str() + chkpos, 8) == 0) {
            return { ErrorCodes::ERR_OK, "OK" };
        }
        return { ErrorCodes::ERR_INTERNAL, "crc not match" };
    }

    Expected<std::string> run(Session *sess) final {
        const auto& args = sess->getArgs();
        const std::string &key = args[1];
        const std::string &sttl = args[2];
        const std::string &payload = args[3];

        // TODO: parse additional args
        {
            for (size_t i = 4; i < args.size(); i++) {
                if (::strcasecmp(args[i].c_str(), "replace")) {
                    return {ErrorCodes::ERR_PARSEOPT, ""};
                }
            }
        }

        // check if key exists
        Expected<RecordValue> rv =
                Command::expireKeyIfNeeded(sess, key, RecordType::RT_DATA_META);
        if (rv.status().code() != ErrorCodes::ERR_EXPIRED &&
            rv.status().code() != ErrorCodes::ERR_NOTFOUND) {
            if (!rv.ok()) {
                return rv.status();
            }
            return Command::fmtBusyKey();
        }

        Expected<int64_t> expttl = tendisplus::stoll(sttl);
        if (!expttl.ok()) {
            return expttl.status();
        }
        if (expttl.value() < 0) {
            return {ErrorCodes::ERR_PARSEPKT,
                    "Invalid TTL value, must be >= 0"};
        }
        uint64_t ts = 0;
        if (expttl.value() != 0)
            ts = msSinceEpoch() + expttl.value();

        Status chk = RestoreCommand::verifyDumpPayload(payload);
        if (!chk.ok()) {
            return {ErrorCodes::ERR_PARSEPKT,
                    "DUMP payload version or checksum are wrong"};
        }

        // do restore
        auto expds = getDeserializer(sess, payload, key, ts);
        if (!expds.ok()) {
            return expds.status();
        }

        Status res = expds.value()->restore();
        if (res.ok()) {
            return Command::fmtOK();
        }
        return res;
    }
} restoreCommand;

class KvDeserializer: public Deserializer {
 public:
    explicit KvDeserializer(
            Session *sess,
            const std::string &payload,
            const std::string &key,
            const uint64_t ttl )
        : Deserializer(sess, payload, key, ttl) {
    }

    virtual Status restore() {
        auto ret = Deserializer::loadString(_payload, &_pos);
        auto server = _sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbHasLocked(_sess, _key);
        if (!expdb.ok()) {
            return expdb.status();
        }
        PStore kvstore = expdb.value().store;
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }

        std::unique_ptr<Transaction> txn = std::move(ptxn.value());

        SessionCtx *pCtx = _sess->getCtx();
        INVARIANT(pCtx != nullptr);

        RecordKey rk(expdb.value().chunkId,
                pCtx->getDbId(),
                RecordType::RT_KV, _key, "");
        RecordValue rv(ret, RecordType::RT_KV, pCtx->getVersionEP(), _ttl);
        for (int32_t i = 0; i < Command::RETRY_CNT - 1; ++i) {
            Status s = kvstore->setKV(rk, rv, txn.get());
            if (!s.ok()) {
                return s;
            }
            Expected<uint64_t> expCmt = txn->commit();
            if (!expCmt.ok()) {
                return expCmt.status();
            }
            ptxn = kvstore->createTransaction();
            if (!ptxn.ok()) {
                return ptxn.status();
            }
            txn = std::move(ptxn.value());
        }

        return { ErrorCodes::ERR_OK, "OK"};
    }
};

class SetDeserializer: public Deserializer {
 public:
    explicit SetDeserializer(
            Session *sess,
            const std::string &payload,
            const std::string &key,
            const uint64_t ttl)
        : Deserializer(sess, payload, key, ttl) {
    }

    virtual Status restore() {
        auto expLen = Deserializer::loadLen(_payload, &_pos);
        if (!expLen.ok()) {
            return expLen.status();
        }

        size_t len = expLen.value();
        // std::vector<std::string> set(2);

        auto server = _sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbHasLocked(_sess, _key);
        if (!expdb.ok()) {
            return expdb.status();
        }
        PStore kvstore = expdb.value().store;
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());

        RecordKey metaRk(expdb.value().chunkId,
                         _sess->getCtx()->getDbId(),
                         RecordType::RT_SET_META, _key, "");
        SetMetaValue sm;

        for (size_t i = 0; i < len; i++) {
            std::string ele = loadString(_payload, &_pos);
            RecordKey rk(metaRk.getChunkId(),
                    metaRk.getDbId(),
                    RecordType::RT_SET_ELE,
                    metaRk.getPrimaryKey(), std::move(ele));
            RecordValue rv("", RecordType::RT_SET_ELE, 0);
            Status s = kvstore->setKV(rk, rv, txn.get());
            if (!s.ok()) {
                return s;
            }
        }
        sm.setCount(len);
        Status s = kvstore->setKV(metaRk,
                RecordValue(sm.encode(), RecordType::RT_SET_META,
                        _sess->getCtx()->getVersionEP(), _ttl),
                txn.get());
        if (!s.ok()) {
            return s;
        }
        Expected<uint64_t> expCmt = txn->commit();
        if (!expCmt.ok()) {
            return expCmt.status();
        }
        return { ErrorCodes::ERR_OK, "OK" };
    }
};

class ZsetDeserializer: public Deserializer {
 public:
    explicit ZsetDeserializer(
            Session *sess,
            const std::string &payload,
            const std::string &key,
            const uint64_t ttl)
        : Deserializer(sess, payload, key, ttl) {
    }

    virtual Status restore() {
        auto expLen = loadLen(_payload, &_pos);
        if (!expLen.ok()) {
            return expLen.status();
        }

        size_t len = expLen.value();
        std::map<std::string, double> scoreMap;
        while (len--) {
            std::string ele = loadString(_payload, &_pos);
            double score;
            INVARIANT(easyCopy(&score, _payload, &_pos) == 8);
            scoreMap[ele] = score;
        }
        auto server = _sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbHasLocked(_sess, _key);
        if (!expdb.ok()) {
            return expdb.status();
        }

        RecordKey rk(expdb.value().chunkId,
                _sess->getCtx()->getDbId(),
                RecordType::RT_ZSET_META, _key, "");
        PStore kvstore = expdb.value().store;
        // set ttl first
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }

        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        Expected<RecordValue> eMeta =
                kvstore->getKV(rk, txn.get());
        if (!eMeta.ok() && eMeta.status().code() != ErrorCodes::ERR_NOTFOUND) {
            return eMeta.status();
        }
        INVARIANT(eMeta.status().code() == ErrorCodes::ERR_NOTFOUND);
        ZSlMetaValue meta(1, 1, 0);
        RecordValue rv(meta.encode(), RecordType::RT_ZSET_META,
                _sess->getCtx()->getVersionEP(), _ttl);
        Status s = kvstore->setKV(rk, rv, txn.get());
        if (!s.ok()) {
            return s;
        }
        RecordKey headRk(rk.getChunkId(),
                rk.getDbId(),
                RecordType::RT_ZSET_S_ELE,
                rk.getPrimaryKey(),
                std::to_string(ZSlMetaValue::HEAD_ID));
        ZSlEleValue headVal;
        RecordValue headRv(headVal.encode(), RecordType::RT_ZSET_S_ELE, 0);
        s = kvstore->setKV(headRk, headRv, txn.get());
        if (!s.ok()) {
            return s;
        }
        Expected<uint64_t> expCmt = txn->commit();
        if (!expCmt.ok()) {
            return expCmt.status();
        }

        for (int32_t i = 0; i < Command::RETRY_CNT; ++i) {
            // maybe very slow
            Expected<std::string> res =
                    genericZadd(_sess, kvstore, rk, rv, scoreMap, ZADD_NX);
            if (res.ok()) {
                return { ErrorCodes::ERR_OK, "OK" };
            }
            if (res.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
                return res.status();
            }
            if (i == Command::RETRY_CNT - 1) {
                return res.status();
            } else {
                continue;
            }
        }

        return { ErrorCodes::ERR_INTERNAL, "not reachable" };
    }
};

class HashDeserializer: public Deserializer {
 public:
    explicit HashDeserializer(
            Session *sess,
            const std::string &payload,
            const std::string &key,
            const uint64_t ttl)
        : Deserializer(sess, payload, key, ttl) {
    }

    virtual Status restore() {
        auto expLen = loadLen(_payload, &_pos);
        if (!expLen.ok()) {
            return expLen.status();
        }

        size_t len = expLen.value();
        auto server = _sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbHasLocked(_sess, _key);
        if (!expdb.ok()) {
            return expdb.status();
        }
        PStore kvstore = expdb.value().store;
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        for (size_t i = 0; i < len; i++) {
            std::string field = loadString(_payload, &_pos);
            std::string value = loadString(_payload, &_pos);
            // need existence check ?
            RecordKey rk(expdb.value().chunkId, _sess->getCtx()->getDbId(),
                    RecordType::RT_HASH_ELE, _key, field);
            RecordValue rv(value, RecordType::RT_HASH_ELE, 0);
            Status s = kvstore->setKV(rk, rv, txn.get());
            if (!s.ok()) {
                return s;
            }
        }

        RecordKey metaRk(expdb.value().chunkId, _sess->getCtx()->getDbId(),
                RecordType::RT_HASH_META, _key, "");
        HashMetaValue hashMeta;
        hashMeta.setCount(len);
        RecordValue metaRv(std::move(hashMeta.encode()),
                RecordType::RT_HASH_META, _sess->getCtx()->getVersionEP(), _ttl);
        Status s = kvstore->setKV(metaRk, metaRv, txn.get());
        if (!s.ok()) {
            return s;
        }
        Expected<uint64_t> expCmt = txn->commit();
        if (!expCmt.ok()) {
            return expCmt.status();
        }
        return { ErrorCodes::ERR_OK, "OK" };
    }
};

class ListDeserializer: public Deserializer {
    std::vector<std::string> deserializeZiplist(
            const std::string &payload, size_t *pos) {
        uint32_t zlbytes(0), zltail(0);
        uint16_t zllen(0);
        std::vector<std::string> zl;
        INVARIANT(easyCopy(&zlbytes, payload, pos) == 4 &&
            easyCopy(&zltail, payload, pos) == 4 &&
            easyCopy(&zllen, payload, pos) == 2);

        zl.reserve(zllen);
        uint32_t prevlen(0);
        while (zllen--) {
            if (prevlen > 254) {
                *pos += 5;
            } else {
                *pos += 1;
            }
            std::string val = loadString(payload, pos);
            prevlen = val.size();
            zl.push_back(std::move(val));
        }
        uint8_t zlend(0);
        INVARIANT(easyCopy(&zlend, payload, pos) == 1);
        INVARIANT(zlend == 0xff);
        return zl;
    }

 public:
    explicit ListDeserializer(
            Session *sess,
            const std::string &payload,
            const std::string &key,
            const uint64_t ttl)
        : Deserializer(sess, payload, key, ttl) {
    }

    virtual Status restore() {
        auto qlExpLen = loadLen(_payload, &_pos);
        if (!qlExpLen.ok()) {
            return qlExpLen.status();
        }

        size_t qlLen = qlExpLen.value();
        auto server = _sess->getServerEntry();
        auto expdb = server->getSegmentMgr()->getDbHasLocked(_sess, _key);
        if (!expdb.ok()) {
            return expdb.status();
        }
        RecordKey metaRk(expdb.value().chunkId, _sess->getCtx()->getDbId(),
                RecordType::RT_LIST_META, _key, "");
        PStore kvstore = expdb.value().store;
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());
        ListMetaValue lm(INITSEQ, INITSEQ);

        uint64_t head = lm.getHead();
        uint64_t tail = lm.getTail();
        while (qlLen--) {
            auto expLen = loadLen(_payload, &_pos);
            if (!expLen.ok()) {
                return expLen.status();
            }

            std::vector<std::string> zl = deserializeZiplist(_payload, &_pos);
            uint64_t idx;
            for (auto iter = zl.begin(); iter != zl.end(); iter++) {
                idx = tail++;
                RecordKey rk(metaRk.getChunkId(),
                             metaRk.getDbId(),
                             RecordType::RT_LIST_ELE,
                             metaRk.getPrimaryKey(),
                             std::to_string(idx));
                RecordValue rv(std::move(*iter), RecordType::RT_LIST_ELE, 0);
                Status s = kvstore->setKV(rk, rv, txn.get());
                if (!s.ok()) {
                    return s;
                }
            }
        }
        lm.setHead(head);
        lm.setTail(tail);
        RecordValue metaRv(lm.encode(), RecordType::RT_LIST_META,
                _sess->getCtx()->getVersionEP(), _ttl);
        Status s = kvstore->setKV(metaRk, metaRv, txn.get());
        if (!s.ok()) {
            return s;
        }
        Expected<uint64_t> expCmt = txn->commit();
        if (!expCmt.ok()) {
            return expCmt.status();
        }
        return { ErrorCodes::ERR_OK, "OK" };
    }
};

Expected<std::unique_ptr<Deserializer>> getDeserializer(
        Session *sess,
        const std::string &payload,
        const std::string &key, const uint64_t ttl) {
    Expected<DumpType> expType = Deserializer::loadObjectType(payload, 0);
    if (!expType.ok()) {
        return expType.status();
    }
    std::unique_ptr<Deserializer> ptr;
    DumpType type = std::move(expType.value());
    switch (type) {
        case DumpType::RDB_TYPE_STRING:
            ptr = std::move(std::unique_ptr<Deserializer>(
                    new KvDeserializer(sess, payload, key, ttl)));
            break;
        case DumpType::RDB_TYPE_SET:
            ptr = std::move(std::unique_ptr<Deserializer>(
                    new SetDeserializer(sess, payload, key, ttl)));
            break;
        case DumpType::RDB_TYPE_ZSET:
            ptr = std::move(std::unique_ptr<Deserializer>(
                    new ZsetDeserializer(sess, payload, key, ttl)));
            break;
        case DumpType::RDB_TYPE_HASH:
            ptr = std::move(std::unique_ptr<Deserializer>(
                    new HashDeserializer(sess, payload, key, ttl)));
            break;
        case DumpType::RDB_TYPE_QUICKLIST:
            ptr = std::move(std::unique_ptr<Deserializer>(
                    new ListDeserializer(sess, payload, key, ttl)));
            break;
        default:
            return {ErrorCodes::ERR_INTERNAL, "Not implemented"};
    }
    return std::move(ptr);
}

} // namespace tendisplus