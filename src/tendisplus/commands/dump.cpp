#include <string>
#include <vector>
#include "tendisplus/commands/dump.h"
#include "tendisplus/commands/command.h"
#include "tendisplus/storage/skiplist.h"
#include "tendisplus/utils/string.h"

namespace tendisplus {

static const uint64_t crc64_tab[256] = {
        UINT64_C(0x0000000000000000), UINT64_C(0x7ad870c830358979),
        UINT64_C(0xf5b0e190606b12f2), UINT64_C(0x8f689158505e9b8b),
        UINT64_C(0xc038e5739841b68f), UINT64_C(0xbae095bba8743ff6),
        UINT64_C(0x358804e3f82aa47d), UINT64_C(0x4f50742bc81f2d04),
        UINT64_C(0xab28ecb46814fe75), UINT64_C(0xd1f09c7c5821770c),
        UINT64_C(0x5e980d24087fec87), UINT64_C(0x24407dec384a65fe),
        UINT64_C(0x6b1009c7f05548fa), UINT64_C(0x11c8790fc060c183),
        UINT64_C(0x9ea0e857903e5a08), UINT64_C(0xe478989fa00bd371),
        UINT64_C(0x7d08ff3b88be6f81), UINT64_C(0x07d08ff3b88be6f8),
        UINT64_C(0x88b81eabe8d57d73), UINT64_C(0xf2606e63d8e0f40a),
        UINT64_C(0xbd301a4810ffd90e), UINT64_C(0xc7e86a8020ca5077),
        UINT64_C(0x4880fbd87094cbfc), UINT64_C(0x32588b1040a14285),
        UINT64_C(0xd620138fe0aa91f4), UINT64_C(0xacf86347d09f188d),
        UINT64_C(0x2390f21f80c18306), UINT64_C(0x594882d7b0f40a7f),
        UINT64_C(0x1618f6fc78eb277b), UINT64_C(0x6cc0863448deae02),
        UINT64_C(0xe3a8176c18803589), UINT64_C(0x997067a428b5bcf0),
        UINT64_C(0xfa11fe77117cdf02), UINT64_C(0x80c98ebf2149567b),
        UINT64_C(0x0fa11fe77117cdf0), UINT64_C(0x75796f2f41224489),
        UINT64_C(0x3a291b04893d698d), UINT64_C(0x40f16bccb908e0f4),
        UINT64_C(0xcf99fa94e9567b7f), UINT64_C(0xb5418a5cd963f206),
        UINT64_C(0x513912c379682177), UINT64_C(0x2be1620b495da80e),
        UINT64_C(0xa489f35319033385), UINT64_C(0xde51839b2936bafc),
        UINT64_C(0x9101f7b0e12997f8), UINT64_C(0xebd98778d11c1e81),
        UINT64_C(0x64b116208142850a), UINT64_C(0x1e6966e8b1770c73),
        UINT64_C(0x8719014c99c2b083), UINT64_C(0xfdc17184a9f739fa),
        UINT64_C(0x72a9e0dcf9a9a271), UINT64_C(0x08719014c99c2b08),
        UINT64_C(0x4721e43f0183060c), UINT64_C(0x3df994f731b68f75),
        UINT64_C(0xb29105af61e814fe), UINT64_C(0xc849756751dd9d87),
        UINT64_C(0x2c31edf8f1d64ef6), UINT64_C(0x56e99d30c1e3c78f),
        UINT64_C(0xd9810c6891bd5c04), UINT64_C(0xa3597ca0a188d57d),
        UINT64_C(0xec09088b6997f879), UINT64_C(0x96d1784359a27100),
        UINT64_C(0x19b9e91b09fcea8b), UINT64_C(0x636199d339c963f2),
        UINT64_C(0xdf7adabd7a6e2d6f), UINT64_C(0xa5a2aa754a5ba416),
        UINT64_C(0x2aca3b2d1a053f9d), UINT64_C(0x50124be52a30b6e4),
        UINT64_C(0x1f423fcee22f9be0), UINT64_C(0x659a4f06d21a1299),
        UINT64_C(0xeaf2de5e82448912), UINT64_C(0x902aae96b271006b),
        UINT64_C(0x74523609127ad31a), UINT64_C(0x0e8a46c1224f5a63),
        UINT64_C(0x81e2d7997211c1e8), UINT64_C(0xfb3aa75142244891),
        UINT64_C(0xb46ad37a8a3b6595), UINT64_C(0xceb2a3b2ba0eecec),
        UINT64_C(0x41da32eaea507767), UINT64_C(0x3b024222da65fe1e),
        UINT64_C(0xa2722586f2d042ee), UINT64_C(0xd8aa554ec2e5cb97),
        UINT64_C(0x57c2c41692bb501c), UINT64_C(0x2d1ab4dea28ed965),
        UINT64_C(0x624ac0f56a91f461), UINT64_C(0x1892b03d5aa47d18),
        UINT64_C(0x97fa21650afae693), UINT64_C(0xed2251ad3acf6fea),
        UINT64_C(0x095ac9329ac4bc9b), UINT64_C(0x7382b9faaaf135e2),
        UINT64_C(0xfcea28a2faafae69), UINT64_C(0x8632586aca9a2710),
        UINT64_C(0xc9622c4102850a14), UINT64_C(0xb3ba5c8932b0836d),
        UINT64_C(0x3cd2cdd162ee18e6), UINT64_C(0x460abd1952db919f),
        UINT64_C(0x256b24ca6b12f26d), UINT64_C(0x5fb354025b277b14),
        UINT64_C(0xd0dbc55a0b79e09f), UINT64_C(0xaa03b5923b4c69e6),
        UINT64_C(0xe553c1b9f35344e2), UINT64_C(0x9f8bb171c366cd9b),
        UINT64_C(0x10e3202993385610), UINT64_C(0x6a3b50e1a30ddf69),
        UINT64_C(0x8e43c87e03060c18), UINT64_C(0xf49bb8b633338561),
        UINT64_C(0x7bf329ee636d1eea), UINT64_C(0x012b592653589793),
        UINT64_C(0x4e7b2d0d9b47ba97), UINT64_C(0x34a35dc5ab7233ee),
        UINT64_C(0xbbcbcc9dfb2ca865), UINT64_C(0xc113bc55cb19211c),
        UINT64_C(0x5863dbf1e3ac9dec), UINT64_C(0x22bbab39d3991495),
        UINT64_C(0xadd33a6183c78f1e), UINT64_C(0xd70b4aa9b3f20667),
        UINT64_C(0x985b3e827bed2b63), UINT64_C(0xe2834e4a4bd8a21a),
        UINT64_C(0x6debdf121b863991), UINT64_C(0x1733afda2bb3b0e8),
        UINT64_C(0xf34b37458bb86399), UINT64_C(0x8993478dbb8deae0),
        UINT64_C(0x06fbd6d5ebd3716b), UINT64_C(0x7c23a61ddbe6f812),
        UINT64_C(0x3373d23613f9d516), UINT64_C(0x49aba2fe23cc5c6f),
        UINT64_C(0xc6c333a67392c7e4), UINT64_C(0xbc1b436e43a74e9d),
        UINT64_C(0x95ac9329ac4bc9b5), UINT64_C(0xef74e3e19c7e40cc),
        UINT64_C(0x601c72b9cc20db47), UINT64_C(0x1ac40271fc15523e),
        UINT64_C(0x5594765a340a7f3a), UINT64_C(0x2f4c0692043ff643),
        UINT64_C(0xa02497ca54616dc8), UINT64_C(0xdafce7026454e4b1),
        UINT64_C(0x3e847f9dc45f37c0), UINT64_C(0x445c0f55f46abeb9),
        UINT64_C(0xcb349e0da4342532), UINT64_C(0xb1eceec59401ac4b),
        UINT64_C(0xfebc9aee5c1e814f), UINT64_C(0x8464ea266c2b0836),
        UINT64_C(0x0b0c7b7e3c7593bd), UINT64_C(0x71d40bb60c401ac4),
        UINT64_C(0xe8a46c1224f5a634), UINT64_C(0x927c1cda14c02f4d),
        UINT64_C(0x1d148d82449eb4c6), UINT64_C(0x67ccfd4a74ab3dbf),
        UINT64_C(0x289c8961bcb410bb), UINT64_C(0x5244f9a98c8199c2),
        UINT64_C(0xdd2c68f1dcdf0249), UINT64_C(0xa7f41839ecea8b30),
        UINT64_C(0x438c80a64ce15841), UINT64_C(0x3954f06e7cd4d138),
        UINT64_C(0xb63c61362c8a4ab3), UINT64_C(0xcce411fe1cbfc3ca),
        UINT64_C(0x83b465d5d4a0eece), UINT64_C(0xf96c151de49567b7),
        UINT64_C(0x76048445b4cbfc3c), UINT64_C(0x0cdcf48d84fe7545),
        UINT64_C(0x6fbd6d5ebd3716b7), UINT64_C(0x15651d968d029fce),
        UINT64_C(0x9a0d8ccedd5c0445), UINT64_C(0xe0d5fc06ed698d3c),
        UINT64_C(0xaf85882d2576a038), UINT64_C(0xd55df8e515432941),
        UINT64_C(0x5a3569bd451db2ca), UINT64_C(0x20ed197575283bb3),
        UINT64_C(0xc49581ead523e8c2), UINT64_C(0xbe4df122e51661bb),
        UINT64_C(0x3125607ab548fa30), UINT64_C(0x4bfd10b2857d7349),
        UINT64_C(0x04ad64994d625e4d), UINT64_C(0x7e7514517d57d734),
        UINT64_C(0xf11d85092d094cbf), UINT64_C(0x8bc5f5c11d3cc5c6),
        UINT64_C(0x12b5926535897936), UINT64_C(0x686de2ad05bcf04f),
        UINT64_C(0xe70573f555e26bc4), UINT64_C(0x9ddd033d65d7e2bd),
        UINT64_C(0xd28d7716adc8cfb9), UINT64_C(0xa85507de9dfd46c0),
        UINT64_C(0x273d9686cda3dd4b), UINT64_C(0x5de5e64efd965432),
        UINT64_C(0xb99d7ed15d9d8743), UINT64_C(0xc3450e196da80e3a),
        UINT64_C(0x4c2d9f413df695b1), UINT64_C(0x36f5ef890dc31cc8),
        UINT64_C(0x79a59ba2c5dc31cc), UINT64_C(0x037deb6af5e9b8b5),
        UINT64_C(0x8c157a32a5b7233e), UINT64_C(0xf6cd0afa9582aa47),
        UINT64_C(0x4ad64994d625e4da), UINT64_C(0x300e395ce6106da3),
        UINT64_C(0xbf66a804b64ef628), UINT64_C(0xc5bed8cc867b7f51),
        UINT64_C(0x8aeeace74e645255), UINT64_C(0xf036dc2f7e51db2c),
        UINT64_C(0x7f5e4d772e0f40a7), UINT64_C(0x05863dbf1e3ac9de),
        UINT64_C(0xe1fea520be311aaf), UINT64_C(0x9b26d5e88e0493d6),
        UINT64_C(0x144e44b0de5a085d), UINT64_C(0x6e963478ee6f8124),
        UINT64_C(0x21c640532670ac20), UINT64_C(0x5b1e309b16452559),
        UINT64_C(0xd476a1c3461bbed2), UINT64_C(0xaeaed10b762e37ab),
        UINT64_C(0x37deb6af5e9b8b5b), UINT64_C(0x4d06c6676eae0222),
        UINT64_C(0xc26e573f3ef099a9), UINT64_C(0xb8b627f70ec510d0),
        UINT64_C(0xf7e653dcc6da3dd4), UINT64_C(0x8d3e2314f6efb4ad),
        UINT64_C(0x0256b24ca6b12f26), UINT64_C(0x788ec2849684a65f),
        UINT64_C(0x9cf65a1b368f752e), UINT64_C(0xe62e2ad306bafc57),
        UINT64_C(0x6946bb8b56e467dc), UINT64_C(0x139ecb4366d1eea5),
        UINT64_C(0x5ccebf68aecec3a1), UINT64_C(0x2616cfa09efb4ad8),
        UINT64_C(0xa97e5ef8cea5d153), UINT64_C(0xd3a62e30fe90582a),
        UINT64_C(0xb0c7b7e3c7593bd8), UINT64_C(0xca1fc72bf76cb2a1),
        UINT64_C(0x45775673a732292a), UINT64_C(0x3faf26bb9707a053),
        UINT64_C(0x70ff52905f188d57), UINT64_C(0x0a2722586f2d042e),
        UINT64_C(0x854fb3003f739fa5), UINT64_C(0xff97c3c80f4616dc),
        UINT64_C(0x1bef5b57af4dc5ad), UINT64_C(0x61372b9f9f784cd4),
        UINT64_C(0xee5fbac7cf26d75f), UINT64_C(0x9487ca0fff135e26),
        UINT64_C(0xdbd7be24370c7322), UINT64_C(0xa10fceec0739fa5b),
        UINT64_C(0x2e675fb4576761d0), UINT64_C(0x54bf2f7c6752e8a9),
        UINT64_C(0xcdcf48d84fe75459), UINT64_C(0xb71738107fd2dd20),
        UINT64_C(0x387fa9482f8c46ab), UINT64_C(0x42a7d9801fb9cfd2),
        UINT64_C(0x0df7adabd7a6e2d6), UINT64_C(0x772fdd63e7936baf),
        UINT64_C(0xf8474c3bb7cdf024), UINT64_C(0x829f3cf387f8795d),
        UINT64_C(0x66e7a46c27f3aa2c), UINT64_C(0x1c3fd4a417c62355),
        UINT64_C(0x935745fc4798b8de), UINT64_C(0xe98f353477ad31a7),
        UINT64_C(0xa6df411fbfb21ca3), UINT64_C(0xdc0731d78f8795da),
        UINT64_C(0x536fa08fdfd90e51), UINT64_C(0x29b7d047efec8728),
};

uint64_t crc64(uint64_t crc, const unsigned char *s, uint64_t l) {
    uint64_t j;

    for (j = 0; j < l; j++) {
        uint8_t byte = s[j];
        crc = crc64_tab[(uint8_t)crc ^ byte] ^ (crc >> 8);
    }
    return crc;
}

template <typename T>
size_t easyCopy(std::vector<byte>& buf, size_t& pos, T element) {
    if (pos + sizeof(T) > buf.size()) {
        buf.resize(pos + sizeof(T));
    }
    auto* ptr = reinterpret_cast<byte*>(&element);
    std::copy(ptr, (ptr + sizeof(T)), buf.begin() + pos);
    pos += sizeof(T);
    return sizeof(T);
}

template <typename T>
size_t easyCopy(std::vector<byte>& buf, size_t& pos, const T *array, size_t len) {
    if (pos + len > buf.size()) {
        buf.resize(pos + len * sizeof(T));
    }
    auto* ptr = const_cast<byte*>(reinterpret_cast<const byte*>(array));
    std::copy(ptr, (ptr + len * sizeof(T)), buf.begin() + pos);
    pos += len * sizeof(T);
    return len * sizeof(T);
}

template <typename T>
size_t easyCopy(T *dest, const std::string &buf, size_t &pos) {
    if (buf.size() < pos) {
        return 0;
    }
    auto *ptr = reinterpret_cast<byte*>(dest);
    std::copy(&buf[pos], &buf[pos + sizeof(T)], ptr);
    pos += sizeof(T);
    return sizeof(T);
}

void memrev64(void *p) {
    byte *x = reinterpret_cast<byte*>(p), t;

    t = x[0];
    x[0] = x[7];
    x[7] = t;
    t = x[1];
    x[1] = x[6];
    x[6] = t;
    t = x[2];
    x[2] = x[5];
    x[5] = t;
    t = x[3];
    x[3] = x[4];
    x[4] = t;
}

uint64_t htonll(uint64_t v) {
    memrev64(&v);
    return v;
}

uint64_t ntohll(uint64_t v) {
    memrev64(&v);
    return v;
}

// dump
// Base class
Serializer::Serializer(Session *sess, const std::string& key, DumpType type)
        : _sess(sess), _key(key), _type(type), _pos(0) {
}

Expected<size_t> Serializer::saveObjectType(std::vector<byte> &payload, size_t &pos, DumpType type) {
    return easyCopy(payload, pos, type);
}

Expected<size_t> Serializer::saveLen(std::vector<byte>& payload, size_t& pos, size_t len) {
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
        uint64_t len64 = htonll(static_cast<uint64_t>(len));
        return (1 + easyCopy(payload, pos, len64));
    }
}

size_t Serializer::saveString(std::vector<byte> &payload, size_t &pos, const std::string &str) {
    size_t written(0);
    auto wr = Serializer::saveLen(payload, pos, str.size());
    INVARIANT(wr.value() > 0);
    written += wr.value();
    written += easyCopy(payload, pos, str.c_str(), str.size());
    return written;
}

Expected<std::vector<byte>> Serializer::dump() {
    std::vector<byte> payload;

    // TODO: imo input param may include key's real type stored in the redis cache.
    Serializer::saveObjectType(payload, _pos, _type);
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
    easyCopy(payload, _pos, version, 2);

    // TODO: add crc64, also want little endian.
    uint64_t crc = crc64(0, &payload[_begin], _pos - _begin);
    easyCopy(payload, _pos, crc);
    _end = _pos;
    return payload;
}

// Command who can only see base class Serializer.
class DumpCommand: public Command {
public:
    DumpCommand()
        :Command("dump") {
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

        auto expdb = sess->getServerEntry()->getSegmentMgr()->getDbWithKeyLock(sess, key, mgl::LockMode::LOCK_X);
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

        // expect our g++ compiler will do cow on this... as it should do.
        std::string output(buf.begin() + exps.value()->_begin, buf.begin() + exps.value()->_end);
        return Command::fmtBulk(output);
    }
} dumpCommand;

// derived classes, each of them should handle the dump of different object.
class KvSerializer: public Serializer {
public:
    explicit KvSerializer(Session *sess, const std::string& key, RecordValue&& rv)
        :Serializer(sess, key, DumpType::RDB_TYPE_STRING),
        _rv(std::forward<RecordValue>(rv)){
    }

    Expected<size_t> dumpObject(std::vector<byte>& payload) {
        // TODO: add other encoding format such as long long or lzf compression.
        Serializer::saveString(payload, _pos, _rv.getValue());
        _begin = 0;
        return _pos - _begin;
        // return { ErrorCodes::ERR_INTERNAL, "Not implemented" };
    }

private:
    RecordValue _rv;
};

class ListSerializer: public Serializer {
public:
    explicit ListSerializer(Session *sess, const std::string& key, RecordValue&& rv)
        :Serializer(sess, key, DumpType::RDB_TYPE_QUICKLIST),
        _rv(std::forward<RecordValue>(rv)){
    }

    Expected<size_t> dumpObject(std::vector<byte>& payload) {
        size_t qlbytes(0);
        auto expwr = saveLen(payload, _pos, 1);
        if (!expwr.ok()) {
            return expwr.status();
        }
        size_t notAligned = _pos;
        size_t qlEnd = notAligned + 9;

        {
            // make room for quicklist length first, remember to move first several bytes to align after.
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

        auto expdb = _sess->getServerEntry()->getSegmentMgr()->getDbHasLocked(_sess, _key);
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
        easyCopy(payload, _pos, zllen);
        for (size_t i = head; i != tail; i++) {
            RecordKey nodeKey(expdb.value().chunkId, _sess->getCtx()->getDbId(),
                    RecordType::RT_LIST_ELE, _key, std::to_string(i));
            Expected<RecordValue> expNodeVal = kvstore->getKV(nodeKey, txn.get());
            if (!expNodeVal.ok()) {
                return expNodeVal.status();
            }
            RecordValue nodeVal = std::move(expNodeVal.value());
            size_t written(0);
            // prevlen
            if (prevlen > 254) {
                written += easyCopy(payload, _pos, static_cast<unsigned char>(0xfe));
                written += easyCopy(payload, _pos, prevlen);
            } else {
                written += easyCopy(payload, _pos, static_cast<unsigned char>(prevlen));
            }

            written += saveString(payload, _pos, nodeVal.getValue());
            prevlen = written;
            zlbytes += written;
        }
        zlbytes += easyCopy(payload, _pos, static_cast<unsigned char>(0xff));
        zltail = zlbytes - 1 - prevlen;
        easyCopy(payload, zlInitPos, zlbytes);
        easyCopy(payload, zlInitPos, zltail);

        qlbytes += zlbytes;
        auto expQlUsed = saveLen(payload, notAligned, qlbytes);
        if (!expQlUsed.ok()) {
            return expQlUsed.status();
        }
        if (expQlUsed.value() < 9) {
            auto offset = notAligned;
            std::copy_backward(payload.begin(), payload.begin() + offset, payload.begin() + qlEnd);
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
    explicit SetSerializer(Session *sess, const std::string &key, RecordValue&& rv)
        :Serializer(sess, key, DumpType::RDB_TYPE_SET),
        _rv(std::move(rv)){
    }

    Expected<size_t> dumpObject(std::vector<byte>& payload) {
        Expected<SetMetaValue> expMeta = SetMetaValue::decode(_rv.getValue());
        size_t len = expMeta.value().getCount();
        INVARIANT(len > 0);

        auto expwr = saveLen(payload, _pos, len);
        if (!expwr.ok()) {
            return expwr.status();
        }

        // traverse the whole set, save it as its a normal sds.
        auto expdb = _sess->getServerEntry()->getSegmentMgr()->getDbHasLocked(_sess, _key);
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
        RecordKey fakeRk(expdb.value().chunkId, _sess->getCtx()->getDbId(), RecordType::RT_SET_ELE, _key, "");
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
            Serializer::saveString(payload, _pos, subk);
        }

        _begin = 0;
        return _pos - _begin;
    }

private:
    RecordValue _rv;
};

class ZsetSerializer: public Serializer {
public:
    explicit ZsetSerializer(Session *sess, const std::string& key, RecordValue&& rv)
        :Serializer(sess, key, DumpType::RDB_TYPE_ZSET),
        _rv(std::move(rv)) {
    }

    Expected<size_t> dumpObject(std::vector<byte>& payload) {
        auto expdb = _sess->getServerEntry()->getSegmentMgr()->getDbHasLocked(_sess, _key);
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
        SkipList zsl(expdb.value().chunkId, _sess->getCtx()->getDbId(), _key, meta, kvstore);

        auto expwr = saveLen(payload, _pos, zsl.getCount() - 1);
        if (!expwr.ok()) {
            return expwr.status();
        }

        auto rev = zsl.scanByRank(0, zsl.getCount() - 1, true, txn.get());
        if (!rev.ok()) {
            return rev.status();
        }
        for (auto& ele : rev.value()) {
            Serializer::saveString(payload, _pos, std::forward<std::string>(ele.second));
            // save binary double score
            double score = ele.first;
            easyCopy(payload, _pos, score);
        }
        _begin = 0;
        return _pos - _begin;
    }

private:
    RecordValue _rv;
};

class HashSerializer: public Serializer {
public:
    explicit HashSerializer(Session *sess, const std::string &key, RecordValue&& rv)
        :Serializer(sess, key, DumpType::RDB_TYPE_HASH),
        _rv(std::forward<RecordValue>(rv)){
    }

    Expected<size_t> dumpObject(std::vector<byte> &payload) {
        Expected<HashMetaValue> expHashMeta = HashMetaValue::decode(_rv.getValue());
        if (!expHashMeta.ok()) {
            return expHashMeta.status();
        }
        auto expwr = saveLen(payload, _pos, expHashMeta.value().getCount());
        if (!expwr.ok()) {
            return expwr.status();
        }

        auto expdb = _sess->getServerEntry()->getSegmentMgr()->getDbHasLocked(_sess, _key);
        if (!expdb.ok()) {
            return expdb.status();
        }

        PStore kvstore = expdb.value().store;
        auto ptxn = kvstore->createTransaction();
        if (!ptxn.ok()) {
            return ptxn.status();
        }
        std::unique_ptr<Transaction> txn = std::move(ptxn.value());

        RecordKey fakeRk(expdb.value().chunkId, _sess->getCtx()->getDbId(),
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
            const std::string &field = expRcd.value().getRecordKey().getSecondaryKey();
            const std::string &value = expRcd.value().getRecordValue().getValue();
            Serializer::saveString(payload, _pos, field);
            Serializer::saveString(payload, _pos, value);
        }
        _begin = 0;
        return _pos - _begin;
    }

private:
    RecordValue _rv;
};

// outlier function
Expected<std::unique_ptr<Serializer>> getSerializer(Session *sess, const std::string &key) {
    Expected<RecordValue> rv = Command::expireKeyIfNeeded(sess, key, RecordType::RT_DATA_META);
    if (!rv.ok()) {
        return rv.status();
    }

    std::unique_ptr<Serializer> ptr;
    auto type = rv.value().getRecordType();
    switch (type) {
        case RecordType::RT_KV:
            ptr = std::move(std::unique_ptr<Serializer>(
                    dynamic_cast<Serializer*>(new KvSerializer(sess, key, std::move(rv.value())))
            ));
            break;
        case RecordType::RT_LIST_META:
            ptr = std::move(std::unique_ptr<Serializer>(
                    dynamic_cast<Serializer*>(new ListSerializer(sess, key, std::move(rv.value())))
                    ));
            break;
        case RecordType::RT_HASH_META:
            ptr = std::move(std::unique_ptr<Serializer>(
                    dynamic_cast<Serializer*>(new HashSerializer(sess, key, std::move(rv.value())))
            ));
            break;
        case RecordType::RT_SET_META:
            ptr = std::move(std::unique_ptr<Serializer>(
                    dynamic_cast<Serializer*>(new SetSerializer(sess, key, std::move(rv.value())))
            ));
            break;
        case RecordType::RT_ZSET_META:
            ptr = std::move(std::unique_ptr<Serializer>(
                    dynamic_cast<Serializer*>(new ZsetSerializer(sess, key, std::move(rv.value())))
            ));
            break;
        default:
            return {ErrorCodes::ERR_WRONG_TYPE, "type can not be dumped"};
    }

    return std::move(ptr);
}

// restore
Deserializer::Deserializer(Session *sess, const std::string &payload, const std::string &key, const uint64_t ttl)
    : _sess(sess), _payload(payload), _key(key), _ttl(ttl), _pos(1) {}

Expected<DumpType> Deserializer::loadObjectType(const std::string &payload, size_t &&pos) {
    uint8_t t;
    easyCopy(&t, payload, pos);
    return static_cast<DumpType>(t);
}

Expected<size_t> Deserializer::loadLen(const std::string &payload, size_t &pos) {
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
        ret = ntohll(len64);
    } else {
        return {ErrorCodes::ERR_INTERNAL, "Unknown length encoding"};
    }
    return ret;
}

std::string Deserializer::loadString(const std::string &payload, size_t &pos) {
    auto expLen = Deserializer::loadLen(payload, pos);
    if (!expLen.ok()) {
        return std::string("");
    }
    size_t len = expLen.value();
    pos += len;
    return std::string(payload.begin() + pos - len, payload.begin() + pos);
}

class RestoreCommand: public Command {
public:
    RestoreCommand()
        :Command("restore") {
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

    Status verifyDumpPayload(const std::string &payload) {
        uint8_t buf[2];
        uint64_t crc;

        size_t len = payload.size();
        if (len < 10) {
            return { ErrorCodes::ERR_INTERNAL, "len cannot be lt 10" };
        }
        size_t chkpos = len - 10;
        INVARIANT(easyCopy(&buf[0], payload, chkpos) == 1 &&
                  easyCopy(&buf[1], payload, chkpos) == 1);

        uint16_t rdbver = (buf[1] << 8) | buf[0];
        if (rdbver > RDB_VERSION) {
            return { ErrorCodes::ERR_INTERNAL, "rdb version not match" };
        }

        crc = crc64(0, reinterpret_cast<const byte*>(payload.c_str()), chkpos);
        if (memcmp(&crc, payload.c_str() + chkpos, 8) == 0) {
            return { ErrorCodes::ERR_OK, "OK" };
        }
        return { ErrorCodes::ERR_INTERNAL, "crc not match" };
    }

    Expected<std::string> run(Session *sess) final {
        const std::string &key = sess->getArgs()[1];
        const std::string &sttl = sess->getArgs()[2];
        const std::string &payload = sess->getArgs()[3];

        // TODO: parse additional args
        // maybe we don't need to do this.
        {

        }

        // check if key exists
        Expected<RecordValue> rv = Command::expireKeyIfNeeded(sess, key, RecordType::RT_DATA_META);
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
            return Command::fmtErr("Invalid TTL value, must be >= 0");
        }

        Status chk = verifyDumpPayload(payload);
        if (!chk.ok()) {
            return Command::fmtErr("DUMP payload version or checksum are wrong");
        }

        // do restore
        auto expds = getDeserializer(sess, payload, key, expttl.value());
        if (!expds.ok()) {
            return expds.status();
        }

        Status res = expds.value()->restore();
        if (res.ok()) {
            return Command::fmtOK();
        }
        return Command::fmtErr(res.toString());
    }
} restoreCommand;

class KvDeserializer: public Deserializer {
public:
    explicit KvDeserializer(Session *sess, const std::string &payload, const std::string &key, const uint64_t ttl)
        : Deserializer(sess, payload, key, ttl) {
    }

    virtual Status restore() {
        auto ret = Deserializer::loadString(_payload, _pos);

        auto expdb = _sess->getServerEntry()->getSegmentMgr()->getDbHasLocked(_sess, _key);
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

        RecordKey rk(expdb.value().chunkId, pCtx->getDbId(), RecordType::RT_KV, _key, "");
        uint64_t ts = 0;
        if (_ttl > 0) {
            ts = msSinceEpoch() + _ttl;
        }
        RecordValue rv(ret, RecordType::RT_KV, ts);
        for (int32_t i = 0; i < Command::RETRY_CNT - 1; ++i) {
            auto result = setGeneric(kvstore, txn.get(), 0, rk, rv, "", "");
            if (result.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
                return result.status();
            }
            ptxn = kvstore->createTransaction();
            if (!ptxn.ok()) {
                return ptxn.status();
            }
            txn = std::move(ptxn.value());
        }
        auto result = setGeneric(kvstore, txn.get(), 0,
                          rk, rv, "", "");
        if (!result.ok()) {
            return result.status();
        }
        return { ErrorCodes::ERR_OK, "OK"};
    }
};

class SetDeserializer: public Deserializer {
public:
    explicit SetDeserializer(Session *sess, const std::string &payload, const std::string &key, const uint64_t ttl)
        : Deserializer(sess, payload, key, ttl) {
    }

    virtual Status restore() {
        auto expLen = Deserializer::loadLen(_payload, _pos);
        if (!expLen.ok()) {
            return expLen.status();
        }

        size_t len = expLen.value();
        std::vector<std::string> set(2);

        while (len--) {
            std::string ele = loadString(_payload, _pos);
            set.push_back(std::move(ele));
        }

        auto expdb = _sess->getServerEntry()->getSegmentMgr()->getDbHasLocked(_sess, _key);
        if (!expdb.ok()) {
            return expdb.status();
        }
        PStore kvstore = expdb.value().store;
        RecordKey setRk(expdb.value().chunkId, _sess->getCtx()->getDbId(), RecordType::RT_SET_META, _key, "");
        for (uint32_t i = 0; i < tendisplus::Command::RETRY_CNT; ++i) {
            auto ptxn = kvstore->createTransaction();
            if (!ptxn.ok()) {
                return ptxn.status();
            }
            std::unique_ptr<Transaction> txn = std::move(ptxn.value());
            Expected<std::string> add = genericSAdd(_sess, kvstore, setRk, set);
            if (add.ok()) {
                return { ErrorCodes::ERR_OK, "OK" };
            }
            if (add.status().code() != ErrorCodes::ERR_COMMIT_RETRY) {
                return add.status();
            }
            if (i == Command::RETRY_CNT - 1) {
                return add.status();
            } else {
                continue;
            }
        }
        return { ErrorCodes::ERR_INTERNAL, "not reachable" };
    }
};

class ZsetDeserializer: public Deserializer {
public:
    explicit ZsetDeserializer(Session *sess, const std::string &payload, const std::string &key, const uint64_t ttl)
        : Deserializer(sess, payload, key, ttl) {
    }

    virtual Status restore() {
        auto expLen = loadLen(_payload, _pos);
        if (!expLen.ok()) {
            return expLen.status();
        }

        size_t len = expLen.value();
        std::map<std::string, double> scoreMap;
        while (len--) {
            std::string ele = loadString(_payload, _pos);
            double score;
            INVARIANT(easyCopy(&score, _payload, _pos) == 8);
            scoreMap[ele] = score;
        }

        auto expdb = _sess->getServerEntry()->getSegmentMgr()->getDbHasLocked(_sess, _key);
        if (!expdb.ok()) {
            return expdb.status();
        }

        RecordKey rk(expdb.value().chunkId, _sess->getCtx()->getDbId(), RecordType::RT_ZSET_META, _key, "");
        PStore kvstore = expdb.value().store;
        for (int32_t i = 0; i < Command::RETRY_CNT; ++i) {
            // maybe very slow
            Expected<std::string> res = genericZadd(_sess, kvstore, rk, scoreMap, ZADD_NONE);
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
    explicit HashDeserializer(Session *sess, const std::string &payload, const std::string &key, const uint64_t ttl)
        : Deserializer(sess, payload, key, ttl) {
    }

    virtual Status restore() {
        auto expLen = loadLen(_payload, _pos);
        if (!expLen.ok()) {
            return expLen.status();
        }

        size_t len = expLen.value();
        auto expdb = _sess->getServerEntry()->getSegmentMgr()->getDbHasLocked(_sess, _key);
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
            std::string field = loadString(_payload, _pos);
            std::string value = loadString(_payload, _pos);
            // need existence check ?
            RecordKey rk(expdb.value().chunkId, _sess->getCtx()->getDbId(),
                    RecordType::RT_HASH_ELE, _key, field);
            RecordValue rv(value, RecordType::RT_HASH_ELE);
            Status s = kvstore->setKV(rk, rv, txn.get());
            if (!s.ok()) {
                return s;
            }
        }

        RecordKey metaRk(expdb.value().chunkId, _sess->getCtx()->getDbId(),
                RecordType::RT_HASH_META, _key, "");
        HashMetaValue hashMeta;
        hashMeta.setCount(len);
        RecordValue metaRv(std::move(hashMeta.encode()), RecordType::RT_HASH_META, _ttl);
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
    std::vector<std::string> deserializeZiplist(const std::string &payload, size_t &pos) {
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
                pos += 5;
            } else {
                pos += 1;
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
    explicit ListDeserializer(Session *sess, const std::string &payload, const std::string &key, const uint64_t ttl)
        : Deserializer(sess, payload, key, ttl) {
    }

    virtual Status restore() {
        auto qlExpLen = loadLen(_payload, _pos);
        if (!qlExpLen.ok()) {
            return qlExpLen.status();
        }

        size_t qlLen = qlExpLen.value();
        auto expdb = _sess->getServerEntry()->getSegmentMgr()->getDbHasLocked(_sess, _key);
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
            auto expLen = loadLen(_payload, _pos);
            if (!expLen.ok()) {
                return expLen.status();
            }

            std::vector<std::string> zl = deserializeZiplist(_payload, _pos);
            uint64_t idx;
            for (auto iter = zl.begin(); iter != zl.end(); iter++) {
                idx = tail++;
                RecordKey rk(metaRk.getChunkId(),
                             metaRk.getDbId(),
                             RecordType::RT_LIST_ELE,
                             metaRk.getPrimaryKey(),
                             std::to_string(idx));
                RecordValue rv(std::move(*iter), RecordType::RT_LIST_ELE);
                Status s = kvstore->setKV(rk, rv, txn.get());
                if (!s.ok()) {
                    return s;
                }
            }
        }
        lm.setHead(head);
        lm.setTail(tail);
        RecordValue metaRv(lm.encode(), RecordType::RT_LIST_META, _ttl);
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

Expected<std::unique_ptr<Deserializer>> getDeserializer(Session *sess, const std::string &payload,
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
                    dynamic_cast<Deserializer*>(new KvDeserializer(sess, payload, key, ttl))
                    ));
            break;
        case DumpType::RDB_TYPE_SET:
            ptr = std::move(std::unique_ptr<Deserializer>(
                    dynamic_cast<Deserializer*>(new SetDeserializer(sess, payload, key, ttl))
            ));
            break;
        case DumpType::RDB_TYPE_ZSET:
            ptr = std::move(std::unique_ptr<Deserializer>(
                    dynamic_cast<Deserializer*>(new ZsetDeserializer(sess, payload, key, ttl))
            ));
            break;
        case DumpType::RDB_TYPE_HASH:
            ptr = std::move(std::unique_ptr<Deserializer>(
                    dynamic_cast<Deserializer*>(new HashDeserializer(sess, payload, key, ttl))
            ));
            break;
        case DumpType::RDB_TYPE_QUICKLIST:
            ptr = std::move(std::unique_ptr<Deserializer>(
                    dynamic_cast<Deserializer*>(new ListDeserializer(sess, payload, key, ttl))
            ));
            break;
        default:
            return {ErrorCodes::ERR_INTERNAL, "Not implemented"};
            break;
    }
    return std::move(ptr);
}

} // namespace tendisplus