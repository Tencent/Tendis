#include <string>
#include "tendisplus/storage/varint.h"
#include "tendisplus/utils/invariant.h"

namespace tendisplus {

std::vector<uint8_t> varintEncode(uint64_t val) {
    std::vector<uint8_t> result;
    while (val >= 0x80) {
        result.emplace_back(0x80 | (val & 0x7f));
        val >>= 7;
    }
    // The last byte always < 0x80
    result.emplace_back(uint8_t(val));
    return result;
}

Expected<VarintDecodeResult> varintDecodeFwd(const uint8_t *input,
        size_t maxSize) {
    uint64_t ret = 0;
    size_t i = 0;
    for (; i < maxSize && (input[i] & 0x80); i++) {
        ret |= uint64_t(input[i] & 0x7f) << (7 * i);
    }
    if (i == maxSize) {
        return {ErrorCodes::ERR_DECODE, "decode varint maxlen"};
    }
    ret |= uint64_t(input[i] & 0x7f) << (7 * (i));
    i++;
    return VarintDecodeResult{ret, i};
}

// NOTE(deyukong): read the headfile before use this function
Expected<VarintDecodeResult> varintDecodeRvs(const uint8_t *input,
        size_t maxSize) {
    uint64_t ret = 0;
    const uint8_t *p = input;
    size_t i = 0;
    for (; i < maxSize && ((*p) & 0x80); i++, p--) {
        ret |= uint64_t((*p) & 0x7f) << (7 * i);
    }
    if (i == maxSize) {
        return {ErrorCodes::ERR_DECODE, "decode varint maxlen"};
    }
    ret |= uint64_t((*p) & 0x7f) << (7 * (i++));
    return VarintDecodeResult{ret, i};
}

std::vector<uint8_t> doubleEncode(double val) {
    std::vector<uint8_t> result;
    uint8_t* ptr = reinterpret_cast<uint8_t*>(&val);

    INVARIANT(sizeof(double) == 8);

    for (int i = 0; i < sizeof(val); ++i) {
        result.emplace_back(ptr[i]);
    }

    return result;
}

Expected<double> doubleDecode(const uint8_t *input, size_t maxSize) {
    if (maxSize < sizeof(double)) {
        return {ErrorCodes::ERR_DECODE, "decode double maxlen"};
    }

    double val = 0;
    uint8_t* ptr = reinterpret_cast<uint8_t*>(&val);

    for (size_t i = 0; i < sizeof(double); i++) {
        ptr[i] = input[i];
    }

    return val;
}

Expected<double> doubleDecode(const std::string& input) {
    INVARIANT(input.size() == 8);
    return doubleDecode(reinterpret_cast<const uint8_t*>(input.c_str()),
                        input.size());
}

}  // namespace tendisplus
