#include "tendisplus/storage/varint.h"

namespace tendisplus {

std::vector<uint8_t> varintEncode(uint64_t val) {
    std::vector<uint8_t> result;
    while (val >= 0x80) {
        result.emplace_back(0x80 | (val & 0x7f));
        val >>= 7;
    }
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

}  // namespace tendisplus
