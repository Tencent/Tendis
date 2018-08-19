#include "tendisplus/storage/varint.h"

namespace tendisplus {

std::vector<uint8_t> varintEncode(uint64_t val) {
    std::vector<uint8_t> result;
    while (val >= 128) {
        result.emplace_back(0x80 | (val & 0x7f));
        val >>= 7;
    }
    result.emplace_back(uint8_t(val));
    return result;
}

uint64_t varintDecodeFwd(uint8_t *input, size_t maxSize) {
    uint64_t ret = 0;
    for (size_t i = 0; i < maxSize; i++) {
        ret |= uint64_t(input[i] & 127) << (7 * i);
        if (!(input[i] & 128)) {
            break;
        }
    }
    return ret;
}

// NOTE(deyukong): read the headfile before use this function
uint64_t varintDecodeRvs(uint8_t *input, size_t maxSize) {
    uint64_t ret = 0;
    uint8_t *p = input;
    for (size_t i = 0; i < maxSize; i++) {
        ret |= uint64_t((*p) & 127) << (7 * i);
        if (!((*p) & 128)) {
            break;
        }
        p--;
    }
    return ret;
}

}  // namespace tendisplus
