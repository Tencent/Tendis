#ifndef SRC_TENDISPLUS_STORAGE_VARINT_H_
#define SRC_TENDISPLUS_STORAGE_VARINT_H_

#include <stdint.h>
#include <cstdlib>
#include <vector>

namespace tendisplus {

std::vector<uint8_t> varintEncode(uint64_t val);

uint64_t varintDecodeFwd(uint8_t *input, size_t maxSize);

// NOTE(deyukong): if you have a buff, named p, with size = maxSize
// you should write varintDecodeRvs(p+maxSize, maxSize)
// donot write varintDecodeRvs(p, maxSize)
uint64_t varintDecodeRvs(uint8_t *input, size_t maxSize);

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_STORAGE_VARINT_H_
