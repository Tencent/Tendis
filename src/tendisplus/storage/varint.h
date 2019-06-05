#ifndef SRC_TENDISPLUS_STORAGE_VARINT_H_
#define SRC_TENDISPLUS_STORAGE_VARINT_H_

#include <stdint.h>
#include <cstdlib>
#include <vector>
#include <utility>
#include "tendisplus/utils/status.h"

namespace tendisplus {

using VarintDecodeResult = std::pair<uint64_t, size_t>;

size_t varintMaxSize(size_t size);
std::vector<uint8_t> varintEncode(uint64_t val);

Expected<VarintDecodeResult> varintDecodeFwd(const uint8_t *input,
        size_t maxSize);

// NOTE(deyukong): if you have a buff, named p, with size = maxSize
// you should write varintDecodeRvs(p+maxSize, maxSize)
// donot write varintDecodeRvs(p, maxSize)
Expected<VarintDecodeResult> varintDecodeRvs(const uint8_t *input,
        size_t maxSize);

std::vector<uint8_t> doubleEncode(double val);
Expected<double> doubleDecode(const uint8_t *input, size_t maxSize);
Expected<double> doubleDecode(const std::string& input);

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_STORAGE_VARINT_H_
