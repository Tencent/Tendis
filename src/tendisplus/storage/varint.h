// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_STORAGE_VARINT_H_
#define SRC_TENDISPLUS_STORAGE_VARINT_H_

#include <stdint.h>
#include <cstdlib>
#include <vector>
#include <utility>
#include <string>
#include "tendisplus/utils/status.h"

namespace tendisplus {

using VarintDecodeResult = std::pair<uint64_t, size_t>;

size_t varintMaxSize(size_t size);
std::vector<uint8_t> varintEncode(uint64_t val);
std::string varintEncodeStr(uint64_t val);
size_t varintEncodeBuf(uint8_t* buf, size_t bufsize, uint64_t val);
size_t varintEncodeSize(uint64_t val);

Expected<VarintDecodeResult> varintDecodeFwd(const uint8_t* input,
                                             size_t maxSize);

// NOTE(deyukong): if you have a buff, named p, with size = maxSize
// you should write varintDecodeRvs(p+maxSize, maxSize)
// donot write varintDecodeRvs(p, maxSize)
Expected<VarintDecodeResult> varintDecodeRvs(const uint8_t* input,
                                             size_t maxSize);

std::vector<uint8_t> doubleEncode(double val);
Expected<double> doubleDecode(const uint8_t* input, size_t maxSize);
Expected<double> doubleDecode(const std::string& input);

uint16_t int16Encode(uint16_t input);
size_t int16Encode(char* dest, uint16_t input);
uint16_t int16Decode(const char* input);
uint32_t int32Encode(uint32_t input);
size_t int32Encode(char* dest, uint32_t input);
uint32_t int32Decode(const char* input);
uint64_t int64Encode(uint64_t input);
size_t int64Encode(char* dest, uint64_t input);
uint64_t int64Decode(const char* input);

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_STORAGE_VARINT_H_
