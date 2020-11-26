// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <string>
#include "tendisplus/storage/varint.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/include/endian.h"

namespace tendisplus {

size_t varintMaxSize(size_t size) {
  switch (size) {
    case sizeof(uint64_t):
      return 10;
      break;
    case sizeof(uint32_t):
      return 5;
    default:
      INVARIANT(0);
      break;
  }

  return 0;
}

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

std::string varintEncodeStr(uint64_t val) {
  uint8_t buf[16];
  size_t size = varintEncodeBuf(&buf[0], sizeof(buf), val);
  INVARIANT_D(size < sizeof(buf));
  return std::string(reinterpret_cast<char*>(&buf[0]), size);
}

size_t varintEncodeBuf(uint8_t* buf, size_t bufsize, uint64_t val) {
  size_t size = 0;
  while (val >= 0x80) {
    buf[size++] = (0x80 | (val & 0x7f));
    val >>= 7;
  }
  // The last byte always < 0x80
  buf[size++] = (uint8_t)val;

  INVARIANT_D(size <= bufsize);
  return size;
}

size_t varintEncodeSize(uint64_t val) {
  size_t size = 0;
  while (val >= 0x80) {
    size++;
    val >>= 7;
  }
  size++;
  return size;
}

Expected<VarintDecodeResult> varintDecodeFwd(const uint8_t* input,
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
Expected<VarintDecodeResult> varintDecodeRvs(const uint8_t* input,
                                             size_t maxSize) {
  uint64_t ret = 0;
  const uint8_t* p = input;
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

  for (uint32_t i = 0; i < sizeof(val); ++i) {
    result.emplace_back(ptr[i]);
  }

  return result;
}

Expected<double> doubleDecode(const uint8_t* input, size_t maxSize) {
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
  INVARIANT_D(input.size() == 8);
  return doubleDecode(reinterpret_cast<const uint8_t*>(input.c_str()),
                      input.size());
}

uint16_t int16Encode(uint16_t input) {
  return htobe16(input);
}

size_t int16Encode(char* dest, uint16_t input) {
  uint16_t v = htobe16(input);
  memcpy(dest, &v, sizeof(v));
  return sizeof(v);
}

uint16_t int16Decode(const char* input) {
#ifdef USE_ALIGNED_ACCESS
  uint16_t tmp;
  memcpy(&tmp, input, sizeof(uint16_t));
  return be16toh(tmp);
#else
  return be16toh(*reinterpret_cast<const uint16_t*>(input));
#endif
}

size_t int32Encode(char* dest, uint32_t input) {
  uint32_t v = htobe32(input);
  memcpy(dest, &v, sizeof(v));
  return sizeof(v);
}

uint32_t int32Encode(uint32_t input) {
  return htobe32(input);
}

uint32_t int32Decode(const char* input) {
#ifdef USE_ALIGNED_ACCESS
  uint32_t tmp;
  memcpy(&tmp, input, sizeof(uint32_t));
  return be32toh(tmp);
#else
  return be32toh(*reinterpret_cast<const uint32_t*>(input));
#endif
}

uint64_t int64Encode(uint64_t input) {
  return htobe64(input);
}

size_t int64Encode(char* dest, uint64_t input) {
  uint64_t v = htobe64(input);
  memcpy(dest, &v, sizeof(v));
  return sizeof(v);
}

uint64_t int64Decode(const char* input) {
#ifdef USE_ALIGNED_ACCESS
  uint64_t tmp;
  memcpy(&tmp, input, sizeof(uint64_t));
  return be64toh(tmp);
#else
  return be64toh(*reinterpret_cast<const uint64_t*>(input));
#endif
}


}  // namespace tendisplus
