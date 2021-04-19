// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <string.h>
#include <inttypes.h>

#include <algorithm>
#include <string>
#include <iostream>
#include <cmath>
#include <cctype>
#include <locale>
#include <sstream>
#include <utility>
#include <vector>
#include <cstdlib>
#include <random>
#include <limits>
#include <thread>  // NOLINT

#include "tendisplus/utils/status.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/storage/varint.h"
#include "tendisplus/utils/invariant.h"

namespace tendisplus {

std::string toLower(const std::string& s) {
  std::string result = s;
  std::transform(result.begin(), result.end(), result.begin(), tolower);
  return result;
}

std::string toUpper(const std::string &s) {
  std::string result(s);
  std::transform(result.begin(), result.end(), result.begin(), toupper);
  return result;
}

Expected<int32_t> stol(const std::string& s) {
  int32_t result;
  try {
    if (s.size() != 0 && (s[0] == ' ' || s[s.size() - 1] == ' ')) {
      return {ErrorCodes::ERR_INTERGER, ""};
    }
    result = static_cast<int32_t>(std::stol(s));
    return result;
  } catch (const std::exception&) {
    return {ErrorCodes::ERR_INTERGER, ""};
  }
}

Expected<uint64_t> stoul(const std::string& s) {
  uint64_t result;
  try {
    if (s.size() != 0 && (s[0] == ' ' || s[s.size() - 1] == ' ')) {
      return {ErrorCodes::ERR_INTERGER, ""};
    }
    result = static_cast<uint64_t>(std::stoul(s));
    return result;
  } catch (const std::exception&) {
    return {ErrorCodes::ERR_INTERGER, ""};
  }
}

Expected<int64_t> stoll(const std::string& s) {
  int64_t result;
  try {
    if (s.size() != 0 && (s[0] == ' ' || s[s.size() - 1] == ' ')) {
      return {ErrorCodes::ERR_INTERGER, ""};
    }
    result = static_cast<int64_t>(std::stoll(s));
    return result;
  } catch (const std::exception&) {
    return {ErrorCodes::ERR_INTERGER, ""};
  }
}

Expected<uint64_t> stoull(const std::string& s) {
  uint64_t result;
  try {
    if (s.size() != 0 && (s[0] == ' ' || s[s.size() - 1] == ' ')) {
      return {ErrorCodes::ERR_INTERGER, ""};
    }
    result = static_cast<uint64_t>(std::stoull(s));
    return result;
  } catch (const std::exception&) {
    return {ErrorCodes::ERR_INTERGER, ""};
  }
}

Expected<long double> stold(const std::string& s) {
  long double result;
  try {
    size_t pos = 0;
    result = std::stold(s, &pos);
    if (s.size() == 0 || isspace(s[0]) ||
        pos != s.size() || std::isnan(result)) {
      return {ErrorCodes::ERR_FLOAT, ""};
    }
    return result;
  } catch (const std::exception&) {
    return {ErrorCodes::ERR_FLOAT, ""};
  }
}

// object.c getDoubleFromObject()
/*

value = strtod(o->ptr, &eptr);
if (sdslen(o->ptr) == 0 ||
isspace(((const char*)o->ptr)[0]) ||
(size_t)(eptr-(char*)o->ptr) != sdslen(o->ptr) ||
(errno == ERANGE &&
(value == HUGE_VAL || value == -HUGE_VAL || value == 0)) ||
std::isnan(value))
return C_ERR;

*/
Expected<double> stod(const std::string& s) {
  double result;
  try {
    size_t pos = 0;
    /* TODO(comboqiu): Check all sto* command's overflow threshold
     *  e.g.: strtod(stdlib)  */
    // result = std::stod(s, &pos);
    char* end;
    result = std::strtod(s.c_str(), &end);
    pos = end - s.c_str();
    if (s.size() == 0 || isspace(s[0]) ||
        pos != s.size() || std::isnan(result)) {
      return {ErrorCodes::ERR_FLOAT, ""};
    }
    return result;
  } catch (const std::exception&) {
    return {ErrorCodes::ERR_FLOAT, ""};
  }
}

// port from networking.c addReplyDouble()
std::string dtos(const double d) {
  if (std::isinf(d)) {
    /* Libc in odd systems (Hi Solaris!) will format infinite in a
     * different way, so better to handle it in an explicit way. */
    return d > 0 ? "inf" : "-inf";
  } else {
    char dbuf[128];
    uint32_t dlen = snprintf(dbuf, sizeof(dbuf), "%.17g", d);
    return std::string(dbuf, dlen);
  }
}

std::string ldtos(const long double d, bool humanfriendly) {
  char buf[256];

  // TODO(vinchen) inf, humanfriendly
  // detailed in util.c/ld2string()
  int len = redis_port::ld2string(buf, sizeof(buf), d, humanfriendly);
  return std::string(buf, len);
}

std::string itos(int32_t d) {
  char dbuf[128];
  uint32_t dlen = snprintf(dbuf, sizeof(dbuf), "%d", d);
  return std::string(dbuf, dlen);
}

std::string uitos(uint32_t d) {
  char dbuf[128];
  uint32_t dlen = snprintf(dbuf, sizeof(dbuf), "%u", d);
  return std::string(dbuf, dlen);
}

std::string ultos(uint64_t d) {
  char dbuf[128];
  uint32_t dlen = snprintf(dbuf, sizeof(dbuf), "%" PRIu64, d);
  return std::string(dbuf, dlen);
}

std::string hexlify(const std::string& s) {
  static const char* lookup = "0123456789ABCDEF";
  std::string result;
  result.resize(s.size() * 2);
  for (size_t i = 0; i < s.size(); ++i) {
    result[2 * i] = (lookup[(s[i] >> 4) & 0xf]);
    result[2 * i + 1] = (lookup[s[i] & 0x0f]);
  }
  return result;
}

Expected<std::string> unhexlify(const std::string& s) {
  static int table_hex[256] = {
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, 0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  -1, -1, -1, -1,
    -1, -1, -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 10, 11, 12,
    13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1};
  if (s.size() % 2 == 1) {
    return {ErrorCodes::ERR_DECODE, "invalid hex size"};
  }
  std::string result;
  result.resize(s.size() / 2);
  for (size_t i = 0; i < s.size(); i += 2) {
    int high = table_hex[static_cast<int>(s[i]) + 128];
    int low = table_hex[static_cast<int>(s[i + 1]) + 128];
    if (high == -1 || low == -1) {
      return {ErrorCodes::ERR_DECODE, "invalid hex str"};
    }
    result[i / 2] = (high << 4) | low;
  }
  return result;
}

bool isOptionOn(const std::string& s) {
  auto x = toLower(s);
  if (x == "on" || x == "1" || x == "true" || x == "yes") {
    return true;
  }
  return false;
}


// trim from start (in place)
static inline void sdsltrim(std::string& s, const char* cset) {  // NOLINT
  s.erase(s.begin(), std::find_if(s.begin(), s.end(), [cset](int ch) {
            return !strchr(cset, ch);
          }));
}

// trim from end (in place)
static inline void sdsrtrim(std::string& s, const char* cset) {  // NOLINT
  s.erase(std::find_if(
            s.rbegin(), s.rend(), [cset](int ch) { return !strchr(cset, ch); })
            .base(),
          s.end());
}

// trim from both ends (in place)
void sdstrim(std::string& s, const char* cset) {  // NOLINT
  sdsltrim(s, cset);
  sdsrtrim(s, cset);
}

std::string trim_left(const std::string& str) {
  const std::string pattern = " \f\n\r\t\v";
  auto iter = str.find_first_not_of(pattern);
  return str.substr(iter == std::string::npos ? 0 : iter);
}

std::string trim_right(const std::string& str) {
  const std::string pattern = " \f\n\r\t\v";
  auto iter = str.find_last_not_of(pattern);
  return str.substr(0, iter == std::string::npos ? 0 : iter + 1);
}

std::string trim(const std::string& str) {
  return trim_left(trim_right(str));
}

/* deal with slot range args like  {1..1000} */
Expected<std::pair<uint32_t, uint32_t>> getSlotRange(const std::string& str) {
  uint64_t len = str.size();
  LOG(INFO) << "range len:" << len;
  if ((str[0] == '{') && (str[len - 1] == '}') && len >= 6) {
    std::string rangeStr = str.substr(1, str.size() - 2);

    if (rangeStr.find("..") == std::string::npos) {
      return {ErrorCodes::ERR_CLUSTER, "Invalid range input withot .."};
    }
    auto vs = stringSplit(rangeStr, "..");

    if (vs.size() != 2) {
      return {ErrorCodes::ERR_CLUSTER, "no find start and end position"};
    }

    auto startSlot = ::tendisplus::stoul(vs[0]);
    auto endSlot = ::tendisplus::stoul(vs[1]);

    if (!startSlot.ok() || !endSlot.ok()) {
      return {ErrorCodes::ERR_CLUSTER, "Invalid slot position range"};
    }

    uint32_t start = startSlot.value();
    uint32_t end = endSlot.value();

    if (start >= end) {
      return {ErrorCodes::ERR_CLUSTER,
              "start position should be less than end"};
    }

    if (end >= CLUSTER_SLOTS) {
      return {ErrorCodes::ERR_CLUSTER,
              "Invalid slot position " + std::to_string(end)};
    }
    return std::make_pair(start, end);
  } else {
    return {ErrorCodes::ERR_CLUSTER, "Invalid slot range string"};
  }
}
std::string& replaceAll(std::string& str,  // NOLINT
                        const std::string& old_value,
                        const std::string& new_value) {
  for (std::string::size_type pos(0); pos != std::string::npos;
       pos += new_value.length()) {
    if ((pos = str.find(old_value, pos)) != std::string::npos)
      str.replace(pos, old_value.length(), new_value);
    else
      break;
  }
  return str;
}

uint64_t getCurThreadId() {
#ifdef _WIN32
  return 0;
#else
  return pthread_self();
#endif
}


size_t lenStrEncode(std::stringstream& ss, const std::string& val) {
  auto sizeStr = varintEncodeStr(val.size());
  ss << sizeStr << val;

  return sizeStr.size() + val.size();
}

std::string lenStrEncode(const std::string& val) {
  auto sizeStr = varintEncodeStr(val.size());

  return sizeStr.append(val);
}

// guarantee dest's size is enough
size_t lenStrEncode(char* dest, size_t destsize, const std::string& val) {
  size_t size =
    varintEncodeBuf(reinterpret_cast<uint8_t*>(dest), destsize, val.size());

  INVARIANT_D(destsize >= size + val.size());
  memcpy(dest + size, val.c_str(), val.size());
  return size + val.size();
}

size_t lenStrEncodeSize(const std::string& val) {
  return varintEncodeSize(val.size()) + val.size();
}

Expected<LenStrDecodeResult> lenStrDecode(const std::string& str) {
  return lenStrDecode(str.c_str(), str.size());
}

Expected<LenStrDecodeResult> lenStrDecode(const char* ptr, size_t max_size) {
  auto eSize = varintDecodeFwd(reinterpret_cast<const uint8_t*>(ptr), max_size);
  if (!eSize.ok()) {
    return eSize.status();
  }
  size_t keySize = eSize.value().first;
  size_t offset = eSize.value().second;

  if (max_size - offset < keySize) {
    return {ErrorCodes::ERR_DECODE, "invalid string"};
  }
  // TODO(vinchen): too more copy
  std::string str(ptr + offset, keySize);
  offset += keySize;

  return LenStrDecodeResult{std::move(str), offset};
}

std::vector<std::string> stringSplit(const std::string& s,
                                     const std::string& delim) {
  std::vector<std::string> elems;
  size_t pos = 0;
  size_t len = s.length();
  size_t delim_len = delim.length();
  if (delim_len == 0)
    return elems;
  while (pos < len) {
    auto find_pos = s.find(delim, pos);
    if (find_pos == std::string::npos) {
      elems.push_back(s.substr(pos, len - pos));
      break;
    }
    elems.push_back(s.substr(pos, find_pos - pos));
    pos = find_pos + delim_len;
  }
  return elems;
}

unsigned char random_char() {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, 255);
  return static_cast<unsigned char>(dis(gen));
}

std::string getUUid(const int len) {
  std::stringstream ss;
  for (auto i = 0; i < len; i++) {
    const auto rc = random_char();
    std::stringstream hexstream;
    hexstream << std::hex << int(rc);
    auto hex = hexstream.str();
    ss << (hex.length() < 2 ? '0' + hex : hex);
  }
  return ss.str();
}

Expected<int64_t> getIntSize(const std::string& str) {
  if (str.size() <= 2) {
    return {ErrorCodes::ERR_DECODE, "getIntSize failed:" + str};
  }
  std::string value = str.substr(0, str.size() - 2);
  std::string unit = str.substr(str.size() - 2, 2);
  Expected<int64_t> size = ::tendisplus::stoll(value);
  if (!size.ok()) {
    return size;
  }
  if (unit == "kB") {
    return size.value() * 1024;
  } else if (unit == "mB") {
    return size.value() * 1024 * 1024;
  } else if (unit == "gB") {
    return size.value() * 1024 * 1024 * 1024;
  }
  return {ErrorCodes::ERR_DECODE, "getIntSize failed:" + str};
}

}  // namespace tendisplus
