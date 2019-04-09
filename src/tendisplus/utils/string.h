#ifndef SRC_TENDISPLUS_UTILS_STRING_H_
#define SRC_TENDISPLUS_UTILS_STRING_H_

#include <string>

#include "tendisplus/utils/status.h"

namespace tendisplus {

std::string toLower(const std::string&);

Expected<int32_t> stol(const std::string&);
Expected<uint64_t> stoul(const std::string&);
Expected<int64_t> stoll(const std::string&);
Expected<uint64_t> stoull(const std::string&);
Expected<long double> stold(const std::string&);

std::string hexlify(const std::string&);
Expected<std::string> unhexlify(const std::string&);

}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_UTILS_STRING_H_
