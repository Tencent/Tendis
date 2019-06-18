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
Expected<double> stod(const std::string& s);
std::string dtos(const double d);
std::string ldtos(const long double d, bool humanfriendly);

std::string hexlify(const std::string&);
Expected<std::string> unhexlify(const std::string&);
bool isOptionOn(const std::string& s);
void sdstrim(std::string &s, const char *cset);

std::string& replaceAll(std::string& str,
    const std::string& old_value,
    const std::string& new_value);

uint64_t getCurThreadId();

}  // namespace tendisplus

#ifdef _MSC_VER
#define strcasecmp stricmp
#define strncasecmp  strnicmp 
#endif

#endif  // SRC_TENDISPLUS_UTILS_STRING_H_ 