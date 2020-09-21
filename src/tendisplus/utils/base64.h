#ifndef SRC_TENDISPLUS_UTILS_BASE64_H_
#define SRC_TENDISPLUS_UTILS_BASE64_H_

#include <string>

class Base64 {
 public:
  static std::string Encode(const unsigned char* str, int bytes);
  static std::string Decode(const char* str, int bytes);
};
#endif  // SRC_TENDISPLUS_UTILS_BASE64_H_
