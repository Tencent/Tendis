#include <algorithm>
#include <string>
#include <iostream>
#include "tendisplus/utils/status.h"
#include "tendisplus/utils/string.h"

namespace tendisplus {

std::string toLower(const std::string& s) {
    std::string result = s;
    std::transform(result.begin(),
        result.end(),
        result.begin(),
        tolower);
    return result;
}

Expected<uint64_t> stoul(const std::string& s) {
    int64_t result;
    try {
        result = static_cast<uint64_t>(std::stoul(s));
        return result;
    } catch (const std::exception& ex) {
        return {ErrorCodes::ERR_DECODE, ex.what()};
    }
}

std::string hexlify(const std::string& s) {
    static const char *lookup = "0123456789ABCDEF";
    std::string result;
    result.resize(s.size()*2);
    for (size_t i = 0; i < s.size(); ++i) {
        result[2*i] = (lookup[(s[i]>>4)&0xf]);
        result[2*i+1] = (lookup[s[i]&0x0f]);
    }
    return result;
}

Expected<std::string> unhexlify(const std::string& s) {
    static int table_hex[256] = {
      -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
      -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
      -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
      -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
      -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
      -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
      -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
      -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
      -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
      -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
      -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
       0, 1, 2, 3,  4, 5, 6, 7,  8, 9,-1,-1, -1,-1,-1,-1,
      -1,10,11,12, 13,14,15,-1, -1,-1,-1,-1, -1,-1,-1,-1,
      -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
      -1,10,11,12, 13,14,15,-1, -1,-1,-1,-1, -1,-1,-1,-1,
      -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1
    };
    if (s.size()%2 == 1) {
        return {ErrorCodes::ERR_DECODE, "invalid hex size"};
    }
    std::string result;
    result.resize(s.size()/2);
    for (size_t i = 0; i < s.size(); i+= 2) {
        int high = table_hex[static_cast<int>(s[i])+128];
        int low = table_hex[static_cast<int>(s[i+1])+128];
        if (high == -1 || low == -1) {
            return {ErrorCodes::ERR_DECODE, "invalid hex str"};
        }
        result[i/2] = (high<<4)|low;
    }
    return result;
}

}  // namespace tendisplus
