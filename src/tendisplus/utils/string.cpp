#include <algorithm>
#include <string>
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
    std::string result;
    result.resize(s.size()*2);
    for (size_t i = 0; i < s.size(); ++i) {
        result[2*i] = (s[i]>>4) + '0';
        result[2*i+1] = (s[i]&0xff) + '0';
    }
    return result;
}

Expected<std::string> unhexlify(const std::string& s) {
    if (s.size()%2 == 1) {
        return {ErrorCodes::ERR_DECODE, "invalid hex size"};
    }
    std::string result;
    result.resize(s.size()/2);
    for (size_t i = 0; i < s.size(); i+= 2) {
        result[i/2] = ((s[i]-'0')<<4)|(s[i+1]-'0');
    }
    return result;
}

}  // namespace tendisplus
