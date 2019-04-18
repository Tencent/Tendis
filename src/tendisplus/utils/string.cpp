#include <algorithm>
#include <string>
#include <iostream>
#include "tendisplus/utils/status.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/redis_port.h"

namespace tendisplus {

std::string toLower(const std::string& s) {
    std::string result = s;
    std::transform(result.begin(),
        result.end(),
        result.begin(),
        tolower);
    return result;
}

Expected<int32_t> stol(const std::string& s) {
    int32_t result;
    try {
        if (s.size() != 0 && (s[0] == ' ' || s[s.size() - 1] == ' ')) {
            return{ ErrorCodes::ERR_DECODE, "trailing empty chars" };
        }
        result = static_cast<int32_t>(std::stol(s));
        return result;
    }
    catch (const std::exception& ex) {
        return{ ErrorCodes::ERR_DECODE, ex.what() };
    }
}

Expected<uint64_t> stoul(const std::string& s) {
    uint64_t result;
    try {
        if (s.size() != 0 && (s[0] == ' ' || s[s.size()-1] == ' ')) {
            return {ErrorCodes::ERR_DECODE, "trailing empty chars"};
        }
        result = static_cast<uint64_t>(std::stoul(s));
        return result;
    } catch (const std::exception& ex) {
        return {ErrorCodes::ERR_DECODE, ex.what()};
    }
}

Expected<int64_t> stoll(const std::string& s) {
    int64_t result;
    try {
        if (s.size() != 0 && (s[0] == ' ' || s[s.size()-1] == ' ')) {
            return {ErrorCodes::ERR_DECODE, "trailing empty chars"};
        }
        result = static_cast<int64_t>(std::stoll(s));
        return result;
    } catch (const std::exception& ex) {
        return {ErrorCodes::ERR_DECODE, ex.what()};
    }
}

Expected<uint64_t> stoull(const std::string& s) {
    uint64_t result;
    try {
        if (s.size() != 0 && (s[0] == ' ' || s[s.size()-1] == ' ')) {
            return {ErrorCodes::ERR_DECODE, "trailing empty chars"};
        }
        result = static_cast<uint64_t>(std::stoull(s));
        return result;
    } catch (const std::exception& ex) {
        return {ErrorCodes::ERR_DECODE, ex.what()};
    }
}

Expected<long double> stold(const std::string& s) {
    long double result;
    try {
        if (s.size() != 0 && (s[0] == ' ' || s[s.size()-1] == ' ')) {
            return {ErrorCodes::ERR_DECODE, "trailing empty chars"};
        }
        result = static_cast<long double>(std::stold(s));
        return result;
    } catch (const std::exception& ex) {
        return {ErrorCodes::ERR_DECODE, ex.what()};
    }
}

Expected<double> stod(const std::string& s) {
    double result;
    try {
        if (s.size() != 0 && (s[0] == ' ' || s[s.size() - 1] == ' ')) {
            return{ ErrorCodes::ERR_DECODE, "trailing empty chars" };
        }
        result = static_cast<double>(std::stod(s));
        return result;
    }
    catch (const std::exception& ex) {
        return{ ErrorCodes::ERR_DECODE, ex.what() };
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
        -1, -1, -1, -1,  -1, -1, -1, -1,  -1, -1, -1, -1,  -1, -1, -1, -1,
        -1, -1, -1, -1,  -1, -1, -1, -1,  -1, -1, -1, -1,  -1, -1, -1, -1,
        -1, -1, -1, -1,  -1, -1, -1, -1,  -1, -1, -1, -1,  -1, -1, -1, -1,
        -1, -1, -1, -1,  -1, -1, -1, -1,  -1, -1, -1, -1,  -1, -1, -1, -1,
        -1, -1, -1, -1,  -1, -1, -1, -1,  -1, -1, -1, -1,  -1, -1, -1, -1,
        -1, -1, -1, -1,  -1, -1, -1, -1,  -1, -1, -1, -1,  -1, -1, -1, -1,
        -1, -1, -1, -1,  -1, -1, -1, -1,  -1, -1, -1, -1,  -1, -1, -1, -1,
        -1, -1, -1, -1,  -1, -1, -1, -1,  -1, -1, -1, -1,  -1, -1, -1, -1,
        -1, -1, -1, -1,  -1, -1, -1, -1,  -1, -1, -1, -1,  -1, -1, -1, -1,
        -1, -1, -1, -1,  -1, -1, -1, -1,  -1, -1, -1, -1,  -1, -1, -1, -1,
        -1, -1, -1, -1,  -1, -1, -1, -1,  -1, -1, -1, -1,  -1, -1, -1, -1,
         0,  1,  2,  3,   4,  5,  6,  7,   8,  9, -1, -1,  -1, -1, -1, -1,
        -1, 10, 11, 12,  13, 14, 15, -1,  -1, -1, -1, -1,  -1, -1, -1, -1,
        -1, -1, -1, -1,  -1, -1, -1, -1,  -1, -1, -1, -1,  -1, -1, -1, -1,
        -1, 10, 11, 12,  13, 14, 15, -1,  -1, -1, -1, -1,  -1, -1, -1, -1,
        -1, -1, -1, -1,  -1, -1, -1, -1,  -1, -1, -1, -1,  -1, -1, -1, -1
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
        result[i/2] = (high << 4)|low;
    }
    return result;
}

}  // namespace tendisplus
