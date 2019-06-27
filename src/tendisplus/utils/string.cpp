#include <string.h>
#include <algorithm>
#include <string>
#include <iostream>
#include <cmath>
#include <cctype>
#include <locale>
#include <thread>
#include <sstream>
#include "tendisplus/utils/status.h"
#include "tendisplus/utils/string.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/storage/varint.h"

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
            return {ErrorCodes::ERR_INTERGER, ""};
        }
        result = static_cast<int32_t>(std::stol(s));
        return result;
    }
    catch (const std::exception&) {
        return {ErrorCodes::ERR_INTERGER, ""};
    }
}

Expected<uint64_t> stoul(const std::string& s) {
    uint64_t result;
    try {
        if (s.size() != 0 && (s[0] == ' ' || s[s.size()-1] == ' ')) {
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
        if (s.size() != 0 && (s[0] == ' ' || s[s.size()-1] == ' ')) {
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
        if (s.size() != 0 && (s[0] == ' ' || s[s.size()-1] == ' ')) {
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
        if (s.size() == 0 ||
            isspace(s[0]) ||
            pos != s.size() ||
            isnan(result)) {
            return{ ErrorCodes::ERR_FLOAT, "" };
        }
        return result;
    }
    catch (const std::exception&) {
        return{ ErrorCodes::ERR_FLOAT, "" };
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
isnan(value))
return C_ERR;

*/
Expected<double> stod(const std::string& s) {
    double result;
    try {
        size_t pos = 0;
        result = std::stod(s, &pos);
        if (s.size() == 0 ||
            isspace(s[0]) ||
            pos != s.size() ||
            isnan(result)) {
            return{ ErrorCodes::ERR_FLOAT, "" };
        }
        return result;
    }
    catch (const std::exception&) {
        return{ ErrorCodes::ERR_FLOAT, "" };
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

bool isOptionOn(const std::string& s) {
    auto x = toLower(s);
    if (x == "on" || x == "1" || x == "true") {
        return true;
    }
    return false;
}


// trim from start (in place)
static inline void sdsltrim(std::string &s, const char *cset) {
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), [cset](int ch) {
        return !strchr(cset, ch);
    }));
}

// trim from end (in place)
static inline void sdsrtrim(std::string &s, const char *cset) {
    s.erase(std::find_if(s.rbegin(), s.rend(), [cset](int ch) {
        return !strchr(cset, ch);
    }).base(), s.end());
}

// trim from both ends (in place)
void sdstrim(std::string &s, const char *cset) {
    sdsltrim(s, cset);
    sdsrtrim(s, cset);
}

std::string& replaceAll(std::string& str,
                            const std::string& old_value,
                            const std::string& new_value) {
    for (std::string::size_type pos(0); pos != std::string::npos; pos += new_value.length()) {
        if ((pos = str.find(old_value, pos)) != std::string::npos)
            str.replace(pos, old_value.length(), new_value);
        else   break;
    }
    return str;
}

uint64_t getCurThreadId() {
    std::ostringstream oss;
    // TODO(vinchen): the performance is?
    oss << std::this_thread::get_id();
    std::string stid = oss.str();
    unsigned long long tid = std::stoull(stid);

    return tid;
}


size_t encodeLenStr(std::stringstream& ss, const std::string& val) {
    auto sizeStr = varintEncodeStr(val.size());
    ss << sizeStr << val;

    return sizeStr.size() + val.size();
}

std::string encodeLenStr(const std::string& val) {
    auto sizeStr = varintEncodeStr(val.size());

    return sizeStr.append(val);
}

Expected<StrDecodeResult> decodeLenStr(const std::string& str) {
    return decodeLenStr(str.c_str(), str.size());
}

Expected<StrDecodeResult> decodeLenStr(const char* ptr, size_t size) {
    auto eSize = varintDecodeFwd(reinterpret_cast<const uint8_t*>(ptr), size);
    if (!eSize.ok()) {
        return eSize.status();
    }
    uint32_t keySize = eSize.value().first;
    size_t offset = eSize.value().second;

    if (size - offset < keySize) {
        return{ ErrorCodes::ERR_DECODE, "invalid string" };
    }
    // TODO(vinchen): too more copy
    std::string str(ptr + offset, keySize);
    offset += keySize;

    return StrDecodeResult{ std::move(str), offset };
}

}  // namespace tendisplus
