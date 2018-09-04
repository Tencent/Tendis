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

}  // namespace tendisplus
