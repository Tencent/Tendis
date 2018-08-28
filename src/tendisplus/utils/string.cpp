#include <algorithm>
#include <string>
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

}  // namespace tendisplus
