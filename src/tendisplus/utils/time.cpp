#include <chrono>
#include "tendisplus/utils/time.h"

namespace tendisplus {

uint64_t nsSinceEpoch() {
    using NS = std::chrono::nanoseconds;
    return std::chrono::duration_cast<NS>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

uint32_t sinceEpoch() {
    using S = std::chrono::seconds;
    uint64_t count = std::chrono::duration_cast<S>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    // we all know seconds since epoch fits in uint32_t
    return  static_cast<uint32_t>(count);
}

}  // namespace tendisplus
