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
    return std::chrono::duration_cast<S>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

}  // namespace tendisplus
