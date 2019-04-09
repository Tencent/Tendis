#include <chrono>
#include <sstream>
#include "tendisplus/utils/time.h"
#ifdef _WIN32 
#include <time.h>
#endif // !


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

uint64_t msSinceEpoch() {
    using MS = std::chrono::milliseconds;
    return std::chrono::duration_cast<MS>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

std::string timePointRepr(const SCLOCK::time_point& tp) {
    std::stringstream ss;
    using SYSCLOCK = std::chrono::system_clock;
    auto t = SYSCLOCK::to_time_t(SYSCLOCK::now() + std::chrono::duration_cast<SYSCLOCK::duration>(tp - SCLOCK::now()));
#ifndef _WIN32
    ss << std::ctime(&t);
#else
    ss << _ctime64(&t);
#endif // _WIN32
    return ss.str();
}

}  // namespace tendisplus
