#ifndef SRC_TENDISPLUS_UTILS_INVARIANT_H_
#define SRC_TENDISPLUS_UTILS_INVARIANT_H_

#include "glog/logging.h"

#define INVARIANT(e) \
    do { \
        if (!__builtin_expect(e, 1)) { \
            LOG(FATAL) << "INVARIANT failed:" << #e \
            << ' ' << __FILE__ << ' ' << __LINE__; \
        } \
    } while (0)


#endif  // SRC_TENDISPLUS_UTILS_INVARIANT_H_
