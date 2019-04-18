#ifndef SRC_TENDISPLUS_UTILS_REDIS_PORT_H_
#define SRC_TENDISPLUS_UTILS_REDIS_PORT_H_

#include <iostream>
#include <vector>
#include <string>

namespace tendisplus {

namespace redis_port {

/* Convert a string into a long long. Returns 1 if the string could be parsed
 * into a (non-overflowing) long long, 0 otherwise. The value will be set to
 * the parsed value when appropriate. */
int string2ll(const char *s, size_t slen, long long *value); // (NOLINT/int)

std::string errorReply(const std::string& s);

// port from redis source code, sds.c::sdssplitargs
std::vector<std::string> splitargs(const std::string& lineStr);

// port from redis source code object.c::createStringObjectFromLongDouble
int ld2string(char *buf, size_t len, long double value, int humanfriendly);

size_t popCount(const void *s, long count); // (NOLINT)

int64_t bitPos(const void *s, size_t count, uint32_t bit);

struct Zrangespec {
    double min;
    double max;
    int minex;
    int maxex;
};

struct Zlexrangespec {
    std::string min;
    std::string max;
    int minex;
    int maxex;
};

#define ZLEXMIN "minstring"
#define ZLEXMAX "maxstring"

int zslParseRange(const char *min, const char *max, Zrangespec *spec);
int zslParseLexRange(const char *min, const char *max, Zlexrangespec *spec);
int stringmatchlen(const char *pattern, int patternLen,
    const char *string, int stringLen, int nocase);

}  // namespace redis_port
}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_UTILS_REDIS_PORT_H_
