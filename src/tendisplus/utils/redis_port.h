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

/* Input flags. */
#define ZADD_NONE 0
#define ZADD_INCR (1<<0)    /* Increment the score instead of setting it. */
#define ZADD_NX (1<<1)      /* Don't touch elements not already existing. */
#define ZADD_XX (1<<2)      /* Only touch elements already exisitng. */

/* Output flags. */
#define ZADD_NOP (1<<3)     /* Operation not performed because of conditionals.*/  // NOLINT
#define ZADD_NAN (1<<4)     /* Only touch elements already exisitng. */
#define ZADD_ADDED (1<<5)   /* The element was new and was added. */
#define ZADD_UPDATED (1<<6) /* The element already existed, score updated. */

/* Flags only used by the ZADD command but not by zsetAdd() API: */
#define ZADD_CH (1<<16)      /* Return num of elements added or updated. */

#define ZSKIPLIST_MAXLEVEL 32 /* Should be enough for 2^32 elements */
#define ZSKIPLIST_P 0.25      /* Skiplist P = 1/4 */

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

int zslRandomLevel(int maxLevel);
int zslParseRange(const char *min, const char *max, Zrangespec *spec);
int zslParseLexRange(const char *min, const char *max, Zlexrangespec *spec);
int stringmatchlen(const char *pattern, int patternLen,
    const char *string, int stringLen, int nocase);
unsigned int keyHashSlot(const char *key, size_t keylen);
unsigned int keyHashTwemproxy(std::string& key);

}  // namespace redis_port
}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_UTILS_REDIS_PORT_H_
