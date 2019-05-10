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

/* Error codes */
#define C_OK                    0
#define C_ERR                   -1

struct hllhdr {
    char magic[4];      /* "HYLL" */
    uint8_t encoding;   /* HLL_DENSE or HLL_SPARSE. */
    uint8_t notused[3]; /* Reserved for future use, must be zero. */
    uint8_t card[8];    /* Cached cardinality, little endian. */
    uint8_t registers[]; /* Data bytes. */
};

/* The cached cardinality MSB is used to signal validity of the cached value. */
#define HLL_INVALIDATE_CACHE(hdr) (hdr)->card[7] |= (1<<7)
#define HLL_VALID_CACHE(hdr) (((hdr)->card[7] & (1<<7)) == 0)

#define HLL_P 14 /* The greater is P, the smaller the error. */
#define HLL_REGISTERS (1<<HLL_P) /* With P=14, 16384 registers. */
#define HLL_P_MASK (HLL_REGISTERS-1) /* Mask to index register. */
#define HLL_BITS 6 /* Enough to count up to 63 leading zeroes. */
#define HLL_REGISTER_MAX ((1<<HLL_BITS)-1)
#define HLL_HDR_SIZE sizeof(struct redis_port::hllhdr)
#define HLL_DENSE_SIZE (HLL_HDR_SIZE+((HLL_REGISTERS*HLL_BITS+7)/8))
#define HLL_DENSE 0 /* Dense encoding. */
#define HLL_SPARSE 1 /* Sparse encoding. */
#define HLL_RAW 255 /* Only used internally, never exposed. */
#define HLL_MAX_ENCODING 1

#define HLL_ERROR -1
#define HLL_ERROR_MEMORY -2
#define HLL_ERROR_PROMOTE -3

#define CONFIG_DEFAULT_HLL_SPARSE_MAX_BYTES 3000

// #define HLL_MAX_SIZE (HLL_DENSE_SIZE+1)
// HLL_RAW size is the biggest(HLL_HDR_SIZE + HLL_REGISTERS),
// and 1+HLL_HDR_SIZE is for align size buffer
#define HLL_MAX_SIZE (HLL_HDR_SIZE + HLL_REGISTERS+1+HLL_HDR_SIZE)

typedef char *sds;
#define serverAssert INVARIANT
#define sdsnewlen(A,B) malloc(B)
#define sdsfree(A) free(A)

hllhdr *createHLLObject(const char* buf, size_t bufSize, size_t* sizeOut);
bool isHLLObject(const std::string & v);
int hllAdd(hllhdr *hdr, size_t* hdrSize, size_t hdrMaxSize,
    unsigned char *ele, size_t elesize);
uint64_t hllCount(struct hllhdr *hdr, size_t hdrSize, int *invalid);
uint64_t hllCountFast(struct hllhdr *hdr, size_t hdrSize, int *invalid);
int hllMerge(uint8_t *max, struct hllhdr* hdr, size_t hdrSize);
int hllSparseToDense(struct hllhdr* oldhdr, size_t oldSize,
    struct hllhdr* hdr, size_t* hdrSize, size_t hdrMaxSize);
int hllUpdateByRawHpll(struct hllhdr* hdr, size_t * hdrSize, size_t hdrMaxSize,
    struct hllhdr* hdrRaw);

}  // namespace redis_port
}  // namespace tendisplus

#endif  // SRC_TENDISPLUS_UTILS_REDIS_PORT_H_
