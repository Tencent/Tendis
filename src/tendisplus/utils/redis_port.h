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
std::vector<std::string>* splitargs(std::vector<std::string>& result,
    const std::string& lineStr);

// port from redis source code object.c::createStringObjectFromLongDouble
int ld2string(char *buf, size_t len, long double value, int humanfriendly);

size_t popCount(const void *s, long count); // (NOLINT)

int64_t bitPos(const void *s, size_t count, uint32_t bit);

/* Command flags. Please check the command table defined in the redis.c file
* for more information about the meaning of every flag. */
#define CMD_WRITE (1<<0)            /* "w" flag */
#define CMD_READONLY (1<<1)         /* "r" flag */
#define CMD_DENYOOM (1<<2)          /* "m" flag */
#define CMD_MODULE (1<<3)           /* Command exported by module. */
#define CMD_ADMIN (1<<4)            /* "a" flag */
#define CMD_PUBSUB (1<<5)           /* "p" flag */
#define CMD_NOSCRIPT (1<<6)         /* "s" flag */
#define CMD_RANDOM (1<<7)           /* "R" flag */
#define CMD_SORT_FOR_SCRIPT (1<<8)  /* "S" flag */
#define CMD_LOADING (1<<9)          /* "l" flag */
#define CMD_STALE (1<<10)           /* "t" flag */
#define CMD_SKIP_MONITOR (1<<11)    /* "M" flag */
#define CMD_ASKING (1<<12)          /* "k" flag */
#define CMD_FAST (1<<13)            /* "F" flag */
#define CMD_MODULE_GETKEYS (1<<14)  /* Use the modules getkeys interface. */
#define CMD_MODULE_NO_CLUSTER (1<<15) /* Deny on Redis Cluster. */

#define CMD_MASK 0xFFFF


#define CONFIG_DEFAULT_PROTO_MAX_BULK_LEN (512ll*1024*1024) /* Bulk request max size */

int getCommandFlags(const char* sflags);
struct redisCommand* getCommandFromTable(const char* cmd);
struct redisCommand* getCommandFromTable(size_t index);
size_t getCommandCount();

typedef void redisCommandProc(void *c);
typedef int *redisGetKeysProc(struct redisCommand *cmd, void **argv, int argc, int *numkeys);
struct redisCommand {
    const char *name;
    redisCommandProc *proc;
    int arity;
    const char *sflags; /* Flags as string representation, one char per flag. */
    int flags;    /* The actual flags, obtained from the 'sflags' field. */
                  /* Use a function to determine keys arguments in a command line.
                  * Used for Redis Cluster redirect. */
    redisGetKeysProc *getkeys_proc;
    /* What keys should be loaded in background when calling this command? */
    int firstkey; /* The first argument that's a key (0 = no keys) */
    int lastkey;  /* The last argument that's a key */
    int keystep;  /* The step between first and last key */
    long long microseconds, calls;
};

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
unsigned int keyHashTwemproxy(const std::string& key);

/* Error codes */
#define C_OK                    0
#define C_ERR                   -1

/* ========================= HyperLogLog begin ========================= */

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


/* Store the value of the register at position 'regnum' into variable 'target'.
* 'p' is an array of unsigned bytes. */
#define HLL_DENSE_GET_REGISTER(target,p,regnum) do { \
    uint8_t *_p = (uint8_t*) p; \
    unsigned long _byte = regnum*HLL_BITS/8; \
    unsigned long _fb = regnum*HLL_BITS&7; \
    unsigned long _fb8 = 8 - _fb; \
    unsigned long b0 = _p[_byte]; \
    unsigned long b1 = _p[_byte+1]; \
    target = ((b0 >> _fb) | (b1 << _fb8)) & HLL_REGISTER_MAX; \
} while(0)

/* Set the value of the register at position 'regnum' to 'val'.
* 'p' is an array of unsigned bytes. */
#define HLL_DENSE_SET_REGISTER(p,regnum,val) do { \
    uint8_t *_p = (uint8_t*) p; \
    unsigned long _byte = regnum*HLL_BITS/8; \
    unsigned long _fb = regnum*HLL_BITS&7; \
    unsigned long _fb8 = 8 - _fb; \
    unsigned long _v = val; \
    _p[_byte] &= ~(HLL_REGISTER_MAX << _fb); \
    _p[_byte] |= _v << _fb; \
    _p[_byte+1] &= ~(HLL_REGISTER_MAX >> _fb8); \
    _p[_byte+1] |= _v >> _fb8; \
} while(0)

/* Macros to access the sparse representation.
* The macros parameter is expected to be an uint8_t pointer. */
#define HLL_SPARSE_XZERO_BIT 0x40 /* 01xxxxxx */
#define HLL_SPARSE_VAL_BIT 0x80 /* 1vvvvvxx */
#define HLL_SPARSE_IS_ZERO(p) (((*(p)) & 0xc0) == 0) /* 00xxxxxx */
#define HLL_SPARSE_IS_XZERO(p) (((*(p)) & 0xc0) == HLL_SPARSE_XZERO_BIT)
#define HLL_SPARSE_IS_VAL(p) ((*(p)) & HLL_SPARSE_VAL_BIT)
#define HLL_SPARSE_ZERO_LEN(p) (((*(p)) & 0x3f)+1)
#define HLL_SPARSE_XZERO_LEN(p) (((((*(p)) & 0x3f) << 8) | (*((p)+1)))+1)
#define HLL_SPARSE_VAL_VALUE(p) ((((*(p)) >> 2) & 0x1f)+1)
#define HLL_SPARSE_VAL_LEN(p) (((*(p)) & 0x3)+1)
#define HLL_SPARSE_VAL_MAX_VALUE 32
#define HLL_SPARSE_VAL_MAX_LEN 4
#define HLL_SPARSE_ZERO_MAX_LEN 64
#define HLL_SPARSE_XZERO_MAX_LEN 16384
#define HLL_SPARSE_VAL_SET(p,val,len) do { \
    *(p) = (((val)-1)<<2|((len)-1))|HLL_SPARSE_VAL_BIT; \
} while(0)
#define HLL_SPARSE_ZERO_SET(p,len) do { \
    *(p) = (len)-1; \
} while(0)
#define HLL_SPARSE_XZERO_SET(p,len) do { \
    int _l = (len)-1; \
    *(p) = (_l>>8) | HLL_SPARSE_XZERO_BIT; \
    *((p)+1) = (_l&0xff); \
} while(0)

/* ========================= HyperLogLog end  ========================= */

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
bool isHLLObject(const char* ptr, size_t size);
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
