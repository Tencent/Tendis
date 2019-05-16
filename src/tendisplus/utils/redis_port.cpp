#include <limits.h>
#include <string.h>
#include <math.h>
#include <sstream>
#include <utility>

#include "glog/logging.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/utils/time.h"

namespace tendisplus {
namespace redis_port {

int stringmatchlen(const char *pattern, int patternLen,
    const char *string, int stringLen, int nocase) {
  while (patternLen) {
    switch (pattern[0]) {
      case '*':
        while (pattern[1] == '*') {
          pattern++;
          patternLen--;
        }
        if (patternLen == 1)
          return 1; /* match */
        while (stringLen) {
          if (stringmatchlen(pattern+1, patternLen-1,
                string, stringLen, nocase))
            return 1; /* match */
          string++;
          stringLen--;
        }
        return 0; /* no match */
        break;
      case '?':
        if (stringLen == 0)
          return 0; /* no match */
        string++;
        stringLen--;
        break;
      case '[':
        {
          int cnot, match;

          pattern++;
          patternLen--;
          cnot = pattern[0] == '^';
          if (cnot) {
            pattern++;
            patternLen--;
          }
          match = 0;
          while (1) {
            if (pattern[0] == '\\') {
              pattern++;
              patternLen--;
              if (pattern[0] == string[0])
                match = 1;
            } else if (pattern[0] == ']') {
              break;
            } else if (patternLen == 0) {
              pattern--;
              patternLen++;
              break;
            } else if (pattern[1] == '-' && patternLen >= 3) {
              int start = pattern[0];
              int end = pattern[2];
              int c = string[0];
              if (start > end) {
                int t = start;
                start = end;
                end = t;
              }
              if (nocase) {
                start = tolower(start);
                end = tolower(end);
                c = tolower(c);
              }
              pattern += 2;
              patternLen -= 2;
              if (c >= start && c <= end)
                match = 1;
            } else {
              if (!nocase) {
                if (pattern[0] == string[0])
                  match = 1;
              } else {
                if (tolower(static_cast<int>(pattern[0]))
                        == tolower(static_cast<int>(string[0])))
                  match = 1;
              }
            }
            pattern++;
            patternLen--;
          }
          if (cnot)
            match = !match;
          if (!match)
            return 0; /* no match */
          string++;
          stringLen--;
          break;
        }
      case '\\':
        if (patternLen >= 2) {
          pattern++;
          patternLen--;
        }
        /* fall through */
      default:
        if (!nocase) {
          if (pattern[0] != string[0])
            return 0; /* no match */
        } else {
          if (tolower(static_cast<int>(pattern[0]))
              != tolower(static_cast<int>(string[0])))
            return 0; /* no match */
        }
        string++;
        stringLen--;
        break;
    }
    pattern++;
    patternLen--;
    if (stringLen == 0) {
      while (*pattern == '*') {
        pattern++;
        patternLen--;
      }
      break;
    }
  }
  if (patternLen == 0 && stringLen == 0)
    return 1;
  return 0;
}

int64_t bitPos(const void *s, size_t count, uint32_t bit) {
    unsigned long *l;       // NOLINT:runtime/int
    unsigned char *c;
    unsigned long skipval, word = 0, one; // NOLINT:runtime/int
    /* Position of bit, to return to the caller. */
    long pos = 0;           // NOLINT:runtime/int
    unsigned long j;        // NOLINT:runtime/int

    /* Process whole words first, seeking for first word that is not
     * all ones or all zeros respectively if we are lookig for zeros
     * or ones. This is much faster with large strings having contiguous
     * blocks of 1 or 0 bits compared to the vanilla bit per bit processing.
     *
     * Note that if we start from an address that is not aligned
     * to sizeof(unsigned long) we consume it byte by byte until it is
     * aligned. */

    /* Skip initial bits not aligned to sizeof(unsigned long) byte by byte. */
    skipval = bit ? 0 : UCHAR_MAX;
    c = (unsigned char*) s;
    while ((unsigned long)c & (sizeof(*l)-1) && count) { // NOLINT:runtime/int
        if (*c != skipval) break;
        c++;
        count--;
        pos += 8;
    }

    /* Skip bits with full word step. */
    skipval = bit ? 0 : ULONG_MAX;
    l = (unsigned long*) c;   // NOLINT:runtime/int
    while (count >= sizeof(*l)) {
        if (*l != skipval) break;
        l++;
        count -= sizeof(*l);
        pos += sizeof(*l)*8;
    }

    /* Load bytes into "word" considering the first byte as the most significant
     * (we basically consider it as written in big endian, since we consider the
     * string as a set of bits from left to right, with the first bit at position
     * zero.
     *
     * Note that the loading is designed to work even when the bytes left
     * (count) are less than a full word. We pad it with zero on the right. */
    c = (unsigned char*)l;
    for (j = 0; j < sizeof(*l); j++) {
        word <<= 8;
        if (count) {
            word |= *c;
            c++;
            count--;
        }
    }

    /* Special case:
     * If bits in the string are all zero and we are looking for one,
     * return -1 to signal that there is not a single "1" in the whole
     * string. This can't happen when we are looking for "0" as we assume
     * that the right of the string is zero padded. */
    if (bit == 1 && word == 0) return -1;

    /* Last word left, scan bit by bit. The first thing we need is to
     * have a single "1" set in the most significant position in an
     * unsigned long. We don't know the size of the long so we use a
     * simple trick. */
    one = ULONG_MAX; /* All bits set to 1.*/
    one >>= 1;       /* All bits set to 1 but the MSB. */
    one = ~one;      /* All bits set to 0 but the MSB. */

    while (one) {
        if (((one & word) != 0) == bit) return pos;
        pos++;
        one >>= 1;
    }

    /* If we reached this point, there is a bug in the algorithm, since
     * the case of no match is handled as a special case before. */
    INVARIANT(0);
    return 0; /* Just to avoid warnings. */
}

size_t popCount(const void *s, long count) {  // (NOLINT)
    size_t bits = 0;
    const unsigned char *p = static_cast<const unsigned char*>(s);
    uint32_t *p4;
    static const unsigned char bitsinbyte[256] = {0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,4,5,5,6,5,6,6,7,5,6,6,7,6,7,7,8};  // (NOLINT)

    /* Count initial bytes not aligned to 32 bit. */
    while((unsigned long)p & 3 && count) {  // (NOLINT)
        bits += bitsinbyte[*p++];
        count--;
    }

    /* Count bits 16 bytes at a time */
    p4 = (uint32_t*)p;  // (NOLINT)
    while(count>=16) {  // (NOLINT)
        uint32_t aux1, aux2, aux3, aux4;

        aux1 = *p4++;
        aux2 = *p4++;
        aux3 = *p4++;
        aux4 = *p4++;
        count -= 16;

        aux1 = aux1 - ((aux1 >> 1) & 0x55555555);
        aux1 = (aux1 & 0x33333333) + ((aux1 >> 2) & 0x33333333);
        aux2 = aux2 - ((aux2 >> 1) & 0x55555555);
        aux2 = (aux2 & 0x33333333) + ((aux2 >> 2) & 0x33333333);
        aux3 = aux3 - ((aux3 >> 1) & 0x55555555);
        aux3 = (aux3 & 0x33333333) + ((aux3 >> 2) & 0x33333333);
        aux4 = aux4 - ((aux4 >> 1) & 0x55555555);
        aux4 = (aux4 & 0x33333333) + ((aux4 >> 2) & 0x33333333);
        bits += ((((aux1 + (aux1 >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24) +
                ((((aux2 + (aux2 >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24) +
                ((((aux3 + (aux3 >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24) +
                ((((aux4 + (aux4 >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24);
    }
    /* Count the remaining bytes. */
    p = (unsigned char*)p4;
    while(count--) bits += bitsinbyte[*p++];  // (NOLINT)
    return bits;
}

/* Convert a long double into a string. If humanfriendly is non-zero
* it does not use exponential format and trims trailing zeroes at the end,
* however this results in loss of precision. Otherwise exp format is used
* and the output of snprintf() is not modified.
*
* The function returns the length of the string or zero if there was not
* enough buffer room to store it. */
int ld2string(char *buf, size_t len, long double value, int humanfriendly) {
    size_t l;

    if (isinf(value)) {
        /* Libc in odd systems (Hi Solaris!) will format infinite in a
        * different way, so better to handle it in an explicit way. */
        if (len < 5) return 0; /* No room. 5 is "-inf\0" */
        if (value > 0) {
            memcpy(buf, "inf", 3);
            l = 3;
        } else {
            memcpy(buf, "-inf", 4);
            l = 4;
        }
    } else if (humanfriendly) {
        /* We use 17 digits precision since with 128 bit floats that precision
        * after rounding is able to represent most small decimal numbers in a
        * way that is "non surprising" for the user (that is, most small
        * decimal numbers will be represented in a way that when converted
        * back into a string are exactly the same as what the user typed.) */
        l = snprintf(buf, len, "%.17Lf", value);
        if (l + 1 > len) return 0; /* No room. */
                              /* Now remove trailing zeroes after the '.' */
        if (strchr(buf, '.') != NULL) {
            char *p = buf + l - 1;
            while (*p == '0') {
                p--;
                l--;
            }
            if (*p == '.') l--;
        }
    } else {
        l = snprintf(buf, len, "%.17Lg", value);
        if (l + 1 > len) return 0; /* No room. */
    }
    buf[l] = '\0';
    return l;
}

/* Convert a string into a long long. Returns 1 if the string could be parsed
 * into a (non-overflowing) long long, 0 otherwise. The value will be set to
 * the parsed value when appropriate. */
int string2ll(const char *s, size_t slen, long long *value) { // (NOLINT/int)
  const char *p = s;
  size_t plen = 0;
  int negative = 0;
  unsigned long long v;  //(NOLINT/int)

  if (plen == slen)
    return 0;

  /* Special case: first and only digit is 0. */
  if (slen == 1 && p[0] == '0') {
    if (value != NULL) *value = 0;
    return 1;
  }

  if (p[0] == '-') {
    negative = 1;
    p++; plen++;

    /* Abort on only a negative sign. */
    if (plen == slen)
      return 0;
  }

  /* First digit should be 1-9, otherwise the string should just be 0. */
  if (p[0] >= '1' && p[0] <= '9') {
    v = p[0]-'0';
    p++; plen++;
  } else if (p[0] == '0' && slen == 1) {
    *value = 0;
    return 1;
  } else {
    return 0;
  }

  while (plen < slen && p[0] >= '0' && p[0] <= '9') {
    if (v > (ULLONG_MAX / 10)) /* Overflow. */
      return 0;
    v *= 10;

    if (v > (ULLONG_MAX - (p[0]-'0'))) /* Overflow. */
      return 0;
    v += p[0]-'0';

    p++; plen++;
  }

  /* Return if not all bytes were used. */
  if (plen < slen)
    return 0;

  if (negative) {
    if (v > ((unsigned long long)(-(LLONG_MIN+1))+1)) /* Overflow. (NOLINT/int)*/
      return 0;
    if (value != NULL) *value = -v;
  } else {
    if (v > LLONG_MAX) /* Overflow. */
      return 0;
    if (value != NULL) *value = v;
  }
  return 1;
}

std::string errorReply(const std::string& s) {
    if (s[0] == '-') {
        INVARIANT(s[s.size() - 2] == '\r');
        INVARIANT(s[s.size() - 1] == '\n');
        return s;
    }
    std::stringstream ss;
    ss << "-ERR " << s << "\r\n";
    return ss.str();
}

int is_hex_digit(char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') ||
           (c >= 'A' && c <= 'F');
}

int hex_digit_to_int(char c) {
    switch (c) {
    case '0': return 0;
    case '1': return 1;
    case '2': return 2;
    case '3': return 3;
    case '4': return 4;
    case '5': return 5;
    case '6': return 6;
    case '7': return 7;
    case '8': return 8;
    case '9': return 9;
    case 'a': case 'A': return 10;
    case 'b': case 'B': return 11;
    case 'c': case 'C': return 12;
    case 'd': case 'D': return 13;
    case 'e': case 'E': return 14;
    case 'f': case 'F': return 15;
    default: return 0;
    }
}

int random() {
    std::srand((uint32_t)msSinceEpoch());
    return std::rand();
}

/* Returns a random level for the new skiplist node we are going to create.
* The return value of this function is between 1 and ZSKIPLIST_MAXLEVEL
* (both inclusive), with a powerlaw-alike distribution where higher
* levels are less likely to be returned. */
int zslRandomLevel(int maxLevel) {
    int level = 1;
    while ((random() & 0xFFFF) < (ZSKIPLIST_P * 0xFFFF))
        level += 1;
    return (level < maxLevel) ? level : maxLevel;
}

int zslParseLexRangeItem(const char *c, std::string* dest, int *ex) {
    switch (c[0]) {
    case '+':
        if (c[1] != '\0') return -1;
        *ex = 0;
        *dest = ZLEXMAX;
        return 0;
    case '-':
        if (c[1] != '\0') return -1;
        *ex = 0;
        *dest = ZLEXMIN;
        return 0;
    case '(':
        *ex = 1;
        *dest = std::string(c+1, strlen(c)-1);
        return 0;
    case '[':
        *ex = 0;
        *dest = std::string(c+1, strlen(c)-1);
        return 0;
    default:
        return -1;
    }
}

int zslParseLexRange(const char *min, const char *max, Zlexrangespec *spec) {
    if (zslParseLexRangeItem(min, &spec->min, &spec->minex) != 0 ||
            zslParseLexRangeItem(max, &spec->max, &spec->maxex) != 0) {
        return -1;
    }
    return 0;
}

int zslParseRange(const char *min, const char *max, Zrangespec *spec) {
    char *eptr;
    spec->minex = spec->maxex = 0;

    /* Parse the min-max interval. If one of the values is prefixed
     * by the "(" character, it's considered "open". For instance
     * ZRANGEBYSCORE zset (1.5 (2.5 will match min < x < max
     * ZRANGEBYSCORE zset 1.5 2.5 will instead match min <= x <= max */
    if (min[0] == '(') {
        spec->min = strtod(min+1, &eptr);
        if (eptr[0] != '\0' || isnan(spec->min)) return -1;
        spec->minex = 1;
    } else {
        spec->min = strtod(min, &eptr);
        if (eptr[0] != '\0' || isnan(spec->min)) return -1;
    }

    if (max[0] == '(') {
        spec->max = strtod(max+1, &eptr);
        if (eptr[0] != '\0' || isnan(spec->max)) return -1;
        spec->maxex = 1;
    } else {
        spec->max = strtod(max, &eptr);
        if (eptr[0] != '\0' || isnan(spec->max)) return -1;
    }
    return 0;
}

std::vector<std::string> splitargs(const std::string& lineStr) {
    const char *line = lineStr.c_str();
    const char *p = line;
    std::vector<std::string> result;

    while (1) {
        /* skip blanks */
        while (*p && isspace(*p)) p++;
        if (*p) {
            /* get a token */
            int inq = 0;  /* set to 1 if we are in "quotes" */
            int insq = 0;  /* set to 1 if we are in 'single quotes' */
            int done = 0;

            std::string current;
            while (!done) {
                if (inq) {
                    if (*p == '\\' && *(p+1) == 'x' &&
                                             is_hex_digit(*(p+2)) &&
                                             is_hex_digit(*(p+3))) {
                        unsigned char byte;

                        byte = static_cast<unsigned char>(
                                (hex_digit_to_int(*(p+2))*16)+
                                hex_digit_to_int(*(p+3)));
                        current.push_back(static_cast<char>(byte));
                        p += 3;
                    } else if (*p == '\\' && *(p+1)) {
                        char c;

                        p++;
                        switch (*p) {
                        case 'n': c = '\n'; break;
                        case 'r': c = '\r'; break;
                        case 't': c = '\t'; break;
                        case 'b': c = '\b'; break;
                        case 'a': c = '\a'; break;
                        default: c = *p; break;
                        }
                        current.push_back(c);
                    } else if (*p == '"') {
                        /* closing quote must be followed by a space or
                         * nothing at all. */
                        if (*(p+1) && !isspace(*(p+1))) goto err;
                        done = 1;
                    } else if (!*p) {
                        /* unterminated quotes */
                        goto err;
                    } else {
                        current.push_back(*p);
                    }
                } else if (insq) {
                    if (*p == '\\' && *(p+1) == '\'') {
                        p++;
                        current.push_back('\'');
                    } else if (*p == '\'') {
                        /* closing quote must be followed by a space or
                         * nothing at all. */
                        if (*(p+1) && !isspace(*(p+1))) goto err;
                        done = 1;
                    } else if (!*p) {
                        /* unterminated quotes */
                        goto err;
                    } else {
                        current.push_back(*p);
                    }
                } else {
                    switch (*p) {
                    case ' ':
                    case '\n':
                    case '\r':
                    case '\t':
                    case '\0':
                        done = 1;
                        break;
                    case '"':
                        inq = 1;
                        break;
                    case '\'':
                        insq = 1;
                        break;
                    default:
                        current.push_back(*p);
                        break;
                    }
                }
                if (*p) p++;
            }
            /* add the token to the vector */
            result.emplace_back(std::move(current));
        } else {
            return result;
        }
    }

err:
    return std::vector<std::string>();
}

/* CRC16 implementation according to CCITT standards.
*
* Note by @antirez: this is actually the XMODEM CRC 16 algorithm, using the
* following parameters:
*
* Name                       : "XMODEM", also known as "ZMODEM", "CRC-16/ACORN"
* Width                      : 16 bit
* Poly                       : 1021 (That is actually x^16 + x^12 + x^5 + 1)
* Initialization             : 0000
* Reflect Input byte         : False
* Reflect Output CRC         : False
* Xor constant to output CRC : 0000
* Output for "123456789"     : 31C3
*/

static const uint16_t crc16tab[256] = {
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
    0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
    0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
    0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
    0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
    0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
    0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
    0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
    0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
    0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
    0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
    0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
    0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
    0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
    0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
    0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
    0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
    0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
    0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
    0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
    0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
    0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
    0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
    0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
    0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
    0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
    0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
    0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0
};

uint16_t crc16(const char *buf, int len) {
    int counter;
    uint16_t crc = 0;
    for (counter = 0; counter < len; counter++)
        crc = (crc << 8) ^ crc16tab[((crc >> 8) ^ *buf++) & 0x00FF];
    return crc;
}

/* We have 16384 hash slots. The hash slot of a given key is obtained
* as the least significant 14 bits of the crc16 of the key.
*
* However if the key contains the {...} pattern, only the part between
* { and } is hashed. This may be useful in the future to force certain
* keys to be in the same node (assuming no resharding is in progress). */
unsigned int keyHashSlot(const char *key, size_t keylen) {
    size_t s, e; /* start-end indexes of { and } */

    for (s = 0; s < keylen; s++)
        if (key[s] == '{') break;

    /* No '{' ? Hash the whole key. This is the base case. */
    if (s == keylen) return crc16(key, keylen) & 0x3FFF;

    /* '{' found? Check if we have the corresponding '}'. */
    for (e = s + 1; e < keylen; e++)
        if (key[e] == '}') break;

    /* No '}' or nothing betweeen {} ? Hash the whole key. */
    if (e == keylen || e == s + 1) return crc16(key, keylen) & 0x3FFF;

    /* If we are here there is both a { and a } on its right. Hash
    * what is in the middle between { and }. */
    return crc16(key + s + 1, e - s - 1) & 0x3FFF;
}

static constexpr uint64_t FNV_64_INIT = 0xcbf29ce484222325ULL;
static constexpr uint64_t FNV_64_PRIME = 0x100000001b3ULL;

unsigned int keyHashTwemproxy(const std::string& key) {
    uint32_t hash = static_cast<uint32_t>(FNV_64_INIT);
    for (auto v : key) {
        uint32_t val = static_cast<uint32_t>(v);
        hash ^= val;
        hash *= static_cast<uint32_t>(FNV_64_PRIME);
    }

    return hash;
}

int getCommandFlags(const char* sflags) {
    int flags = 0;
    const char *f = sflags;

    while (*f != '\0') {
        switch (*f) {
        case 'w': flags |= CMD_WRITE; break;
        case 'r': flags |= CMD_READONLY; break;
        case 'm': flags |= CMD_DENYOOM; break;
        case 'a': flags |= CMD_ADMIN; break;
        case 'p': flags |= CMD_PUBSUB; break;
        case 's': flags |= CMD_NOSCRIPT; break;
        case 'R': flags |= CMD_RANDOM; break;
        case 'S': flags |= CMD_SORT_FOR_SCRIPT; break;
        case 'l': flags |= CMD_LOADING; break;
        case 't': flags |= CMD_STALE; break;
        case 'M': flags |= CMD_SKIP_MONITOR; break;
        case 'k': flags |= CMD_ASKING; break;
        case 'F': flags |= CMD_FAST; break;
        default:
            INVARIANT(0);
            break;
        }
        f++;
    }
    return flags;
}

#define moduleCommand NULL
#define getCommand NULL
#define getvsnCommand NULL
#define setCommand NULL
#define casCommand NULL
#define setnxCommand NULL
#define setexCommand NULL
#define psetexCommand NULL
#define appendCommand NULL
#define strlenCommand NULL
#define delCommand NULL
#define unlinkCommand NULL
#define existsCommand NULL
#define setbitCommand NULL
#define getbitCommand NULL
#define bitfieldCommand NULL
#define setrangeCommand NULL
#define getrangeCommand NULL
#define getrangeCommand NULL
#define incrCommand NULL
#define decrCommand NULL
#define increxCommand NULL
#define mgetCommand NULL
#define rpushCommand NULL
#define lpushCommand NULL
#define rpushxCommand NULL
#define lpushxCommand NULL
#define linsertCommand NULL
#define rpopCommand NULL
#define lpopCommand NULL
#define brpopCommand NULL
#define brpoplpushCommand NULL
#define blpopCommand NULL
#define llenCommand NULL
#define lindexCommand NULL
#define lsetCommand NULL
#define lrangeCommand NULL
#define ltrimCommand NULL
#define lremCommand NULL
#define rpoplpushCommand NULL
#define saddCommand NULL
#define sremCommand NULL
#define smoveCommand NULL
#define sismemberCommand NULL
#define scardCommand NULL
#define spopCommand NULL
#define srandmemberCommand NULL
#define sinterCommand NULL
#define sinterstoreCommand NULL
#define sunionCommand NULL
#define sunionstoreCommand NULL
#define sdiffCommand NULL
#define sdiffstoreCommand NULL
#define sinterCommand NULL
#define sscanCommand NULL
#define zaddCommand NULL
#define zincrbyCommand NULL
#define zremCommand NULL
#define zremrangebyscoreCommand NULL
#define zremrangebyrankCommand NULL
#define zremrangebylexCommand NULL
#define zunionstoreCommand NULL
#define zinterstoreCommand NULL
#define zrangeCommand NULL
#define zrangebyscoreCommand NULL
#define zrevrangebyscoreCommand NULL
#define zrangebylexCommand NULL
#define zrevrangebylexCommand NULL
#define zcountCommand NULL
#define zlexcountCommand NULL
#define zrevrangeCommand NULL
#define zcardCommand NULL
#define zscoreCommand NULL
#define zrankCommand NULL
#define zrevrankCommand NULL
#define zscanCommand NULL
#define hsetCommand NULL
#define hsetnxCommand NULL
#define hgetCommand NULL
#define hsetCommand NULL
#define hmgetCommand NULL
#define hincrbyCommand NULL
#define hincrbyfloatCommand NULL
#define hdelCommand NULL
#define hlenCommand NULL
#define hstrlenCommand NULL
#define hkeysCommand NULL
#define hvalsCommand NULL
#define hgetallCommand NULL
#define hmgetallCommand NULL
#define hexistsCommand NULL
#define hscanCommand NULL
#define incrbyCommand NULL
#define decrbyCommand NULL
#define incrbyfloatCommand NULL
#define getsetCommand NULL
#define msetCommand NULL
#define msetnxCommand NULL
#define randomkeyCommand NULL
#define selectCommand NULL
#define swapdbCommand NULL
#define moveCommand NULL
#define renameCommand NULL
#define renamenxCommand NULL
#define expireCommand NULL
#define expireatCommand NULL
#define pexpireCommand NULL
#define pexpireatCommand NULL
#define keysCommand NULL
#define scanCommand NULL
#define dbsizeCommand NULL
#define authCommand NULL
#define pingCommand NULL
#define echoCommand NULL
#define saveCommand NULL
#define bgsaveCommand NULL
#define bgrewriteaofCommand NULL
#define rotateAOFIncrLogCommand NULL
#define shutdownCommand NULL
#define lastsaveCommand NULL
#define typeCommand NULL
#define multiCommand NULL
#define execCommand NULL
#define discardCommand NULL
#define syncCommand NULL
#define syncCommand NULL
#define replconfCommand NULL
#define flushdbCommand NULL
#define flushallCommand NULL
#define sortCommand NULL
#define infoCommand NULL
#define monitorCommand NULL
#define ttlCommand NULL
#define touchCommand NULL
#define pttlCommand NULL
#define persistCommand NULL
#define slaveofCommand NULL
#define roleCommand NULL
#define debugCommand NULL
#define configCommand NULL
#define subscribeCommand NULL
#define unsubscribeCommand NULL
#define psubscribeCommand NULL
#define punsubscribeCommand NULL
#define publishCommand NULL
#define pubsubCommand NULL
#define watchCommand NULL
#define unwatchCommand NULL
#define clusterCommand NULL
#define restoreCommand NULL
#define restoreCommand NULL
#define migrateCommand NULL
#define askingCommand NULL
#define readonlyCommand NULL
#define readwriteCommand NULL
#define dumpCommand NULL
#define objectCommand NULL
#define memoryCommand NULL
#define clientCommand NULL
#define evalCommand NULL
#define evalShaCommand NULL
#define slowlogCommand NULL
#define scriptCommand NULL
#define timeCommand NULL
#define bitopCommand NULL
#define bitcountCommand NULL
#define bitposCommand NULL
#define waitCommand NULL
#define commandCommand NULL
#define geoaddCommand NULL
#define georadiusCommand NULL
#define georadiusroCommand NULL
#define georadiusbymemberCommand NULL
#define georadiusbymemberroCommand NULL
#define geohashCommand NULL
#define geoposCommand NULL
#define geodistCommand NULL
#define pfselftestCommand NULL
#define pfaddCommand NULL
#define pfcountCommand NULL
#define pfmergeCommand NULL
#define pfdebugCommand NULL
#define securityWarningCommand NULL
#define securityWarningCommand NULL
#define latencyCommand NULL

// take care of it!
#define zunionInterGetKeys NULL
#define zunionInterGetKeys NULL
#define sortGetKeys NULL
#define migrateGetKeys NULL
#define evalGetKeys NULL
#define evalGetKeys NULL
#define georadiusGetKeys NULL
#define georadiusGetKeys NULL
#define georadiusGetKeys NULL
#define georadiusGetKeys NULL

struct redisCommand redisCommandTable[] = {
    { "module",moduleCommand,-2,"as",0,NULL,0,0,0,0,0 },
    { "get",getCommand,2,"rF",0,NULL,1,1,1,0,0 },
    { "getvsn",getvsnCommand,2,"rF",0,NULL,1,1,1,0,0 },
    { "set",setCommand,-3,"wm",0,NULL,1,1,1,0,0 },
    { "cas",casCommand,-4,"wm",0,NULL,2,2,1,0,0 },
    { "setnx",setnxCommand,3,"wmF",0,NULL,1,1,1,0,0 },
    { "setex",setexCommand,4,"wm",0,NULL,1,1,1,0,0 },
    { "psetex",psetexCommand,4,"wm",0,NULL,1,1,1,0,0 },
    { "append",appendCommand,3,"wm",0,NULL,1,1,1,0,0 },
    { "strlen",strlenCommand,2,"rF",0,NULL,1,1,1,0,0 },
    { "del",delCommand,-2,"w",0,NULL,1,-1,1,0,0 },
    { "unlink",unlinkCommand,-2,"wF",0,NULL,1,-1,1,0,0 },
    { "exists",existsCommand,-2,"rF",0,NULL,1,-1,1,0,0 },
    { "setbit",setbitCommand,4,"wm",0,NULL,1,1,1,0,0 },
    { "getbit",getbitCommand,3,"rF",0,NULL,1,1,1,0,0 },
    { "bitfield",bitfieldCommand,-2,"wm",0,NULL,1,1,1,0,0 },
    { "setrange",setrangeCommand,4,"wm",0,NULL,1,1,1,0,0 },
    { "getrange",getrangeCommand,4,"r",0,NULL,1,1,1,0,0 },
    { "substr",getrangeCommand,4,"r",0,NULL,1,1,1,0,0 },
    { "incr",incrCommand,2,"wmF",0,NULL,1,1,1,0,0 },
    { "decr",decrCommand,2,"wmF",0,NULL,1,1,1,0,0 },
    { "increx",increxCommand,3,"wmF",0,NULL,1,1,1,0,0 },
    { "mget",mgetCommand,-2,"rF",0,NULL,1,-1,1,0,0 },
    { "rpush",rpushCommand,-3,"wmF",0,NULL,1,1,1,0,0 },
    { "lpush",lpushCommand,-3,"wmF",0,NULL,1,1,1,0,0 },
    { "rpushx",rpushxCommand,-3,"wmF",0,NULL,1,1,1,0,0 },
    { "lpushx",lpushxCommand,-3,"wmF",0,NULL,1,1,1,0,0 },
    { "linsert",linsertCommand,5,"wm",0,NULL,1,1,1,0,0 },
    { "rpop",rpopCommand,2,"wF",0,NULL,1,1,1,0,0 },
    { "lpop",lpopCommand,2,"wF",0,NULL,1,1,1,0,0 },
    { "brpop",brpopCommand,-3,"ws",0,NULL,1,-2,1,0,0 },
    { "brpoplpush",brpoplpushCommand,4,"wms",0,NULL,1,2,1,0,0 },
    { "blpop",blpopCommand,-3,"ws",0,NULL,1,-2,1,0,0 },
    { "llen",llenCommand,2,"rF",0,NULL,1,1,1,0,0 },
    { "lindex",lindexCommand,3,"r",0,NULL,1,1,1,0,0 },
    { "lset",lsetCommand,4,"wm",0,NULL,1,1,1,0,0 },
    { "lrange",lrangeCommand,4,"r",0,NULL,1,1,1,0,0 },
    { "ltrim",ltrimCommand,4,"w",0,NULL,1,1,1,0,0 },
    { "lrem",lremCommand,4,"w",0,NULL,1,1,1,0,0 },
    { "rpoplpush",rpoplpushCommand,3,"wm",0,NULL,1,2,1,0,0 },
    { "sadd",saddCommand,-3,"wmF",0,NULL,1,1,1,0,0 },
    { "srem",sremCommand,-3,"wF",0,NULL,1,1,1,0,0 },
    { "smove",smoveCommand,4,"wF",0,NULL,1,2,1,0,0 },
    { "sismember",sismemberCommand,3,"rF",0,NULL,1,1,1,0,0 },
    { "scard",scardCommand,2,"rF",0,NULL,1,1,1,0,0 },
    { "spop",spopCommand,-2,"wRF",0,NULL,1,1,1,0,0 },
    { "srandmember",srandmemberCommand,-2,"rR",0,NULL,1,1,1,0,0 },
    { "sinter",sinterCommand,-2,"rS",0,NULL,1,-1,1,0,0 },
    { "sinterstore",sinterstoreCommand,-3,"wm",0,NULL,1,-1,1,0,0 },
    { "sunion",sunionCommand,-2,"rS",0,NULL,1,-1,1,0,0 },
    { "sunionstore",sunionstoreCommand,-3,"wm",0,NULL,1,-1,1,0,0 },
    { "sdiff",sdiffCommand,-2,"rS",0,NULL,1,-1,1,0,0 },
    { "sdiffstore",sdiffstoreCommand,-3,"wm",0,NULL,1,-1,1,0,0 },
    { "smembers",sinterCommand,2,"rS",0,NULL,1,1,1,0,0 },
    { "sscan",sscanCommand,-3,"rR",0,NULL,1,1,1,0,0 },
    { "zadd",zaddCommand,-4,"wmF",0,NULL,1,1,1,0,0 },
    { "zincrby",zincrbyCommand,4,"wmF",0,NULL,1,1,1,0,0 },
    { "zrem",zremCommand,-3,"wF",0,NULL,1,1,1,0,0 },
    { "zremrangebyscore",zremrangebyscoreCommand,4,"w",0,NULL,1,1,1,0,0 },
    { "zremrangebyrank",zremrangebyrankCommand,4,"w",0,NULL,1,1,1,0,0 },
    { "zremrangebylex",zremrangebylexCommand,4,"w",0,NULL,1,1,1,0,0 },
    { "zunionstore",zunionstoreCommand,-4,"wm",0,zunionInterGetKeys,0,0,0,0,0 },
    { "zinterstore",zinterstoreCommand,-4,"wm",0,zunionInterGetKeys,0,0,0,0,0 },
    { "zrange",zrangeCommand,-4,"r",0,NULL,1,1,1,0,0 },
    { "zrangebyscore",zrangebyscoreCommand,-4,"r",0,NULL,1,1,1,0,0 },
    { "zrevrangebyscore",zrevrangebyscoreCommand,-4,"r",0,NULL,1,1,1,0,0 },
    { "zrangebylex",zrangebylexCommand,-4,"r",0,NULL,1,1,1,0,0 },
    { "zrevrangebylex",zrevrangebylexCommand,-4,"r",0,NULL,1,1,1,0,0 },
    { "zcount",zcountCommand,4,"rF",0,NULL,1,1,1,0,0 },
    { "zlexcount",zlexcountCommand,4,"rF",0,NULL,1,1,1,0,0 },
    { "zrevrange",zrevrangeCommand,-4,"r",0,NULL,1,1,1,0,0 },
    { "zcard",zcardCommand,2,"rF",0,NULL,1,1,1,0,0 },
    { "zscore",zscoreCommand,3,"rF",0,NULL,1,1,1,0,0 },
    { "zrank",zrankCommand,3,"rF",0,NULL,1,1,1,0,0 },
    { "zrevrank",zrevrankCommand,3,"rF",0,NULL,1,1,1,0,0 },
    { "zscan",zscanCommand,-3,"rR",0,NULL,1,1,1,0,0 },
    { "hset",hsetCommand,-4,"wmF",0,NULL,1,1,1,0,0 },
    { "hsetnx",hsetnxCommand,4,"wmF",0,NULL,1,1,1,0,0 },
    { "hget",hgetCommand,3,"rF",0,NULL,1,1,1,0,0 },
    { "hmset",hsetCommand,-4,"wmF",0,NULL,1,1,1,0,0 },
    { "hmget",hmgetCommand,-3,"rF",0,NULL,1,1,1,0,0 },
    { "hincrby",hincrbyCommand,4,"wmF",0,NULL,1,1,1,0,0 },
    { "hincrbyfloat",hincrbyfloatCommand,4,"wmF",0,NULL,1,1,1,0,0 },
    { "hdel",hdelCommand,-3,"wF",0,NULL,1,1,1,0,0 },
    { "hlen",hlenCommand,2,"rF",0,NULL,1,1,1,0,0 },
    { "hstrlen",hstrlenCommand,3,"rF",0,NULL,1,1,1,0,0 },
    { "hkeys",hkeysCommand,2,"rS",0,NULL,1,1,1,0,0 },
    { "hvals",hvalsCommand,2,"rS",0,NULL,1,1,1,0,0 },
    { "hgetall",hgetallCommand,2,"r",0,NULL,1,1,1,0,0 },
    { "hmgetall",hmgetallCommand,-2,"r",0,NULL,1,-1,1,0,0 },
    { "hexists",hexistsCommand,3,"rF",0,NULL,1,1,1,0,0 },
    { "hscan",hscanCommand,-3,"rR",0,NULL,1,1,1,0,0 },
    { "incrby",incrbyCommand,3,"wmF",0,NULL,1,1,1,0,0 },
    { "decrby",decrbyCommand,3,"wmF",0,NULL,1,1,1,0,0 },
    { "incrbyfloat",incrbyfloatCommand,3,"wmF",0,NULL,1,1,1,0,0 },
    { "getset",getsetCommand,3,"wm",0,NULL,1,1,1,0,0 },
    { "mset",msetCommand,-3,"wm",0,NULL,1,-1,2,0,0 },
    { "msetnx",msetnxCommand,-3,"wm",0,NULL,1,-1,2,0,0 },
    { "randomkey",randomkeyCommand,1,"rR",0,NULL,0,0,0,0,0 },
    { "select",selectCommand,2,"lF",0,NULL,0,0,0,0,0 },
    { "swapdb",swapdbCommand,3,"wF",0,NULL,0,0,0,0,0 },
    { "move",moveCommand,3,"wF",0,NULL,1,1,1,0,0 },
    { "rename",renameCommand,3,"w",0,NULL,1,2,1,0,0 },
    { "renamenx",renamenxCommand,3,"wF",0,NULL,1,2,1,0,0 },
    { "expire",expireCommand,3,"wF",0,NULL,1,1,1,0,0 },
    { "expireat",expireatCommand,3,"wF",0,NULL,1,1,1,0,0 },
    { "pexpire",pexpireCommand,3,"wF",0,NULL,1,1,1,0,0 },
    { "pexpireat",pexpireatCommand,3,"wF",0,NULL,1,1,1,0,0 },
    { "keys",keysCommand,2,"rS",0,NULL,0,0,0,0,0 },
    { "scan",scanCommand,-2,"rR",0,NULL,0,0,0,0,0 },
    { "dbsize",dbsizeCommand,1,"rF",0,NULL,0,0,0,0,0 },
    { "auth",authCommand,2,"sltF",0,NULL,0,0,0,0,0 },
    { "ping",pingCommand,-1,"tF",0,NULL,0,0,0,0,0 },
    { "echo",echoCommand,2,"F",0,NULL,0,0,0,0,0 },
    { "save",saveCommand,1,"as",0,NULL,0,0,0,0,0 },
    { "bgsave",bgsaveCommand,-1,"a",0,NULL,0,0,0,0,0 },
    { "bgrewriteaof",bgrewriteaofCommand,1,"a",0,NULL,0,0,0,0,0 },
    { "rotateaoflog",rotateAOFIncrLogCommand,1,"a",0,NULL,0,0,0,0,0 },
    { "shutdown",shutdownCommand,-1,"alt",0,NULL,0,0,0,0,0 },
    { "lastsave",lastsaveCommand,1,"RF",0,NULL,0,0,0,0,0 },
    { "type",typeCommand,2,"rF",0,NULL,1,1,1,0,0 },
    { "multi",multiCommand,1,"sF",0,NULL,0,0,0,0,0 },
    { "exec",execCommand,1,"sM",0,NULL,0,0,0,0,0 },
    { "discard",discardCommand,1,"sF",0,NULL,0,0,0,0,0 },
    { "sync",syncCommand,1,"ars",0,NULL,0,0,0,0,0 },
    { "psync",syncCommand,3,"ars",0,NULL,0,0,0,0,0 },
    { "replconf",replconfCommand,-1,"aslt",0,NULL,0,0,0,0,0 },
    { "flushdb",flushdbCommand,-1,"w",0,NULL,0,0,0,0,0 },
    { "flushall",flushallCommand,-1,"w",0,NULL,0,0,0,0,0 },
    { "sort",sortCommand,-2,"wm",0,sortGetKeys,1,1,1,0,0 },
    { "info",infoCommand,-1,"lt",0,NULL,0,0,0,0,0 },
    { "monitor",monitorCommand,1,"as",0,NULL,0,0,0,0,0 },
    { "ttl",ttlCommand,2,"rF",0,NULL,1,1,1,0,0 },
    { "touch",touchCommand,-2,"rF",0,NULL,1,1,1,0,0 },
    { "pttl",pttlCommand,2,"rF",0,NULL,1,1,1,0,0 },
    { "persist",persistCommand,2,"wF",0,NULL,1,1,1,0,0 },
    { "slaveof",slaveofCommand,3,"ast",0,NULL,0,0,0,0,0 },
    { "role",roleCommand,1,"lst",0,NULL,0,0,0,0,0 },
    { "debug",debugCommand,-1,"as",0,NULL,0,0,0,0,0 },
    { "config",configCommand,-2,"lat",0,NULL,0,0,0,0,0 },
    { "subscribe",subscribeCommand,-2,"pslt",0,NULL,0,0,0,0,0 },
    { "unsubscribe",unsubscribeCommand,-1,"pslt",0,NULL,0,0,0,0,0 },
    { "psubscribe",psubscribeCommand,-2,"pslt",0,NULL,0,0,0,0,0 },
    { "punsubscribe",punsubscribeCommand,-1,"pslt",0,NULL,0,0,0,0,0 },
    { "publish",publishCommand,3,"pltF",0,NULL,0,0,0,0,0 },
    { "pubsub",pubsubCommand,-2,"pltR",0,NULL,0,0,0,0,0 },
    { "watch",watchCommand,-2,"sF",0,NULL,1,-1,1,0,0 },
    { "unwatch",unwatchCommand,1,"sF",0,NULL,0,0,0,0,0 },
    { "cluster",clusterCommand,-2,"a",0,NULL,0,0,0,0,0 },
    { "restore",restoreCommand,-4,"wm",0,NULL,1,1,1,0,0 },
    { "restore-asking",restoreCommand,-4,"wmk",0,NULL,1,1,1,0,0 },
    { "migrate",migrateCommand,-6,"w",0,migrateGetKeys,0,0,0,0,0 },
    { "asking",askingCommand,1,"F",0,NULL,0,0,0,0,0 },
    { "readonly",readonlyCommand,1,"F",0,NULL,0,0,0,0,0 },
    { "readwrite",readwriteCommand,1,"F",0,NULL,0,0,0,0,0 },
    { "dump",dumpCommand,2,"r",0,NULL,1,1,1,0,0 },
    { "object",objectCommand,-2,"r",0,NULL,2,2,2,0,0 },
    { "memory",memoryCommand,-2,"r",0,NULL,0,0,0,0,0 },
    { "client",clientCommand,-2,"as",0,NULL,0,0,0,0,0 },
    { "eval",evalCommand,-3,"s",0,evalGetKeys,0,0,0,0,0 },
    { "evalsha",evalShaCommand,-3,"s",0,evalGetKeys,0,0,0,0,0 },
    { "slowlog",slowlogCommand,-2,"a",0,NULL,0,0,0,0,0 },
    { "script",scriptCommand,-2,"s",0,NULL,0,0,0,0,0 },
    { "time",timeCommand,1,"RF",0,NULL,0,0,0,0,0 },
    { "bitop",bitopCommand,-4,"wm",0,NULL,2,-1,1,0,0 },
    { "bitcount",bitcountCommand,-2,"r",0,NULL,1,1,1,0,0 },
    { "bitpos",bitposCommand,-3,"r",0,NULL,1,1,1,0,0 },
    { "wait",waitCommand,3,"s",0,NULL,0,0,0,0,0 },
    { "command",commandCommand,0,"lt",0,NULL,0,0,0,0,0 },
    { "geoadd",geoaddCommand,-5,"wm",0,NULL,1,1,1,0,0 },
    { "georadius",georadiusCommand,-6,"w",0,georadiusGetKeys,1,1,1,0,0 },
    { "georadius_ro",georadiusroCommand,-6,"r",0,georadiusGetKeys,1,1,1,0,0 },
    { "georadiusbymember",georadiusbymemberCommand,-5,"w",0,georadiusGetKeys,1,1,1,0,0 },               // NOLINT
    { "georadiusbymember_ro",georadiusbymemberroCommand,-5,"r",0,georadiusGetKeys,1,1,1,0,0 },          // NOLINT
    { "geohash",geohashCommand,-2,"r",0,NULL,1,1,1,0,0 },
    { "geopos",geoposCommand,-2,"r",0,NULL,1,1,1,0,0 },
    { "geodist",geodistCommand,-4,"r",0,NULL,1,1,1,0,0 },
    { "pfselftest",pfselftestCommand,1,"a",0,NULL,0,0,0,0,0 },
    { "pfadd",pfaddCommand,-2,"wmF",0,NULL,1,1,1,0,0 },
    { "pfcount",pfcountCommand,-2,"r",0,NULL,1,-1,1,0,0 },
    { "pfmerge",pfmergeCommand,-2,"wm",0,NULL,1,-1,1,0,0 },
    { "pfdebug",pfdebugCommand,-3,"w",0,NULL,2,2,1,0,0 },
    { "post",securityWarningCommand,-1,"lt",0,NULL,0,0,0,0,0 },
    { "host:",securityWarningCommand,-1,"lt",0,NULL,0,0,0,0,0 },
    { "latency",latencyCommand,-2,"aslt",0,NULL,0,0,0,0,0 }
};

/* Populates the Redis Command Table starting from the hard coded list
* we have on top of redis.c file. */
void populateCommandTable(void) {
    int j;
    int numcommands = sizeof(redisCommandTable) / sizeof(struct redisCommand);

    for (j = 0; j < numcommands; j++) {
        struct redisCommand *c = redisCommandTable + j;
        c->flags = getCommandFlags(c->sflags);
    }
}

struct redisCommand* getCommandFromTable(const char* cmd) {
    int j;
    int numcommands = sizeof(redisCommandTable) / sizeof(struct redisCommand);

    for (j = 0; j < numcommands; j++) {
        struct redisCommand *c = redisCommandTable + j;

        if (!strcmp(c->name, cmd)) {
            return c;
        }
    }

    return NULL;
}

struct redisCommand* getCommandFromTable(size_t index) {
    return redisCommandTable + index;
    
}
size_t getCommandCount() {
    return sizeof(redisCommandTable) / sizeof(struct redisCommand);
}

class dummyClass {
 public:
    dummyClass() {
        populateCommandTable();
    }
};

static dummyClass dummy;

}  // namespace redis_port
}  // namespace tendisplus
