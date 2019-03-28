#include <limits.h>
#include <string.h>
#include <math.h>
#include <sstream>
#include <utility>

#include "glog/logging.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/redis_port.h"

namespace tendisplus {
namespace redis_port {

int stringmatchlen(const char *pattern, int patternLen,
    const char *string, int stringLen, int nocase)
{
  while(patternLen) {
    switch(pattern[0]) {
      case '*':
        while (pattern[1] == '*') {
          pattern++;
          patternLen--;
        }
        if (patternLen == 1)
          return 1; /* match */
        while(stringLen) {
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
          while(1) {
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
                if (tolower((int)pattern[0]) == tolower((int)string[0]))
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
          if (tolower((int)pattern[0]) != tolower((int)string[0]))
            return 0; /* no match */
        }
        string++;
        stringLen--;
        break;
    }
    pattern++;
    patternLen--;
    if (stringLen == 0) {
      while(*pattern == '*') {
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
    unsigned long *l;
    unsigned char *c;
    unsigned long skipval, word = 0, one;
    long pos = 0; /* Position of bit, to return to the caller. */
    unsigned long j;

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
    while ((unsigned long)c & (sizeof(*l)-1) && count) {
        if (*c != skipval) break;
        c++;
        count--;
        pos += 8;
    }

    /* Skip bits with full word step. */
    skipval = bit ? 0 : ULONG_MAX;
    l = (unsigned long*) c;
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

std::string ldtos(long double value) {
    char buf[256];
    int len;

    /* We use 17 digits precision since with 128 bit floats that precision
     * after rounding is able to represent most small decimal numbers in a way
     * that is "non surprising" for the user (that is, most small decimal
     * numbers will be represented in a way that when converted back into
     * a string are exactly the same as what the user typed.) */
    len = snprintf(buf, sizeof(buf), "%.17Lf", value);
    /* Now remove trailing zeroes after the '.' */
    if (strchr(buf, '.') != NULL) {
        char *p = buf+len-1;
        while (*p == '0') {
            p--;
            len--;
        }
        if (*p == '.') len--;
    }
    return std::string(buf, len);
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

int zslParseLexRangeItem(const char *c, std::string* dest, int *ex) {
    switch(c[0]) {
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
        *dest = std::string(c+1,strlen(c)-1);
        return 0;
    case '[': 
        *ex = 0; 
        *dest = std::string(c+1,strlen(c)-1);
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

}  // namespace redis_port
}  // namespace tendisplus
