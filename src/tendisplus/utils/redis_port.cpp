// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#include <limits.h>
#include <string.h>
#include <math.h>
#include <stdarg.h>
#include <sstream>
#include <utility>
#ifndef _WIN32
#include "sys/time.h"
#endif

#include "glog/logging.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/redis_port.h"
#include "tendisplus/utils/time.h"

namespace tendisplus {
namespace redis_port {

int stringmatchlen(const char* pattern,
                   int patternLen,
                   const char* string,
                   int stringLen,
                   int nocase) {
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
          if (stringmatchlen(
                pattern + 1, patternLen - 1, string, stringLen, nocase))
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
      case '[': {
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
              if (tolower(static_cast<int>(pattern[0])) ==
                  tolower(static_cast<int>(string[0])))
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
          if (tolower(static_cast<int>(pattern[0])) !=
              tolower(static_cast<int>(string[0])))
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

int64_t bitPos(const void* s, size_t count, uint32_t bit) {
  unsigned long* l;  // NOLINT:runtime/int
  unsigned char* c;
  unsigned long skipval, word = 0, one;  // NOLINT:runtime/int
  /* Position of bit, to return to the caller. */
  long pos = 0;     // NOLINT:runtime/int
  unsigned long j;  // NOLINT:runtime/int

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
  c = (unsigned char*)s;
  while ((unsigned long)c & (sizeof(*l) - 1) && count) {  // NOLINT:runtime/int
    if (*c != skipval)
      break;
    c++;
    count--;
    pos += 8;
  }

  /* Skip bits with full word step. */
  skipval = bit ? 0 : ULONG_MAX;
  l = (unsigned long*)c;  // NOLINT:runtime/int
  while (count >= sizeof(*l)) {
    if (*l != skipval)
      break;
    l++;
    count -= sizeof(*l);
    pos += sizeof(*l) * 8;
  }

  /* Load bytes into "word" considering the first byte as the most significant
   * (we basically consider it as written in big endian, since we consider the
   * string as a set of bits from left to right, with the first bit at
   * position zero.
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
  if (bit == 1 && word == 0)
    return -1;

  /* Last word left, scan bit by bit. The first thing we need is to
   * have a single "1" set in the most significant position in an
   * unsigned long. We don't know the size of the long so we use a
   * simple trick. */
  one = ULONG_MAX; /* All bits set to 1.*/
  one >>= 1;       /* All bits set to 1 but the MSB. */
  one = ~one;      /* All bits set to 0 but the MSB. */

  while (one) {
    if (((one & word) != 0) == bit)
      return pos;
    pos++;
    one >>= 1;
  }

  /* If we reached this point, there is a bug in the algorithm, since
   * the case of no match is handled as a special case before. */
  INVARIANT_D(0);
  return 0; /* Just to avoid warnings. */
}

size_t popCount(const void* s, long count) {  // (NOLINT)
  size_t bits = 0;
  const unsigned char* p = static_cast<const unsigned char*>(s);
  uint32_t* p4;
  static const unsigned char bitsinbyte[256] = {
    0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4,
    2, 3, 3, 4, 3, 4, 4, 5, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 1, 2, 2, 3, 2, 3, 3, 4,
    2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6,
    4, 5, 5, 6, 5, 6, 6, 7, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5,
    3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6,
    4, 5, 5, 6, 5, 6, 6, 7, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8};  // (NOLINT)

  /* Count initial bytes not aligned to 32 bit. */
  while ((unsigned long)p & 3 && count) {  // (NOLINT)
    bits += bitsinbyte[*p++];
    count--;
  }

  /* Count bits 16 bytes at a time */
  p4 = (uint32_t*)p;     // (NOLINT)
  while (count >= 16) {  // (NOLINT)
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
  while (count--)
    bits += bitsinbyte[*p++];  // (NOLINT)
  return bits;
}

/* Convert a long double into a string. If humanfriendly is non-zero
 * it does not use exponential format and trims trailing zeroes at the end,
 * however this results in loss of precision. Otherwise exp format is used
 * and the output of snprintf() is not modified.
 *
 * The function returns the length of the string or zero if there was not
 * enough buffer room to store it. */
int ld2string(char* buf, size_t len, long double value, int humanfriendly) {
  size_t l;

  if (isinf(value)) {
    /* Libc in odd systems (Hi Solaris!) will format infinite in a
     * different way, so better to handle it in an explicit way. */
    if (len < 5)
      return 0; /* No room. 5 is "-inf\0" */
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
    if (l + 1 > len)
      return 0; /* No room. */
                /* Now remove trailing zeroes after the '.' */
    if (strchr(buf, '.') != NULL) {
      char* p = buf + l - 1;
      while (*p == '0') {
        p--;
        l--;
      }
      if (*p == '.')
        l--;
    }
  } else {
    l = snprintf(buf, len, "%.17Lg", value);
    if (l + 1 > len)
      return 0; /* No room. */
  }
  buf[l] = '\0';
  return l;
}

/* Convert a string into a long long. Returns 1 if the string could be parsed
 * into a (non-overflowing) long long, 0 otherwise. The value will be set to
 * the parsed value when appropriate. */
int string2ll(const char* s, size_t slen, long long* value) {  // (NOLINT/int)
  const char* p = s;
  size_t plen = 0;
  int negative = 0;
  unsigned long long v;  //(NOLINT/int)

  if (plen == slen)
    return 0;

  /* Special case: first and only digit is 0. */
  if (slen == 1 && p[0] == '0') {
    if (value != NULL)
      *value = 0;
    return 1;
  }

  if (p[0] == '-') {
    negative = 1;
    p++;
    plen++;

    /* Abort on only a negative sign. */
    if (plen == slen)
      return 0;
  }

  /* First digit should be 1-9, otherwise the string should just be 0. */
  if (p[0] >= '1' && p[0] <= '9') {
    v = p[0] - '0';
    p++;
    plen++;
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

    if (v > (ULLONG_MAX - (p[0] - '0'))) /* Overflow. */
      return 0;
    v += p[0] - '0';

    p++;
    plen++;
  }

  /* Return if not all bytes were used. */
  if (plen < slen)
    return 0;

  if (negative) {
    if (v > ((uint64_t)(-(LLONG_MIN + 1)) + 1)) /* Overflow. (NOLINT/int)*/
      return 0;
    if (value != NULL)
      *value = -v;
  } else {
    if (v > LLONG_MAX) /* Overflow. */
      return 0;
    if (value != NULL)
      *value = v;
  }
  return 1;
}

std::string errorReply(const std::string& s) {
  if (s[0] == '-') {
    INVARIANT_D(s[s.size() - 2] == '\r');
    INVARIANT_D(s[s.size() - 1] == '\n');
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
    case '0':
      return 0;
    case '1':
      return 1;
    case '2':
      return 2;
    case '3':
      return 3;
    case '4':
      return 4;
    case '5':
      return 5;
    case '6':
      return 6;
    case '7':
      return 7;
    case '8':
      return 8;
    case '9':
      return 9;
    case 'a':
    case 'A':
      return 10;
    case 'b':
    case 'B':
      return 11;
    case 'c':
    case 'C':
      return 12;
    case 'd':
    case 'D':
      return 13;
    case 'e':
    case 'E':
      return 14;
    case 'f':
    case 'F':
      return 15;
    default:
      return 0;
  }
}

int random() {
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

int zslParseLexRangeItem(const char* c, std::string* dest, int* ex) {
  switch (c[0]) {
    case '+':
      if (c[1] != '\0')
        return -1;
      *ex = 0;
      *dest = ZLEXMAX;
      return 0;
    case '-':
      if (c[1] != '\0')
        return -1;
      *ex = 0;
      *dest = ZLEXMIN;
      return 0;
    case '(':
      *ex = 1;
      *dest = std::string(c + 1, strlen(c) - 1);
      return 0;
    case '[':
      *ex = 0;
      *dest = std::string(c + 1, strlen(c) - 1);
      return 0;
    default:
      return -1;
  }
}

int zslParseLexRange(const char* min, const char* max, Zlexrangespec* spec) {
  if (zslParseLexRangeItem(min, &spec->min, &spec->minex) != 0 ||
      zslParseLexRangeItem(max, &spec->max, &spec->maxex) != 0) {
    return -1;
  }
  return 0;
}

int zslParseRange(const char* min, const char* max, Zrangespec* spec) {
  char* eptr;
  spec->minex = spec->maxex = 0;

  /* Parse the min-max interval. If one of the values is prefixed
   * by the "(" character, it's considered "open". For instance
   * ZRANGEBYSCORE zset (1.5 (2.5 will match min < x < max
   * ZRANGEBYSCORE zset 1.5 2.5 will instead match min <= x <= max */
  if (min[0] == '(') {
    spec->min = strtod(min + 1, &eptr);
    if (eptr[0] != '\0' || isnan(spec->min))
      return -1;
    spec->minex = 1;
  } else {
    spec->min = strtod(min, &eptr);
    if (eptr[0] != '\0' || isnan(spec->min))
      return -1;
  }

  if (max[0] == '(') {
    spec->max = strtod(max + 1, &eptr);
    if (eptr[0] != '\0' || isnan(spec->max))
      return -1;
    spec->maxex = 1;
  } else {
    spec->max = strtod(max, &eptr);
    if (eptr[0] != '\0' || isnan(spec->max))
      return -1;
  }
  return 0;
}

std::vector<std::string>* splitargs(std::vector<std::string>& result,  // NOLINT
                                    const std::string& lineStr) {
  const char* line = lineStr.c_str();
  const char* p = line;

  while (1) {
    /* skip blanks */
    while (*p && isspace(*p))
      p++;
    if (*p) {
      /* get a token */
      int inq = 0;  /* set to 1 if we are in "quotes" */
      int insq = 0; /* set to 1 if we are in 'single quotes' */
      int done = 0;

      std::string current;
      while (!done) {
        if (inq) {
          if (*p == '\\' && *(p + 1) == 'x' && is_hex_digit(*(p + 2)) &&
              is_hex_digit(*(p + 3))) {
            unsigned char byte;

            byte = static_cast<unsigned char>(
              (hex_digit_to_int(*(p + 2)) * 16) + hex_digit_to_int(*(p + 3)));
            current.push_back(static_cast<char>(byte));
            p += 3;
          } else if (*p == '\\' && *(p + 1)) {
            char c;

            p++;
            switch (*p) {
              case 'n':
                c = '\n';
                break;
              case 'r':
                c = '\r';
                break;
              case 't':
                c = '\t';
                break;
              case 'b':
                c = '\b';
                break;
              case 'a':
                c = '\a';
                break;
              default:
                c = *p;
                break;
            }
            current.push_back(c);
          } else if (*p == '"') {
            /* closing quote must be followed by a space or
             * nothing at all. */
            if (*(p + 1) && !isspace(*(p + 1)))
              goto err;
            done = 1;
          } else if (!*p) {
            /* unterminated quotes */
            goto err;
          } else {
            current.push_back(*p);
          }
        } else if (insq) {
          if (*p == '\\' && *(p + 1) == '\'') {
            p++;
            current.push_back('\'');
          } else if (*p == '\'') {
            /* closing quote must be followed by a space or
             * nothing at all. */
            if (*(p + 1) && !isspace(*(p + 1)))
              goto err;
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
        if (*p)
          p++;
      }
      /* add the token to the vector */
      result.emplace_back(std::move(current));
    } else {
      /* Even on empty input string return something not NULL. */
      return &result;
    }
  }

err:
  return NULL;
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
  0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7, 0x8108,
  0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef, 0x1231, 0x0210,
  0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6, 0x9339, 0x8318, 0xb37b,
  0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de, 0x2462, 0x3443, 0x0420, 0x1401,
  0x64e6, 0x74c7, 0x44a4, 0x5485, 0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee,
  0xf5cf, 0xc5ac, 0xd58d, 0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6,
  0x5695, 0x46b4, 0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d,
  0xc7bc, 0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
  0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b, 0x5af5,
  0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12, 0xdbfd, 0xcbdc,
  0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a, 0x6ca6, 0x7c87, 0x4ce4,
  0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41, 0xedae, 0xfd8f, 0xcdec, 0xddcd,
  0xad2a, 0xbd0b, 0x8d68, 0x9d49, 0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13,
  0x2e32, 0x1e51, 0x0e70, 0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a,
  0x9f59, 0x8f78, 0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e,
  0xe16f, 0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
  0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e, 0x02b1,
  0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256, 0xb5ea, 0xa5cb,
  0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d, 0x34e2, 0x24c3, 0x14a0,
  0x0481, 0x7466, 0x6447, 0x5424, 0x4405, 0xa7db, 0xb7fa, 0x8799, 0x97b8,
  0xe75f, 0xf77e, 0xc71d, 0xd73c, 0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657,
  0x7676, 0x4615, 0x5634, 0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9,
  0xb98a, 0xa9ab, 0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882,
  0x28a3, 0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
  0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92, 0xfd2e,
  0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9, 0x7c26, 0x6c07,
  0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1, 0xef1f, 0xff3e, 0xcf5d,
  0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8, 0x6e17, 0x7e36, 0x4e55, 0x5e74,
  0x2e93, 0x3eb2, 0x0ed1, 0x1ef0};

uint16_t crc16(const char* buf, int len) {
  int counter;
  uint16_t crc = 0;
  for (counter = 0; counter < len; counter++)
    crc = (crc << 8) ^ crc16tab[((crc >> 8) ^ *buf++) & 0x00FF];
  return crc;
}

static const uint64_t crc64_tab[256] = {
  UINT64_C(0x0000000000000000), UINT64_C(0x7ad870c830358979),
  UINT64_C(0xf5b0e190606b12f2), UINT64_C(0x8f689158505e9b8b),
  UINT64_C(0xc038e5739841b68f), UINT64_C(0xbae095bba8743ff6),
  UINT64_C(0x358804e3f82aa47d), UINT64_C(0x4f50742bc81f2d04),
  UINT64_C(0xab28ecb46814fe75), UINT64_C(0xd1f09c7c5821770c),
  UINT64_C(0x5e980d24087fec87), UINT64_C(0x24407dec384a65fe),
  UINT64_C(0x6b1009c7f05548fa), UINT64_C(0x11c8790fc060c183),
  UINT64_C(0x9ea0e857903e5a08), UINT64_C(0xe478989fa00bd371),
  UINT64_C(0x7d08ff3b88be6f81), UINT64_C(0x07d08ff3b88be6f8),
  UINT64_C(0x88b81eabe8d57d73), UINT64_C(0xf2606e63d8e0f40a),
  UINT64_C(0xbd301a4810ffd90e), UINT64_C(0xc7e86a8020ca5077),
  UINT64_C(0x4880fbd87094cbfc), UINT64_C(0x32588b1040a14285),
  UINT64_C(0xd620138fe0aa91f4), UINT64_C(0xacf86347d09f188d),
  UINT64_C(0x2390f21f80c18306), UINT64_C(0x594882d7b0f40a7f),
  UINT64_C(0x1618f6fc78eb277b), UINT64_C(0x6cc0863448deae02),
  UINT64_C(0xe3a8176c18803589), UINT64_C(0x997067a428b5bcf0),
  UINT64_C(0xfa11fe77117cdf02), UINT64_C(0x80c98ebf2149567b),
  UINT64_C(0x0fa11fe77117cdf0), UINT64_C(0x75796f2f41224489),
  UINT64_C(0x3a291b04893d698d), UINT64_C(0x40f16bccb908e0f4),
  UINT64_C(0xcf99fa94e9567b7f), UINT64_C(0xb5418a5cd963f206),
  UINT64_C(0x513912c379682177), UINT64_C(0x2be1620b495da80e),
  UINT64_C(0xa489f35319033385), UINT64_C(0xde51839b2936bafc),
  UINT64_C(0x9101f7b0e12997f8), UINT64_C(0xebd98778d11c1e81),
  UINT64_C(0x64b116208142850a), UINT64_C(0x1e6966e8b1770c73),
  UINT64_C(0x8719014c99c2b083), UINT64_C(0xfdc17184a9f739fa),
  UINT64_C(0x72a9e0dcf9a9a271), UINT64_C(0x08719014c99c2b08),
  UINT64_C(0x4721e43f0183060c), UINT64_C(0x3df994f731b68f75),
  UINT64_C(0xb29105af61e814fe), UINT64_C(0xc849756751dd9d87),
  UINT64_C(0x2c31edf8f1d64ef6), UINT64_C(0x56e99d30c1e3c78f),
  UINT64_C(0xd9810c6891bd5c04), UINT64_C(0xa3597ca0a188d57d),
  UINT64_C(0xec09088b6997f879), UINT64_C(0x96d1784359a27100),
  UINT64_C(0x19b9e91b09fcea8b), UINT64_C(0x636199d339c963f2),
  UINT64_C(0xdf7adabd7a6e2d6f), UINT64_C(0xa5a2aa754a5ba416),
  UINT64_C(0x2aca3b2d1a053f9d), UINT64_C(0x50124be52a30b6e4),
  UINT64_C(0x1f423fcee22f9be0), UINT64_C(0x659a4f06d21a1299),
  UINT64_C(0xeaf2de5e82448912), UINT64_C(0x902aae96b271006b),
  UINT64_C(0x74523609127ad31a), UINT64_C(0x0e8a46c1224f5a63),
  UINT64_C(0x81e2d7997211c1e8), UINT64_C(0xfb3aa75142244891),
  UINT64_C(0xb46ad37a8a3b6595), UINT64_C(0xceb2a3b2ba0eecec),
  UINT64_C(0x41da32eaea507767), UINT64_C(0x3b024222da65fe1e),
  UINT64_C(0xa2722586f2d042ee), UINT64_C(0xd8aa554ec2e5cb97),
  UINT64_C(0x57c2c41692bb501c), UINT64_C(0x2d1ab4dea28ed965),
  UINT64_C(0x624ac0f56a91f461), UINT64_C(0x1892b03d5aa47d18),
  UINT64_C(0x97fa21650afae693), UINT64_C(0xed2251ad3acf6fea),
  UINT64_C(0x095ac9329ac4bc9b), UINT64_C(0x7382b9faaaf135e2),
  UINT64_C(0xfcea28a2faafae69), UINT64_C(0x8632586aca9a2710),
  UINT64_C(0xc9622c4102850a14), UINT64_C(0xb3ba5c8932b0836d),
  UINT64_C(0x3cd2cdd162ee18e6), UINT64_C(0x460abd1952db919f),
  UINT64_C(0x256b24ca6b12f26d), UINT64_C(0x5fb354025b277b14),
  UINT64_C(0xd0dbc55a0b79e09f), UINT64_C(0xaa03b5923b4c69e6),
  UINT64_C(0xe553c1b9f35344e2), UINT64_C(0x9f8bb171c366cd9b),
  UINT64_C(0x10e3202993385610), UINT64_C(0x6a3b50e1a30ddf69),
  UINT64_C(0x8e43c87e03060c18), UINT64_C(0xf49bb8b633338561),
  UINT64_C(0x7bf329ee636d1eea), UINT64_C(0x012b592653589793),
  UINT64_C(0x4e7b2d0d9b47ba97), UINT64_C(0x34a35dc5ab7233ee),
  UINT64_C(0xbbcbcc9dfb2ca865), UINT64_C(0xc113bc55cb19211c),
  UINT64_C(0x5863dbf1e3ac9dec), UINT64_C(0x22bbab39d3991495),
  UINT64_C(0xadd33a6183c78f1e), UINT64_C(0xd70b4aa9b3f20667),
  UINT64_C(0x985b3e827bed2b63), UINT64_C(0xe2834e4a4bd8a21a),
  UINT64_C(0x6debdf121b863991), UINT64_C(0x1733afda2bb3b0e8),
  UINT64_C(0xf34b37458bb86399), UINT64_C(0x8993478dbb8deae0),
  UINT64_C(0x06fbd6d5ebd3716b), UINT64_C(0x7c23a61ddbe6f812),
  UINT64_C(0x3373d23613f9d516), UINT64_C(0x49aba2fe23cc5c6f),
  UINT64_C(0xc6c333a67392c7e4), UINT64_C(0xbc1b436e43a74e9d),
  UINT64_C(0x95ac9329ac4bc9b5), UINT64_C(0xef74e3e19c7e40cc),
  UINT64_C(0x601c72b9cc20db47), UINT64_C(0x1ac40271fc15523e),
  UINT64_C(0x5594765a340a7f3a), UINT64_C(0x2f4c0692043ff643),
  UINT64_C(0xa02497ca54616dc8), UINT64_C(0xdafce7026454e4b1),
  UINT64_C(0x3e847f9dc45f37c0), UINT64_C(0x445c0f55f46abeb9),
  UINT64_C(0xcb349e0da4342532), UINT64_C(0xb1eceec59401ac4b),
  UINT64_C(0xfebc9aee5c1e814f), UINT64_C(0x8464ea266c2b0836),
  UINT64_C(0x0b0c7b7e3c7593bd), UINT64_C(0x71d40bb60c401ac4),
  UINT64_C(0xe8a46c1224f5a634), UINT64_C(0x927c1cda14c02f4d),
  UINT64_C(0x1d148d82449eb4c6), UINT64_C(0x67ccfd4a74ab3dbf),
  UINT64_C(0x289c8961bcb410bb), UINT64_C(0x5244f9a98c8199c2),
  UINT64_C(0xdd2c68f1dcdf0249), UINT64_C(0xa7f41839ecea8b30),
  UINT64_C(0x438c80a64ce15841), UINT64_C(0x3954f06e7cd4d138),
  UINT64_C(0xb63c61362c8a4ab3), UINT64_C(0xcce411fe1cbfc3ca),
  UINT64_C(0x83b465d5d4a0eece), UINT64_C(0xf96c151de49567b7),
  UINT64_C(0x76048445b4cbfc3c), UINT64_C(0x0cdcf48d84fe7545),
  UINT64_C(0x6fbd6d5ebd3716b7), UINT64_C(0x15651d968d029fce),
  UINT64_C(0x9a0d8ccedd5c0445), UINT64_C(0xe0d5fc06ed698d3c),
  UINT64_C(0xaf85882d2576a038), UINT64_C(0xd55df8e515432941),
  UINT64_C(0x5a3569bd451db2ca), UINT64_C(0x20ed197575283bb3),
  UINT64_C(0xc49581ead523e8c2), UINT64_C(0xbe4df122e51661bb),
  UINT64_C(0x3125607ab548fa30), UINT64_C(0x4bfd10b2857d7349),
  UINT64_C(0x04ad64994d625e4d), UINT64_C(0x7e7514517d57d734),
  UINT64_C(0xf11d85092d094cbf), UINT64_C(0x8bc5f5c11d3cc5c6),
  UINT64_C(0x12b5926535897936), UINT64_C(0x686de2ad05bcf04f),
  UINT64_C(0xe70573f555e26bc4), UINT64_C(0x9ddd033d65d7e2bd),
  UINT64_C(0xd28d7716adc8cfb9), UINT64_C(0xa85507de9dfd46c0),
  UINT64_C(0x273d9686cda3dd4b), UINT64_C(0x5de5e64efd965432),
  UINT64_C(0xb99d7ed15d9d8743), UINT64_C(0xc3450e196da80e3a),
  UINT64_C(0x4c2d9f413df695b1), UINT64_C(0x36f5ef890dc31cc8),
  UINT64_C(0x79a59ba2c5dc31cc), UINT64_C(0x037deb6af5e9b8b5),
  UINT64_C(0x8c157a32a5b7233e), UINT64_C(0xf6cd0afa9582aa47),
  UINT64_C(0x4ad64994d625e4da), UINT64_C(0x300e395ce6106da3),
  UINT64_C(0xbf66a804b64ef628), UINT64_C(0xc5bed8cc867b7f51),
  UINT64_C(0x8aeeace74e645255), UINT64_C(0xf036dc2f7e51db2c),
  UINT64_C(0x7f5e4d772e0f40a7), UINT64_C(0x05863dbf1e3ac9de),
  UINT64_C(0xe1fea520be311aaf), UINT64_C(0x9b26d5e88e0493d6),
  UINT64_C(0x144e44b0de5a085d), UINT64_C(0x6e963478ee6f8124),
  UINT64_C(0x21c640532670ac20), UINT64_C(0x5b1e309b16452559),
  UINT64_C(0xd476a1c3461bbed2), UINT64_C(0xaeaed10b762e37ab),
  UINT64_C(0x37deb6af5e9b8b5b), UINT64_C(0x4d06c6676eae0222),
  UINT64_C(0xc26e573f3ef099a9), UINT64_C(0xb8b627f70ec510d0),
  UINT64_C(0xf7e653dcc6da3dd4), UINT64_C(0x8d3e2314f6efb4ad),
  UINT64_C(0x0256b24ca6b12f26), UINT64_C(0x788ec2849684a65f),
  UINT64_C(0x9cf65a1b368f752e), UINT64_C(0xe62e2ad306bafc57),
  UINT64_C(0x6946bb8b56e467dc), UINT64_C(0x139ecb4366d1eea5),
  UINT64_C(0x5ccebf68aecec3a1), UINT64_C(0x2616cfa09efb4ad8),
  UINT64_C(0xa97e5ef8cea5d153), UINT64_C(0xd3a62e30fe90582a),
  UINT64_C(0xb0c7b7e3c7593bd8), UINT64_C(0xca1fc72bf76cb2a1),
  UINT64_C(0x45775673a732292a), UINT64_C(0x3faf26bb9707a053),
  UINT64_C(0x70ff52905f188d57), UINT64_C(0x0a2722586f2d042e),
  UINT64_C(0x854fb3003f739fa5), UINT64_C(0xff97c3c80f4616dc),
  UINT64_C(0x1bef5b57af4dc5ad), UINT64_C(0x61372b9f9f784cd4),
  UINT64_C(0xee5fbac7cf26d75f), UINT64_C(0x9487ca0fff135e26),
  UINT64_C(0xdbd7be24370c7322), UINT64_C(0xa10fceec0739fa5b),
  UINT64_C(0x2e675fb4576761d0), UINT64_C(0x54bf2f7c6752e8a9),
  UINT64_C(0xcdcf48d84fe75459), UINT64_C(0xb71738107fd2dd20),
  UINT64_C(0x387fa9482f8c46ab), UINT64_C(0x42a7d9801fb9cfd2),
  UINT64_C(0x0df7adabd7a6e2d6), UINT64_C(0x772fdd63e7936baf),
  UINT64_C(0xf8474c3bb7cdf024), UINT64_C(0x829f3cf387f8795d),
  UINT64_C(0x66e7a46c27f3aa2c), UINT64_C(0x1c3fd4a417c62355),
  UINT64_C(0x935745fc4798b8de), UINT64_C(0xe98f353477ad31a7),
  UINT64_C(0xa6df411fbfb21ca3), UINT64_C(0xdc0731d78f8795da),
  UINT64_C(0x536fa08fdfd90e51), UINT64_C(0x29b7d047efec8728),
};

uint64_t crc64(uint64_t crc, const unsigned char* s, uint64_t l) {
  uint64_t j;

  for (j = 0; j < l; j++) {
    uint8_t byte = s[j];
    crc = crc64_tab[(uint8_t)crc ^ byte] ^ (crc >> 8);
  }
  return crc;
}

void memrev64(void* p) {
  unsigned char *x = reinterpret_cast<unsigned char*>(p), t;

  t = x[0];
  x[0] = x[7];
  x[7] = t;
  t = x[1];
  x[1] = x[6];
  x[6] = t;
  t = x[2];
  x[2] = x[5];
  x[5] = t;
  t = x[3];
  x[3] = x[4];
  x[4] = t;
}

uint64_t htonll(uint64_t v) {
  memrev64(&v);
  return v;
}

uint64_t ntohll(uint64_t v) {
  memrev64(&v);
  return v;
}

/* We have 16384 hash slots. The hash slot of a given key is obtained
 * as the least significant 14 bits of the crc16 of the key.
 *
 * However if the key contains the {...} pattern, only the part between
 * { and } is hashed. This may be useful in the future to force certain
 * keys to be in the same node (assuming no resharding is in progress). */
unsigned int keyHashSlot(const char* key, size_t keylen) {
  size_t s, e; /* start-end indexes of { and } */

  for (s = 0; s < keylen; s++)
    if (key[s] == '{')
      break;

  /* No '{' ? Hash the whole key. This is the base case. */
  if (s == keylen)
    return crc16(key, keylen) & 0x3FFF;

  /* '{' found? Check if we have the corresponding '}'. */
  for (e = s + 1; e < keylen; e++)
    if (key[e] == '}')
      break;

  /* No '}' or nothing betweeen {} ? Hash the whole key. */
  if (e == keylen || e == s + 1)
    return crc16(key, keylen) & 0x3FFF;

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
  const char* f = sflags;

  while (*f != '\0') {
    switch (*f) {
      case 'w':
        flags |= CMD_WRITE;
        break;
      case 'r':
        flags |= CMD_READONLY;
        break;
      case 'm':
        flags |= CMD_DENYOOM;
        break;
      case 'a':
        flags |= CMD_ADMIN;
        break;
      case 'p':
        flags |= CMD_PUBSUB;
        break;
      case 's':
        flags |= CMD_NOSCRIPT;
        break;
      case 'R':
        flags |= CMD_RANDOM;
        break;
      case 'S':
        flags |= CMD_SORT_FOR_SCRIPT;
        break;
      case 'l':
        flags |= CMD_LOADING;
        break;
      case 't':
        flags |= CMD_STALE;
        break;
      case 'M':
        flags |= CMD_SKIP_MONITOR;
        break;
      case 'k':
        flags |= CMD_ASKING;
        break;
      case 'F':
        flags |= CMD_FAST;
        break;
      default:
        INVARIANT_D(0);
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
  {"module", moduleCommand, -2, "as", 0, NULL, 0, 0, 0, 0, 0},
  {"get", getCommand, 2, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"getvsn", getvsnCommand, 2, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"set", setCommand, -3, "wm", 0, NULL, 1, 1, 1, 0, 0},
  {"cas", casCommand, 4, "wm", 0, NULL, 1, 1, 1, 0, 0},
  {"setnx", setnxCommand, 3, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"setex", setexCommand, 4, "wm", 0, NULL, 1, 1, 1, 0, 0},
  {"psetex", psetexCommand, 4, "wm", 0, NULL, 1, 1, 1, 0, 0},
  {"append", appendCommand, 3, "wm", 0, NULL, 1, 1, 1, 0, 0},
  {"strlen", strlenCommand, 2, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"del", delCommand, -2, "w", 0, NULL, 1, -1, 1, 0, 0},
  {"unlink", unlinkCommand, -2, "wF", 0, NULL, 1, -1, 1, 0, 0},
  {"exists", existsCommand, -2, "rF", 0, NULL, 1, -1, 1, 0, 0},
  {"setbit", setbitCommand, 4, "wm", 0, NULL, 1, 1, 1, 0, 0},
  {"getbit", getbitCommand, 3, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"bitfield", bitfieldCommand, -2, "wm", 0, NULL, 1, 1, 1, 0, 0},
  {"setrange", setrangeCommand, 4, "wm", 0, NULL, 1, 1, 1, 0, 0},
  {"getrange", getrangeCommand, 4, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"substr", getrangeCommand, 4, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"incr", incrCommand, 2, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"decr", decrCommand, 2, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"increx", increxCommand, 3, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"mget", mgetCommand, -2, "rF", 0, NULL, 1, -1, 1, 0, 0},
  {"rpush", rpushCommand, -3, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"lpush", lpushCommand, -3, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"rpushx", rpushxCommand, -3, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"lpushx", lpushxCommand, -3, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"linsert", linsertCommand, 5, "wm", 0, NULL, 1, 1, 1, 0, 0},
  {"rpop", rpopCommand, 2, "wF", 0, NULL, 1, 1, 1, 0, 0},
  {"lpop", lpopCommand, 2, "wF", 0, NULL, 1, 1, 1, 0, 0},
  {"brpop", brpopCommand, -3, "ws", 0, NULL, 1, -2, 1, 0, 0},
  {"brpoplpush", brpoplpushCommand, 4, "wms", 0, NULL, 1, 2, 1, 0, 0},
  {"blpop", blpopCommand, -3, "ws", 0, NULL, 1, -2, 1, 0, 0},
  {"llen", llenCommand, 2, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"lindex", lindexCommand, 3, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"lset", lsetCommand, 4, "wm", 0, NULL, 1, 1, 1, 0, 0},
  {"lrange", lrangeCommand, 4, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"ltrim", ltrimCommand, 4, "w", 0, NULL, 1, 1, 1, 0, 0},
  {"lrem", lremCommand, 4, "w", 0, NULL, 1, 1, 1, 0, 0},
  {"rpoplpush", rpoplpushCommand, 3, "wm", 0, NULL, 1, 2, 1, 0, 0},
  {"sadd", saddCommand, -3, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"srem", sremCommand, -3, "wF", 0, NULL, 1, 1, 1, 0, 0},
  {"smove", smoveCommand, 4, "wF", 0, NULL, 1, 2, 1, 0, 0},
  {"sismember", sismemberCommand, 3, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"scard", scardCommand, 2, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"spop", spopCommand, -2, "wRF", 0, NULL, 1, 1, 1, 0, 0},
  {"srandmember", srandmemberCommand, -2, "rR", 0, NULL, 1, 1, 1, 0, 0},
  {"sinter", sinterCommand, -2, "rS", 0, NULL, 1, -1, 1, 0, 0},
  {"sinterstore", sinterstoreCommand, -3, "wm", 0, NULL, 1, -1, 1, 0, 0},
  {"sunion", sunionCommand, -2, "rS", 0, NULL, 1, -1, 1, 0, 0},
  {"sunionstore", sunionstoreCommand, -3, "wm", 0, NULL, 1, -1, 1, 0, 0},
  {"sdiff", sdiffCommand, -2, "rS", 0, NULL, 1, -1, 1, 0, 0},
  {"sdiffstore", sdiffstoreCommand, -3, "wm", 0, NULL, 1, -1, 1, 0, 0},
  {"smembers", sinterCommand, 2, "rS", 0, NULL, 1, 1, 1, 0, 0},
  {"sscan", sscanCommand, -3, "rR", 0, NULL, 1, 1, 1, 0, 0},
  {"zadd", zaddCommand, -4, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"zincrby", zincrbyCommand, 4, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"zrem", zremCommand, -3, "wF", 0, NULL, 1, 1, 1, 0, 0},
  {"zremrangebyscore", zremrangebyscoreCommand, 4, "w", 0, NULL, 1, 1, 1, 0, 0},
  {"zremrangebyrank", zremrangebyrankCommand, 4, "w", 0, NULL, 1, 1, 1, 0, 0},
  {"zremrangebylex", zremrangebylexCommand, 4, "w", 0, NULL, 1, 1, 1, 0, 0},
  {"zunionstore",
   zunionstoreCommand,
   -4,
   "wm",
   0,
   zunionInterGetKeys,
   0,
   0,
   0,
   0,
   0},  // NOLINT
  {"zinterstore",
   zinterstoreCommand,
   -4,
   "wm",
   0,
   zunionInterGetKeys,
   0,
   0,
   0,
   0,
   0},  // NOLINT
  {"zrange", zrangeCommand, -4, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"zrangebyscore", zrangebyscoreCommand, -4, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"zrevrangebyscore",
   zrevrangebyscoreCommand,
   -4,
   "r",
   0,
   NULL,
   1,
   1,
   1,
   0,
   0},  // NOLINT
  {"zrangebylex", zrangebylexCommand, -4, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"zrevrangebylex", zrevrangebylexCommand, -4, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"zcount", zcountCommand, 4, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"zlexcount", zlexcountCommand, 4, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"zrevrange", zrevrangeCommand, -4, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"zcard", zcardCommand, 2, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"zscore", zscoreCommand, 3, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"zrank", zrankCommand, 3, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"zrevrank", zrevrankCommand, 3, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"zscan", zscanCommand, -3, "rR", 0, NULL, 1, 1, 1, 0, 0},
  {"hset", hsetCommand, -4, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"hsetnx", hsetnxCommand, 4, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"hget", hgetCommand, 3, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"hmset", hsetCommand, -4, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"hmget", hmgetCommand, -3, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"hincrby", hincrbyCommand, 4, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"hincrbyfloat", hincrbyfloatCommand, 4, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"hdel", hdelCommand, -3, "wF", 0, NULL, 1, 1, 1, 0, 0},
  {"hlen", hlenCommand, 2, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"hstrlen", hstrlenCommand, 3, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"hkeys", hkeysCommand, 2, "rS", 0, NULL, 1, 1, 1, 0, 0},
  {"hvals", hvalsCommand, 2, "rS", 0, NULL, 1, 1, 1, 0, 0},
  {"hgetall", hgetallCommand, 2, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"hexists", hexistsCommand, 3, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"hscan", hscanCommand, -3, "rR", 0, NULL, 1, 1, 1, 0, 0},
  {"incrby", incrbyCommand, 3, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"decrby", decrbyCommand, 3, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"incrbyfloat", incrbyfloatCommand, 3, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"getset", getsetCommand, 3, "wm", 0, NULL, 1, 1, 1, 0, 0},
  {"mset", msetCommand, -3, "wm", 0, NULL, 1, -1, 2, 0, 0},
  {"msetnx", msetnxCommand, -3, "wm", 0, NULL, 1, -1, 2, 0, 0},
  {"randomkey", randomkeyCommand, 1, "rR", 0, NULL, 0, 0, 0, 0, 0},
  {"select", selectCommand, 2, "lF", 0, NULL, 0, 0, 0, 0, 0},
  {"swapdb", swapdbCommand, 3, "wF", 0, NULL, 0, 0, 0, 0, 0},
  {"move", moveCommand, 3, "wF", 0, NULL, 1, 1, 1, 0, 0},
  {"rename", renameCommand, 3, "w", 0, NULL, 1, 2, 1, 0, 0},
  {"renamenx", renamenxCommand, 3, "wF", 0, NULL, 1, 2, 1, 0, 0},
  {"expire", expireCommand, 3, "wF", 0, NULL, 1, 1, 1, 0, 0},
  {"expireat", expireatCommand, 3, "wF", 0, NULL, 1, 1, 1, 0, 0},
  {"pexpire", pexpireCommand, 3, "wF", 0, NULL, 1, 1, 1, 0, 0},
  {"pexpireat", pexpireatCommand, 3, "wF", 0, NULL, 1, 1, 1, 0, 0},
  {"keys", keysCommand, 2, "rS", 0, NULL, 0, 0, 0, 0, 0},
  {"scan", scanCommand, -2, "rR", 0, NULL, 0, 0, 0, 0, 0},
  {"dbsize", dbsizeCommand, 1, "rF", 0, NULL, 0, 0, 0, 0, 0},
  {"auth", authCommand, 2, "sltF", 0, NULL, 0, 0, 0, 0, 0},
  {"ping", pingCommand, -1, "tF", 0, NULL, 0, 0, 0, 0, 0},
  {"echo", echoCommand, 2, "F", 0, NULL, 0, 0, 0, 0, 0},
  {"save", saveCommand, 1, "as", 0, NULL, 0, 0, 0, 0, 0},
  {"bgsave", bgsaveCommand, -1, "a", 0, NULL, 0, 0, 0, 0, 0},
  {"bgrewriteaof", bgrewriteaofCommand, 1, "a", 0, NULL, 0, 0, 0, 0, 0},
  {"rotateaoflog", rotateAOFIncrLogCommand, 1, "a", 0, NULL, 0, 0, 0, 0, 0},
  {"shutdown", shutdownCommand, -1, "alt", 0, NULL, 0, 0, 0, 0, 0},
  {"lastsave", lastsaveCommand, 1, "RF", 0, NULL, 0, 0, 0, 0, 0},
  {"type", typeCommand, 2, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"multi", multiCommand, 1, "sF", 0, NULL, 0, 0, 0, 0, 0},
  {"exec", execCommand, 1, "sM", 0, NULL, 0, 0, 0, 0, 0},
  {"discard", discardCommand, 1, "sF", 0, NULL, 0, 0, 0, 0, 0},
  {"sync", syncCommand, 1, "ars", 0, NULL, 0, 0, 0, 0, 0},
  {"psync", syncCommand, 3, "ars", 0, NULL, 0, 0, 0, 0, 0},
  {"replconf", replconfCommand, -1, "aslt", 0, NULL, 0, 0, 0, 0, 0},
  {"flushdb", flushdbCommand, -1, "w", 0, NULL, 0, 0, 0, 0, 0},
  {"flushall", flushallCommand, -1, "w", 0, NULL, 0, 0, 0, 0, 0},
  {"sort", sortCommand, -2, "wm", 0, sortGetKeys, 1, 1, 1, 0, 0},
  {"info", infoCommand, -1, "lt", 0, NULL, 0, 0, 0, 0, 0},
  {"monitor", monitorCommand, 1, "r", 0, NULL, 0, 0, 0, 0, 0},
  {"ttl", ttlCommand, 2, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"touch", touchCommand, -2, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"pttl", pttlCommand, 2, "rF", 0, NULL, 1, 1, 1, 0, 0},
  {"persist", persistCommand, 2, "wF", 0, NULL, 1, 1, 1, 0, 0},
  {"slaveof", slaveofCommand, 3, "ast", 0, NULL, 0, 0, 0, 0, 0},
  {"role", roleCommand, 1, "lst", 0, NULL, 0, 0, 0, 0, 0},
  {"debug", debugCommand, -1, "as", 0, NULL, 0, 0, 0, 0, 0},
  {"config", configCommand, -2, "lat", 0, NULL, 0, 0, 0, 0, 0},
  {"subscribe", subscribeCommand, -2, "pslt", 0, NULL, 0, 0, 0, 0, 0},
  {"unsubscribe", unsubscribeCommand, -1, "pslt", 0, NULL, 0, 0, 0, 0, 0},
  {"psubscribe", psubscribeCommand, -2, "pslt", 0, NULL, 0, 0, 0, 0, 0},
  {"punsubscribe", punsubscribeCommand, -1, "pslt", 0, NULL, 0, 0, 0, 0, 0},
  {"publish", publishCommand, 3, "pltF", 0, NULL, 0, 0, 0, 0, 0},
  {"pubsub", pubsubCommand, -2, "pltR", 0, NULL, 0, 0, 0, 0, 0},
  {"watch", watchCommand, -2, "sF", 0, NULL, 1, -1, 1, 0, 0},
  {"unwatch", unwatchCommand, 1, "sF", 0, NULL, 0, 0, 0, 0, 0},
  {"cluster", clusterCommand, -2, "a", 0, NULL, 0, 0, 0, 0, 0},
  {"restore", restoreCommand, -4, "wm", 0, NULL, 1, 1, 1, 0, 0},
  {"restore-asking", restoreCommand, -4, "wmk", 0, NULL, 1, 1, 1, 0, 0},
  {"migrate", migrateCommand, -6, "w", 0, migrateGetKeys, 0, 0, 0, 0, 0},
  {"asking", askingCommand, 1, "F", 0, NULL, 0, 0, 0, 0, 0},
  {"readonly", readonlyCommand, 1, "F", 0, NULL, 0, 0, 0, 0, 0},
  {"readwrite", readwriteCommand, 1, "F", 0, NULL, 0, 0, 0, 0, 0},
  {"dump", dumpCommand, 2, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"object", objectCommand, -2, "r", 0, NULL, 2, 2, 2, 0, 0},
  {"memory", memoryCommand, -2, "r", 0, NULL, 0, 0, 0, 0, 0},
  {"client", clientCommand, -2, "as", 0, NULL, 0, 0, 0, 0, 0},
  {"eval", evalCommand, -3, "s", 0, evalGetKeys, 0, 0, 0, 0, 0},
  {"evalsha", evalShaCommand, -3, "s", 0, evalGetKeys, 0, 0, 0, 0, 0},
  {"slowlog", slowlogCommand, -2, "a", 0, NULL, 0, 0, 0, 0, 0},
  {"script", scriptCommand, -2, "s", 0, NULL, 0, 0, 0, 0, 0},
  {"time", timeCommand, 1, "RF", 0, NULL, 0, 0, 0, 0, 0},
  {"bitop", bitopCommand, -4, "wm", 0, NULL, 2, -1, 1, 0, 0},
  {"bitcount", bitcountCommand, -2, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"bitpos", bitposCommand, -3, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"wait", waitCommand, 3, "s", 0, NULL, 0, 0, 0, 0, 0},
  {"command", commandCommand, 0, "lt", 0, NULL, 0, 0, 0, 0, 0},
  {"geoadd", geoaddCommand, -5, "wm", 0, NULL, 1, 1, 1, 0, 0},
  {"georadius", georadiusCommand, -6, "w", 0, georadiusGetKeys, 1, 1, 1, 0, 0},
  {"georadius_ro",
   georadiusroCommand,
   -6,
   "r",
   0,
   georadiusGetKeys,
   1,
   1,
   1,
   0,
   0},  // NOLINT
  {"georadiusbymember",
   georadiusbymemberCommand,
   -5,
   "w",
   0,
   georadiusGetKeys,
   1,
   1,
   1,
   0,
   0},  // NOLINT
  {"georadiusbymember_ro",
   georadiusbymemberroCommand,
   -5,
   "r",
   0,
   georadiusGetKeys,
   1,
   1,
   1,
   0,
   0},  // NOLINT
  {"geohash", geohashCommand, -2, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"geopos", geoposCommand, -2, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"geodist", geodistCommand, -4, "r", 0, NULL, 1, 1, 1, 0, 0},
  {"pfselftest", pfselftestCommand, 1, "a", 0, NULL, 0, 0, 0, 0, 0},
  {"pfadd", pfaddCommand, -2, "wmF", 0, NULL, 1, 1, 1, 0, 0},
  {"pfcount", pfcountCommand, -2, "r", 0, NULL, 1, -1, 1, 0, 0},
  {"pfmerge", pfmergeCommand, -2, "wm", 0, NULL, 1, -1, 1, 0, 0},
  {"pfdebug", pfdebugCommand, -3, "w", 0, NULL, 2, 2, 1, 0, 0},
  {"post", securityWarningCommand, -1, "lt", 0, NULL, 0, 0, 0, 0, 0},
  {"host:", securityWarningCommand, -1, "lt", 0, NULL, 0, 0, 0, 0, 0},
  {"latency", latencyCommand, -2, "aslt", 0, NULL, 0, 0, 0, 0, 0}};

/* Populates the Redis Command Table starting from the hard coded list
 * we have on top of redis.c file. */
void populateCommandTable(void) {
  int j;
  int numcommands = sizeof(redisCommandTable) / sizeof(struct redisCommand);

  for (j = 0; j < numcommands; j++) {
    struct redisCommand* c = redisCommandTable + j;
    c->flags = getCommandFlags(c->sflags);
  }
}

struct redisCommand* getCommandFromTable(const char* cmd) {
  int j;
  int numcommands = sizeof(redisCommandTable) / sizeof(struct redisCommand);

  for (j = 0; j < numcommands; j++) {
    struct redisCommand* c = redisCommandTable + j;

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

/* Like serverLogRaw() but with printf-alike support. This is the function that
 * is used across the code. The raw version is only used in order to dump
 * the INFO output on crash. */
void serverLogOld(int level, const char* fmt, ...) {
  va_list ap;
  char msg[1024];

  va_start(ap, fmt);
  vsnprintf(msg, sizeof(msg), fmt, ap);
  va_end(ap);

  switch (level) {
    case LL_DEBUG:
      DLOG(INFO) << msg;
      break;
    case LL_WARNING:
      LOG(ERROR) << msg;
      break;
    case LL_NOTICE:
      LOG(WARNING) << msg;
      break;
    case LL_VERBOSE:
      LOG(INFO) << msg;
      break;
    default:
      INVARIANT_D(0);
      LOG(ERROR) << msg;
      break;
  }
}

void sha256_transform(SHA256_CTX* ctx, const BYTE data[]) {
  WORD a, b, c, d, e, f, g, h, i, j, t1, t2, m[64];

  for (i = 0, j = 0; i < 16; ++i, j += 4)
    m[i] = (data[j] << 24) | (data[j + 1] << 16) | (data[j + 2] << 8) |
      (data[j + 3]);
  for (; i < 64; ++i)
    m[i] = SIG1(m[i - 2]) + m[i - 7] + SIG0(m[i - 15]) + m[i - 16];

  a = ctx->state[0];
  b = ctx->state[1];
  c = ctx->state[2];
  d = ctx->state[3];
  e = ctx->state[4];
  f = ctx->state[5];
  g = ctx->state[6];
  h = ctx->state[7];

  for (i = 0; i < 64; ++i) {
    t1 = h + EP1(e) + CH(e, f, g) + k[i] + m[i];
    t2 = EP0(a) + MAJ(a, b, c);
    h = g;
    g = f;
    f = e;
    e = d + t1;
    d = c;
    c = b;
    b = a;
    a = t1 + t2;
  }

  ctx->state[0] += a;
  ctx->state[1] += b;
  ctx->state[2] += c;
  ctx->state[3] += d;
  ctx->state[4] += e;
  ctx->state[5] += f;
  ctx->state[6] += g;
  ctx->state[7] += h;
}

void sha256_init(SHA256_CTX* ctx) {
  ctx->datalen = 0;
  ctx->bitlen = 0;
  ctx->state[0] = 0x6a09e667;
  ctx->state[1] = 0xbb67ae85;
  ctx->state[2] = 0x3c6ef372;
  ctx->state[3] = 0xa54ff53a;
  ctx->state[4] = 0x510e527f;
  ctx->state[5] = 0x9b05688c;
  ctx->state[6] = 0x1f83d9ab;
  ctx->state[7] = 0x5be0cd19;
}

void sha256_update(SHA256_CTX* ctx, const BYTE data[], size_t len) {
  WORD i;

  for (i = 0; i < len; ++i) {
    ctx->data[ctx->datalen] = data[i];
    ctx->datalen++;
    if (ctx->datalen == 64) {
      sha256_transform(ctx, ctx->data);
      ctx->bitlen += 512;
      ctx->datalen = 0;
    }
  }
}

void sha256_final(SHA256_CTX* ctx, BYTE hash[]) {
  WORD i;

  i = ctx->datalen;

  // Pad whatever data is left in the buffer.
  if (ctx->datalen < 56) {
    ctx->data[i++] = 0x80;
    while (i < 56)
      ctx->data[i++] = 0x00;
  } else {
    ctx->data[i++] = 0x80;
    while (i < 64)
      ctx->data[i++] = 0x00;
    sha256_transform(ctx, ctx->data);
    memset(ctx->data, 0, 56);
  }

  // Append to the padding the total message's length in bits and transform.
  ctx->bitlen += ctx->datalen * 8;
  ctx->data[63] = ctx->bitlen;
  ctx->data[62] = ctx->bitlen >> 8;
  ctx->data[61] = ctx->bitlen >> 16;
  ctx->data[60] = ctx->bitlen >> 24;
  ctx->data[59] = ctx->bitlen >> 32;
  ctx->data[58] = ctx->bitlen >> 40;
  ctx->data[57] = ctx->bitlen >> 48;
  ctx->data[56] = ctx->bitlen >> 56;
  sha256_transform(ctx, ctx->data);

  // Since this implementation uses little endian byte ordering and SHA uses big
  // endian, reverse all the bytes when copying the final state to the output
  // hash.
  for (i = 0; i < 4; ++i) {
    hash[i] = (ctx->state[0] >> (24 - i * 8)) & 0x000000ff;
    hash[i + 4] = (ctx->state[1] >> (24 - i * 8)) & 0x000000ff;
    hash[i + 8] = (ctx->state[2] >> (24 - i * 8)) & 0x000000ff;
    hash[i + 12] = (ctx->state[3] >> (24 - i * 8)) & 0x000000ff;
    hash[i + 16] = (ctx->state[4] >> (24 - i * 8)) & 0x000000ff;
    hash[i + 20] = (ctx->state[5] >> (24 - i * 8)) & 0x000000ff;
    hash[i + 24] = (ctx->state[6] >> (24 - i * 8)) & 0x000000ff;
    hash[i + 28] = (ctx->state[7] >> (24 - i * 8)) & 0x000000ff;
  }
}

/* Get random bytes, attempts to get an initial seed from /dev/urandom and
 * the uses a one way hash function in counter mode to generate a random
 * stream. However if /dev/urandom is not available, a weaker seed is used.
 *
 * This function is not thread safe, since the state is global. */
void getRandomBytes(unsigned char* p, size_t len) {
  /* Global state. */
  static int seed_initialized = 0;
  static unsigned char seed[64]; /* 512 bit internal block size. */
  static uint64_t counter = 0;   /* The counter we hash with the seed. */

#ifdef _WIN32
  if (!seed_initialized) {
    for (unsigned int j = 0; j < sizeof(seed); j++) {
      seed[j] = random();
    }
    seed_initialized = 1;
  }
#else
  if (!seed_initialized) {
    /* Initialize a seed and use SHA1 in counter mode, where we hash
     * the same seed with a progressive counter. For the goals of this
     * function we just need non-colliding strings, there are no
     * cryptographic security needs. */
    FILE* fp = fopen("/dev/urandom", "r");
    if (fp == NULL || fread(seed, sizeof(seed), 1, fp) != 1) {
      /* Revert to a weaker seed, and in this case reseed again
       * at every call.*/
      for (unsigned int j = 0; j < sizeof(seed); j++) {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        pid_t pid = getpid();
        seed[j] = tv.tv_sec ^ tv.tv_usec ^ pid ^ (int64_t)fp;
      }
    } else {
      seed_initialized = 1;
    }
    if (fp)
      fclose(fp);
  }
#endif

  while (len) {
    /* This implements SHA256-HMAC. */
    unsigned char digest[SHA256_BLOCK_SIZE];
    unsigned char kxor[64];
    unsigned int copylen = len > SHA256_BLOCK_SIZE ? SHA256_BLOCK_SIZE : len;

    /* IKEY: key xored with 0x36. */
    memcpy(kxor, seed, sizeof(kxor));
    for (unsigned int i = 0; i < sizeof(kxor); i++)
      kxor[i] ^= 0x36;

    /* Obtain HASH(IKEY||MESSAGE). */
    SHA256_CTX ctx;
    sha256_init(&ctx);
    sha256_update(&ctx, kxor, sizeof(kxor));
    sha256_update(&ctx, (unsigned char*)&counter, sizeof(counter));
    sha256_final(&ctx, digest);

    /* OKEY: key xored with 0x5c. */
    memcpy(kxor, seed, sizeof(kxor));
    for (unsigned int i = 0; i < sizeof(kxor); i++)
      kxor[i] ^= 0x5C;

    /* Obtain HASH(OKEY || HASH(IKEY||MESSAGE)). */
    sha256_init(&ctx);
    sha256_update(&ctx, kxor, sizeof(kxor));
    sha256_update(&ctx, digest, SHA256_BLOCK_SIZE);
    sha256_final(&ctx, digest);

    /* Increment the counter for the next iteration. */
    counter++;

    memcpy(p, digest, copylen);
    len -= copylen;
    p += copylen;
  }
}

/* Generate the Redis "Run ID", a SHA1-sized random number that identifies a
 * given execution of Redis, so that if you are talking with an instance
 * having run_id == A, and you reconnect and it has run_id == B, you can be
 * sure that it is either a different instance or it was restarted. */
void getRandomHexChars(char* p, size_t len) {
  char charset[] = "0123456789abcdef";
  size_t j;

  getRandomBytes((unsigned char*)p, len);
  for (j = 0; j < len; j++)
    p[j] = charset[p[j] & 0x0F];
}

/* Modify the string substituting all the occurrences of the set of
 * characters specified in the 'from' string to the corresponding character
 * in the 'to' array.
 *
 * For instance: strmapchars(mystring, "ho", "01", 2)
 * will have the effect of turning the string "hello" into "0ell1".
 *
 * The function returns the sds string pointer, that is always the same
 * as the input pointer since no resize is needed. */
void strmapchars(std::string& s, const char *from,      // NOLINT
                 const char *to, size_t setlen) {
  size_t j, i, l = s.length();

  for (j = 0; j < l; j++) {
    for (i = 0; i < setlen; i++) {
      if (s[j] == from[i]) {
        s[j] = to[i];
        break;
      }
    }
  }
  return;
}

}  // namespace redis_port
}  // namespace tendisplus
