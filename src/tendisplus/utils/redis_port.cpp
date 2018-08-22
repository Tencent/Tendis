#include <sstream>

#include <limits.h>
#include "tendisplus/utils/redis_port.h"

namespace tendisplus {
namespace redis_port {

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

}  // namespace redis_port
}  // namespace tendisplus
