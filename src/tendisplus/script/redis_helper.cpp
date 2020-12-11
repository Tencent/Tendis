//
// Created by takenliu on 2020/12/9.
//

#include "redis_helper.h"

/* Modify the string substituting all the occurrences of the set of
 * characters specified in the 'from' string to the corresponding character
 * in the 'to' array.
 *
 * For instance: strmapchars(mystring, "ho", "01", 2)
 * will have the effect of turning the string "hello" into "0ell1".
 *
 * The function returns the sds string pointer, that is always the same
 * as the input pointer since no resize is needed. */
void strmapchars(std::string& s, const char *from, const char *to, size_t setlen) {
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