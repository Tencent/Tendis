#ifndef SRC_TENDISPLUS_INCLUDE_UNISTD_H_

#define SRC_TENDISPLUS_INCLUDE_UNISTD_H_
#include <io.h>
#include <process.h>

#define __attribute__(x)
#define __builtin_expect(EXP, C) ((EXP) == (C))

#ifndef _SSIZE_T
#define _SSIZE_T
typedef int ssize_t;
#endif


void SetThreadName(uint64_t dwThreadID, const char* szThreadName);

#define pthread_self GetCurrentThreadId
#define pthread_getname_np(id, str, size) \
  strncpy(str, "windows thread name", (size)-1)

int pthread_setname_np(uint32_t id, const char* name) {
  SetThreadName(id, name);
  return 0;
}

int gettimeofday(struct timeval* tp, void* tzp);

void sleep(uint64_t seconds);

int rand_r(unsigned int* seedp);
struct tm* localtime_r(const time_t* timep, struct tm* tmp);

#endif  // SRC_TENDISPLUS_INCLUDE_UNISTD_H_
