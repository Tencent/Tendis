#ifndef _UNISTD_H

#define _UNISTD_H
#include <io.h>
#include <process.h>

#define __attribute__(x)
#define __builtin_expect(EXP, C) ((EXP) == (C))

#ifndef _SSIZE_T
#define _SSIZE_T
typedef int ssize_t;
#endif


void SetThreadName(unsigned long dwThreadID, const char* szThreadName);

#define pthread_self GetCurrentThreadId
#define pthread_getname_np(id, str, size) \
  strncpy(str, "windows thread name", (size)-1)
#define pthread_setname_np(id, str) SetThreadName(id, str)

int gettimeofday(struct timeval* tp, void* tzp);

void sleep(unsigned long seconds);

int rand_r(unsigned int* seedp);
struct tm* localtime_r(const time_t* timep, struct tm* tmp);

#endif /* _UNISTD_H */