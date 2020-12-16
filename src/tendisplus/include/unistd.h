// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_INCLUDE_UNISTD_H_

#define SRC_TENDISPLUS_INCLUDE_UNISTD_H_
#include <io.h>
#include <process.h>
#include <stdint.h>

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

int pthread_setname_np(uint32_t id, const char* name);

int gettimeofday(struct timeval* tp, void* tzp);

void sleep(uint64_t seconds);

int rand_r(unsigned int* seedp);
struct tm* mylocaltime_r(const time_t* timep, struct tm* result);
#define localtime_r mylocaltime_r

#endif  // SRC_TENDISPLUS_INCLUDE_UNISTD_H_
