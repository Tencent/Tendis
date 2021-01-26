// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifdef _WIN32
#include <stdlib.h>
#include <assert.h>
#include <time.h>
#include <unistd.h>
#include <windows.h>
#include <WinBase.h>
#include "tendisplus/utils/invariant.h"

int gettimeofday(struct timeval* tp, void* tzp) {
  unsigned int ticks;
  ticks = GetTickCount();
  tp->tv_usec = ticks * 1000;
  tp->tv_sec = ticks / 1000;

  return 0;
}

void sleep(uint64_t seconds) {
  Sleep(seconds * 1000);
}

int rand_r(unsigned int* seedp) {
  srand(*seedp ? *seedp : (unsigned)time(NULL));

  return rand();
}

typedef struct tagTHREADNAME_INFO {
  DWORD dwType;      // must be 0x1000
  LPCSTR szName;     // pointer to name (in user addr space)
  DWORD dwThreadID;  // thread ID (-1=caller thread)
  DWORD dwFlags;     // reserved for future use, must be zero
} THREADNAME_INFO;

void SetThreadName(DWORD dwThreadID, LPCSTR szThreadName) {
  THREADNAME_INFO info;
  info.dwType = 0x1000;
  info.szName = szThreadName;
  info.dwThreadID = dwThreadID;
  info.dwFlags = 0;

  if (strlen(szThreadName) > 15) {
    abort();
  }

  __try {
    RaiseException(0x406D1388,
                   0,
                   sizeof(info) / sizeof(DWORD),
                   (ULONG_PTR*)&info);  // NOLINT
  } __except(EXCEPTION_CONTINUE_EXECUTION) {
  }
}

int pthread_setname_np(uint32_t id, const char* name) {
  SetThreadName((DWORD)id, name);
  return 0;
}


struct tm* mylocaltime_r(const time_t* timep, struct tm* result) {
  localtime_s(result, timep);
  return result;
}

#endif  // _WIN32

