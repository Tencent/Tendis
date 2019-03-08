#ifdef _WIN32
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <windows.h>
#include <WinBase.h>

int gettimeofday(struct timeval *tp, void *tzp)
{
  unsigned int ticks;
  ticks = GetTickCount();
  tp->tv_usec = ticks * 1000;
  tp->tv_sec = ticks / 1000;

  return 0;
}

void sleep(unsigned long seconds)
{
  Sleep(seconds * 1000);
}

int rand_r(unsigned int *seedp)
{
  srand(*seedp ? *seedp : (unsigned)time(NULL));

  return rand();

}

//struct tm * localtime_r(const time_t *timep, struct tm *tmp)
//{
//  localtime_s(tmp, timep);
//  return tmp;
//}

#endif // _WIN32