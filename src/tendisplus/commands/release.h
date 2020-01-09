#ifndef RELEASE_H
#define RELEASE_H

#define TENDISPLUS_GIT_SHA1 "7f34c080"
#define TENDISPLUS_GIT_DIRTY "42"
#define TENDISPLUS_BUILD_ID "TENCENT64site-1578575524"

#include <stdint.h>
char *redisGitSHA1(void);
char *redisGitDirty(void);
uint64_t redisBuildId(void);

#endif // RELEASE_H
